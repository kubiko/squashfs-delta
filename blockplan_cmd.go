// -*- Mode: Go; indent-tabs-mode: t -*-

/*
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 3 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// applyBlockPlanToFile applies a block-plan delta from an already-open delta
// file, writing targetSnap.
func applyBlockPlanToFile(ctx context.Context, sourceSnap string, delta io.Reader, targetSnap string, opts blockPlanApplyOpts) error {
	_, err := applyBlockPlanReport(ctx, sourceSnap, delta, targetSnap, opts, false)
	return err
}

// applyBlockPlanReport applies a delta and optionally prints what the apply
// cost, which is the number the format exists to reduce.
func applyBlockPlanReport(ctx context.Context, sourceSnap string, delta io.Reader, targetSnap string, opts blockPlanApplyOpts, report bool) (*applyStats, error) {
	if opts.Comp == nil {
		opts.Comp = &xzCLI{}
	}
	src, err := os.Open(sourceSnap)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	// The target is written to a temporary file and renamed, so a failed apply
	// never leaves a half-built snap where a real one is expected.
	tmp := targetSnap + ".part"
	out, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	defer func() {
		out.Close()
		os.Remove(tmp)
	}()

	t0 := time.Now()
	stats, err := applyBlockPlan(ctx, src, delta, out, opts)
	if err != nil {
		return nil, err
	}
	if err := out.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, targetSnap); err != nil {
		return nil, err
	}
	if report {
		printApplyStats(stats, time.Since(t0))
	}
	return stats, nil
}

// peakRSS is this process's own high-water resident size, which is the quantity
// the run cap is supposed to bound. It is reported separately from
// /usr/bin/time -v because that reports the maximum over the whole process tree,
// so a heavy xz or hpatchz child hides -- or takes the blame for -- what the
// applier itself holds. Returns 0 if the kernel does not offer VmHWM.
func peakRSS() int64 {
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(status), "\n") {
		rest, ok := strings.CutPrefix(line, "VmHWM:")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) < 1 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0
		}
		return kb << 10
	}
	return 0
}

func printApplyStats(s *applyStats, elapsed time.Duration) {
	fmt.Printf("applied in %.1fs\n", elapsed.Seconds())
	fmt.Printf("  instructions   %d (%d copy, %d literal, %d patch run)\n",
		s.Instructions, s.Copies, s.Literals, s.PatchRuns)
	fmt.Printf("  copied         %s straight from the source, no compressor\n", humanBytes(s.CopiedBytes))
	fmt.Printf("  literal        %s from the delta\n", humanBytes(s.LiteralBytes))
	fmt.Printf("  patched        %s of plaintext rebuilt from %s of source plaintext\n",
		humanBytes(s.PatchedBytes), humanBytes(s.WindowUBytes))
	fmt.Printf("  compressed     %s of plaintext in %d blocks\n",
		humanBytes(s.UCompressedBytes), s.BlocksCompressed)
	fmt.Printf("                 of which metadata %s in %d blocks\n",
		humanBytes(s.MetaUBytes), s.MetaBlocks)
	if rss := peakRSS(); rss > 0 {
		fmt.Printf("  peak RSS       %s in this process\n", humanBytes(rss))
	}
	fmt.Printf("  peak scratch   %s in memfds (RAM, but not resident)\n",
		humanBytes(s.PeakScratchBytes))
}

// cmdGenerateBlocks generates a block-plan delta and reports its composition.
func cmdGenerateBlocks(ctx context.Context, sourceSnap, targetSnap, delta string, threads, maxRun int, verify, noPatchRuns bool, windowRatio, minSavingRate float64) error {
	// The sweeps drive the cost model from the command line; everything else
	// leaves it alone.
	var tune *patchRunTuning
	if windowRatio > 0 || minSavingRate != 0 {
		t := defaultPatchRunTuning(maxRun)
		if windowRatio > 0 {
			t.WindowRatio = windowRatio
		}
		if minSavingRate != 0 {
			t.MinSavingRate = max(minSavingRate, 0)
		}
		tune = &t
	}
	stats, err := generateBlockPlan(ctx, sourceSnap, targetSnap, delta, blockPlanGenOpts{
		Comp:        &xzCLI{threads: threads},
		MaxRunUSize: maxRun,
		Verify:      verify,
		NoPatchRuns: noPatchRuns,
		Tuning:      tune,
	})
	if err != nil {
		return err
	}
	fmt.Printf("%s -> %s\n", sourceSnap, targetSnap)
	fmt.Printf("  delta          %s in %.1fs\n", humanBytes(stats.DeltaSize), stats.Elapsed.Seconds())
	fmt.Printf("  instructions   %d (%d copy, %d literal, %d patch run), %s encoded -> %s stored\n",
		stats.Instructions, stats.Copies, stats.Literals, stats.PatchRuns,
		humanBytes(int64(stats.InstrBytes)), humanBytes(int64(stats.InstrStored)))
	fmt.Printf("  data region    %s on disk, %s of plaintext\n",
		humanBytes(stats.TargetDataBytes), humanBytes(stats.TargetUBytes))
	fmt.Printf("  copied         %s (%.1f%% of on-disk bytes)\n",
		humanBytes(stats.CopiedBytes), pct(stats.CopiedBytes, stats.TargetDataBytes))
	fmt.Printf("  literal        %s\n", humanBytes(stats.LiteralBytes))
	fmt.Printf("  patch runs     %s of patch rebuilding %s of plaintext\n",
		humanBytes(stats.PatchBytes), humanBytes(stats.PatchedUBytes))
	if n := stats.RunsNoWindow + stats.RunsTooExpensive + stats.RunsVerifyFailed; n > 0 {
		fmt.Printf("  runs as literal %d (%d no window, %d not worth it, %d failed verify), %s\n",
			n, stats.RunsNoWindow, stats.RunsTooExpensive, stats.RunsVerifyFailed,
			humanBytes(stats.RunsRejectedBytes))
	}
	fmt.Printf("  metadata       %d blocks, %s of plaintext, %s patch\n",
		stats.MetaBlocks, humanBytes(stats.MetaUBytes), humanBytes(int64(stats.MDPatchBytes)))
	// The headline. The old pseudo-file formats decompress the whole source and
	// recompress the whole target, so the comparison is against the target's
	// entire plaintext; here the compressor only ever sees a patch run's blocks
	// plus the metadata region.
	whole := stats.TargetUBytes + stats.MetaUBytes
	comp := stats.PatchedUBytes + stats.MetaUBytes
	fmt.Printf("  apply compresses %s of %s (%.1f%% avoided) and decompresses %s\n",
		humanBytes(comp), humanBytes(whole), pct(whole-comp, whole),
		humanBytes(stats.WindowUBytes))
	if verify {
		fmt.Printf("  verified       the delta reconstructs %s exactly\n", targetSnap)
	}
	return nil
}

func pct(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	return 100 * float64(part) / float64(whole)
}
