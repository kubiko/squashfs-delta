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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// semiCompressible returns bytes in the regime patch runs exist for: hex digits
// carry four bits per byte, so xz halves them. That matters because the two ends
// of the trade are both non-trivial -- text compresses to nearly nothing, making
// literals free, and random bytes do not compress at all, so mksquashfs stores
// them raw and there is no compression to avoid. Real snap content (ELF objects,
// Python bytecode) sits between the two, which is where this does.
func semiCompressible(n int, seed int64) []byte {
	raw := make([]byte, n/2+1)
	rand.New(rand.NewSource(seed)).Read(raw)
	return []byte(hex.EncodeToString(raw))[:n]
}

// populateChurn is the source side of a patch-run fixture. steady.bin never
// changes and big.bin changes wholesale, which is the shape of a real revision
// pair: most of the image is carried by OP_COPY and the rest has to be rebuilt.
func populateChurn(t *testing.T, dir string) {
	t.Helper()
	writeFile(t, dir, "big.bin", semiCompressible(600000, 42))
	writeFile(t, dir, "steady.bin", semiCompressible(600000, 7))
	writeFile(t, dir, "sub/small.txt", []byte("hello world\n"))
}

// churnEdit inserts a few kilobytes near the front of big.bin. The insertion
// shifts every following block, so not one of them matches the source verbatim
// and OP_COPY can carry none of the file -- yet the source still holds almost
// all of the plaintext. That is exactly the case OP_PATCHRUN exists for, and the
// case the pseudo-file formats pay for by recompressing the whole image.
func churnEdit(t *testing.T, dir string) {
	t.Helper()
	b := semiCompressible(600000, 42)
	edited := make([]byte, 0, len(b)+4000)
	edited = append(edited, b[:1000]...)
	edited = append(edited, semiCompressible(4000, 99)...)
	edited = append(edited, b[1000:]...)
	writeFile(t, dir, "big.bin", edited)
}

// churnPair builds the source/target pair the patch-run tests share. Each test
// generates its own delta, since what is under test is the generator's choices.
func churnPair(t *testing.T) (source, target string) {
	t.Helper()
	requireTools(t, "mksquashfs", "xz", "hdiffz", "hpatchz")
	source = buildImage(t, "churn-source.snap", populateChurn)
	target = buildImage(t, "churn-target.snap", func(t *testing.T, dir string) {
		populateChurn(t, dir)
		churnEdit(t, dir)
	})
	return source, target
}

// applyAndCompare applies a delta and fails unless the result is the target byte
// for byte, which is the only acceptance criterion the format has.
func applyAndCompare(t *testing.T, source, delta, target string, comp Compressor) *applyStats {
	t.Helper()
	src, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	df, err := os.Open(delta)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()

	var got bytes.Buffer
	stats, err := applyBlockPlan(context.Background(), src, df, &got, blockPlanApplyOpts{Comp: comp})
	if err != nil {
		t.Fatalf("applying the delta: %v", err)
	}
	want, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("the reconstruction differs from the target at offset %d", firstDiff(got.Bytes(), want))
	}
	return stats
}

// TestPatchRunBeatsLiterals is the whole point of OP_PATCHRUN: the same pair is
// encoded twice, once with runs disabled, and the run version has to be
// substantially smaller while still reconstructing the target exactly.
func TestPatchRunBeatsLiterals(t *testing.T) {
	ctx := context.Background()
	source, target := churnPair(t)
	dir := t.TempDir()

	litPath := filepath.Join(dir, "literals.delta")
	lit, err := generateBlockPlan(ctx, source, target, litPath, blockPlanGenOpts{
		Comp: &xzCLI{}, Verify: true, NoPatchRuns: true,
	})
	if err != nil {
		t.Fatalf("generating a literals-only delta: %v", err)
	}
	if lit.PatchRuns != 0 {
		t.Errorf("NoPatchRuns still emitted %d patch runs", lit.PatchRuns)
	}
	if lit.PatchedUBytes != 0 {
		t.Errorf("a literals-only delta asks the device to compress %d bytes of data plaintext",
			lit.PatchedUBytes)
	}

	runPath := filepath.Join(dir, "runs.delta")
	run, err := generateBlockPlan(ctx, source, target, runPath, blockPlanGenOpts{
		Comp: &xzCLI{}, Verify: true,
	})
	if err != nil {
		t.Fatalf("generating a delta with patch runs: %v", err)
	}
	if run.PatchRuns == 0 {
		t.Fatalf("no patch run was emitted for a wholly shifted file; runs went to literals instead "+
			"(%d no window, %d not worth it, %d failed verify)",
			run.RunsNoWindow, run.RunsTooExpensive, run.RunsVerifyFailed)
	}
	// The insertion shifts every block of big.bin, so the source holds the
	// plaintext but none of the compressed bytes. A patch against it should be
	// a rounding error next to shipping those blocks whole.
	if run.DeltaSize > lit.DeltaSize/2 {
		t.Errorf("patch runs saved too little: %d bytes against %d for literals", run.DeltaSize, lit.DeltaSize)
	}
	if run.PatchBytes >= int64(len(semiCompressible(4000, 99)))*4 {
		t.Errorf("a patch for a 4000-byte insertion came to %d bytes", run.PatchBytes)
	}

	// What the device pays for that: it compresses the run's plaintext and
	// decompresses a window to feed it, and both must be far below the whole
	// image -- otherwise this is just the pseudo-file format again.
	st := applyAndCompare(t, source, runPath, target, &xzCLI{})
	if st.PatchRuns != run.PatchRuns {
		t.Errorf("applied %d patch runs, the delta declares %d", st.PatchRuns, run.PatchRuns)
	}
	if st.WindowUBytes == 0 {
		t.Error("a patch run ran without reading any source plaintext")
	}
	if st.UCompressedBytes <= st.MetaUBytes {
		t.Error("a patch run compressed no data plaintext at all")
	}
	// steady.bin did not change, so the compressor must never have seen it.
	// This is the CPU saving itself, and the assertion the pseudo-file formats
	// cannot make at any size.
	if st.CopiedBytes == 0 {
		t.Error("nothing was copied verbatim, so the unchanged half of the image was rebuilt")
	}
	if st.UCompressedBytes >= run.TargetUBytes {
		t.Errorf("the apply compressed %d bytes of plaintext out of a %d-byte target -- no CPU was saved",
			st.UCompressedBytes, run.TargetUBytes)
	}
}

// TestPatchRunSplitsAtRunCap holds the applier's memory bound: a changed region
// larger than the cap has to become several runs, not one oversized one.
func TestPatchRunSplitsAtRunCap(t *testing.T) {
	ctx := context.Background()
	source, target := churnPair(t)
	delta := filepath.Join(t.TempDir(), "capped.delta")

	// Two blocks' worth. big.bin spans five, so the run cannot be emitted whole.
	const cap2 = 2 * 131072
	stats, err := generateBlockPlan(ctx, source, target, delta, blockPlanGenOpts{
		Comp: &xzCLI{}, Verify: true, MaxRunUSize: cap2,
	})
	if err != nil {
		t.Fatalf("generating under a %d-byte run cap: %v", cap2, err)
	}
	if stats.PatchRuns < 2 {
		t.Errorf("a five-block change under a two-block cap produced %d patch runs", stats.PatchRuns)
	}
	// The header's cap is what the applier enforces, so it has to travel.
	df, err := os.Open(delta)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	br, err := openBlockPlan(df)
	if err != nil {
		t.Fatal(err)
	}
	if br.Header.MaxRunUSize != cap2 {
		t.Errorf("the delta declares a %d-byte run cap, want %d", br.Header.MaxRunUSize, cap2)
	}
	applyAndCompare(t, source, delta, target, &xzCLI{})
}

// sabotagingComp is a compressor that produces wrong bytes for data blocks and
// correct ones for metadata. It stands in for the real hazard behind the
// generator's per-block verification: a compressor that does not reproduce what
// mksquashfs produced, whether that is a different liblzma, a different preset,
// or a squashfs-tools that has moved on.
//
// Only data blocks are perturbed, so the metadata patch still applies and the
// only thing under test is the data-block path.
type sabotagingComp struct {
	inner    Compressor
	metaDict int
	// blocks counts the data blocks perturbed, so a test can tell the
	// sabotage happened rather than the run being declined for some other
	// reason.
	blocks int
}

func (s *sabotagingComp) MaxBlocksPerCall() int { return s.inner.MaxBlocksPerCall() }

func (s *sabotagingComp) CompressBlocks(ctx context.Context, plain BlockPlain, uSizes []int, dictSize int,
	fn func(idx int, blk CompressedBlock) error) error {

	return s.inner.CompressBlocks(ctx, plain, uSizes, dictSize, func(idx int, blk CompressedBlock) error {
		// A raw block's on-disk bytes are its plaintext, so there is nothing
		// in the compressor's output to corrupt.
		if dictSize != s.metaDict && !blk.Raw && len(blk.OnDisk) > 8 {
			// Flip a bit deep inside the LZMA2 payload rather than truncating,
			// so the block stays a well-formed stream of exactly the right
			// length. The applier's own check is on the length, so only the
			// generator's byte comparison can catch this -- which is the point.
			bad := append([]byte(nil), blk.OnDisk...)
			bad[len(bad)/2] ^= 0x01
			blk.OnDisk = bad
			s.blocks++
		}
		return fn(idx, blk)
	})
}

// TestPatchRunDowngradesOnVerifyFailure is the safety property that lets the
// cost model be a knob rather than a correctness risk. Given a compressor whose
// data blocks do not match the target's, every candidate run must fall back to
// OP_LITERAL and the delta must still reconstruct the target exactly.
//
// Both sides use the sabotaging compressor, because it stands for the machine's
// xz rather than a fault in one process: the generator discovers it cannot
// reproduce a data block, ships literals, and the applier -- now never asked to
// compress a data block -- produces the right image regardless.
func TestPatchRunDowngradesOnVerifyFailure(t *testing.T) {
	ctx := context.Background()
	source, target := churnPair(t)
	delta := filepath.Join(t.TempDir(), "sabotaged.delta")

	im, err := openSquashfsImage(target)
	if err != nil {
		t.Fatal(err)
	}
	comp := &sabotagingComp{inner: &xzCLI{}, metaDict: squashfsMetadataSize}
	if int(im.SB.BlockSize) == squashfsMetadataSize {
		t.Fatalf("the fixture's block size equals the metadata dictionary, so the saboteur cannot tell them apart")
	}

	// Verify is what makes this a proof rather than a hope: had any run
	// survived with a corrupt block, the gate would have caught the bad image
	// here and generation would fail.
	stats, err := generateBlockPlan(ctx, source, target, delta, blockPlanGenOpts{
		Comp: comp, Verify: true,
	})
	if err != nil {
		t.Fatalf("generation did not survive a compressor it cannot trust: %v", err)
	}
	if comp.blocks == 0 {
		t.Fatal("no data block was perturbed, so nothing was under test")
	}
	if stats.RunBlockMismatches == 0 || stats.RunsVerifyFailed == 0 {
		t.Errorf("the corruption went unnoticed: %d block mismatches over %d failed runs",
			stats.RunBlockMismatches, stats.RunsVerifyFailed)
	}
	if stats.PatchRuns != 0 {
		t.Errorf("%d patch runs were emitted from blocks that do not recompress", stats.PatchRuns)
	}
	if stats.PatchedUBytes != 0 {
		t.Errorf("a fully downgraded delta still asks the device to compress %d bytes", stats.PatchedUBytes)
	}
	if stats.RunsRejectedBytes == 0 {
		t.Error("the downgraded runs are not accounted for in RunsRejectedBytes")
	}

	st := applyAndCompare(t, source, delta, target, comp)
	if st.PatchRuns != 0 || st.PatchedBytes != 0 {
		t.Errorf("the apply ran %d patch runs over %d bytes, expected none", st.PatchRuns, st.PatchedBytes)
	}
	// The only plaintext the compressor saw was metadata, which the saboteur
	// leaves alone -- so a delta this conservative asks for no data-block
	// compression whatsoever, which is what makes it safe.
	if st.UCompressedBytes != st.MetaUBytes {
		t.Errorf("compressed %d bytes of plaintext but only %d were metadata",
			st.UCompressedBytes, st.MetaUBytes)
	}
}

// TestSrcWindowPicker holds the two rules the applier depends on: a window is
// whole blocks, and every block in it agrees on being compressed or raw. Both
// exist because the window is decompressed as a unit by a single `xz -dc`, which
// cannot walk past a block that is not an xz stream.
func TestSrcWindowPicker(t *testing.T) {
	requireTools(t, "mksquashfs", "xz")
	ctx := context.Background()
	// populateMixed has both compressed blocks and a raw one, so both kinds of
	// window occur.
	im, err := openSquashfsImage(buildImage(t, "windows.snap", populateMixed))
	if err != nil {
		t.Fatal(err)
	}
	ext, gaps, overlaps, err := im.CheckCoverage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 0 || len(overlaps) != 0 {
		t.Fatalf("fixture has %d gaps and %d overlaps", len(gaps), len(overlaps))
	}
	pick := newSrcWindowPicker(im, ext)

	byOffset := map[int64]Extent{}
	for _, e := range ext {
		byOffset[e.Offset] = e
	}

	sawPlain, sawCompressed := false, false
	for _, from := range ext {
		w, ok := pick.window(from.Offset, 1<<30)
		if !ok {
			t.Fatalf("no window at offset %d, which is a block boundary", from.Offset)
		}
		if w.Off != from.Offset {
			t.Errorf("window from %d starts at %d", from.Offset, w.Off)
		}
		// Walk the window back into extents: it must be exactly the blocks
		// extentsIn reports, contiguous, all of one kind, and its declared
		// plaintext must be their total.
		in := pick.extentsIn(w)
		if len(in) == 0 {
			t.Fatalf("window %+v spans no extents", w)
		}
		var c, u int
		at := w.Off
		for _, e := range in {
			if e.Offset != at {
				t.Errorf("window %+v is not contiguous at %d", w, e.Offset)
			}
			if e.Raw != in[0].Raw {
				t.Errorf("window %+v mixes raw and compressed blocks", w)
			}
			c += e.CSize
			u += e.USize
			at += int64(e.CSize)
		}
		if c != w.Len || u != w.ULen {
			t.Errorf("window %+v spans %d on-disk and %d plaintext bytes", w, c, u)
		}
		// The invariant the applier reads the window through: already-plaintext
		// exactly when the blocks are stored raw.
		if w.Plain() != in[0].Raw {
			t.Errorf("window %+v reports Plain()=%v over raw=%v blocks", w, w.Plain(), in[0].Raw)
		}
		if in[0].Raw {
			sawPlain = true
		} else {
			sawCompressed = true
		}

		// A window is at least one whole block even when that overshoots the
		// budget, because half a block cannot be decompressed.
		one, ok := pick.window(from.Offset, 1)
		if !ok {
			t.Fatalf("no minimal window at %d", from.Offset)
		}
		if one.Len != from.CSize || one.ULen != from.USize {
			t.Errorf("minimal window at %d is %+v, want one block of %d/%d",
				from.Offset, one, from.CSize, from.USize)
		}
	}
	if !sawPlain || !sawCompressed {
		t.Errorf("the fixture exercised only one kind of window (plain %v, compressed %v)", sawPlain, sawCompressed)
	}

	// Landing mid-block rounds forward, since a partial block is not
	// decompressible; and there is nothing to be had past the region's end.
	mid := ext[0].Offset + 1
	if w, ok := pick.window(mid, 1<<30); !ok || w.Off <= mid {
		t.Errorf("a window from mid-block %d did not round forward: %+v (ok=%v)", mid, w, ok)
	}
	last := ext[len(ext)-1]
	if w, ok := pick.window(last.Offset+int64(last.CSize), 1<<30); ok {
		t.Errorf("a window was found past the end of the data region: %+v", w)
	}
	if _, ok := pick.window(0, 0); ok {
		t.Error("a window was found with no plaintext budget")
	}
}

// TestRunWorthCompressing walks each branch of the cost model, which is the only
// place the delta-size-against-device-CPU trade is decided.
func TestRunWorthCompressing(t *testing.T) {
	tune := defaultPatchRunTuning(8 << 20)
	tests := []struct {
		name                          string
		patch, literal, uTotal, would int
		want                          bool
	}{
		{name: "a small patch replacing large literals", patch: 2000, literal: 300000, uTotal: 600000, want: true},
		// Under the absolute floor, this is not worth three forks whatever its
		// rate looks like.
		{name: "saving below the process overhead", patch: 1000, literal: 9000, uTotal: 20000},
		// Above the floor but the device would compress 2 MiB to save 20 KB of
		// delta, which is the trade MinSavingRate exists to refuse.
		{name: "a good ratio at a bad rate", patch: 80000, literal: 100000, uTotal: 2000000},
		// A fine rate, but the patch is barely under the literals, so there is
		// nothing here worth having.
		{name: "too close to the literals", patch: 950000, literal: 1000000, uTotal: 1000000},
		{name: "a patch larger than the literals", patch: 200000, literal: 100000, uTotal: 400000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := runWorthCompressing(tc.patch, tc.literal, tc.uTotal, tune); got != tc.want {
				t.Errorf("runWorthCompressing(patch=%d, literal=%d, uTotal=%d) = %v, want %v",
					tc.patch, tc.literal, tc.uTotal, got, tc.want)
			}
		})
	}
	// buildPatchRun screens runs with a zero-byte patch before it does any work,
	// which is only sound if the model is monotonic in the patch size: whatever
	// passes at some size must pass at every smaller one. Check that over a grid
	// rather than trusting the arithmetic, since the pre-check and the real check
	// are the same function and a non-monotonic model would silently drop runs
	// that deserved to be emitted.
	for literal := 1; literal <= 1<<20; literal *= 4 {
		for uTotal := 1; uTotal <= 1<<24; uTotal *= 8 {
			best := runWorthCompressing(0, literal, uTotal, tune)
			for patch := 1; patch <= 2*literal; patch = patch*3 + 1 {
				if runWorthCompressing(patch, literal, uTotal, tune) && !best {
					t.Fatalf("a %d-byte patch passes where a zero-byte one does not "+
						"(literal=%d, uTotal=%d), so the pre-check would drop it", patch, literal, uTotal)
				}
			}
		}
	}

	// Disabling the rate check must let the bad-rate case through, since that
	// is how the sweeps measure what the check is buying.
	loose := tune
	loose.MinSavingRate = 0
	if !runWorthCompressing(80000, 100000, 2000000, loose) {
		t.Error("clearing MinSavingRate did not admit the run it was rejecting")
	}
}

// flakyComp gives the right answer the first time it sees a block's plaintext
// and a wrong one every time after. That is what it takes to slip past the
// generator's per-block verification: the run is checked once, passes, and is
// emitted -- and then the applier compresses the same plaintext again and gets
// different bytes. Nothing but the final gate catches it.
//
// The bit flip keeps the block's length intact, so the applier's own cSize check
// cannot see it either. Metadata is left alone, as in sabotagingComp.
type flakyComp struct {
	inner    Compressor
	metaDict int
	seen     map[[32]byte]bool
	spoiled  int
}

func (f *flakyComp) MaxBlocksPerCall() int { return f.inner.MaxBlocksPerCall() }

func (f *flakyComp) CompressBlocks(ctx context.Context, plain BlockPlain, uSizes []int, dictSize int,
	fn func(idx int, blk CompressedBlock) error) error {

	if f.seen == nil {
		f.seen = map[[32]byte]bool{}
	}
	at := 0
	offs := make([]int, len(uSizes))
	for i, u := range uSizes {
		offs[i] = at
		at += u
	}
	return f.inner.CompressBlocks(ctx, plain, uSizes, dictSize, func(idx int, blk CompressedBlock) error {
		if dictSize != f.metaDict && !blk.Raw && len(blk.OnDisk) > 8 {
			// Reading the block's plaintext back may reuse the buffer the inner
			// compressor read it into, which is safe here only because this
			// branch excludes raw blocks -- for a compressed one OnDisk is the
			// freshly framed stream, not a view of that buffer.
			src, err := plain.Block(offs[idx], uSizes[idx])
			if err != nil {
				return err
			}
			key := sha256.Sum256(src)
			if f.seen[key] {
				bad := append([]byte(nil), blk.OnDisk...)
				bad[len(bad)/2] ^= 0x01
				blk.OnDisk = bad
				f.spoiled++
			}
			f.seen[key] = true
		}
		return fn(idx, blk)
	})
}

// TestBlockPlanGateRefusesUnverifiableDelta is the last line of defence. A run
// that passes per-block verification and then rebuilds wrongly must be caught by
// the final gate, and the delta must not be left behind on disk -- otherwise a
// caller that drops the error would publish an image that does not apply.
func TestBlockPlanGateRefusesUnverifiableDelta(t *testing.T) {
	ctx := context.Background()
	source, target := churnPair(t)
	delta := filepath.Join(t.TempDir(), "unverifiable.delta")

	comp := &flakyComp{inner: &xzCLI{}, metaDict: squashfsMetadataSize}
	if _, err := generateBlockPlan(ctx, source, target, delta, blockPlanGenOpts{
		Comp: comp, Verify: true,
	}); err == nil {
		t.Fatal("a delta that does not reconstruct the target was accepted")
	}
	if comp.spoiled == 0 {
		t.Error("no block was spoiled on its second sighting, so the gate was never tested")
	}
	if _, err := os.Stat(delta); !os.IsNotExist(err) {
		t.Errorf("the rejected delta is still on disk (stat: %v)", err)
	}

	// Without the gate the same generation succeeds, which is what makes the
	// gate rather than the verification the thing under test here.
	comp2 := &flakyComp{inner: &xzCLI{}, metaDict: squashfsMetadataSize}
	if _, err := generateBlockPlan(ctx, source, target, delta, blockPlanGenOpts{Comp: comp2}); err != nil {
		t.Fatalf("generating without the gate: %v", err)
	}
	if _, err := os.Stat(delta); err != nil {
		t.Errorf("an ungated delta was not written: %v", err)
	}
}
