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
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/exec"
	"time"
)

// Generating a block-plan delta is where all the squashfs knowledge lives. It
// runs on a build machine with both images in memory and does the work the
// applier must not: enumerate every data block from the inode table, prove the
// blocks tile the data region exactly, match target blocks against the source by
// content, and record the exact geometry the applier will replay.
//
// The matcher is content-addressed on the *compressed* bytes. That is the whole
// trick: two revisions of a snap share most of their file contents, mksquashfs
// compresses each block independently, and the compressor is reproducible -- so a
// block whose plaintext did not change has byte-identical compressed bytes, and
// can be copied rather than rebuilt.

// genStats reports what a generated delta consists of.
type genStats struct {
	DeltaSize int64

	Instructions int
	Copies       int
	Literals     int
	PatchRuns    int

	TargetDataBytes int64
	CopiedBytes     int64
	LiteralBytes    int64
	PatchBytes      int64

	// TargetUBytes is the target's total plaintext; ReusedUBytes is how much of
	// it the applier gets without running the compressor. Their ratio is the
	// CPU saving, which is the point of the format.
	TargetUBytes int64
	ReusedUBytes int64

	// PatchedUBytes is the plaintext patch runs make the device compress, and
	// WindowUBytes what it must decompress to feed them. Together they are the
	// price paid for the delta-size reduction the runs buy.
	PatchedUBytes int64
	WindowUBytes  int64

	// Why candidate runs did not become patch runs. Each one shipped as
	// literals instead, which is correct but larger; RunsRejectedBytes is how
	// many on-disk bytes that cost.
	RunsNoWindow       int
	RunsTooExpensive   int
	RunsVerifyFailed   int
	RunBlockMismatches int
	RunsRejectedBytes  int64

	MetaBlocks   int
	MetaUBytes   int64
	MDPatchBytes int
	InstrBytes   int
	InstrStored  int

	Elapsed time.Duration
}

// blockPlanGenOpts configures generation.
type blockPlanGenOpts struct {
	Comp Compressor
	// HdiffzPath and HpatchzPath are the patch tool binaries; empty means
	// look them up.
	HdiffzPath  string
	HpatchzPath string
	// MaxRunUSize caps the plaintext one patch run may reconstruct, which is
	// what bounds the applier's peak memory. Zero picks a default.
	MaxRunUSize int
	// Verify runs the applier over the finished delta and refuses to keep it
	// unless the result is byte-identical to the real target.
	Verify bool
	// NoPatchRuns ships every changed block as a literal, which asks the
	// device for no data-block compression at all. It is the baseline the
	// patch runs are measured against, and a fallback if a patch tool is
	// unavailable.
	NoPatchRuns bool
	// Tuning replaces the whole patch-run cost model. Nil takes the defaults,
	// which is what everything but the sweeps and the tests wants.
	Tuning *patchRunTuning
}

// defaultMaxRunUSize sets what an apply demands in RAM, because a run's
// plaintext and its source window are held in scratch at once. With the default
// WindowRatio of 1.5 the scratch high-water mark is maxRun + 1.5*maxRun + the
// patch, which the M8 matrix measured at 20.4-21.6 MiB across the snapcraft and
// kernel pairs, on top of 4.3-13.2 MiB resident -- so roughly 33 MiB of total
// demand in the worst case, all of it bounded by this constant rather than by
// the largest file in the image.
//
// That is deliberately above the ~20 MiB first estimated for this work, which
// assumed a window ratio of 1.0; the ratio was later raised to 1.5 because it
// cuts the delta from 16.56 MiB to 5.13 MiB on the pair with the most churn.
// A caller that must hold a tighter ceiling lowers this and the applier's
// negotiation enforces it -- at the cost of a larger delta.
const defaultMaxRunUSize = 8 << 20

// generateBlockPlan writes a snap-2-1-blocks delta from sourcePath to
// targetPath at deltaPath.
func generateBlockPlan(ctx context.Context, sourcePath, targetPath, deltaPath string, opts blockPlanGenOpts) (*genStats, error) {
	t0 := time.Now()
	if opts.Comp == nil {
		return nil, fmt.Errorf("no compressor configured")
	}
	if opts.MaxRunUSize == 0 {
		opts.MaxRunUSize = defaultMaxRunUSize
	}

	src, err := openSquashfsImage(sourcePath)
	if err != nil {
		return nil, err
	}
	tgt, err := openSquashfsImage(targetPath)
	if err != nil {
		return nil, err
	}
	if err := src.checkSupported(); err != nil {
		return nil, fmt.Errorf("source %s: %w", sourcePath, err)
	}
	if err := tgt.checkSupported(); err != nil {
		return nil, fmt.Errorf("target %s: %w", targetPath, err)
	}
	// A delta only makes sense between images the same compressor produced the
	// same way; anything else means the applier cannot reproduce a block.
	if src.SB.BlockSize != tgt.SB.BlockSize {
		return nil, fmt.Errorf("source uses %d-byte blocks and target %d", src.SB.BlockSize, tgt.SB.BlockSize)
	}
	if src.SB.CompressionId != tgt.SB.CompressionId {
		return nil, fmt.Errorf("source uses compressor %d and target %d", src.SB.CompressionId, tgt.SB.CompressionId)
	}
	blockSize := int(tgt.SB.BlockSize)
	if opts.MaxRunUSize < blockSize {
		return nil, fmt.Errorf("run cap %d is below one block (%d)", opts.MaxRunUSize, blockSize)
	}

	srcExt, gaps, overlaps, err := src.CheckCoverage(ctx)
	if err != nil {
		return nil, err
	}
	if len(gaps) != 0 || len(overlaps) != 0 {
		return nil, fmt.Errorf("source data region has %d gaps and %d overlaps, so its blocks cannot be reused safely",
			len(gaps), len(overlaps))
	}
	tgtExt, gaps, overlaps, err := tgt.CheckCoverage(ctx)
	if err != nil {
		return nil, err
	}
	if len(gaps) != 0 || len(overlaps) != 0 {
		return nil, fmt.Errorf("target data region has %d gaps and %d overlaps, so it cannot be described block by block",
			len(gaps), len(overlaps))
	}

	stats := &genStats{}
	for _, e := range tgtExt {
		stats.TargetDataBytes += int64(e.CSize)
		stats.TargetUBytes += int64(e.USize)
	}

	// --- metadata ---

	srcMeta, err := src.MetaRegionAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("source metadata: %w", err)
	}
	tgtMeta, err := tgt.MetaRegionAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("target metadata: %w", err)
	}
	mdFrame, err := encodeMDFrame(tgtMeta.Blocks, tgtMeta.Blob)
	if err != nil {
		return nil, err
	}
	stats.MetaBlocks = len(tgtMeta.Blocks)
	stats.MetaUBytes = int64(len(tgtMeta.Blob))

	// --- instructions and payload ---

	pay, err := NewReusableMemFD("blockplan-pay")
	if err != nil {
		return nil, err
	}
	defer pay.Close()
	payw := newCRCWriter(pay.File)

	enc := newInstrEncoder(blockSize)
	index := indexExtents(src, srcExt)
	pick := newSrcWindowPicker(src, srcExt)
	tune := defaultPatchRunTuning(opts.MaxRunUSize)
	if opts.Tuning != nil {
		tune = *opts.Tuning
		tune.MaxRunUSize = opts.MaxRunUSize
	}
	tune.Disabled = tune.Disabled || opts.NoPatchRuns
	if err := emitDataRegion(ctx, tgt, tgtExt, index, pick, enc, payw, tune, opts, stats); err != nil {
		return nil, err
	}
	stats.Instructions = enc.Count()

	// --- assemble ---

	var w blockPlanWriter
	w.Header = blockPlanHeader{
		PatchTool:             DeltaToolHdiffz,
		BlockSize:             uint32(blockSize),
		MaxRunUSize:           uint32(opts.MaxRunUSize),
		InstrCount:            uint32(enc.Count()),
		SourceSize:            uint64(src.Size()),
		TargetSize:            uint64(tgt.Size()),
		TargetBytesUsed:       tgt.SB.BytesUsed,
		TargetInodeTableStart: tgt.SB.InodeTableStart,
		SourceSHA256:          sha256.Sum256(src.Data),
		TargetSHA256:          sha256.Sum256(tgt.Data),
	}

	canary, err := buildCanary(ctx, srcMeta.Blob, opts.Comp, blockSize)
	if err != nil {
		return nil, fmt.Errorf("building the canary: %w", err)
	}
	w.addSection(secSB, tgt.Data[:96])
	w.addSection(secCanary, canary)
	w.addSection(secMDFrame, mdFrame)
	w.addSection(secMDTail, tgt.Data[tgt.SB.ExportTableStart:tgt.SB.BytesUsed])
	if !bytes.Equal(srcMeta.Blob, tgtMeta.Blob) {
		// The metadata never survives verbatim between revisions: the inode
		// table encodes every block's size and offset. It is small, so a
		// patch plus one recompression pass is cheap.
		patch, err := runHdiffz(ctx, srcMeta.Blob, tgtMeta.Blob, opts.HdiffzPath)
		if err != nil {
			return nil, fmt.Errorf("diffing the metadata: %w", err)
		}
		stats.MDPatchBytes = len(patch)
		w.addSection(secMDPatch, patch)
	}
	stats.InstrBytes = len(enc.Bytes())
	if err := w.addSectionXZ(ctx, secInstr, enc.Bytes()); err != nil {
		return nil, err
	}
	for _, e := range w.sections {
		if e.entry.ID == secInstr {
			stats.InstrStored = int(e.entry.StoredLen)
		}
	}
	if payw.n > 0 {
		w.addSectionFile(secPay, pay.File, int(payw.n), payw.crc)
	}

	out, err := os.Create(deltaPath)
	if err != nil {
		return nil, err
	}
	if err := w.writeTo(out); err != nil {
		out.Close()
		os.Remove(deltaPath)
		return nil, err
	}
	if err := out.Close(); err != nil {
		os.Remove(deltaPath)
		return nil, err
	}
	if fi, err := os.Stat(deltaPath); err == nil {
		stats.DeltaSize = fi.Size()
	}

	// The final gate: a delta ships only once this machine has proved it
	// reconstructs the target, using the same code the device will run. One that
	// fails is deleted rather than left where it was written, so a caller that
	// drops the error cannot go on to publish it.
	if opts.Verify {
		if err := verifyBlockPlan(ctx, sourcePath, deltaPath, tgt, opts); err != nil {
			os.Remove(deltaPath)
			return nil, fmt.Errorf("the generated delta does not reconstruct the target: %w", err)
		}
	}
	stats.Elapsed = time.Since(t0)
	return stats, nil
}

// extentIndex maps the content of a source block to every offset it appears at.
type extentIndex struct {
	src    *SquashfsImage
	byHash map[[32]byte][]Extent
}

// indexExtents hashes every source block's compressed bytes. Hashing the
// compressed form rather than the plaintext is deliberate: it is what an OP_COPY
// actually needs to be true, it costs no decompression, and it cannot be fooled
// by two blocks that hold the same plaintext but were compressed differently.
func indexExtents(im *SquashfsImage, ext []Extent) *extentIndex {
	idx := &extentIndex{src: im, byHash: make(map[[32]byte][]Extent, len(ext))}
	for _, e := range ext {
		h := sha256.Sum256(im.Data[e.Offset : e.Offset+int64(e.CSize)])
		idx.byHash[h] = append(idx.byHash[h], e)
	}
	return idx
}

// find returns a source extent whose bytes equal want, preferring one that
// continues the current source run so the copies merge.
func (idx *extentIndex) find(want []byte, srcCursor int64) (Extent, bool) {
	var first Extent
	found := false
	for _, e := range idx.byHash[sha256.Sum256(want)] {
		// Guard against a hash collision the cheap way: a candidate is only
		// usable if the bytes really are equal.
		if !bytes.Equal(idx.src.Data[e.Offset:e.Offset+int64(e.CSize)], want) {
			continue
		}
		if e.Offset == srcCursor {
			return e, true
		}
		if !found {
			first, found = e, true
		}
	}
	return first, found
}

// emitDataRegion walks the target's blocks in ascending offset and turns each
// into a copy from the source, a patch run, or a literal in SEC_PAY.
//
// The extents tile the data region exactly, which CheckCoverage has already
// proved, so emitting them in order reproduces the region byte for byte.
//
// Matched blocks become OP_COPY and cost the device nothing. Everything else
// accumulates into a candidate run, which is closed out by the next match, by
// the run cap, or by the end of the region -- and only then does the cost model
// decide between a patch and literals. The decision has to wait that long
// because it needs the run's whole plaintext, both to diff it and to prove the
// blocks recompress.
func emitDataRegion(ctx context.Context, tgt *SquashfsImage, ext []Extent, idx *extentIndex,
	pick *srcWindowPicker, enc *instrEncoder, pay *crcWriter, tune patchRunTuning,
	opts blockPlanGenOpts, stats *genStats) error {

	// A copy can absorb the next block when both the target and the source
	// offsets are contiguous.
	var (
		copyOff, copyLen int64
		srcCursor        int64
		run              candidateRun
		maxBlocks        = opts.Comp.MaxBlocksPerCall()
	)
	flushCopy := func() error {
		if copyLen == 0 {
			return nil
		}
		if err := enc.Copy(copyOff, copyLen); err != nil {
			return err
		}
		stats.Copies++
		stats.CopiedBytes += copyLen
		copyLen = 0
		return nil
	}
	// litRun ships a run's blocks verbatim: their on-disk bytes are already
	// final, so one instruction covers the lot.
	litRun := func(r *candidateRun) error {
		var n int64
		for _, e := range r.ext {
			if _, err := pay.Write(tgt.Data[e.Offset : e.Offset+int64(e.CSize)]); err != nil {
				return err
			}
			n += int64(e.CSize)
		}
		if err := enc.Literal(n); err != nil {
			return err
		}
		stats.Literals++
		stats.LiteralBytes += n
		return nil
	}
	flushRun := func() error {
		if len(run.ext) == 0 {
			return nil
		}
		defer func() { run.ext = nil }()
		built, err := buildPatchRun(ctx, tgt, &run, pick, tune, opts, stats)
		if err != nil {
			return err
		}
		if built == nil {
			return litRun(&run)
		}
		if _, err := pay.Write(built.patch); err != nil {
			return err
		}
		if err := enc.PatchRun(built.blocks, built.windows, len(built.patch)); err != nil {
			return err
		}
		stats.PatchRuns++
		stats.PatchBytes += int64(len(built.patch))
		stats.PatchedUBytes += int64(run.USizeTotal())
		for _, w := range built.windows {
			stats.WindowUBytes += int64(w.ULen)
		}
		return nil
	}

	for _, e := range ext {
		want := tgt.Data[e.Offset : e.Offset+int64(e.CSize)]
		if m, ok := idx.find(want, srcCursor); ok {
			if err := flushRun(); err != nil {
				return err
			}
			if copyLen != 0 && copyOff+copyLen == m.Offset {
				copyLen += int64(m.CSize)
			} else {
				if err := flushCopy(); err != nil {
					return err
				}
				copyOff, copyLen = m.Offset, int64(m.CSize)
			}
			srcCursor = m.Offset + int64(m.CSize)
			stats.ReusedUBytes += int64(e.USize)
			continue
		}
		if err := flushCopy(); err != nil {
			return err
		}
		// Close the run before it outgrows either the memory cap or what the
		// compressor will take in one call.
		if len(run.ext) != 0 &&
			(run.USizeTotal()+e.USize > tune.MaxRunUSize || len(run.ext)+1 > maxBlocks) {
			if err := flushRun(); err != nil {
				return err
			}
		}
		if len(run.ext) == 0 {
			// The source cursor is where the preceding copy left off, so it
			// points at the source's version of whatever changed next.
			run.srcAnchor = srcCursor
		}
		run.ext = append(run.ext, e)
	}
	if err := flushRun(); err != nil {
		return err
	}
	return flushCopy()
}

// runHdiffz diffs old against updated, returning the patch. hdiffz works on
// paths, so all three files live in memfds.
func runHdiffz(ctx context.Context, old, updated []byte, hdiffzPath string) ([]byte, error) {
	if hdiffzPath == "" {
		var err error
		if hdiffzPath, err = toolPath("hdiffz"); err != nil {
			return nil, err
		}
	}
	oldFD, err := writeToMemFD("old", old)
	if err != nil {
		return nil, err
	}
	defer oldFD.Close()
	newFD, err := writeToMemFD("new", updated)
	if err != nil {
		return nil, err
	}
	defer newFD.Close()
	diffFD, err := NewReusableMemFD("diff")
	if err != nil {
		return nil, err
	}
	defer diffFD.Close()

	args := append(append([]string{}, hdiffzTuning...), "-f", oldFD.Path, newFD.Path, diffFD.Path)
	if err := runWithContext(ctx, exec.CommandContext(ctx, hdiffzPath, args...)); err != nil {
		return nil, fmt.Errorf("hdiffz: %w", err)
	}
	if _, err := diffFD.File.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return io.ReadAll(diffFD.File)
}

// verifyBlockPlan runs the applier over the finished delta and compares the
// result with the real target byte for byte.
func verifyBlockPlan(ctx context.Context, sourcePath, deltaPath string, tgt *SquashfsImage, opts blockPlanGenOpts) error {
	srcFile, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	deltaFile, err := os.Open(deltaPath)
	if err != nil {
		return err
	}
	defer deltaFile.Close()

	// The reconstruction is compared as it arrives rather than buffered: the
	// applier writes the image once, in order, so a comparing writer catches the
	// first wrong byte without a second copy of a 70 MB image in the heap.
	cmp := &compareWriter{want: tgt.Data}
	if _, err := applyBlockPlan(ctx, srcFile, deltaFile, cmp, blockPlanApplyOpts{
		Comp:        opts.Comp,
		HpatchzPath: opts.HpatchzPath,
		// The source is right here on disk and was just hashed into the
		// header, so re-reading it to hash it again proves nothing.
		SkipSourceDigest: true,
	}); err != nil {
		return err
	}
	if cmp.at != len(cmp.want) {
		return fmt.Errorf("the reconstruction is %d bytes, %s is %d", cmp.at, tgt.Path, len(cmp.want))
	}
	return nil
}

// compareWriter checks a stream against expected bytes as they are written,
// failing at the first difference. It is what lets the final gate hold the target
// image once rather than twice.
type compareWriter struct {
	want []byte
	at   int
}

func (c *compareWriter) Write(p []byte) (int, error) {
	if c.at+len(p) > len(c.want) {
		return 0, fmt.Errorf("the reconstruction is longer than the %d-byte target", len(c.want))
	}
	want := c.want[c.at : c.at+len(p)]
	if !bytes.Equal(p, want) {
		return 0, fmt.Errorf("the reconstruction differs from the target at offset %d", c.at+firstDiff(p, want))
	}
	c.at += len(p)
	return len(p), nil
}

// firstDiff is the offset of the first differing byte, or the shorter length.
func firstDiff(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// crcWriter counts and checksums what goes into SEC_PAY, so its table entry can
// be filled in without a second pass over the payload.
type crcWriter struct {
	w   io.Writer
	n   int64
	crc uint32
}

func newCRCWriter(w io.Writer) *crcWriter { return &crcWriter{w: w} }

func (c *crcWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.crc = crc32.Update(c.crc, crc32.IEEETable, p[:n])
		c.n += int64(n)
	}
	return n, err
}
