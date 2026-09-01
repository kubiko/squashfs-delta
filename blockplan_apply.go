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
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
)

// Applying a block-plan delta is a single forward pass that writes the target
// exactly once:
//
//	[superblock from SEC_SB][data region from the instructions]
//	[metadata region recompressed from the patched blob][SEC_MDTAIL][zero pad]
//
// The applier makes no squashfs structural decisions. It never parses an inode,
// never walks a directory, never chooses between compressed and raw. Every such
// decision was made and verified by the generator and arrives as an explicit
// number, so the only thing that can go wrong here is a mismatch -- and every
// mismatch is checked:
//
//   - SEC_CANARY proves the local xz reproduces the generator's bytes, before a
//     single target byte is written;
//   - every recompressed block's on-disk length must equal the recorded one;
//   - the data region must end exactly on inode_table_start, the metadata region
//     exactly on export_table_start, the tail exactly on bytes_used;
//   - the whole image's sha256 must equal targetSHA256.
//
// The source is read at random offsets and never held whole, so peak memory is
// set by the run cap in the header, not by the size of the image.

// applyStats records what the apply actually did. UCompressedBytes is the number
// that justifies the format: it is the plaintext the device had to push through
// xz, against the whole image in the pseudo-file formats.
type applyStats struct {
	Instructions int
	Copies       int
	Literals     int
	PatchRuns    int

	CopiedBytes  int64
	LiteralBytes int64
	PatchedBytes int64

	// BlocksCompressed and UCompressedBytes count the compression work: data
	// blocks from patch runs plus every metadata block.
	BlocksCompressed int
	UCompressedBytes int64

	// WindowUBytes is the source plaintext read back to feed the patch runs.
	// It is the format's only data-region decompression, and it is bounded by
	// what changed rather than by the size of the image.
	WindowUBytes int64

	// MetaBlocks and MetaUBytes break out the metadata region, which is
	// recompressed unconditionally and so is the format's CPU floor.
	MetaBlocks int
	MetaUBytes int64

	// PeakScratchBytes is the most one patch run held in its three scratch files
	// at once: the source window, the patch, and the reconstructed plaintext.
	//
	// It is reported separately from resident memory because those files are
	// memfds. That keeps them out of the heap and out of the resident set, but
	// tmpfs is still RAM, so this is the other half of an apply's memory demand
	// -- and the half MaxRunUSize bounds directly.
	PeakScratchBytes int64
}

// blockPlanApplyOpts carries what the apply needs beyond the three streams.
type blockPlanApplyOpts struct {
	// Comp compresses recompressed blocks. Required.
	Comp Compressor
	// HpatchzPath is the patch tool binary; empty means look it up.
	HpatchzPath string
	// MaxRunUSize is the largest patch run this applier is willing to accept.
	// It is how a device with a memory budget refuses a delta up front rather
	// than discovering the cost partway through assembling an image -- falling
	// back to a full download is a far better outcome than failing late.
	//
	// The header's cap is what the instruction decoder bounds every run and
	// window against, so capping the header is enough to cap the work. Zero
	// accepts whatever the delta asks for.
	MaxRunUSize int
	// SkipSourceDigest omits hashing the source. The digest is what makes
	// every OP_COPY safe, so it is only skipped by callers that already know
	// the source is right -- the generator's own final gate.
	SkipSourceDigest bool
}

// applyState is the mutable context threaded through one apply.
type applyState struct {
	br    *blockPlanReader
	src   *os.File
	w     *targetWriter
	pay   *crcReader
	opts  blockPlanApplyOpts
	stats applyStats
	// blockSize is both the image block size and the LZMA2 dictionary size
	// for data blocks.
	blockSize int
}

// applyBlockPlan reconstructs the target image from source and delta, writing it
// to out. src must be the exact source revision the delta was generated against.
func applyBlockPlan(ctx context.Context, src *os.File, delta io.Reader, out io.Writer, opts blockPlanApplyOpts) (*applyStats, error) {
	if opts.Comp == nil {
		return nil, fmt.Errorf("no compressor configured")
	}
	br, err := openBlockPlan(delta)
	if err != nil {
		return nil, err
	}
	h := br.Header

	// Negotiate the run cap before anything is read, written or forked. A patch
	// run needs its plaintext and its source window at once, so this one number
	// sets the apply's whole memory demand -- and a caller that cannot afford it
	// wants to know now.
	if opts.MaxRunUSize > 0 && int64(h.MaxRunUSize) > int64(opts.MaxRunUSize) {
		return nil, fmt.Errorf("the delta's patch runs need up to %s at once and this applier allows %s",
			humanBytes(int64(h.MaxRunUSize)), humanBytes(int64(opts.MaxRunUSize)))
	}

	fi, err := src.Stat()
	if err != nil {
		return nil, err
	}
	if uint64(fi.Size()) != h.SourceSize {
		return nil, fmt.Errorf("source is %d bytes, delta was built against %d", fi.Size(), h.SourceSize)
	}
	if !opts.SkipSourceDigest {
		if err := verifySourceDigest(src, fi.Size(), h.SourceSHA256); err != nil {
			return nil, err
		}
	}

	// The target superblock has to survive the same rules the generator
	// checked the real target against, or the delta describes an image this
	// code cannot assemble.
	sbBytes := br.section(secSB)
	if len(sbBytes) != 96 {
		return nil, fmt.Errorf("SEC_SB is %d bytes, want 96", len(sbBytes))
	}
	tsb, err := parseSquashfsHeader(sbBytes)
	if err != nil {
		return nil, fmt.Errorf("SEC_SB: %w", err)
	}
	if err := tsb.checkSupportedGeometry(int64(h.TargetSize)); err != nil {
		return nil, fmt.Errorf("target superblock: %w", err)
	}
	// The header duplicates three of the superblock's numbers so they can be
	// bounds-checked before any section is read. They must agree.
	if tsb.InodeTableStart != h.TargetInodeTableStart || tsb.BytesUsed != h.TargetBytesUsed {
		return nil, fmt.Errorf("SEC_SB disagrees with the delta header: inode_table %d vs %d, bytes_used %d vs %d",
			tsb.InodeTableStart, h.TargetInodeTableStart, tsb.BytesUsed, h.TargetBytesUsed)
	}
	if tsb.BlockSize != h.BlockSize {
		return nil, fmt.Errorf("SEC_SB block size %d disagrees with the delta header's %d", tsb.BlockSize, h.BlockSize)
	}

	st := &applyState{br: br, src: src, opts: opts, blockSize: int(h.BlockSize)}
	st.pay = newCRCReader(br.pay, br.payLen, br.payCRC)

	// The source's own metadata region feeds both the metadata patch and the
	// canary, so decode it up front: ~200 KB of xz, and any failure lands
	// before the target has been touched.
	srcMeta, err := readSourceMetaBlob(ctx, src, fi.Size())
	if err != nil {
		return nil, fmt.Errorf("source metadata: %w", err)
	}
	if canary := br.section(secCanary); canary != nil {
		if err := checkCanary(ctx, canary, srcMeta, opts.Comp, st.blockSize); err != nil {
			return nil, err
		}
	}

	// Everything the metadata region needs is settled before any data byte is
	// written, so a bad metadata patch costs no data work at all.
	mdFrame, mdDigest, err := decodeMDFrame(br.section(secMDFrame), int64(h.TargetInodeTableStart))
	if err != nil {
		return nil, fmt.Errorf("SEC_MDFRAME: %w", err)
	}
	var mdOnDisk int64
	var mdUTotal int
	for _, b := range mdFrame {
		mdOnDisk += int64(2 + b.CSize)
		mdUTotal += b.USize
	}
	if want := int64(tsb.ExportTableStart - tsb.InodeTableStart); mdOnDisk != want {
		return nil, fmt.Errorf("SEC_MDFRAME describes %d bytes of metadata, the superblock leaves room for %d",
			mdOnDisk, want)
	}
	targetMeta, err := buildTargetMetaBlob(ctx, br, srcMeta, mdUTotal, mdDigest, opts.HpatchzPath)
	if err != nil {
		return nil, err
	}

	st.w = newTargetWriter(out)
	if _, err := st.w.Write(sbBytes); err != nil {
		return nil, err
	}
	if err := st.applyInstructions(ctx); err != nil {
		return nil, err
	}
	if st.w.pos != int64(h.TargetInodeTableStart) {
		return nil, fmt.Errorf("the instructions produced %d bytes of data region, the superblock puts the inode table at %d",
			st.w.pos-96, h.TargetInodeTableStart)
	}

	if err := st.writeMetaRegion(ctx, targetMeta, mdFrame); err != nil {
		return nil, err
	}
	if st.w.pos != int64(tsb.ExportTableStart) {
		return nil, fmt.Errorf("the metadata region ended at %d, the superblock puts the export table at %d",
			st.w.pos, tsb.ExportTableStart)
	}

	tail := br.section(secMDTail)
	if want := int64(tsb.BytesUsed - tsb.ExportTableStart); int64(len(tail)) != want {
		return nil, fmt.Errorf("SEC_MDTAIL is %d bytes, the superblock leaves room for %d", len(tail), want)
	}
	if _, err := st.w.Write(tail); err != nil {
		return nil, err
	}
	if err := st.w.writeZeros(int64(h.TargetSize) - st.w.pos); err != nil {
		return nil, err
	}
	if st.w.pos != int64(h.TargetSize) {
		return nil, fmt.Errorf("wrote %d bytes, the delta describes a %d-byte image", st.w.pos, h.TargetSize)
	}

	// SEC_PAY is the last section, so draining it both finishes its CRC and
	// reveals payload the instructions never claimed.
	left, err := st.pay.drain()
	if err != nil {
		return nil, err
	}
	if err := st.pay.verify(); err != nil {
		return nil, err
	}
	if left != 0 {
		return nil, fmt.Errorf("%d bytes of SEC_PAY were not consumed by the instructions", left)
	}
	if got := st.w.digest.Sum(nil); !bytes.Equal(got, h.TargetSHA256[:]) {
		return nil, fmt.Errorf("reconstructed image digest %x does not match the delta's %x",
			got[:8], h.TargetSHA256[:8])
	}
	return &st.stats, nil
}

// applyInstructions walks the instruction stream, writing the data region.
func (st *applyState) applyInstructions(ctx context.Context) error {
	h := st.br.Header
	instr := st.br.section(secInstr)
	d := newInstrDecoder(instr, st.blockSize, int64(h.SourceSize), int64(h.MaxRunUSize))

	var in Instruction
	for i := uint32(0); i < h.InstrCount; i++ {
		if err := d.Next(&in); err != nil {
			return fmt.Errorf("instruction %d: %w", i, err)
		}
		switch in.Op {
		case opCopy:
			// The whole point of the format: already-compressed bytes
			// move from source to target with no compressor involved.
			n, err := copyNBuffer(st.w, io.NewSectionReader(st.src, in.SrcOff, in.Len), in.Len)
			if err != nil {
				return fmt.Errorf("instruction %d: copying %d bytes from source offset %d: %w",
					i, in.Len, in.SrcOff, err)
			}
			if n != in.Len {
				return fmt.Errorf("instruction %d: copied %d of %d bytes from source offset %d",
					i, n, in.Len, in.SrcOff)
			}
			st.stats.Copies++
			st.stats.CopiedBytes += in.Len
		case opLiteral:
			n, err := copyNBuffer(st.w, st.pay, in.Len)
			if err != nil {
				return fmt.Errorf("instruction %d: %w", i, err)
			}
			if n != in.Len {
				return fmt.Errorf("instruction %d: SEC_PAY ran out after %d of %d literal bytes", i, n, in.Len)
			}
			st.stats.Literals++
			st.stats.LiteralBytes += in.Len
		case opPatchRun:
			if err := st.applyPatchRun(ctx, &in); err != nil {
				return fmt.Errorf("instruction %d: %w", i, err)
			}
			st.stats.PatchRuns++
		default:
			return fmt.Errorf("instruction %d: opcode %v reached the applier", i, in.Op)
		}
		st.stats.Instructions++
	}
	if !d.done() {
		return fmt.Errorf("%d bytes remain after the header's %d instructions", len(instr)-d.pos, h.InstrCount)
	}
	return nil
}

// applyPatchRun reconstructs a run of blocks' plaintext by patching source
// windows, compresses each block, and writes it only if its on-disk length is
// exactly what the generator recorded.
func (st *applyState) applyPatchRun(ctx context.Context, in *Instruction) error {
	uTotal := in.USizeTotal()

	// Three files rather than three heap buffers, because these are the largest
	// things an apply handles: the run's plaintext, which MaxRunUSize caps, and
	// the source window it is rebuilt from, which is a multiple of that again.
	// The window streams from the source through `xz -dc` straight into the file
	// hpatchz reads, hpatchz writes the plaintext into a file of its own, and the
	// compressor reads that one block at a time -- so a 12 MiB window and an
	// 8 MiB run cost this process a pipe buffer and one block, not 20 MiB.
	oldFD, err := NewReusableMemFD("old")
	if err != nil {
		return err
	}
	defer oldFD.Close()
	nWin, err := gatherWindowsTo(ctx, st.src, in.Windows, oldFD.File)
	if err != nil {
		return err
	}
	st.stats.WindowUBytes += nWin

	patchFD, err := NewReusableMemFD("patch")
	if err != nil {
		return err
	}
	defer patchFD.Close()
	n, err := copyNBuffer(patchFD.File, st.pay, int64(in.PatchLen))
	if err != nil {
		return fmt.Errorf("reading a %d-byte patch from SEC_PAY: %w", in.PatchLen, err)
	}
	if n != int64(in.PatchLen) {
		return fmt.Errorf("SEC_PAY ran out after %d of %d patch bytes", n, in.PatchLen)
	}

	newFD, err := NewReusableMemFD("new")
	if err != nil {
		return err
	}
	defer newFD.Close()
	if err := runHpatchzFiles(ctx, oldFD, patchFD, newFD, uTotal, st.opts.HpatchzPath); err != nil {
		return err
	}
	// All three scratch files are live at once at this point, which makes now the
	// high-water mark of everything but the heap.
	if scratch := nWin + int64(in.PatchLen) + uTotal; scratch > st.stats.PeakScratchBytes {
		st.stats.PeakScratchBytes = scratch
	}

	blocks := in.Blocks
	uSizes := make([]int, len(blocks))
	for i, b := range blocks {
		uSizes[i] = b.USize
	}
	plain := newPlainFile(newFD.File, int(uTotal))
	return st.opts.Comp.CompressBlocks(ctx, plain, uSizes, st.blockSize, func(idx int, blk CompressedBlock) error {
		want := blocks[idx]
		if blk.OnDiskLen() != want.CSize || blk.Raw != want.Raw() {
			return fmt.Errorf("block %d recompressed to %d bytes (raw=%v), the delta says %d (raw=%v)",
				idx, blk.OnDiskLen(), blk.Raw, want.CSize, want.Raw())
		}
		if _, err := st.w.Write(blk.OnDisk); err != nil {
			return err
		}
		st.stats.BlocksCompressed++
		st.stats.UCompressedBytes += int64(want.USize)
		st.stats.PatchedBytes += int64(want.USize)
		return nil
	})
}

// gatherWindowsTo decompresses the source windows to dst, in order, and reports
// how much plaintext that was. dst is the patch's "old" file, holding exactly
// what the generator diffed against.
//
// This is the only decompression the applier does over the data region, and it is
// the format's second economy after OP_COPY: xz decompression runs roughly an
// order of magnitude faster than compression, so reading back the little source
// plaintext a changed run needs costs far less than the whole-image decompress the
// pseudo-file formats do -- and unlike them, it is never followed by recompressing
// bytes that did not change.
func gatherWindowsTo(ctx context.Context, src *os.File, windows []SrcWindow, dst io.Writer) (int64, error) {
	var total int64
	for _, win := range windows {
		// The compressed window streams out of the source rather than being read
		// into a buffer first, so the window's size does not reach the heap from
		// this side either.
		stored := io.NewSectionReader(src, win.Off, int64(win.Len))
		if win.Plain() {
			// Already plaintext, so there is nothing to decompress and no
			// process to run -- see SrcWindow.Plain.
			n, err := copyNBuffer(dst, stored, int64(win.Len))
			if err != nil {
				return total, fmt.Errorf("reading source window [%d,+%d): %w", win.Off, win.Len, err)
			}
			if n != int64(win.Len) {
				return total, fmt.Errorf("source window [%d,+%d) ended after %d bytes", win.Off, win.Len, n)
			}
			total += n
			continue
		}
		// One process for the whole window: the blocks in it are consecutive
		// complete xz streams, and concatenated streams decode in a single pass.
		// The declared plaintext length is enforced, so a window that does not
		// decompress to exactly ULen is rejected here.
		n, err := xzDecompressTo(ctx, dst, stored, win.ULen)
		if err != nil {
			return total, fmt.Errorf("decompressing source window [%d,+%d): %w", win.Off, win.Len, err)
		}
		total += n
	}
	return total, nil
}

// --- metadata region ---

// readSourceMetaBlob decompresses the source's metadata region. This is the only
// place the applier looks inside the source image, and it reads nothing but the
// superblock's table pointers and the two-byte metadata block headers -- no
// inodes, no directories, no block size words.
func readSourceMetaBlob(ctx context.Context, src *os.File, size int64) ([]byte, error) {
	head := make([]byte, 96)
	if _, err := src.ReadAt(head, 0); err != nil {
		return nil, err
	}
	sb, err := parseSquashfsHeader(head)
	if err != nil {
		return nil, err
	}
	if err := sb.checkSupportedGeometry(size); err != nil {
		return nil, err
	}
	start, end := int64(sb.InodeTableStart), int64(sb.ExportTableStart)
	region := make([]byte, end-start)
	if _, err := src.ReadAt(region, start); err != nil {
		return nil, err
	}
	// walkMetaRegion works over an image view, so present the region as one.
	view := &SquashfsImage{Path: src.Name(), Data: region, SB: sb}
	reg, err := view.walkMetaRegion(ctx, 0, int64(len(region)))
	if err != nil {
		return nil, err
	}
	return reg.Blob, nil
}

// buildTargetMetaBlob turns the source metadata blob into the target's, by
// patching it when SEC_MDPATCH is present and taking it unchanged when it is
// not -- which is how "the metadata did not change" is encoded.
//
// The digest check is what makes the ordering claim above true: it settles the
// whole metadata region here, before a single data byte is written, so a bad
// patch costs no data work rather than being caught at recompression time or,
// worse, only by the final image digest.
func buildTargetMetaBlob(ctx context.Context, br *blockPlanReader, srcMeta []byte, wantLen int, wantDigest [32]byte, hpatchzPath string) ([]byte, error) {
	out := srcMeta
	if patch := br.section(secMDPatch); patch != nil {
		var err error
		if out, err = runHpatchz(ctx, srcMeta, patch, int64(wantLen), hpatchzPath); err != nil {
			return nil, fmt.Errorf("SEC_MDPATCH: %w", err)
		}
	}
	if len(out) != wantLen {
		return nil, fmt.Errorf("target metadata is %d bytes, SEC_MDFRAME describes %d", len(out), wantLen)
	}
	if got := sha256.Sum256(out); got != wantDigest {
		return nil, fmt.Errorf("reconstructed metadata digest %x does not match SEC_MDFRAME's %x",
			got[:8], wantDigest[:8])
	}
	return out, nil
}

// writeMetaRegion recompresses the target metadata blob block by block, checking
// each against the framing the delta recorded. Unlike the data region this is
// unconditional work, but it is only ~600 KB of plaintext.
func (st *applyState) writeMetaRegion(ctx context.Context, blob []byte, frame []MetaBlock) error {
	uSizes := make([]int, len(frame))
	for i, b := range frame {
		uSizes[i] = b.USize
	}
	max := st.opts.Comp.MaxBlocksPerCall()
	base := 0
	for i := 0; i < len(frame); {
		j := min(i+max, len(frame))
		batch, batchU := frame[i:j], 0
		for _, b := range batch {
			batchU += b.USize
		}
		err := st.opts.Comp.CompressBlocks(ctx, plainBytes(blob[base:base+batchU]), uSizes[i:j], squashfsMetadataSize,
			func(idx int, blk CompressedBlock) error {
				want := batch[idx]
				if blk.OnDiskLen() != want.CSize || blk.Raw != want.Raw {
					return fmt.Errorf("metadata block %d recompressed to %d bytes (raw=%v), the delta says %d (raw=%v)",
						i+idx, blk.OnDiskLen(), blk.Raw, want.CSize, want.Raw)
				}
				size := uint16(want.CSize)
				if want.Raw {
					size |= metaUncompressedBit
				}
				var hdr [2]byte
				binary.LittleEndian.PutUint16(hdr[:], size)
				if _, err := st.w.Write(hdr[:]); err != nil {
					return err
				}
				_, err := st.w.Write(blk.OnDisk)
				return err
			})
		if err != nil {
			return err
		}
		st.stats.MetaBlocks += len(batch)
		st.stats.MetaUBytes += int64(batchU)
		st.stats.BlocksCompressed += len(batch)
		st.stats.UCompressedBytes += int64(batchU)
		base += batchU
		i = j
	}
	return nil
}

// --- patch tool ---

// runHpatchzFiles applies patchFD to oldFD, leaving exactly wantLen bytes in
// outFD. hpatchz works on paths, so all three are memfds -- and keeping the
// result in one is what lets a run's plaintext stay out of the heap.
//
// The length is checked here rather than by the caller because it is the only
// thing that can be checked before the bytes are used: a patch that reconstructs
// the wrong length has already gone wrong, whatever the blocks then recompress to.
func runHpatchzFiles(ctx context.Context, oldFD, patchFD, outFD *ReusableMemFD, wantLen int64, hpatchzPath string) error {
	if hpatchzPath == "" {
		var err error
		if hpatchzPath, err = toolPath("hpatchz"); err != nil {
			return err
		}
	}
	if err := applyHdiffzPatch(ctx, oldFD.Path, patchFD.Path, outFD.Path, hpatchzPath); err != nil {
		return fmt.Errorf("hpatchz: %w", err)
	}
	fi, err := outFD.File.Stat()
	if err != nil {
		return err
	}
	if fi.Size() != wantLen {
		return fmt.Errorf("the patch produced %d bytes, the delta says the run is %d", fi.Size(), wantLen)
	}
	return nil
}

// runHpatchz is runHpatchzFiles for callers that have, and want, plain byte
// slices: the metadata blob, which is under a megabyte, and the generator.
func runHpatchz(ctx context.Context, old, patch []byte, wantLen int64, hpatchzPath string) ([]byte, error) {
	oldFD, err := writeToMemFD("old", old)
	if err != nil {
		return nil, err
	}
	defer oldFD.Close()
	patchFD, err := writeToMemFD("patch", patch)
	if err != nil {
		return nil, err
	}
	defer patchFD.Close()
	outFD, err := NewReusableMemFD("new")
	if err != nil {
		return nil, err
	}
	defer outFD.Close()

	if err := runHpatchzFiles(ctx, oldFD, patchFD, outFD, wantLen, hpatchzPath); err != nil {
		return nil, err
	}
	if _, err := outFD.File.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	out := make([]byte, wantLen)
	if _, err := io.ReadFull(outFD.File, out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeToMemFD(name string, data []byte) (*ReusableMemFD, error) {
	fd, err := NewReusableMemFD(name)
	if err != nil {
		return nil, err
	}
	if _, err := fd.File.Write(data); err != nil {
		fd.Close()
		return nil, err
	}
	return fd, nil
}

// --- canary ---

// canarySize is how many bytes of the source metadata blob the canary
// compresses. It is well under a metadata block so both configurations see a
// realistic partial block, which is the common case: most blocks in a snap are
// partial tails.
const canarySize = 4096

// canaryPayloadLen is 4 + (4 + 32) per configuration.
const canaryPayloadLen = 4 + 2*36

// canaryDicts are the two configurations a delta uses, in the order the payload
// records them.
func canaryDicts(blockSize int) [2]int { return [2]int{blockSize, squashfsMetadataSize} }

// buildCanary compresses the head of the source metadata blob under both
// configurations and records what came out. It carries no input bytes: both
// sides already have the source.
func buildCanary(ctx context.Context, srcMeta []byte, comp Compressor, blockSize int) ([]byte, error) {
	n := min(canarySize, len(srcMeta))
	if n == 0 {
		return nil, fmt.Errorf("source metadata is empty, so no canary can be built")
	}
	out := make([]byte, 0, canaryPayloadLen)
	out = binary.LittleEndian.AppendUint32(out, uint32(n))
	for _, dict := range canaryDicts(blockSize) {
		blk, err := canaryBlock(ctx, comp, srcMeta[:n], dict)
		if err != nil {
			return nil, err
		}
		out = binary.LittleEndian.AppendUint32(out, uint32(len(blk)))
		sum := sha256.Sum256(blk)
		out = append(out, sum[:]...)
	}
	return out, nil
}

// checkCanary is the toolchain gate: if the local xz does not reproduce the
// generator's bytes then no block can be recompressed here, and that has to be
// found now rather than halfway through assembling an image.
func checkCanary(ctx context.Context, canary, srcMeta []byte, comp Compressor, blockSize int) error {
	if len(canary) != canaryPayloadLen {
		return fmt.Errorf("SEC_CANARY is %d bytes, want %d", len(canary), canaryPayloadLen)
	}
	n := int(binary.LittleEndian.Uint32(canary))
	if n <= 0 || n > len(srcMeta) || n > squashfsMetadataSize {
		return fmt.Errorf("SEC_CANARY declares %d bytes of a %d-byte source metadata blob", n, len(srcMeta))
	}
	for i, dict := range canaryDicts(blockSize) {
		rec := canary[4+i*36:]
		wantLen, wantSum := binary.LittleEndian.Uint32(rec), rec[4:36]
		blk, err := canaryBlock(ctx, comp, srcMeta[:n], dict)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(blk)
		if uint32(len(blk)) != wantLen || !bytes.Equal(sum[:], wantSum) {
			return fmt.Errorf("incompatible xz toolchain: with dict=%d the canary compresses to %d bytes %x, the delta expects %d bytes %x",
				dict, len(blk), sum[:8], wantLen, wantSum[:8])
		}
	}
	return nil
}

// canaryBlock compresses the canary input as a single block and returns its
// framed bytes. A store-raw verdict would mean the input was incompressible,
// which real metadata never is, so it is treated as a broken canary.
func canaryBlock(ctx context.Context, comp Compressor, plain []byte, dictSize int) ([]byte, error) {
	var framed []byte
	var raw bool
	err := comp.CompressBlocks(ctx, plainBytes(plain), []int{len(plain)}, dictSize, func(_ int, blk CompressedBlock) error {
		// OnDisk is only valid inside the callback, so keep a copy rather than
		// the block.
		framed = append([]byte(nil), blk.OnDisk...)
		raw = blk.Raw
		return nil
	})
	if err != nil {
		return nil, err
	}
	if raw {
		return nil, fmt.Errorf("the canary input did not compress at all, which no real metadata does")
	}
	return framed, nil
}

// --- output plumbing ---

// targetWriter counts and hashes everything written to the target, so the length
// and digest checks cost nothing extra.
type targetWriter struct {
	w      io.Writer
	pos    int64
	digest hash.Hash
	zeros  []byte
}

func newTargetWriter(w io.Writer) *targetWriter {
	return &targetWriter{w: w, digest: sha256.New()}
}

func (t *targetWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if n > 0 {
		t.digest.Write(p[:n])
		t.pos += int64(n)
	}
	return n, err
}

func (t *targetWriter) writeZeros(n int64) error {
	if n < 0 {
		return fmt.Errorf("image is %d bytes past its declared size", -n)
	}
	if t.zeros == nil {
		t.zeros = make([]byte, squashfsPadding)
	}
	for n > 0 {
		chunk := min(n, int64(len(t.zeros)))
		if _, err := t.Write(t.zeros[:chunk]); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

// crcReader hands out SEC_PAY while checksumming it. The section's CRC cannot be
// checked at open time -- the payload is never held whole -- so it is checked
// once the last byte has gone past.
type crcReader struct {
	r    io.Reader
	left int64
	crc  uint32
	want uint32
}

func newCRCReader(r io.Reader, n int64, want uint32) *crcReader {
	if r == nil {
		r = bytes.NewReader(nil)
	}
	return &crcReader{r: r, left: n, want: want}
}

func (c *crcReader) Read(p []byte) (int, error) {
	if c.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > c.left {
		p = p[:c.left]
	}
	n, err := c.r.Read(p)
	if n > 0 {
		c.crc = crc32.Update(c.crc, crc32.IEEETable, p[:n])
		c.left -= int64(n)
	}
	return n, err
}

// drain consumes whatever is left, so the CRC covers the whole section, and
// reports how much that was.
func (c *crcReader) drain() (int64, error) {
	return io.Copy(io.Discard, c)
}

func (c *crcReader) verify() error {
	if c.left != 0 {
		return fmt.Errorf("SEC_PAY is %d bytes short", c.left)
	}
	if c.crc != c.want {
		return fmt.Errorf("SEC_PAY is corrupt: CRC32 %#08x, expected %#08x", c.crc, c.want)
	}
	return nil
}

// verifySourceDigest is what makes every OP_COPY safe: the copied bytes are
// never checked individually, so the source as a whole is checked once.
func verifySourceDigest(src *os.File, size int64, want [32]byte) error {
	h := sha256.New()
	if _, err := copyBuffer(h, io.NewSectionReader(src, 0, size)); err != nil {
		return err
	}
	if got := h.Sum(nil); !bytes.Equal(got, want[:]) {
		return fmt.Errorf("source digest %x does not match the delta's %x", got[:8], want[:8])
	}
	return nil
}
