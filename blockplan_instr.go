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
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// The instruction stream describes the target's data region as a sequence of
// steps over ascending target offsets.
//
// Target offsets are never encoded: a cursor starts at 96, just past the
// superblock, and each instruction advances it by however many bytes it emits.
// Source offsets are encoded as signed deltas from a source cursor, which is
// close to zero for consecutive revisions of the same snap because their layouts
// stay near-monotonic.
//
// Two encoding invariants remove every flag byte:
//
//   - cSize == uSize means the block is stored uncompressed. This is airtight
//     because mksquashfs stores a block raw exactly when compressing it did not
//     shrink it, so a compressed block always has cSize < uSize.
//
//   - uSizeEnc == 0 means the full image block size. It saves less than it
//     looks: most files in a snap are under 128 KiB, so most blocks are partial
//     tails (7552 of 8756 in a snapcraft snap). It is free, so it stays.
//
// There is deliberately no OP_SPARSE. A hole occupies zero bytes on disk, so a
// byte-exact assembler emits nothing for it; the hole survives through the
// inode's size word, which travels inside the metadata patch.

type opcode byte

const (
	// opCopy copies already-compressed bytes straight from the source image.
	// This is the whole point of the format: zero CPU, and it carries 62-88%
	// of the bytes between consecutive revisions.
	opCopy opcode = 1
	// opPatchRun reconstructs a run of blocks' plaintext by patching source
	// windows, then compresses each block and checks its size against the
	// recorded one.
	opPatchRun opcode = 2
	// opLiteral copies final on-disk bytes out of SEC_PAY. Also the landing
	// place for any block whose recompression the generator could not verify.
	opLiteral opcode = 3
)

func (op opcode) String() string {
	switch op {
	case opCopy:
		return "COPY"
	case opPatchRun:
		return "PATCHRUN"
	case opLiteral:
		return "LITERAL"
	}
	return fmt.Sprintf("OP_%d", byte(op))
}

// PlanBlock is one target block's geometry, as the applier needs it: enough to
// build the --block-list argument and to check the result.
type PlanBlock struct {
	USize int
	CSize int
}

// Raw reports the store-uncompressed invariant.
func (b PlanBlock) Raw() bool { return b.CSize == b.USize }

// SrcWindow is a run of whole source data blocks whose *plaintext* is fed to the
// patch tool.
//
// Plaintext is the only useful thing to diff against. The target blocks a patch
// run rebuilds are plaintext, and compressed bytes share nothing with the
// plaintext they came from -- a patch from one to the other would be no smaller
// than simply shipping the target block, and would cost the device a
// recompression on top. So a window is decompressed before it is used.
//
// Off and Len address the on-disk bytes; ULen is what they decompress to. The
// applier needs no block boundaries within the window: concatenated xz streams
// decode in a single pass, so a window is one `xz -dc` regardless of how many
// blocks it spans. That only holds while every block in it is compressed, which
// is the generator's job to ensure.
//
// ULen == Len means the window is already plaintext and must not be
// decompressed, which is how a run of raw-stored blocks is expressed. The
// invariant is airtight for the same reason as PlanBlock.Raw: mksquashfs stores
// a block raw exactly when compressing it did not shrink it, so a run of
// compressed blocks always has Len < ULen.
type SrcWindow struct {
	Off  int64
	Len  int
	ULen int
}

// Plain reports that the window's on-disk bytes are already plaintext.
func (w SrcWindow) Plain() bool { return w.ULen == w.Len }

// Instruction is one decoded step. Which fields are meaningful depends on Op.
type Instruction struct {
	Op opcode

	// SrcOff and Len describe an opCopy; Len alone describes an opLiteral.
	SrcOff int64
	Len    int64

	// Blocks, Windows and PatchLen describe an opPatchRun.
	Blocks   []PlanBlock
	Windows  []SrcWindow
	PatchLen int
}

// OutLen is how many bytes this instruction writes to the target.
func (in *Instruction) OutLen() int64 {
	switch in.Op {
	case opCopy, opLiteral:
		return in.Len
	case opPatchRun:
		var n int64
		for _, b := range in.Blocks {
			n += int64(b.CSize)
		}
		return n
	}
	return 0
}

// USizeTotal is how much plaintext an opPatchRun reconstructs, which is what
// MaxRunUSize caps.
func (in *Instruction) USizeTotal() int64 {
	var n int64
	for _, b := range in.Blocks {
		n += int64(b.USize)
	}
	return n
}

// encodeUSize applies the "0 means a full block" invariant.
func encodeUSize(uSize, blockSize int) (uint64, error) {
	switch {
	case uSize == blockSize:
		return 0, nil
	case uSize <= 0 || uSize > blockSize:
		return 0, fmt.Errorf("block uncompressed size %d is outside (0,%d]", uSize, blockSize)
	}
	return uint64(uSize), nil
}

func decodeUSize(enc uint64, blockSize int) (int, error) {
	if enc == 0 {
		return blockSize, nil
	}
	if enc >= uint64(blockSize) {
		// A partial tail equal to blockSize would have encoded as 0, so
		// anything at or above it is a malformed stream.
		return 0, fmt.Errorf("block uncompressed size %d is not below the block size %d", enc, blockSize)
	}
	return int(enc), nil
}

// instrEncoder builds the instruction stream, tracking the source cursor so
// callers pass absolute source offsets and get deltas on the wire.
type instrEncoder struct {
	buf       []byte
	srcCur    int64
	count     int
	blockSize int
}

func newInstrEncoder(blockSize int) *instrEncoder {
	return &instrEncoder{srcCur: 96, blockSize: blockSize}
}

// Bytes returns the encoded stream, and Count the number of instructions in it.
func (e *instrEncoder) Bytes() []byte { return e.buf }
func (e *instrEncoder) Count() int    { return e.count }

func (e *instrEncoder) u(v uint64) { e.buf = binary.AppendUvarint(e.buf, v) }

// src encodes a source reference as a zigzag delta from the cursor, then moves
// the cursor past the referenced range.
func (e *instrEncoder) src(off int64, length int64) {
	e.buf = binary.AppendVarint(e.buf, off-e.srcCur)
	e.srcCur = off + length
}

// Copy emits an opCopy of length bytes of already-compressed source data.
func (e *instrEncoder) Copy(srcOff, length int64) error {
	if length <= 0 {
		return fmt.Errorf("copy of %d bytes", length)
	}
	if srcOff < 0 {
		return fmt.Errorf("copy from negative source offset %d", srcOff)
	}
	e.buf = append(e.buf, byte(opCopy))
	e.src(srcOff, length)
	e.u(uint64(length))
	e.count++
	return nil
}

// Literal emits an opLiteral consuming length bytes of SEC_PAY.
func (e *instrEncoder) Literal(length int64) error {
	if length <= 0 {
		return fmt.Errorf("literal of %d bytes", length)
	}
	e.buf = append(e.buf, byte(opLiteral))
	e.u(uint64(length))
	e.count++
	return nil
}

// PatchRun emits an opPatchRun over blocks, reconstructed from windows by a
// patch of patchLen bytes in SEC_PAY.
func (e *instrEncoder) PatchRun(blocks []PlanBlock, windows []SrcWindow, patchLen int) error {
	if len(blocks) == 0 {
		return fmt.Errorf("patch run with no blocks")
	}
	if patchLen < 0 {
		return fmt.Errorf("patch run with negative patch length %d", patchLen)
	}
	e.buf = append(e.buf, byte(opPatchRun))
	e.u(uint64(len(blocks)))
	for i, b := range blocks {
		enc, err := encodeUSize(b.USize, e.blockSize)
		if err != nil {
			return fmt.Errorf("patch run block %d: %w", i, err)
		}
		if b.CSize <= 0 || b.CSize > b.USize {
			return fmt.Errorf("patch run block %d: compressed size %d is outside (0,%d]", i, b.CSize, b.USize)
		}
		e.u(enc)
		e.u(uint64(b.CSize))
	}
	e.u(uint64(len(windows)))
	for i, w := range windows {
		if w.Len <= 0 || w.Off < 0 {
			return fmt.Errorf("patch run window %d: [%d,+%d)", i, w.Off, w.Len)
		}
		if w.ULen < w.Len {
			return fmt.Errorf("patch run window %d: %d on-disk bytes decompress to %d", i, w.Len, w.ULen)
		}
		e.src(w.Off, int64(w.Len))
		e.u(uint64(w.Len))
		// ULen >= Len always, so send the excess: zero for a plaintext window.
		e.u(uint64(w.ULen - w.Len))
	}
	e.u(uint64(patchLen))
	e.count++
	return nil
}

// instrDecoder walks an encoded stream, resolving deltas back to absolute
// offsets. It validates every field against the delta's geometry, since the
// stream is untrusted input on the device.
type instrDecoder struct {
	buf       []byte
	pos       int
	srcCur    int64
	blockSize int
	// sourceSize and maxRunUSize bound what the stream may ask for.
	sourceSize  int64
	maxRunUSize int64
}

func newInstrDecoder(buf []byte, blockSize int, sourceSize, maxRunUSize int64) *instrDecoder {
	return &instrDecoder{
		buf: buf, srcCur: 96, blockSize: blockSize,
		sourceSize: sourceSize, maxRunUSize: maxRunUSize,
	}
}

// done reports that the whole stream has been consumed.
func (d *instrDecoder) done() bool { return d.pos >= len(d.buf) }

func (d *instrDecoder) u() (uint64, error) {
	v, n := binary.Uvarint(d.buf[d.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("truncated or overlong unsigned varint at offset %d", d.pos)
	}
	d.pos += n
	return v, nil
}

func (d *instrDecoder) i() (int64, error) {
	v, n := binary.Varint(d.buf[d.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("truncated or overlong signed varint at offset %d", d.pos)
	}
	d.pos += n
	return v, nil
}

// srcRef decodes a source reference and checks it lies inside the source image.
func (d *instrDecoder) srcRef(length int64) (int64, error) {
	delta, err := d.i()
	if err != nil {
		return 0, err
	}
	off := d.srcCur + delta
	if off < 0 || length < 0 || off+length > d.sourceSize {
		return 0, fmt.Errorf("source reference [%d,+%d) is outside the %d-byte source", off, length, d.sourceSize)
	}
	d.srcCur = off + length
	return off, nil
}

// Next decodes one instruction into in, reusing its slices.
func (d *instrDecoder) Next(in *Instruction) error {
	if d.done() {
		return fmt.Errorf("instruction stream is exhausted")
	}
	op := opcode(d.buf[d.pos])
	d.pos++
	in.Op = op
	in.Blocks, in.Windows, in.PatchLen, in.SrcOff, in.Len = in.Blocks[:0], in.Windows[:0], 0, 0, 0

	switch op {
	case opCopy:
		// The length is needed to advance the source cursor, but it is
		// encoded after the delta, so read the delta raw and validate
		// once the length is known.
		delta, err := d.i()
		if err != nil {
			return err
		}
		length, err := d.u()
		if err != nil {
			return err
		}
		if length == 0 || length > uint64(d.sourceSize) {
			return fmt.Errorf("copy of %d bytes from a %d-byte source", length, d.sourceSize)
		}
		off := d.srcCur + delta
		if off < 0 || off+int64(length) > d.sourceSize {
			return fmt.Errorf("copy [%d,+%d) is outside the %d-byte source", off, length, d.sourceSize)
		}
		d.srcCur = off + int64(length)
		in.SrcOff, in.Len = off, int64(length)

	case opLiteral:
		length, err := d.u()
		if err != nil {
			return err
		}
		if length == 0 || length > uint64(d.maxRunUSize)*2 {
			return fmt.Errorf("literal of %d bytes exceeds twice the run cap", length)
		}
		in.Len = int64(length)

	case opPatchRun:
		n, err := d.u()
		if err != nil {
			return err
		}
		// Bound the count before allocating: each block costs at least two
		// varint bytes on the wire, so the buffer that remains is a tight
		// limit, and it cannot be inflated by a large declared count.
		if n == 0 || n > uint64(len(d.buf)-d.pos)/2 {
			return fmt.Errorf("patch run declares %d blocks with %d bytes of stream left",
				n, len(d.buf)-d.pos)
		}
		var uTotal int64
		for i := uint64(0); i < n; i++ {
			enc, err := d.u()
			if err != nil {
				return err
			}
			uSize, err := decodeUSize(enc, d.blockSize)
			if err != nil {
				return fmt.Errorf("patch run block %d: %w", i, err)
			}
			cSize, err := d.u()
			if err != nil {
				return err
			}
			if cSize == 0 || cSize > uint64(uSize) {
				return fmt.Errorf("patch run block %d: compressed size %d is outside (0,%d]", i, cSize, uSize)
			}
			uTotal += int64(uSize)
			if uTotal > d.maxRunUSize {
				return fmt.Errorf("patch run reconstructs %d bytes, over the %d cap", uTotal, d.maxRunUSize)
			}
			in.Blocks = append(in.Blocks, PlanBlock{USize: uSize, CSize: int(cSize)})
		}
		nw, err := d.u()
		if err != nil {
			return err
		}
		if nw > 1024 {
			return fmt.Errorf("patch run declares %d source windows", nw)
		}
		// Windows are decompressed into memory before the patch runs, so their
		// total plaintext is bounded here rather than left to the source size.
		// Twice the run cap allows a window comfortably larger than the run it
		// rebuilds while keeping the applier's peak allocation predictable.
		var winUTotal int64
		for i := uint64(0); i < nw; i++ {
			delta, err := d.i()
			if err != nil {
				return err
			}
			length, err := d.u()
			if err != nil {
				return err
			}
			excess, err := d.u()
			if err != nil {
				return err
			}
			off := d.srcCur + delta
			if length == 0 || off < 0 || off+int64(length) > d.sourceSize {
				return fmt.Errorf("patch run window %d: [%d,+%d) is outside the %d-byte source",
					i, off, length, d.sourceSize)
			}
			uLen := int64(length) + int64(excess)
			winUTotal += uLen
			if winUTotal > 2*d.maxRunUSize {
				return fmt.Errorf("patch run windows decompress to %d bytes, over twice the %d cap",
					winUTotal, d.maxRunUSize)
			}
			d.srcCur = off + int64(length)
			in.Windows = append(in.Windows, SrcWindow{Off: off, Len: int(length), ULen: int(uLen)})
		}
		patchLen, err := d.u()
		if err != nil {
			return err
		}
		if patchLen > uint64(d.maxRunUSize)*2 {
			return fmt.Errorf("patch run patch is %d bytes, over twice the run cap", patchLen)
		}
		in.PatchLen = int(patchLen)

	default:
		return fmt.Errorf("unknown opcode %d at offset %d", byte(op), d.pos-1)
	}
	return nil
}

// --- metadata framing section ---

// encodeMDFrame writes the target metadata region's framing: the sha256 of the
// blob the blocks compress from, then (uSizeEnc, cSize) per block, with the same
// two invariants as data blocks but a fixed 8192 nominal block size.
//
// The digest is here rather than left to the per-block size checks because it is
// the only thing that can vet the patched blob *before* the data region is
// written. Sizes are coarse: a corrupt blob can easily recompress to the same
// lengths, and would then survive to the final image digest -- correct, but only
// after the whole image has been assembled for nothing.
func encodeMDFrame(blocks []MetaBlock, blob []byte) ([]byte, error) {
	sum := sha256.Sum256(blob)
	buf := append([]byte(nil), sum[:]...)
	for i, b := range blocks {
		enc, err := encodeUSize(b.USize, squashfsMetadataSize)
		if err != nil {
			return nil, fmt.Errorf("metadata block %d: %w", i, err)
		}
		if b.CSize <= 0 || b.CSize > b.USize {
			return nil, fmt.Errorf("metadata block %d: compressed size %d is outside (0,%d]", i, b.CSize, b.USize)
		}
		buf = binary.AppendUvarint(buf, enc)
		buf = binary.AppendUvarint(buf, uint64(b.CSize))
	}
	return buf, nil
}

// decodeMDFrame reads the framing back, returning the blocks and the digest the
// patched metadata blob must have. Offsets are not encoded: the blocks are
// contiguous from the region start, each occupying 2+cSize bytes.
func decodeMDFrame(buf []byte, start int64) ([]MetaBlock, [32]byte, error) {
	var digest [32]byte
	if len(buf) < sha256.Size {
		return nil, digest, fmt.Errorf("metadata framing is %d bytes, too short to hold a digest", len(buf))
	}
	copy(digest[:], buf)
	buf = buf[sha256.Size:]

	var out []MetaBlock
	off := start
	for pos := 0; pos < len(buf); {
		enc, n := binary.Uvarint(buf[pos:])
		if n <= 0 {
			return nil, digest, fmt.Errorf("truncated metadata framing at offset %d", pos)
		}
		pos += n
		uSize, err := decodeUSize(enc, squashfsMetadataSize)
		if err != nil {
			return nil, digest, fmt.Errorf("metadata block %d: %w", len(out), err)
		}
		cSize, n := binary.Uvarint(buf[pos:])
		if n <= 0 {
			return nil, digest, fmt.Errorf("truncated metadata framing at offset %d", pos)
		}
		pos += n
		if cSize == 0 || cSize > uint64(uSize) {
			return nil, digest, fmt.Errorf("metadata block %d: compressed size %d is outside (0,%d]", len(out), cSize, uSize)
		}
		out = append(out, MetaBlock{
			Offset: off,
			CSize:  int(cSize),
			USize:  uSize,
			Raw:    int(cSize) == uSize,
		})
		off += 2 + int64(cSize)
	}
	if len(out) == 0 {
		return nil, digest, fmt.Errorf("metadata framing is empty")
	}
	return out, digest, nil
}
