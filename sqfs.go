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
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

// This file reads enough of a squashfs 4.0 image to describe its exact byte
// layout, which is what lets a delta replay that layout instead of asking
// mksquashfs to re-derive it.
//
// The image is a superblock, then the data region, then a run of metadata
// blocks holding the inode and directory tables, then the lookup, id and
// (optionally) xattr tables, then zero padding to a 4 KiB boundary:
//
//	0                              96                inode_table_start
//	+---------------+---------------------------------+---------------------
//	| superblock    | data blocks                     | inode table
//	+---------------+---------------------------------+---------------------
//	                                  export_table_start        bytes_used
//	---------------+--------------------------+-----------------+-----------+
//	 directory tbl | lookup / id / xattr tbls |     padding     |
//	---------------+--------------------------+-----------------+-----------+
//
// Two boundary rules matter, both established by sweeping every snap available
// locally:
//
//   - The data region is exactly [96, inode_table_start), and the extents
//     derived from the inode table cover it with no gap and no overlap. That is
//     what makes verbatim block reuse safe: nothing lives in the data region
//     that the inode table does not describe.
//
//   - frag_table_start must never be used to derive a boundary. On an image
//     with no fragments it is a stale write position pointing into the middle
//     of the metadata region -- true of all 43 snaps swept. Use
//     [inode_table_start, export_table_start), which walks to an exact
//     metadata block boundary on every one.

const (
	squashfsMagic = uint32(0x73717368)

	// squashfsMetadataSize is SQUASHFS_METADATA_SIZE: the uncompressed size
	// of a full metadata block, and the dictionary size mksquashfs uses for
	// them.
	squashfsMetadataSize = 8192

	// dataUncompressedBit is SQUASHFS_COMPRESSED_BIT_BLOCK. Set in a block
	// size word when the block is stored verbatim.
	dataUncompressedBit = uint32(1) << 24
	// metaUncompressedBit is SQUASHFS_COMPRESSED_BIT, the same flag in the
	// two-byte metadata block header.
	metaUncompressedBit = uint16(1) << 15

	// squashfsNoTable and squashfsNoFragment are the "absent" sentinels for
	// a table pointer and for an inode's fragment index.
	squashfsNoTable    = uint64(0xFFFFFFFFFFFFFFFF)
	squashfsNoFragment = uint32(0xFFFFFFFF)

	// inodeTypeFile and inodeTypeExtFile are the two regular-file inode
	// types; only these reference data blocks.
	inodeTypeFile    = 2
	inodeTypeExtFile = 9

	// compressorXz is the squashfs compressor id for xz.
	compressorXz = 4

	// squashfsPadding is the alignment mksquashfs zero-pads the image to.
	squashfsPadding = 4096
)

// SquashfsHeader is the full 96-byte superblock, extending SquashfsSuperblock
// with the eight table pointers that describe the layout.
type SquashfsHeader struct {
	SquashfsSuperblock
	RootInodeRef     uint64
	BytesUsed        uint64
	IdTableStart     uint64
	XattrTableStart  uint64
	InodeTableStart  uint64
	DirTableStart    uint64
	FragTableStart   uint64
	ExportTableStart uint64
}

func parseSquashfsHeader(raw []byte) (*SquashfsHeader, error) {
	if len(raw) < 96 {
		return nil, fmt.Errorf("short superblock: %d bytes", len(raw))
	}
	le := binary.LittleEndian
	h := &SquashfsHeader{
		SquashfsSuperblock: SquashfsSuperblock{
			Magic:            le.Uint32(raw[0:]),
			InodeCount:       le.Uint32(raw[4:]),
			ModificationTime: le.Uint32(raw[8:]),
			BlockSize:        le.Uint32(raw[12:]),
			FragmentEntryCnt: le.Uint32(raw[16:]),
			CompressionId:    le.Uint16(raw[20:]),
			BlockLog:         le.Uint16(raw[22:]),
			Flags:            le.Uint16(raw[24:]),
			IdCount:          le.Uint16(raw[26:]),
			MajorVersion:     le.Uint16(raw[28:]),
			MinorVersion:     le.Uint16(raw[30:]),
		},
		RootInodeRef:     le.Uint64(raw[32:]),
		BytesUsed:        le.Uint64(raw[40:]),
		IdTableStart:     le.Uint64(raw[48:]),
		XattrTableStart:  le.Uint64(raw[56:]),
		InodeTableStart:  le.Uint64(raw[64:]),
		DirTableStart:    le.Uint64(raw[72:]),
		FragTableStart:   le.Uint64(raw[80:]),
		ExportTableStart: le.Uint64(raw[88:]),
	}
	if h.Magic != squashfsMagic {
		return nil, fmt.Errorf("not a squashfs image (magic %#08x)", h.Magic)
	}
	return h, nil
}

func (h *SquashfsHeader) String() string {
	return fmt.Sprintf("v%d.%d comp=%d bsize=%d log=%d inodes=%d frags=%d flags=%#04x bytes_used=%d\n"+
		"  inode_tbl=%d dir_tbl=%d frag_tbl=%d export_tbl=%d id_tbl=%d xattr_tbl=%d",
		h.MajorVersion, h.MinorVersion, h.CompressionId, h.BlockSize, h.BlockLog,
		h.InodeCount, h.FragmentEntryCnt, h.Flags, h.BytesUsed,
		h.InodeTableStart, h.DirTableStart, h.FragTableStart, h.ExportTableStart,
		h.IdTableStart, h.XattrTableStart)
}

// Extent is one data block as it sits in the image.
type Extent struct {
	// Offset is the byte position in the image.
	Offset int64
	// CSize is the number of bytes occupied on disk.
	CSize int
	// USize is the uncompressed size, which for the last block of a file is
	// less than the image block size.
	USize int
	// Raw reports that the block is stored uncompressed, in which case CSize
	// equals USize.
	Raw bool
}

// MetaBlock is one metadata block as it sits in the image.
type MetaBlock struct {
	// Offset is the byte position of the two-byte size header.
	Offset int64
	// CSize is the payload length, excluding the two-byte header.
	CSize int
	// USize is the uncompressed payload length, at most 8192.
	USize int
	// Raw reports that the payload is stored uncompressed.
	Raw bool
}

// FileInode is the part of a regular-file inode that describes data placement.
type FileInode struct {
	Number     uint32
	Type       uint16
	StartBlock int64
	FileSize   uint64
	Fragment   uint32
	// Sizes are the raw block size words, one per full or partial block.
	Sizes []uint32
}

// SquashfsImage is a read-only view of a squashfs image held in memory.
//
// Holding the whole image is deliberate for the generator, which needs random
// access to both source and target and runs on a build machine. The applier
// reads the source with pread instead and never materializes it.
type SquashfsImage struct {
	Path string
	Data []byte
	SB   *SquashfsHeader
}

func openSquashfsImage(path string) (*SquashfsImage, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	sb, err := parseSquashfsHeader(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &SquashfsImage{Path: path, Data: data, SB: sb}, nil
}

// Size is the image length in bytes.
func (im *SquashfsImage) Size() int64 { return int64(len(im.Data)) }

// DataRegionEnd is the first byte past the data region.
func (im *SquashfsImage) DataRegionEnd() int64 { return int64(im.SB.InodeTableStart) }

// checkSupported rejects images whose layout this delta format does not
// reproduce. Refusing is cheap: the caller falls back to a full download, so a
// wrong answer here costs bandwidth, while a wrong image costs correctness.
func (im *SquashfsImage) checkSupported() error {
	if err := im.SB.checkSupportedGeometry(im.Size()); err != nil {
		return err
	}
	for _, b := range im.Data[im.SB.BytesUsed:] {
		if b != 0 {
			return fmt.Errorf("trailing padding is not all zero")
		}
	}
	return nil
}

// checkSupportedGeometry holds every rule that the superblock alone decides, so
// the applier can vet the target's superblock -- which arrives in SEC_SB, long
// before the image it describes exists -- against exactly the rules the
// generator applied to the real thing.
func (sb *SquashfsHeader) checkSupportedGeometry(imageSize int64) error {
	switch {
	case sb.MajorVersion != 4 || sb.MinorVersion != 0:
		return fmt.Errorf("unsupported squashfs version %d.%d", sb.MajorVersion, sb.MinorVersion)
	case sb.CompressionId != compressorXz:
		return fmt.Errorf("unsupported compressor id %d, only xz (%d) is implemented", sb.CompressionId, compressorXz)
	case sb.Flags&flagCompressorOptions != 0:
		return fmt.Errorf("image carries COMPRESSOR_OPTIONS, whose filter chain is not reproduced")
	case sb.FragmentEntryCnt != 0:
		return fmt.Errorf("image has %d fragments, which are not described by the extent walk", sb.FragmentEntryCnt)
	}
	// FragTableStart is deliberately not checked, and never read: with no
	// fragments it holds a stale write position, which on all 43 snaps swept
	// points into the middle of the metadata region.
	// Gate on the table pointer, never on the NO_XATTRS flag: core26 has
	// that flag clear while carrying no xattr table at all.
	if sb.XattrTableStart != squashfsNoTable {
		return fmt.Errorf("image has an xattr table at %d, which is not reproduced", sb.XattrTableStart)
	}
	if sb.ExportTableStart == squashfsNoTable {
		return fmt.Errorf("image has no export table, so inodes cannot be enumerated without walking directories")
	}
	if sb.BlockSize != 1<<sb.BlockLog {
		return fmt.Errorf("block size %d disagrees with block log %d", sb.BlockSize, sb.BlockLog)
	}
	if _, err := dictProp(int(sb.BlockSize)); err != nil {
		return fmt.Errorf("block size %d cannot be an LZMA2 dictionary size: %w", sb.BlockSize, err)
	}
	if sb.BytesUsed > uint64(imageSize) {
		return fmt.Errorf("superblock claims %d bytes used but the file is %d", sb.BytesUsed, imageSize)
	}
	if !(96 <= sb.InodeTableStart && sb.InodeTableStart <= sb.ExportTableStart && sb.ExportTableStart <= sb.BytesUsed) {
		return fmt.Errorf("table pointers are not ordered: inode=%d export=%d bytes_used=%d",
			sb.InodeTableStart, sb.ExportTableStart, sb.BytesUsed)
	}
	// The image is zero-padded up to the next 4 KiB boundary and no further, so
	// its length is fully determined by bytes_used. The applier relies on this
	// to know when it is finished.
	if want := paddedImageSize(int64(sb.BytesUsed)); want != imageSize {
		return fmt.Errorf("image is %d bytes, but %d bytes used pads to %d", imageSize, sb.BytesUsed, want)
	}
	return nil
}

// paddedImageSize rounds a bytes_used up to the padding mksquashfs applies.
func paddedImageSize(bytesUsed int64) int64 {
	return (bytesUsed + squashfsPadding - 1) / squashfsPadding * squashfsPadding
}

// --- metadata blocks ---

// readMetaBlock decodes the metadata block whose two-byte header sits at off,
// returning its uncompressed payload and its total on-disk length.
func (im *SquashfsImage) readMetaBlock(ctx context.Context, off int64) (content []byte, diskLen int, raw bool, err error) {
	if off < 0 || off+2 > int64(len(im.Data)) {
		return nil, 0, false, fmt.Errorf("metadata block header at %d is out of range", off)
	}
	hdr := binary.LittleEndian.Uint16(im.Data[off:])
	raw = hdr&metaUncompressedBit != 0
	cSize := int(hdr & (metaUncompressedBit - 1))
	if off+2+int64(cSize) > int64(len(im.Data)) {
		return nil, 0, false, fmt.Errorf("metadata block at %d claims %d bytes, past end of image", off, cSize)
	}
	payload := im.Data[off+2 : off+2+int64(cSize)]
	if raw {
		return payload, 2 + cSize, true, nil
	}
	out, err := xzDecompressAll(ctx, payload, -1)
	if err != nil {
		return nil, 0, false, fmt.Errorf("metadata block at %d: %w", off, err)
	}
	if len(out) > squashfsMetadataSize {
		return nil, 0, false, fmt.Errorf("metadata block at %d expands to %d bytes, over the %d limit",
			off, len(out), squashfsMetadataSize)
	}
	return out, 2 + cSize, false, nil
}

// MetaRegion is a run of metadata blocks decompressed into one blob, plus the
// index needed to resolve inode references into it.
type MetaRegion struct {
	// Start is the image offset the region begins at.
	Start int64
	// Blob is every block's payload concatenated.
	Blob []byte
	// Blocks describes each block's on-disk framing, in order.
	Blocks []MetaBlock
	// index maps a block's start offset relative to Start to its position in
	// Blob, which is exactly the mapping an inode reference needs.
	index map[uint64]int
}

// walkMetaRegion decompresses the metadata blocks in [start, end). It requires
// the range to end exactly on a block boundary, which is the check that proves
// the region really is a pure metadata run.
func (im *SquashfsImage) walkMetaRegion(ctx context.Context, start, end int64) (*MetaRegion, error) {
	if start > end {
		return nil, fmt.Errorf("metadata region [%d,%d) is inverted", start, end)
	}
	// One xz process for the whole region: each block payload is a complete
	// stream and xz decodes concatenated streams in a single pass.
	type slot struct {
		blk    MetaBlock
		stream []byte
	}
	var slots []slot
	var streams []byte
	for off := start; off < end; {
		if off+2 > end {
			return nil, fmt.Errorf("metadata block header at %d crosses the region end %d", off, end)
		}
		hdr := binary.LittleEndian.Uint16(im.Data[off:])
		raw := hdr&metaUncompressedBit != 0
		cSize := int(hdr & (metaUncompressedBit - 1))
		if cSize == 0 || off+2+int64(cSize) > end {
			return nil, fmt.Errorf("metadata block at %d claims %d bytes, past the region end %d", off, cSize, end)
		}
		payload := im.Data[off+2 : off+2+int64(cSize)]
		s := slot{blk: MetaBlock{Offset: off, CSize: cSize, Raw: raw}}
		if raw {
			s.blk.USize = cSize
		} else {
			s.stream = payload
			streams = append(streams, payload...)
		}
		slots = append(slots, s)
		off += 2 + int64(cSize)
		if off == end {
			break
		}
		if off > end {
			return nil, fmt.Errorf("metadata walk overran the region end: %d != %d", off, end)
		}
	}

	var plain []byte
	if len(streams) > 0 {
		var err error
		plain, err = xzDecompressAll(ctx, streams, -1)
		if err != nil {
			return nil, fmt.Errorf("metadata region [%d,%d): %w", start, end, err)
		}
	}

	reg := &MetaRegion{Start: start, index: make(map[uint64]int, len(slots))}
	pos := 0
	for i := range slots {
		s := &slots[i]
		reg.index[uint64(s.blk.Offset-start)] = len(reg.Blob)
		if s.blk.Raw {
			reg.Blob = append(reg.Blob, im.Data[s.blk.Offset+2:s.blk.Offset+2+int64(s.blk.CSize)]...)
			reg.Blocks = append(reg.Blocks, s.blk)
			continue
		}
		// Blocks are 8192 bytes uncompressed except the last, so the split
		// point is unknown until the payload is decoded -- but xz's own
		// stream boundaries give it away only in aggregate. Decode each
		// block's declared length from its stream index instead.
		n, err := xzStreamUncompressedSize(s.stream)
		if err != nil {
			return nil, fmt.Errorf("metadata block at %d: %w", s.blk.Offset, err)
		}
		if pos+n > len(plain) {
			return nil, fmt.Errorf("metadata block at %d claims %d bytes but only %d remain",
				s.blk.Offset, n, len(plain)-pos)
		}
		if n > squashfsMetadataSize {
			return nil, fmt.Errorf("metadata block at %d expands to %d bytes, over the %d limit",
				s.blk.Offset, n, squashfsMetadataSize)
		}
		s.blk.USize = n
		reg.Blob = append(reg.Blob, plain[pos:pos+n]...)
		pos += n
		reg.Blocks = append(reg.Blocks, s.blk)
	}
	if pos != len(plain) {
		return nil, fmt.Errorf("metadata region [%d,%d): consumed %d of %d decompressed bytes", start, end, pos, len(plain))
	}
	return reg, nil
}

// xzStreamUncompressedSize reads the uncompressed size out of the first block
// header of an .xz stream, which the buffer encoder always records.
func xzStreamUncompressedSize(stream []byte) (int, error) {
	if len(stream) < 14 {
		return 0, fmt.Errorf("stream too short to hold a block header")
	}
	hdr := stream[12:]
	flags := hdr[1]
	pos := 2
	if flags&blockHeaderFlagCompressedSize != 0 {
		var err error
		if _, pos, err = readVLI(hdr, pos); err != nil {
			return 0, err
		}
	}
	if flags&blockHeaderFlagUncompressedSize == 0 {
		return 0, fmt.Errorf("block header omits the uncompressed size (flags %#02x)", flags)
	}
	n, _, err := readVLI(hdr, pos)
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// --- inodes ---

// InodeRegion decompresses [inode_table_start, dir_table_start), the run of
// metadata blocks holding the inode table.
func (im *SquashfsImage) InodeRegion(ctx context.Context) (*MetaRegion, error) {
	return im.walkMetaRegion(ctx, int64(im.SB.InodeTableStart), int64(im.SB.DirTableStart))
}

// MetaRegionAll decompresses the whole metadata region,
// [inode_table_start, export_table_start), which is what the delta patches as
// one blob.
func (im *SquashfsImage) MetaRegionAll(ctx context.Context) (*MetaRegion, error) {
	return im.walkMetaRegion(ctx, int64(im.SB.InodeTableStart), int64(im.SB.ExportTableStart))
}

// exportRefs reads the lookup table: inode references for inodes 1..inode_count,
// which avoids walking the directory tree to find every inode.
func (im *SquashfsImage) exportRefs(ctx context.Context) ([]uint64, error) {
	sb := im.SB
	nPtr := (int(sb.InodeCount)*8 + squashfsMetadataSize - 1) / squashfsMetadataSize
	base := int64(sb.ExportTableStart)
	if base+int64(nPtr)*8 > int64(len(im.Data)) {
		return nil, fmt.Errorf("lookup table at %d needs %d pointers, past end of image", base, nPtr)
	}
	refs := make([]uint64, 0, sb.InodeCount)
	for i := 0; i < nPtr; i++ {
		ptr := binary.LittleEndian.Uint64(im.Data[base+int64(i)*8:])
		content, _, _, err := im.readMetaBlock(ctx, int64(ptr))
		if err != nil {
			return nil, fmt.Errorf("lookup table block %d: %w", i, err)
		}
		for j := 0; j+8 <= len(content); j += 8 {
			refs = append(refs, binary.LittleEndian.Uint64(content[j:]))
		}
	}
	if len(refs) < int(sb.InodeCount) {
		return nil, fmt.Errorf("lookup table yielded %d refs, expected %d", len(refs), sb.InodeCount)
	}
	return refs[:sb.InodeCount], nil
}

// parseFileInode decodes a regular-file inode at blob[off:], or returns nil for
// any other inode type. Only these two types reference data blocks.
func (im *SquashfsImage) parseFileInode(blob []byte, off int) (*FileInode, error) {
	if off < 0 || off+16 > len(blob) {
		return nil, fmt.Errorf("inode at %d is out of range", off)
	}
	le := binary.LittleEndian
	fi := &FileInode{
		Type:   le.Uint16(blob[off:]),
		Number: le.Uint32(blob[off+12:]),
	}
	var sizesOff int
	switch fi.Type {
	case inodeTypeFile:
		if off+32 > len(blob) {
			return nil, fmt.Errorf("basic file inode at %d is truncated", off)
		}
		fi.StartBlock = int64(le.Uint32(blob[off+16:]))
		fi.Fragment = le.Uint32(blob[off+20:])
		fi.FileSize = uint64(le.Uint32(blob[off+28:]))
		sizesOff = off + 32
	case inodeTypeExtFile:
		if off+56 > len(blob) {
			return nil, fmt.Errorf("extended file inode at %d is truncated", off)
		}
		fi.StartBlock = int64(le.Uint64(blob[off+16:]))
		fi.FileSize = le.Uint64(blob[off+24:])
		fi.Fragment = le.Uint32(blob[off+44:])
		sizesOff = off + 56
	default:
		return nil, nil
	}
	bsz := uint64(im.SB.BlockSize)
	var nBlocks int
	if fi.Fragment == squashfsNoFragment {
		nBlocks = int((fi.FileSize + bsz - 1) / bsz)
	} else {
		nBlocks = int(fi.FileSize / bsz)
	}
	if sizesOff+nBlocks*4 > len(blob) {
		return nil, fmt.Errorf("inode %d needs %d block size words, past end of inode table", fi.Number, nBlocks)
	}
	fi.Sizes = make([]uint32, nBlocks)
	for i := range fi.Sizes {
		fi.Sizes[i] = le.Uint32(blob[sizesOff+i*4:])
	}
	return fi, nil
}

// FileInodes returns every regular-file inode, in lookup-table order.
func (im *SquashfsImage) FileInodes(ctx context.Context) ([]*FileInode, error) {
	reg, err := im.InodeRegion(ctx)
	if err != nil {
		return nil, err
	}
	refs, err := im.exportRefs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*FileInode, 0, len(refs))
	for _, ref := range refs {
		blk := (ref >> 16) & 0xFFFFFFFFFFFF
		inOff := int(ref & 0xFFFF)
		base, ok := reg.index[blk]
		if !ok {
			return nil, fmt.Errorf("inode reference %#x names metadata block %d, which is not in the inode table", ref, blk)
		}
		fi, err := im.parseFileInode(reg.Blob, base+inOff)
		if err != nil {
			return nil, err
		}
		if fi != nil {
			out = append(out, fi)
		}
	}
	return out, nil
}

// Extents returns every distinct data block in the image, sorted by offset.
//
// Distinct matters: mksquashfs deduplicates identical files, so several inodes
// can point at the same blocks. Keying on (offset, size) reproduces that
// sharing for free -- the delta emits the bytes once and every inode that
// referenced them keeps working, because the inode table is carried verbatim.
func (im *SquashfsImage) Extents(ctx context.Context) ([]Extent, error) {
	inodes, err := im.FileInodes(ctx)
	if err != nil {
		return nil, err
	}
	bsz := uint64(im.SB.BlockSize)
	type key struct {
		off   int64
		cSize int
	}
	seen := make(map[key]Extent, 1024)
	for _, fi := range inodes {
		off := fi.StartBlock
		remaining := fi.FileSize
		for _, w := range fi.Sizes {
			uSize := bsz
			if remaining < bsz {
				uSize = remaining
			}
			remaining -= uSize
			if w == 0 {
				// A sparse hole occupies no bytes on disk. It
				// survives through the inode's size word, which
				// the metadata patch carries, so the assembler
				// emits nothing for it.
				continue
			}
			cSize := int(w & ^dataUncompressedBit)
			e := Extent{
				Offset: off,
				CSize:  cSize,
				USize:  int(uSize),
				Raw:    w&dataUncompressedBit != 0,
			}
			if e.Raw && e.CSize != e.USize {
				return nil, fmt.Errorf("inode %d: raw block at %d occupies %d bytes but is %d uncompressed",
					fi.Number, off, e.CSize, e.USize)
			}
			if prev, ok := seen[key{off, cSize}]; ok && prev.USize != e.USize {
				return nil, fmt.Errorf("block at %d is shared with disagreeing uncompressed sizes %d and %d",
					off, prev.USize, e.USize)
			}
			seen[key{off, cSize}] = e
			off += int64(cSize)
		}
	}
	out := make([]Extent, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Offset != out[j].Offset {
			return out[i].Offset < out[j].Offset
		}
		return out[i].CSize < out[j].CSize
	})
	return out, nil
}

// CoverageGap is a byte range of the data region that no extent accounts for,
// or that two extents both claim.
type CoverageGap struct {
	From, To int64
}

// CheckCoverage verifies that the extents tile the data region exactly. Any gap
// means bytes exist that the inode table does not describe, and any overlap
// means an extent was misplaced; either makes verbatim reuse unsafe, so both
// are refusals rather than warnings.
func (im *SquashfsImage) CheckCoverage(ctx context.Context) (ext []Extent, gaps, overlaps []CoverageGap, err error) {
	ext, err = im.Extents(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	pos := int64(96)
	end := im.DataRegionEnd()
	for _, e := range ext {
		switch {
		case e.Offset > pos:
			gaps = append(gaps, CoverageGap{pos, e.Offset})
		case e.Offset < pos:
			overlaps = append(overlaps, CoverageGap{e.Offset, pos})
		}
		if e.Offset+int64(e.CSize) > pos {
			pos = e.Offset + int64(e.CSize)
		}
	}
	if pos != end {
		gaps = append(gaps, CoverageGap{pos, end})
	}
	return ext, gaps, overlaps, nil
}

// DecompressExtent returns an extent's uncompressed contents.
func (im *SquashfsImage) DecompressExtent(ctx context.Context, e Extent) ([]byte, error) {
	if e.Offset < 0 || e.Offset+int64(e.CSize) > int64(len(im.Data)) {
		return nil, fmt.Errorf("extent at %d (%d bytes) is out of range", e.Offset, e.CSize)
	}
	blob := im.Data[e.Offset : e.Offset+int64(e.CSize)]
	if e.Raw {
		return blob, nil
	}
	return xzDecompressAll(ctx, blob, e.USize)
}

// DecompressExtents returns the concatenated contents of a run of extents,
// costing one xz process for the whole run rather than one per block.
func (im *SquashfsImage) DecompressExtents(ctx context.Context, ext []Extent) ([]byte, error) {
	var streams []byte
	uTotal, cTotal := 0, 0
	allCompressed := true
	for _, e := range ext {
		uTotal += e.USize
		cTotal += e.CSize
		if e.Raw {
			allCompressed = false
		}
	}
	if allCompressed {
		streams = make([]byte, 0, cTotal)
		for _, e := range ext {
			if e.Offset+int64(e.CSize) > int64(len(im.Data)) {
				return nil, fmt.Errorf("extent at %d (%d bytes) is out of range", e.Offset, e.CSize)
			}
			streams = append(streams, im.Data[e.Offset:e.Offset+int64(e.CSize)]...)
		}
		return xzDecompressAll(ctx, streams, uTotal)
	}
	// Mixed run: decode the compressed prefix runs in batches and splice the
	// raw blocks in place.
	out := make([]byte, 0, uTotal)
	for i := 0; i < len(ext); {
		if ext[i].Raw {
			out = append(out, im.Data[ext[i].Offset:ext[i].Offset+int64(ext[i].CSize)]...)
			i++
			continue
		}
		j, want := i, 0
		var batch []byte
		for ; j < len(ext) && !ext[j].Raw; j++ {
			batch = append(batch, im.Data[ext[j].Offset:ext[j].Offset+int64(ext[j].CSize)]...)
			want += ext[j].USize
		}
		plain, err := xzDecompressAll(ctx, batch, want)
		if err != nil {
			return nil, err
		}
		out = append(out, plain...)
		i = j
	}
	return out, nil
}
