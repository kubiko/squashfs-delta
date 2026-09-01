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
	"hash/crc32"
	"io"
	"os"
)

// This file defines the snap-2-1-blocks delta container.
//
// The format describes the target image byte for byte, so applying it never
// re-derives any mksquashfs decision -- no duplicate detection, no block
// ordering, no compressed-versus-raw choice. Unchanged blocks are copied from
// the source still compressed, which is where the CPU saving comes from: the
// pseudo-file formats decompress and recompress 100% of the image even when
// almost nothing changed.
//
//	[header 128 B][section table n x 16 B][section payloads, in table order]
//
// The applier reads the delta strictly forward, the source at random offsets,
// and the target append-only. Metadata sections come before the instructions so
// that ~200 KB of high-signal work is validated before a single data byte is
// touched.

const (
	// blockPlanMagic is "sqbp", distinct from the pseudo-file formats' "sqpf"
	// so ApplyDelta's magic switch simply gains a case.
	blockPlanMagic = uint32(0x70627173)

	blockPlanFormatVersion = uint8(0x01)
	blockPlanHeaderSize    = 128
	blockPlanEntrySize     = 16

	// snapDeltaFormatBlocks is the store name of this format.
	snapDeltaFormatBlocks = "snap-2-1-blocks"
)

// Section ids. Order in the table is the order on disk, and the applier relies
// on the metadata sections preceding SEC_INSTR.
const (
	secSB      uint16 = 1 // target superblock, 96 bytes verbatim
	secCanary  uint16 = 2 // compressor self-check, no input bytes
	secMDFrame uint16 = 3 // target metadata framing: (uSizeEnc, cSize) per block
	secMDTail  uint16 = 4 // [export_table_start, bytes_used) verbatim
	secMDPatch uint16 = 5 // patch: source metadata blob -> target metadata blob
	secInstr   uint16 = 6 // instruction stream
	secPay     uint16 = 7 // patch blobs and literal bytes, in instruction order
)

// Section codecs.
const (
	codecNone uint16 = 0
	codecXZ   uint16 = 1
)

func sectionName(id uint16) string {
	switch id {
	case secSB:
		return "SEC_SB"
	case secCanary:
		return "SEC_CANARY"
	case secMDFrame:
		return "SEC_MDFRAME"
	case secMDTail:
		return "SEC_MDTAIL"
	case secMDPatch:
		return "SEC_MDPATCH"
	case secInstr:
		return "SEC_INSTR"
	case secPay:
		return "SEC_PAY"
	}
	return fmt.Sprintf("SEC_%d", id)
}

// blockPlanHeader is the fixed 128-byte header:
//
//	 0  4  magic "sqbp"            24  8  sourceSize
//	 4  1  formatVersion           32  8  targetSize
//	 5  1  toolsVersion            40  8  targetBytesUsed
//	 6  2  patchTool               48  8  targetInodeTableStart
//	 8  4  blockSize               56 32  sourceSHA256
//	12  4  maxRunUSize             88 32  targetSHA256
//	16  4  instrCount             120  8  reserved, zero
//	20  2  sectionCount
//	22  2  flags, zero
type blockPlanHeader struct {
	FormatVersion uint8
	ToolsVersion  uint8
	PatchTool     uint16
	BlockSize     uint32
	// MaxRunUSize caps how much plaintext one OP_PATCHRUN may reconstruct,
	// which is what bounds the applier's peak memory.
	MaxRunUSize  uint32
	InstrCount   uint32
	SectionCount uint16
	Flags        uint16

	SourceSize            uint64
	TargetSize            uint64
	TargetBytesUsed       uint64
	TargetInodeTableStart uint64

	SourceSHA256 [32]byte
	TargetSHA256 [32]byte
}

func (h *blockPlanHeader) marshal() []byte {
	b := make([]byte, blockPlanHeaderSize)
	le := binary.LittleEndian
	le.PutUint32(b[0:], blockPlanMagic)
	b[4] = h.FormatVersion
	b[5] = h.ToolsVersion
	le.PutUint16(b[6:], h.PatchTool)
	le.PutUint32(b[8:], h.BlockSize)
	le.PutUint32(b[12:], h.MaxRunUSize)
	le.PutUint32(b[16:], h.InstrCount)
	le.PutUint16(b[20:], h.SectionCount)
	le.PutUint16(b[22:], h.Flags)
	le.PutUint64(b[24:], h.SourceSize)
	le.PutUint64(b[32:], h.TargetSize)
	le.PutUint64(b[40:], h.TargetBytesUsed)
	le.PutUint64(b[48:], h.TargetInodeTableStart)
	copy(b[56:], h.SourceSHA256[:])
	copy(b[88:], h.TargetSHA256[:])
	return b
}

func parseBlockPlanHeader(b []byte) (*blockPlanHeader, error) {
	if len(b) < blockPlanHeaderSize {
		return nil, fmt.Errorf("short delta header: %d bytes", len(b))
	}
	le := binary.LittleEndian
	if got := le.Uint32(b[0:]); got != blockPlanMagic {
		return nil, fmt.Errorf("not a block-plan delta (magic %#08x)", got)
	}
	h := &blockPlanHeader{
		FormatVersion: b[4],
		ToolsVersion:  b[5],
		PatchTool:     le.Uint16(b[6:]),
		BlockSize:     le.Uint32(b[8:]),
		MaxRunUSize:   le.Uint32(b[12:]),
		InstrCount:    le.Uint32(b[16:]),
		SectionCount:  le.Uint16(b[20:]),
		Flags:         le.Uint16(b[22:]),

		SourceSize:            le.Uint64(b[24:]),
		TargetSize:            le.Uint64(b[32:]),
		TargetBytesUsed:       le.Uint64(b[40:]),
		TargetInodeTableStart: le.Uint64(b[48:]),
	}
	copy(h.SourceSHA256[:], b[56:88])
	copy(h.TargetSHA256[:], b[88:120])

	if h.FormatVersion != blockPlanFormatVersion {
		return nil, fmt.Errorf("unsupported block-plan format version %d", h.FormatVersion)
	}
	if h.ToolsVersion != deltaFormatToolsVersion {
		return nil, fmt.Errorf("delta was made with tools version %d, this build is %d",
			h.ToolsVersion, deltaFormatToolsVersion)
	}
	if h.Flags != 0 {
		return nil, fmt.Errorf("delta header sets unknown flags %#04x", h.Flags)
	}
	if h.SectionCount == 0 || h.SectionCount > 64 {
		return nil, fmt.Errorf("delta declares %d sections", h.SectionCount)
	}
	if _, err := dictProp(int(h.BlockSize)); err != nil {
		return nil, fmt.Errorf("delta block size %d: %w", h.BlockSize, err)
	}
	if h.MaxRunUSize < h.BlockSize {
		return nil, fmt.Errorf("delta run cap %d is below one block (%d)", h.MaxRunUSize, h.BlockSize)
	}
	if !(96 <= h.TargetInodeTableStart && h.TargetInodeTableStart <= h.TargetBytesUsed &&
		h.TargetBytesUsed <= h.TargetSize) {
		return nil, fmt.Errorf("delta target geometry is not ordered: inode_table=%d bytes_used=%d size=%d",
			h.TargetInodeTableStart, h.TargetBytesUsed, h.TargetSize)
	}
	return h, nil
}

// sectionEntry is one 16-byte section table row. CRC32 covers the stored bytes,
// so a truncated or corrupt section is caught before it is decompressed.
type sectionEntry struct {
	ID        uint16
	Codec     uint16
	StoredLen uint32
	RawLen    uint32
	CRC       uint32
}

func (e sectionEntry) marshal() []byte {
	b := make([]byte, blockPlanEntrySize)
	le := binary.LittleEndian
	le.PutUint16(b[0:], e.ID)
	le.PutUint16(b[2:], e.Codec)
	le.PutUint32(b[4:], e.StoredLen)
	le.PutUint32(b[8:], e.RawLen)
	le.PutUint32(b[12:], e.CRC)
	return b
}

func parseSectionEntry(b []byte) sectionEntry {
	le := binary.LittleEndian
	return sectionEntry{
		ID:        le.Uint16(b[0:]),
		Codec:     le.Uint16(b[2:]),
		StoredLen: le.Uint32(b[4:]),
		RawLen:    le.Uint32(b[8:]),
		CRC:       le.Uint32(b[12:]),
	}
}

// --- writing ---

// blockPlanWriter accumulates sections and emits the delta. Sections are added
// in the order they should appear on disk.
//
// Every section but SEC_PAY is small enough to hold in memory. SEC_PAY is the
// one that scales with how much of the image changed, so it can be backed by a
// file (a memfd in practice) instead.
type blockPlanWriter struct {
	Header   blockPlanHeader
	sections []pendingSection
}

type pendingSection struct {
	entry sectionEntry
	// exactly one of bytes and file is set
	bytes []byte
	file  *os.File
}

// addSection stores raw uncompressed, under the given id.
func (w *blockPlanWriter) addSection(id uint16, raw []byte) {
	w.sections = append(w.sections, pendingSection{
		entry: sectionEntry{
			ID:        id,
			Codec:     codecNone,
			StoredLen: uint32(len(raw)),
			RawLen:    uint32(len(raw)),
			CRC:       crc32.ChecksumIEEE(raw),
		},
		bytes: raw,
	})
}

// addSectionCompressed compresses raw with the image's own compressor before
// storing it. Worth it only for the instruction stream; the patch and payload
// sections are already compressed.
//
// Using the image's compressor rather than always xz is what keeps an apply's
// tool requirements to exactly one compressor: the section is decoded by the
// same code the blocks are.
func (w *blockPlanWriter) addSectionCompressed(ctx context.Context, id uint16, raw []byte, comp Compressor) error {
	stored, err := comp.CompressBlob(ctx, raw)
	if err != nil {
		return err
	}
	if len(stored) >= len(raw) {
		w.addSection(id, raw)
		return nil
	}
	w.sections = append(w.sections, pendingSection{
		entry: sectionEntry{
			ID:        id,
			Codec:     comp.SectionCodec(),
			StoredLen: uint32(len(stored)),
			RawLen:    uint32(len(raw)),
			CRC:       crc32.ChecksumIEEE(stored),
		},
		bytes: stored,
	})
	return nil
}

// addSectionFile stores a file-backed section. The caller has already computed
// the CRC32 of its contents, which is cheap to do while writing it.
func (w *blockPlanWriter) addSectionFile(id uint16, f *os.File, storedLen int, crc uint32) {
	w.sections = append(w.sections, pendingSection{
		entry: sectionEntry{
			ID:        id,
			Codec:     codecNone,
			StoredLen: uint32(storedLen),
			RawLen:    uint32(storedLen),
			CRC:       crc,
		},
		file: f,
	})
}

// writeTo emits header, section table and payloads.
func (w *blockPlanWriter) writeTo(out io.Writer) error {
	w.Header.FormatVersion = blockPlanFormatVersion
	w.Header.ToolsVersion = deltaFormatToolsVersion
	w.Header.SectionCount = uint16(len(w.sections))
	if _, err := out.Write(w.Header.marshal()); err != nil {
		return err
	}
	for _, s := range w.sections {
		if _, err := out.Write(s.entry.marshal()); err != nil {
			return err
		}
	}
	for _, s := range w.sections {
		if s.file != nil {
			if _, err := s.file.Seek(0, io.SeekStart); err != nil {
				return err
			}
			n, err := io.Copy(out, io.LimitReader(s.file, int64(s.entry.StoredLen)))
			if err != nil {
				return err
			}
			if n != int64(s.entry.StoredLen) {
				return fmt.Errorf("%s: wrote %d of %d bytes", sectionName(s.entry.ID), n, s.entry.StoredLen)
			}
			continue
		}
		if _, err := out.Write(s.bytes); err != nil {
			return err
		}
	}
	return nil
}

// --- reading ---

// blockPlanReader walks a delta forward. Sections up to and including SEC_INSTR
// are materialized eagerly, because they are small and must all be validated
// before any data work starts. SEC_PAY is left as a stream, since it is the one
// section that scales with the size of the change.
type blockPlanReader struct {
	Header   *blockPlanHeader
	entries  []sectionEntry
	sections map[uint16][]byte
	// pay is positioned at the first byte of SEC_PAY.
	pay    io.Reader
	payLen int64
	payCRC uint32
}

// openBlockPlan reads the header, table and every section except SEC_PAY, which
// must be the last section so it can stay streamed.
func openBlockPlan(r io.Reader) (*blockPlanReader, error) {
	head := make([]byte, blockPlanHeaderSize)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, fmt.Errorf("cannot read delta header: %w", err)
	}
	h, err := parseBlockPlanHeader(head)
	if err != nil {
		return nil, err
	}
	tbl := make([]byte, int(h.SectionCount)*blockPlanEntrySize)
	if _, err := io.ReadFull(r, tbl); err != nil {
		return nil, fmt.Errorf("cannot read delta section table: %w", err)
	}
	br := &blockPlanReader{Header: h, sections: make(map[uint16][]byte, h.SectionCount)}
	seen := make(map[uint16]bool, h.SectionCount)
	for i := 0; i < int(h.SectionCount); i++ {
		e := parseSectionEntry(tbl[i*blockPlanEntrySize:])
		if seen[e.ID] {
			return nil, fmt.Errorf("%s appears twice in the section table", sectionName(e.ID))
		}
		seen[e.ID] = true
		if e.ID == secPay && i != int(h.SectionCount)-1 {
			return nil, fmt.Errorf("SEC_PAY must be the last section, it is entry %d of %d", i+1, h.SectionCount)
		}
		br.entries = append(br.entries, e)
	}
	for _, id := range []uint16{secSB, secMDFrame, secInstr} {
		if !seen[id] {
			return nil, fmt.Errorf("delta is missing %s", sectionName(id))
		}
	}

	for _, e := range br.entries {
		if e.ID == secPay {
			br.pay, br.payLen, br.payCRC = r, int64(e.StoredLen), e.CRC
			if e.Codec != codecNone {
				return nil, fmt.Errorf("SEC_PAY must be stored uncompressed, codec is %d", e.Codec)
			}
			break
		}
		if e.StoredLen > 64<<20 {
			return nil, fmt.Errorf("%s is %d bytes, over the 64 MiB cap for an eager section",
				sectionName(e.ID), e.StoredLen)
		}
		stored := make([]byte, e.StoredLen)
		if _, err := io.ReadFull(r, stored); err != nil {
			return nil, fmt.Errorf("cannot read %s: %w", sectionName(e.ID), err)
		}
		if got := crc32.ChecksumIEEE(stored); got != e.CRC {
			return nil, fmt.Errorf("%s is corrupt: CRC32 %#08x, expected %#08x", sectionName(e.ID), got, e.CRC)
		}
		raw := stored
		switch e.Codec {
		case codecNone:
			if e.RawLen != e.StoredLen {
				return nil, fmt.Errorf("%s is stored uncompressed but declares raw length %d for %d stored bytes",
					sectionName(e.ID), e.RawLen, e.StoredLen)
			}
		default:
			if raw, err = decompressBlob(context.Background(), e.Codec, stored, int(e.RawLen)); err != nil {
				return nil, fmt.Errorf("cannot decompress %s: %w", sectionName(e.ID), err)
			}
		}
		br.sections[e.ID] = raw
	}
	return br, nil
}

// section returns a decoded section's raw bytes, or nil if it is absent.
func (br *blockPlanReader) section(id uint16) []byte { return br.sections[id] }

// hasSection reports whether the delta carries the section at all, which is how
// "no metadata change" is encoded (SEC_MDPATCH absent).
func (br *blockPlanReader) hasSection(id uint16) bool {
	_, ok := br.sections[id]
	return ok
}

// --- whole-blob section codecs ---

// decompressBlob undoes a section codec.
//
// It dispatches on the codec the section table records rather than on the
// image's compressor, because sections are decoded before SEC_SB is even parsed
// -- and because a codec is a property of the delta container, not of the image
// the delta describes. Each compressor's SectionCodec names the one it writes.
func decompressBlob(ctx context.Context, codec uint16, stored []byte, rawLen int) ([]byte, error) {
	switch codec {
	case codecXZ:
		return xzDecompressAll(ctx, stored, rawLen)
	}
	return nil, fmt.Errorf("unknown codec %d", codec)
}

// humanBytes formats a byte count for the messages this format reports sizes
// in. It lives here rather than beside the command line because the applier's
// own refusals quote sizes too, and those must read the same way wherever the
// applier is driven from.
func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}
