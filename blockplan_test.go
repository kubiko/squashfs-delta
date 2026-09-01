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
	"math/rand"
	"reflect"
	"testing"
)

const testBlockSize = 131072

// testSourceSize is deliberately large enough that random source offsets in the
// fuzz test are usually in range, so failures are about encoding rather than
// about bounds rejection.
const testSourceSize = 1 << 30

func decodeAll(t *testing.T, e *instrEncoder, sourceSize, maxRun int64) []Instruction {
	t.Helper()
	// The decoder's framing must be the encoder's, which is the compressor's.
	d := newInstrDecoder(e.Bytes(), testBlockSize, sourceSize, maxRun, e.winSizes)
	var out []Instruction
	for !d.done() {
		var in Instruction
		if err := d.Next(&in); err != nil {
			t.Fatalf("decode instruction %d: %v", len(out), err)
		}
		out = append(out, in)
	}
	if len(out) != e.Count() {
		t.Fatalf("decoded %d instructions, encoded %d", len(out), e.Count())
	}
	return out
}

func TestInstrRoundTrip(t *testing.T) {
	const maxRun = 8 << 20
	e := newInstrEncoder(testBlockSize, false)

	// A copy from exactly the cursor start, which must encode a zero delta.
	if err := e.Copy(96, 1000); err != nil {
		t.Fatal(err)
	}
	// Contiguous continuation: also a zero delta.
	if err := e.Copy(1096, 5000); err != nil {
		t.Fatal(err)
	}
	// Backwards jump: a negative delta, which zigzag must survive.
	if err := e.Copy(200, 7); err != nil {
		t.Fatal(err)
	}
	if err := e.Literal(4096); err != nil {
		t.Fatal(err)
	}
	blocks := []PlanBlock{
		{USize: testBlockSize, CSize: 1000},     // full block, compressed
		{USize: 7, CSize: 7},                    // tiny tail stored raw
		{USize: testBlockSize - 1, CSize: 4095}, // largest partial tail
		{USize: 1, CSize: 1},                    // one raw byte
	}
	// One compressed window and one already-plaintext window (ULen == Len),
	// which is how a run of raw-stored source blocks travels.
	windows := []SrcWindow{
		{Off: 1 << 20, Len: 1 << 16, ULen: 6 << 16},
		{Off: 4096, Len: 100, ULen: 100},
	}
	if err := e.PatchRun(blocks, windows, 12345); err != nil {
		t.Fatal(err)
	}
	// A patch run with no windows at all: a block built from nothing but the
	// patch, which is how a wholly new file lands here.
	if err := e.PatchRun([]PlanBlock{{USize: 99, CSize: 50}}, nil, 60); err != nil {
		t.Fatal(err)
	}

	got := decodeAll(t, e, testSourceSize, maxRun)
	want := []Instruction{
		{Op: opCopy, SrcOff: 96, Len: 1000},
		{Op: opCopy, SrcOff: 1096, Len: 5000},
		{Op: opCopy, SrcOff: 200, Len: 7},
		{Op: opLiteral, Len: 4096},
		{Op: opPatchRun, Blocks: blocks, Windows: windows, PatchLen: 12345},
		{Op: opPatchRun, Blocks: []PlanBlock{{USize: 99, CSize: 50}}, PatchLen: 60},
	}
	for i := range want {
		// Next reuses slices, so compare field by field with empty and nil
		// treated alike.
		if got[i].Op != want[i].Op || got[i].SrcOff != want[i].SrcOff ||
			got[i].Len != want[i].Len || got[i].PatchLen != want[i].PatchLen {
			t.Errorf("instruction %d: got %+v, want %+v", i, got[i], want[i])
			continue
		}
		if len(got[i].Blocks) != len(want[i].Blocks) ||
			(len(want[i].Blocks) > 0 && !reflect.DeepEqual(got[i].Blocks, want[i].Blocks)) {
			t.Errorf("instruction %d blocks: got %+v, want %+v", i, got[i].Blocks, want[i].Blocks)
		}
		if len(got[i].Windows) != len(want[i].Windows) ||
			(len(want[i].Windows) > 0 && !reflect.DeepEqual(got[i].Windows, want[i].Windows)) {
			t.Errorf("instruction %d windows: got %+v, want %+v", i, got[i].Windows, want[i].Windows)
		}
	}

	// The zero-delta cases must really be one byte each, or the source-cursor
	// scheme is not buying anything.
	e2 := newInstrEncoder(testBlockSize, false)
	if err := e2.Copy(96, 1<<20); err != nil {
		t.Fatal(err)
	}
	before := len(e2.Bytes())
	if err := e2.Copy(96+(1<<20), 1<<20); err != nil {
		t.Fatal(err)
	}
	if n := len(e2.Bytes()) - before; n != 5 {
		// opcode + one-byte zero delta + three-byte length
		t.Errorf("contiguous copy encoded in %d bytes, want 5", n)
	}
}

func TestInstrRejectsMalformed(t *testing.T) {
	const maxRun = 8 << 20
	tests := []struct {
		name string
		buf  []byte
	}{
		{"empty opcode", []byte{0}},
		{"unknown opcode", []byte{99, 0, 1}},
		{"copy truncated after opcode", []byte{byte(opCopy)}},
		{"copy zero length", []byte{byte(opCopy), 0, 0}},
		{"literal zero length", []byte{byte(opLiteral), 0}},
		{"patchrun zero blocks", []byte{byte(opPatchRun), 0}},
		{"patchrun block count beyond stream", []byte{byte(opPatchRun), 100}},
		{"patchrun uSize at block size", func() []byte {
			e := newInstrEncoder(testBlockSize, false)
			e.PatchRun([]PlanBlock{{USize: 4, CSize: 2}}, nil, 0)
			b := append([]byte(nil), e.Bytes()...)
			// Rewrite the encoded uSize from 4 to 0x80 0x80 0x08 (131072),
			// which must be rejected as it should have encoded as 0.
			return append(b[:2], append([]byte{0x80, 0x80, 0x08}, b[3:]...)...)
		}()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := newInstrDecoder(tc.buf, testBlockSize, testSourceSize, maxRun, false)
			var in Instruction
			var err error
			for !d.done() && err == nil {
				err = d.Next(&in)
			}
			if err == nil {
				t.Errorf("malformed stream %x was accepted", tc.buf)
			}
		})
	}
}

func TestInstrBoundsChecks(t *testing.T) {
	// A copy past the end of the source must be refused even though it
	// encoded fine, because on the device the stream is untrusted.
	e := newInstrEncoder(testBlockSize, false)
	if err := e.Copy(1000, 500); err != nil {
		t.Fatal(err)
	}
	d := newInstrDecoder(e.Bytes(), testBlockSize, 1200, 8<<20, false)
	var in Instruction
	if err := d.Next(&in); err == nil {
		t.Error("copy running past the end of the source was accepted")
	}

	// Windows whose plaintext exceeds the cap must be refused: the applier
	// holds all of it in memory at once, so this is a memory bound, not a
	// formatting rule.
	e = newInstrEncoder(testBlockSize, false)
	if err := e.PatchRun([]PlanBlock{{USize: 4096, CSize: 100}}, []SrcWindow{
		{Off: 0, Len: 1 << 16, ULen: 20 << 20},
	}, 10); err != nil {
		t.Fatal(err)
	}
	d = newInstrDecoder(e.Bytes(), testBlockSize, testSourceSize, 8<<20, false)
	if err := d.Next(&in); err == nil {
		t.Error("a patch run whose windows decompress past the cap was accepted")
	}

	// A patch run over the run cap must be refused.
	e = newInstrEncoder(testBlockSize, false)
	blocks := make([]PlanBlock, 100)
	for i := range blocks {
		blocks[i] = PlanBlock{USize: testBlockSize, CSize: 100}
	}
	if err := e.PatchRun(blocks, nil, 10); err != nil {
		t.Fatal(err)
	}
	d = newInstrDecoder(e.Bytes(), testBlockSize, testSourceSize, 8<<20, false)
	if err := d.Next(&in); err == nil {
		t.Error("patch run over the run cap was accepted")
	}
}

func TestMDFrameRoundTrip(t *testing.T) {
	blocks := []MetaBlock{
		{Offset: 1000, CSize: 4000, USize: squashfsMetadataSize},
		{Offset: 5002, CSize: squashfsMetadataSize, USize: squashfsMetadataSize, Raw: true},
		{Offset: 13196, CSize: 7, USize: 7, Raw: true},
		{Offset: 13205, CSize: 100, USize: 321},
	}
	blob := bytes.Repeat([]byte("metadata"), 100)
	buf, err := encodeMDFrame(blocks, blob)
	if err != nil {
		t.Fatal(err)
	}
	got, digest, err := decodeMDFrame(buf, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, blocks) {
		t.Errorf("metadata framing round trip:\n got %+v\nwant %+v", got, blocks)
	}
	if digest != sha256.Sum256(blob) {
		t.Error("the metadata blob digest did not round trip")
	}
	// The framing must be compact: a digest plus two varints per block, so a
	// 58-block region stays a few hundred bytes.
	if len(buf) > sha256.Size+4*len(blocks) {
		t.Errorf("metadata framing used %d bytes for %d blocks", len(buf), len(blocks))
	}
	for _, n := range []int{0, 1, sha256.Size - 1, sha256.Size} {
		if _, _, err := decodeMDFrame(buf[:n], 1000); err == nil {
			t.Errorf("metadata framing truncated to %d bytes was accepted", n)
		}
	}
}

func TestBlockPlanContainerRoundTrip(t *testing.T) {
	ctx := context.Background()
	instr := bytes.Repeat([]byte("instruction stream, highly compressible "), 500)

	var w blockPlanWriter
	w.Header = blockPlanHeader{
		PatchTool:             DeltaToolHdiffz,
		BlockSize:             testBlockSize,
		MaxRunUSize:           8 << 20,
		InstrCount:            7,
		SourceSize:            1234,
		TargetSize:            8192,
		TargetBytesUsed:       8000,
		TargetInodeTableStart: 4096,
	}
	w.Header.SourceSHA256[0] = 0xaa
	w.Header.TargetSHA256[31] = 0xbb

	sb := bytes.Repeat([]byte{0x5a}, 96)
	w.addSection(secSB, sb)
	w.addSection(secMDFrame, []byte{0x00, 0x80, 0x01})
	w.addSection(secMDTail, []byte("tail"))
	if err := w.addSectionCompressed(ctx, secInstr, instr, &xzCLI{}); err != nil {
		t.Fatal(err)
	}
	pay := bytes.Repeat([]byte{0x11, 0x22}, 4096)
	w.addSection(secPay, pay)

	var buf bytes.Buffer
	if err := w.writeTo(&buf); err != nil {
		t.Fatal(err)
	}
	whole := buf.Bytes()

	br, err := openBlockPlan(bytes.NewReader(whole))
	if err != nil {
		t.Fatal(err)
	}
	if br.Header.BlockSize != testBlockSize || br.Header.InstrCount != 7 ||
		br.Header.SourceSHA256[0] != 0xaa || br.Header.TargetSHA256[31] != 0xbb {
		t.Errorf("header did not round trip: %+v", br.Header)
	}
	if !bytes.Equal(br.section(secSB), sb) {
		t.Error("SEC_SB did not round trip")
	}
	if !bytes.Equal(br.section(secInstr), instr) {
		t.Error("SEC_INSTR did not round trip through xz")
	}
	if br.hasSection(secMDPatch) {
		t.Error("absent SEC_MDPATCH reported as present")
	}
	// SEC_PAY stays streamed, so it is not in the eager map.
	if br.hasSection(secPay) {
		t.Error("SEC_PAY was materialized eagerly")
	}
	if br.payLen != int64(len(pay)) {
		t.Errorf("SEC_PAY length %d, want %d", br.payLen, len(pay))
	}
	got := make([]byte, br.payLen)
	if _, err := br.pay.Read(got[:1]); err != nil {
		t.Fatal(err)
	}
	if got[0] != 0x11 {
		t.Errorf("SEC_PAY starts with %#x, want 0x11", got[0])
	}

	// xz really is pulling its weight on the instruction stream.
	for _, e := range br.entries {
		if e.ID == secInstr {
			if e.Codec != codecXZ {
				t.Errorf("SEC_INSTR codec %d, want xz", e.Codec)
			}
			if e.StoredLen > e.RawLen/10 {
				t.Errorf("SEC_INSTR compressed %d -> %d, expected far better", e.RawLen, e.StoredLen)
			}
		}
	}

	// Every single-byte corruption from the section table onward must be
	// caught, by the table's own consistency rules or by a section CRC. Walk
	// a sample rather than all of them, since the payload is large.
	//
	// Header corruption deliberately is not a parse-time failure: most of the
	// header is digests and geometry that only mean anything once the target
	// exists, and those are checked while applying.
	start := blockPlanHeaderSize + len(br.entries)*blockPlanEntrySize
	eagerEnd := len(whole) - len(pay)
	for off := start; off < eagerEnd; off += 37 {
		bad := append([]byte(nil), whole...)
		bad[off] ^= 0x01
		if _, err := openBlockPlan(bytes.NewReader(bad)); err == nil {
			t.Errorf("flipping byte %d of the delta was not detected", off)
		}
	}

	// Truncation must be caught too, at any point.
	for _, n := range []int{0, 1, 64, blockPlanHeaderSize, blockPlanHeaderSize + 8, eagerEnd - 1} {
		if _, err := openBlockPlan(bytes.NewReader(whole[:n])); err == nil {
			t.Errorf("delta truncated to %d bytes was accepted", n)
		}
	}
}

func TestBlockPlanRejectsBadHeader(t *testing.T) {
	base := blockPlanHeader{
		BlockSize:             testBlockSize,
		MaxRunUSize:           8 << 20,
		SectionCount:          1,
		TargetSize:            8192,
		TargetBytesUsed:       8000,
		TargetInodeTableStart: 4096,
		FormatVersion:         blockPlanFormatVersion,
		ToolsVersion:          deltaFormatToolsVersion,
	}
	tests := []struct {
		name  string
		mutfn func(*blockPlanHeader)
	}{
		{"bad format version", func(h *blockPlanHeader) { h.FormatVersion = 9 }},
		{"bad tools version", func(h *blockPlanHeader) { h.ToolsVersion = 9 }},
		{"unknown flags", func(h *blockPlanHeader) { h.Flags = 1 }},
		{"no sections", func(h *blockPlanHeader) { h.SectionCount = 0 }},
		{"block size not a dictionary size", func(h *blockPlanHeader) { h.BlockSize = 100000 }},
		{"run cap below one block", func(h *blockPlanHeader) { h.MaxRunUSize = 4096 }},
		{"inode table past bytes used", func(h *blockPlanHeader) { h.TargetInodeTableStart = 9000 }},
		{"bytes used past image size", func(h *blockPlanHeader) { h.TargetBytesUsed = 9999 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := base
			tc.mutfn(&h)
			if _, err := parseBlockPlanHeader(h.marshal()); err == nil {
				t.Error("malformed header was accepted")
			}
		})
	}
	if _, err := parseBlockPlanHeader(base.marshal()); err != nil {
		t.Errorf("the unmutated header was rejected: %v", err)
	}
	// A "sqpf" pseudo-file delta must not be mistaken for a block plan.
	old := make([]byte, blockPlanHeaderSize)
	binary.LittleEndian.PutUint32(old, deltaMagicNumber)
	if _, err := parseBlockPlanHeader(old); err == nil {
		t.Error("a pseudo-file delta header was accepted as a block plan")
	}
}

// FuzzInstrStream generates long random instruction streams, checks they survive
// a round trip, and -- on arbitrary bytes -- that the decoder never panics or
// accepts something out of bounds.
// randomPartition splits n into between one and eight positive parts, which is
// what a window's block sizes have to be: every part at least one byte, and the
// whole summing to the window.
func randomPartition(rng *rand.Rand, n int) []int {
	parts := 1 + rng.Intn(8)
	if parts > n {
		parts = n
	}
	out := make([]int, 0, parts)
	left := n
	for i := 0; i < parts-1; i++ {
		// Leave one byte for each remaining part.
		take := 1 + rng.Intn(left-(parts-i-1))
		out = append(out, take)
		left -= take
	}
	return append(out, left)
}

func FuzzInstrStream(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add(bytes.Repeat([]byte{0xff}, 64))
	f.Fuzz(func(t *testing.T, seed []byte) {
		// Half the budget goes on encode/decode symmetry over structured
		// input, half on the decoder's tolerance of raw garbage.
		var s int64
		for i, b := range seed {
			s = s*31 + int64(b)*int64(i+1)
		}
		rng := rand.New(rand.NewSource(s))
		const maxRun = 8 << 20
		// Both window framings: a self-delimiting compressor records no block
		// sizes inside a window, one that needs them records every one.
		winSizes := rng.Intn(2) == 0
		e := newInstrEncoder(testBlockSize, winSizes)
		type expect struct {
			op       opcode
			srcOff   int64
			length   int64
			blocks   []PlanBlock
			windows  []SrcWindow
			patchLen int
		}
		var want []expect
		n := 1 + rng.Intn(200)
		for i := 0; i < n; i++ {
			switch rng.Intn(3) {
			case 0:
				off := rng.Int63n(testSourceSize - 1<<20)
				length := int64(1 + rng.Intn(1<<20))
				if err := e.Copy(off, length); err != nil {
					t.Fatalf("Copy: %v", err)
				}
				want = append(want, expect{op: opCopy, srcOff: off, length: length})
			case 1:
				length := int64(1 + rng.Intn(maxRun))
				if err := e.Literal(length); err != nil {
					t.Fatalf("Literal: %v", err)
				}
				want = append(want, expect{op: opLiteral, length: length})
			default:
				nb := 1 + rng.Intn(8)
				blocks := make([]PlanBlock, nb)
				for j := range blocks {
					u := 1 + rng.Intn(testBlockSize)
					c := 1 + rng.Intn(u)
					blocks[j] = PlanBlock{USize: u, CSize: c}
				}
				nw := rng.Intn(4)
				windows := make([]SrcWindow, nw)
				// The windows' plaintext must stay inside the decoder's cap of
				// twice the run, so share that budget between them.
				winBudget := 2*maxRun/(nw+1) - 1
				for j := range windows {
					length := 1 + rng.Intn(1<<20)
					if length > winBudget {
						length = winBudget
					}
					windows[j] = SrcWindow{
						Off: rng.Int63n(testSourceSize - 1<<20),
						Len: length,
						// Anything from plaintext (ULen == Len) up to the
						// budget, so both invariant branches get exercised.
						ULen: length + rng.Intn(winBudget-length+1),
					}
					if winSizes && !windows[j].Plain() {
						windows[j].CSizes = randomPartition(rng, length)
					}
				}
				patchLen := rng.Intn(maxRun)
				if err := e.PatchRun(blocks, windows, patchLen); err != nil {
					t.Fatalf("PatchRun: %v", err)
				}
				want = append(want, expect{op: opPatchRun, blocks: blocks, windows: windows, patchLen: patchLen})
			}
		}
		d := newInstrDecoder(e.Bytes(), testBlockSize, testSourceSize, maxRun, winSizes)
		for i, wa := range want {
			var in Instruction
			if err := d.Next(&in); err != nil {
				t.Fatalf("instruction %d (%v): %v", i, wa.op, err)
			}
			if in.Op != wa.op || in.SrcOff != wa.srcOff || in.Len != wa.length || in.PatchLen != wa.patchLen {
				t.Fatalf("instruction %d: got op=%v src=%d len=%d patch=%d, want op=%v src=%d len=%d patch=%d",
					i, in.Op, in.SrcOff, in.Len, in.PatchLen, wa.op, wa.srcOff, wa.length, wa.patchLen)
			}
			if len(in.Blocks) != len(wa.blocks) {
				t.Fatalf("instruction %d: %d blocks, want %d", i, len(in.Blocks), len(wa.blocks))
			}
			for j := range wa.blocks {
				if in.Blocks[j] != wa.blocks[j] {
					t.Fatalf("instruction %d block %d: got %+v, want %+v", i, j, in.Blocks[j], wa.blocks[j])
				}
			}
			for j := range wa.windows {
				gw, ww := in.Windows[j], wa.windows[j]
				// Next reuses each window's size slice, so an empty one and a
				// nil one mean the same thing.
				if gw.Off != ww.Off || gw.Len != ww.Len || gw.ULen != ww.ULen ||
					len(gw.CSizes) != len(ww.CSizes) ||
					(len(ww.CSizes) > 0 && !reflect.DeepEqual(gw.CSizes, ww.CSizes)) {
					t.Fatalf("instruction %d window %d: got %+v, want %+v", i, j, gw, ww)
				}
			}
		}
		if !d.done() {
			t.Fatalf("%d bytes left after decoding %d instructions", len(e.Bytes())-d.pos, len(want))
		}

		// Now the garbage path: whatever the decoder does with the raw
		// seed, it must not panic and must not hand back a reference
		// outside the declared source.
		gd := newInstrDecoder(seed, testBlockSize, 4096, maxRun, false)
		for !gd.done() {
			var in Instruction
			if err := gd.Next(&in); err != nil {
				break
			}
			switch in.Op {
			case opCopy:
				if in.SrcOff < 0 || in.SrcOff+in.Len > 4096 {
					t.Fatalf("accepted copy [%d,+%d) outside a 4096-byte source", in.SrcOff, in.Len)
				}
			case opPatchRun:
				if in.USizeTotal() > maxRun {
					t.Fatalf("accepted patch run reconstructing %d bytes", in.USizeTotal())
				}
				for _, w := range in.Windows {
					if w.Off < 0 || w.Off+int64(w.Len) > 4096 {
						t.Fatalf("accepted window [%d,+%d) outside a 4096-byte source", w.Off, w.Len)
					}
				}
			}
		}
	})
}
