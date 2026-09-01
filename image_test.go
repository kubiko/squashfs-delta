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
	"hash/crc32"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// These tests build real squashfs images with mksquashfs rather than using
// checked-in fixtures, so they exercise the same producer the store does and
// stay honest when squashfs-tools changes. Everything here skips cleanly when
// the tools are missing.

// snapdMksquashfsArgs is exactly how snapd builds an app snap, from
// snap/squashfs/squashfs.go. The delta format is designed against these
// options, so the fixtures must use them and nothing else.
var snapdMksquashfsArgs = []string{
	"-noappend", "-comp", "xz", "-no-fragments", "-no-progress", "-all-root", "-no-xattrs",
}

func requireTools(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		if _, err := toolPath(n); err != nil {
			t.Skipf("%s is not available: %v", n, err)
		}
	}
}

// buildImage populates a directory through populate and packs it the way snapd
// does, returning the image path. extra is appended, which is how a test asks
// for something snapd's own options do not already cover.
func buildImage(t *testing.T, name string, populate func(t *testing.T, dir string), extra ...string) string {
	t.Helper()
	return buildImageArgs(t, name, populate, append(append([]string{}, snapdMksquashfsArgs...), extra...)...)
}

// buildImageArgs packs a tree under an explicit argument list. The refusal tests
// need this rather than extra arguments, because mksquashfs refuses two
// conflicting -comp options outright and silently keeps -no-fragments whatever
// follows it.
func buildImageArgs(t *testing.T, name string, populate func(t *testing.T, dir string), args ...string) string {
	t.Helper()
	bin, err := toolPath("mksquashfs")
	if err != nil {
		t.Skipf("mksquashfs is not available: %v", err)
	}

	root := t.TempDir()
	tree := filepath.Join(root, "tree")
	if err := os.MkdirAll(tree, 0755); err != nil {
		t.Fatal(err)
	}
	populate(t, tree)

	img := filepath.Join(root, name)
	full := append([]string{tree, img}, args...)
	out, err := exec.Command(bin, full...).CombinedOutput()
	if err != nil {
		t.Fatalf("mksquashfs %v: %v\n%s", full, err, out)
	}
	return img
}

// writeFile is a populate helper; mode 0644 throughout, since -all-root
// normalizes ownership anyway.
func writeFile(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	full := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, data, 0644); err != nil {
		t.Fatal(err)
	}
}

// incompressible returns bytes no compressor can shrink, so mksquashfs stores
// the block raw -- the case where cSize == uSize and the compressed bit is set.
func incompressible(n int, seed int64) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

// compressibleText returns highly compressible bytes, so the block is stored
// compressed and has cSize far below uSize.
func compressibleText(n int, tag string) []byte {
	out := make([]byte, 0, n+64)
	for len(out) < n {
		out = append(out, "the quick brown fox jumps over the lazy dog "+tag+"\n"...)
	}
	return out[:n]
}

// populateMixed lays down every block shape the extent walk has to handle:
// several full compressed blocks plus a partial tail, a raw block, a wholly
// sparse file (extended inode, zero size words), a duplicate that mksquashfs
// dedups onto shared extents, and a hard link so one inode has two names.
func populateMixed(t *testing.T, dir string) {
	t.Helper()
	writeFile(t, dir, "multi.txt", compressibleText(400000, "multi"))
	writeFile(t, dir, "raw.bin", incompressible(200000, 1))
	writeFile(t, dir, "sub/small.txt", []byte("hello world\n"))
	// Same bytes as multi.txt: dedup makes both inodes share extents.
	writeFile(t, dir, "sub/dup.txt", compressibleText(400000, "multi"))

	sparse := filepath.Join(dir, "sparse.bin")
	f, err := os.Create(sparse)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(300000); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if err := os.Link(filepath.Join(dir, "sub/small.txt"), filepath.Join(dir, "sub/link.txt")); err != nil {
		t.Fatal(err)
	}
}

func TestImageGeometryAndExtents(t *testing.T) {
	requireTools(t, "mksquashfs", "xz")
	ctx := context.Background()
	img := buildImage(t, "mixed.snap", populateMixed)

	im, err := openSquashfsImage(img)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.checkSupported(); err != nil {
		t.Fatalf("a snapd-style image was refused: %v", err)
	}

	ext, gaps, overlaps, err := im.CheckCoverage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 0 || len(overlaps) != 0 {
		t.Errorf("data region has %d gaps and %d overlaps, want none: %v %v",
			len(gaps), len(overlaps), gaps, overlaps)
	}
	var onDisk int64
	sawRaw, sawPartial := false, false
	for _, e := range ext {
		onDisk += int64(e.CSize)
		if e.Raw {
			sawRaw = true
			if e.CSize != e.USize {
				t.Errorf("raw extent at %d occupies %d bytes for %d uncompressed", e.Offset, e.CSize, e.USize)
			}
		} else if e.CSize >= e.USize {
			t.Errorf("compressed extent at %d is %d bytes for %d uncompressed, so it should have been stored raw",
				e.Offset, e.CSize, e.USize)
		}
		if e.USize < int(im.SB.BlockSize) {
			sawPartial = true
		}
	}
	if want := im.DataRegionEnd() - 96; onDisk != want {
		t.Errorf("extents occupy %d bytes, the data region is %d", onDisk, want)
	}
	if !sawRaw {
		t.Error("no raw block in the fixture, so the raw path is untested")
	}
	if !sawPartial {
		t.Error("no partial tail in the fixture, so the partial path is untested")
	}

	// Both file inode types must appear: the sparse file forces an extended
	// inode, everything else is basic.
	inodes, err := im.FileInodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	types := map[uint16]int{}
	sparseHoles := 0
	for _, fi := range inodes {
		types[fi.Type]++
		for _, w := range fi.Sizes {
			if w == 0 {
				sparseHoles++
			}
		}
	}
	if types[inodeTypeFile] == 0 {
		t.Error("no basic file inode (type 2) in the fixture")
	}
	if types[inodeTypeExtFile] == 0 {
		t.Errorf("no extended file inode (type 9) in the fixture, got types %v", types)
	}
	if sparseHoles == 0 {
		t.Error("no sparse hole in the fixture, so the zero-size-word path is untested")
	}

	// Dedup: two inodes describe the same 400000 bytes, and Extents must
	// report those blocks once, not twice.
	dupBytes := 0
	for _, fi := range inodes {
		if fi.FileSize == 400000 {
			dupBytes++
		}
	}
	if dupBytes < 2 {
		t.Fatalf("expected two inodes of 400000 bytes for the dedup case, found %d", dupBytes)
	}
	seen := map[int64]bool{}
	for _, e := range ext {
		if seen[e.Offset] {
			t.Errorf("extent at %d reported twice", e.Offset)
		}
		seen[e.Offset] = true
	}
}

func TestImageRefusals(t *testing.T) {
	requireTools(t, "mksquashfs", "xz")
	// Each of these produces an image the format cannot replay, and each must
	// be refused rather than mis-described. snapd falls back to a full
	// download, so a refusal costs bandwidth and nothing else.
	tests := []struct {
		name string
		// args replaces snapd's whole argument list; extra appends to it.
		args     []string
		extra    []string
		populate func(t *testing.T, dir string)
	}{
		// Fragments pack several small files into a shared block the extent
		// walk does not describe. -no-fragments has to go: mksquashfs keeps
		// it whatever follows, so appending -always-use-fragments only sets
		// the flag and still writes no fragments.
		{name: "fragments", args: []string{
			"-noappend", "-comp", "xz", "-always-use-fragments", "-no-progress", "-all-root", "-no-xattrs",
		}},
		// The default: fragments on, which is what an image not built by
		// snapd looks like.
		{name: "default fragments", args: []string{
			"-noappend", "-comp", "xz", "-no-progress", "-all-root", "-no-xattrs",
		}},
		// A BCJ filter is recorded as COMPRESSOR_OPTIONS, and its filter
		// chain is not what the applier reproduces.
		{name: "bcj filter", extra: []string{"-Xbcj", "x86"}},
		// So is a dictionary that differs from the block size.
		{name: "xz dict size", extra: []string{"-Xdict-size", "32K"}},
		// gzip is a different compressor id entirely, and mksquashfs refuses
		// a second -comp, so this one also needs the full list.
		{name: "gzip", args: []string{
			"-noappend", "-comp", "gzip", "-no-fragments", "-no-progress", "-all-root", "-no-xattrs",
		}},
		// No export table means inodes cannot be enumerated without walking
		// directories, which the generator deliberately does not do.
		{name: "no exports", extra: []string{"-no-exports"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			populate := tc.populate
			if populate == nil {
				populate = populateMixed
			}
			var img string
			if tc.args != nil {
				img = buildImageArgs(t, "bad.snap", populate, tc.args...)
			} else {
				img = buildImage(t, "bad.snap", populate, tc.extra...)
			}
			im, err := openSquashfsImage(img)
			if err != nil {
				// Refusing to parse at all is also a refusal.
				return
			}
			if err := im.checkSupported(); err == nil {
				t.Errorf("an image built with %v%v was accepted:\n%s", tc.args, tc.extra, im.SB)
			}
		})
	}
}

// populateXattr is populateMixed plus real extended attributes, which is what
// makes mksquashfs emit an xattr table. Note it takes a file that really has
// one: -xattrs on a tree without any produces an image with NO_XATTRS clear and
// no xattr table at all, which is exactly the core26 shape and why the geometry
// check gates on the table pointer rather than the flag.
func populateXattr(t *testing.T, dir string, value string) {
	t.Helper()
	populateMixed(t, dir)
	for _, name := range []string{"sub/small.txt", "multi.txt"} {
		if err := syscall.Setxattr(filepath.Join(dir, name), "user.test", []byte(value), 0); err != nil {
			t.Skipf("cannot set an xattr under %s: %v", dir, err)
		}
	}
}

// TestImageXattrs pins down that an xattr table needs no work of its own. It
// costs nothing because mksquashfs writes the xattr value blocks and id table
// after every other table, so they land above export_table_start and travel
// verbatim in SEC_MDTAIL -- and because the inode walk is driven by the export
// table and both extended inode layouts carry their xattr word ahead of the
// fields it reads. The test asserts the premise (the table really is up there),
// not just the outcome, since the outcome would also hold if mksquashfs stopped
// putting it there and the delta silently dropped it.
func TestImageXattrs(t *testing.T) {
	requireTools(t, "mksquashfs", "xz", "hdiffz", "hpatchz")
	ctx := context.Background()

	source := buildImage(t, "xattr-source.snap", func(t *testing.T, dir string) {
		populateXattr(t, dir, "hello")
	}, "-xattrs")
	// The target's attributes differ in both name count and value length, so
	// the xattr value blocks and id table both change and the tail section has
	// to carry the new ones rather than the source's.
	target := buildImage(t, "xattr-target.snap", func(t *testing.T, dir string) {
		populateXattr(t, dir, "a rather longer value than the source had")
		if err := syscall.Setxattr(filepath.Join(dir, "raw.bin"), "user.extra", []byte("2"), 0); err != nil {
			t.Skipf("cannot set an xattr under %s: %v", dir, err)
		}
	}, "-xattrs")

	for _, path := range []string{source, target} {
		im, err := openSquashfsImage(path)
		if err != nil {
			t.Fatal(err)
		}
		if im.SB.XattrTableStart == squashfsNoTable {
			t.Fatalf("%s has no xattr table, so the fixture proves nothing", filepath.Base(path))
		}
		if err := im.checkSupported(); err != nil {
			t.Fatalf("%s was refused: %v", filepath.Base(path), err)
		}
		// The value blocks are the lowest xattr byte, and the whole point is
		// that they sit inside [export_table_start, bytes_used).
		values := binary.LittleEndian.Uint64(im.Data[im.SB.XattrTableStart:])
		if values < im.SB.ExportTableStart {
			t.Errorf("%s puts its xattr value blocks at %d, below the export table at %d",
				filepath.Base(path), values, im.SB.ExportTableStart)
		}
	}

	delta := filepath.Join(t.TempDir(), "xattr.delta")
	if _, err := generateBlockPlan(ctx, source, target, delta, blockPlanGenOpts{
		Comp: &xzCLI{}, Verify: true,
	}); err != nil {
		t.Fatalf("generating a delta between images with xattrs: %v", err)
	}
	applyAndCompare(t, source, delta, target, &xzCLI{})
}

// TestImageNonDefaultBlockSize pins down what is *not* a refusal: a block size
// other than 128 KiB is fine, because the only thing the format needs from it is
// that it be a representable LZMA2 dictionary size. Only a source and target
// disagreeing about it is refused.
func TestImageNonDefaultBlockSize(t *testing.T) {
	requireTools(t, "mksquashfs", "xz", "hdiffz", "hpatchz")
	ctx := context.Background()
	img := buildImage(t, "small-blocks.snap", populateMixed, "-b", "64K")
	im, err := openSquashfsImage(img)
	if err != nil {
		t.Fatal(err)
	}
	if im.SB.BlockSize != 64<<10 {
		t.Fatalf("mksquashfs produced %d-byte blocks, wanted 65536", im.SB.BlockSize)
	}
	if err := im.checkSupported(); err != nil {
		t.Errorf("a 64 KiB-block image was refused: %v", err)
	}
	delta := filepath.Join(t.TempDir(), "d.delta")
	if _, err := generateBlockPlan(ctx, img, img, delta, blockPlanGenOpts{Comp: &xzCLI{}, Verify: true}); err != nil {
		t.Errorf("generating a delta between 64 KiB-block images: %v", err)
	}

	// Mixing block sizes has to be refused: a copied block only stays valid
	// under the dictionary it was compressed with.
	other := buildImage(t, "big-blocks.snap", populateMixed)
	if _, err := generateBlockPlan(ctx, img, other, delta, blockPlanGenOpts{Comp: &xzCLI{}}); err == nil {
		t.Error("a delta across differing block sizes was generated")
	}
}

// deltaFixture builds a source/target pair and a delta between them, returning
// all three paths.
func deltaFixture(t *testing.T, mutate func(t *testing.T, dir string)) (source, target, delta string) {
	t.Helper()
	requireTools(t, "mksquashfs", "xz", "hdiffz", "hpatchz")
	source = buildImage(t, "source.snap", populateMixed)
	target = buildImage(t, "target.snap", func(t *testing.T, dir string) {
		populateMixed(t, dir)
		mutate(t, dir)
	})
	delta = filepath.Join(t.TempDir(), "d.delta")
	if _, err := generateBlockPlan(context.Background(), source, target, delta, blockPlanGenOpts{
		Comp:   &xzCLI{},
		Verify: true,
	}); err != nil {
		t.Fatalf("generating a delta: %v", err)
	}
	return source, target, delta
}

// smallEdit changes one file and adds another, so the metadata differs (it
// always does) while most data blocks still match.
func smallEdit(t *testing.T, dir string) {
	writeFile(t, dir, "sub/small.txt", []byte("hello again\n"))
	writeFile(t, dir, "sub/added.txt", compressibleText(9000, "added"))
}

func TestBlockPlanRoundTripsRealImages(t *testing.T) {
	ctx := context.Background()
	source, target, delta := deltaFixture(t, smallEdit)

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
	stats, err := applyBlockPlan(ctx, src, df, &got, blockPlanApplyOpts{Comp: &xzCLI{}})
	if err != nil {
		t.Fatalf("applying the delta: %v", err)
	}
	want, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("reconstruction differs from the target at offset %d", firstDiff(got.Bytes(), want))
	}
	// The point of the format: most of the image arrived without the
	// compressor, and the only plaintext pushed through xz was metadata.
	if stats.CopiedBytes == 0 {
		t.Error("nothing was copied verbatim, so no CPU was saved")
	}
	if stats.UCompressedBytes != stats.MetaUBytes {
		t.Errorf("compressed %d bytes of plaintext but only %d were metadata; with no patch runs they must agree",
			stats.UCompressedBytes, stats.MetaUBytes)
	}
}

// countingWriter records how much was written without keeping it, so a test can
// tell whether a failure happened before any target bytes were produced.
type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

// sectionOffsets maps each section id to where its stored bytes begin, and to
// its table entry offset, so a test can rewrite a section in place.
func sectionOffsets(t *testing.T, delta []byte) (payload, entry map[uint16]int, entries map[uint16]sectionEntry) {
	t.Helper()
	h, err := parseBlockPlanHeader(delta)
	if err != nil {
		t.Fatal(err)
	}
	payload, entry, entries = map[uint16]int{}, map[uint16]int{}, map[uint16]sectionEntry{}
	at := blockPlanHeaderSize + int(h.SectionCount)*blockPlanEntrySize
	for i := 0; i < int(h.SectionCount); i++ {
		eoff := blockPlanHeaderSize + i*blockPlanEntrySize
		e := parseSectionEntry(delta[eoff:])
		payload[e.ID], entry[e.ID], entries[e.ID] = at, eoff, e
		at += int(e.StoredLen)
	}
	if at != len(delta) {
		t.Fatalf("section payloads end at %d but the delta is %d bytes", at, len(delta))
	}
	return payload, entry, entries
}

// corruptSection flips one bit inside a section's stored bytes and repairs the
// section's CRC, so the container stays well-formed. Without the repair every
// such test would merely prove that CRC32 works; with it, whatever rejects the
// delta is the check under test.
func corruptSection(t *testing.T, delta []byte, id uint16, at int) []byte {
	t.Helper()
	bad := append([]byte(nil), delta...)
	payload, entry, entries := sectionOffsets(t, bad)
	e, ok := entries[id]
	if !ok {
		t.Fatalf("the delta has no %s to corrupt", sectionName(id))
	}
	if at >= int(e.StoredLen) {
		t.Fatalf("%s is %d bytes, cannot corrupt offset %d", sectionName(id), e.StoredLen, at)
	}
	start := payload[id]
	bad[start+at] ^= 0x01
	e.CRC = crc32.ChecksumIEEE(bad[start : start+int(e.StoredLen)])
	copy(bad[entry[id]:], e.marshal())
	return bad
}

// TestBlockPlanRejectsCorruptMDPatch is the metadata gate: a delta whose
// metadata patch has been tampered with must be refused, and refused before any
// of the target is produced. That ordering is the reason the metadata sections
// come first in the format.
func TestBlockPlanRejectsCorruptMDPatch(t *testing.T) {
	ctx := context.Background()
	source, _, delta := deltaFixture(t, smallEdit)

	whole, err := os.ReadFile(delta)
	if err != nil {
		t.Fatal(err)
	}
	_, _, entries := sectionOffsets(t, whole)
	patch, ok := entries[secMDPatch]
	if !ok {
		t.Fatal("the fixture produced no SEC_MDPATCH, so there is nothing to corrupt")
	}
	if patch.StoredLen == 0 {
		t.Fatal("SEC_MDPATCH is empty")
	}

	src, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	// Walk a spread of offsets: patch headers, control data and literals all
	// behave differently under a bit flip.
	offs := []int{0, 1, int(patch.StoredLen) / 4, int(patch.StoredLen) / 2, int(patch.StoredLen) - 1}
	for _, at := range offs {
		bad := corruptSection(t, whole, secMDPatch, at)
		var out countingWriter
		if _, err := src.Seek(0, 0); err != nil {
			t.Fatal(err)
		}
		_, err := applyBlockPlan(ctx, src, bytes.NewReader(bad), &out, blockPlanApplyOpts{Comp: &xzCLI{}})
		if err == nil {
			t.Errorf("a metadata patch corrupted at byte %d was accepted", at)
			continue
		}
		if out.n != 0 {
			t.Errorf("byte %d: the corrupt patch was only caught after %d bytes of the target had been written: %v",
				at, out.n, err)
		}
	}
}

// TestBlockPlanRejectsCorruptMDFrame covers the other half of the metadata
// gate: the framing itself, including the blob digest that makes the patch
// check possible before any data work.
func TestBlockPlanRejectsCorruptMDFrame(t *testing.T) {
	ctx := context.Background()
	source, _, delta := deltaFixture(t, smallEdit)

	whole, err := os.ReadFile(delta)
	if err != nil {
		t.Fatal(err)
	}
	_, _, entries := sectionOffsets(t, whole)
	frame := entries[secMDFrame]

	src, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	cases := []struct {
		at int
		// wantErr, when set, is text the error must contain, so the test
		// proves which check fired rather than only that one did.
		wantErr string
	}{
		// The first and last bytes of the digest. Nothing else can catch
		// these: the patch is untouched, so it applies cleanly and produces a
		// blob of exactly the right length.
		{at: 0, wantErr: "metadata digest"},
		{at: sha256.Size - 1, wantErr: "metadata digest"},
		// The framing proper: a uSize and a cSize varint.
		{at: sha256.Size},
		{at: int(frame.StoredLen) - 1},
	}
	for _, tc := range cases {
		bad := corruptSection(t, whole, secMDFrame, tc.at)
		var out countingWriter
		if _, err := src.Seek(0, 0); err != nil {
			t.Fatal(err)
		}
		_, err := applyBlockPlan(ctx, src, bytes.NewReader(bad), &out, blockPlanApplyOpts{Comp: &xzCLI{}})
		if err == nil {
			t.Errorf("metadata framing corrupted at byte %d was accepted", tc.at)
			continue
		}
		if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("byte %d: expected the %q check to fire, got: %v", tc.at, tc.wantErr, err)
		}
		if out.n != 0 {
			t.Errorf("byte %d: corrupt framing was only caught after %d bytes of the target had been written: %v",
				tc.at, out.n, err)
		}
	}
}

// TestBlockPlanRejectsWrongSource proves the source digest is what makes every
// OP_COPY safe: applied against the wrong revision, the delta must be refused
// outright rather than producing a plausible-looking image.
func TestBlockPlanRejectsWrongSource(t *testing.T) {
	ctx := context.Background()
	source, target, delta := deltaFixture(t, smallEdit)

	df, err := os.Open(delta)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	// The target is the same size as no other image here, so use it as a
	// stand-in for "some other revision".
	wrong, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	defer wrong.Close()

	var out countingWriter
	_, err = applyBlockPlan(ctx, wrong, df, &out, blockPlanApplyOpts{Comp: &xzCLI{}})
	if err == nil {
		t.Fatal("the delta was applied to the wrong source image")
	}
	if out.n != 0 {
		t.Errorf("the wrong source was only caught after %d bytes had been written: %v", out.n, err)
	}
	_ = source
}

// TestBlockPlanRejectsCorruptPayload checks the one section whose CRC cannot be
// verified at open time, because it is never held whole.
func TestBlockPlanRejectsCorruptPayload(t *testing.T) {
	ctx := context.Background()
	source, _, delta := deltaFixture(t, smallEdit)

	whole, err := os.ReadFile(delta)
	if err != nil {
		t.Fatal(err)
	}
	_, _, entries := sectionOffsets(t, whole)
	pay, ok := entries[secPay]
	if !ok || pay.StoredLen == 0 {
		t.Fatal("the fixture produced no SEC_PAY, so there is nothing to corrupt")
	}

	src, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	// A literal block's bytes go straight to the target, so this is caught by
	// the payload CRC and the image digest rather than up front -- which is
	// exactly why both exist.
	bad := corruptSection(t, whole, secPay, int(pay.StoredLen)/2)
	var out bytes.Buffer
	if _, err := applyBlockPlan(ctx, src, bytes.NewReader(bad), &out, blockPlanApplyOpts{Comp: &xzCLI{}}); err == nil {
		t.Error("a corrupt SEC_PAY was accepted")
	}
}

// TestBlockPlanUnrelatedImages is the negative control: two images with almost
// nothing in common must still produce a correct delta, merely a large one.
func TestBlockPlanUnrelatedImages(t *testing.T) {
	requireTools(t, "mksquashfs", "xz", "hdiffz", "hpatchz")
	ctx := context.Background()
	source := buildImage(t, "source.snap", populateMixed)
	target := buildImage(t, "target.snap", func(t *testing.T, dir string) {
		writeFile(t, dir, "everything.bin", incompressible(500000, 99))
		writeFile(t, dir, "else.txt", compressibleText(250000, "unrelated"))
	})
	delta := filepath.Join(t.TempDir(), "d.delta")
	stats, err := generateBlockPlan(ctx, source, target, delta, blockPlanGenOpts{
		Comp: &xzCLI{}, Verify: true,
	})
	if err != nil {
		t.Fatalf("generating a delta between unrelated images: %v", err)
	}
	// Verify already proved it reconstructs; what matters here is that the
	// generator did not pretend to find reuse that is not there.
	if stats.CopiedBytes > stats.TargetDataBytes/10 {
		t.Errorf("claimed to copy %d of %d bytes between unrelated images",
			stats.CopiedBytes, stats.TargetDataBytes)
	}
}

// TestBlockPlanIdentity is the simplest possible delta: source and target are
// the same image, so the whole data region is one copy and no metadata patch is
// needed at all.
func TestBlockPlanIdentity(t *testing.T) {
	requireTools(t, "mksquashfs", "xz", "hdiffz", "hpatchz")
	ctx := context.Background()
	img := buildImage(t, "same.snap", populateMixed)
	delta := filepath.Join(t.TempDir(), "d.delta")
	stats, err := generateBlockPlan(ctx, img, img, delta, blockPlanGenOpts{Comp: &xzCLI{}, Verify: true})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Instructions != 1 || stats.Copies != 1 || stats.Literals != 0 {
		t.Errorf("identity delta used %d instructions (%d copy, %d literal), want exactly one copy",
			stats.Instructions, stats.Copies, stats.Literals)
	}
	if stats.MDPatchBytes != 0 {
		t.Errorf("identity delta carries a %d-byte metadata patch", stats.MDPatchBytes)
	}
	if stats.CopiedBytes != stats.TargetDataBytes {
		t.Errorf("identity delta copied %d of %d data bytes", stats.CopiedBytes, stats.TargetDataBytes)
	}
	// It should also be tiny: a header, a section table and the superblock.
	if stats.DeltaSize > 4096 {
		t.Errorf("identity delta is %d bytes", stats.DeltaSize)
	}
}

// TestCanaryDetectsToolchainDrift proves SEC_CANARY does its job: a delta whose
// canary was recorded by a different compressor configuration must be refused
// before the target is created, which is the difference between a clean
// fall-back and a corrupt image.
func TestCanaryDetectsToolchainDrift(t *testing.T) {
	requireTools(t, "mksquashfs", "xz", "hdiffz", "hpatchz")
	ctx := context.Background()
	source, _, delta := deltaFixture(t, smallEdit)

	whole, err := os.ReadFile(delta)
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite the canary's recorded compressed length for the data
	// configuration, which is what a drifting liblzma would change.
	payload, entry, entries := sectionOffsets(t, whole)
	e := entries[secCanary]
	bad := append([]byte(nil), whole...)
	start := payload[secCanary]
	got := binary.LittleEndian.Uint32(bad[start+4:])
	binary.LittleEndian.PutUint32(bad[start+4:], got+1)
	e.CRC = crc32.ChecksumIEEE(bad[start : start+int(e.StoredLen)])
	copy(bad[entry[secCanary]:], e.marshal())

	src, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	var out countingWriter
	if _, err := applyBlockPlan(ctx, src, bytes.NewReader(bad), &out, blockPlanApplyOpts{Comp: &xzCLI{}}); err == nil {
		t.Error("a delta recording different compressor output was accepted")
	}
	if out.n != 0 {
		t.Errorf("the canary fired only after %d bytes had been written", out.n)
	}
}
