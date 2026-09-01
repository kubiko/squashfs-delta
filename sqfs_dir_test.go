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
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// populateNamed lays down the naming shapes the matcher has to survive: nested
// directories, a version number in a path, a file whose name differs from its
// neighbour only by digits, a hard link giving one inode two names, a dedup pair
// giving two inodes the same blocks, a symlink and a sparse file. Together they
// cover every branch of the tree walk that is not an error.
func populateNamed(t *testing.T, dir string) {
	t.Helper()
	writeFile(t, dir, "meta/snap.yaml", []byte("name: demo\nversion: 1.2.3\n"))
	writeFile(t, dir, "usr/lib/libdemo.so.1.2.3", compressibleText(300000, "lib"))
	writeFile(t, dir, "usr/lib/python3.12/site-packages/demo/__init__.py", []byte("x = 1\n"))
	writeFile(t, dir, "usr/share/doc/demo/README", compressibleText(5000, "doc"))
	// Same bytes as the library: mksquashfs dedups these onto shared extents,
	// so two paths resolve to two inodes over one set of blocks.
	writeFile(t, dir, "usr/lib/libdemo-copy.so.1.2.3", compressibleText(300000, "lib"))
	writeFile(t, dir, "bin/tool", incompressible(200000, 7))

	if err := os.Symlink("tool", filepath.Join(dir, "bin/tool-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(dir, "bin/tool"), filepath.Join(dir, "bin/tool-hard")); err != nil {
		t.Fatal(err)
	}

	sparse, err := os.Create(filepath.Join(dir, "var/sparse.bin"))
	if err != nil {
		if err := os.MkdirAll(filepath.Join(dir, "var"), 0755); err != nil {
			t.Fatal(err)
		}
		if sparse, err = os.Create(filepath.Join(dir, "var/sparse.bin")); err != nil {
			t.Fatal(err)
		}
	}
	if err := sparse.Truncate(300000); err != nil {
		t.Fatal(err)
	}
	sparse.Close()
}

// TestFileTreeNamesEveryFile is the whole contract the matcher rests on: the
// directory walk must give every data-bearing inode a path, and the blocks it
// reaches through those paths must be exactly the blocks the offset-ordered
// extent list holds. A path map missing a file silently costs delta size; one
// pointing at the wrong blocks would cost correctness, which is why the two
// enumerations are cross-checked here rather than trusted separately.
func TestFileTreeNamesEveryFile(t *testing.T) {
	requireTools(t, "mksquashfs", "xz")
	ctx := context.Background()

	img := buildImage(t, "named.squashfs", populateNamed)
	im, err := openSquashfsImage(img)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := im.MetaRegionAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := im.FileTree(meta)
	if err != nil {
		t.Fatalf("walking the directory table: %v", err)
	}

	got := make(map[string]*FileInode, len(tree))
	for _, e := range tree {
		if prev, dup := got[e.Path]; dup {
			t.Errorf("path %q appears twice, for inodes %d and %d", e.Path, prev.Number, e.Inode.Number)
		}
		got[e.Path] = e.Inode
	}
	want := []string{
		"bin/tool",
		"bin/tool-hard",
		"meta/snap.yaml",
		"usr/lib/libdemo-copy.so.1.2.3",
		"usr/lib/libdemo.so.1.2.3",
		"usr/lib/python3.12/site-packages/demo/__init__.py",
		"usr/share/doc/demo/README",
		"var/sparse.bin",
	}
	for _, p := range want {
		if _, ok := got[p]; !ok {
			t.Errorf("the tree does not name %q", p)
		}
	}
	// The symlink holds no data blocks, so it must not appear: an entry with a
	// nil inode would fault the matcher.
	if _, ok := got["bin/tool-link"]; ok {
		t.Error("the tree names a symlink, which has no data blocks")
	}
	if len(got) != len(want) {
		var names []string
		for p := range got {
			names = append(names, p)
		}
		sort.Strings(names)
		t.Errorf("the tree names %d files, want %d: %v", len(got), len(want), names)
	}

	// The hard link is one inode under two names, which is exactly what the
	// matcher must tolerate: two paths may legitimately resolve to one file.
	if a, b := got["bin/tool"], got["bin/tool-hard"]; a != nil && b != nil && a.Number != b.Number {
		t.Errorf("the hard link resolved to inode %d rather than %d", b.Number, a.Number)
	}

	// Every inode the export table knows must be reachable by name. This is the
	// check that would catch a walk that quietly stopped early -- the one
	// failure mode that costs delta size without ever producing a wrong image.
	inodes, err := im.FileInodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	named := make(map[uint32]bool, len(got))
	for _, fi := range got {
		named[fi.Number] = true
	}
	for _, fi := range inodes {
		if !named[fi.Number] {
			t.Errorf("inode %d bears data blocks but the tree gives it no name", fi.Number)
		}
	}

	// And the blocks reached through the paths have to be the same blocks the
	// generator emits, or an anchor would point outside the data region.
	ext, err := im.Extents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	all := make(map[int64]int, len(ext))
	for _, e := range ext {
		all[e.Offset] = e.CSize
	}
	for path, fi := range got {
		blocks, err := im.inodeExtents(fi)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		var uCovered int64
		for _, b := range blocks {
			if cSize, ok := all[b.Offset]; !ok {
				t.Errorf("%s has a block at %d that the extent list does not hold", path, b.Offset)
			} else if cSize != b.CSize {
				t.Errorf("%s has a %d-byte block at %d, the extent list says %d", path, b.CSize, b.Offset, cSize)
			}
			if b.UOff != uCovered {
				t.Errorf("%s: block at %d claims plaintext offset %d, want %d", path, b.Offset, b.UOff, uCovered)
			}
			uCovered = b.UOff + int64(b.USize)
		}
		// The plaintext offsets have to account for the whole file, holes
		// included, since that is the coordinate the matcher anchors on.
		if len(blocks) != 0 && uCovered != int64(fi.FileSize) {
			t.Errorf("%s: blocks cover %d plaintext bytes, the inode says %d", path, uCovered, fi.FileSize)
		}
	}
	// The sparse file is all hole, so it has no blocks at all -- and its
	// plaintext offsets are the only thing that would have gone wrong silently.
	if fi := got["var/sparse.bin"]; fi != nil {
		blocks, err := im.inodeExtents(fi)
		if err != nil {
			t.Fatal(err)
		}
		if len(blocks) != 0 {
			t.Errorf("a wholly sparse file yielded %d blocks", len(blocks))
		}
	}
}

// TestFileTreeSpansManyDirectoryHeaders covers the listing loop's outer step. A
// directory header carries at most 256 entries and mksquashfs starts a new one
// past that, so a walk that read only the first header would silently lose most
// of a large directory -- the same failure the cross-check above catches, but
// this is where it comes from.
func TestFileTreeSpansManyDirectoryHeaders(t *testing.T) {
	requireTools(t, "mksquashfs", "xz")
	ctx := context.Background()

	const files = 700
	img := buildImage(t, "wide.squashfs", func(t *testing.T, dir string) {
		for i := 0; i < files; i++ {
			// Distinct contents, or dedup would collapse them and the test
			// would prove less than it looks.
			writeFile(t, dir, fmt.Sprintf("many/f%03d.txt", i), compressibleText(2000+i, fmt.Sprintf("f%d", i)))
		}
	})
	im, err := openSquashfsImage(img)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := im.MetaRegionAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := im.FileTree(meta)
	if err != nil {
		t.Fatalf("walking a %d-entry directory: %v", files, err)
	}
	n := 0
	for _, e := range tree {
		if strings.HasPrefix(e.Path, "many/") {
			n++
		}
	}
	if n != files {
		t.Errorf("the walk found %d of %d entries in one directory", n, files)
	}
}

// synthListing builds a MetaRegion holding one directory listing, so the framing
// refusals can be provoked exactly. A real image cannot be corrupted here: the
// directory table lives inside compressed metadata blocks, and rewriting one
// means recompressing it.
func synthListing(t *testing.T, entries []byte) (*MetaRegion, dirListing) {
	t.Helper()
	// One metadata block at relative offset 0, which is both the inode table's
	// and -- with dirRel 0 -- the directory table's first block.
	return &MetaRegion{
		Start:  0,
		Blob:   entries,
		index:  map[uint64]int{0: 0},
		Blocks: []MetaBlock{{Offset: 0, CSize: len(entries), USize: len(entries)}},
	}, dirListing{StartBlock: 0, Offset: 0, Size: uint32(len(entries)) + 3}
}

// dirEntryBytes assembles a header covering one entry, the shape everything in
// the refusal table starts from.
func dirEntryBytes(name string, count uint32, nameLen int) []byte {
	buf := make([]byte, 12+8+len(name))
	le := binary.LittleEndian
	le.PutUint32(buf[0:], count) // stored one less than the true count
	le.PutUint32(buf[4:], 0)     // inode table block
	le.PutUint32(buf[8:], 1)     // base inode number
	le.PutUint16(buf[12:], 0)    // inode offset within the block
	le.PutUint16(buf[14:], 0)    // inode number delta
	le.PutUint16(buf[16:], inodeTypeFile)
	le.PutUint16(buf[18:], uint16(nameLen-1)) // stored one less
	copy(buf[20:], name)
	return buf
}

// TestReadDirListingRefusals covers the bounds checks. Each of these is a
// malformed listing that, unchecked, would either read past the metadata region
// or produce a path the matcher would key on -- and since the listing arrives
// from an image the generator is being asked to trust, the difference between a
// refusal and a wrong answer is the check itself.
func TestReadDirListingRefusals(t *testing.T) {
	var im SquashfsImage

	t.Run("well-formed", func(t *testing.T) {
		meta, list := synthListing(t, dirEntryBytes("file.txt", 0, len("file.txt")))
		got, err := im.readDirListing(meta, 0, list)
		if err != nil {
			t.Fatalf("a well-formed listing was refused: %v", err)
		}
		if len(got) != 1 || got[0].name != "file.txt" {
			t.Fatalf("read %+v, want one entry named file.txt", got)
		}
	})

	t.Run("name runs past the listing", func(t *testing.T) {
		// The declared name is longer than the bytes that follow it, which is
		// the one overrun that would hand the caller somebody else's metadata
		// as a filename.
		meta, list := synthListing(t, dirEntryBytes("file.txt", 0, len("file.txt")+40))
		if _, err := im.readDirListing(meta, 0, list); err == nil {
			t.Fatal("an over-long name was accepted")
		}
	})

	t.Run("count runs past the listing", func(t *testing.T) {
		meta, list := synthListing(t, dirEntryBytes("file.txt", 4, len("file.txt")))
		if _, err := im.readDirListing(meta, 0, list); err == nil {
			t.Fatal("a header claiming five entries in a one-entry listing was accepted")
		}
	})

	t.Run("truncated header", func(t *testing.T) {
		meta, list := synthListing(t, dirEntryBytes("file.txt", 0, len("file.txt"))[:8])
		if _, err := im.readDirListing(meta, 0, list); err == nil {
			t.Fatal("an 8-byte listing was accepted")
		}
	})

	t.Run("separator in a name", func(t *testing.T) {
		// A name holding a separator would make two different trees produce the
		// same path, which is precisely what the matcher must not be fed.
		meta, list := synthListing(t, dirEntryBytes("a/b", 0, len("a/b")))
		if _, err := im.readDirListing(meta, 0, list); err == nil {
			t.Fatal("a name containing a separator was accepted")
		}
	})

	t.Run("dot name", func(t *testing.T) {
		meta, list := synthListing(t, dirEntryBytes("..", 0, len("..")))
		if _, err := im.readDirListing(meta, 0, list); err == nil {
			t.Fatal("a listing naming its own parent was accepted")
		}
	})

	t.Run("empty directory", func(t *testing.T) {
		// file_size 3 with no bytes stored is how an empty directory is
		// expressed, and it has to read as empty rather than as an error.
		meta, _ := synthListing(t, nil)
		got, err := im.readDirListing(meta, 0, dirListing{StartBlock: 0, Offset: 0, Size: 3})
		if err != nil {
			t.Fatalf("an empty directory was refused: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("an empty directory yielded %d entries", len(got))
		}
	})

	t.Run("size below the empty-directory minimum", func(t *testing.T) {
		meta, _ := synthListing(t, nil)
		if _, err := im.readDirListing(meta, 0, dirListing{StartBlock: 0, Offset: 0, Size: 2}); err == nil {
			t.Fatal("a listing declaring two bytes was accepted")
		}
	})

	t.Run("unknown block", func(t *testing.T) {
		meta, _ := synthListing(t, nil)
		if _, err := im.readDirListing(meta, 0, dirListing{StartBlock: 8192, Offset: 0, Size: 3}); err == nil {
			t.Fatal("a listing in a block outside the region was accepted")
		}
	})
}
