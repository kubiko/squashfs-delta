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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
)

// These tests run over every compressor the build implements rather than a fixed
// list, so a new one is covered the moment it registers itself -- and a build
// without cgo tests exactly what it supports instead of skipping.
//
// What they hold is the whole contract: that this code reproduces mksquashfs's
// own blocks (the delta's own verify pass is that check), that a delta over such
// an image applies byte for byte, and that a compressor whose blocks are not
// self-delimiting really does carry the block sizes a window needs.

// implementedCompressorIDs lists the ids this build registered, in a stable
// order so failures name the same subtest run to run.
func implementedCompressorIDs() []uint16 {
	ids := make([]uint16, 0, len(compressorFactories))
	for id := range compressorFactories {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// mksquashfsArgsForComp is snapd's own option set with the compressor swapped,
// which is the only thing that differs about a snap built with another one.
func mksquashfsArgsForComp(t *testing.T, name string, extra ...string) []string {
	t.Helper()
	out := append([]string{}, snapdMksquashfsArgs...)
	for i, a := range out {
		if a == "-comp" {
			out[i+1] = name
			return append(out, extra...)
		}
	}
	t.Fatalf("snapdMksquashfsArgs no longer passes -comp: %v", out)
	return nil
}

// requireCompressor skips unless all three halves of the pair are available: a
// local mksquashfs that can write the compressor, a compressor here that can be
// used at all, and agreement between the two on the bytes. A machine missing any
// of them is not a failing machine -- what it cannot do is produce a delta, which
// is what the generator's own gate reports there.
func requireCompressor(t *testing.T, id uint16) {
	t.Helper()
	requireTools(t, "mksquashfs", "hdiffz", "hpatchz")
	if _, err := newCompressor(id, 0); err != nil {
		t.Skipf("cannot use the %s compressor here: %v", compressorName(id), err)
	}
	if !mksquashfsWritesComp(t, compressorName(id)) {
		t.Skipf("the local mksquashfs cannot write %s images", compressorName(id))
	}
	if bad := unreproducedBy(t, id); bad != "" {
		t.Skipf("the %s library available here does not reproduce what the local mksquashfs writes, "+
			"so no delta of such an image can be made on this machine: %s", compressorName(id), bad)
	}
}

// unreproducedBy packs an image with the compressor and runs the recompression
// gate over it, returning the first mismatch or "" when every block matched.
//
// This is the one thing about a compressor that cannot be assumed and is not a
// property of this code: mksquashfs compresses with the library it was linked
// against and this code with the one it finds (see dynlib.go), and for zstd the
// two versions do not agree -- level 15 output drifted between 1.4.8 and 1.5.7 on
// short inputs, which is every metadata block. Where they disagree, refusing is
// the whole answer, so a test that needs the pair skips rather than fails.
func unreproducedBy(t *testing.T, id uint16) string {
	t.Helper()
	img := buildImageArgs(t, "comp-probe.snap", populateChurn, mksquashfsArgsForComp(t, compressorName(id))...)
	r, err := runSelftest(context.Background(), img, 0, 0)
	if err != nil {
		return err.Error()
	}
	switch {
	case r.clean():
		return ""
	case r.FirstBad != "":
		return r.FirstBad
	default:
		return fmt.Sprintf("%d/%d data and %d/%d metadata blocks reproduced",
			r.DataOK, r.DataChecked, r.MetaOK, r.MetaChecked)
	}
}

// mksquashfsWritesComp reports whether the local mksquashfs was built with a
// compressor, by packing a one-file tree with it. There is no way to ask: the
// -help output lists the compressors compiled in, but its wording has changed
// across releases, and packing is what the answer is needed for anyway.
func mksquashfsWritesComp(t *testing.T, name string) bool {
	t.Helper()
	dir := t.TempDir()
	tree := filepath.Join(dir, "probe")
	if err := os.MkdirAll(tree, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tree, "f"), []byte("probe\n"), 0644); err != nil {
		t.Fatal(err)
	}
	bin, err := toolPath("mksquashfs")
	if err != nil {
		return false
	}
	img := filepath.Join(dir, "probe.img")
	args := append([]string{tree, img}, mksquashfsArgsForComp(t, name)...)
	return exec.Command(bin, args...).Run() == nil
}

// TestCompressorRoundTripsBlocks is the unit-level contract: whatever
// CompressBlocks produces, DecompressBlocks turns back into the same plaintext,
// with each block's length reported and a raw-stored block handled by the caller
// rather than by the decompressor.
func TestCompressorRoundTripsBlocks(t *testing.T) {
	ctx := context.Background()
	for _, id := range implementedCompressorIDs() {
		t.Run(compressorName(id), func(t *testing.T) {
			comp, err := newCompressor(id, 0)
			if err != nil {
				t.Skipf("cannot use the %s compressor here: %v", compressorName(id), err)
			}
			// A compressible block, a partial tail and a block nothing can
			// shrink, which is the one that comes back raw.
			blocks := [][]byte{
				compressibleText(testBlockSize, "round"),
				compressibleText(5000, "tail"),
				incompressible(4096, 3),
			}
			var plain []byte
			uSizes := make([]int, len(blocks))
			for i, b := range blocks {
				plain = append(plain, b...)
				uSizes[i] = len(b)
			}

			var stored []byte
			var cSizes []int
			rawAt := -1
			err = comp.CompressBlocks(ctx, plainBytes(plain), uSizes, testBlockSize, func(idx int, blk CompressedBlock) error {
				if blk.USize != uSizes[idx] {
					t.Errorf("block %d reports %d bytes of plaintext, want %d", idx, blk.USize, uSizes[idx])
				}
				if blk.Raw {
					if !bytes.Equal(blk.OnDisk, blocks[idx]) {
						t.Errorf("block %d is stored raw but its bytes are not the plaintext", idx)
					}
					rawAt = idx
					return nil
				}
				if blk.OnDiskLen() >= blk.USize {
					t.Errorf("block %d is stored compressed at %d bytes for %d of plaintext",
						idx, blk.OnDiskLen(), blk.USize)
				}
				stored = append(stored, blk.OnDisk...)
				cSizes = append(cSizes, blk.OnDiskLen())
				return nil
			})
			if err != nil {
				t.Fatalf("compressing: %v", err)
			}
			if rawAt != 2 {
				t.Errorf("the incompressible block was stored raw at index %d, want 2", rawAt)
			}

			// Only the compressed blocks go back through the decompressor; a raw
			// block's bytes are its plaintext and the image's callers splice
			// them in, which is the same split walkMetaRegion makes.
			out, gotU, err := comp.DecompressBlocks(ctx, nil, stored, cSizes, testBlockSize)
			if err != nil {
				t.Fatalf("decompressing: %v", err)
			}
			if len(gotU) != len(cSizes) {
				t.Fatalf("decompressed %d blocks, gave it %d", len(gotU), len(cSizes))
			}
			want := plain[:len(blocks[0])+len(blocks[1])]
			if !bytes.Equal(out, want) {
				t.Errorf("the round trip differs from the plaintext at offset %d", firstDiff(out, want))
			}
			for i, u := range gotU {
				if u != uSizes[i] {
					t.Errorf("block %d came back as %d bytes, want %d", i, u, uSizes[i])
				}
			}

			// DecompressTo has to agree with DecompressBlocks, since the applier
			// uses it for source windows and nothing else checks it.
			var streamed bytes.Buffer
			n, err := comp.DecompressTo(ctx, &streamed, bytes.NewReader(stored), cSizes, testBlockSize, len(want))
			if err != nil {
				t.Fatalf("streaming decompression: %v", err)
			}
			if n != int64(len(want)) || !bytes.Equal(streamed.Bytes(), want) {
				t.Errorf("streaming decompression produced %d bytes that differ from the buffered %d", n, len(want))
			}

			// The section codec, which is what carries the instruction stream.
			blob := compressibleText(40000, "blob")
			sec, err := comp.CompressBlob(ctx, blob)
			if err != nil {
				t.Fatalf("compressing a section blob: %v", err)
			}
			if len(sec) >= len(blob) {
				t.Errorf("a compressible blob came back as %d bytes for %d", len(sec), len(blob))
			}
			back, err := decompressBlob(ctx, comp.SectionCodec(), sec, len(blob))
			if err != nil {
				t.Fatalf("decompressing a section blob: %v", err)
			}
			if !bytes.Equal(back, blob) {
				t.Errorf("the section blob round trip differs at offset %d", firstDiff(back, blob))
			}
		})
	}
}

// TestCompressorReproducesRealImages is the end-to-end gate per compressor: an
// image built by mksquashfs, a delta generated against it -- whose verify pass
// reassembles the target and compares it byte for byte -- and an apply that has
// to produce the target again from the source alone.
//
// The fixture is the churn pair rather than a small edit, so a patch run is
// emitted: for a compressor whose blocks are not self-delimiting that is what
// exercises SrcWindow.CSizes, and nothing else in the suite would.
func TestCompressorReproducesRealImages(t *testing.T) {
	ctx := context.Background()
	for _, id := range implementedCompressorIDs() {
		t.Run(compressorName(id), func(t *testing.T) {
			requireCompressor(t, id)
			args := mksquashfsArgsForComp(t, compressorName(id))
			source := buildImageArgs(t, "comp-source.snap", populateChurn, args...)
			target := buildImageArgs(t, "comp-target.snap", func(t *testing.T, dir string) {
				populateChurn(t, dir)
				churnEdit(t, dir)
			}, args...)

			for _, path := range []string{source, target} {
				im, err := openSquashfsImage(path)
				if err != nil {
					t.Fatalf("%s: %v", filepath.Base(path), err)
				}
				if im.SB.CompressionId != id {
					t.Fatalf("%s was built with %s, wanted %s",
						filepath.Base(path), compressorName(im.SB.CompressionId), compressorName(id))
				}
				if err := im.checkSupported(); err != nil {
					t.Fatalf("%s was refused: %v", filepath.Base(path), err)
				}
			}

			delta := filepath.Join(t.TempDir(), "comp.delta")
			// No Comp: the generator derives it from the target's superblock,
			// which is the path a real run takes.
			stats, err := generateBlockPlan(ctx, source, target, delta, blockPlanGenOpts{Verify: true})
			if err != nil {
				t.Fatalf("generating a %s delta: %v", compressorName(id), err)
			}
			if stats.PatchRuns == 0 {
				t.Fatalf("no patch run was emitted, so source windows went untested "+
					"(%d no window, %d not worth it, %d failed verify)",
					stats.RunsNoWindow, stats.RunsTooExpensive, stats.RunsVerifyFailed)
			}
			if stats.CopiedBytes == 0 {
				t.Error("nothing was copied verbatim, so the compressor rebuilt the whole image")
			}
			// Applying with no Comp either: it comes from SEC_SB.
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
			if _, err := applyBlockPlan(ctx, src, df, &got, blockPlanApplyOpts{}); err != nil {
				t.Fatalf("applying a %s delta: %v", compressorName(id), err)
			}
			want, err := os.ReadFile(target)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("the reconstruction differs from the target at offset %d", firstDiff(got.Bytes(), want))
			}
		})
	}
}

// TestCompressorRefusesMismatchedOverride holds the override check: a caller may
// name a compressor, but not one the image contradicts, because the blocks it
// produced would be valid and wrong.
func TestCompressorRefusesMismatchedOverride(t *testing.T) {
	for _, id := range implementedCompressorIDs() {
		comp, err := newCompressor(id, 0)
		if err != nil {
			continue
		}
		if err := checkCompressorMatches(comp, id); err != nil {
			t.Errorf("%s rejected its own id: %v", compressorName(id), err)
		}
		for _, other := range implementedCompressorIDs() {
			if other == id {
				continue
			}
			if err := checkCompressorMatches(comp, other); err == nil {
				t.Errorf("a %s compressor was accepted for a %s image",
					compressorName(id), compressorName(other))
			}
		}
	}
}
