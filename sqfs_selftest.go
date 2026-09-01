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
	"strings"
	"time"
)

// The block-plan delta rests on two claims about squashfs images, and this file
// is how they are checked against a real image rather than assumed:
//
//  1. the inode-derived extents tile the data region exactly, and
//  2. recompressing a block's contents reproduces its on-disk bytes byte for
//     byte, so an unchanged block can be copied instead of recompressed.
//
// `selftest` is the gate: if (2) ever fails on a machine, the local compressor
// differs from the one that built the image and no delta of this format may be
// produced there. That is what ToolsVersion in the header exists to invalidate.
// Each image is checked with the compressor its own superblock names, so a
// directory of snaps built with different ones reports one line each.

// selftestResult tallies one image.
type selftestResult struct {
	Path string

	Extents    int
	Gaps       int
	Overlaps   int
	RawExtents int
	PartialEnd int

	DataChecked int
	DataOK      int
	MetaChecked int
	MetaOK      int
	MetaBlocks  int
	MetaRaw     int

	AcctOK  bool
	PadZero bool
	Pad     int64

	FirstBad string
	Elapsed  time.Duration
}

func (r selftestResult) clean() bool {
	return r.Gaps == 0 && r.Overlaps == 0 && r.AcctOK && r.PadZero &&
		r.DataOK == r.DataChecked && r.MetaOK == r.MetaChecked
}

// runSelftest recompresses up to sample data blocks (all of them when sample is
// zero) and every compressible metadata block, comparing against the image.
func runSelftest(ctx context.Context, path string, sample, threads int) (*selftestResult, error) {
	t0 := time.Now()
	im, err := openSquashfsImage(path)
	if err != nil {
		return nil, err
	}
	if err := im.checkSupported(); err != nil {
		return nil, err
	}
	// Per image rather than once for the run: this command is pointed at a
	// directory of snaps, and the whole question it answers is whether each
	// image's own compressor can be reproduced.
	comp, err := newCompressor(im.SB.CompressionId, threads)
	if err != nil {
		return nil, err
	}
	r := &selftestResult{Path: path}

	ext, gaps, overlaps, err := im.CheckCoverage(ctx)
	if err != nil {
		return nil, err
	}
	r.Extents, r.Gaps, r.Overlaps = len(ext), len(gaps), len(overlaps)
	for _, e := range ext {
		if e.Raw {
			r.RawExtents++
		}
		if e.USize != int(im.SB.BlockSize) {
			r.PartialEnd++
		}
	}

	// Byte accounting: the four regions plus padding must add up to the file.
	sb := im.SB
	total := int64(96) + (int64(sb.InodeTableStart) - 96) +
		int64(sb.ExportTableStart-sb.InodeTableStart) +
		int64(sb.BytesUsed-sb.ExportTableStart) +
		(im.Size() - int64(sb.BytesUsed))
	r.AcctOK = total == im.Size()
	r.Pad = im.Size() - int64(sb.BytesUsed)
	r.PadZero = true
	for _, b := range im.Data[sb.BytesUsed:] {
		if b != 0 {
			r.PadZero = false
			break
		}
	}

	// --- data blocks ---
	compressible := make([]Extent, 0, len(ext))
	for _, e := range ext {
		if !e.Raw {
			compressible = append(compressible, e)
		}
	}
	// Stride-sample so a partial run still spans the whole image rather than
	// only its first megabytes.
	chosen := compressible
	if sample > 0 && sample < len(compressible) {
		stride := len(compressible) / sample
		chosen = chosen[:0]
		for i := 0; i < len(compressible) && len(chosen) < sample; i += stride {
			chosen = append(chosen, compressible[i])
		}
	}
	r.DataChecked, r.DataOK, err = verifyBlocks(ctx, im, comp, chosen, int(im.SB.BlockSize), &r.FirstBad)
	if err != nil {
		return nil, err
	}

	// --- metadata blocks ---
	reg, err := im.MetaRegionAll(ctx)
	if err != nil {
		return nil, err
	}
	r.MetaBlocks = len(reg.Blocks)
	var mExt []Extent
	for _, b := range reg.Blocks {
		if b.Raw {
			r.MetaRaw++
			continue
		}
		// A metadata block's payload sits just past its two-byte header,
		// and is otherwise framed identically to a data block.
		mExt = append(mExt, Extent{Offset: b.Offset + 2, CSize: b.CSize, USize: b.USize})
	}
	r.MetaChecked, r.MetaOK, err = verifyBlocks(ctx, im, comp, mExt, squashfsMetadataSize, &r.FirstBad)
	if err != nil {
		return nil, err
	}

	r.Elapsed = time.Since(t0)
	return r, nil
}

// verifyBlocks recompresses each extent's contents and compares the framed
// result against the image, returning how many blocks were checked and how many
// matched. The first mismatch is recorded in firstBad if it is still empty.
func verifyBlocks(ctx context.Context, im *SquashfsImage, comp Compressor, ext []Extent, dictSize int, firstBad *string) (checked, ok int, err error) {
	max := comp.MaxBlocksPerCall()
	for i := 0; i < len(ext); {
		// Bound each batch by block count and by plaintext bytes, so a
		// run of full 128 KiB blocks does not need a gigabyte resident.
		const maxBatchBytes = 64 << 20
		j, bytesIn := i, 0
		for ; j < len(ext) && j-i < max; j++ {
			if j > i && bytesIn+ext[j].USize > maxBatchBytes {
				break
			}
			bytesIn += ext[j].USize
		}
		batch := ext[i:j]

		plain, err := im.DecompressExtents(ctx, batch)
		if err != nil {
			return checked, ok, err
		}
		uSizes := make([]int, len(batch))
		for k, e := range batch {
			uSizes[k] = e.USize
		}
		err = comp.CompressBlocks(ctx, plainBytes(plain), uSizes, dictSize, func(idx int, blk CompressedBlock) error {
			e := batch[idx]
			want := im.Data[e.Offset : e.Offset+int64(e.CSize)]
			checked++
			switch {
			case blk.Raw:
				// The image stored this block compressed, so a
				// store-raw verdict here is a real mismatch.
				if *firstBad == "" {
					*firstBad = fmt.Sprintf("block at %d: recompression says store-raw (%d bytes) but the image has %d compressed bytes",
						e.Offset, e.USize, e.CSize)
				}
			case bytes.Equal(blk.OnDisk, want):
				ok++
			case *firstBad == "":
				*firstBad = fmt.Sprintf("block at %d: got %d bytes %x..., want %d bytes %x...",
					e.Offset, len(blk.OnDisk), blk.OnDisk[:min(16, len(blk.OnDisk))],
					len(want), want[:min(16, len(want))])
			}
			return nil
		})
		if err != nil {
			return checked, ok, err
		}
		i = j
	}
	return checked, ok, nil
}

// cmdInspect dumps an image's layout: superblock, region sizes, extent
// statistics and coverage.
func cmdInspect(ctx context.Context, paths []string) error {
	for _, p := range paths {
		im, err := openSquashfsImage(p)
		if err != nil {
			return err
		}
		fmt.Printf("== %s (%d bytes)\n  %s\n", p, im.Size(),
			strings.ReplaceAll(im.SB.String(), "\n", "\n  "))
		if err := im.checkSupported(); err != nil {
			fmt.Printf("  UNSUPPORTED: %v\n\n", err)
			continue
		}
		sb := im.SB
		ext, gaps, overlaps, err := im.CheckCoverage(ctx)
		if err != nil {
			return err
		}
		raw, partial, covered := 0, 0, int64(0)
		for _, e := range ext {
			if e.Raw {
				raw++
			}
			if e.USize != int(sb.BlockSize) {
				partial++
			}
			covered += int64(e.CSize)
		}
		fmt.Printf("  data   [96,%d) = %d bytes, %d extents covering %d\n",
			sb.InodeTableStart, sb.InodeTableStart-96, len(ext), covered)
		fmt.Printf("         raw=%d partial-tail=%d gaps=%d overlaps=%d\n",
			raw, partial, len(gaps), len(overlaps))
		for _, g := range gaps[:min(3, len(gaps))] {
			fmt.Printf("         GAP [%d,%d) %d bytes\n", g.From, g.To, g.To-g.From)
		}
		for _, o := range overlaps[:min(3, len(overlaps))] {
			fmt.Printf("         OVERLAP [%d,%d) %d bytes\n", o.From, o.To, o.To-o.From)
		}
		reg, err := im.MetaRegionAll(ctx)
		if err != nil {
			return err
		}
		mraw := 0
		for _, b := range reg.Blocks {
			if b.Raw {
				mraw++
			}
		}
		fmt.Printf("  meta   [%d,%d) = %d bytes on disk, %d blocks (%d raw), %d bytes uncompressed\n",
			sb.InodeTableStart, sb.ExportTableStart, sb.ExportTableStart-sb.InodeTableStart,
			len(reg.Blocks), mraw, len(reg.Blob))
		// The tree is only used to match a target run against source bytes, so
		// it is reported rather than enforced: an inode the walk misses costs
		// delta size and nothing else.
		tree, err := im.FileTree(reg)
		if err != nil {
			fmt.Printf("  tree   UNWALKABLE: %v\n", err)
		} else {
			inodes, err := im.FileInodes(ctx)
			if err != nil {
				return err
			}
			named := make(map[uint32]bool, len(tree))
			var withBlocks int
			for _, e := range tree {
				named[e.Inode.Number] = true
			}
			for _, fi := range inodes {
				if len(fi.Sizes) != 0 {
					withBlocks++
				}
			}
			missing := 0
			for _, fi := range inodes {
				if !named[fi.Number] {
					missing++
				}
			}
			fmt.Printf("  tree   %d paths over %d file inodes (%d with blocks), %d unnamed\n",
				len(tree), len(inodes), withBlocks, missing)
		}
		fmt.Printf("  tail   [%d,%d) = %d bytes\n  pad    %d bytes\n\n",
			sb.ExportTableStart, sb.BytesUsed, sb.BytesUsed-sb.ExportTableStart,
			im.Size()-int64(sb.BytesUsed))
	}
	return nil
}

// cmdSelftest runs the recompression gate over each image and prints one line
// per image plus a tally, failing if any image is not clean.
func cmdSelftest(ctx context.Context, paths []string, sample, threads int) error {
	hdr := fmt.Sprintf("%-44s%7s %6s %5s %5s %5s %12s %12s %6s %5s",
		"image", "MB", "ext", "raw", "part", "gap/ov", "data", "meta", "acct", "time")
	fmt.Println(hdr)
	fmt.Println(strings.Repeat("-", len(hdr)))
	bad := 0
	for _, p := range paths {
		r, err := runSelftest(ctx, p, sample, threads)
		if err != nil {
			fmt.Printf("%-44s ERROR %v\n", trimName(p), err)
			bad++
			continue
		}
		acct := "ok"
		if !r.AcctOK || !r.PadZero {
			acct = "BAD"
		}
		fmt.Printf("%-44s%7d %6d %5d %5d %5s %12s %12s %6s %4.1fs\n",
			trimName(p), imageMB(p), r.Extents, r.RawExtents, r.PartialEnd,
			fmt.Sprintf("%d/%d", r.Gaps, r.Overlaps),
			fmt.Sprintf("%d/%d", r.DataOK, r.DataChecked),
			fmt.Sprintf("%d/%d", r.MetaOK, r.MetaChecked),
			acct, r.Elapsed.Seconds())
		if !r.clean() {
			bad++
			if r.FirstBad != "" {
				fmt.Printf("    %s\n", r.FirstBad)
			}
		}
	}
	fmt.Printf("\n%d/%d clean\n", len(paths)-bad, len(paths))
	if bad != 0 {
		return fmt.Errorf("%d of %d images failed the selftest", bad, len(paths))
	}
	return nil
}

func trimName(p string) string {
	name := p
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		name = p[i+1:]
	}
	if len(name) > 43 {
		name = name[:43]
	}
	return name
}

func imageMB(p string) int64 {
	st, err := os.Stat(p)
	if err != nil {
		return 0
	}
	return st.Size() >> 20
}
