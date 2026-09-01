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
	"path/filepath"
	"testing"
)

// TestNormalizeDigitsAgreesWithFuzzyMatch pins the equivalence the bucket index
// rests on: two paths land in the same bucket exactly when pathsMatchFuzzy
// accepts them. If they ever diverged, the matcher would silently stop finding
// version bumps -- a bucket miss is not an error, it is just a worse delta -- so
// the agreement is asserted rather than assumed.
func TestNormalizeDigitsAgreesWithFuzzyMatch(t *testing.T) {
	pairs := []struct {
		a, b  string
		match bool
	}{
		{"usr/lib/libdemo.so.1.2.3", "usr/lib/libdemo.so.1.2.3", true},
		{"usr/lib/libdemo.so.1.2.3", "usr/lib/libdemo.so.1.2.4", true},
		// A digit run collapses to one placeholder, so a version going from one
		// digit to two still matches -- which is the 9 -> 10 case every project
		// eventually hits.
		{"usr/lib/libdemo.so.1.2.9", "usr/lib/libdemo.so.1.2.10", true},
		{"usr/lib/python3.12/x.py", "usr/lib/python3.13/x.py", true},
		{"usr/lib/python3.12/x.py", "usr/lib/python3.12/y.py", false},
		// Same name, different depth: a file that moved between directories is
		// not something either side is prepared to call a match.
		{"usr/lib/x.so", "usr/local/lib/x.so", false},
		// A digit appearing where there was none is not a version bump: only a
		// digit run changing value is, so tool and tool2 stay distinct files.
		{"bin/tool", "bin/tool2", false},
		{"bin/tool2", "bin/tool3", true},
		{"bin/tool", "bin/other", false},
		{"a/b/c", "a/b/c", true},
		{"", "", true},
	}
	for _, p := range pairs {
		gotFuzzy := pathsMatchFuzzy(p.a, p.b) != 0
		gotNorm := normalizeDigits(p.a) == normalizeDigits(p.b)
		if gotFuzzy != p.match {
			t.Errorf("pathsMatchFuzzy(%q, %q) says %v, want %v", p.a, p.b, gotFuzzy, p.match)
		}
		if gotNorm != gotFuzzy {
			t.Errorf("%q and %q: the bucket says %v but the fuzzy match says %v (%q vs %q)",
				p.a, p.b, gotNorm, gotFuzzy, normalizeDigits(p.a), normalizeDigits(p.b))
		}
	}
}

func TestSizeSimilarity(t *testing.T) {
	cases := []struct {
		a, b int64
		want int
	}{
		{100, 100, 100},
		{0, 0, 100},
		{0, 100, 0},
		{100, 0, 0},
		{50, 100, 50},
		{100, 50, 50},
		{99, 100, 99},
		// The defect this replaces: getSimilarityScore rounds the ratio before
		// scaling it, so it answers 100 here. Ranking candidates needs the
		// difference between "nearly the same file" and "half the size".
		{75, 100, 75},
	}
	for _, c := range cases {
		if got := sizeSimilarity(c.a, c.b); got != c.want {
			t.Errorf("sizeSimilarity(%d, %d) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

// TestFileLayoutUOffAt covers the step from a plaintext offset to a source
// offset, holes and short files included.
func TestFileLayoutUOffAt(t *testing.T) {
	// A file with a hole in the middle: the second block's plaintext starts at
	// 200 even though it directly follows the first on disk.
	f := &fileLayout{
		Path:  "x",
		USize: 300,
		Blocks: []FileBlock{
			{Extent: Extent{Offset: 1000, CSize: 40, USize: 100}, UOff: 0},
			{Extent: Extent{Offset: 1040, CSize: 50, USize: 100}, UOff: 200},
		},
	}
	cases := []struct {
		uOff int64
		want int64
	}{
		{0, 1000},
		{99, 1000},
		// Inside the hole. The next stored block is the honest answer: the hole
		// itself has no bytes to diff against.
		{150, 1040},
		{200, 1040},
		{299, 1040},
		// Past the end, which is what a file that grew looks like from the
		// target's side. The last block is as close as the source gets.
		{5000, 1040},
	}
	for _, c := range cases {
		got, ok := f.uOffAt(c.uOff)
		if !ok {
			t.Errorf("uOffAt(%d) found nothing", c.uOff)
			continue
		}
		if got != c.want {
			t.Errorf("uOffAt(%d) = %d, want %d", c.uOff, got, c.want)
		}
	}
	// A wholly sparse or empty file has nothing to anchor on, and saying so is
	// what sends the caller to its fallback rather than to offset zero.
	empty := &fileLayout{Path: "hole", USize: 4096}
	if off, ok := empty.uOffAt(0); ok {
		t.Errorf("a file with no blocks yielded an anchor at %d", off)
	}
}

// TestAnchorPrefersExactPath drives the decision table directly, without an
// image: which of the two indexes answers, and when neither does.
func TestAnchorPrefersExactPath(t *testing.T) {
	block := func(off int64, uOff int64) FileBlock {
		return FileBlock{Extent: Extent{Offset: off, CSize: 100, USize: 1000}, UOff: uOff}
	}
	src := []*fileLayout{
		{Path: "usr/lib/libdemo.so.1.2.3", USize: 2000, Blocks: []FileBlock{block(500, 0), block(600, 1000)}},
		{Path: "bin/tool", USize: 1000, Blocks: []FileBlock{block(700, 0)}},
		// Same normalized path as the target's libdemo.so.1.2.4 below, but a
		// fifth of the size: a coincidence, not a version bump.
		{Path: "usr/lib/libother.so.9", USize: 400, Blocks: []FileBlock{block(800, 0)}},
		// Present under the right name but holding no blocks, which is what a
		// file truncated to a hole looks like.
		{Path: "var/emptied.bin", USize: 0},
	}
	m := &pathMatcher{
		owner:  map[int64]blockOwner{},
		byPath: map[string]*fileLayout{},
		byNorm: map[string][]*fileLayout{},
	}
	for _, f := range src {
		m.byPath[f.Path] = f
		norm := normalizeDigits(f.Path)
		m.byNorm[norm] = append(m.byNorm[norm], f)
	}
	tgt := map[int64]blockOwner{
		// Exact path, second block: must land on the source's second block.
		10000: {file: &fileLayout{Path: "usr/lib/libdemo.so.1.2.3", USize: 2000}, uOff: 1000},
		// A version bump of the same library.
		20000: {file: &fileLayout{Path: "usr/lib/libdemo.so.1.2.4", USize: 2100}, uOff: 0},
		// Same shape as libother.so.9 but nowhere near its size.
		30000: {file: &fileLayout{Path: "usr/lib/libother.so.8", USize: 4000}, uOff: 0},
		// A file the source does not have at all.
		40000: {file: &fileLayout{Path: "usr/lib/libnew.so", USize: 1000}, uOff: 0},
		// Named in the source, but that copy has no blocks to diff against.
		50000: {file: &fileLayout{Path: "var/emptied.bin", USize: 9000}, uOff: 0},
	}
	for off, own := range tgt {
		m.owner[off] = own
	}

	cases := []struct {
		tgtOff   int64
		wantOff  int64
		wantKind anchorKind
	}{
		{10000, 600, anchorPath},
		{20000, 500, anchorFuzzy},
		{30000, 0, anchorNone},
		{40000, 0, anchorNone},
		{50000, 0, anchorNone},
		// A block belonging to no file -- the walk missed its inode -- has no
		// correspondence to offer.
		{99999, 0, anchorNone},
	}
	for _, c := range cases {
		gotOff, gotKind := m.anchor(c.tgtOff)
		if gotKind != c.wantKind {
			t.Errorf("anchor(%d) kind = %v, want %v", c.tgtOff, gotKind, c.wantKind)
		}
		if gotKind != anchorNone && gotOff != c.wantOff {
			t.Errorf("anchor(%d) = %d, want %d", c.tgtOff, gotOff, c.wantOff)
		}
	}
	// A nil matcher stands for an image whose directory table would not walk,
	// and has to answer rather than fault: generation continues without it.
	var absent *pathMatcher
	if _, kind := absent.anchor(10000); kind != anchorNone {
		t.Errorf("a nil matcher claimed an anchor of kind %v", kind)
	}
}

// TestPathAnchorBeatsSourceCursor is the milestone's claim, on an image pair
// built to isolate it. One large file is edited near its front, so every
// following block shifts and none can be copied, and the run cap splits it into
// several runs. Offset proximity anchors every one of them at the same stale
// source cursor -- there has been no copy to advance it -- so only the first run
// sees its own plaintext and the rest are diffed against bytes they have nothing
// to do with. Path correspondence gives each run the part of the source file it
// actually came from.
//
// The delta size is the measurement; both modes must still reconstruct the target
// exactly, because an anchor may never affect correctness.
func TestPathAnchorBeatsSourceCursor(t *testing.T) {
	requireTools(t, "mksquashfs", "xz", "hdiffz", "hpatchz")
	ctx := context.Background()

	const size = 4 << 20
	body := semiCompressible(size, 23)
	source := buildImage(t, "anchor-source.snap", func(t *testing.T, dir string) {
		writeFile(t, dir, "data/big.bin", body)
	})
	target := buildImage(t, "anchor-target.snap", func(t *testing.T, dir string) {
		edited := make([]byte, 0, size+4000)
		edited = append(edited, body[:1000]...)
		edited = append(edited, semiCompressible(4000, 77)...)
		edited = append(edited, body[1000:]...)
		writeFile(t, dir, "data/big.bin", edited)
	})

	dir := t.TempDir()
	gen := func(name string, noPathMatch bool) *genStats {
		t.Helper()
		stats, err := generateBlockPlan(ctx, source, target, filepath.Join(dir, name), blockPlanGenOpts{
			Comp: &xzCLI{}, Verify: true,
			// Well below the run's plaintext, so the file is split into several
			// runs and every one after the first has to find its own way back
			// into the source.
			MaxRunUSize: 1 << 20,
			NoPathMatch: noPathMatch,
		})
		if err != nil {
			t.Fatalf("generating with noPathMatch=%v: %v", noPathMatch, err)
		}
		return stats
	}

	offset := gen("offset.delta", true)
	path := gen("path.delta", false)

	if offset.RunsPathAnchored+offset.RunsFuzzyAnchored != 0 {
		t.Errorf("NoPathMatch still used the path map for %d runs",
			offset.RunsPathAnchored+offset.RunsFuzzyAnchored)
	}
	if path.MatchUnavailable != "" {
		t.Fatalf("the path map could not be built: %s", path.MatchUnavailable)
	}
	if path.RunsPathAnchored < 2 {
		t.Errorf("only %d runs were anchored by path (%d by source offset); the file is one path in both images",
			path.RunsPathAnchored, path.RunsCursorAnchored)
	}
	if path.PatchRuns <= offset.PatchRuns {
		t.Errorf("path anchoring produced %d patch runs, offset proximity %d -- the runs it should have rescued "+
			"went to literals instead (%d no window, %d not worth it, %d failed verify)",
			path.PatchRuns, offset.PatchRuns,
			path.RunsNoWindow, path.RunsTooExpensive, path.RunsVerifyFailed)
	}
	if path.DeltaSize >= offset.DeltaSize/2 {
		t.Errorf("path anchoring gave %d bytes against %d for offset proximity, which is not the saving "+
			"a whole-file correspondence should buy", path.DeltaSize, offset.DeltaSize)
	}
	// And the point of the format is still intact: the device compresses the
	// runs' plaintext, not the image's.
	if path.PatchedUBytes > path.TargetUBytes {
		t.Errorf("patch runs ask the device to compress %d bytes of a %d-byte data region",
			path.PatchedUBytes, path.TargetUBytes)
	}
}

// TestPathAnchorFollowsAVersionBump is the fuzzy half: the same content under a
// path whose version changed. Nothing about the file's identity survives except
// its name, and mksquashfs writes it at a different offset, so a match here can
// only have come from the digit-normalized bucket.
func TestPathAnchorFollowsAVersionBump(t *testing.T) {
	requireTools(t, "mksquashfs", "xz", "hdiffz", "hpatchz")
	ctx := context.Background()

	const size = 2 << 20
	body := semiCompressible(size, 41)
	// A filler that exists only in the source, and only to push the library far
	// enough down the source's data region that the run's fallback anchor -- the
	// start of the region, since nothing has been copied yet -- cannot reach it.
	filler := semiCompressible(6<<20, 5)

	source := buildImage(t, "bump-source.snap", func(t *testing.T, dir string) {
		writeFile(t, dir, "aaa-filler.bin", filler)
		writeFile(t, dir, "usr/lib/libdemo.so.1.2.3", body)
	})
	target := buildImage(t, "bump-target.snap", func(t *testing.T, dir string) {
		edited := make([]byte, 0, size+4000)
		edited = append(edited, body[:1000]...)
		edited = append(edited, semiCompressible(4000, 78)...)
		edited = append(edited, body[1000:]...)
		writeFile(t, dir, "usr/lib/libdemo.so.1.2.4", edited)
	})

	dir := t.TempDir()
	stats, err := generateBlockPlan(ctx, source, target, filepath.Join(dir, "bump.delta"), blockPlanGenOpts{
		Comp: &xzCLI{}, Verify: true, MaxRunUSize: 1 << 20,
	})
	if err != nil {
		t.Fatalf("generating: %v", err)
	}
	if stats.RunsFuzzyAnchored == 0 {
		t.Fatalf("no run was anchored on the source's earlier version of the library "+
			"(%d by exact path, %d by source offset)", stats.RunsPathAnchored, stats.RunsCursorAnchored)
	}
	if stats.PatchRuns == 0 {
		t.Errorf("the renamed library shipped as literals (%d no window, %d not worth it, %d failed verify)",
			stats.RunsNoWindow, stats.RunsTooExpensive, stats.RunsVerifyFailed)
	}
	// The library is 2 MiB of semi-compressible bytes, about 1 MiB on disk, and
	// only 4000 bytes of it are new. A delta anywhere near its stored size means
	// the rename lost the correspondence.
	if stats.DeltaSize > 300<<10 {
		t.Errorf("a 4000-byte edit under a bumped version number cost %d bytes of delta", stats.DeltaSize)
	}
}
