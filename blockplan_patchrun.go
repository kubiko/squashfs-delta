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
	"sort"
)

// A patch run is how the format pays for the blocks OP_COPY cannot carry.
//
// The trade is explicit. OP_LITERAL ships a changed block's final on-disk bytes,
// costing delta size but zero device CPU. OP_PATCHRUN ships a patch against
// source plaintext instead, which is far smaller, but the device must decompress
// a source window and recompress the run. So a run is only worth emitting when
// the bytes it saves justify the compression it adds -- runCostModel below is
// where that judgement lives, and it is a knob rather than a rule.
//
// Everything structural stays here on the build machine: the generator picks the
// windows, proves the patch reconstructs the plaintext, compresses every block
// and byte-compares it against the real target. A run reaches the delta only
// once this machine has watched it work.

// patchRunTuning is the cost model's parameters.
type patchRunTuning struct {
	// MaxRunUSize caps the plaintext one run reconstructs, bounding the
	// applier's peak memory.
	MaxRunUSize int
	// WindowRatio sizes a source window relative to the run's plaintext. The
	// window has to be big enough to contain the run's unchanged
	// neighbourhood, since that is what the patch matches against.
	WindowRatio float64
	// MinSaving is the fewest bytes a run must save over the equivalent
	// literals to be worth any device compression at all. This one is about
	// process overhead, not the trade: a run costs three forks (hdiffz here,
	// xz -dc and xz on the device), so a trivial gain is not worth having
	// however good its rate looks.
	MinSaving int
	// MinSavingRate is the trade itself: the delta bytes a run must save per
	// byte of plaintext it makes the device compress. A relative
	// patch-to-literal ratio cannot express this -- a run whose patch is 85%
	// of its literals still passes any such ratio while saving 150 KiB for
	// 3 MiB of compression -- so the rate is the primary filter and
	// MaxCostRatio is only a backstop.
	MinSavingRate float64
	// MaxCostRatio is the largest patch-to-literal size ratio still worth
	// compressing for: 0.9 means a run must come in at least 10% under the
	// literals it replaces. A run that fails this is not saving anything
	// worth having regardless of its size.
	MaxCostRatio float64
	// Disabled ships every run as literals. That is the M5 behaviour: the
	// largest delta the format can produce, and the only one that asks the
	// device for no data-block compression whatsoever. It is the baseline the
	// patch runs are measured against.
	Disabled bool
}

// defaultPatchRunTuning is what the sweeps over the snapcraft revision pairs
// settled on. The two knobs came out with quite different characters, and it is
// worth being straight about which is measured and which is merely safe.
func defaultPatchRunTuning(maxRunUSize int) patchRunTuning {
	return patchRunTuning{
		MaxRunUSize: maxRunUSize,
		// Measured. On 8.14.4.post129 -> post194 the ratio trades delta size
		// against window decompression, and it has a knee: 0.5 gives a 16.56 MiB
		// delta for 27.92 MiB decompressed, 1.0 gives 5.74/63.38, 1.5 gives
		// 5.13/98.96, 2.0 gives 4.97/124.81, and 3.0 is worse on *both* counts
		// at 5.00/144.17 -- past the knee a wider window only gives hdiffz more
		// places to find a mediocre match. 1.5 is within 3% of the best delta
		// the ratio can reach while decompressing a fifth less than 2.0 does.
		WindowRatio: 1.5,
		// Load-bearing, and about process overhead rather than the trade: a
		// gadget revision's whole change is a single 10.6 KiB run, which this
		// declines, and the delta is 11.3 KiB either way.
		MinSaving: 16 << 10,
		// A floor rather than a tuner, and the sweeps say where the floor
		// belongs. Read the rate's effect as an exchange: bytes of device
		// compression avoided per byte the delta grows. On 8.13.2.post77 ->
		// 8.14.4.post129, the pair with the most churn, that exchange decays
		// steeply -- 0.02 declines a single run and avoids 54 bytes of
		// compression per delta byte, 0.05 avoids 42, 0.10 avoids 12 and 0.20
		// only 8.8, by which point it is refusing runs worth having (delta
		// 8.21 -> 10.90 MiB to bring compression 85.03 -> 62.48 MiB). On the two
		// quieter pairs 0.02 is inert: post129 -> post194 and post75 -> post77
		// produce the identical delta with the check off, because their runs are
		// all comfortably worth compressing.
		//
		// So 0.02 sits where the check only ever removes outliers -- free on a
		// quiet pair, and on a noisy one paying 0.34% of delta size to drop
		// 1.7% of the device's compression. It is not the rate that minimises
		// delta size (that is 0) nor the one that minimises device CPU (raise it
		// until runs stop); it is the largest rate that was still purely
		// favourable on every pair measured. A caller who wants to trade delta
		// size for CPU in earnest raises it knowing the exchange rate above.
		MinSavingRate: 0.02,
		MaxCostRatio:  0.9,
	}
}

// srcWindowPicker turns a byte range of the source data region into a window of
// whole source blocks the applier can decompress in one pass.
type srcWindowPicker struct {
	src *SquashfsImage
	// ext is every source block, sorted by offset and tiling the data region
	// exactly, which CheckCoverage has already proved.
	ext []Extent
}

func newSrcWindowPicker(src *SquashfsImage, ext []Extent) *srcWindowPicker {
	return &srcWindowPicker{src: src, ext: ext}
}

// window returns a window of source blocks starting at or after lo, holding at
// most maxU bytes of plaintext.
//
// Two constraints come from the applier's side of the contract. The window must
// consist of whole blocks, because it is decompressed as a unit; and every block
// in it must agree on being compressed or being raw, because a single `xz -dc`
// cannot walk past a block that is not an xz stream. Raw blocks are the minority
// (about 7% of blocks in a snap), so in practice they just end a window early.
func (p *srcWindowPicker) window(lo int64, maxU int) (SrcWindow, bool) {
	if maxU <= 0 || len(p.ext) == 0 {
		return SrcWindow{}, false
	}
	// The first block at or after lo. Landing mid-block rounds forward: a
	// partial block cannot be decompressed.
	i := sort.Search(len(p.ext), func(k int) bool { return p.ext[k].Offset >= lo })
	if i == len(p.ext) {
		return SrcWindow{}, false
	}
	first := p.ext[i]
	w := SrcWindow{Off: first.Offset}
	for ; i < len(p.ext); i++ {
		e := p.ext[i]
		if e.Raw != first.Raw {
			break
		}
		// Extents tile the region, so a discontinuity here means the run has
		// ended -- which cannot happen on a verified image, but the check
		// costs nothing and keeps the invariant local.
		if w.Off+int64(w.Len) != e.Offset {
			break
		}
		if w.ULen != 0 && w.ULen+e.USize > maxU {
			break
		}
		w.Len += e.CSize
		w.ULen += e.USize
	}
	if w.Len == 0 {
		return SrcWindow{}, false
	}
	// A run of raw blocks satisfies ULen == Len -- the applier's "already
	// plaintext" invariant -- for free: Extents refuses an image whose raw
	// block occupies a different number of bytes than it holds, so every
	// extent here already has CSize == USize when Raw.
	return w, true
}

// candidateRun is a span of consecutive unmatched target blocks, before the cost
// model has decided how to ship it.
type candidateRun struct {
	// ext is the target's blocks, in ascending offset.
	ext []Extent
	// srcAnchor is where in the source the run's plaintext is expected to
	// live: the source offset just past the block the preceding OP_COPY
	// matched. It is the generator's whole idea of correspondence at this
	// milestone -- consecutive revisions keep near-monotonic layouts, so the
	// bytes bracketed by two matches are the source's version of what changed
	// between them.
	srcAnchor int64
}

// USizeTotal is the plaintext the run reconstructs.
func (r *candidateRun) USizeTotal() int {
	n := 0
	for _, e := range r.ext {
		n += e.USize
	}
	return n
}

// CSizeTotal is what the run would cost as literals.
func (r *candidateRun) CSizeTotal() int {
	n := 0
	for _, e := range r.ext {
		n += e.CSize
	}
	return n
}

// builtRun is a patch run the generator has proved out: the patch reconstructs
// the plaintext, and every block recompresses to the target's exact bytes.
type builtRun struct {
	blocks  []PlanBlock
	windows []SrcWindow
	patch   []byte
}

// buildPatchRun tries to turn a candidate into a patch run. It returns nil, nil
// when the run should ship as literals instead -- because no window was
// available, because a block did not recompress to the target's bytes, or
// because the cost model judged the saving too small to be worth the device's
// compression. None of those are errors: OP_LITERAL is always a correct answer.
func buildPatchRun(ctx context.Context, tgt *SquashfsImage, run *candidateRun, pick *srcWindowPicker,
	tune patchRunTuning, opts blockPlanGenOpts, stats *genStats) (*builtRun, error) {

	if tune.Disabled {
		return nil, nil
	}
	uTotal := run.USizeTotal()
	maxWinU := int(float64(uTotal) * tune.WindowRatio)
	if maxWinU > 2*tune.MaxRunUSize {
		maxWinU = 2 * tune.MaxRunUSize
	}
	// The cost model's necessary condition, evaluated before any work: the best
	// conceivable patch is a zero-byte one, so a run that would fail the model
	// even then cannot pass it. Asking runWorthCompressing itself keeps the two
	// in step -- there is no second inequality here to drift out of agreement --
	// and it saves the two decompressions and the hdiffz that would otherwise be
	// run only to be thrown away. Small runs are the common case: a gadget snap
	// revision is nothing but small runs.
	literal := run.CSizeTotal()
	if !runWorthCompressing(0, literal, uTotal, tune) {
		stats.RunsTooExpensive++
		stats.RunsRejectedBytes += int64(literal)
		return nil, nil
	}

	win, ok := pick.window(run.srcAnchor, maxWinU)
	if !ok {
		stats.RunsNoWindow++
		return nil, nil
	}

	old, err := pick.src.DecompressExtents(ctx, pick.extentsIn(win))
	if err != nil {
		return nil, fmt.Errorf("decompressing the source window at %d: %w", win.Off, err)
	}
	if len(old) != win.ULen {
		return nil, fmt.Errorf("source window at %d decompressed to %d bytes, expected %d",
			win.Off, len(old), win.ULen)
	}
	plain, err := tgt.DecompressExtents(ctx, run.ext)
	if err != nil {
		return nil, fmt.Errorf("decompressing the target run at %d: %w", run.ext[0].Offset, err)
	}
	if len(plain) != uTotal {
		return nil, fmt.Errorf("target run at %d decompressed to %d bytes, expected %d",
			run.ext[0].Offset, len(plain), uTotal)
	}

	patch, err := runHdiffz(ctx, old, plain, opts.HdiffzPath)
	if err != nil {
		return nil, fmt.Errorf("diffing a run of %d blocks: %w", len(run.ext), err)
	}

	// Now the real thing: if the patch does not beat the literals by enough,
	// there is nothing to verify.
	if !runWorthCompressing(len(patch), literal, uTotal, tune) {
		stats.RunsTooExpensive++
		stats.RunsRejectedBytes += int64(literal)
		return nil, nil
	}

	// Per-block verification. The applier can only check that a block's
	// recompressed length matches; the generator has the target image in
	// hand, so it compares the bytes. Any disagreement -- a compressor that
	// does not reproduce mksquashfs's output, a patch that reconstructed
	// something subtly wrong -- downgrades the whole run to literals rather
	// than shipping a delta that fails on the device.
	blocks := make([]PlanBlock, len(run.ext))
	uSizes := make([]int, len(run.ext))
	for i, e := range run.ext {
		blocks[i] = PlanBlock{USize: e.USize, CSize: e.CSize}
		uSizes[i] = e.USize
	}
	mismatch := false
	err = opts.Comp.CompressBlocks(ctx, plainBytes(plain), uSizes, int(tgt.SB.BlockSize),
		func(idx int, blk CompressedBlock) error {
			e := run.ext[idx]
			want := tgt.Data[e.Offset : e.Offset+int64(e.CSize)]
			if blk.Raw != e.Raw || !bytes.Equal(blk.OnDisk, want) {
				mismatch = true
				stats.RunBlockMismatches++
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("recompressing a run of %d blocks: %w", len(run.ext), err)
	}
	if mismatch {
		stats.RunsVerifyFailed++
		stats.RunsRejectedBytes += int64(literal)
		return nil, nil
	}

	return &builtRun{blocks: blocks, windows: []SrcWindow{win}, patch: patch}, nil
}

// extentsIn returns the source blocks a window spans, which is what
// DecompressExtents needs to decode it the same way the applier will.
func (p *srcWindowPicker) extentsIn(w SrcWindow) []Extent {
	i := sort.Search(len(p.ext), func(k int) bool { return p.ext[k].Offset >= w.Off })
	end := w.Off + int64(w.Len)
	j := i
	for ; j < len(p.ext) && p.ext[j].Offset < end; j++ {
	}
	return p.ext[i:j]
}

// runWorthCompressing is the cost model. A patch run costs the device a source
// decompression plus a full recompression of the run's plaintext, so it has to
// buy a reduction in delta size proportionate to the work it adds -- uTotal is
// that work, and it is why the decision cannot be made on delta bytes alone.
func runWorthCompressing(patchLen, literalLen, uTotal int, tune patchRunTuning) bool {
	saving := literalLen - patchLen
	if saving < tune.MinSaving {
		return false
	}
	if uTotal > 0 && float64(saving)/float64(uTotal) < tune.MinSavingRate {
		return false
	}
	return float64(patchLen) <= float64(literalLen)*tune.MaxCostRatio
}
