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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// applyWithBudget applies a delta under a stated memory budget, returning what it
// managed to write as well as the outcome -- because for a refusal the point is
// that it wrote nothing.
func applyWithBudget(t *testing.T, source, delta string, budget int) (*applyStats, []byte, error) {
	t.Helper()
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
	stats, err := applyBlockPlan(context.Background(), src, df, &got, blockPlanApplyOpts{
		Comp:        &xzCLI{},
		MaxRunUSize: budget,
	})
	return stats, got.Bytes(), err
}

// TestApplyNegotiatesRunCap is the trade the format offers a device with a memory
// budget. A patch run needs its plaintext and its source window in scratch at
// once, so the header's run cap sets the whole apply's memory demand -- and an
// applier that cannot afford what a delta declares must say so before reading,
// writing or forking anything. Falling back to a full download is a far better
// outcome than failing part-way through assembling an image.
func TestApplyNegotiatesRunCap(t *testing.T) {
	ctx := context.Background()
	source, target := churnPair(t)
	delta := filepath.Join(t.TempDir(), "negotiated.delta")

	const cap4 = 4 * 131072
	gen, err := generateBlockPlan(ctx, source, target, delta, blockPlanGenOpts{
		Comp: &xzCLI{}, Verify: true, MaxRunUSize: cap4,
	})
	if err != nil {
		t.Fatalf("generating under a %d-byte run cap: %v", cap4, err)
	}
	if gen.PatchRuns == 0 {
		t.Fatal("no patch run was emitted, so there is no memory demand to negotiate over")
	}

	// A budget one byte below what the delta declares must refuse, and must not
	// have written any of the image on the way to finding out.
	_, out, err := applyWithBudget(t, source, delta, cap4-1)
	if err == nil {
		t.Fatalf("a delta declaring a %d-byte run cap was accepted by an applier allowing %d", cap4, cap4-1)
	}
	if len(out) != 0 {
		t.Errorf("the refusal still wrote %d bytes of image, so it was not decided up front", len(out))
	}
	// Both numbers have to appear: which side was too small is what decides the
	// caller's next move.
	if msg := err.Error(); !strings.Contains(msg, humanBytes(cap4)) || !strings.Contains(msg, humanBytes(cap4-1)) {
		t.Errorf("the refusal does not name both sizes: %v", err)
	}

	// A budget exactly at the declared cap is affordable, and the scratch the
	// apply actually used has to come in under it -- otherwise the number the
	// negotiation is conducted in does not bound anything.
	st, got, err := applyWithBudget(t, source, delta, cap4)
	if err != nil {
		t.Fatalf("a delta was refused by an applier whose budget exactly matches it: %v", err)
	}
	want, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the reconstruction differs from the target at offset %d", firstDiff(got, want))
	}
	if st.PeakScratchBytes == 0 {
		t.Fatal("a delta with patch runs reported no scratch, so the measurement is not wired up")
	}
	// The decoder bounds a run at the cap and its windows at twice that, and the
	// patch cannot exceed the delta that carries it. Those three are what the
	// scratch files hold at once.
	if bound := int64(3*cap4) + gen.DeltaSize; st.PeakScratchBytes > bound {
		t.Errorf("one run held %s of scratch under a %s cap, above the %s the format bounds it to",
			humanBytes(st.PeakScratchBytes), humanBytes(cap4), humanBytes(bound))
	}

	// Zero means "whatever the delta asks for", which is what a caller with no
	// budget of its own wants.
	if _, _, err := applyWithBudget(t, source, delta, 0); err != nil {
		t.Errorf("an applier with no stated budget refused a delta: %v", err)
	}
}

// TestRunCapBoundsScratch is the other half: the cap is not merely declared and
// checked, it is what the apply's peak memory actually follows. Halving it has to
// halve what one run holds, or the negotiation is theatre.
func TestRunCapBoundsScratch(t *testing.T) {
	ctx := context.Background()
	source, target := churnPair(t)
	dir := t.TempDir()

	scratchAt := func(cap int) int64 {
		t.Helper()
		delta := filepath.Join(dir, "cap.delta")
		if _, err := generateBlockPlan(ctx, source, target, delta, blockPlanGenOpts{
			Comp: &xzCLI{}, MaxRunUSize: cap,
		}); err != nil {
			t.Fatalf("generating under a %d-byte run cap: %v", cap, err)
		}
		st, got, err := applyWithBudget(t, source, delta, cap)
		if err != nil {
			t.Fatalf("applying under a %d-byte run cap: %v", cap, err)
		}
		want, err := os.ReadFile(target)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("a %d-byte cap reconstructed the wrong image at offset %d", cap, firstDiff(got, want))
		}
		return st.PeakScratchBytes
	}

	wide := scratchAt(8 * 131072)
	narrow := scratchAt(2 * 131072)
	if narrow >= wide {
		t.Errorf("a two-block cap held %s of scratch against %s for an eight-block cap, so the cap does not bound memory",
			humanBytes(narrow), humanBytes(wide))
	}
}

// TestCompareWriter covers the generator's final gate directly. It compares the
// reconstruction as it arrives rather than buffering it, so its failure cases are
// the only thing standing between a bad delta and a published one -- and unlike
// the gate as a whole, they can be provoked exactly.
func TestCompareWriter(t *testing.T) {
	want := []byte("the reconstruction must match this exactly")

	t.Run("streams in pieces", func(t *testing.T) {
		c := &compareWriter{want: want}
		for i := 0; i < len(want); i += 7 {
			end := min(i+7, len(want))
			if n, err := c.Write(want[i:end]); err != nil || n != end-i {
				t.Fatalf("writing [%d,%d): %d bytes, %v", i, end, n, err)
			}
		}
		if c.at != len(want) {
			t.Errorf("consumed %d bytes of %d", c.at, len(want))
		}
	})

	t.Run("catches a difference", func(t *testing.T) {
		c := &compareWriter{want: want}
		bad := append([]byte(nil), want...)
		bad[20] ^= 0x20
		if _, err := c.Write(bad); err == nil {
			t.Fatal("a differing reconstruction was accepted")
		} else if !strings.Contains(err.Error(), "offset 20") {
			t.Errorf("the difference was not located at offset 20: %v", err)
		}
	})

	t.Run("catches overrun", func(t *testing.T) {
		c := &compareWriter{want: want}
		if _, err := c.Write(append(append([]byte(nil), want...), '!')); err == nil {
			t.Fatal("a reconstruction longer than the target was accepted")
		}
	})

	t.Run("leaves a short stream short", func(t *testing.T) {
		// Nothing here fails: a truncated reconstruction is only detectable once
		// the writing stops, which is why the gate checks `at` afterwards rather
		// than relying on Write alone.
		c := &compareWriter{want: want}
		if _, err := c.Write(want[:10]); err != nil {
			t.Fatal(err)
		}
		if c.at == len(want) {
			t.Error("a 10-byte prefix was counted as the whole target")
		}
	})
}

// TestReusableMemFD covers both backings of the scratch files a patch run holds
// its window, patch and plaintext in. The memfd path is what every apply on a
// current kernel takes; the disk path exists for a kernel without
// memfd_create, where it is the difference between a working apply and none --
// and where, until this test, nothing established that it worked at all. Both
// have to offer the same three things: a Path a child process can open, a Reset
// that truly empties the file, and a Close that leaves nothing behind.
func TestReusableMemFD(t *testing.T) {
	backings := []struct {
		name   string
		open   func(string) (*ReusableMemFD, error)
		onDisk bool
	}{
		{"memfd", NewReusableMemFD, false},
		{"disk fallback", newDiskBackedFD, true},
	}

	for _, b := range backings {
		t.Run(b.name, func(t *testing.T) {
			m, err := b.open("scratch")
			if err != nil {
				t.Fatalf("creating a %s scratch file: %v", b.name, err)
			}
			defer m.Close()

			if m.isDiskFile != b.onDisk {
				t.Errorf("isDiskFile is %v, so Close will clean up the wrong way", m.isDiskFile)
			}

			// Path is how xz and hpatchz are handed the scratch file, so it has to
			// be openable by name and not merely by descriptor.
			if _, err := os.Stat(m.Path); err != nil {
				t.Fatalf("the path handed to child processes does not resolve: %v", err)
			}

			payload := []byte("scratch contents that a patch run would hold")
			if _, err := m.File.Write(payload); err != nil {
				t.Fatalf("writing: %v", err)
			}
			// Reading back through Path is the child's view, which is the one that
			// matters -- a descriptor-only file would pass a Read on m.File and
			// still fail an xz invocation.
			got, err := os.ReadFile(m.Path)
			if err != nil {
				t.Fatalf("reading back through the path: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Errorf("read back %q, want %q", got, payload)
			}

			// Reset has to leave an empty file with the cursor at 0: reuse across
			// runs is the whole point, and a stale tail would silently corrupt the
			// next run's window.
			if err := m.Reset(); err != nil {
				t.Fatalf("Reset: %v", err)
			}
			if fi, err := os.Stat(m.Path); err != nil {
				t.Fatal(err)
			} else if fi.Size() != 0 {
				t.Errorf("Reset left %d bytes behind", fi.Size())
			}
			shorter := []byte("shorter")
			if _, err := m.File.Write(shorter); err != nil {
				t.Fatalf("writing after Reset: %v", err)
			}
			if got, err := os.ReadFile(m.Path); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(got, shorter) {
				t.Errorf("after Reset the file holds %q, want %q -- the cursor did not rewind", got, shorter)
			}

			// Close must not leave a temp file on a device that may have very little
			// room. The memfd case has nothing to unlink; the disk case does.
			path := m.Path
			if err := m.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if b.onDisk {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Errorf("Close left %s on disk (stat error %v)", path, err)
				}
			}
		})
	}
}
