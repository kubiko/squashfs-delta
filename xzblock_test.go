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
	"hash/crc32"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// xzStreamFor compresses plaintext into a single .xz stream with a forced block
// boundary at each of blockSizes, the way CompressBlocks does, but with the
// thread count and check under the test's control -- those are exactly the two
// framing decisions the splitter has to police.
func xzStreamFor(t *testing.T, threads int, check string, blockSizes []int, plain []byte) []byte {
	t.Helper()
	bin, err := toolPath("xz")
	if err != nil {
		t.Skipf("no xz available: %v", err)
	}
	list := make([]string, len(blockSizes))
	for i, u := range blockSizes {
		list[i] = strconv.Itoa(u)
	}
	cmd := exec.CommandContext(context.Background(), bin,
		"-c", "-q", "--format=xz", "--check="+check,
		fmt.Sprintf("-T%d", threads),
		fmt.Sprintf("--lzma2=preset=6,dict=%d", 131072),
		"--block-list="+strings.Join(list, ","),
		"-")
	cmd.Stdin = bytes.NewReader(plain)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("xz -T%d --check=%s failed: %v\n%s", threads, check, err, stderr.String())
	}
	return out.Bytes()
}

// compressibleBlocks builds plaintext that xz will genuinely shrink, so the
// blocks come back with a compressed size below their uncompressed size and the
// splitter is walking real payloads rather than stored-raw ones.
func compressibleBlocks(sizes []int) []byte {
	var b bytes.Buffer
	for i, n := range sizes {
		pattern := fmt.Sprintf("block %d is filled with repeating text. ", i)
		for b.Len() < cumulative(sizes, i)+n {
			b.WriteString(pattern)
		}
		b.Truncate(cumulative(sizes, i) + n)
	}
	return b.Bytes()
}

func cumulative(sizes []int, upto int) int {
	total := 0
	for _, n := range sizes[:upto] {
		total += n
	}
	return total
}

// TestXZBlockSplitterWalksStream is the streaming half of M8. The splitter
// consumes a running xz process's stdout, which is what keeps a whole image's
// compressed output from having to be buffered in order to reach the index at
// the end -- so it has to recover every block boundary from the headers alone,
// in order, and stop cleanly at the index.
func TestXZBlockSplitterWalksStream(t *testing.T) {
	sizes := []int{4000, 131072, 777}
	plain := compressibleBlocks(sizes)
	stream := xzStreamFor(t, 2, "crc32", sizes, plain)

	split, err := newXZBlockSplitter(bytes.NewReader(stream))
	if err != nil {
		t.Fatalf("opening a well-formed stream: %v", err)
	}
	var payloads [][]byte
	for i := 0; ; i++ {
		payload, uSize, err := split.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
		if i >= len(sizes) {
			t.Fatalf("the splitter produced more than the %d blocks xz was asked for", len(sizes))
		}
		// -T2 uses the buffer encoder, whose headers carry both sizes. The
		// declared uncompressed size is what CompressBlocks cross-checks its own
		// block list against, so a splitter that lost it would disable that check.
		if uSize != sizes[i] {
			t.Errorf("block %d declares %d uncompressed bytes, want %d", i, uSize, sizes[i])
		}
		if len(payload) == 0 {
			t.Errorf("block %d has an empty payload", i)
		}
		if len(payload) >= sizes[i] {
			t.Errorf("block %d payload is %d bytes against %d of plaintext, so it did not compress",
				i, len(payload), sizes[i])
		}
		// The slice is documented as valid only until the next call, so copy.
		payloads = append(payloads, append([]byte(nil), payload...))
	}
	if len(payloads) != len(sizes) {
		t.Fatalf("walked %d blocks, want %d", len(payloads), len(sizes))
	}

	// Once EOF is reached it must stay reached, because the caller loops on it.
	if _, _, err := split.next(); err != io.EOF {
		t.Errorf("a second call past the index returned %v, want io.EOF", err)
	}

	// The payloads have to be the real LZMA2 data: framed individually they must
	// decompress back to their own slice of the plaintext. This is what makes the
	// forward walk equivalent to compressing each block on its own.
	for i, payload := range payloads {
		framed, err := appendXZFrame(nil, payload, sizes[i], crc32OfBlock(plain, sizes, i), 131072)
		if err != nil {
			t.Fatalf("framing block %d: %v", i, err)
		}
		got, err := xzDecompressAll(context.Background(), framed, sizes[i])
		if err != nil {
			t.Fatalf("decompressing block %d: %v", i, err)
		}
		want := plain[cumulative(sizes, i) : cumulative(sizes, i)+sizes[i]]
		if !bytes.Equal(got, want) {
			t.Errorf("block %d did not round-trip through the splitter", i)
		}
	}
}

// TestXZBlockSplitterRefusesUnwalkableStreams covers the refusals that exist so
// the splitter never has to buffer. Each of these would otherwise surface as a
// corrupt image rather than a clean failure.
func TestXZBlockSplitterRefusesUnwalkableStreams(t *testing.T) {
	sizes := []int{4000, 4000}
	plain := compressibleBlocks(sizes)

	t.Run("single-threaded framing", func(t *testing.T) {
		// This is the reason threadArg() has a floor of 2. -T1 uses the streaming
		// encoder, whose block headers omit the compressed size, so there is no way
		// to find the next boundary without reading the index at the end of the
		// stream -- which a reader consuming a running xz has not got yet.
		stream := xzStreamFor(t, 1, "crc32", sizes, plain)
		split, err := newXZBlockSplitter(bytes.NewReader(stream))
		if err != nil {
			t.Fatalf("the stream header itself is fine, so this should open: %v", err)
		}
		_, _, err = split.next()
		if err == nil || err == io.EOF {
			t.Fatalf("a -T1 stream was walked rather than refused (err %v)", err)
		}
		// The message has to name the remedy: this failure is only ever a
		// misconfigured thread count.
		if !strings.Contains(err.Error(), "-T2") {
			t.Errorf("the refusal does not point at the thread count: %v", err)
		}
	})

	t.Run("wrong check type", func(t *testing.T) {
		// squashfs blocks carry CRC32. Accepting another check would produce frames
		// that mksquashfs never emits, so the image would differ from the target.
		stream := xzStreamFor(t, 2, "crc64", sizes, plain)
		if _, err := newXZBlockSplitter(bytes.NewReader(stream)); err == nil {
			t.Fatal("a CRC64 stream was accepted")
		} else if !strings.Contains(err.Error(), "CRC32") {
			t.Errorf("the refusal does not name the expected check: %v", err)
		}
	})

	t.Run("not an xz stream", func(t *testing.T) {
		if _, err := newXZBlockSplitter(strings.NewReader("this is not compressed at all!!")); err == nil {
			t.Fatal("a non-xz stream was accepted")
		} else if !strings.Contains(err.Error(), "not an xz stream") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("truncated stream header", func(t *testing.T) {
		stream := xzStreamFor(t, 2, "crc32", sizes, plain)
		if _, err := newXZBlockSplitter(bytes.NewReader(stream[:8])); err == nil {
			t.Fatal("an 8-byte stream was accepted")
		}
	})

	t.Run("truncated mid-payload", func(t *testing.T) {
		// A short read inside a payload must be an error rather than a short
		// payload, because a silently truncated block would be framed and written
		// into the image. The cut has to land inside the payload: everything past
		// the last block is the index and the footer, which the splitter never
		// reads -- it stops at the index indicator, and a stream that ends early
		// there is caught by the caller's own block count instead.
		one := []int{4000}
		stream := xzStreamFor(t, 2, "crc32", one, compressibleBlocks(one))
		split, err := newXZBlockSplitter(bytes.NewReader(stream))
		if err != nil {
			t.Fatal(err)
		}
		payload, _, err := split.next()
		if err != nil {
			t.Fatal(err)
		}
		// Stream header, then the block header sized for this block, then half the
		// payload: a boundary the splitter has committed to reading past.
		cut := 12 + blockHeaderSize(one[0]) + len(payload)/2
		if cut >= len(stream) {
			t.Fatalf("the cut at %d is not inside a %d-byte stream", cut, len(stream))
		}

		split, err = newXZBlockSplitter(bytes.NewReader(stream[:cut]))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := split.next(); err == nil || err == io.EOF {
			t.Fatalf("a block cut in half was accepted (err %v)", err)
		}
	})
}

// crc32OfBlock is the CRC32 of one block's plaintext, which appendXZFrame needs
// in order to rebuild the frame the splitter took apart.
func crc32OfBlock(plain []byte, sizes []int, i int) uint32 {
	start := cumulative(sizes, i)
	return crc32.ChecksumIEEE(plain[start : start+sizes[i]])
}
