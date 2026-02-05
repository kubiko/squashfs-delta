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
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

// This file implements the support for snap deltas. Currently two formats are
// supported:
//
// - plain xdelta3 diff file on the compressed snaps
// - xdelta3 diff on an uncompressed representation of the snap files defined
//   by squashfs-tools called pseudo-files
//
// The format supporting pseudo-files has files with a header preceding the
// xdelta3 information. This header is padded to 'deltaHeaderSize' size to
// allow for future fields. Current definition is (bytes in each field are in
// little endian order):
//
// |       32b    |    8b    |    8b   |     16b    |     32b    |     16b     |        16b        |
// | magic number | format v | tools v | delta tool | time stamp | compression | super block flags |
//
// Magic number is "sqpf" in ASCII, then we have the format version and the
// tools version, which are 0x01 at the moment. The tools version identify a
// given bundle of tools included in the snapd snap.
//
// Time stamp, compression and super blog flags are respectively the
// modification_time, compression_id and flags fields of the squashfs header of
// the target file, and are needed to ensure reproducibility of the target
// files, as this information is not included in the pseudo-file definitions.
//
// Optional compression options (included in the squashfs file if the
// COMPRESSOR_OPTIONS flag is set) are currently not supported. If the target
// squashfs is detected to use them, we fallback to plain xdelta.
//
// Delta between two snaps (squashfs files) is generated on the squashfs pseudo
// file definition, which is an uncompressed representation of the content of
// the files. The header data is later used as input parameters to mksquashfs
// when recreating the target squashfs from the reconstructed pseudo file.
//
// Reference for the squashfs superblock: https://dr-emann.github.io/squashfs

type DeltaFormat int

const (
	// Identifiers for the formats in the API
	Xdelta3Format DeltaFormat = iota
	SnapXdelta3Format
	SnapHdiffzFormat

	// Identifiers for the store
	xdelta3Format = "xdelta3"
	// This follows compatibility labels conventions. First and second
	// number represent format and tools versions respectively, and could
	// use intervals in the future.
	snapDeltaFormatXdelta3 = "snap-1-1-xdelta3"
	snapDeltaFormatHdiffz  = "snap-1-1-Hdiffz"
)

const (
	// xdelta3 header, see https://datatracker.ietf.org/doc/html/rfc3284
	xdelta3MagicNumber = uint32(0x00c4c3d6)
	// squashfs magic number ("hsqs")
	squashfsMagicNumber = uint32(0x73717368)
)

// Snap delta format constants
const (
	// Delta Header Configuration
	deltaHeaderSize = 32

	// Magic number for our delta files: "sqpf" in hex
	deltaMagicNumber        = uint32(0x66707173)
	deltaFormatVersion      = uint8(0x01)
	deltaFormatToolsVersion = uint8(0x01)

	// Tool IDs
	DeltaToolXdelta3 = uint16(0x1)
	DeltaToolHdiffz  = uint16(0x2)

	// default delta tool
	defaultDeltaTool = DeltaToolHdiffz
)

// SquashfsSuperblock represents a SquashFS header up to the minor_version field.
// Reference: https://dr-emann.github.io/squashfs
type SquashfsSuperblock struct {
	Magic            uint32
	InodeCount       uint32
	ModificationTime uint32
	BlockSize        uint32
	FragmentEntryCnt uint32
	CompressionId    uint16
	BlockLog         uint16
	Flags            uint16
	IdCount          uint16
	MajorVersion     uint16
	MinorVersion     uint16
}

// SquashfsSuperblock.Flags constants
const (
	flagCheck             uint16 = 0x0004
	flagNoFragments       uint16 = 0x0010
	flagDuplicates        uint16 = 0x0040 // Note: logic is inverted (default is duplicates)
	flagExports           uint16 = 0x0080
	flagNoXattrs          uint16 = 0x0200
	flagCompressorOptions uint16 = 0x0400
)

// Tuning parameters for external tools
var (
	// xdelta3 on compressed files tuning.
	// Default compression level set to 3, no measurable gain between 3 and
	// 9 comp level.
	xdelta3PlainTuning = []string{"-3"}
	// xdelta3 on pseudo-files tuning.
	// The gain in size reduction between 3 and 7 comp level is 10 to 20%.
	// Delta size gains flattens at 7 There is no noticeable gain from
	// changing source window size (-B), bytes input window (-W) or size
	// compression duplicates window (-P)
	xdelta3Tuning = []string{"-7"}

	// hdiffz tuning
	hdiffzTuning  = []string{"-m-6", "-SD", "-c-zstd-21-24", "-d"}
	hpatchzTuning = []string{"-s-8m"}

	// unsquashfs tuning.
	// By default unsquashfs would allocate ~2x256 for any size of squashfs image.
	// We need to tame it down and use different tuning for:
	// - generating delta: runs on server side -> no tuning, we have memory
	// - apply delta: maybe low spec systems, limit data and fragment queues sizes
	unsquashfsTuningGenerate = []string{"-da", "128", "-fr", "128"}
	unsquashfsTuningApply    = []string{"-da", "8", "-fr", "8"}

	// mksquashfs tuning
	// by default mksquashfs can grab up to 25% of the physical memory
	// limit this as we migh run on contrained systems
	mksquashfsTuningApply = []string{"-mem-percent", "10"}

	// IO buffer size for efficient piping (1MB)
	CopyBufferSize = 1024 * 1024
)

// For testing purposes
var (
	osutilRunManyWithContext       = runManyWithContext
	osutilRunWithContext           = runWithContext
	setupPipes                     = setupPipesImpl
	snapdtoolCommandFromSystemSnap = cmdFromSystemSnapImpl
)

// SnapDeltaHeader is the header wrapping the actual delta stream. See
// description above.
type SnapDeltaHeader struct {
	Magic         uint32
	FormatVersion uint8
	ToolsVersion  uint8
	DeltaTool     uint16
	Timestamp     uint32
	Compression   uint16
	Flags         uint16
}

// toBytes serialises a delta header to a buffer of deltaHeaderSize length.
func (h *SnapDeltaHeader) toBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, h); err != nil {
		return nil, fmt.Errorf("cannot write header struct: %w", err)
	}

	// Pad to full size (deltaHeaderSize is 32)
	if buf.Len() < deltaHeaderSize {
		buf.Write(make([]byte, deltaHeaderSize-buf.Len()))
	}

	return buf.Bytes(), nil
}

// newDeltaHeaderFromSnap builds a delta header. It takes modification_time,
// compression_id, and flags from the squashfs superblock of targetSnap and
// writes that in the corresponding delta header fields.
func newDeltaHeaderFromSnap(targetSnap string, deltaFormat uint16) (*SnapDeltaHeader, error) {
	// we need to get some basic info from the target snap
	f, err := os.Open(targetSnap)
	if err != nil {
		return nil, fmt.Errorf("cannot open target: %w", err)
	}
	defer f.Close()

	var sb SquashfsSuperblock
	if err := binary.Read(f, binary.LittleEndian, &sb); err != nil {
		return nil, fmt.Errorf("while reading target superblock: %w", err)
	}

	if sb.Magic != squashfsMagicNumber {
		return nil, fmt.Errorf("target is not a squashfs")
	}

	if sb.Flags&flagCompressorOptions != 0 {
		return nil, fmt.Errorf("compression options section present in target, which is unsupported")
	}

	// We expect squashfs 4.0 format
	if sb.MajorVersion != 4 || sb.MinorVersion != 0 {
		return nil, fmt.Errorf("unexpected squashfs version %d.%d", sb.MajorVersion, sb.MinorVersion)
	}

	hdr := &SnapDeltaHeader{
		Magic:         deltaMagicNumber,
		FormatVersion: deltaFormatVersion,
		ToolsVersion:  deltaFormatToolsVersion,
		DeltaTool:     deltaFormat,
	}
	// Populate some header fields from the parsed struct
	hdr.Timestamp = sb.ModificationTime
	hdr.Compression = sb.CompressionId
	hdr.Flags = sb.Flags

	return hdr, nil
}

// PseudoEntry represents a parsed line from the definition
type PseudoEntry struct {
	FilePath string
	Type     string
	// only we only care about size of offset
	DataSize      int64
	DataOffset    int64
	OriginalIndex int // index to the pseudo definition header
}

// type DeltaToolingCmd func(ctx context.Context, args ...string) *exec.Cmd
type DeltaToolingCmd func(ctx context.Context, args ...string) *exec.Cmd

// --- Memory Pools ---

// Pool for small bytes.Buffer
var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

// Pool for large IO buffers (1MB) to reduce GC pressure during io.Copy
var ioBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, CopyBufferSize)
		return b
	},
}

// Helper to copy using pooled buffers
func copyBuffer(dst io.Writer, src io.Reader) (int64, error) {
	bufPtr := ioBufPool.Get().([]byte)
	defer ioBufPool.Put(bufPtr)
	return io.CopyBuffer(dst, src, bufPtr)
}

func copyNBuffer(dst io.Writer, src io.Reader, n int64) (int64, error) {
	bufPtr := ioBufPool.Get().([]byte)
	defer ioBufPool.Put(bufPtr)
	return io.CopyBuffer(dst, io.LimitReader(src, n), bufPtr)
}

func formatStoreString(id DeltaFormat) string {
	switch id {
	case Xdelta3Format:
		return xdelta3Format
	case SnapXdelta3Format:
		return snapDeltaFormatXdelta3
	case SnapHdiffzFormat:
		return snapDeltaFormatHdiffz
	}
	return "unexpected"
}

// Supported delta formats
func SupportedDeltaFormats() []string {
	return []string{formatStoreString(SnapHdiffzFormat), formatStoreString(SnapXdelta3Format), formatStoreString(Xdelta3Format)}
}

// GenerateDelta creates a delta file called delta from sourceSnap and
// targetSnap, using deltaFormat.
func GenerateDelta(sourceSnap, targetSnap, delta string, deltaFormat DeltaFormat) error {
	// Context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch deltaFormat {
	case Xdelta3Format:
		// Plain xdelta3 on compressed files
		return generatePlainXdelta3Delta(ctx, sourceSnap, targetSnap, delta)
	case SnapXdelta3Format:
		return generateSnapDelta(ctx, sourceSnap, targetSnap, delta, DeltaToolXdelta3)
	case SnapHdiffzFormat:
		return generateSnapDelta(ctx, sourceSnap, targetSnap, delta, DeltaToolHdiffz)
	default:
		return fmt.Errorf("unsupported delta format %d", deltaFormat)
	}
}

func generateSnapDelta(ctx context.Context, sourceSnap, targetSnap, delta string, deltaFormat uint16) error {
	fmt.Println("Generating delta...")

	// Build delta header, using the target header
	hdr, err := newDeltaHeaderFromSnap(targetSnap, deltaFormat)
	if err != nil {
		return err
	}

	headerBytes, err := hdr.toBytes()
	if err != nil {
		return fmt.Errorf("cannot build delta header: %w", err)
	}

	// Create delta file and write header
	deltaFile, err := os.OpenFile(delta, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("cannot create delta file: %w", err)
	}
	defer deltaFile.Close()
	if _, err := deltaFile.Write(headerBytes); err != nil {
		return fmt.Errorf("cannot write delta header: %w", err)
	}

	// run delta producer for given deta tool
	switch deltaFormat {
	case DeltaToolXdelta3:
		err = generateXdelta3Delta(ctx, deltaFile, sourceSnap, targetSnap)
	case DeltaToolHdiffz:
		err = generateHdiffzDelta(ctx, deltaFile, sourceSnap, targetSnap)
	default:
		err = fmt.Errorf("unsupported delta tool 0x%X", hdr.DeltaTool)
	}
	if err != nil {
		deltaFile.Close()
		if err := os.Remove(delta); err != nil {
			fmt.Printf("cannot clean-up delta file: %s\n", err)
		}
		return err
	}
	return nil
}

// ApplyDelta uses sourceSnap and delta files to generate targetSnap.
func ApplyDelta(sourceSnap, delta, targetSnap string) error {
	// Global Context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	deltaFile, err := os.Open(delta)
	if err != nil {
		return fmt.Errorf("cannot open delta: %w", err)
	}
	defer deltaFile.Close()

	// Read a maximum of deltaHeaderSize, then check it to find out the
	// format that we need to decode.
	buf := make([]byte, deltaHeaderSize)
	n, err := io.ReadFull(deltaFile, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return fmt.Errorf("cannot read snap delta file: %w", err)
	}
	if n < 4 {
		return fmt.Errorf("delta file does not contain a header")
	}
	magic := binary.LittleEndian.Uint32(buf[0:4])
	switch magic {
	case xdelta3MagicNumber:
		fmt.Printf("plain xdelta3 detected\n")
		return applyPlainXdelta3Delta(ctx, sourceSnap, delta, targetSnap)
	case deltaMagicNumber:
		if n < deltaHeaderSize {
			return fmt.Errorf("snap delta header too short (%d bytes read)", n)
		}
		hdr := &SnapDeltaHeader{}
		if err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, hdr); err != nil {
			return fmt.Errorf("cannot decode header: %w", err)
		}
		return applySnapDelta(ctx, sourceSnap, targetSnap, deltaFile, hdr)
	default:
		return fmt.Errorf("unknown delta file format")
	}
}

func applySnapDelta(ctx context.Context, sourceSnap, targetSnap string, deltaFile *os.File, hdr *SnapDeltaHeader) error {
	if hdr.Magic != deltaMagicNumber {
		return fmt.Errorf("invalid magic 0x%X", hdr.Magic)
	}
	// We require compatibility both with format and tools version
	if hdr.FormatVersion != deltaFormatVersion || hdr.ToolsVersion != deltaFormatToolsVersion {
		return fmt.Errorf("incompatible version %d.%d",
			hdr.FormatVersion, hdr.ToolsVersion)
	}
	if hdr.DeltaTool != DeltaToolXdelta3 && hdr.DeltaTool != DeltaToolHdiffz {
		return fmt.Errorf("unsupported delta tool %d", hdr.DeltaTool)
	}

	// Prepare mksquashfs arguments by looking at delta header
	var err error
	mksqfsArgs := []string{}
	if mksqfsArgs, err = compIdToMksquashfsArgs(hdr.Compression, mksqfsArgs); err != nil {
		return fmt.Errorf("bad compression id from delta header: %w", err)
	}
	if mksqfsArgs, err = superBlockFlagsToMksquashfsArgs(hdr.Flags, mksqfsArgs); err != nil {
		return fmt.Errorf("bad flags from delta header: %w", err)
	}
	mksqfsArgs = append(mksqfsArgs, "-mkfs-time", strconv.FormatUint(uint64(hdr.Timestamp), 10))

	// run delta apply for given deta tool
	switch hdr.DeltaTool {
	case DeltaToolXdelta3:
		return applyXdelta3Delta(ctx, sourceSnap, targetSnap, deltaFile, mksqfsArgs)
	case DeltaToolHdiffz:
		return applyHdiffzDelta(ctx, sourceSnap, targetSnap, deltaFile, mksqfsArgs)
	default:
		return fmt.Errorf("unsupported delta tool 0x%X", hdr.DeltaTool)
	}
}

// generatePlainXdelta3Delta generates a delta between compressed snaps
func generatePlainXdelta3Delta(ctx context.Context, sourceSnap, targetSnap, delta string) error {
	// Compression level, force overwrite (-f), compress (-e), source (-s <file>), target, delta
	opts := append([]string{}, xdelta3PlainTuning...)
	opts = append(opts, "-f", "-e", "-s", sourceSnap, targetSnap, delta)
	cmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/xdelta3", opts...)
	if err != nil {
		return fmt.Errorf("cannot generate delta: %w", err)
	}

	return osutilRunWithContext(ctx, cmd)
}

// applyPlainXdelta3Delta applies a delta between compressed snaps
func applyPlainXdelta3Delta(ctx context.Context, sourceSnap, delta, targetSnap string) error {
	// Force overwrite (-f), decompress (-d), source (-s <file>), target, delta
	cmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/xdelta3",
		"-f", "-d", "-s", sourceSnap, delta, targetSnap)
	if err != nil {
		return fmt.Errorf("cannot apply delta: %w", err)
	}

	return osutilRunWithContext(ctx, cmd)
}

// generateXdelta3Delta runs in parallel two instances of unsquashfs (one for
// sourceSnap and the other for targetSnap) and xdelta3. unsquashfs output is
// in pseudo-file format. This is read by xdelta3, which calculates the delta
// between the two files (xdelta3 uses windows to calculate differences so this
// can happen in a stream way). Named pipes are used to feed xdelta3 as it does
// not read from stdin.
//
// The output of xdelta3 is sent to deltaFile, which is an open file where we
// already stored the snap delta header. This diagram summarizes this:
//
//	+-----------------+             +-----------------+
//	|   sourceSnap    |             |   targetSnap    |
//	+-------+---------+             +-------+---------+
//	        |                               |
//	        v                               v
//	+-----------------+             +-----------------+
//	|   unsquashfs    |             |   unsquashfs    |
//	| (pseudo-format) |             | (pseudo-format) |
//	+-------+---------+             +-------+---------+
//	        |                               |
//	        | [named pipe: src-pipe]        | [named pipe: trgt-pipe]
//	        |                               |
//	        +--------------+ +--------------+
//	                       | |
//	                       v v
//	            +-------------------------+
//	            |         xdelta3         |
//	            |    (calculate delta)    |
//	            +------------+------------+
//	                         |
//	                         | (stdout)
//	                         v
//	            +-------------------------+
//	            |       deltaFile         |
//	            | (Appended after header) |
//	            +-------------------------+
func generateXdelta3Delta(ctx context.Context, deltaFile *os.File, sourceSnap, targetSnap string) error {
	// Setup named pipes
	tempDir, pipes, err := setupPipes("src-pipe", "trgt-pipe")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	sourcePipe := pipes[0]
	targetPipe := pipes[1]

	// Output to sourcePipe, -pf stands for pseudo-file representation
	unsquashSrcArg := append([]string{}, unsquashfsTuningGenerate...)
	unsquashSrcArg = append(unsquashSrcArg, "-no-progress", "-pf", sourcePipe, sourceSnap)
	unsquashSrcCmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/unsquashfs", unsquashSrcArg...)
	if err != nil {
		return fmt.Errorf("cannot find unsquashfs: %w", err)
	}
	// Output to targetPipe.
	// Leave progress output to show it when we run "snap delta".
	unsquashTrgArg := append([]string{}, unsquashfsTuningGenerate...)
	unsquashTrgArg = append(unsquashTrgArg, "-pf", targetPipe, targetSnap)
	unsquashTrgCmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/unsquashfs", unsquashTrgArg...)
	if err != nil {
		return fmt.Errorf("cannot find unsquashfs: %w", err)
	}
	// Compress (-e), force overwrite (-f), no app header (-A), source from sourcePipe (-s)
	xdelta3Arg := append([]string{}, xdelta3Tuning...)
	xdelta3Arg = append(xdelta3Arg, "-e", "-f", "-A", "-s", sourcePipe, targetPipe)
	xdelta3Cmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/xdelta3", xdelta3Arg...)
	if err != nil {
		return fmt.Errorf("cannot find xdelta3: %w", err)
	}
	// Output to the file where we already wrote the header
	xdelta3Cmd.Stdout = deltaFile

	cmds := []*exec.Cmd{unsquashSrcCmd, unsquashTrgCmd, xdelta3Cmd}
	return osutilRunManyWithContext(ctx, cmds, nil)
}

// applyXdelta3Delta runs in parallel unsquashfs (to get the pseudo-file from
// sourceSnap), a goroutine (sends the delta information in deltaFile to
// xdelta3), xdelta3 (to apply the delta) and mksquashfs (to re-create
// targetSnap).
//
// Named pipes are used to stream the data to xdelta3. The goroutine is needed
// as we have to remove the snap delta header that is stored before the xdelta3
// data in deltaFile. We can use a regular pipe to stream data between xdelta3
// and mksquashfs, as the former can write to stdout and the latter can read
// the pseudo-file from stdin. This diagram summarizes this:
//
//	+-----------------+
//	|   sourceSnap    | (SquashFS file)
//	+-------+---------+
//	        |
//	        v
//	+-----------------+      [named pipe: srcPipe]      +-----------------+
//	|   unsquashfs    | ------------------------------> |                 |
//	| (pseudo-format) |                                 |     xdelta3     |
//	+-----------------+                                 |  (apply delta)  |
//	                                                    |                 |
//	+-----------------+      [named pipe: deltaPipe]    |                 |
//	|  deltaWriter    | ------------------------------> |                 |
//	|  (goroutine)    | (raw xdelta3 data)              +--------+--------+
//	+-------+---------+                                          |
//	        ^                                                    | (stdout pipe)
//	        |                                                    |
//	+-------+---------+                                          v
//	|    deltaFile    |                                 +-----------------+
//	| (Seek past 32b) |                                 |   mksquashfs    |
//	+-----------------+                                 | (rebuild snap)  |
//	                                                    +--------+--------+
//	                                                             |
//	                                                             v
//	                                                    +-----------------+
//	                                                    |   targetSnap    |
//	                                                    +-----------------+
func applyXdelta3Delta(ctx context.Context, sourceSnap, targetSnap string, deltaFile *os.File, mksqfsHdrArgs []string) error {
	// setup pipes to apply delta
	tempDir, pipes, err := setupPipes("src", "delta")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	srcPipe := pipes[0]
	deltaPipe := pipes[1]

	// Output to srcPipe, -pf stands for pseudo-file representation
	unsquashCmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/unsquashfs",
		"-no-progress", "-pf", srcPipe, sourceSnap)
	if err != nil {
		return fmt.Errorf("cannot find unsquashfs: %w", err)
	}
	// Decompress (-d), force overwrite (-f), source from srcPipe (-s),
	// delta from deltaPipe, output is to stdout
	xdelta3Cmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/xdelta3",
		"-d", "-f", "-s", srcPipe, deltaPipe)
	if err != nil {
		return fmt.Errorf("cannot find xdelta3: %w", err)
	}
	// Source from stdin (-), create targetSnap, pseudo-file from stdin
	// (-pf -), not append to existing filesystem, quiet, append additional
	// args built from our header.
	mksquashArgs := append([]string{
		"-", targetSnap, "-pf", "-", "-noappend", "-quiet",
	}, mksqfsHdrArgs...)
	mksquashCmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/mksquashfs", mksquashArgs...)
	if err != nil {
		return fmt.Errorf("cannot find mksquashfs: %w", err)
	}
	// Shows progress when creating squashfs.
	// TODO make this happen only in "snap apply" command
	mksquashCmd.Stdout = os.Stdout
	// Connect xdelta3 output to mksquashfs input
	mksquashCmd.Stdin, err = xdelta3Cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("while connecting xdelta to mksqfs: %w", err)
	}

	// task that writes to deltaPipe named FIFO
	deltaWriter := func(context.Context) error {
		pf, err := os.OpenFile(deltaPipe, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		defer pf.Close()
		// seek past header
		if _, err := deltaFile.Seek(deltaHeaderSize, io.SeekStart); err != nil {
			return err
		}
		// If there is an error in one of the processes, all of them
		// will be killed by RunManyWithContext, which will in turn
		// close the named pipe and we will return with error here.
		if _, err := copyBuffer(pf, deltaFile); err != nil {
			return err
		}
		return nil
	}

	cmds := []*exec.Cmd{unsquashCmd, mksquashCmd, xdelta3Cmd}
	return osutilRunManyWithContext(ctx, cmds, []func(context.Context) error{deltaWriter})
}

// --- Hdiffz Implementations ---
func generateHdiffzDelta(ctx context.Context, deltaFile *os.File, sourceSnap, targetSnap string) error {
	// Setup Context & WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	// Setup all pipes
	tempDir, pipes, err := setupPipes("src-pipe", "trgt-pipe")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	sourcePipe := pipes[0]
	targetPipe := pipes[1]

	// Output to sourcePipe, -pf stands for pseudo-file representation
	unsquashSrcArg := append([]string{}, unsquashfsTuningGenerate...)
	unsquashSrcArg = append(unsquashSrcArg, "-no-progress", "-pf", sourcePipe, sourceSnap)
	unsquashSrcCmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/unsquashfs", unsquashSrcArg...)
	if err != nil {
		return fmt.Errorf("cannot find unsquashfs: %w", err)
	}
	// Output to targetPipe.
	// Leave progress output to show it when we run "snap delta".
	unsquashTrgArg := append([]string{}, unsquashfsTuningGenerate...)
	unsquashTrgArg = append(unsquashTrgArg, "-pf", targetPipe, targetSnap)
	unsquashTrgCmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/unsquashfs", unsquashTrgArg...)
	if err != nil {
		return fmt.Errorf("cannot find unsquashfs: %w", err)
	}

	// prepare hpatchz
	hdiffzCmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/hdiffz", "")
	hdiffzPath := hdiffzCmd.Path

	wg.Add(2)
	// unsquash source
	go func() {
		defer wg.Done()
		if err := runWithContext(ctx, unsquashSrcCmd); err != nil {
			select {
			case errCh <- wrapErr(err, "unsqfs-src"):
			default:
			}
			cancel()
		}
	}()

	// unsquash target
	go func() {
		defer wg.Done()
		if err := runWithContext(ctx, unsquashTrgCmd); err != nil {
			select {
			case errCh <- wrapErr(err, "unsqfs-trg"):
			default:
			}
			cancel()
		}
	}()

	sp, err := os.Open(sourcePipe)
	if err != nil {
		return fmt.Errorf("failed to open source pipe:%v", err)
	}
	tp, err := os.Open(targetPipe)
	if err != nil {
		return fmt.Errorf("failed to open target pipe:%v", err)
	}
	sourceReader := bufio.NewReaderSize(sp, CopyBufferSize)
	targetReader := bufio.NewReaderSize(tp, CopyBufferSize)

	// 2. Parse Headers
	// Parse source into a Map for O(1) lookup
	sourceEntries, sourceHeaderBuff, err := parsePseudoStream(sourceReader)
	if err != nil {
		return fmt.Errorf("failed to parse source header: %w", err)
	}

	sourceEntriesCount := len(sourceEntries)
	// Map for fast lookups: FilePath -> Entry
	sourceMap := make(map[string]*PseudoEntry, sourceEntriesCount)
	for i := range sourceEntries {
		sourceEntries[i].OriginalIndex = i
		sourceMap[sourceEntries[i].FilePath] = &sourceEntries[i]
	}

	targetEntries, targetHeaderBuff, err := parsePseudoStream(targetReader)
	if err != nil {
		return fmt.Errorf("failed to parse target header: %w", err)
	}

	// Prepare reusable Processors for diffing
	srcMem, err := NewReusableMemFD("src-seg")
	if err != nil {
		return fmt.Errorf("failed to prepare reusable memFd: %w", err)
	}
	defer srcMem.Close()
	targetMem, err := NewReusableMemFD("trgt-seg")
	if err != nil {
		return fmt.Errorf("failed to prepare reusable memFd: %w", err)
	}
	defer targetMem.Close()

	diffMem, err := NewReusableMemFD("seg-diff")
	if err != nil {
		return fmt.Errorf("failed to prepare reusable memFd: %w", err)
	}
	defer diffMem.Close()

	// calculate header Delta and write it to the delta stream, use prepare mem processors
	_, err = srcMem.File.Write(sourceHeaderBuff.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write source header to memFd: %w", err)
	}
	_, err = targetMem.File.Write(targetHeaderBuff.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write target header to memFd: %w", err)
	}
	segmentDeltaSize, err := writeHdiffzToDeltaStream(ctx, deltaFile, 0, int64(sourceHeaderBuff.Len()), srcMem, targetMem, diffMem, hdiffzPath)
	if err != nil {
		return fmt.Errorf("failed to calculate delta on headers: %w", err)
	}

	sourceHeaderSize := int64(sourceHeaderBuff.Len())
	bufferPool.Put(sourceHeaderBuff)
	bufferPool.Put(targetHeaderBuff)

	sourceRead := int64(0)
	totalDeltaSize := int64(24 + segmentDeltaSize)
	lastSourceIndex := 0
	targetEntrieCount := len(targetEntries)
	fmt.Printf("Processing %d target entries against %d source entries\n", targetEntrieCount, sourceEntriesCount)
	// Main Processing Loop
LOOP:
	for i, te := range targetEntries {
		// Check context before processing entry
		if ctx.Err() != nil {
			break LOOP
		}

		// Reset MemFDs for reuse
		srcMem.Reset()
		targetMem.Reset()
		diffMem.Reset()

		// Try Exact Match
		sourceEntry := sourceMap[te.FilePath]
		// Fallback: Fuzzy Match for directory or filename change
		if sourceEntry == nil {
			// We must select a candidate that is physically AHEAD in the stream as cannot rewind the source pipe
			// We do not want to advance too much either, it could be false "match"
			// assuming we compare software, allow "version" change in fuzzy match
			// find first fuzzy match, max 20 entries eahead
			// build score from different criteria
			//  - exact basefilename match: or exact directory match: 10
			//  - size +-20% difference: up to 10
			//  - index delta from last index: 20 - index delta
			// if score > 25 it's match
			lookout := min(lastSourceIndex+20, sourceEntriesCount) // which ever is smaller
			for i := lastSourceIndex + 1; i < lookout; i++ {
				se := sourceEntries[i]
				fuzzyMatch := pathsMatchFuzzy(se.FilePath, te.FilePath)
				if fuzzyMatch != 0 {
					// build the rest of the score
					score := getSimilarityScore(se.DataSize, te.DataSize, 20) / 10
					score += 20 + lastSourceIndex + 1 - i
					if fuzzyMatch < 3 {
						score += 10
					}
					if score > 25 {
						fmt.Printf("Fuzzy Match(%d): %s matched with old %s\n", score, te.FilePath, se.FilePath)
						sourceEntry = &sourceEntries[i]
						break
					} else {
						fmt.Printf("Ignoring Fuzzy Match(%d): %s with old %s\n", score, te.FilePath, se.FilePath)
					}
				}
			}
		}
		sourceSize := int64(0)
		sourceOffset := int64(0)

		if sourceEntry != nil {
			sourceSize = sourceEntry.DataSize
			sourceOffset = sourceEntry.DataOffset
			lastSourceIndex = sourceEntry.OriginalIndex
		} else {
			fmt.Printf("[%d/%d] No original version for: %s\n", i, targetEntrieCount, te.FilePath)
		}

		// Handle Source Stream extraction
		// Calculate Source CRC while copying to detect identity without re-reading
		srcCRC := crc32.NewIEEE()
		// Only attempt to read source if we have a valid entry AND it's not behind us
		// (The fuzzy logic ensures offset >= sourceRead, but exact match might not if the stream was mixed up)
		if sourceSize > 0 && sourceOffset >= sourceRead {
			toSkip := sourceOffset - sourceRead
			if toSkip > 0 {
				if _, err := copyNBuffer(io.Discard, sourceReader, toSkip); err != nil {
					errCh <- fmt.Errorf("failed to skip source stream: %w", err)
					cancel()
					break LOOP
				}
				sourceRead += toSkip
			}

			// TeeReader reads from source, writes to srcMem AND srcCRC
			mw := io.MultiWriter(srcMem.File, srcCRC)
			if _, err := copyNBuffer(mw, sourceReader, sourceSize); err != nil {
				errCh <- fmt.Errorf("failed to extract source segment: %w", err)
				cancel()
				break LOOP
			}
			sourceRead += sourceSize
		} else if sourceEntry != nil && sourceOffset < sourceRead {
			// Edge case: We found a match (exact or fuzzy), but it is physically located
			// BEFORE our current pipe position. We cannot use it.
			// Reset sourceSize so we treat this as a "New File" insertion.
			sourceSize = 0
			fmt.Printf("Skipping unsearchable source match (stream moved past): %s\n", sourceEntry.FilePath)
		}

		// Handle Target Stream extraction
		targetCRC := crc32.NewIEEE()
		mw := io.MultiWriter(targetMem.File, targetCRC)
		if _, err := copyNBuffer(mw, targetReader, te.DataSize); err != nil {
			errCh <- fmt.Errorf("failed to extract target segment: %w", err)
			cancel()
			break LOOP
		}

		// Determine Identity
		isIdentical := false
		if sourceEntry != nil && sourceSize == te.DataSize {
			// Compare checksums instead of reading files again
			if sourceSize == 0 || srcCRC.Sum32() == targetCRC.Sum32() {
				isIdentical = true
			}
		}

		if isIdentical {
			// Files match, write negative index header
			headerBuf := bufferPool.Get().(*bytes.Buffer)
			headerBuf.Reset()
			binary.Write(headerBuf, binary.LittleEndian, int64(-sourceEntry.OriginalIndex))
			if _, err := deltaFile.Write(headerBuf.Bytes()); err != nil {
				bufferPool.Put(headerBuf)
				errCh <- err
				cancel()
				break LOOP
			}
			bufferPool.Put(headerBuf)
			totalDeltaSize += 8
		} else {
			// Files differ, run hdiffz and store the delta
			segSize, err := writeHdiffzToDeltaStream(ctx, deltaFile, sourceHeaderSize+sourceOffset, sourceSize, srcMem, targetMem, diffMem, hdiffzPath)
			if err != nil {
				errCh <- err
				cancel()
				break LOOP
			}
			totalDeltaSize += (segSize + 24)
			fmt.Printf("[%d/%d] Delta: %s (%d bytes -> %d bytes)\n", i, targetEntrieCount, te.FilePath, te.DataSize, segSize)
		}
	}

	// Wait for background tasks to finish
	wg.Wait()
	close(errCh)

	// Check if any error occurred in main loop or background
	if err := <-errCh; err != nil {
		return err
	}
	// Check context
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Validation that nothing was left in the target reader
	if b := targetReader.Buffered(); b > 0 {
		return fmt.Errorf("target stream has %d bytes left unconsumed", b)
	}

	fmt.Printf("Delta generation complete. Total size: %d\n", totalDeltaSize)
	return nil
}

func applyHdiffzDelta(ctx context.Context, sourceSnap, targetSnap string, deltaFile *os.File, mksqfsHdrArgs []string) error {
	// Setup Context & WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	// Start Source Stream (unsquashfs)
	// We read FROM this pipe
	// Output to srcPipe, -pf stands for pseudo-file representation
	sourceCmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/unsquashfs",
		"-no-progress", "-pf", "-", sourceSnap)
	if err != nil {
		return fmt.Errorf("cannot find unsquashfs: %w", err)
	}
	sourcePipe, err := sourceCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create source pipe: %w", err)
	}

	// Wrap source in a buffered reader for efficient seeking/skipping
	sourceReader := bufio.NewReaderSize(sourcePipe, CopyBufferSize)

	// Source from stdin (-), create targetSnap, pseudo-file from stdin
	// (-pf -), not append to existing filesystem, quiet, append additional
	// args built from our header.
	mksquashArgs := append([]string{
		"-", targetSnap, "-pf", "-", "-noappend", "-quiet",
	}, mksqfsHdrArgs...)
	mksquashCmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/mksquashfs", mksquashArgs...)
	if err != nil {
		return fmt.Errorf("cannot find mksquashfs: %w", err)
	}
	// Shows progress when creating squashfs.
	// TODO make this happen only in "snap apply" command
	mksquashCmd.Stdout = os.Stdout

	targetStdin, err := mksquashCmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create target stdin pipe: %w", err)
	}

	// prepare hpatchz
	hpatchzCmd, err := snapdtoolCommandFromSystemSnap("/usr/bin/hpatchz", "")
	hpatchzPath := hpatchzCmd.Path

	// unsquash source
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := runWithContext(ctx, sourceCmd); err != nil {
			select {
			case errCh <- wrapErr(err, "unsquashfs"):
			default:
			}
			cancel()
		}
	}()

	// mksquash target
	go func() {
		defer wg.Done()
		if err := runWithContext(ctx, mksquashCmd); err != nil {
			select {
			case errCh <- wrapErr(err, "mksquashfs"):
			default:
			}
			cancel()
		}
	}()

	// Parse Source Header
	// We need the source entries to resolve "Identical" file references (negative indices)
	sourceEntries, sourceHeaderBuff, err := parsePseudoStream(sourceReader)
	if err != nil {
		return fmt.Errorf("failed to parse source header: %w", err)
	}
	sourceHeaderSize := int64(sourceHeaderBuff.Len())

	// Reconstruct and Write Target Header
	// The first segment in the delta file is ALWAYS the header patch
	// Read Header Patch Metadata: [Offset (0)][SourceSize][PatchSize]
	var headOffset, headSrcSize, headPatchSize int64
	if err := binary.Read(deltaFile, binary.LittleEndian, &headOffset); err != nil {
		return fmt.Errorf("failed to read header delta offset: %w", err)
	}
	if err := binary.Read(deltaFile, binary.LittleEndian, &headSrcSize); err != nil {
		return fmt.Errorf("failed to read header source size: %w", err)
	}
	if err := binary.Read(deltaFile, binary.LittleEndian, &headPatchSize); err != nil {
		return fmt.Errorf("failed to read header patch size: %w", err)
	}

	// Prepare reusable mem Processors for patch applying
	srcMem, err := NewReusableMemFD("src-seg")
	if err != nil {
		return fmt.Errorf("failed to prepare reusable memFd: %w", err)
	}
	defer srcMem.Close()
	targetMem, err := NewReusableMemFD("target_seg")
	if err != nil {
		return fmt.Errorf("failed to prepare reusable memFd: %w", err)
	}
	defer targetMem.Close()

	patchMem, err := NewReusableMemFD("seg-patch")
	if err != nil {
		return fmt.Errorf("failed to prepare reusable memFd: %w", err)
	}
	defer patchMem.Close()

	// get header patch into memory
	if _, err := copyNBuffer(patchMem.File, deltaFile, headPatchSize); err != nil {
		return fmt.Errorf("failed to read header patch data: %w", err)
	}

	// get source header to the memory
	if _, err := copyNBuffer(srcMem.File, sourceHeaderBuff, sourceHeaderSize); err != nil {
		return fmt.Errorf("failed to copy source header data: %w", err)
	}

	// Apply Patch: Source Header + Patch -> Target Header
	if err := applyHdiffzPatch(ctx, srcMem.Path, patchMem.Path, targetMem.Path, hpatchzPath); err != nil {
		return fmt.Errorf("failed to patch header: %w", err)
	}

	// We don't need the raw source header text anymore, so return to pool
	bufferPool.Put(sourceHeaderBuff)

	// Write Reconstructed Header to mksquashfs
	// This tells mksquashfs what files are coming
	if _, err := copyBuffer(targetStdin, targetMem.File); err != nil {
		return fmt.Errorf("failed to write target header to mksquashfs: %w", err)
	}

	// Parse the *Target* header we just generated so we know the order of files expected
	// We need to rewind the targetMem to parse it
	targetMem.File.Seek(0, 0)
	targetHeadReader := bufio.NewReader(targetMem.File)
	targetEntries, _, err := parsePseudoStream(targetHeadReader)
	if err != nil {
		return fmt.Errorf("failed to parse reconstructed target header: %w", err)
	}
	fmt.Printf("Reconstructing %d entries...\n", len(targetEntries))

	// Process Stream Loop
	// we can process delta stream directly, it has all the information we need
	// but using reconstructed target header as entry for the loop
	// gives us debug info at which file we failed to apply patch
	srcMem.Reset()
	patchMem.Reset()
	targetMem.Reset()
	sourceReadCursor := sourceHeaderSize

LOOP:
	for _, entry := range targetEntries {
		if ctx.Err() != nil {
			break LOOP
		}
		// Read Control Int64
		var controlVal int64
		if err := binary.Read(deltaFile, binary.LittleEndian, &controlVal); err != nil {
			errCh <- fmt.Errorf("failed to read control value for %s: %w", entry.FilePath, err)
			cancel()
			break LOOP
		}
		if controlVal <= 0 {
			// source file is idential to target file, just stream it
			// control value is negative index to the source header
			sourceIndex := int(-controlVal)
			srcEntry := sourceEntries[sourceIndex]

			// stream can only move forward, do sanity check we haven't advanced allready too far
			neededOffset := srcEntry.DataOffset + sourceHeaderSize
			if sourceReadCursor > neededOffset {
				errCh <- fmt.Errorf("critical: source stream cursor (%d) passed needed offset (%d). Generator logic flaw or unsorted input", sourceReadCursor, neededOffset)
				cancel()
				break LOOP
			}
			// do we need to skip some data in the source stream?
			skip := neededOffset - sourceReadCursor
			if skip > 0 {
				copyNBuffer(io.Discard, sourceReader, skip)
				sourceReadCursor += skip
			}

			// ready to pump data from source stream to -> mksquashfs
			if _, err := copyNBuffer(targetStdin, sourceReader, srcEntry.DataSize); err != nil {
				errCh <- fmt.Errorf("failed to copy source data for %s: %w", entry.FilePath, err)
				cancel()
				break LOOP
			}
			sourceReadCursor += srcEntry.DataSize

		} else {
			// source and tatget file differ, apply patch on the source
			// controlVal becomes SourceOffset
			srcOffset := controlVal
			var srcSize, patchSize int64

			if err := binary.Read(deltaFile, binary.LittleEndian, &srcSize); err != nil {
				errCh <- err
				cancel()
				break LOOP
			}
			if err := binary.Read(deltaFile, binary.LittleEndian, &patchSize); err != nil {
				errCh <- err
				cancel()
				break LOOP
			}
			// prepare patch file
			patchMem.Reset()
			if _, err := copyNBuffer(patchMem.File, deltaFile, patchSize); err != nil {
				errCh <- fmt.Errorf("failed to read patch data: %w", err)
				cancel()
				break LOOP
			}

			// Prepare Source Segment
			srcMem.Reset()
			if srcSize > 0 {
				// align source stream to what patch applies to
				// mostl likely files from source are not present in the target
				// !! sourceOffset in delta includes source header size for consistency with header delta which has offset 0
				// offset values in the header start at 0 after the header ends

				if sourceReadCursor > srcOffset {
					errCh <- fmt.Errorf("critical: source cursor advanced too far for patch %s", entry.FilePath)
					cancel()
					break LOOP
				}

				skip := srcOffset - sourceReadCursor
				if skip > 0 {
					copyNBuffer(io.Discard, sourceReader, skip)
					sourceReadCursor += skip
				}

				// Read from stream to MemFD
				if _, err := copyNBuffer(srcMem.File, sourceReader, srcSize); err != nil {
					errCh <- fmt.Errorf("failed to extract source segment for patch: %w", err)
					cancel()
					break LOOP
				}
				sourceReadCursor += srcSize
			}

			// 3. Apply Patch
			targetMem.Reset()
			// if srcSize is 0, hpatchz treats it as creating a new file from patch
			if err := applyHdiffzPatch(ctx, srcMem.Path, patchMem.Path, targetMem.Path, hpatchzPath); err != nil {
				errCh <- fmt.Errorf("failed to patch file %s: %w", entry.FilePath, err)
				cancel()
				break LOOP
			}
			// write reconstructed result to mksquashfs
			// DEBUG: fmt.Printf("%s\t(from %d bytes delta)\n", entry.FilePath, patchSize)
			if _, err := copyBuffer(targetStdin, targetMem.File); err != nil {
				errCh <- fmt.Errorf("failed to write patched data to mksquashfs: %w", err)
				cancel()
				break LOOP
			}
		}
	}

	targetStdin.Close() // Close stdin to signal EOF to mksquashfs
	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return err
	}
	return nil
}

// --- Shared Helpers ---

// writeHdiffzToltaStream
func writeHdiffzToDeltaStream(ctx context.Context, deltaFile *os.File, sourceOffset, sourceSize int64, source, target, diff *ReusableMemFD, hdiffzPath string) (int64, error) {

	// Files differ, run hdiffz, use the /proc paths which remain valid for the reused FDs
	hdiffzArgs := append(hdiffzTuning, "-f", source.Path, target.Path, diff.Path)
	hdiffzCmd := exec.CommandContext(ctx, hdiffzPath, hdiffzArgs...)
	if err := runWithContext(ctx, hdiffzCmd); err != nil {
		return 0, fmt.Errorf("hdiffz failed: %v", err)
	}

	headerBuf := bufferPool.Get().(*bytes.Buffer)
	headerBuf.Reset()
	defer bufferPool.Put(headerBuf)

	// Get segment size
	st, err := diff.File.Stat()
	if err != nil {
		return 0, err
	}
	segSize := st.Size()

	binary.Write(headerBuf, binary.LittleEndian, int64(sourceOffset))
	binary.Write(headerBuf, binary.LittleEndian, int64(sourceSize))
	binary.Write(headerBuf, binary.LittleEndian, int64(segSize))

	if _, err := deltaFile.Write(headerBuf.Bytes()); err != nil {
		return 0, err
	}

	// Rewind segment file
	if _, err := diff.File.Seek(0, 0); err != nil {
		return 0, err
	}

	// Copy data
	if _, err := copyBuffer(deltaFile, diff.File); err != nil {
		return 0, err
	}
	return segSize, nil
}

func applyHdiffzPatch(ctx context.Context, oldPath, diffPath, outPath, hpatchzPath string) error {
	hpatchzArgs := append(hpatchzTuning, "-f", oldPath, diffPath, outPath)
	cmd := exec.CommandContext(ctx, hpatchzPath, hpatchzArgs...)
	if err := runWithContext(ctx, cmd); err != nil {
		return fmt.Errorf("%v", err)
	}
	return nil
}

// unescape: only allocates if backslash is present
func unescape(s string) string {
	if strings.IndexByte(s, '\\') == -1 {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) && (s[i+1] == ' ' || s[i+1] == '\\') {
			i++
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func parsePseudoDefinitionLine(line string) []string {
	// find first space not preceded by escape
	splitIdx := -1
	for i := 0; i < len(line); i++ {
		if line[i] == ' ' && (i == 0 || line[i-1] != '\\') {
			splitIdx = i
			break
		}
	}

	if splitIdx == -1 {
		return []string{unescape(line)}
	}

	name := unescape(line[:splitIdx])
	rest := strings.Fields(line[splitIdx+1:])

	// Pre-allocate slice
	out := make([]string, 1, len(rest)+1)
	out[0] = name
	out = append(out, rest...)
	return out
}

// parsePseudoStream encapsulates the logic to read the mixed text/binary stream
func parsePseudoStream(reader *bufio.Reader) ([]PseudoEntry, *bytes.Buffer, error) {

	// 2. Storage for parsed data
	var entries []PseudoEntry
	headerBuffer := bufferPool.Get().(*bytes.Buffer)
	headerBuffer.Reset()
	headerEnd := false

	for {
		// Read until the next newline (Text Mode)
		lineBytes, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, nil, fmt.Errorf("Stream read error: %v", err)
		}

		// If we got no bytes and EOF, we are done
		if len(lineBytes) == 0 && err == io.EOF {
			break
		}

		// Store this raw line in our header buffer
		headerBuffer.Write(lineBytes)

		// Trim whitespace for parsing
		lineStr := string(lineBytes)
		trimmed := strings.TrimSpace(lineStr)

		// Skip empty lines or comments
		if len(trimmed) == 0 {
			if err == io.EOF {
				break
			}
		}
		if trimmed[0] == '#' {
			// is this the comment after "# START OF DATA - DO NOT MODIFY"
			if headerEnd {
				break
			}
			// detect if this is end of the header "# START OF DATA - DO NOT MODIFY"
			if trimmed == "# START OF DATA - DO NOT MODIFY" {
				// read one more line and break
				headerEnd = true
				continue
			}
		}

		// Parse the Text Line
		fields := parsePseudoDefinitionLine(trimmed)
		if len(fields) < 3 {
			// Handle malformed lines gracefully
			if err == io.EOF {
				break
			}
			continue
		}

		entry := PseudoEntry{
			FilePath: fields[0],
			Type:     fields[1],
		}

		// Logic to handle different types
		switch entry.Type {
		// ignore all the types without inline data
		// D: Directory, S: Symbolic Link, L: hard link, C: Char device
		// x: extended security capability
		case "D", "S", "L", "C", "x":
			continue

		// R: Regular File with INLINE DATA
		// Format: Path  Type  Time  Mode  UID  GID  Size  Offset  XattrIndex
		case "R":
			var parseErr error
			// Format: FilePath R Time Mode UID GID Size Offset <>
			entry.DataSize, parseErr = strconv.ParseInt(fields[6], 10, 64)
			if parseErr != nil {
				log.Fatalf("Invalid data size: %v", parseErr)
			}
			entry.DataOffset, parseErr = strconv.ParseInt(fields[7], 10, 64)
			if parseErr != nil {
				log.Fatalf("Invalid data offset: %v", parseErr)
			}
		default:
			return nil, nil, fmt.Errorf("unknown type in pseudo definition: %s", trimmed)
		}

		entries = append(entries, entry)

		if err == io.EOF {
			break
		}
	}

	return entries, headerBuffer, nil
}

// --- Infrastructure ---

// setupPipes creates a temporary directory and named pipes within it.
func setupPipesImpl(pipeNames ...string) (string, []string, error) {
	tempDir, err := os.MkdirTemp("", "snap-delta-")
	if err != nil {
		return "", nil, fmt.Errorf("cannot create temp dir: %w", err)
	}

	pipePaths := make([]string, 0, len(pipeNames))
	for _, name := range pipeNames {
		pipePath := filepath.Join(tempDir, name)
		if err := syscall.Mkfifo(pipePath, 0600); err != nil {
			os.RemoveAll(tempDir) // cleanup
			return "", nil, fmt.Errorf("cannot create fifo %s: %w", pipePath, err)
		}
		pipePaths = append(pipePaths, pipePath)
	}

	return tempDir, pipePaths, nil
}

// Tooling paths
var (
	toolboxSnap    = "/snap/toolbox/current"
	snapdSnap      = "/snap/snapd/current"
	toolMksquashfs = filepath.Join(toolboxSnap, "/usr/bin/mksquashfs")
	toolUnsquashfs = filepath.Join(toolboxSnap, "/usr/bin/unsquashfs")
	toolXdelta3    = filepath.Join(snapdSnap, "/usr/bin/xdelta3")
	toolHdiffz     = filepath.Join(toolboxSnap, "/usr/bin/hdiffz")
	toolHpatchz    = filepath.Join(toolboxSnap, "/usr/bin/hpatchz")
)

func getOptionForTool(tool string) string {
	switch tool {
	case "xdelta3":
		return "config"
	case "mksquashfs":
		return "-version"
	case "unsquashfs":
		return "-help"
	case "hdiffz":
		return "-v"
	case "hpatchz":
		return "-v"
	default:
		return ""
	}
}

// we do not run in snapd context, or snapd does not have yet required tooling
// use alternative path
func getAlternativeToolPath(tool string) string {
	switch tool {
	case "mksquashfs":
		return toolMksquashfs
	case "unsquashfs":
		return toolUnsquashfs
	case "xdelta3":
		return toolXdelta3
	case "hdiffz":
		return toolHdiffz
	case "hpatchz":
		return toolHpatchz
	default:
		return ""
	}
}

// helper to check for the presence of the required tools
func cmdFromSystemSnapImpl(toolPath string, cmdArgs ...string) (*exec.Cmd, error) {
	tool := filepath.Base(toolPath)
	loc := getAlternativeToolPath(tool)
	if _, err := os.Stat(loc); err != nil {
		if p, err := exec.LookPath(tool); err == nil {
			loc = p
		} else {
			return nil, fmt.Errorf("tool not found")
		}
	}
	// Verify execution
	toolOptions := getOptionForTool(tool)
	if err := exec.Command(loc, toolOptions).Run(); err != nil {
		return nil, fmt.Errorf("tool verification failed: [%s][%s] %v", tool, toolOptions, err)
	}
	// TODO: check minimal required version
	// the 'tool' in the env worked, so use that one
	return exec.Command(loc, cmdArgs...), nil
}

// --- Utils ---

func wrapErr(err error, msg string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s failed: %w", msg, err)
}

// ReusableMemFD wraps the file descriptor logic to reuse resources
type ReusableMemFD struct {
	Fd   int
	File *os.File
	Path string
}

func NewReusableMemFD(name string) (*ReusableMemFD, error) {
	// create mem backed file
	fd, err := unix.MemfdCreate(name, 0)
	if err != nil {
		return nil, err
	}
	// Wrap in os.File for easy Go IO, but we manage the FD manually mostly
	return &ReusableMemFD{
		Fd:   fd,
		File: os.NewFile(uintptr(fd), name),
		Path: fmt.Sprintf("/proc/self/fd/%d", fd),
	}, nil
}

func (m *ReusableMemFD) Reset() error {
	// Truncate file to 0 size
	if err := unix.Ftruncate(m.Fd, 0); err != nil {
		return err
	}
	// Seek to start
	_, err := m.File.Seek(0, 0)
	return err
}

func (m *ReusableMemFD) Close() {
	m.File.Close() // This closes the FD as well
}

// compIdToMksquashfsArgs converts SquashFS compression ID to a name.
func compIdToMksquashfsArgs(id uint16, mksqfsArgs []string) ([]string, error) {
	// compression map from squashfs spec
	m := map[uint16]string{1: "gzip", 2: "lzma", 3: "lzo", 4: "xz", 5: "lz4", 6: "zstd"}
	if s, ok := m[id]; ok {
		return append(mksqfsArgs, "-comp", s), nil
	}
	return nil, fmt.Errorf("unknown compression id: %d", id)
}

// superBlockFlagsToMksquashfsArgs converts SquashFS flags to mksquashfs arguments.
func superBlockFlagsToMksquashfsArgs(flags uint16, mksqfsArgs []string) ([]string, error) {
	// Always unset according to spec
	if (flags & flagCheck) != 0 {
		return nil, fmt.Errorf("unexpected value in superblock flags")
	}
	if (flags & flagNoFragments) != 0 {
		mksqfsArgs = append(mksqfsArgs, "-no-fragments")
	}
	// Note: The flag is "DUPLICATES", so if it's *not* set add -no-duplicates.
	if (flags & flagDuplicates) == 0 {
		mksqfsArgs = append(mksqfsArgs, "-no-duplicates")
	}
	if (flags & flagExports) != 0 {
		mksqfsArgs = append(mksqfsArgs, "-exports")
	}
	if (flags & flagNoXattrs) != 0 {
		mksqfsArgs = append(mksqfsArgs, "-no-xattrs")
	}
	if (flags & flagCompressorOptions) != 0 {
		return nil, fmt.Errorf("compression options was set in target, which is unsupported")
	}

	return mksqfsArgs, nil
}

// PathsMatchFuzzy compares two paths. It returns true if they are identical
// OR if they only differ by the numbers contained within them (versions).
// the returned score is 0: no match, 1: perfect match, 1<: 1 + number of fuzzy matches
func pathsMatchFuzzy(pathA, pathB string) int {
	// regex to find all sequences of digits.
	var digitPattern = regexp.MustCompile(`[0-9]+`)
	// split paths into components (directories/filenames)
	partsA := strings.Split(pathA, "/")
	partsB := strings.Split(pathB, "/")

	// Structural check: Paths must have the same depth
	if len(partsA) != len(partsB) {
		return 0
	}
	fuzzyMatches := 0
	// Component-wise comparison
	for i := 0; i < len(partsA); i++ {
		partA := partsA[i]
		partB := partsB[i]
		// idential, next one
		if partA == partB {
			continue
		}
		// normalize strings by replacing all numbers with a placeholder "#"
		// if components match, we assume it's version change
		normA := digitPattern.ReplaceAllString(partA, "#")
		normB := digitPattern.ReplaceAllString(partB, "#")
		if normA != normB {
			return 0
		}
		fuzzyMatches++
	}
	return 1 + fuzzyMatches
}

// GetSimilarityScore takes two int64s and returns a score
// maxDelta is maximum delta in %, otherwise 0 is returned

func getSimilarityScore(a, b int64, maxDelta int) int {
	// edge cases when both or one is zero
	if a == 0 && b == 0 {
		return 100
	}
	if a == 0 || b == 0 {
		return 0
	}

	// Convert to float for calculations
	floatA := math.Abs(float64(a))
	floatB := math.Abs(float64(b))
	minVal := math.Min(floatA, floatB)
	maxVal := math.Max(floatA, floatB)
	score := int(math.Round(minVal/maxVal) * 100)
	if score < 100-maxDelta {
		return 0
	} else {
		return score
	}
}

// RunWithContext runs the given command, but kills it if the context
// becomes done before the command finishes.
func runWithContext(ctx context.Context, cmd *exec.Cmd) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	var ctxDone uint32
	var wg sync.WaitGroup
	waitDone := make(chan struct{})

	wg.Add(1)
	go func() {
		select {
		case <-ctx.Done():
			atomic.StoreUint32(&ctxDone, 1)
			cmd.Process.Kill()
		case <-waitDone:
		}
		wg.Done()
	}()

	err := cmd.Wait()
	close(waitDone)
	wg.Wait()

	if atomic.LoadUint32(&ctxDone) != 0 {
		// do one last check to make sure the error from Wait is what we expect from Kill
		if err, ok := err.(*exec.ExitError); ok {
			if ws, ok := err.ProcessState.Sys().(unix.WaitStatus); ok && ws.Signal() == unix.SIGKILL {
				return ctx.Err()
			}
		}
	}
	return err
}

// helper to run multiple commands in one context
func runManyWithContext(ctx context.Context, cmds []*exec.Cmd, tasks []func(context.Context) error) error {
	// Create the group and a derived cancellable context
	g, gCtx := withContext(ctx)

	for _, cmd := range cmds {
		c := cmd
		g.Go(func() error {
			if err := c.Start(); err != nil {
				return err
			}
			// Create a channel to wait for the process result
			waitDone := make(chan error, 1)
			go func() {
				waitDone <- c.Wait()
			}()

			// Wait for the context to cancel OR for the process to finish (waitDone)
			select {
			case <-gCtx.Done():
				c.Process.Kill()
				<-waitDone
				return gCtx.Err()
			case err := <-waitDone:
				return err
			}
		})
	}

	for _, task := range tasks {
		t := task
		g.Go(func() error { return t(gCtx) })
	}

	// Return nil or the first error (if any) returned by the spawned go routines
	return g.Wait()
}

// --- import from errgroup for WinthContext implementation -----
// Group manages a collection of goroutines working on subtasks.
type Group struct {
	cancel func()
	wg     sync.WaitGroup

	errOnce sync.Once
	err     error
}

// WithContext returns a new Group and a derived Context that is
// cancelled the first time a function passed to Go returns an error.
func withContext(ctx context.Context) (*Group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{cancel: cancel}, ctx
}

// Wait blocks until all function calls from the Go method have returned,
// then returns the first non-nil error (if any) from them.
func (g *Group) Wait() error {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel()
	}
	return g.err
}

// Go calls the given function in a new goroutine.
func (g *Group) Go(f func() error) {
	g.wg.Add(1)

	go func() {
		defer g.wg.Done()

		if err := f(); err != nil {
			g.errOnce.Do(func() {
				g.err = err
				if g.cancel != nil {
					g.cancel()
				}
			})
		}
	}()
}

// --- end of import from errgroup for WinthContext implementation -----

// --- Main ---

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit("Missing operation")
	}

	// Setup subcommands
	var genSource, genTarget, genDelta, appSource, appTarget, appDelta string
	var xdelta3Tool, hdiffzTool bool
	generateCmd := flag.NewFlagSet("generate", flag.ExitOnError)
	generateCmd.StringVar(&genSource, "source", "", "source snap file (required)")
	generateCmd.StringVar(&genSource, "s", "", "source snap file (required)")
	generateCmd.StringVar(&genTarget, "target", "", "target snap file (required)")
	generateCmd.StringVar(&genTarget, "t", "", "target snap file (required)")
	generateCmd.StringVar(&genDelta, "delta", "", "delta output file (required)")
	generateCmd.StringVar(&genDelta, "d", "", "delta output file (required)")
	generateCmd.BoolVar(&xdelta3Tool, "xdelta3", false, "used complression tool")
	generateCmd.BoolVar(&hdiffzTool, "hdiffz", false, "used complression tool")

	applyCmd := flag.NewFlagSet("apply", flag.ExitOnError)
	applyCmd.StringVar(&appSource, "source", "", "source snap file (required)")
	applyCmd.StringVar(&appSource, "s", "", "source snap file (required)")
	applyCmd.StringVar(&appTarget, "target", "", "target snap file (required)")
	applyCmd.StringVar(&appTarget, "t", "", "target snap file (required)")
	applyCmd.StringVar(&appDelta, "delta", "", "delta input file (required)")
	applyCmd.StringVar(&appDelta, "d", "", "delta input file (required)")

	// Custom usage for subcommands
	generateCmd.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: go run snap-delta generate [options]")
		generateCmd.PrintDefaults()
	}
	applyCmd.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: go run snap-delta apply [options]")
		applyCmd.PrintDefaults()
	}
	var err error
	switch os.Args[1] {
	case "generate":
		generateCmd.Parse(os.Args[2:])
		if genSource == "" || genTarget == "" || genDelta == "" {
			generateCmd.Usage()
			log.Fatal("Missing required parameters for 'generate'")
		}
		var deltaFormat DeltaFormat
		if hdiffzTool {
			deltaFormat = SnapHdiffzFormat
		} else if xdelta3Tool {
			deltaFormat = SnapXdelta3Format
		} else {
			log.Fatal("missing delta tool setting for generate operation")
		}
		fmt.Printf("requested delta tool: 0x%X\n", deltaFormat)
		err = GenerateDelta(genSource, genTarget, genDelta, deltaFormat)

	case "apply":
		applyCmd.Parse(os.Args[2:])
		if appSource == "" || appTarget == "" || appDelta == "" {
			applyCmd.Usage()
			log.Fatal("Missing required parameters for 'apply'")
		}
		err = ApplyDelta(appSource, appDelta, appTarget)

	case "--help", "-h":
		printUsageAndExit("")

	default:
		printUsageAndExit(fmt.Sprintf("Unrecognised operation: %s", os.Args[1]))
	}

	if err != nil {
		log.Fatalf("Operation failed: %v", err)
	}
	fmt.Println("Operation completed successfully.")
}

// print help
func printUsageAndExit(msg ...string) {
	if len(msg) > 0 && msg[0] != "" {
		fmt.Fprintln(os.Stderr, msg[0])
		fmt.Fprintln(os.Stderr)
	}
	appName := filepath.Base(os.Args[0])
	fmt.Fprintln(os.Stderr, "Generate / apply 'smart' delta between source and target squashfs images.")
	fmt.Fprintln(os.Stderr, "Operations:")
	fmt.Fprintln(os.Stderr, "\tgenerate: generate delta between source and target")
	fmt.Fprintln(os.Stderr, "\tapply:    apply delta on the source")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Compulsory arguments:")
	fmt.Fprintln(os.Stderr, "\t--source | -s: source snap")
	fmt.Fprintln(os.Stderr, "\t--target | -t: target snap")
	fmt.Fprintln(os.Stderr, "\t--delta  | -d: delta between source and target snap")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Optional arguments:")
	fmt.Fprintln(os.Stderr, "\t--hdiffz:   use hdiffz(hpatchz) as delta tool to generate and apply delta on squashfs pseudo file definition")
	fmt.Fprintln(os.Stderr, "\t            As this delta tool does not support streaming, pseudo file definition is processed per file within the stream.")
	fmt.Fprintln(os.Stderr, "\t            !! only available during delta generation !!")
	fmt.Fprintln(os.Stderr, "\t--xdelta3:  use xdelta3 as delta tool to generate and apply delta on squashfs pseudo file definition")
	fmt.Fprintln(os.Stderr, "\t            !! only available during delta generation !!")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintf(os.Stderr, "\t%s generate --hdiffz --source <SNAP>  --target <SNAP> --delta <DELTA>\n", appName)
	fmt.Fprintf(os.Stderr, "\t%s apply             --source <SNAP>  --target <Snew NAP> --delta <DELTA>\n", appName)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Example:")
	fmt.Fprintf(os.Stderr, "\t%s generate --source core22_2134.snap --target core22_2140.snap --delta delta-core22-2134-2140.delta\n", appName)
	fmt.Fprintf(os.Stderr, "\t%s apply    --source core22_2134.snap --target core22_2140.snap --delta delta-core22-2134-2140.delta\n", appName)
	os.Exit(1)
}
