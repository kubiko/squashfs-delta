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
	"syscall"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

// snap delta support
// Custom Delta header (padded to 'deltaHeaderSize' size)
// generated delta is using following custom header to capture the delta content
// |       32b    |   16b   |    32b     |     16b     |        16b        |
// | magic number | version | time stamp | compression | super block flags |
// reference squashfs supperblock https://dr-emann.github.io/squashfs
// Optional compressor options are currently not supported, if target squashfs is detected to
// use those, we fallback to plain xdelta
// Delta between two snaps(squashfs) is generated on the squashfs pseudo file definition
// this represents uncompressed content of the squashfs packages, custom header data is
// later used as input parameters to mksquashfs when recreated target squashfs from the
// reconstructed pseudo file definition

// --- Constants & Configuration ---

const (
	// Delta Header Configuration
	deltaHeaderSize    = 32
	deltaFormatVersion = uint16(0x101)
	deltaMagicNumber   = uint32(0xF989789C)
	xdelta3MagicNumber = uint32(0x00c4c3d6)

	// Format Identifiers
	snapDeltaFormatXdelta3 = "snapDeltaV1Xdelta3"
	snapDeltaFormatHdiffz  = "snapDeltaV1Hdiffz"
	xdelta3Format          = "xdelta3"

	// Tool IDs
	detlaToolXdelta3 = uint16(0x1)
	detlaToolHdiffz  = uint16(0x2)
	defaultDeltaTool = detlaToolXdelta3

	// SquashFS Superblock Flags
	flagCheck             uint16 = 0x0004
	flagNoFragments       uint16 = 0x0010
	flagDuplicates        uint16 = 0x0040 // Note: logic is inverted (default is duplicates)
	flagExports           uint16 = 0x0080
	flagNoXattrs          uint16 = 0x0200
	flagCompressorOptions uint16 = 0x0400
)

// Tuning Parameters
var (
	// xdelta3 tuning
	// default compression level assumed 3
	// plain squashfs to squashfs delta size has no measurable gain between  3 and 9 comp level
	xdelta3PlainTuning = []string{"-3"}
	// gain in delta pseudo file between 3 and 7 comp level is 10 to 20% size reduction
	// delta size gain flattens at 7
	// no noticeable gain from changing source window size(-B) or bytes input window(-W)
	// or size compression duplicates window (-P)
	xdelta3Tuning = []string{"-7"}

	// hdiffz tuning
	hdiffzTuning  = []string{"-m-6", "-SD", "-c-zstd-21-24", "-d"}
	hpatchzTuning = []string{"-s-8m"}

	// unsquashfs tuning
	// by default unsquashfs would allocated ~2x256 for any size of squashfs image
	// We need to tame it down use different tuning for:
	// - generating detla: running server side -> no tuning, we have memory
	// - apply delta: possibly low spec systems
	unsquashfsTuningGenerate = []string{"-da", "128", "-fr", "128"}
	unsquashfsTuningApply    = []string{"-da", "8", "-fr", "8"}

	// mksquashfs tuning
	// by default mksquashfs can grab up to 25% of the physical memory
	// limit this as we migh run on contrained systems
	mksquashfsTuningApply = []string{"-mem-percent", "10"}

	// IO buffer size for efficient piping (1MB)
	CopyBufferSize = 1024 * 1024
)

// Tool Paths
var (
	toolboxSnap = "/snap/toolbox/current"
	snapdSnap   = "/snap/snapd/current"
	mksquashfs  = filepath.Join(toolboxSnap, "/usr/bin/mksquashfs")
	unsquashfs  = filepath.Join(toolboxSnap, "/usr/bin/unsquashfs")
	xdelta3     = filepath.Join(snapdSnap, "/usr/bin/xdelta3")
	hdiffz      = filepath.Join(toolboxSnap, "/usr/bin/hdiffz")
	hpatchz     = filepath.Join(toolboxSnap, "/usr/bin/hpatchz")
)

// --- Structs ---

// custom delta format header wrapping actual delta stream
type SnapDeltaHeader struct {
	Magic       uint32
	Version     uint16
	DeltaTool   uint16
	Timestamp   uint32
	Compression uint16
	Flags       uint16
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

// OK: type DeltaToolingCmd func(ctx context.Context, args ...string) *exec.Cmd
type DeltaToolingCmd func(ctx context.Context, args ...string) *exec.Cmd

// --- Memory Pools ---

// Pool for small bytes.Buffer
var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

// Pool for large IO buffers (1MB) to reduce GC pressure during io.Copy
var ioBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, CopyBufferSize)
		return &b
	},
}

// Helper to copy using pooled buffers
func copyBuffer(dst io.Writer, src io.Reader) (int64, error) {
	bufPtr := ioBufPool.Get().(*[]byte)
	defer ioBufPool.Put(bufPtr)
	return io.CopyBuffer(dst, src, *bufPtr)
}

func copyNBuffer(dst io.Writer, src io.Reader, n int64) (int64, error) {
	bufPtr := ioBufPool.Get().(*[]byte)
	defer ioBufPool.Put(bufPtr)
	return io.CopyBuffer(dst, io.LimitReader(src, n), *bufPtr)
}

// --- Main ---

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit("Missing operation")
	}

	// Global Context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		// default to xdelta3
		deltaTool := defaultDeltaTool
		if hdiffzTool {
			deltaTool = detlaToolHdiffz
		}
		fmt.Printf("requested delta tool: 0x%X\n", deltaTool)
		err = handleGenerateDelta(ctx, genSource, genTarget, genDelta, deltaTool)

	case "apply":
		applyCmd.Parse(os.Args[2:])
		if appSource == "" || appTarget == "" || appDelta == "" {
			applyCmd.Usage()
			log.Fatal("Missing required parameters for 'apply'")
		}
		err = handleApplyDelta(ctx, appSource, appDelta, appTarget)

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
	fmt.Fprintln(os.Stderr, "\t--hdiff`:   use hdiffz(hpatchz) as delta tool to generate and apply delta on squashfs pseudo file definition")
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

// Supported version of the snap delta algorythms
func SupportedDeltaFormats() string {
	return snapDeltaFormatHdiffz + "," + snapDeltaFormatXdelta3 + "," + xdelta3Format
}

// generate delta between source and target snap
func handleGenerateDelta(ctx context.Context, sourceSnap, targetSnap, delta string, deltaTool uint16) error {
	fmt.Println("Generating delta...")

	// we need to get some basic info from the target snap
	f, err := os.Open(targetSnap)
	if err != nil {
		return fmt.Errorf("open target: %w", err)
	}
	defer f.Close()

	// Check compressor options flag
	var flagsBuf [2]byte
	if _, err := f.ReadAt(flagsBuf[:], 24); err != nil {
		return fmt.Errorf("read flags: %w", err)
	}
	if binary.LittleEndian.Uint16(flagsBuf[:])&flagCompressorOptions != 0 {
		log.Println("Custom compression options detected. Falling back to plain xdelta3.")
		return generatePlainXdelta3Delta(ctx, sourceSnap, targetSnap, delta)
	}

	// Build delta header
	hdr := SnapDeltaHeader{
		Magic:     deltaMagicNumber,
		Version:   deltaFormatVersion,
		DeltaTool: deltaTool,
	}

	if err := hdr.loadDeltaHeaderFromSnap(f); err != nil {
		return err
	}

	headerBytes, err := hdr.toBytes()
	if err != nil {
		return fmt.Errorf("build delta header: %w", err)
	}

	// prepare delta file
	deltaFile, err := os.OpenFile(delta, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("create delta file: %w", err)
	}
	defer deltaFile.Close()

	if _, err := deltaFile.Write(headerBytes); err != nil {
		return fmt.Errorf("write delta header: %w", err)
	}

	// run delta producer for given deta tool
	switch deltaTool {
	case detlaToolXdelta3:
		return generateXdelta3Delta(ctx, deltaFile, sourceSnap, targetSnap)
	case detlaToolHdiffz:
		return generateHdiffzDelta(ctx, deltaFile, sourceSnap, targetSnap)
	default:
		return fmt.Errorf("unsupported delta tool 0x%X", hdr.DeltaTool)
	}
}

// apply delta between source and delta
func handleApplyDelta(ctx context.Context, sourceSnap, delta, targetSnap string) error {
	fmt.Println("Applying delta...")

	deltaFile, err := os.Open(delta)
	if err != nil {
		return fmt.Errorf("open delta: %w", err)
	}
	defer deltaFile.Close()

	// get delta header and check it
	hdr, err := readDeltaHeader(deltaFile)
	if err != nil {
		return err
	}

	if hdr.Magic == xdelta3MagicNumber {
		fmt.Println("Plain xdelta3 detected; using fallback.")
		return applyPlainXdelta3Delta(ctx, sourceSnap, delta, targetSnap)
	}

	if hdr.Magic != deltaMagicNumber {
		return fmt.Errorf("invalid magic 0x%X", hdr.Magic)
	}
	if hdr.Version != deltaFormatVersion {
		return fmt.Errorf("version mismatch %d!=%d", hdr.Version, deltaFormatVersion)
	}
	if hdr.DeltaTool != detlaToolXdelta3 && hdr.DeltaTool != detlaToolHdiffz {
		return fmt.Errorf("unsupported delta tool %d", hdr.DeltaTool)
	}

	// Prepare mksquashfs arguments from delta header
	mksqfsArgs := []string{}
	if mksqfsArgs, err = parseCompression(hdr.Compression, mksqfsArgs); err != nil {
		return fmt.Errorf("failed to parse compression from delta header:%v", err)
	}
	if mksqfsArgs, err = parseSuperblockFlags(hdr.Flags, mksqfsArgs); err != nil {
		return fmt.Errorf("failed to parse flags from delta header:%v", err)
	}
	// run delta apply for given deta tool
	switch hdr.DeltaTool {
	case detlaToolXdelta3:
		return applyXdelta3Delta(ctx, sourceSnap, targetSnap, deltaFile, hdr, mksqfsArgs)
	case detlaToolHdiffz:
		return applyHdiffzDelta(ctx, sourceSnap, targetSnap, deltaFile, hdr, mksqfsArgs)
	default:
		return fmt.Errorf("unsupported delta tool 0x%X", hdr.DeltaTool)
	}
}

func generateXdelta3Delta(ctx context.Context, deltaFile *os.File, sourceSnap, targetSnap string) error {
	// Ensure we have required tooling
	supportedFormats, xdelta3ToolCmdFn, _, unsquashfsCmdFn, _, _, err := CheckSupportedDetlaFormats(ctx)
	if err != nil || !strings.Contains(supportedFormats, snapDeltaFormatXdelta3) {
		return fmt.Errorf("missing delta tooling for xdelta3: %v", err)
	}

	// Setup all pipes
	tempDir, pipes, err := setupPipes("src-pipe", "trgt-pipe")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	sourcePipe := pipes[0]
	targetPipe := pipes[1]

	// Setup Context & ErrGroup
	g, gctx := errgroup.WithContext(ctx)

	// Prepare all the commands
	// unsquashfs source -> src-pipe
	unsquashSourceArg := append(unsquashfsTuningGenerate, "-n", "-pf", sourcePipe, sourceSnap)
	unsquashSourceCmd := unsquashfsCmdFn(gctx, unsquashSourceArg...)
	// unsquashfs target -> trgt-pipe
	unsquashTargetArg := append(unsquashfsTuningGenerate, "-pf", targetPipe, targetSnap)
	unsquashTargetCmd := unsquashfsCmdFn(gctx, unsquashTargetArg...)
	unsquashTargetCmd.Stdout = os.Stdout
	// xdelta3 src-pipe, trgt-pipe -> delta-file
	// Note: We use the tuning args and append specific inputs
	xdelta3Args := append(xdelta3Tuning, "-e", "-f", "-A", "-s", sourcePipe, targetPipe)
	xdelta3Cmd := xdelta3ToolCmdFn(gctx, xdelta3Args...)
	xdelta3Cmd.Stdout = deltaFile

	// run all prepared services
	// Run unsquashfs (Source)
	g.Go(func() error { return wrapErr(runService(unsquashSourceCmd), "unsquashfs (source)") })
	// Run unsquashfs (Target)
	g.Go(func() error { return wrapErr(runService(unsquashTargetCmd), "unsquashfs (target)") })
	// Run xdelta3
	g.Go(func() error { return wrapErr(runService(xdelta3Cmd), "xdelta3") })

	// wait: first error cancels all others
	return g.Wait()
}

func applyXdelta3Delta(ctx context.Context, sourceSnap, targetSnap string, deltaFile *os.File, hdr *SnapDeltaHeader, mksqfsArgs []string) error {
	// check if we have required tooling to apply delta
	supportedFormats, xdelta3CmdFn, mksquashfsCmdFn, unsquashfsCmdFn, _, _, err := CheckSupportedDetlaFormats(ctx)
	if err != nil {
		return fmt.Errorf("failed to validate required tooling for delta format: %v", err)
	}
	if !strings.Contains(supportedFormats, snapDeltaFormatXdelta3) {
		return fmt.Errorf("failed to validate required tooling for delta format'%s', supported: '%s': %v", supportedFormats, snapDeltaFormatHdiffz)
	}

	// setup pipes to apply delta
	tempDir, pipes, err := setupPipes("src", "delta")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	srcPipe := pipes[0]
	deltaPipe := pipes[1]

	// Setup Context & ErrGroup
	g, gctx := errgroup.WithContext(ctx)

	unsq := unsquashfsCmdFn(gctx, "-n", "-pf", srcPipe, sourceSnap)
	xdelta := xdelta3CmdFn(gctx, "-f", "-d", "-s", srcPipe, deltaPipe)

	// prepare target consumer (mksquashfs)
	sqfsArgs := append([]string{
		"-", targetSnap,
		"-pf", "-",
		"-noappend",
		"-quiet",
		"-mkfs-time", strconv.FormatUint(uint64(hdr.Timestamp), 10),
	}, mksqfsArgs...)
	sqfsArgs = append(sqfsArgs, mksquashfsTuningApply...)
	targetCmd := mksquashfsCmdFn(gctx, sqfsArgs...)
	targetCmd.Stdout = os.Stdout

	// connect xdelta → mksquashfs
	targetCmd.Stdin, err = xdelta.StdoutPipe()
	if err != nil {
		return fmt.Errorf("pipe xdelta→mksqfs: %w", err)
	}

	// unsquash source → src-pipe
	g.Go(func() error { return wrapErr(runService(unsq), "unsquashfs") })

	// xdelta3 filters (src-pipe, delta-pipe) → output
	g.Go(func() error { return wrapErr(runService(xdelta), "xdelta3") })

	// mksquashfs builds final snap
	g.Go(func() error { return wrapErr(runService(targetCmd), "mksquashfs") })

	// delta-body writer ("dd")
	g.Go(func() error {
		pf, err := os.OpenFile(deltaPipe, os.O_WRONLY, 0)
		if err != nil {
			return wrapErr(err, "delta pipe open")
		}
		defer pf.Close()

		// seek past header
		if _, err := deltaFile.Seek(deltaHeaderSize, io.SeekStart); err != nil {
			if gctx.Err() == nil {
				return wrapErr(err, "delta seek")
			}
			return nil
		}

		if _, err := copyBuffer(pf, deltaFile); err != nil && gctx.Err() == nil {
			return wrapErr(err, "delta copy")
		}
		return nil
	})

	return g.Wait()
}

// --- Hdiffz Implementations ---
func generateHdiffzDelta(ctx context.Context, deltaFile *os.File, sourceSnap, targetSnap string) error {

	// Ensure we have required tooling
	supportedFormats, _, _, unsquashfsCmdFn, hdiffzCmdFn, _, err := CheckSupportedDetlaFormats(ctx)
	if err != nil || !strings.Contains(supportedFormats, snapDeltaFormatHdiffz) {
		return fmt.Errorf("failed to validate required tooling for snap-delta: %v", err)
	}

	// Setup Context & ErrGroup
	g, gctx := errgroup.WithContext(ctx)

	// Setup all pipes
	tempDir, pipes, err := setupPipes("src-pipe", "trgt-pipe")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	sourcePipe := pipes[0]
	targetPipe := pipes[1]

	unsquashSourceArg := append(unsquashfsTuningGenerate, "-n", "-pf", sourcePipe, sourceSnap)
	unsquashSourceCmd := unsquashfsCmdFn(gctx, unsquashSourceArg...)

	unsquashTargetArg := append(unsquashfsTuningGenerate, "-n", "-pf", targetPipe, targetSnap)
	unsquashTargetCmd := unsquashfsCmdFn(gctx, unsquashTargetArg...)

	// unsquash source
	g.Go(func() error { return wrapErr(runService(unsquashSourceCmd), "unsqfs-src") })
	// unsquash target
	g.Go(func() error { return wrapErr(runService(unsquashTargetCmd), "unsqfs-trg") })

	sp, err := os.Open(sourcePipe)
	if err != nil {
		return fmt.Errorf("failed to open source pipe:%v")
	}
	tp, err := os.Open(targetPipe)
	if err != nil {
		return fmt.Errorf("failed to open target pipe:%v")
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
	_, err = unix.Write(srcMem.Fd, sourceHeaderBuff.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write source header to memFd: %w", err)
	}
	_, err = unix.Write(targetMem.Fd, targetHeaderBuff.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write target header to memFd: %w", err)
	}
	segmentDeltaSize, err := writeHdiffzToDeltaStream(gctx, deltaFile, 0, int64(sourceHeaderBuff.Len()), srcMem, targetMem, diffMem, hdiffzCmdFn)
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
	for i, te := range targetEntries {
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
					return fmt.Errorf("failed to skip source stream: %w", err)
				}
				sourceRead += toSkip
			}

			// TeeReader reads from source, writes to srcMem AND srcCRC
			mw := io.MultiWriter(srcMem.File, srcCRC)
			if _, err := copyNBuffer(mw, sourceReader, sourceSize); err != nil {
				return fmt.Errorf("failed to extract source segment: %w", err)
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
			return fmt.Errorf("failed to extract target segment: %w", err)
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
				return err
			}
			bufferPool.Put(headerBuf)
			totalDeltaSize += 8
		} else {
			// Files differ, run hdiffz and store the delta
			segSize, err := writeHdiffzToDeltaStream(gctx, deltaFile, sourceHeaderSize+sourceOffset, sourceSize, srcMem, targetMem, diffMem, hdiffzCmdFn)
			if err != nil {
				return err
			}
			totalDeltaSize += (segSize + 24)
			fmt.Printf("[%d/%d] Delta: %s (%d bytes -> %d bytes)\n", i, targetEntrieCount, te.FilePath, te.DataSize, segSize)
		}
	}

	// Validation
	if b := targetReader.Buffered(); b > 0 {
		return fmt.Errorf("target stream has %d bytes left unconsumed", b)
	}

	fmt.Printf("Delta generation complete. Total size: %d\n", totalDeltaSize)
	return g.Wait()
}

func applyHdiffzDelta(ctx context.Context, sourceSnap, targetSnap string, deltaFile *os.File, hdr *SnapDeltaHeader, mksqfsArgs []string) error {
	// check if we have required tooling to apply delta
	supportedFormats, _, mksquashfsCmdFn, unsquashfsCmdFn, _, hpatchzCmdFn, err := CheckSupportedDetlaFormats(ctx)
	if err != nil {
		return fmt.Errorf("failed to validate required tooling for delta format: %v", err)
	}
	if !strings.Contains(supportedFormats, snapDeltaFormatHdiffz) {
		return fmt.Errorf("failed to validate required tooling for delta format'%s', supported: '%s': %v", supportedFormats, snapDeltaFormatHdiffz)
	}

	// Setup Context & ErrGroup
	g, gctx := errgroup.WithContext(ctx)

	// Start Source Stream (unsquashfs)
	// We read FROM this pipe
	sourceCmd := unsquashfsCmdFn(gctx, "-n", "-pf", "-", sourceSnap)
	sourcePipe, err := sourceCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create source pipe: %w", err)
	}
	// unsquash source
	g.Go(func() error { return wrapErr(runService(sourceCmd), "unsquashfs") })

	// Wrap source in a buffered reader for efficient seeking/skipping
	sourceReader := bufio.NewReaderSize(sourcePipe, CopyBufferSize)

	// Start Target Stream consumer (mksquashfs)
	sqfsArgs := append([]string{
		"-", targetSnap,
		"-pf", "-",
		"-noappend",
		"-quiet",
		"-mkfs-time", strconv.FormatUint(uint64(hdr.Timestamp), 10),
	}, mksqfsArgs...)
	sqfsArgs = append(sqfsArgs, mksquashfsTuningApply...)
	targetCmd := mksquashfsCmdFn(gctx, sqfsArgs...)
	targetCmd.Stdout = os.Stdout

	targetStdin, err := targetCmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create target stdin pipe: %w", err)
	}

	// mksquash target
	g.Go(func() error { return wrapErr(runService(targetCmd), "mksquashfs") })

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
	if err := applyPatch(ctx, srcMem.Path, patchMem.Path, targetMem.Path, hpatchzCmdFn); err != nil {
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
	for _, entry := range targetEntries {
		// Read Control Int64
		var controlVal int64
		if err := binary.Read(deltaFile, binary.LittleEndian, &controlVal); err != nil {
			return fmt.Errorf("failed to read control value for %s: %w", entry.FilePath, err)
		}
		if controlVal <= 0 {
			// source file is idential to target file, just stream it
			// control value is negative index to the source header
			sourceIndex := int(-controlVal)
			srcEntry := sourceEntries[sourceIndex]

			// stream can only move forward, do sanity check we haven't advanced allready too far
			neededOffset := srcEntry.DataOffset + sourceHeaderSize
			if sourceReadCursor > neededOffset {
				return fmt.Errorf("critical: source stream cursor (%d) passed needed offset (%d). Generator logic flaw or unsorted input", sourceReadCursor, neededOffset)
			}
			// do we need to skip some data in the source stream?
			skip := neededOffset - sourceReadCursor
			if skip > 0 {
				copyNBuffer(io.Discard, sourceReader, skip)
				sourceReadCursor += skip
			}

			// ready to pump data from source stream to -> mksquashfs
			if _, err := copyNBuffer(targetStdin, sourceReader, srcEntry.DataSize); err != nil {
				return fmt.Errorf("failed to copy source data for %s: %w", entry.FilePath, err)
			}
			sourceReadCursor += srcEntry.DataSize

		} else {
			// source and tatget file differ, apply patch on the source
			// controlVal becomes SourceOffset
			srcOffset := controlVal
			var srcSize, patchSize int64

			if err := binary.Read(deltaFile, binary.LittleEndian, &srcSize); err != nil {
				return err
			}
			if err := binary.Read(deltaFile, binary.LittleEndian, &patchSize); err != nil {
				return err
			}
			// prepare patch file
			patchMem.Reset()
			if _, err := copyNBuffer(patchMem.File, deltaFile, patchSize); err != nil {
				return fmt.Errorf("failed to read patch data: %w", err)
			}

			// Prepare Source Segment
			srcMem.Reset()
			if srcSize > 0 {
				// align source stream to what patch applies to
				// mostl likely files from source are not present in the target
				// !! sourceOffset in delta includes source header size for consistency with header delta which has offset 0
				// offset values in the header start at 0 after the header ends

				if sourceReadCursor > srcOffset {
					return fmt.Errorf("critical: source cursor advanced too far for patch %s", entry.FilePath)
				}

				skip := srcOffset - sourceReadCursor
				if skip > 0 {
					copyNBuffer(io.Discard, sourceReader, skip)
					sourceReadCursor += skip
				}

				// Read from stream to MemFD
				if _, err := copyNBuffer(srcMem.File, sourceReader, srcSize); err != nil {
					return fmt.Errorf("failed to extract source segment for patch: %w", err)
				}
				sourceReadCursor += srcSize
			}

			// 3. Apply Patch
			targetMem.Reset()
			// if srcSize is 0, hpatchz treats it as creating a new file from patch
			if err := applyPatch(ctx, srcMem.Path, patchMem.Path, targetMem.Path, hpatchzCmdFn); err != nil {
				return fmt.Errorf("failed to patch file %s: %w", entry.FilePath, err)
			}
			// write reconstructed result to mksquashfs
			// DEBUG: fmt.Printf("%s\t(from %d bytes delta)\n", entry.FilePath, patchSize)
			if _, err := copyBuffer(targetStdin, targetMem.File); err != nil {
				return fmt.Errorf("failed to write patched data to mksquashfs: %w", err)
			}
		}
	}

	targetStdin.Close() // Close stdin to signal EOF to mksquashfs
	return g.Wait()
}

// --- Shared Helpers ---

// writeHdiffzToltaStream
func writeHdiffzToDeltaStream(ctx context.Context, deltaFile *os.File, sourceOffset, sourceSize int64, source, target, diff *ReusableMemFD, hdiffzCmdFn DeltaToolingCmd) (int64, error) {

	// Files differ, run hdiffz, use the /proc paths which remain valid for the reused FDs
	hdiffzArgs := append(hdiffzTuning, "-f", source.Path, target.Path, diff.Path)
	hdiffzCmd := hdiffzCmdFn(ctx, hdiffzArgs...)
	if output, err := hdiffzCmd.CombinedOutput(); err != nil {
		return 0, fmt.Errorf("hdiffz failed: %v %s", err, string(output))
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

func applyPatch(ctx context.Context, oldPath, diffPath, outPath string, hpatchzCmdFn DeltaToolingCmd) error {
	xdhpatchzArgs := append(hpatchzTuning, "-f", oldPath, diffPath, outPath)
	cmd := hpatchzCmdFn(ctx, xdhpatchzArgs...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%v: %s", err, string(output))
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
			log.Fatalf("unknown type in pseudo definition!!: %s", trimmed)
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
func setupPipes(pipeNames ...string) (string, []string, error) {
	tempDir, err := os.MkdirTemp("", "snap-delta-")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	var pipePaths []string
	for _, name := range pipeNames {
		pipePath := filepath.Join(tempDir, name)
		if err := syscall.Mkfifo(pipePath, 0600); err != nil {
			os.RemoveAll(tempDir) // cleanup
			return "", nil, fmt.Errorf("failed to create fifo %s: %w", pipePath, err)
		}
		pipePaths = append(pipePaths, pipePath)
	}

	return tempDir, pipePaths, nil
}

// Check if all the required tools are actually present
// returns ready to use commands for xdelta3, hdiffz, hpatchz, mksquashfs and unsquashfs
func CheckSupportedDetlaFormats(ctx context.Context) (string, DeltaToolingCmd, DeltaToolingCmd, DeltaToolingCmd, DeltaToolingCmd, DeltaToolingCmd, error) {
	// Run checks in parallel to speed up startup
	type res struct {
		name string
		cmd  DeltaToolingCmd
		err  error
	}

	check := func(path, tool, opt string) res {
		c, err := checkForTooling(ctx, path, tool, opt)
		return res{tool, c, err}
	}

	var wg sync.WaitGroup
	wg.Add(5)

	var xdelta, mksq, unsq, hdiff, hpatch res

	go func() { defer wg.Done(); xdelta = check(xdelta3, "xdelta3", "config") }()
	// use -help since '-version' does not return 0 error code
	go func() { defer wg.Done(); mksq = check(mksquashfs, "mksquashfs", "-version") }()
	go func() { defer wg.Done(); unsq = check(unsquashfs, "unsquashfs", "-help") }()
	go func() { defer wg.Done(); hdiff = check(hdiffz, "hdiffz", "-v") }()
	go func() { defer wg.Done(); hpatch = check(hpatchz, "hpatchz", "-v") }()

	wg.Wait()

	if xdelta.err != nil {
		return "", nil, nil, nil, nil, nil, xdelta.err
	}
	// Fallback level 1: only xdelta3
	if mksq.err != nil || unsq.err != nil {
		return xdelta3Format, xdelta.cmd, nil, nil, nil, nil, fmt.Errorf("squashfs tools missing")
	}
	// Fallback level 2: streaming xdelta3
	if hdiff.err != nil || hpatch.err != nil {
		return snapDeltaFormatXdelta3 + "," + xdelta3Format, xdelta.cmd, mksq.cmd, unsq.cmd, nil, nil, fmt.Errorf("hdiffz tools missing")
	}

	return snapDeltaFormatHdiffz + "," + snapDeltaFormatXdelta3 + "," + xdelta3Format,
		xdelta.cmd, mksq.cmd, unsq.cmd, hdiff.cmd, hpatch.cmd, nil
}

// helper to check for the presence of the required tools
func checkForTooling(ctx context.Context, toolPath, tool, option string) (DeltaToolingCmd, error) {
	loc := toolPath
	if _, err := os.Stat(toolPath); err != nil {
		if p, err := exec.LookPath(tool); err == nil {
			loc = p
		} else {
			return nil, fmt.Errorf("tool not found")
		}
	}
	// Verify execution
	if err := exec.Command(loc, option).Run(); err != nil {
		return nil, fmt.Errorf("tool verification failed: %v", err)
	}
	// TODO: check minimal required version
	// the 'tool' in the env worked, so use that one
	return func(c context.Context, args ...string) *exec.Cmd {
		if c == nil {
			return exec.Command(loc, args...)
		}
		return exec.CommandContext(c, loc, args...)
	}, nil
}

// --- Utils ---

func generatePlainXdelta3Delta(ctx context.Context, sourceSnap, targetSnap, delta string) error {
	cmd, err := checkForTooling(ctx, "/usr/bin/xdelta3", "xdelta3", "config")
	if err != nil {
		return fmt.Errorf("missing xdelta3 tooling: %v", err)
	}
	return cmd(ctx, append(xdelta3PlainTuning, "-f", "-e", "-s", sourceSnap, targetSnap, delta)...).Run()
}

func applyPlainXdelta3Delta(ctx context.Context, sourceSnap, delta, targetSnap string) error {
	cmd, err := checkForTooling(ctx, "/usr/bin/xdelta3", "xdelta3", "config")
	if err != nil {
		return fmt.Errorf("missing xdelta3 tooling: %v", err)
	}
	return cmd(ctx, "-f", "-d", "-s", sourceSnap, delta, targetSnap).Run()
}

// // run command in context, ensure commans is terminated if context is cancelled
func runService(cmd *exec.Cmd) error {
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Wait()
}

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

// Loads timestamp, compression, and flags from a squashfs superblock.
func (h *SnapDeltaHeader) loadDeltaHeaderFromSnap(f *os.File) error {
	buf := make([]byte, 26) // Read enough for all fields
	if _, err := f.ReadAt(buf, 8); err != nil {
		return err
	}

	// Timestamp @ offset 8 (u32): 8 -> 0
	h.Timestamp = binary.LittleEndian.Uint32(buf[0:4])
	// Compression @ offset 20 (u16): 20 -> 12
	h.Compression = binary.LittleEndian.Uint16(buf[12:14])
	// Flags @ offset 24 (u16): 24 -> 16
	h.Flags = binary.LittleEndian.Uint16(buf[16:18])
	return nil
}

// Serialises delta header into deltaHeaderSize
func (h *SnapDeltaHeader) toBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, h.Magic); err != nil {
		return nil, fmt.Errorf("failed to write header magic: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, h.Version); err != nil {
		return nil, fmt.Errorf("failed to write header version: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, h.DeltaTool); err != nil {
		return nil, fmt.Errorf("failed to write header tooling: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, h.Timestamp); err != nil {
		return nil, fmt.Errorf("failed to write header timestamp: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, h.Compression); err != nil {
		return nil, fmt.Errorf("failed to write header compression: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, h.Flags); err != nil {
		return nil, fmt.Errorf("failed to write header flags: %w", err)
	}
	// Pad to full size
	if buf.Len() < deltaHeaderSize {
		buf.Write(make([]byte, deltaHeaderSize-buf.Len()))
	}
	return buf.Bytes(), nil
}

func readDeltaHeader(r io.Reader) (*SnapDeltaHeader, error) {
	buf := make([]byte, deltaHeaderSize)
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if n < deltaHeaderSize {
		return nil, fmt.Errorf("header too short")
	}

	hdr := &SnapDeltaHeader{}
	if err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, hdr); err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}
	return hdr, nil
}

// parseCompression converts SquashFS compression ID to a name.
func parseCompression(id uint16, mksqfsArgs []string) ([]string, error) {
	// compression map from squashfs spec
	m := map[uint16]string{1: "gzip", 2: "lzma", 3: "lzo", 4: "xz", 5: "lz4", 6: "zstd"}
	if s, ok := m[id]; ok {
		return append(mksqfsArgs, "-comp", s), nil
	}
	return nil, fmt.Errorf("unknown compression: %d", id)
}

// parseSuperblockFlags converts SquashFS flags to mksquashfs arguments.
func parseSuperblockFlags(flags uint16, mksqfsArgs []string) ([]string, error) {
	if (flags & flagCheck) != 0 {
		return nil, fmt.Errorf("this does not look like Squashfs 4+ superblock flags")
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
		log.Println("warning: Custom compression options detected, created target snap is likely be different from target snap!")
	}

	return mksqfsArgs, nil
}

// regex to find all sequences of digits.
var digitPattern = regexp.MustCompile(`\d+`)

// PathsMatchFuzzy compares two paths. It returns true if they are identical
// OR if they only differ by the numbers contained within them (versions).
// the returned score is 0: no match, 1: perfect match, 1<: 1 + number of fuzzy matches
func pathsMatchFuzzy(pathA, pathB string) int {
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
