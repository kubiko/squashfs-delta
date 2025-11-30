package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
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

const (
	deltaHeaderSize           = 32 // bytes
	deltaFormatVersion uint16 = 0x101
	deltaMagicNumber   uint32 = 0xF989789C
	xdelta3MagicNumber uint32 = 0x00c4c3d6
	snapDeltaFormat           = "snapDeltaV1"
	xdelta3Format             = "xdelta3"
	detlaToolXdelta3   uint16 = 0x1
	detlaToolBsdiff    uint16 = 0x2
	defaultDeltaTool   uint16 = detlaToolXdelta3
)

var (
	// default compression level assumed 3
	// plain squashfs to squashfs delta size has no measurable gain between  3 and 9 comp level
	xdelta3PlainTuning = []string{"-3"}
	// gain in delta pseudo file between 3 and 7 comp level is 10 to 20% size reduction
	// delta size gain flattens at 7
	// no noticeable gain from changing source window size(-B) or bytes input window(-W)
	// or size compression duplicates window (-P)
	xdelta3Tuning = []string{"-7"}
)

// SquashFS Superblock Flags
const (
	flagCheck             uint16 = 0x0004
	flagNoFragments       uint16 = 0x0010
	flagDuplicates        uint16 = 0x0040 // Note: logic is inverted (default is duplicates)
	flagExports           uint16 = 0x0080
	flagNoXattrs          uint16 = 0x0200
	flagCompressorOptions uint16 = 0x0400
)

// Define tool paths as variables to match the script's structure
var (
	toolboxSnap = "/snap/toolbox/current"
	snapdSnap   = "/snap/snapd/current"
	mksquashfs  = filepath.Join(toolboxSnap, "/usr/bin/mksquashfs")
	unsquashfs  = filepath.Join(toolboxSnap, "/usr/bin/unsquashfs")
	xdelta3     = filepath.Join(snapdSnap, "/usr/bin/xdelta3")
	bsdiff      = filepath.Join(snapdSnap, "/usr/bin/bsdiff")
	bspatch     = filepath.Join(snapdSnap, "/usr/bin/bspatch")
)

// --- Main Execution ---

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit("Missing operation")
	}

	// Setup subcommands
	var genSource, genTarget, genDelta, appSource, appTarget, appDelta string
	var xdelta3Tool, bsdiffTool bool
	generateCmd := flag.NewFlagSet("generate", flag.ExitOnError)
	generateCmd.StringVar(&genSource, "source", "", "source snap file (required)")
	generateCmd.StringVar(&genSource, "s", "", "source snap file (required)")
	generateCmd.StringVar(&genTarget, "target", "", "target snap file (required)")
	generateCmd.StringVar(&genTarget, "t", "", "target snap file (required)")
	generateCmd.StringVar(&genDelta, "delta", "", "delta output file (required)")
	generateCmd.StringVar(&genDelta, "d", "", "delta output file (required)")
	generateCmd.BoolVar(&xdelta3Tool, "xdelta3", false, "used complression tool")
	generateCmd.BoolVar(&bsdiffTool, "bsdiff", false, "used complression tool")

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
		// default to xdelta3 if bsdiffTool is not defined
		deltaTool := defaultDeltaTool
		if bsdiffTool {
			deltaTool = detlaToolBsdiff
		}
		err = handleGenerateDelta(genSource, genTarget, genDelta, deltaTool)

	case "apply":
		applyCmd.Parse(os.Args[2:])
		if appSource == "" || appTarget == "" || appDelta == "" {
			applyCmd.Usage()
			log.Fatal("Missing required parameters for 'apply'")
		}
		err = handleApplyDelta(appSource, appDelta, appTarget)

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

// --- Help Function ---

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
	fmt.Fprintln(os.Stderr, "\t--bsdiff:   use bsdiff(bfpatch) as delta tool to generate and apply delta on squashfs pseudo file definition")
	fmt.Fprintln(os.Stderr, "\t            this delta tool does not support streams, full sized temp source and target pseudo files will be created!")
	fmt.Fprintln(os.Stderr, "\t            !! only available during delta generation !!")
	fmt.Fprintln(os.Stderr, "\t--xdelta3:  use xdelta3 as delta tool to generate and apply delta on squashfs pseudo file definition")
	fmt.Fprintln(os.Stderr, "\t            !! only available during delta generation !!")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintf(os.Stderr, "\t%s generate --bsdiff --source <SNAP>  --target <SNAP> --delta <DELTA>\n", appName)
	fmt.Fprintf(os.Stderr, "\t%s apply    --source <SNAP>  --target <SNAP> --delta <DELTA>\n", appName)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Example:")
	fmt.Fprintf(os.Stderr, "\t%s generate --source core22_2134.snap --target core22_2140.snap --delta delta-core22-2134-2140.delta\n", appName)
	fmt.Fprintf(os.Stderr, "\t%s apply    --source core22_2134.snap --target core22_2140.snap --delta delta-core22-2134-2140.delta\n", appName)
	os.Exit(1)
}

// Supported version of the snap delta algorythms
func SupportedDeltaFormats() string {
	return snapDeltaFormat + ";" + xdelta3Format
}

type SquashfsCommand func(args ...string) *exec.Cmd

// Check if all the required tools are actually present
// returns ready to use commands for xdelta3, bsdiff, mksquashfs and unsquashfs
func CheckSupportedDetlaFormats(ctx context.Context) (string, SquashfsCommand, SquashfsCommand, SquashfsCommand, SquashfsCommand, error) {
	// check if we have required tools available
	xdeltaCmd, err := checkForTooling(ctx, xdelta3, "xdelta3", "config")
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("missing snapd delta dependencies: %v", err)
	}
	// from here we can support plain xdelta3
	mksquashfsCmd, err := checkForTooling(ctx, mksquashfs, "mksquashfs", "-version")
	if err != nil {
		return xdelta3Format, xdeltaCmd, nil, nil, fmt.Errorf("missing snapd delta dependencies: %v", err)
	}
	// use -help since '-version' does not return 0 error code
	unsquashfsCmd, err := checkForTooling(ctx, unsquashfs, "unsquashfs", "-help")
	if err != nil {
		return xdelta3Format, xdeltaCmd, nil, nil, fmt.Errorf("missing snapd delta dependencies: %v", err)
	}
	bsdiffCmd, err := checkForTooling(ctx, bsdiff, "bsdiff", "config")
	if err != nil {
		return "", nil, nil, nil, fmt.Errorf("missing snapd delta dependencies: %v", err)
	}

	return snapDeltaFormat + ";" + xdelta3Format, xdeltaCmd, mksquashfsCmd, unsquashfsCmd, nil
}

// helper to check for the presence of the required tools
func checkForTooling(ctx context.Context, toolPath, tool, option string) (SquashfsCommand, error) {
	var loc string
	if _, err := os.Stat(toolPath); err == nil {
		loc = toolPath
	}
	if toolPath == "" {
		// trying 'tool' from the system
		var err error
		loc, err = exec.LookPath(tool)
		if err != nil {
			// no 'tool' in the env, so no deltas
			return nil, fmt.Errorf("no host system %s available", tool)
		}
	}
	if loc == "" {
		return nil, fmt.Errorf("unable to find %s in the search path", tool)
	}
	if err := exec.Command(loc, option).Run(); err != nil {
		// xdelta3 in the env failed to run, so no deltas
		return nil, fmt.Errorf("unable to use host system %s, running '%s %s' command failed: %v", tool, loc, option, err)
	}
	// TODO: check minimal required version
	// the 'tool' in the env worked, so use that one
	if ctx == nil {
		return func(toolArgs ...string) *exec.Cmd {
			return exec.Command(loc, toolArgs...)
		}, nil
	} else {
		return func(toolArgs ...string) *exec.Cmd {
			return exec.CommandContext(ctx, loc, toolArgs...)
		}, nil
	}
}

// parseCompression converts SquashFS compression ID to a name.
func parseCompression(id uint16, mksqfsArgs []string) ([]string, error) {
	switch id {
	case 1:
		return append(mksqfsArgs, "-comp", "gzip"), nil
	case 2:
		return append(mksqfsArgs, "-comp", "lzma"), nil
	case 3:
		return append(mksqfsArgs, "-comp", "lzo"), nil
	case 4:
		return append(mksqfsArgs, "-comp", "xz"), nil
	case 5:
		return append(mksqfsArgs, "-comp", "lz4"), nil
	case 6:
		return append(mksqfsArgs, "-comp", "zstd"), nil
	default:
		return nil, fmt.Errorf("unknown compression id: %d", id)
	}
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

func generatePlainXdelta3Delta(sourceSnap, targetSnap, delta string) error {
	xdelta3Cmd, err := checkForTooling(nil, "/usr/bin/xdelta3", "xdelta3", "config")
	if err != nil {
		return fmt.Errorf("missing xdelta3 tooling: %v", err)
	}
	xdelta3Args := append(xdelta3PlainTuning, "-f", "-e", "-s", sourceSnap, targetSnap, delta)
	cmd := xdelta3Cmd(xdelta3Args...)
	return cmd.Run()
}

func applyPlainXdelta3Delta(sourceSnap, delta, targetSnap string) error {
	xdelta3Cmd, err := checkForTooling(nil, "/usr/bin/xdelta3", "xdelta3", "config")
	if err != nil {
		return fmt.Errorf("missing xdelta3 tooling: %v", err)
	}
	xdelta3Args := []string{
		"-f", "-d", "-s", sourceSnap, delta, targetSnap,
	}
	cmd := xdelta3Cmd(xdelta3Args...)
	return cmd.Run()
}

// // run command in context, ensure commans is terminated if context is cancelled
func runService(ctx context.Context, cmd *exec.Cmd) error {
	if err := cmd.Start(); err != nil {
		return err
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	select {
	case <-ctx.Done():
		if err := cmd.Process.Kill(); err != nil {
			fmt.Println("Failed to kill process (%s): %v\n", cmd.Path, err)
		}
		<-waitDone
		return ctx.Err()
	case err := <-waitDone:
		return err
	}
}

// generate delta file.
func handleGenerateDelta(sourceSnap, targetSnap, delta string, deltaTool uint16) error {
	fmt.Println("Generating delta...")

	// Open target snap to read superblock
	targetFile, err := os.Open(targetSnap)
	if err != nil {
		return fmt.Errorf("failed to open target file %s: %w", targetSnap, err)
	}
	defer targetFile.Close()

	// Read superblock flags and check for custom compression options (flag flagCompressorOptions)
	flagsBuf := make([]byte, 2)
	if _, err := targetFile.ReadAt(flagsBuf, 24); err != nil {
		return fmt.Errorf("failed to read flags from target: %w", err)
	}
	targetFlags := binary.LittleEndian.Uint16(flagsBuf)

	if (targetFlags & flagCompressorOptions) != 0 {
		log.Println("Custom compression options detected. Falling back to plain xdelta3.")
		return generatePlainXdelta3Delta(sourceSnap, targetSnap, delta)
	}

	// --- Write Custom Header ---
	headerBuf := new(bytes.Buffer)

	// Magic number (32b)
	if err := binary.Write(headerBuf, binary.LittleEndian, uint32(deltaMagicNumber)); err != nil {
		return fmt.Errorf("failed to write snap-delta magic: %w", err)
	}

	// Version (16b)
	if err := binary.Write(headerBuf, binary.LittleEndian, uint16(deltaFormatVersion)); err != nil {
		return fmt.Errorf("failed to write snap-delta version: %w", err)
	}

	// Delta tool used (16b)
	if err := binary.Write(headerBuf, binary.LittleEndian, uint16(deltaTool)); err != nil {
		return fmt.Errorf("failed to write snap-delta tool info: %w", err)
	}

	// Read/Write Timestamp (32b at offset 8)
	tsBuf := make([]byte, 4)
	if _, err := targetFile.ReadAt(tsBuf, 8); err != nil {
		return fmt.Errorf("failed to read target snap timestamp: %w", err)
	}
	headerBuf.Write(tsBuf)

	// Read/Write Compression (16b at offset 20)
	compBuf := make([]byte, 2)
	if _, err := targetFile.ReadAt(compBuf, 20); err != nil {
		return fmt.Errorf("failed to read target snap compression: %w", err)
	}
	headerBuf.Write(compBuf)

	// Super block flags (16b) - read from target (16b at offset 24)
	headerBuf.Write(flagsBuf) // We already read this

	// Padding to deltaHeaderSize
	padding := make([]byte, deltaHeaderSize-headerBuf.Len())
	headerBuf.Write(padding)

	// --- End Header ---

	// Create delta file and write header there
	deltaFile, err := os.OpenFile(delta, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create delta file %s: %w", delta, err)
	}
	defer deltaFile.Close()
	if _, err := deltaFile.Write(headerBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write header to delta file: %w", err)
	}
	return generateXdelta3Delta(deltaFile, sourceSnap, targetSnap, delta)
}

func generateXdelta3Delta(deltaFile *os.File, sourceSnap, targetSnap, delta string) error {
	// 1. Setup pipes
	tempDir, pipePaths, err := setupPipes("source-pipe", "target-pipe")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	sourcePipe := pipePaths[0]
	targetPipe := pipePaths[1]

	// Run all as concurrent processes
	var wg sync.WaitGroup
	errChan := make(chan error, 3)

	// cancellable context for all tasks
	ctx, cancel := context.WithCancel(context.Background())
	// Defer cancel to clean up, though fail() will usually call it first
	defer cancel()

	// prepare tooling, make sure we have enough for snapDeltaV1 format
	supportedFormats, xdelta3Cmd, _, unsquashfsCmd, err := CheckSupportedDetlaFormats(ctx)
	if err != nil || !strings.Contains(supportedFormats, snapDeltaFormat) {
		return fmt.Errorf("failed to validate required tooling for snap-delta: %v", err)
	}

	// unsquashfs source -> source-pipe
	unsquashfsSourceArgs := []string{
		"-pf", sourcePipe, sourceSnap,
	}
	unsquashSourceCmd := unsquashfsCmd(unsquashfsSourceArgs...)

	// unsquashfs target -> target-pipe
	unsquashfsTargetArgs := []string{
		"-pf", targetPipe, targetSnap,
	}
	unsquashfsTargetCmd := unsquashfsCmd(unsquashfsTargetArgs...)

	// xdelta3 source-pipe, target-pipe -> delta-file (append)
	xdelta3Args := append(xdelta3Tuning, "-e", "-f", "-A", "-s", sourcePipe, targetPipe)
	xdeltaCmd := xdelta3Cmd(xdelta3Args...)
	xdeltaCmd.Stdout = deltaFile // Append to the already open delta file

	// handle fail of any task
	var failOnce sync.Once
	fail := func(err error) {
		failOnce.Do(func() {
			cancel() // <-- This signals all other tasks to stop
			errChan <- err
		})
	}

	// run unsquashfs source
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runService(ctx, unsquashSourceCmd); err != nil {
			if ctx.Err() == nil {
				fail(fmt.Errorf("unsquashfs (source) failed: %w", err))
			}
		}
	}()

	// run unsquashfs target
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runService(ctx, unsquashfsTargetCmd); err != nil {
			if ctx.Err() == nil {
				fail(fmt.Errorf("unsquashfs (target) failed: %w", err))
			}
		}
	}()

	// run xdelta3 source-pipe, target-pipe -> delta-file, appending
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runService(ctx, xdeltaCmd); err != nil {
			if ctx.Err() == nil {
				fail(fmt.Errorf("xdelta3 failed: %w", err))
			}
		}
	}()

	// Wait for all processes and collect errors
	wg.Wait()
	close(errChan)

	var allErrors []string
	for err := range errChan {
		allErrors = append(allErrors, err.Error())
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("delta generation failed:%s", strings.Join(allErrors, "\n"))
	}
	return nil
}

func generatebsdiffDelta(deltaFile *os.File, sourceSnap, targetSnap, delta string) error {
	// 1. create pseudo files
	tempDir, pseudoFilePaths, err := setupPipes("source", "target")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)
	sourcePf := pseudoFilePaths[0]
	targetPf := pseudoFilePaths[1]

	// 2. unsquash source and target
	// unsquashfs source -> pseudo file
	unsquashfsSourceArgs := []string{
		"-n", "-pf", sourcePf, sourceSnap,
	}
	unsquashSourceCmd := unsquashfsCmd(unsquashfsSourceArgs...)
	unsquashSourceCmd.run()

	// unsquashfs target -> pseudo file
	unsquashfsTargetArgs := []string{
		"-n", "-pf", targetPf, targetSnap,
	}
	unsquashfsTargetCmd := unsquashfsCmd(unsquashfsTargetArgs...)

}

// --- Apply Delta ---
func handleApplyDelta(sourceSnap, delta, targetSnap string) error {
	fmt.Println("Applying delta...")

	// Open delta file to read header
	deltaFile, err := os.Open(delta)
	if err != nil {
		return fmt.Errorf("failed to open delta file %s: %w", delta, err)
	}
	defer deltaFile.Close()

	// --- Read Custom Header ---
	header := make([]byte, deltaHeaderSize)
	n, err := io.ReadFull(deltaFile, header)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return fmt.Errorf("failed to read delta header: %w", err)
	}
	if n < 14 { // Size of magic + version + time + comp + flags
		return fmt.Errorf("delta file is too small to be valid")
	}

	headerReader := bytes.NewReader(header)
	var magicNumber uint32
	binary.Read(headerReader, binary.LittleEndian, &magicNumber)

	// Check for plain xdelta3
	if magicNumber == xdelta3MagicNumber {
		fmt.Println("This is a plain xdelta3 diff, falling back to plain xdelta3!!")
		return applyPlainXdelta3Delta(sourceSnap, delta, targetSnap)
	}

	if magicNumber != deltaMagicNumber {
		return fmt.Errorf("wrong magic number! (Expected 0x%X, Got 0x%X)", deltaMagicNumber, magicNumber)
	}

	var versionNumber uint16
	binary.Read(headerReader, binary.LittleEndian, &versionNumber)
	if versionNumber != deltaFormatVersion {
		return fmt.Errorf("mismatch delta version number! (Expected 0x%X, Got 0x%X)", deltaFormatVersion, versionNumber)
	}

	// delta tool used (16b)
	var deltaTool uint16
	binary.Read(headerReader, binary.LittleEndian, &deltaTool)
	if (deltaTool != detlaToolXdelta3) && (deltaTool != detlaToolBsdiff) {
		return fmt.Errorf("unrecognised delta tool! (Expected 0x%X or 0x%X but got 0x%X)", detlaToolXdelta3, detlaToolBsdiff, deltaTool)
	}

	var fstimeint uint32
	var targetCompressionID uint16
	var targetFlags uint16
	binary.Read(headerReader, binary.LittleEndian, &fstimeint)
	binary.Read(headerReader, binary.LittleEndian, &targetCompressionID)
	binary.Read(headerReader, binary.LittleEndian, &targetFlags)

	var mksqfsArgs []string
	mksqfsArgs, err = parseCompression(targetCompressionID, mksqfsArgs)
	if err != nil {
		return fmt.Errorf("failed to parse target compression from snap-delta: %v", err)
	}

	mksqfsArgs, err = parseSuperblockFlags(targetFlags, mksqfsArgs)
	if err != nil {
		return fmt.Errorf("failed to parse target supper block flags from snap-delta:%v", err)
	}

	// prepare pipes
	tempDir, pipePaths, err := setupPipes("source-pipe", "delta-pipe")
	if err != nil {
		return fmt.Errorf("failed to prepare pipes for snap delta: %v", err)
	}
	defer os.RemoveAll(tempDir)
	sourceSnapPipe := pipePaths[0]
	deltaPipe := pipePaths[1]

	// Run concurrent processes
	var wg sync.WaitGroup
	errChan := make(chan error, 4) // unsq, dd, xdelta, mksqfs
	// cancellable context for all tasks
	ctx, cancel := context.WithCancel(context.Background())
	// Defer cancel to clean up, though fail() will usually call it first
	defer cancel()

	// prepare tooling, make sure we have enough for snapDeltaV1 format
	supportedFormats, xdelta3Cmd, mksquashfsCmd, unsquashfsCmd, err := CheckSupportedDetlaFormats(ctx)
	if err != nil || !strings.Contains(supportedFormats, snapDeltaFormat) {
		return fmt.Errorf("failed to validate required tooling for snap-delta: %v", err)
	}

	// Setup command chains
	// xdelta3 between two pipes (source and delta pipes)
	xdelta3Args := []string{
		"-f", "-d", "-s", sourceSnapPipe, deltaPipe,
	}
	xdeltaCmd := xdelta3Cmd(xdelta3Args...)

	// mksquashfs from xdelta3 stream to target snap with correct extra arguments
	mksqfsFullArgs := []string{
		"-", // Source from stdin
		targetSnap,
		"-pf", "-", // Read patch file list from stdin
		"-no-progress",
		"-quiet",
		"-noappend",
		"-mkfs-time", strconv.FormatUint(uint64(fstimeint), 10),
	}
	mksqfsFullArgs = append(mksqfsFullArgs, mksqfsArgs...)
	mksqfsCmd := mksquashfsCmd(mksqfsFullArgs...)

	// Connect xdelta3 stdout to mksquashfs stdin
	mksqfsCmd.Stdin, err = xdeltaCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create pipe between xdelta3 and mksquashfs: %w", err)
	}

	// unsquash source snap into source pipe
	unsquashfsArgs := []string{
		"-n", "-pf", sourceSnapPipe, sourceSnap,
	}
	unsquashSourceCmd := unsquashfsCmd(unsquashfsArgs...)

	// handle fail of any task
	var failOnce sync.Once
	fail := func(err error) {
		failOnce.Do(func() {
			cancel() // <-- This signals all other tasks to stop
			errChan <- err
		})
	}

	// Start target consumers
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runService(ctx, mksqfsCmd); err != nil {
			if ctx.Err() == nil {
				fail(fmt.Errorf("mksquashfs (target) failed: %w", err))
			}
		}
	}()

	// start delta consumers: xdelta3
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runService(ctx, xdeltaCmd); err != nil {
			if ctx.Err() == nil {
				fail(fmt.Errorf("xdelta3 failed: %w", err))
			}
		}
	}()

	// Start source producers: unsquashfs source -> source-pipe
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runService(ctx, unsquashSourceCmd); err != nil {
			fail(fmt.Errorf("unsquashfs (source) failed: %w:", err))
		}
	}()

	// dd (Go copy) delta-body -> delta-pipe
	wg.Add(1)
	go func() {
		defer wg.Done()
		pipeF, err := os.OpenFile(deltaPipe, os.O_WRONLY, 0)
		if err != nil {
			fail(fmt.Errorf("failed to open delta pipe for writing: %w", err))
			return
		}
		defer pipeF.Close()

		// Make io.Copy cancellable by closing its pipe.
		go func() {
			<-ctx.Done()  // Wait for cancellation
			pipeF.Close() // This will force io.Copy to unblock with an error
		}()

		// Seek delta file to start of body
		if _, err := deltaFile.Seek(deltaHeaderSize, io.SeekStart); err != nil {
			// Check context, as this could fail if pipeF was closed
			if ctx.Err() == nil {
				fail(fmt.Errorf("failed to seek delta file: %w", err))
			}
			return
		}

		if _, err := io.Copy(pipeF, deltaFile); err != nil {
			// If context is cancelled, io.Copy will fail.
			// We check if this error was an *expected* cancellation.
			if ctx.Err() == nil {
				fail(fmt.Errorf("failed to copy delta body to pipe: %w", err))
			}
		}
	}()

	// Wait for all processes and collect errors
	wg.Wait()
	close(errChan)

	var allErrors []string
	for err := range errChan {
		allErrors = append(allErrors, err.Error())
	}

	if len(allErrors) > 0 {
		return fmt.Errorf("applying snap delta failed: %s", strings.Join(allErrors, "\n"))
	}
	return nil
}
