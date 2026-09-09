# squashfs-delta

Generate and apply 'smart' deltas between source and target squashfs (snap) images.

## Prerequisites

Before using this tool, ensure the following requirements are met:

* **squashfs-tools:** You must have the newest version of `squashfs-tools` installed to avoid buffer overrun errors.
* **Delta Tools:** The Go tool needs `hdiffz`/`hpatchz` for `--hdiffz` and `xdelta3` for `--xdelta3`. The bash script needs those too, plus `bsdiff`/`bspatch` for `--bsdiff`.
    * *Alternative:* Install the [toolbox snap](https://snapcraft.io/toolbox), which contains these utilities. The Go tool looks for `hdiffz`, `hpatchz`, `xz`, `mksquashfs` and `unsquashfs` under `/snap/toolbox/current`, and for `xdelta3` under `/snap/snapd/current`, falling back to `$PATH` for any of them.
* **Compressor:** Applying a `snap-2-1-hdiffz` delta recompresses blocks with the image's own compressor: the `xz` binary for xz images, or liblzo2/libzstd for lzo and zstd images, preferring the copies the snapd snap bundles under `/snap/snapd/current`.


## Build (Go Version)

To build the Go implementation from source:

```bash
go build -o snap-delta .
```

## Usage

The Go tool builds as `snap-delta`; the bash script ships as `snap-delta.sh`
(also under its old name, `squashfs-delta.sh`). They are separate
implementations -- see Technical Details -- and their deltas are not
interchangeable.

### Commands (Go tool)
* `generate`: create a delta between a source and a target image.
* `apply`: apply a delta to a source image to reconstruct the target.
* `selftest`: check that the local compressor reproduces an image's own blocks.
* `inspect`: dump the byte layout of one or more squashfs images.

`snap-delta <command> --help` lists that command's options with their defaults,
and is the authority on both.

### Common arguments
* `--source` / `-s`: the source snap file (the version already on disk).
* `--target` / `-t`: the target snap file -- the new version for `generate`, the
  image to write for `apply`.
* `--delta`  / `-d`: the delta file path.

### generate
Pick exactly one delta format:
* `--hdiffz`: the `snap-2-1-hdiffz` block-plan format, which lets a device
  reassemble the target without recompressing unchanged blocks.
* `--xdelta3`: the `snap-1-1-xdelta3` format, xdelta3 over the squashfs
  pseudo-file definition.

These tune `--hdiffz` and are refused with any other format:
* `--max-run`: cap the plaintext one patch run reconstructs, which bounds what
  applying costs the device at once.
* `--min-saving`, `--min-saving-rate`, `--window-ratio`, `--window-back`: the
  cost model -- how much a run must save to be worth the device compressing it,
  and how much source context it is diffed against.
* `--no-patch-runs`: ship every changed block verbatim, asking the device for no
  data-block compression at all.
* `--no-path-match`: anchor runs by source offset alone, ignoring filenames.
* `--no-verify`: skip the pass that reconstructs the target from the finished
  delta and compares it byte for byte. That pass runs by default.
* `--run-log`: log each run considered -- plaintext, window, cost -- to stderr,
  under a header explaining the columns.

### apply
The format is read from the delta, so no format option is given.
* `--jobs` / `-j`: how many blocks the compressor may work on at once, which is
  what uses the machine's cores. Each job holds its own encoder state, so
  raising it raises peak memory as well as speed.
* `--max-run`: refuse a delta that would need more than N bytes reconstructed at
  once, rather than honouring whatever the generator chose.
* `--stats`: report instructions, bytes copied and compressed, and peak memory.

### Command Syntax

```bash
# General Syntax
$ ./snap-delta <command> --source <file> --target <file> --delta <file> [options]
$ ./snap-delta.sh <operation> --source <file> --target <file> --delta <file>
```

### Examples
### Generating a delta:

```bash
$ ./snap-delta generate --hdiffz --source core22_2134.snap --target core22_2140.snap --delta core22.delta
```
### Applying a delta:

```bash
$ ./snap-delta apply --stats --source core22_2134.snap --delta core22.delta --target core22_2140_restored.snap
```
### Checking this machine can reproduce an image's blocks:

```bash
$ ./snap-delta selftest core22_2140.snap
```

### Bash script
`snap-delta.sh generate` takes one of `--xdelta3`, `--hdiffz` or `--bsdiff`, and
`snap-delta.sh apply` reads the tool back out of the delta's own header. Run it
with no arguments for its usage.

## Technical Details: Bash vs. Go
The Go and Bash versions of this tool handle delta generation differently depending on the algorithm selected.

### 1. xdelta3 (Shared Implementation)
Both the Bash and Go versions share the same implementation logic for `xdelta3`.
* **Method:** The source and target are always streamed and compared directly.
* **Pros:** Efficient resource usage for both generating and applying the delta.
* **Cons:** The produced delta size is not strictly optimal compared to other methods.

### 2. hdiffz (and bsdiff)
The implementation for `hdiffz` differs significantly between the two versions and they are **not compatible**.

#### Bash Implementation
* **Supported Tools:** `hdiffz`, `bsdiff`.
* **Method:** Assumes unlimited resources. It generates the entire squashfs pseudo-definition as a temporary file and performs the delta operation on the whole definition.
* **Pros:** Produces the most size-efficient delta.
* **Cons:** Can be **very slow** and demands significant working disk space depending on the squashfs size and compression level.
    * *Note: `bsdiff` generation can be exceptionally slow.*

#### Go Implementation
* **Supported Tools:** `hdiffz` only.
* **Method:** The `snap-2-1-hdiffz` format works on the images' own compressed
  blocks rather than a pseudo-definition. The generator splits both images
  into squashfs blocks, copies blocks that are byte-identical, and emits
  small `hdiffz` patches for the runs that changed, which the device
  reconstructs by patching a source window and recompressing.
* **Pros:** Deltas are byte-exact against the target -- the generator proves
  that before shipping one and the applier re-proves it at apply time -- and
  applying is a single streaming pass with bounded memory, spending device
  CPU only on the blocks that actually changed.
* **Cons:** Applying needs the device's `hpatchz` and the image's compressor
  (the `xz` binary, or snapd's bundled liblzo2/libzstd), and fragment-based
  images are not supported.

> [!WARNING]
> **Incompatibility Notice**
> The Go `--hdiffz` generator produces `snap-2-1-hdiffz` block-plan deltas,
> while the Bash tool produces the older snap-1-1 pseudo-file deltas. They
> are not compatible -- apply a delta with the generator that produced it.
