# squashfs-delta

Generate and apply 'smart' deltas between source and target squashfs (snap) images.

## Prerequisites

Before using this tool, ensure the following requirements are met:

* **squashfs-tools:** You must have the newest version of `squashfs-tools` installed to avoid buffer overrun errors.
* **Delta Tools:** Ensure the necessary delta tools (`xdelta3`, `hdiffz`, `bsdiff`) are installed and available in your PATH.
    * *Alternative:* You can install the [toolbox snap](https://snapcraft.io/toolbox) which contains these utilities.


## Build (Go Version)

To build the Go implementation from source:

```bash
go build -o squashfs-delta snap-delta.go
```

## Usage

### Operations
* `generate`: Create a delta file between a source and a target image.
* `apply`: Apply a delta file to a source image to recreate the target.

### Arguments
* `--source` / `-s`: The source snap file (original version).
* `--target` / `-t`: The target snap file (new version or output file).
* `--delta`  / `-d`: The delta file path.

### Extra arguments for generate operation
* `--xdelta3` : Use xdelta3 tool, go and bash supported and equal.
* `--hdiffz`  : Use hdiffz/hpatchz too, go and bash implementation differ, delta is incompatible!
* `--bsdiff`  : Use bsdiff/bspatch, only bash implementation is supported, mostly for reference.

### Command Syntax

```bash
# General Syntax
$ ./squashfs-delta[.sh] <operation> --source <file> --target <file> --delta <file>
```

### Examples
### Generating a delta:

```bash
$ ./squashfs-delta generate --source core22_2134.snap --target core22_2140.snap --delta core22.delta
```
### Applying a delta:

```bash
$ ./squashfs-delta apply --source core22_2134.snap --target core22_2140_restored.snap --delta core22.delta
```

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
* **Method:** Processes the pseudo-stream by identifying segments of source/target streams that belong to the same (or related) files.
    * Includes **fuzzy matching logic** to detect library version changes, allowing old and new versions to be delta-compressed against each other.
* **Pros:** Faster generation times; memory usage is proportional only to the largest processed file in the squashfs.

> [!WARNING]
> **Incompatibility Notice**
> When using `hdiffz`, the Go and Bash versions are not compatible. You must use the same version for applying the delta that was used to generate it.
