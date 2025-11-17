# squashfs-delta

Generate / apply 'smart' delta between source and target squashfs images.

## How to use

Ensure newest squashfs-tools to avoid buffer overrung error

## How to use

# Generate / apply 'smart' delta between source and target squashfs images.
Operations:
	generate: generate delta between source and target
	apply:    apply delta on the source

Arguments:
  ```
  --source | -s: source snap
  --target | -t: target snap
  --delta  | -d: delta between source and target snap
  ```

Usage:
  ```
  $ ./squashfs-delta[.sh] generate --source <source snap file>  --target <output file> --delta <delta file>
  $ ./squashfs-delta[.sh] apply    --source <source snap file>  --target <output file> --delta <delta file>
  ```

Example:
  ```
  $ ./squashfs-delta[.s]h generate --source core22_2134.snap --target core22_2140.snap --delta delta-core22-2134-2140.delta
  $ ./squashfs-delta[.sh] apply    --source core22_2134.snap --target core22_2140.snap --delta delta-core22-2134-2140.delta
  ```

## Go version
  ```
  $ go build -o squashfs-delta snap-delta.go
  ```

**Prerequisites:**
Ensure newest squashfs-tools to avoid buffer overrung error
