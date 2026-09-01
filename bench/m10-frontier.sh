#!/bin/bash
# M10 follow-up: where the size/CPU frontier actually lies.
#
# The M10 comparison shows the new format's apply costing 6-9x less CPU than
# snap-1-1-Hdiffz while its delta comes out 2-5x larger. Most of that extra size
# is not patch bytes -- it is whole blocks shipped verbatim because the cost
# model's absolute floor (MinSaving, 16 KiB) declined the run. Those runs are
# small: each costs the device one block of compression, about 128 KiB of
# plaintext, to save maybe 10 KiB of delta. The floor was set when a rejected run
# was often rejected for good reason, because its window was badly chosen; path
# anchoring changes that, so this re-measures the floor.
#
# Each row is one floor setting at the default rate: delta size, and what the
# device pays for it in compression. The hdiffz column is the delta the shipped
# format produces for the same pair, which is the size to beat.
set -u

SC=/home/ondrak/development/snapcraft
KRN=/home/ondrak/development/kernel
BIN=/home/ondrak/development/squashfs-delta/snap-delta-blocks
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

# The reference column is the snap-1-1-Hdiffz delta size measured in
# bench/m10-compare.sh for the same pair. post61->post60 is here because it is
# the worst ratio of the seven: a near-empty revision where the shipped format
# needs 252 KiB and this one needs 2.7 MiB, almost all of it literals.
pairs=(
	"$SC/snapcraft_8.13.2.post75_amd64.snap $SC/snapcraft_8.13.2.post77_amd64.snap 735386"
	"$SC/snapcraft_9.0.0.post61_amd64.snap $SC/snapcraft_9.0.0.post60_amd64.snap 252447"
	"$SC/snapcraft_8.13.2.post77_amd64.snap $SC/snapcraft_8.14.4.post129_amd64.snap 3886554"
	"$KRN/imx-kernel_6.18.0-1013.13_arm64.snap $KRN/imx-kernel_6.18.0-1014.14_arm64.snap 4242862"
)

for p in "${pairs[@]}"; do
	set -- $p
	s=$1
	t=$2
	ref=$3
	echo "==== $(basename "$s") -> $(basename "$t")"
	[ "$ref" != 0 ] && echo "   snap-1-1-Hdiffz delta $ref bytes"
	printf "%10s %12s %8s %14s %10s %10s\n" floor delta "vs ref" compresses avoided literals
	for floor in 0 2048 4096 8192 16384 65536; do
		out=$("$BIN" generate --blocks -no-verify -min-saving "$floor" \
			-s "$s" -t "$t" -d "$OUT/d" 2>&1)
		size=$(stat -c%s "$OUT/d" 2>/dev/null || echo 0)
		line=$(echo "$out" | grep "apply compresses")
		comp=$(echo "$line" | sed 's/.*compresses \(.*\) of .*/\1/')
		avoid=$(echo "$line" | sed 's/.*(\(.*\) avoided).*/\1/')
		# Anchored at the start of the report's own "literal" line: the
		# instructions line and the "runs as literal" line both contain the word.
		lit=$(echo "$out" | sed -n 's/^  literal  *\(.*\)$/\1/p')
		vs="-"
		[ "$ref" != 0 ] && vs=$(awk -v a="$size" -v b="$ref" 'BEGIN{printf "%.2fx", a/b}')
		printf "%10s %12s %8s %14s %10s %10s\n" "$floor" "$size" "$vs" "$comp" "$avoid" "$lit"
		rm -f "$OUT/d"
	done
	echo
done
