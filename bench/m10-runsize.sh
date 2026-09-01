#!/bin/bash
# M10 follow-up 2: does the run cap explain the churn-heavy pairs?
#
# The floor sweep (m10-frontier.sh) explains the quiet pairs, where most of the
# excess is whole blocks shipped verbatim. It explains nothing about the kernel
# pair: there the literals are already 0 B at every floor, so all 20 MiB is patch
# bytes, against 4.2 MiB for whole-image hdiffz over the same plaintext.
#
# The difference is that a patch run is a separate diff problem. At the default
# cap, 85 MiB of changed plaintext becomes 23 runs of at most 8 MiB, each matched
# against a window of at most 12 MiB. A rebuilt kernel changes a little in every
# module, so what one run needs may sit in a neighbouring run's window, and no
# run can reference what another already reconstructed.
#
# Raising the cap trades apply memory for that: scratch is roughly the run plus
# its window, so 32 MiB of run means about 80 MiB of scratch. The point of the
# sweep is to find out whether the size actually responds -- if it does not, run
# chunking is not the cause and the next suspect is window placement.
#
# It does not respond, and the sweep answers its own question: on the kernel pair
# 8M gives 21,026,657 bytes and every larger cap is worse (16M 21,625,425, 32M
# 23,167,660, 64M and 128M 23,171,912) while scratch climbs 20.75 -> 123.77 MiB,
# and post77 -> post129 is flat within 0.2%. So chunking was not the cause. The
# cause was that a window ended at the first raw stretch, which no cap can fix --
# see blockplan_patchrun.go's windowsFrom.
set -u

SC=/home/ondrak/development/snapcraft
KRN=/home/ondrak/development/kernel
BIN=/home/ondrak/development/squashfs-delta/snap-delta-blocks
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

# Reference sizes are the snap-1-1-Hdiffz deltas from bench/m10-compare.sh.
pairs=(
	"$KRN/imx-kernel_6.18.0-1013.13_arm64.snap $KRN/imx-kernel_6.18.0-1014.14_arm64.snap 4242862"
	"$SC/snapcraft_8.13.2.post77_amd64.snap $SC/snapcraft_8.14.4.post129_amd64.snap 3886554"
)

for p in "${pairs[@]}"; do
	set -- $p
	s=$1
	t=$2
	ref=$3
	echo "==== $(basename "$s") -> $(basename "$t")"
	echo "   snap-1-1-Hdiffz delta $ref bytes"
	printf "%10s %12s %8s %7s %12s %12s\n" cap delta "vs ref" runs patch scratch
	for cap in 8 16 32 64 128; do
		out=$("$BIN" generate --blocks -no-verify -min-saving 0 \
			-max-run $((cap << 20)) -s "$s" -t "$t" -d "$OUT/d" 2>&1)
		size=$(stat -c%s "$OUT/d" 2>/dev/null || echo 0)
		runs=$(echo "$out" | sed -n 's/.*, \([0-9]*\) patch run).*/\1/p' | head -1)
		patch=$(echo "$out" | sed -n 's/^  patch runs *\(.*\) of patch.*/\1/p')
		# Apply is what pays for the bigger cap, so take the scratch high-water
		# mark from a real apply rather than predicting it.
		scratch=$("$BIN" apply -s "$s" -d "$OUT/d" -t "$OUT/rebuilt" -stats 2>&1 |
			sed -n 's/^  peak scratch *\(.*\) in memfds.*/\1/p')
		cmp -s "$OUT/rebuilt" "$t" || scratch="$scratch MISMATCH"
		vs=$(awk -v a="$size" -v b="$ref" 'BEGIN{printf "%.2fx", a/b}')
		printf "%10s %12s %8s %7s %12s %12s\n" "${cap}M" "$size" "$vs" "$runs" "$patch" "$scratch"
		rm -f "$OUT/d" "$OUT/rebuilt"
	done
	echo
done
