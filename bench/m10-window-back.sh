#!/bin/bash
# M10 follow-up 3: which side of the anchor should the window sit on?
#
# The floor sweep explained the quiet pairs and the run-cap sweep ruled out
# chunking, which left the anchor itself. A window starts at its anchor and
# extends forward, so it finds the corresponding source bytes when they moved
# earlier and misses them when they moved later. Inside one large file that
# matters: imx-kernel's kernel.img is a 65.7 MB FIT image, 64% of the snap's
# plaintext, and when the compressed kernel inside it changes size everything
# after the change shifts by an arbitrary amount. hdiffz diffs that file whole to
# 3.0 MB with the shipped tuning, so the content does match -- our runs were
# looking in the wrong direction for it.
#
# Each row places that fraction of the window before the anchor. 0 is the old
# forward-only placement and 0.5 centres the window; the window size does not
# change, so neither does apply memory or the plaintext decompressed.
#
# The hypothesis was wrong, both before the window fix and after it. Every pair
# is flat across 0, 0.1 and 0.25 -- within 0.1% -- and then falls apart at 0.4
# and 0.5, imx-kernel to 7.24 and 13.95 MiB, post61 -> post60 from 100,705 bytes
# to 1.39 and 2.74 MiB. Budget spent behind the anchor is budget not spent ahead
# of it, and the match is nearly always ahead. What was actually starving those
# runs was window fragmentation at raw stretches, not window placement; the dial
# is kept at 0 and this script is why.
set -u

SC=/home/ondrak/development/snapcraft
KRN=/home/ondrak/development/kernel
IMX=/home/ondrak/development/imx-gadget
BIN=/home/ondrak/development/squashfs-delta/snap-delta-blocks
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

# Reference sizes are the snap-1-1-Hdiffz deltas from bench/m10-compare.sh.
pairs=(
	"$KRN/imx-kernel_6.18.0-1013.13_arm64.snap $KRN/imx-kernel_6.18.0-1014.14_arm64.snap 4242862"
	"$SC/snapcraft_8.13.2.post77_amd64.snap $SC/snapcraft_8.14.4.post129_amd64.snap 3886554"
	"$SC/snapcraft_8.13.2.post75_amd64.snap $SC/snapcraft_8.13.2.post77_amd64.snap 735386"
	"$SC/snapcraft_9.0.0.post61_amd64.snap $SC/snapcraft_9.0.0.post60_amd64.snap 252447"
	"$IMX/imx93-frdm_26-11-6.18.2-1.0.0_arm64.snap $IMX/imx93-frdm_26-12-6.18.20-2.0.0_arm64.snap 658794"
)

for p in "${pairs[@]}"; do
	set -- $p
	s=$1
	t=$2
	ref=$3
	echo "==== $(basename "$s") -> $(basename "$t")"
	echo "   snap-1-1-Hdiffz delta $ref bytes"
	printf "%10s %12s %8s %12s %14s %10s\n" back delta "vs ref" patch compresses avoided
	for back in 0 0.1 0.25 0.4 0.5; do
		out=$("$BIN" generate --blocks -no-verify -min-saving 0 -window-back "$back" \
			-s "$s" -t "$t" -d "$OUT/d" 2>&1)
		size=$(stat -c%s "$OUT/d" 2>/dev/null || echo 0)
		patch=$(echo "$out" | sed -n 's/^  patch runs *\(.*\) of patch.*/\1/p')
		line=$(echo "$out" | grep "apply compresses")
		comp=$(echo "$line" | sed 's/.*compresses \(.*\) of .*/\1/')
		avoid=$(echo "$line" | sed 's/.*(\(.*\) avoided).*/\1/')
		vs=$(awk -v a="$size" -v b="$ref" 'BEGIN{printf "%.2fx", a/b}')
		printf "%10s %12s %8s %12s %14s %10s\n" "$back" "$size" "$vs" "$patch" "$comp" "$avoid"
		rm -f "$OUT/d"
	done
	echo
done
