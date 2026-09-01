#!/bin/bash
# M9: what path-based correspondence buys over offset proximity.
#
# Each pair is generated twice from the same binary: -no-path-match reproduces
# the M8 behaviour (a run is diffed against wherever the preceding copy left
# off), and the default anchors each run on the source's version of the file the
# run's first block belongs to. Everything else -- the cost model, the window
# ratio, the run cap -- is held fixed, so the delta-size difference is the
# matcher's and nothing else's.
#
# Verification stays on in both modes: an anchor cannot make a delta wrong, and
# this is where that claim gets checked rather than asserted.
set -u

SC=/home/ondrak/development/snapcraft
KRN=/home/ondrak/development/kernel
IMX=/home/ondrak/development/imx-gadget
BIN=/home/ondrak/development/squashfs-delta/snap-delta-blocks
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

pairs=(
	# The plan's largest-churn pair: two minor versions apart.
	"$SC/snapcraft_8.13.2.post77_amd64.snap $SC/snapcraft_8.14.4.post129_amd64.snap"
	# The two the M8 matrix showed leaving the most on the floor.
	"$SC/snapcraft_8.14.4.post129_amd64.snap $SC/snapcraft_8.14.4.post194_amd64.snap"
	"$KRN/imx-kernel_6.18.0-1013.13_arm64.snap $KRN/imx-kernel_6.18.0-1014.14_arm64.snap"
	# A gadget revision that rebuilt its kernel: almost nothing copies.
	"$IMX/imx93-frdm_26-11-6.18.2-1.0.0_arm64.snap $IMX/imx93-frdm_26-12-6.18.20-2.0.0_arm64.snap"
	# And two that were already doing well, to prove nothing regressed.
	"$SC/snapcraft_8.13.2.post75_amd64.snap $SC/snapcraft_8.13.2.post77_amd64.snap"
	"$SC/snapcraft_9.0.0.post61_amd64.snap $SC/snapcraft_9.0.0.post60_amd64.snap"
)

for p in "${pairs[@]}"; do
	set -- $p
	s=$1
	t=$2
	echo "==== $(basename "$s") -> $(basename "$t")"
	for mode in offset path; do
		flag=""
		[ "$mode" = offset ] && flag="-no-path-match"
		echo "-- anchored by $mode"
		"$BIN" generate --blocks $flag -s "$s" -t "$t" -d "$OUT/d.$mode" 2>&1 |
			sed 's/^/   /'
	done
	so=$(stat -c%s "$OUT/d.offset" 2>/dev/null || echo 0)
	sp=$(stat -c%s "$OUT/d.path" 2>/dev/null || echo 0)
	if [ "$so" -gt 0 ] && [ "$sp" -gt 0 ]; then
		echo "   >> offset $so bytes, path $sp bytes, $(awk -v a="$so" -v b="$sp" 'BEGIN{printf "%+.1f%%", 100*(b-a)/a}')"
	fi
	rm -f "$OUT/d.offset" "$OUT/d.path"
	echo
done
