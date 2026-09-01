#!/bin/bash
# M9 follow-up: the size/CPU trade the better anchors expose.
#
# Path correspondence makes patches worth having on runs that used to ship as
# literals, so more of the target's plaintext now goes through the device's
# compressor -- which is the cost this whole format exists to avoid. The cost
# model's MinSavingRate is the dial: delta bytes a run must save per byte of
# plaintext it makes the device compress. It was tuned when anchors were poor, so
# this re-measures the curve now that they are not.
#
# Each row is one setting: delta size, what the device compresses, and what it
# decompresses to feed the patches.
set -u

SC=/home/ondrak/development/snapcraft
KRN=/home/ondrak/development/kernel
BIN=/home/ondrak/development/squashfs-delta/snap-delta-blocks
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

pairs=(
	"$KRN/imx-kernel_6.18.0-1013.13_arm64.snap $KRN/imx-kernel_6.18.0-1014.14_arm64.snap"
	"$SC/snapcraft_8.13.2.post77_amd64.snap $SC/snapcraft_8.14.4.post129_amd64.snap"
	"$SC/snapcraft_8.13.2.post75_amd64.snap $SC/snapcraft_8.13.2.post77_amd64.snap"
)

for p in "${pairs[@]}"; do
	set -- $p
	s=$1
	t=$2
	echo "==== $(basename "$s") -> $(basename "$t")"
	printf "%8s %12s %14s %10s %10s\n" rate delta compresses avoided decompresses
	for rate in 0.02 0.05 0.10 0.20 0.40; do
		out=$("$BIN" generate --blocks -no-verify -min-saving-rate "$rate" \
			-s "$s" -t "$t" -d "$OUT/d" 2>&1)
		size=$(stat -c%s "$OUT/d" 2>/dev/null || echo 0)
		line=$(echo "$out" | grep "apply compresses")
		comp=$(echo "$line" | sed 's/.*compresses \(.*\) of .*/\1/')
		avoid=$(echo "$line" | sed 's/.*(\(.*\) avoided).*/\1/')
		dec=$(echo "$line" | sed 's/.*decompresses //')
		printf "%8s %12s %14s %10s %10s\n" "$rate" "$size" "$comp" "$avoid" "$dec"
		rm -f "$OUT/d"
	done
	echo
done
