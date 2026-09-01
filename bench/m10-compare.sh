#!/bin/bash
# M10: the new format against the one it is meant to replace.
#
# snap-1-1-Hdiffz turns both images into a pseudo-file definition, which means an
# apply decompresses the whole source and recompresses the whole target -- the CPU
# cost that started this work. snap-2-1-blocks copies unchanged blocks verbatim and
# only ever hands the compressor a patch run's plaintext.
#
# What is measured, per pair and per format: delta size, and for the apply, wall
# clock, CPU (user+sys, which is the number that matters on a device -- wall clock
# hides thread count), peak RSS over the whole process tree, and byte-identity of
# the result. Generation time is recorded too, but it is a publisher's cost and
# not what is being defended here.
#
# The new format is run twice. The default cost model spends the CPU headroom that
# path anchoring frees on a smaller delta; -min-saving-rate 0.20 holds roughly the
# CPU of the pre-anchor measurements instead. Which of the two should be the
# default is exactly what these numbers are for.
set -u

SC=/home/ondrak/development/snapcraft
KRN=/home/ondrak/development/kernel
IMX=/home/ondrak/development/imx-gadget
BIN=/home/ondrak/development/squashfs-delta/snap-delta-blocks
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

pairs=(
	"$SC/snapcraft_8.13.2.post75_amd64.snap $SC/snapcraft_8.13.2.post77_amd64.snap"
	"$SC/snapcraft_8.13.2.post77_amd64.snap $SC/snapcraft_8.14.4.post129_amd64.snap"
	"$SC/snapcraft_8.14.4.post129_amd64.snap $SC/snapcraft_8.14.4.post194_amd64.snap"
	"$SC/snapcraft_9.0.0.post61_amd64.snap $SC/snapcraft_9.0.0.post60_amd64.snap"
	"$KRN/imx-kernel_6.18.0-1013.13_arm64.snap $KRN/imx-kernel_6.18.0-1014.14_arm64.snap"
	"$IMX/imx93-frdm_26-12-6.18.20-2.0.0_arm64.snap $IMX/imx93-frdm_26-13-6.18.20-2.0.0_arm64.snap"
	"$IMX/imx93-frdm_26-11-6.18.2-1.0.0_arm64.snap $IMX/imx93-frdm_26-12-6.18.20-2.0.0_arm64.snap"
)

# timed runs a command under /usr/bin/time -v and prints one line: wall, cpu, rss.
timed() {
	local label=$1
	shift
	local tf="$OUT/time.txt"
	/usr/bin/time -v -o "$tf" "$@" >"$OUT/cmd.log" 2>&1
	local rc=$?
	if [ $rc -ne 0 ]; then
		echo "   $label FAILED rc=$rc"
		tail -3 "$OUT/cmd.log" | sed 's/^/     /'
		return 1
	fi
	# Elapsed is m:ss.ss; user and system are seconds. CPU is what a device
	# actually spends, so the two are summed rather than reported apart.
	local wall user sys rss
	wall=$(awk -F': ' '/Elapsed \(wall clock\)/{print $NF}' "$tf")
	user=$(awk -F': ' '/User time/{print $2}' "$tf")
	sys=$(awk -F': ' '/System time/{print $2}' "$tf")
	rss=$(awk -F': ' '/Maximum resident set size/{print $2}' "$tf")
	printf "   %-22s wall %-9s cpu %6.1fs  peak RSS %8.1f MiB\n" \
		"$label" "$wall" "$(awk -v u="$user" -v s="$sys" 'BEGIN{print u+s}')" \
		"$(awk -v r="$rss" 'BEGIN{print r/1024}')"
}

for p in "${pairs[@]}"; do
	set -- $p
	s=$1
	t=$2
	echo "==== $(basename "$s") -> $(basename "$t")"
	echo "   target $(stat -c%s "$t") bytes"

	for variant in hdiffz blocks blocks-cpu; do
		case $variant in
		hdiffz) gen_args=(--hdiffz) ;;
		blocks) gen_args=(--blocks -no-verify) ;;
		blocks-cpu) gen_args=(--blocks -no-verify -min-saving-rate 0.20) ;;
		esac
		d="$OUT/d.$variant"
		if ! timed "generate $variant" "$BIN" generate "${gen_args[@]}" -s "$s" -t "$t" -d "$d"; then
			continue
		fi
		size=$(stat -c%s "$d")
		printf "   %-22s %d bytes (%.2f%% of target)\n" "delta $variant" "$size" \
			"$(awk -v a="$size" -v b="$(stat -c%s "$t")" 'BEGIN{print 100*a/b}')"
		if ! timed "apply $variant" "$BIN" apply -s "$s" -d "$d" -t "$OUT/rebuilt"; then
			continue
		fi
		if cmp -s "$OUT/rebuilt" "$t"; then
			echo "   apply $variant: identical"
		else
			echo "   apply $variant: MISMATCH"
		fi
		rm -f "$OUT/rebuilt" "$d"
	done
	echo
done
