#!/bin/bash
# M10 follow-up 4: what does the MinSaving floor actually buy the device?
#
# bench/m10-frontier.sh, re-run after the window fix, says floor 0 gives the
# smallest delta on every pair -- 3.2x smaller than the 16 KiB default on
# post75->post77 and 11x smaller on post61->post60 -- while the compression it
# asks the device for rises by only 1-2 points. On that evidence the floor looks
# like a pure loss.
#
# But delta size and "plaintext compressed" are not the whole cost. The floor was
# put in for process overhead: every run a floor of 0 accepts is three more forks
# on the device (hdiffz's hpatchz, xz -dc, xz), and a small run's fork triple can
# cost more than compressing its few blocks. That does not show up in the
# generator's report at all, so it is measured here directly: run count, and the
# apply's own wall clock, CPU and peak RSS.
#
# The gadget pairs are in even though the frontier sweep skipped them, because
# they are where the floor's justification came from: a revision whose entire
# change is one 10.6 KiB run.
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
	"$SC/snapcraft_9.0.0.post61_amd64.snap $SC/snapcraft_9.0.0.post60_amd64.snap"
	"$KRN/imx-kernel_6.18.0-1013.13_arm64.snap $KRN/imx-kernel_6.18.0-1014.14_arm64.snap"
	"$IMX/imx93-frdm_26-11-6.18.2-1.0.0_arm64.snap $IMX/imx93-frdm_26-12-6.18.20-2.0.0_arm64.snap"
	"$IMX/imx93-frdm_26-12-6.18.20-2.0.0_arm64.snap $IMX/imx93-frdm_26-13-6.18.20-2.0.0_arm64.snap"
)

for p in "${pairs[@]}"; do
	set -- $p
	s=$1
	t=$2
	echo "==== $(basename "$s") -> $(basename "$t")"
	printf "%8s %12s %6s %6s %8s %9s %8s %10s\n" \
		floor delta runs lits compress wall cpu "peak RSS"
	for floor in 0 2048 4096 16384; do
		gen=$("$BIN" generate --blocks -no-verify -min-saving "$floor" \
			-s "$s" -t "$t" -d "$OUT/d" 2>&1)
		size=$(stat -c%s "$OUT/d" 2>/dev/null || echo 0)
		# The instructions line reads "N (a copy, b literal, c patch run)".
		runs=$(echo "$gen" | sed -n 's/^  instructions .*, \([0-9]*\) patch run).*/\1/p')
		lits=$(echo "$gen" | sed -n 's/^  instructions .*copy, \([0-9]*\) literal.*/\1/p')
		comp=$(echo "$gen" | sed -n 's/.*apply compresses \(.*\) of .* avoided.*/\1/p' |
			sed 's/ of .*//')
		/usr/bin/time -v -o "$OUT/time.txt" "$BIN" apply \
			-s "$s" -d "$OUT/d" -t "$OUT/rebuilt" >"$OUT/apply.log" 2>&1
		rc=$?
		wall=$(awk -F': ' '/Elapsed \(wall clock\)/{print $NF}' "$OUT/time.txt")
		cpu=$(awk -F': ' '/User time/{u=$2} /System time/{s=$2} END{printf "%.1f", u+s}' \
			"$OUT/time.txt")
		rss=$(awk -F': ' '/Maximum resident set size/{printf "%.1f", $2/1024}' "$OUT/time.txt")
		ok=ok
		if [ $rc -ne 0 ]; then
			ok="rc=$rc"
		elif ! cmp -s "$OUT/rebuilt" "$t"; then
			ok=MISMATCH
		fi
		printf "%8s %12s %6s %6s %8s %9s %7ss %9sM  %s\n" \
			"$floor" "$size" "$runs" "$lits" "$comp" "$wall" "$cpu" "$rss" "$ok"
		rm -f "$OUT/d" "$OUT/rebuilt"
	done
	echo
done
