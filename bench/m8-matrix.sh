#!/bin/bash
# M8 matrix: per pair, generate a block-plan delta, apply it, and record what the
# apply cost in memory. The plan's M8 target is peak apply memory under ~20 MB
# (65.76 MB measured before M8 started), so the two numbers that matter are the
# applier's own VmHWM and the memfd scratch high-water mark, which -max-run
# bounds directly. /usr/bin/time -v is recorded too, but it is the maximum over
# the whole process tree, so an xz child can take the blame for it.

set -u

BIN=/home/ondrak/development/squashfs-delta/snap-delta-blocks
SC=/home/ondrak/development/snapcraft
IMX=/home/ondrak/development/imx-gadget
KRN=/home/ondrak/development/kernel
WORK=$(mktemp -d /tmp/m8-work-XXXXXX)
trap 'rm -rf "$WORK"' EXIT

# name:source:target
PAIRS=(
	"snapcraft 8.13.2.post72>post75:$SC/snapcraft_8.13.2.post72_amd64.snap:$SC/snapcraft_8.13.2.post75_amd64.snap"
	"snapcraft 8.13.2.post75>post77:$SC/snapcraft_8.13.2.post75_amd64.snap:$SC/snapcraft_8.13.2.post77_amd64.snap"
	"snapcraft 8.14.4.post129>post194:$SC/snapcraft_8.14.4.post129_amd64.snap:$SC/snapcraft_8.14.4.post194_amd64.snap"
	"snapcraft 8.14.4.post194>8.14.5.post251:$SC/snapcraft_8.14.4.post194_amd64.snap:$SC/snapcraft_8.14.5.post251_amd64.snap"
	"snapcraft 8.14.5.post251>9.0.0.post60:$SC/snapcraft_8.14.5.post251_amd64.snap:$SC/snapcraft_9.0.0.post60_amd64.snap"
	"snapcraft 9.0.0.post61>post60:$SC/snapcraft_9.0.0.post61_amd64.snap:$SC/snapcraft_9.0.0.post60_amd64.snap"
	"imx93-frdm 26-12>26-13:$IMX/imx93-frdm_26-12-6.18.20-2.0.0_arm64.snap:$IMX/imx93-frdm_26-13-6.18.20-2.0.0_arm64.snap"
	"imx93-frdm 24-12>26-12 (neg control):$IMX/imx93-frdm_24-12-6.18.20-2.0.0_arm64.snap:$IMX/imx93-frdm_26-12-6.18.20-2.0.0_arm64.snap"
	"imx-kernel 1013.13>1014.14:$KRN/imx-kernel_6.18.0-1013.13_arm64.snap:$KRN/imx-kernel_6.18.0-1014.14_arm64.snap"
)

for entry in "${PAIRS[@]}"; do
	name=${entry%%:*}
	rest=${entry#*:}
	src=${rest%%:*}
	tgt=${rest#*:}

	echo "############ PAIR: $name"
	if [ ! -f "$src" ] || [ ! -f "$tgt" ]; then
		echo "  SKIP: missing input"
		continue
	fi
	echo "  source $(stat -c%s "$src") bytes  $(basename "$src")"
	echo "  target $(stat -c%s "$tgt") bytes  $(basename "$tgt")"

	delta=$WORK/d.delta
	out=$WORK/out.snap
	rm -f "$delta" "$out"

	echo "  ---- generate"
	gen_t0=$(date +%s.%N)
	# Piping into sed would report sed's status, not the generator's, so keep
	# the exit code and indent afterwards.
	"$BIN" generate --blocks -s "$src" -t "$tgt" -d "$delta" > "$WORK/gen.out" 2>&1
	genrc=$?
	sed 's/^/    /' "$WORK/gen.out"
	if [ $genrc -ne 0 ]; then
		echo "  GENERATE FAILED (rc=$genrc)"
		continue
	fi
	gen_t1=$(date +%s.%N)
	echo "    generate wall $(echo "$gen_t1 - $gen_t0" | bc)s"
	dsize=$(stat -c%s "$delta")
	tsize=$(stat -c%s "$tgt")
	echo "    delta $dsize bytes ($(echo "scale=2; 100*$dsize/$tsize" | bc)% of target)"

	echo "  ---- apply"
	/usr/bin/time -v "$BIN" apply -stats -s "$src" -d "$delta" -t "$out" \
		> "$WORK/apply.out" 2> "$WORK/apply.time"
	rc=$?
	sed 's/^/    /' "$WORK/apply.out"
	if [ $rc -ne 0 ]; then
		echo "  APPLY FAILED (rc=$rc)"
		sed 's/^/    /' "$WORK/apply.time"
		continue
	fi
	grep -E "Maximum resident set size|Elapsed \(wall|User time|System time" "$WORK/apply.time" \
		| sed 's/^[[:space:]]*/    tree /'

	echo "  ---- verify"
	if cmp -s "$out" "$tgt"; then
		echo "    BYTE-IDENTICAL ok"
	else
		echo "    MISMATCH -- reconstruction differs from real target"
	fi
	rm -f "$delta" "$out"
done

echo "############ MATRIX COMPLETE"
