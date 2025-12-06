#!/bin/bash

DELTA_HEADER_SIZE=32 # bytes
DELTA_FORMAT_VERSION=0x101
DELTA_MAGIC_NUMBER=0xF989789C
XDELTA3_MAGIC_NUMBER=0x00c4c3d6
DELTA_TOOL_XDELTA3="0x1"
DELTA_TOOL_BSDIFF="0x2"
DELTA_TOOL_HDIFFPATCH="0x3"
DEFAULT_DELTA_TOOL="${DELTA_TOOL_XDELTA3}"

##
# default compression level assumed 3
# plain squashfs to squashfs delta size has no measurable gain between  3 and 9 comp level
# gain in delta pseudo file between 3 and 7 comp level is 10 to 20% size reduction
# delta size gain flattens at 7
# no noticeable gain from changing source window size(-B) or bytes input window(-W)
# or size compression duplicates window (-P)
XDELTA3_PLAIN_TUNING_PARAMS="-3"
XDELTA3_TUNING_PARAMS="-7"

# use vendored squashfs tools
TOOLBOX_SNAP="/snap/toolbox/current"
SNAPD_SNAP="/snap/snapd/current"
UNSQUASHFS="${TOOLBOX_SNAP}/usr/bin/unsquashfs"
MKSQUASHFS="${TOOLBOX_SNAP}/usr/bin/mksquashfs"
XDELTA3="${SNAPD_SNAP}/usr/bin/xdelta3"
BSDIFF="${TOOLBOX_SNAP}/usr/bin/bsdiff"
BSPATCH="${TOOLBOX_SNAP}/usr/bin/bspatch"
HDIFFZ="${TOOLBOX_SNAP}/usr/bin//hdiffz"
HPATCHZ="${TOOLBOX_SNAP}/usr/bin/hpatchz"

# Custom Delta header (padded to DELTA_HEADER_SIZE size)
# generated delta is using following custom header to capture the delta content
# |       32b    |   16b   |     16b    |    32b     |     16b     |        16b        |
# | magic number | version | delta tool | time stamp | compression | super block flags |
# reference squashfs supperblock https://dr-emann.github.io/squashfs
# Optional compressor options are currently not supported, if target squashfs is detected to
# use those, we fallback to plain xdelta
# Delta between two snaps(squashfs) is generated on the squashfs pseudo file definition
# this represents uncompressed content of the squashfs packages, custom header data is
# later used as input parameters to mksquashfs when recreated target squashfs from the
# reconstructed pseudo file definition

print_help() {
  echo -e "Generate / apply 'smart' delta between source and target squashfs images."
  echo -e "Operations:"
  echo -e "\tgenerate: generate delta between source and target"
  echo -e "\tapply:    apply delta on the source"
  echo -e ""
  echo -e "Compulsory arguments:"
  echo -e "\t--source | -s: source snap"
  echo -e "\t--target | -t: target snap"
  echo -e "\t--delta  | -d: delta between source and target snap"
  echo -e ""
  echo -e "Optional arguments:"
  echo -e "\t--bsdiff:  use bsdiff(bfpatch) as delta tool to generate and apply delta on squashfs pseudo file definition"
  echo -e "\t           this delta tool does not support streams, full sized temp source and target pseudo files will be created!"
  echo -e "\t           !! only available during delta generation !!"
  echo -e "\t--hdiffz:  use HDiffPatch(hdiffz/hpatchz) as delta tool to generate and apply delta on squashfs pseudo file definition"
  echo -e "\t           this delta tool does not support streams, full sized temp source and target pseudo files will be created!"
  echo -e "\t--xdelta3: use xdelta3 as delta tool to generate and apply delta on squashfs pseudo file definition"
  echo -e "\t           !! only available during delta generation !!"
  echo -e ""
  echo -e "Usage:"
  echo -e "\t$(basename "${0}") generate --bsdiff --source <SNAP>  --target <SNAP> --delta <DELTA>"
  echo -e "\t$(basename "${0}") apply    --source <SNAP>  --target <SNAP> --delta <DELTA>"
  echo -e ""
  echo -e "Example:"
  echo -e "\t$(basename "${0}") generate --xdelta3 --source core22_2134.snap --target core22_2140.snap --delta delta-core22-2134-2140.delta"
  echo -e "\t$(basename "${0}") apply    --source core22_2134.snap --target core22_2140.snap --delta delta-core22-2134-2140.delta"
  exit
}

parse_compression() {
  case ${1} in
    1)
      echo "gzip"
      ;;
    2)
      echo "lzma"
      ;;
    3)
      echo "lzo"
      ;;
    4)
      echo "xz"
      ;;
    5)
      echo "lz4"
      ;;
    6)
      echo "zstd"
      ;;
  esac
}

parse_superblock_flags() {
  ##  as per squashfs format following options are supported
  #  Name                       Value     mksquashfs argument
  #  UNCOMPRESSED_INODES        0x0001
  #  UNCOMPRESSED_DATA          0x0002
  #  CHECK                      0x0004    # should be always unset
  #  UNCOMPRESSED_FRAGMENTS     0x0008
  #  NO_FRAGMENTS               0x0010    -no-fragments
  #  ALWAYS_FRAGMENTS           0x0020
  #  DUPLICATES                 0x0040    (default) if not set then -no-duplicates
  #  EXPORTABLE                 0x0080    -exports (default)
  #  UNCOMPRESSED_XATTRS        0x0100
  #  NO_XATTRS                  0x0200    -no-xattrs
  #  COMPRESSOR_OPTIONS         0x0400    compressor option is present
  #  UNCOMPRESSED_IDS           0x0800
  mksqfs_args=""
  local flags=${1}
  if ((flags & 0x0004)); then
    echo -e "This does not look like Squashfs 4+ superblock flags"
    exit
  fi
  if ((flags & 0x0010)); then
    mksqfs_args="${mksqfs_args} -no-fragments"
  fi
  if ! ((flags & 0x0040)); then
    mksqfs_args="${mksqfs_args} -no-duplicates"
  fi
  if ((flags & 0x0080)); then
    mksqfs_args="${mksqfs_args} -exports"
  fi
  if ((flags & 0x0200)); then
    mksqfs_args="${mksqfs_args} -no-xattrs"
  fi
  if ((target_flags &  0x0400)); then
    # TODO: handle optional compression options
    >&2 echo "Custom compression options detected, created target snap is likely be different from target snap!"
  fi

  echo "${mksqfs_args}"
}

handle_plain_xdelta3_generate() {
  local source=${1}
  local target=${3}
  local delta=${2}
  # shellcheck disable=SC2046 # SC2046 does not apply as we specifically want to allow splitting
  "${XDELTA3}" ${XDELTA3_PLAIN_TUNING_PARAMS} -f -e -s "${source}" "${target}" "${delta}"
}

handle_plain_xdelta3_apply() {
  local source=${1}
  local delta=${2}
  local target=${3}
  "${XDELTA3}" -f -d -s "${source}" "${delta}" "${target}"
}

handle_generate_delta() {
  local source_snap_pf target_snap_pf seek_count target_flags
  source_snap_pf=$(mktemp -u /tmp/snap-delta-XXXXXX)
  target_snap_pf=$(mktemp -u /tmp/snap-delta-XXXXXX)

  rm -rf "${source_snap_pf}" "${target_snap_pf}" "${DELTA}"

  # read timestamp and store it at the start of the delta
  seek_count=0
  # store magic number: DELTA_MAGIC_NUMBER
  for b in $((DELTA_MAGIC_NUMBER & 255)) $(((DELTA_MAGIC_NUMBER >> 8) & 255)) $(((DELTA_MAGIC_NUMBER >> 16) & 255)) $(((DELTA_MAGIC_NUMBER >> 24) & 255))
  do
    printf "%b" "$(printf '\\%03o' "$b")" >> "$DELTA"
  done
  seek_count=$((seek_count + 4))
  # store delta version: DELTA_FORMAT_VERSION
  for b in $((DELTA_FORMAT_VERSION & 255)) $(((DELTA_FORMAT_VERSION >> 8) & 255))
  do
    printf "%b" "$(printf '\\%03o' "$b")" >> "$DELTA"
  done
  seek_count=$((seek_count + 2))
  # store used delta tool
  for b in $((DELTA_TOOL & 255)) $(((DELTA_TOOL >> 8) & 255))
  do
    printf "%b" "$(printf '\\%03o' "$b")" >> "$DELTA"
  done
  seek_count=$((seek_count + 2))

  # read time stamp from the target squashfs superblock
  dd if="${TARGET_SNAP}" bs=1 skip=8 count=4 status=none of="${DELTA}" seek="${seek_count}"
  seek_count=$((seek_count + 4))
  # read compression from the target squashfs superblock
  dd if="${TARGET_SNAP}" bs=1 skip=20 count=2 status=none of="${DELTA}" seek="${seek_count}"
  seek_count=$((seek_count + 2))
  # read super block flags from the target squashfs superblock
  dd if="${TARGET_SNAP}" bs=1 skip=24 count=2 status=none of="${DELTA}" seek="${seek_count}"
  seek_count=$((seek_count + 2))
  # check compression options are pressent
  target_flags="0x$(od --address-radix n --format x2 --skip-bytes 24 --read-bytes 2 "${TARGET_SNAP}" | xargs)"
  if ((target_flags &  0x0400)); then
    # TODO: handle optional compression options
    echo "Custom compression options detected, created target snap is likely be different from target snap!"
    # falling back to traditional delta, till custom compression options are supported
    handle_plain_xdelta3_generate "${SOURCE_SNAP}" "${TARGET_SNAP}" "${DELTA}"
    return
  fi

  # padd delta header to DELTA_HEADER_SIZE
  truncate -s +$((DELTA_HEADER_SIZE - seek_count)) "${DELTA}"

  if (( DELTA_TOOL_XDELTA3 == DELTA_TOOL )); then
    mkfifo "${source_snap_pf}" "${target_snap_pf}"
    "${UNSQUASHFS}" -f -pf "${source_snap_pf}" "${SOURCE_SNAP}" &
    "${UNSQUASHFS}" -f -pf "${target_snap_pf}" "${TARGET_SNAP}" &

    # shellcheck disable=SC2046 # SC2046 does not apply as we specifically want to allow splitting
    "${XDELTA3}" ${XDELTA3_TUNING_PARAMS} -e -f -A -s "${source_snap_pf}" "${target_snap_pf}" >> "${DELTA}"
  elif (( DELTA_TOOL_BSDIFF == DELTA_TOOL )) || (( DELTA_TOOL_HDIFFPATCH == DELTA_TOOL )); then
    local pf_delta
    pf_delta=$(mktemp -u /tmp/snap-delta-XXXXXX)
    "${UNSQUASHFS}" -n -f -pf "${source_snap_pf}" "${SOURCE_SNAP}"
    "${UNSQUASHFS}" -n -f -pf "${target_snap_pf}" "${TARGET_SNAP}"
    if (( DELTA_TOOL_BSDIFF == DELTA_TOOL )); then
      "${BSDIFF}" "${source_snap_pf}" "${target_snap_pf}" "${pf_delta}"
    elif (( DELTA_TOOL_HDIFFPATCH == DELTA_TOOL )); then
      "${HDIFFZ}" -f -m-6 -SD -c-zstd-21-24 -d "${source_snap_pf}" "${target_snap_pf}" "${pf_delta}"
    fi
    cat "${pf_delta}" >> "${DELTA}"
    rm -rf "${pf_delta}"
  fi
  rm "${source_snap_pf}" "${target_snap_pf}"
}

handle_apply_delta() {
  local magic_number version_number fstimeint target_compression_id
  local target_compression target_flags source_snap_pf delta used_delta_tool
  # get magic and version number to validate
  magic_number="0x$(od --address-radix n --format x4 --skip-bytes 0 --read-bytes 4 "${DELTA}" | xargs)"
  if (( XDELTA3_MAGIC_NUMBER == magic_number )); then
    echo "This is plain xdelta3 diff, falling back to plain xdelta3!!"
    handle_plain_xdelta3_apply "${SOURCE_SNAP}" "${TARGET_SNAP}" "${DELTA}"
    exit
  elif (( DELTA_MAGIC_NUMBER != magic_number )); then
    echo "Wrong magic number!!!"
    exit
  fi
  version_number="0x$(od --address-radix n --format x2 --skip-bytes 4 --read-bytes 2 "${DELTA}" | xargs)"
  if (( DELTA_FORMAT_VERSION != version_number )); then
    echo "Missmatch delta version number!!!"
    exit
  fi

  # get used delta tool
  used_delta_tool="0x$(od --address-radix n --format x2 --skip-bytes 6 --read-bytes 2 "${DELTA}" | xargs)"

  # get fs time stamp
  fstimeint=$(od --address-radix n --format u8 --skip-bytes 8 --read-bytes 4 "${DELTA}" | xargs)
  # get compression
  target_compression_id=$(od --address-radix n --format u8 --skip-bytes 12 --read-bytes 2 "${DELTA}" | xargs)
  target_compression=$(parse_compression "${target_compression_id}")
  # get flags
  target_flags="0x$(od --address-radix n --format x2 --skip-bytes 14 --read-bytes 2 "${DELTA}" | xargs)"

  # xdelta3 supports pipes, anything else we need to use tmp pseudo files
  source_snap_pf=$(mktemp -u /tmp/snap-delta-XXXXXX)
  delta=$(mktemp -u /tmp/snap-delta-XXXXXX)
  rm -rf "${source_snap_pf}" "${delta}"

  if (( DELTA_TOOL_XDELTA3 == used_delta_tool )); then
    # setup detla pipes
    mkfifo "${source_snap_pf}" "${delta}"
    "${UNSQUASHFS}" -n -pf "${source_snap_pf}" "${SOURCE_SNAP}" &
    dd if="${DELTA}" of="${delta}" status=none bs="${DELTA_HEADER_SIZE}" skip=1 &

    # shellcheck disable=SC2046 # SC2046 does not apply as we specifically want to allow splitting
    "${XDELTA3}" -f -d -s "${source_snap_pf}" "${delta}" | "${MKSQUASHFS}" - "${TARGET_SNAP}" -pf - -quiet -noappend -comp "${target_compression}" -mkfs-time "${fstimeint}" $(parse_superblock_flags "${target_flags}")
  elif (( DELTA_TOOL_BSDIFF == used_delta_tool )) || (( DELTA_TOOL_HDIFFPATCH == used_delta_tool )); then
    local target_pf raw_delta
    target_pf=$(mktemp -u /tmp/snap-delta-XXXXXX)
    "${UNSQUASHFS}" -n -f -pf "${source_snap_pf}" "${SOURCE_SNAP}"

    dd if="${DELTA}" of="${delta}" status=none bs="${DELTA_HEADER_SIZE}" skip=1
    if (( DELTA_TOOL_BSDIFF == used_delta_tool )); then
      "${BSPATCH}" "${source_snap_pf}" "${target_pf}" "${delta}"
      "${MKSQUASHFS}" - "${TARGET_SNAP}" -pf "${target_pf}" -quiet -noappend -comp "${target_compression}" -mkfs-time "${fstimeint}" $(parse_superblock_flags "${target_flags}")
    elif (( DELTA_TOOL_HDIFFPATCH == used_delta_tool )); then
      "${HPATCHZ}" -f -s-8m "${source_snap_pf}" "${delta}" /dev/stdout | "${MKSQUASHFS}" - "${TARGET_SNAP}" -pf - -quiet -noappend -comp "${target_compression}" -mkfs-time "${fstimeint}" $(parse_superblock_flags "${target_flags}")
    fi
    rm -rf "${target_pf}" "${raw_delta}"
  else
    echo -e "Unsupported delta tool passed: ${used_delta_tool}"
    print_help
  fi
  # cleanup
  rm "${source_snap_pf}" "${delta}"
}

# first check for the operation
if [[ "${1}" == "apply" ]]; then
  DIRECTION="apply"
elif [[ "${1}" == "generate" ]]; then
  DIRECTION="generate"
else
  echo "Missing operastion!!"
  print_help
fi
shift

# parse passed parameters
while [ "$1" != "" ];
do
  case ${1} in
    --source | -s)
      SOURCE_SNAP=${2}
      shift
      ;;
    --target | -t)
      TARGET_SNAP=${2}
      shift
      ;;
    --delta  | -d)
      DELTA=${2}
      shift
      ;;
    --bsdiff)
      DELTA_TOOL="${DELTA_TOOL_BSDIFF}"
      ;;
    --xdelta3)
      DELTA_TOOL="${DELTA_TOOL_XDELTA3}"
      ;;
    --hdiffz)
      DELTA_TOOL="${DELTA_TOOL_HDIFFPATCH}"
      ;;
    --help | -h)
      print_help
      ;;
    *)
      echo "Unrecognised argument: [${1}]"
      print_help
      ;;
  esac
  shift
done

if [ -z "${SOURCE_SNAP}" ] || [ -z "${DELTA}" ] || [ -z "${TARGET_SNAP}" ]; then
  echo -e "Missing required parameters"
  print_help
fi
if [[ "apply" == "${DIRECTION}" ]] && [[ -n "${DELTA_TOOL}" ]]; then
  echo "Cannot specify delta tool when applying existing delta!"
  print_help
fi
if [[ "apply" == "${DIRECTION}" ]]; then
  handle_apply_delta
elif [[ "generate" == "${DIRECTION}" ]]; then
  if [ -z "${DELTA_TOOL}" ] ; then
    echo -e "No delta tool specified, defaulting to xdelta3..."
    DELTA_TOOL="${DEFAULT_DELTA_TOOL}"
  fi
  handle_generate_delta
else
  echo "Unknown operation!!"
  print_help
fi
