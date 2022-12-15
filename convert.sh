#!/bin/bash
set -uo pipefail

if [ ! $# -eq 4 ]; then
    echo "usage: $0 <input_dir> <output_dir> <start_ts> <num_days>"
    exit 0
fi

readonly INPUT_DIR="${1%/}"
readonly OUTPUT_DIR="${2%/}"
readonly START_TS="${3}"
readonly DAYS="${4}"

for ((d = 0; d < DAYS; d++)) do
    TS=$(date +%Y%m%d --date "$START_TS + $d months")
    FOLDER="cycle-${TS}"
    DIR="${INPUT_DIR}/${FOLDER}"
    if [ ! -d $DIR ]; then
        continue
    fi
    OUT_DIR="${OUTPUT_DIR}/${FOLDER}"
    mkdir -p "${OUT_DIR}"
    echo "${DIR}"
    find "${DIR}" -name "*.warts.gz" | \
        nice \
        ionice -c 2 -n 7 \
        parallel \
            --bar \
            -j jobs \
            python3 \
                ./transform-traceroute.py \
                {} \
                "${OUT_DIR}"
done
