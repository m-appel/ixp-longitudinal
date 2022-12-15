#!/bin/bash
set -euo pipefail

if [ ! $# -eq 3 ]; then
    echo "usage: $0 <year> <start-month> <end-month>"
    exit 0
fi

readonly OUTPUT_DIR="./stats/hegemony"
readonly YEAR=${1}
readonly START_MONTH=${2}
readonly END_MONTH=${3}

for ((m = START_MONTH; m <= END_MONTH; m++)); do
    TS=$(date +%Y-%m-%dT00:00 -d "${YEAR}-${m}-01 + 1 month")
    FILE_DATE=$(date +%Y-%m-%d -d "${YEAR}-${m}-01 + 1 month")
    TOPIC="ihr_hegemony_ark_probe_data_${YEAR}"
    OUTPUT_FILE="${OUTPUT_DIR}/ark_probe_data.${FILE_DATE}.hegemony.csv"
    echo "${TS} ${FILE_DATE}"
    python3 ./extract-hegemony.py "${TOPIC}" "${OUTPUT_FILE}" --timestamp "${TS}"
done
