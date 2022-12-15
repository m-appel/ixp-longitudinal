#!/bin/bash
set -euo pipefail

if [ ! $# -eq 3 ]; then
    echo "usage: $0 <year> <start-month> <end-month>"
    exit 0
fi

YEAR="${1}"
START_MONTH="${2}"
END_MONTH="${3}"

if [ ${START_MONTH} -gt ${END_MONTH} ]; then
    echo "error: start month (${START_MONTH}) is after end month (${END_MONTH})."
    exit 1
fi

for ((m = START_MONTH; m <= END_MONTH; m++)); do
    START_TS=$(date +%Y-%m-%dT%H:%M --date "${YEAR}-${m}-01")
    STOP_TS=$(date +%Y-%m-%dT%H:%M --date "${YEAR}-${m}-01 + 1 month")
    echo "${STOP_TS}" >> "${YEAR}.log"
    python3 ./traceroute_to_bgp.py "config/ark_probe_data_${YEAR}.ini" "${START_TS}" "${STOP_TS}" >> "${YEAR}.log"
done
