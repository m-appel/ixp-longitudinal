import argparse
import os
import subprocess as sp
import sys
from collections import defaultdict
from typing import Tuple


INPUT_SUFFIX_PATTERN = '*.warts.gz'
OUTPUT_DELIMITER = ','


def get_monitor_files(input_dir: str) -> list:
    res = sp.run(['find', input_dir, '-name', INPUT_SUFFIX_PATTERN],
                 text=True,
                 stdout=sp.PIPE,
                 check=True)
    return [os.path.basename(f) for f in res.stdout.splitlines()]


def get_hostname_and_date(warts_file: str) -> Tuple[str, str]:
    file_name_split = warts_file.split('.')
    if file_name_split[0] == 'daily':
        date = file_name_split[4]
        hostname = file_name_split[5]
    else:
        hostname = file_name_split[0]
        date = file_name_split[3]
    return hostname, date


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('input_dir')
    parser.add_argument('output_file')
    args = parser.parse_args()

    input_dir = args.input_dir
    if not input_dir.endswith('/'):
        input_dir += 1

    monitor_files = get_monitor_files(input_dir)
    print(f'Found {len(monitor_files)} warts files.')
    monitor_dates = defaultdict(set)
    for monitor_file in monitor_files:
        hostname, date = get_hostname_and_date(monitor_file)
        monitor_dates[hostname].add(date)

    output_file = args.output_file
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        headers = ('monitor', 'date')
        f.write(OUTPUT_DELIMITER.join(headers) + '\n')
        for monitor, dates in sorted(monitor_dates.items()):
            for date in sorted(dates):
                f.write(OUTPUT_DELIMITER.join((monitor, date)) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
