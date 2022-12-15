import argparse
import bz2
import logging
import os
import pickle
import subprocess as sp
import sys
from collections import defaultdict
from datetime import datetime
from multiprocessing import Pool
from socket import AF_INET
from typing import Tuple

import numpy as np

from plots.shared_functions import increment_months


START_DATE = datetime(2010, 8, 1)
END_DATE = datetime(2023, 1, 1)
INPUT_FILE_SUFFIX = '.pickle.bz2'
INPUT_FILE_FORMAT = f'rib.%Y%m%d{INPUT_FILE_SUFFIX}'
OUTPUT_DATA_DELIMITER = ','


def get_prefix_len_stats(rib_file_date: tuple) -> Tuple[datetime, tuple]:
    file_date, rib_file = rib_file_date
    prefix_lengths = list()
    with bz2.open(rib_file, 'rb') as f:
        rtree = pickle.load(f)
        for node in rtree.nodes():
            # Only use IPv4 for now.
            if node.family != AF_INET:
                continue
            prefix_lengths.append(node.prefixlen)
    return file_date, (np.min(prefix_lengths),
                       np.median(prefix_lengths),
                       np.mean(prefix_lengths),
                       np.max(prefix_lengths))


def get_file_date(file_name: str) -> datetime:
    try:
        return datetime.strptime(file_name, INPUT_FILE_FORMAT)
    except ValueError as e:
        logging.error(f'Failed to get date from file {file_name}: {e}')
        return None


def get_monthly_file(input_dir: str) -> list:
    files = sp.run(['find', input_dir, '-name', f'*{INPUT_FILE_SUFFIX}'],
                   text=True,
                   stdout=sp.PIPE,
                   check=True)

    files_by_month = defaultdict(list)
    for file_path in files.stdout.splitlines():
        file_name = os.path.basename(file_path)
        file_date = get_file_date(file_name)
        if file_date is None or file_date < START_DATE or file_date >= END_DATE:
            continue
        key = file_date.strftime('%Y%m')
        files_by_month[key].append((file_date, file_path))

    # Return only the first file for each month.
    ret = list()
    for monthly_files in files_by_month.values():
        monthly_files.sort()
        ret.append(monthly_files[0])
    ret.sort()
    return ret


def check_gaps(monthly_files: list) -> None:
    DATE_FMT = "%Y-%m"
    logging.info(f'Checking for gaps from {START_DATE.strftime(DATE_FMT)} to '
                 f'{END_DATE.strftime(DATE_FMT)} (exclusive)')
    curr_date = START_DATE
    curr_file_idx = 0
    while curr_date < END_DATE and curr_file_idx < len(monthly_files):
        file_date = monthly_files[curr_file_idx][0]
        if curr_date.year < file_date.year \
                or curr_date.year == file_date.year and curr_date.month < file_date.month:
            logging.warning(f'Missing file for {curr_date.strftime(DATE_FMT)}')
            curr_date = increment_months(curr_date, 1)
            continue
        # Since we increment the current date in 1-month steps it
        # can never be larger than the file date, i.e., the only gap
        # can occur in the file date.
        curr_date = increment_months(curr_date, 1)
        curr_file_idx += 1
    if curr_date < END_DATE:
        logging.warning(f'Ran out of files at {curr_date.strftime(DATE_FMT)}.')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('input_dir')
    parser.add_argument('output_file')
    parser.add_argument('-n', '--num-proc',
                        type=int,
                        default=4,
                        help='number of parallel processes')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    input_dir = args.input_dir
    if not input_dir.endswith('/'):
        input_dir += '/'

    monthly_files = get_monthly_file(input_dir)
    check_gaps(monthly_files)
    with Pool(args.num_proc) as p:
        prefix_lengths = p.map(get_prefix_len_stats, monthly_files)

    if not prefix_lengths:
        return

    with open(args.output_file, 'w') as f:
        headers = ('date', 'min_pfx_len', 'median_pfx_len', 'mean_pfx_len', 'max_pfx_len')
        f.write(OUTPUT_DATA_DELIMITER.join(headers) + '\n')
        for date, prefix_length_stats in sorted(prefix_lengths):
            line = [date.strftime('%Y%m')]
            for stat in prefix_length_stats:
                line.append(f'{stat:.2f}')
            f.write(OUTPUT_DATA_DELIMITER.join(line) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
