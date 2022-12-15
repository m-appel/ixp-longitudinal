import argparse
import gzip
import os
import pickle
import sys
from datetime import datetime
from multiprocessing import Pool
from typing import Tuple


INPUT_DIR = '../stats/targets/'
INPUT_FILE_SUFFIX = '.pickle.gz'
OUTPUT_FILE = '../stats/target_counts.csv'
DATA_DELIMITER = ','


def get_target_count(input_tuple: tuple) -> Tuple[int, int]:
    timestamp, input_file = input_tuple
    with gzip.open(input_file) as f:
        return timestamp, len(pickle.load(f))


def get_timestamp(file_name: str) -> int:
    date = file_name.split('.')[1]
    return int(datetime.strptime(date, '%Y%m%d').timestamp())


def get_input_files() -> list:
    ret = list()
    for entry in os.scandir(INPUT_DIR):
        if not entry.is_file() or not entry.name.endswith(INPUT_FILE_SUFFIX):
            continue
        timestamp = get_timestamp(entry.name)
        ret.append((timestamp, f'{INPUT_DIR}{entry.name}'))
    return ret


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--num-proc',
                        type=int,
                        default=4,
                        help='number of parallel processes')
    args = parser.parse_args()
    input_files = get_input_files()

    with Pool(args.num_proc) as p:
        counts = p.map(get_target_count, input_files)

    with open(OUTPUT_FILE, 'w') as f:
        headers = ('timestamp', 'target_count')
        f.write(DATA_DELIMITER.join(headers) + '\n')
        for timestamp, count in sorted(counts):
            line = (timestamp, count)
            f.write(DATA_DELIMITER.join(map(str, line)) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
