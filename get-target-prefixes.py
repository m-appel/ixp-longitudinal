import argparse
import gzip
import ipaddress
import json
import logging
import os
import pickle
import sys
from collections import Counter, defaultdict
from multiprocessing import Pool
from typing import Tuple


INPUT_FILE_SUFFIX = '.json.gz'
OUTPUT_FILE_SUFFIX = '.pickle.gz'
DATE_FMT = '%Y%m%d'


def process_cycle(cycle_file: str) -> list:
    ret = list()
    try:
        with gzip.open(cycle_file, 'rt') as i:
            for line in i:
                tr = json.loads(line)
                ip_octets = tr['dst_addr'].split('.')
                ip_octets[3] = '0'
                ip_24_prefix = '.'.join(ip_octets)
                # Use byte representation for more compact storage.
                ret.append(ipaddress.ip_address(ip_24_prefix).packed)
    except EOFError as e:
        logging.error(f'Decompressing file {cycle_file} failed: {e}')
        sys.exit(1)
    return ret


def sanitize_dir(dir: str) -> str:
    if not dir.endswith('/'):
        dir += '/'
    return dir


def get_cycle_and_date(file_name: str) -> Tuple[str, str]:
    if file_name.startswith('daily'):
        cycle_id_idx = 3
    else:
        cycle_id_idx = 2
    date_idx = cycle_id_idx + 1
    # The cycle id has format c###### so strip the 'c' before
    # conversion.
    file_name_split = file_name.split('.')
    cycle_id = file_name_split[cycle_id_idx]
    date = file_name_split[date_idx]
    return cycle_id, date


def get_cycle_files(cycle_dir: str) -> dict:
    ret = defaultdict(list)
    for entry in os.scandir(cycle_dir):
        if not entry.is_file() or not entry.name.endswith(INPUT_FILE_SUFFIX):
            continue

        cycle_id, date = get_cycle_and_date(entry.name)
        ret[cycle_id].append((date, f'{cycle_dir}{entry.name}'))
    return ret


def get_common_date(date_list: list) -> str:
    return Counter(date_list).most_common(1)[0][0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('cycle_dir')
    parser.add_argument('output_dir')
    parser.add_argument('-n', '--num-proc',
                        type=int,
                        default=4,
                        help='number of parallel processes')
    parser.add_argument('-f', '--force',
                        action='store_true',
                        help='overwrite existing files')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    cycle_dir = sanitize_dir(args.cycle_dir)
    output_dir = sanitize_dir(args.output_dir)

    cycle_files = get_cycle_files(cycle_dir)
    num_cycle_files = sum([len(l) for l in cycle_files.values()])

    num_proc = args.num_proc
    logging.info(f'Processing {len(cycle_files)} cycles over {num_cycle_files} files with '
                 f'{num_proc} workers.')
    for cycle_id, file_list in cycle_files.items():
        file_dates, file_paths = zip(*file_list)
        cycle_date = get_common_date(file_dates)
        logging.info(f'Processing cycle {cycle_id} {cycle_date}')

        output_file = f'{output_dir}{cycle_id}.{cycle_date}.targets{OUTPUT_FILE_SUFFIX}'
        if os.path.exists(output_file) and not args.force:
            logging.info(f'Output file already exists. Use -f to overwrite.')
            continue

        with Pool(num_proc) as p:
            prefixes = [e for l in p.map(process_cycle, file_paths) for e in l]
        unique_prefixes = set(prefixes)
        logging.info(f'Found {len(prefixes)} targets ({len(unique_prefixes)} unique)')
        with gzip.open(output_file, 'w') as o:
            pickle.dump(unique_prefixes, o)


if __name__ == '__main__':
    main()
    sys.exit(0)
