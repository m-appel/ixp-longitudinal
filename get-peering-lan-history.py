import argparse
import bz2
import logging
import os
import pickle
import subprocess as sp
import sys
from collections import defaultdict
from multiprocessing import Pool
from socket import AF_INET
from typing import Tuple

import radix


DATA_DELIMITER = ','


def get_input_files(input_dir: str) -> list:
    res = sp.run(['find', input_dir, '-name', 'pdb.*.pickle.bz2'],
                 text=True,
                 stdout=sp.PIPE,
                 check=True)
    return res.stdout.splitlines()


def get_prefix_lengths(input_file: str) -> Tuple[str, dict, dict]:
    date = os.path.basename(input_file).split('.')[1]
    with bz2.open(input_file, 'rb') as f:
        rtree: radix.Radix = pickle.load(f)['rtree']
    v4_prefixes = defaultdict(set)
    v6_prefixes = defaultdict(set)
    for node in rtree.nodes():
        ix_id = node.data['id']
        prefix = node.prefix
        if node.family == AF_INET:
            # if ix_id in v4_prefixes:
            #     logging.warning(f'ix_id {ix_id} has multiple prefixes? {v4_prefixes[ix_id]} {prefix}')
            v4_prefixes[ix_id].add(prefix)
        else:
            v6_prefixes[ix_id].add(prefix)
    return date, v4_prefixes, v6_prefixes


def create_history(prefixes: list) -> dict:
    ret = defaultdict(lambda: {'v4': list(), 'v6': list()})
    current_intervals = defaultdict(lambda: {'v4': None, 'v6': None})
    for date, v4_prefixes, v6_prefixes in sorted(prefixes):
        for ix_id, prefix in v4_prefixes.items():
            if current_intervals[ix_id]['v4'] is None:
                current_intervals[ix_id]['v4'] = [prefix, date, date]
            if current_intervals[ix_id]['v4'][0] != prefix:
                # Commit old interval and start new one.
                ret[ix_id]['v4'].append(current_intervals[ix_id]['v4'])
                current_intervals[ix_id]['v4'] = [prefix, date, date]
            else:
                # Update end date.
                current_intervals[ix_id]['v4'][2] = date
        for ix_id, prefix in v6_prefixes.items():
            if current_intervals[ix_id]['v6'] is None:
                current_intervals[ix_id]['v6'] = [prefix, date, date]
            if current_intervals[ix_id]['v6'][0] != prefix:
                # Commit old interval and start new one.
                ret[ix_id]['v6'].append(current_intervals[ix_id]['v6'])
                current_intervals[ix_id]['v6'] = [prefix, date, date]
            else:
                # Update end date.
                current_intervals[ix_id]['v6'][2] = date
    # Commit final intervals.
    for ix_id, intervals in current_intervals.items():
        ret[ix_id]['v4'].append(intervals['v4'])
        ret[ix_id]['v6'].append(intervals['v6'])
    return ret


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

    output_file = args.output_file

    input_files = get_input_files(input_dir)

    with Pool(args.num_proc) as p:
        prefixes = p.map(get_prefix_lengths, input_files)

    if not prefixes:
        sys.exit(1)

    intervals = create_history(prefixes)

    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_file, 'w') as f:
        headers = ('ix_id', 'prefix', 'start_date', 'end_date')
        f.write(DATA_DELIMITER.join(headers) + '\n')
        for ix_id, intervals in sorted(intervals.items(), key=lambda t: int(t[0])):
            for interval in intervals['v4']:
                if interval is None:
                    continue
                line = [ix_id]
                line.append(';'.join(interval[0]))
                line.append(interval[1])
                line.append(interval[2])
                f.write(DATA_DELIMITER.join(line) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
