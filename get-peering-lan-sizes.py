# number of /24s = 2 ** (24 - prefix len)
import argparse
import bz2
import logging
import os
import pickle
import subprocess as sp
import sys
from collections import Counter
from multiprocessing import Pool
from socket import AF_INET
from typing import Tuple

import radix

EXPECTED_OUTPUT_FILE_SUFFIX = '.pickle.bz2'


def get_input_files(input_dir: str) -> list:
    res = sp.run(['find', input_dir, '-name', 'pdb.*.pickle.bz2'],
                 text=True,
                 stdout=sp.PIPE,
                 check=True)
    return res.stdout.splitlines()


def get_prefix_lengths(input_file: str) -> Tuple[str, Counter, Counter]:
    date = os.path.basename(input_file).split('.')[1]
    with bz2.open(input_file, 'rb') as f:
        rtree: radix.Radix = pickle.load(f)['rtree']
    v4_prefix_lengths = list()
    v6_prefix_lengths = list()
    for node in rtree.nodes():
        if node.family == AF_INET:
            v4_prefix_lengths.append(node.prefixlen)
        else:
            v6_prefix_lengths.append(node.prefixlen)
    return date, Counter(v4_prefix_lengths), Counter(v6_prefix_lengths)


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
    if not output_file.endswith(EXPECTED_OUTPUT_FILE_SUFFIX):
        logging.warning(f'Output will be written in {EXPECTED_OUTPUT_FILE_SUFFIX} format, but '
                        f'specified output file suffix is different.')

    input_files = get_input_files(input_dir)

    with Pool(args.num_proc) as p:
        counts = p.map(get_prefix_lengths, input_files)

    if not counts:
        sys.exit(1)

    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with bz2.open(output_file, 'wb') as f:
        pickle.dump(counts, f)


if __name__ == '__main__':
    main()
    sys.exit(0)
