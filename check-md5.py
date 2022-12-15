import argparse
import os
import subprocess as sp
import sys
from hashlib import md5
from multiprocessing import Pool


def read_hashes(md5_file: str) -> list:
    ret = list()
    file_path = os.path.dirname(md5_file)
    with open(md5_file, 'r') as f:
        for line in f:
            file_name, md5_hash = line.strip().split()
            ret.append((f'{file_path}/{file_name}', md5_hash))
    return ret


def process_md5_file(md5_file: str) -> list:
    ret = list()
    for file_path, md5_hash in read_hashes(md5_file):
        if not os.path.exists(file_path):
            ret.append((file_path, 'NOT_FOUND'))
            continue
        with open(file_path, 'rb') as f:
            file_hash = md5(f.read()).hexdigest()
            print(file_path, md5_hash, file_hash)
            if file_hash != md5_hash:
                ret.append((file_path, 'HASH_MISMATCH'))
    return ret


def get_md5_files(input_dir: str) -> list:
    res = sp.run(['find', input_dir, '-name', 'md5.md5'],
                 text=True,
                 stdout=sp.PIPE,
                 check=True)
    return res.stdout.splitlines()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('input_dir')
    parser.add_argument('output_file')
    parser.add_argument('-n', '--num-proc',
                        type=int,
                        default=4,
                        help='number of parallel processes')
    args = parser.parse_args()

    input_dir = args.input_dir
    if not input_dir.endswith('/'):
        input_dir += '/'

    md5_files = get_md5_files(input_dir)
    print(f'Found {len(md5_files)} md5 files.')
    with Pool(args.num_proc) as p:
        broken_files = [e for l in p.map(process_md5_file, md5_files) for e in l]
    print(broken_files)
    with open(args.output_file, 'w') as f:
        for file_reason in broken_files:
            f.write(' '.join(file_reason) + '\n')

if __name__ == '__main__':
    main()
    sys.exit(0)
