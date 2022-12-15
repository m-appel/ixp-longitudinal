import argparse
import gzip
import json
import logging
import os
import subprocess as sp
import sys
from ipaddress import ip_address

from collections import defaultdict
from json import JSONDecodeError
from typing import Tuple


INPUT_SUFFIX = '.warts.gz'
OUTPUT_SUFFIX = '.json.gz'
OLD_WARTS2JSON_BIN = '/home/malte/scamper-cvs-20210324/utils/sc_warts2json/sc_warts2json'


def transform(ark: dict, ark_metadata: dict) -> dict:
    atlas = {'fw': 5020,  # Required for the parser
             'prb_id': ark_metadata['hostname'],
             'msm_id': 0,
             'ark_metadata': ark_metadata,
             'dst_addr': ark['dst'],
             'af': 4,
             'timestamp': ark['start']['sec'],
             'from': ark['src'],
             'result': list()
             }

    stop_reason = ark['stop_reason']

    result = defaultdict(list)
    # There are traceroutes without any valid hops.
    if 'hops' in ark:
        for hop in ark['hops']:
            ttl = hop['probe_ttl']
            icmp_type = hop['icmp_type']
            reply = {'from': hop['addr'],
                     'rtt': hop['rtt']}
            if icmp_type == 0 or icmp_type == 11:
                # "Echo reply" or "Time exceeded" (normal hop
                # behavior)
                result[ttl].append(reply)
            elif icmp_type == 3:
                if stop_reason != 'UNREACH':
                    logging.warning(f'Got ICMP type 3 hop, bot stop reason is '
                                    f'not "UNREACH", but {stop_reason}.')
                # Destination unreachable
                icmp_code = hop['icmp_code']
                if icmp_code == 0:
                    # Network unreachable
                    reply['err'] = 'N'
                elif icmp_code == 1:
                    # Host unreachable
                    reply['err'] = 'H'
                elif icmp_code == 2:
                    # Protocol unreachable
                    reply['err'] = 'P'
                elif icmp_code == 3:
                    # Port unreachable
                    reply['err'] = 'p'
                elif icmp_code in (9, 10, 13):
                    # Administratively forbidden
                    reply['err'] = 'A'
                else:
                    reply['err'] = icmp_code
                result[ttl].append(reply)
            else:
                logging.warning(f'Unknown ICMP type: {icmp_type}')

    # Insert non-responsive hops
    star_reply = {'x': '*'}
    last_ttl = 0
    for ttl in sorted(result.keys()):
        while last_ttl + 1 < ttl:
            # Gap in the hops
            last_ttl += 1
            result[last_ttl].append(star_reply)
        last_ttl = ttl

    if stop_reason == 'GAPLIMIT':
        # GAPLIMIT is triggered after five consecutive timeouts.
        for _ in range(5):
            last_ttl += 1
            result[last_ttl].append(star_reply)
    elif stop_reason == 'LOOP':
        # In case of a LOOP the traceroute stops without any errors
        # in the hops, so add a fake non-responsive hop to show that
        # there was a problem.
        last_ttl += 1
        result[last_ttl].append(star_reply)
    elif stop_reason == 'COMPLETED':
        # Sanity check.
        reply_found = False
        for reply in result[last_ttl]:
            if reply['from'] == ark['dst']:
                reply_found = True
                break
        if not reply_found:
            logging.warning(f'Stop reason is COMPLETED, but destination '
                            f'address {ark["dst"]} not in last result: '
                            f'{result[last_ttl]}')

    # If the monitor uses a private IP, treat the first public IP as
    # source instead.
    if ip_address(ark['src']).is_private:
        new_src = None
        original_size = len(result)
        for ttl in sorted(result.keys()):
            non_private_replies = list()
            for reply in result[ttl]:
                if 'x' in reply \
                    or 'from' in reply \
                        and ip_address(reply['from']).is_private:
                    continue
                non_private_replies.append(reply)
            if len(non_private_replies) > 0:
                new_src = non_private_replies[0]['from']
                break
            result.pop(ttl)
        if new_src is None:
            logging.debug(f'Failed to find non-private address in traceroute: '
                          f'{ark}')
            return dict()
        atlas['from'] = new_src

        # Shift hop numbers to start at hop 1.
        shift = None
        new_result = dict()
        for ttl in sorted(result.keys()):
            if shift is None:
                shift = ttl - 1
            new_result[ttl - shift] = result[ttl]
        result = new_result

        logging.debug(f'Changed from private source {ark["src"]} to {new_src} '
                      f'and removed {original_size - len(new_result)} hops.')

    # Convert to structure used in Atlas.
    for ttl, replies in sorted(result.items(), key=lambda t: t[0]):
        atlas['result'].append({'hop': ttl, 'result': replies})

    return atlas


def warts2json(warts_file: str, ark_metadata: dict, use_old_bin: bool = False) -> list:
    hostname = ark_metadata['hostname']
    cycle_id = None
    if 'cycle_id' in ark_metadata:
        cycle_id = ark_metadata['cycle_id']
    sc_warts2json_bin = 'sc_warts2json'
    if use_old_bin:
        sc_warts2json_bin = OLD_WARTS2JSON_BIN

    with sp.Popen([sc_warts2json_bin],
                  stdin=sp.PIPE,
                  stdout=sp.PIPE) as process:
        with gzip.open(warts_file, 'rb') as f:
            stdout, stderr = process.communicate(f.read())
        if process.returncode != 0:
            logging.error(f'sc_warts2json exited with non-zero returncode: '
                          f'{process.returncode}')
            return list()
    json_data = stdout.decode('utf-8').split('\n')
    logging.debug(f'Read {len(json_data)} JSON lines.')

    traceroutes = list()
    for traceroute in json_data:
        # Ignore empty lines.
        if not traceroute:
            continue

        try:
            traceroute_json = json.loads(traceroute)
        except JSONDecodeError as e:
            logging.warning(f'Failed to parse JSON: {traceroute}')
            logging.warning(e)
            continue

        if 'type' not in traceroute_json:
            logging.warning(f'Skipping malformed JSON entry: '
                            f'{traceroute_json}')
            continue

        if traceroute_json['type'].startswith('cycle'):
            # Sanity check.
            if 'hostname' not in traceroute_json or 'id' not in traceroute_json:
                logging.warning(f'Skipping malformed cycle entry: '
                                f'{traceroute_json}')
                continue
            if cycle_id is not None \
                    and traceroute_json['id'] != cycle_id:
                logging.error(f'Cycle id extracted from filename does not '
                              f'match metadata in warts file. File cycle id: '
                              f'{cycle_id} | Metadata cycle id: '
                              f'{traceroute_json["id"]}')
                return list()
            if not traceroute_json['hostname'].startswith(hostname):
                logging.warning(f'Hostname extracted from filename does not '
                                f'match metadata in warts file. File hostname: '
                                f'{hostname} | Metadata hostname: '
                                f'{traceroute_json["hostname"]}')

        elif traceroute_json['type'] == 'trace':
            traceroutes.append(traceroute_json)
        else:
            logging.warning(f'Skipping unknown JSON entry type: '
                            f'{traceroute_json}')

    logging.debug(f'Read {len(traceroutes)} traceroutes.')

    return traceroutes


def generate_output_file_name(input_file: str, output_dir: str) -> str:
    if not input_file.endswith(INPUT_SUFFIX):
        logging.error(f'Expected {INPUT_SUFFIX} input file.')
        return str()
    base = os.path.basename(input_file)
    return f'{output_dir}{base[:-len(INPUT_SUFFIX)]}{OUTPUT_SUFFIX}'


def write_output(traceroutes: list, output_file: str) -> None:
    logging.debug(f'Writing gzipped JSON output to file: {output_file}')
    with gzip.open(output_file, 'wt') as f:
        for traceroute in traceroutes:
            if not traceroute:
                continue
            json.dump(traceroute, f)
            f.write('\n')


def get_host_and_cycle(warts_file: str) -> Tuple[str, int]:
    file_name = os.path.basename(warts_file)
    file_name_split = file_name.split('.')
    if file_name_split[0] == 'daily':
        cycle = file_name_split[3]
        hostname = file_name_split[5]
    else:
        hostname = file_name_split[0]
        cycle = file_name_split[2]
    cycle_id = int(cycle[1:])
    return hostname, cycle_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='input warts file')
    parser.add_argument('output_dir')
    parser.add_argument('-f', '--force',
                        action='store_true',
                        help='overwrite existing files')
    parser.add_argument('--old',
                        action='store_true',
                        help='use old version of sc_warts2json')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO,
        filename='transform.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    input_file = args.input
    output_dir = args.output_dir
    if not output_dir.endswith('/'):
        output_dir += '/'
    output_file = generate_output_file_name(input_file, output_dir)
    if not output_file:
        sys.exit(1)

    if output_file and os.path.exists(output_file) and not args.force:
        logging.info(f'Output file {output_file} already exists. Use -f to '
                     f'force overwrite.')
        return

    hostname, cycle_id = get_host_and_cycle(input_file)
    ark_metadata = {'mode': 'probe_data',
                    'hostname': hostname,
                    'cycle_id': cycle_id}

    logging.debug(f'Reading file {input_file}')
    traceroutes = warts2json(input_file, ark_metadata, use_old_bin=args.old)

    atlas_traceroutes = [transform(traceroute, ark_metadata)
                         for traceroute in traceroutes]
    if not atlas_traceroutes:
        sys.exit(1)
    os.makedirs(output_dir, exist_ok=True)
    write_output(atlas_traceroutes, output_file)


if __name__ == '__main__':
    main()
    sys.exit(0)
