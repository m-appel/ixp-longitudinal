import argparse
import logging
import sys

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END

from network_dependency.utils.helper_functions import parse_timestamp_argument
from kafka_wrapper.kafka_reader import KafkaReader


DATA_DELIMITER = ','


def make_line(msg: dict) -> str:
    data = (msg['scope'], msg['asn'], msg['hege'], msg['nb_peers'])
    return DATA_DELIMITER.join(map(str, data)) + '\n'


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('kafka_topic')
    parser.add_argument('output_file')
    parser.add_argument('-ts', '--timestamp',
                        help='Timestamp (as UNIX epoch in seconds or '
                             'milliseconds, or in YYYY-MM-DDThh:mm format)')
    parser.add_argument('--bootstrap-server', default='localhost:9092')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    kafka_topic = args.kafka_topic
    output_file = args.output_file

    start_ts = OFFSET_BEGINNING
    end_ts = OFFSET_END
    ts_arg = args.timestamp
    if ts_arg:
        start_ts = parse_timestamp_argument(ts_arg) * 1000
        if start_ts == 0:
            logging.error(f'Invalid start timestamp specified: {ts_arg}')
            sys.exit(1)
        end_ts = start_ts + 1

    reader = KafkaReader([kafka_topic], args.bootstrap_server, start_ts, end_ts)
    msg_count = 0
    with reader, open(output_file, 'w') as o:
        headers = ('scope', 'asn', 'hegemony', 'peers')
        o.write(DATA_DELIMITER.join(headers) + '\n')
        for msg in reader.read():
            o.write(make_line(msg))
            msg_count += 1
    logging.info(f'Wrote {msg_count} lines to {output_file}')


if __name__ == '__main__':
    main()
    sys.exit(0)
