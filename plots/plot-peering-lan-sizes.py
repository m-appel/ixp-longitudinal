import bz2
import pickle
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Tuple

import matplotlib.pyplot as plt

from shared_functions import generate_labels

INPUT_FILE = '../stats/pdb.prefix_lengths.pickle.bz2'
DATA_DELIMITER = ','
MONTH_STEP = 12
LABEL_FMT = '%Y'

def insert_values(data_classes: dict, values: Counter, timestamp: int) -> None:
    for prefix_len, count in values.items():
        data_classes[prefix_len][timestamp] = count


def create_filled_dict(timestamps: list, data_classes: dict) -> dict:
    ret = defaultdict(list)
    for prefix_len in sorted(data_classes):
        for timestamp in timestamps:
            count = 0
            if timestamp in data_classes[prefix_len]:
                count = data_classes[prefix_len][timestamp]
            ret[prefix_len].append(count)
    return ret


def load_prefix_lengths() -> Tuple[list, dict, dict]:
    with bz2.open(INPUT_FILE) as f:
        data = pickle.load(f)
    timestamps = list()
    v4_data_classes = defaultdict(dict)
    v6_data_classes = defaultdict(dict)

    for date, v4, v6 in data:
        timestamp = int(datetime.strptime(date, '%Y%m%d').timestamp())
        timestamps.append(timestamp)
        insert_values(v4_data_classes, v4, timestamp)
        insert_values(v6_data_classes, v6, timestamp)

    # Sort and fill gaps.
    timestamps.sort()
    v4_data = create_filled_dict(timestamps, v4_data_classes)
    v6_data = create_filled_dict(timestamps, v6_data_classes)
    return timestamps, v4_data, v6_data




def plot(timestamps: list, data: dict, output_file: str, stat_file: str = str(), params: dict = dict()) -> None:
    fig, ax = plt.subplots()
    ax.stackplot(timestamps, data.values(), labels=data.keys())

    x_ticks, x_tick_labels = generate_labels(timestamps, MONTH_STEP, LABEL_FMT)
    ax.set_xticks(x_ticks, labels=x_tick_labels, rotation=90, ha='center')

    ax.set_ylabel('#Peering LANs')
    if 'title' in params:
        ax.set_title(params['title'])
    ax.legend(bbox_to_anchor=(1, 1), loc='upper left')
    plt.savefig(output_file, bbox_inches='tight')

    if stat_file:
        with open(stat_file, 'w') as f:
            headers = ('timestamp', *data.keys())
            f.write(DATA_DELIMITER.join(map(str, headers)) + '\n')
            for idx in range(len(timestamps)):
                line = [datetime.fromtimestamp(timestamps[idx], tz=timezone.utc).strftime('%Y%m%d')]
                for data_class in data.values():
                    line.append(data_class[idx])
                f.write(DATA_DELIMITER.join(map(str, line)) + '\n')


def main() -> None:
    timestamps, v4_data, v6_data = load_prefix_lengths()
    plot(timestamps, v4_data,
         '../figs/peering_lan_sizes.v4.pdf',
         stat_file='../figs/peering_lan_sizes.v4.csv',
         params={'title': 'Size of IX IPv4 Peering LANs'})
    plot(timestamps, v6_data,
         '../figs/peering_lan_sizes.v6.pdf',
         stat_file='../figs/peering_lan_sizes.v6.csv',
         params={'title': 'Size of IX IPv6 Peering LANs'})


if __name__ == '__main__':
    main()
