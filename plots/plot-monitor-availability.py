import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import matplotlib as mpl
import matplotlib.pyplot as plt


DATA_DELIMITER = ','
SECONDS_IN_DAY = 60 * 60 * 24
COLORS = ['#e41a1c', '#377eb8']

mpl.rcParams['figure.figsize'] = (6.4, 25)
mpl.rcParams['axes.grid'] = True
mpl.rcParams['axes.grid.axis'] = 'x'


def get_dates(input_file: str) -> dict:
    ret = defaultdict(list)
    with open(input_file, 'r') as f:
        f.readline()
        for line in f:
            monitor, date_str = line.strip().split(DATA_DELIMITER)
            date = datetime.strptime(date_str, '%Y%m%d')
            if date.year < 2010:
                continue
            ret[monitor].append(int(date.timestamp()))
    return ret


def simplify(timestamps: list) -> list:
    ret = list()
    interval_start = timestamps[0]
    prev_ts = timestamps[0]
    for idx in range(1, len(timestamps)):
        curr_ts = timestamps[idx]
        if curr_ts == prev_ts + SECONDS_IN_DAY:
            prev_ts = curr_ts
            continue
        ret.append((interval_start, timestamps[idx - 1]))
        prev_ts = curr_ts
    print(f'{len(timestamps)} to {len(ret)} intervals.')
    return ret


def plot(data: dict, output_file: str) -> None:
    fig, ax = plt.subplots()

    y_labels = list()
    color_cycle = 0
    first_timestamp = None
    last_timestamp = 0
    for y_idx, (monitor, timestamps) in enumerate(sorted(data.items())):
        y_labels.append(monitor)
        timestamps.sort()
        if timestamps[-1] > last_timestamp:
            last_timestamp = timestamps[-1]
        if first_timestamp is None or timestamps[0] < first_timestamp:
            first_timestamp = timestamps[0]
        timestamp_intervals = simplify(timestamps)
        for x_interval in timestamp_intervals:
            ax.plot(x_interval, [y_idx] * 2, color=COLORS[color_cycle])
        color_cycle = int(not color_cycle)
    ax.set_yticks(range(len(y_labels)), labels=y_labels, fontsize=6)
    ax.set_ylim(-1, len(y_labels))

    ax.invert_yaxis()

    curr_ts = datetime.fromtimestamp(first_timestamp, tz=timezone.utc).replace(day=1)
    last_ts = datetime.fromtimestamp(last_timestamp, tz=timezone.utc)
    x_ticks = [(curr_ts.timestamp(), curr_ts.strftime('%Y-%m'))]
    while curr_ts <= last_ts:
        curr_month = curr_ts.month + 6
        curr_year = curr_ts.year
        if curr_month > 12:
            curr_month -= 12
            curr_year += 1
        curr_ts = curr_ts.replace(year=curr_year, month=curr_month)
        x_ticks.append((curr_ts.timestamp(), curr_ts.strftime('%Y-%m')))
    x_t, x_tl = zip(*x_ticks)
    ax.set_xticks(x_t, labels=x_tl, rotation=45, horizontalalignment='right')



    plt.savefig(output_file, bbox_inches='tight')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file')
    parser.add_argument('output_file')
    args = parser.parse_args()

    input_file = args.input_file
    data = get_dates(input_file)
    output_file = args.output_file
    plot(data, output_file)


if __name__ == '__main__':
    main()
    sys.exit(0)
