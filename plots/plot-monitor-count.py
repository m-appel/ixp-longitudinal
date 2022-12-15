import sys
from collections import defaultdict
from datetime import datetime

import matplotlib.pyplot as plt

from shared_functions import generate_labels


INPUT_FILE = '../stats/monitor_availability.txt'
OUTPUT_FILE = '../figs/monitor_count.pdf'
DATA_DELIMITER = ','
MONTH_STEP = 12
LABEL_FMT = '%Y'


def get_dates() -> dict:
    ret = defaultdict(set)
    with open(INPUT_FILE, 'r') as f:
        f.readline()
        for line in f:
            monitor, date_str = line.strip().split(DATA_DELIMITER)
            date = datetime.strptime(date_str, '%Y%m%d')
            if date.year < 2010:
                continue
            ret[int(date.timestamp())].add(monitor)
    return to_count(ret)

def to_count(data: dict) -> dict:
    return {k: len(v) for k, v in sorted(data.items())}


def plot(data: dict) -> None:
    fig, ax = plt.subplots()

    x_vals = list()
    y_vals = list()
    for timestamp, count in data.items():
        x_vals.append(timestamp)
        y_vals.append(count)

    ax.plot(x_vals, y_vals, )

    x_ticks, x_tick_labels = generate_labels(x_vals, MONTH_STEP, LABEL_FMT)
    ax.set_xticks(x_ticks, labels=x_tick_labels, rotation=90, ha='center')

    ax.set_ylabel('#Monitors')
    ax.set_title('Ark Monitor Availability')

    plt.savefig(OUTPUT_FILE, bbox_inches='tight')


def main() -> None:
    data = get_dates()
    plot(data)


if __name__ == '__main__':
    main()
    sys.exit(0)
