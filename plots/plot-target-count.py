import sys

import matplotlib.pyplot as plt

from shared_functions import generate_labels

INPUT_FILE = '../stats/target_counts.csv'
OUTPUT_FILE = '../figs/target_counts.pdf'
DATA_DELIMITER = ','
MONTH_STEP = 12
LABEL_FMT = '%Y'

def load_counts() -> list:
    ret = list()
    with open(INPUT_FILE, 'r') as f:
        f.readline()
        for line in f:
            timestamp, count = line.strip().split(DATA_DELIMITER)
            ret.append((int(timestamp), int(count)))
    return ret

def plot(data: list) -> None:
    timestamps, counts = zip(*data)

    fig, ax = plt.subplots()

    ax.plot(timestamps, counts)

    x_ticks, x_tick_labels = generate_labels(timestamps, MONTH_STEP, LABEL_FMT)
    ax.set_xticks(x_ticks, labels=x_tick_labels, rotation=90, ha='center')

    ax.set_ylabel('/24 Prefixes')
    ax.set_title('Number of Target /24 Prefixes')

    plt.savefig(OUTPUT_FILE, bbox_inches='tight')

def main() -> None:
    data = load_counts()
    plot(data)

if __name__ == '__main__':
    main()
    sys.exit(0)
