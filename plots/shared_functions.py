from datetime import datetime, timezone
from typing import Tuple

def increment_months(dt: datetime, months: int) -> datetime:
    new_month = dt.month + months
    if new_month > 12:
        return dt.replace(year=dt.year + (new_month // 12), month=new_month % 12)
    return dt.replace(month=new_month)


def generate_labels(timestamps: list, month_step: int, label_fmt: str) -> Tuple[list, list]:
    curr_dt = datetime.fromtimestamp(timestamps[0], tz=timezone.utc).replace(month=1, day=1)
    last_dt = increment_months(datetime.fromtimestamp(timestamps[-1], tz=timezone.utc).replace(month=12, day=1), 1)

    label_timestamps = list()
    label_strings = list()
    while curr_dt <= last_dt:
        label_timestamps.append(int(curr_dt.timestamp()))
        label_strings.append(curr_dt.strftime(label_fmt))
        curr_dt = increment_months(curr_dt, month_step)
    return label_timestamps, label_strings