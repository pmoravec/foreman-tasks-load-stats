#!/usr/bin/env python

import argparse
from datetime import *
from dateutil.tz import tzutc

intervals = []
timestamps = set()
utctz = tzutc()
now = datetime.now().replace(tzinfo=utctz).timestamp()


def _convert_date_time_to_seconds(ts):
    try:
        ret = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f") \
                      .replace(tzinfo=utctz).timestamp()
    except ValueError:
        ret = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") \
                      .replace(tzinfo=utctz).timestamp()
    return ret


def _convert_ts_arg_to_seconds(ts):
    try:
        ret = float(ts)
    except ValueError:
        ret = _convert_date_time_to_seconds(ts)
    return ret


parser = argparse.ArgumentParser(description="Sidekiq workers heat stats and "
                                             "graph")
parser.add_argument("dynflow_steps",
                    help="Input CSV file with dynflow_steps")
parser.add_argument("--from",
                    dest='from_ts',
                    type=str,
                    default="0",
                    help="Consider tasks from this timestamp (seconds since "
                         "Epoch or '%%Y-%%m-%%d %%H:%%M:%%S' format)")
parser.add_argument("--to",
                    type=str,
                    default=str(now),
                    help="Consider tasks to this timestamp (seconds since "
                         "Epoch or '%%Y-%%m-%%d %%H:%%M:%%S' format)")
parser.add_argument("--items-limit",
                    type=int,
                    default=5,
                    help="Limit of ordered dynflow steps statistics")
parser.add_argument("--show-graph",
                    type=bool,
                    default=True,
                    help="Show graph of dynflow concurrency & sidekiq load. "
                         "Requires matplotlib library")

args = parser.parse_args()
from_ts = _convert_ts_arg_to_seconds(args.from_ts)
to_ts = _convert_ts_arg_to_seconds(args.to)

if from_ts == 0 and to_ts == now:
    print("Warning: with no --from and --to, the processing time may be long.")

print(f"Processing '{args.dynflow_steps}'..")
for line in open(args.dynflow_steps, 'r'):
    cols = line.split(',')
    # ignore incomplete lines
    if len(cols) < 16:
        continue
    start = cols[4]
    finish = cols[5]
    exectime = cols[7]
    label = cols[11]
    if len(start)*len(exectime) == 0:
        continue
    try:
        start = _convert_date_time_to_seconds(start)
    except ValueError:
        continue
    if len(finish) == 0:
        finish = now
    else:
        try:
            finish = _convert_date_time_to_seconds(finish)
        except ValueError:
            continue
    try:
        exectime = float(exectime)
    except Exception:
        continue
    # skip tasks completely outside specified interval
    if start > to_ts or finish < from_ts:
        continue
    # truncate steps starting before --from and adjust exectime accordingly
    if start < from_ts:
        exectime *= (finish-from_ts) / (finish-start)
        start = from_ts
    # truncate steps ending after --to and adjust exectime accordingly
    if finish > to_ts:
        exectime *= (to_ts-start) / (finish-start)
        finish = to_ts
    intervals.append((start, finish, exectime, label))
    timestamps.add(start)
    timestamps.add(finish)

# TODO: add progress bar e.g. from
# https://github.com/pavlinamv/rails-load-stats-py/blob/main/progress_bar.py
print("Summarizing input data..")
print()
# sorted list of timestamps for easy "get me next timestamp/interval" search
timestamps_sorted = list(sorted(timestamps))
# output data structure to keep info like
# start_interval - end_interval: #dynflow_steps, avg_exec_load
# where start_interval is key to the dict
heat_intervals = dict()
# labels: dict with key of dynflow step label and values:
#   'count': count of the label in input data
#   'exectime': sum of execution times of steps with this label
labels = dict()
ts_prev = 0
for ts in timestamps_sorted:
    heat_intervals[ts] = {'end': now, 'steps': 0, 'load': 0.0}
    if ts_prev > 0:
        heat_intervals[ts_prev]['end'] = ts
    ts_prev = ts

for start, finish, exectime, label in intervals:
    if label not in labels.keys():
        labels[label] = {'count': 0, 'exectime': 0.0}
    labels[label]['count'] += 1
    labels[label]['exectime'] += exectime
    load = exectime / (finish-start)
    ts = start
    while finish > heat_intervals[ts]['end']:
        heat_intervals[ts]['steps'] += 1
        heat_intervals[ts]['load'] += load
        ts = heat_intervals[ts]['end']  # skip to the next interval

labels_list = [(label[1]['count'], label[1]['exectime'], label[0])
               for label in labels.items()]

s = f"Top {args.items_limit} dynflow step labels per count:"
print(s)
print("-"*len(s))
labels_list.sort(key=lambda x: x[0], reverse=True)
print(f"{'steps':<8}{'exec.time':<10}label")
for steps, exectime, label in labels_list[0:args.items_limit]:
    print(f"{steps:<8}{exectime:<10,.2f}{label}")
print()

s = f"Top {args.items_limit} dynflow step labels per execution time:"
print(s)
print("-"*len(s))
labels_list.sort(key=lambda x: x[1], reverse=True)
print(f"{'steps':<8}{'exec.time':<10}label")
for steps, exectime, label in labels_list[0:args.items_limit]:
    print(f"{steps:<8}{exectime:<10,.2f}{label}")
print()

s = "Intervals with distinct sidekiq load"
print(s)
print("-"*len(s))
fname = f"{args.dynflow_steps}.sidekiq_load.csv"
with open(fname, "w") as _file:
    _file.write("start;duration;concur.steps;avg.exec.load\n")
    for ts in heat_intervals.keys():
        ts_out = datetime.fromtimestamp(ts, timezone.utc)
        _file.write(f"{ts_out.isoformat('T', 'microseconds')};"
                    f"{heat_intervals[ts]['end']-ts};"
                    f"{heat_intervals[ts]['steps']};"
                    f"{heat_intervals[ts]['load']}\n")
print(f".. in {fname}")

if args.show_graph:
    print()
    try:
        import matplotlib.pyplot as plt
        from matplotlib import rcParams
        from matplotlib.dates import DateFormatter
    except ImportError:
        print("Missing matplotlib library, try installing python3-matplotlib "
              "package.")
        exit()
    print("Generating heat graph of dynflow/sidekiq usage..")
    timestamps = [datetime.fromtimestamp(ts, timezone.utc)
                  for ts in heat_intervals.keys()]
    steps = [val['steps'] for val in heat_intervals.values()]
    loads = [val['load'] for val in heat_intervals.values()]
    fig, ax = plt.subplots()
    ax.xaxis.set_major_formatter(DateFormatter("%Y-%m-%dT%H:%M:%S"))
    # TODO: on x-axis, print value at minimum/start?
    ax.set(xlim=(timestamps[0], timestamps[-1]))
    fig.autofmt_xdate()
    plt.plot(timestamps, steps, marker='.', antialiased=True, mouseover=False,
             label="concur. dynflow steps")
    plt.plot(timestamps, loads, marker='.', antialiased=True, mouseover=True,
             label="sidekiq avg.load")
    plt.grid(visible=True, linestyle='dotted')
    plt.legend()
    plt.suptitle(f"Sidekiq load over time", weight='bold')
    plt.title(f"from {timestamps[0]} to {timestamps[-1]}",
              size=rcParams['font.size']-2)
    plt.show()
