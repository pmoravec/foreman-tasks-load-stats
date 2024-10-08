#!/usr/bin/env python

from copy import deepcopy
import argparse
import json
import csv
import sys
import os
from datetime import *
from dateutil.tz import tzutc

# increase a csv buffer size to prevent "field larger than field limit" error
# when processing too long fields
maxInt = sys.maxsize
while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

utctz = tzutc()
now = datetime.now().replace(tzinfo=utctz).timestamp()


def _convert_datetime_to_seconds(ts):
    try:
        ret = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f") \
                      .replace(tzinfo=utctz).timestamp()
    except ValueError:
        ret = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") \
                      .replace(tzinfo=utctz).timestamp()
    return ret


def _convert_pulp_datetime_to_seconds(ts):
    return _convert_datetime_to_seconds(ts.replace('T', ' '))


# for an external task, add times from "task created/started/finished" to
# internal structures
def process_external_task(step_id, who, created, started, finished):
    try:
        created = _convert_pulp_datetime_to_seconds(created)
        started = _convert_pulp_datetime_to_seconds(started)
        finished = _convert_pulp_datetime_to_seconds(finished)
    except (KeyError, TypeError):
        return
    timestamps.add(created)
    timestamps.add(started)
    timestamps.add(finished)
    action_intervals[step_id].append((created, started-created, f'{who}wait'))
    action_intervals[step_id].append((started, finished-started, f'{who}exec'))
    absolute_times['sidewait'] -= finished-created
    absolute_times[f'{who}wait'] += started-created
    absolute_times[f'{who}exec'] += finished-started


parser = argparse.ArgumentParser(description="Blame task duration among "
                                             "sidekiq/pulp/candlepin. For all "
                                             "these components, treat real "
                                             "execution time as well as the "
                                             "waiting time untill the work "
                                             "starts",
                                 formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("foreman_directory",
                    help="Directory with foreman_tasks_tasks, dynflow_steps "
                         "and dynflow_actions")
parser.add_argument("--uuid",
                    type=str,
                    required=True,
                    help="Foreman or dynflow task UUID")
parser.add_argument("--metric",
                    type=str,
                    choices=['absolute', 'absolute-blame', 'relative-blame',
                             'all'],
                    default="relative-blame",
                    help="What metric to use for blaming.\n"
                         "'absolute': Sum of absolute values regardless of "
                         "concurrency. 'sidewait' can be negative as it is "
                         "realtime subsctracted by other possibly concurrent "
                         "values.\n"
                         "'absolute-blame': Summarize blame times regardless "
                         "of concurrency. I.e. when two dynflow steps run "
                         "concurrently, count them both.\n"
                         "'relative-blame': Relativize blame times per "
                         "concurrency. I.e. when two dynflow steps run "
                         "concurrently, count half of each blame time from "
                         "both.\n"
                         "'all': Show all three metrics.")

args = parser.parse_args()

# absolute / cumulative times, not respecting concurrency
zero_blame_times = {
    'sidewait': 0.0,
    'sideexec': 0.0,
    'pulpwait': 0.0,
    'pulpexec': 0.0,
    'candlewait': 0.0,
    'candleexec': 0.0
}
absolute_times = deepcopy(zero_blame_times)
# track all timestamps in a set
timestamps = set()
# for each dynflow step, keep start and finish timestamps and sidekiq's
# execution time - index them per action_id which is a sufficient common id for
# both action and step (for us)
steps_times = {}
# for each dynflow action, keep list of intervals "we started to wait on
# (pulp|candlepin) task at .. for time .."
action_intervals = {}
# even once we have action_intervals complete, we can distribute sidekiq
# execution time evenly to these "not sidekiq responsibility" intervals and to
# the remaining "sidekiq responsibility" intervals & store within blame_periods
blame_periods = set()  # of start_time, duration, who-to-blame, weight
# cumul_blame_intervals are cumulative blame_periods grouped into individual
# intervals from `timestamps`
# format: key=start_time, value={duration, zero_blame_times, concurrency) where
# concurrency is the number of concurrent steps (to split the blame evenly)
cumul_blame_intervals = {}
relative_blame_intervals = deepcopy(zero_blame_times)
absolute_blame_intervals = deepcopy(zero_blame_times)

foreman_uuid = None
dynflow_uuid = None
fdir = args.foreman_directory
foreman_tasks_fname = os.path.join(fdir, "foreman_tasks_tasks")
dynflow_steps_fname = os.path.join(fdir, "dynflow_steps")
dynflow_actions_fname = os.path.join(fdir, "dynflow_actions")

# identify foreman_uuid and dynflow_uuid by traversing foreman_tasks_fname
for line in open(foreman_tasks_fname, 'r'):
    cols = line.split(',')
    # ignore incomplete lines
    if len(cols) < 14:
        continue
    if cols[0] == args.uuid or cols[7] == args.uuid:
        foreman_uuid = cols[0]
        dynflow_uuid = cols[7]
        break

if not foreman_uuid:
    print(f"Could not find a foreman or dynflow task with id {args.uuid} in"
          f" file {foreman_tasks_fname}")
    exit()

for line in open(dynflow_steps_fname, 'r'):
    cols = line.split(',')
    # ignore incomplete lines
    if len(cols) < 16:
        continue
    # ignore steps from other tasks
    if cols[0] != dynflow_uuid:
        continue
    step_id = cols[2]  # in fact it is action_id, not step_id
    started_at = cols[4]
    ended_at = cols[5]
    realtime = cols[6]
    exectime = cols[7]
    # ignore incomplete data - neither length can be zero
    if 0 in (len(started_at), len(ended_at), len(realtime), len(exectime)):
        continue
    try:
        realtime = float(realtime)
        exectime = float(exectime)
    except Exception:
        continue
    try:
        started_at = _convert_datetime_to_seconds(started_at)
        ended_at = _convert_datetime_to_seconds(ended_at)
    except ValueError:
        continue

    timestamps.add(started_at)
    timestamps.add(ended_at)
    if step_id not in steps_times.keys():
        steps_times[step_id] = []
        action_intervals[step_id] = []
    steps_times[step_id].append((started_at, ended_at, realtime, exectime))
    absolute_times['sidewait'] += realtime-exectime
    absolute_times['sideexec'] += exectime

with open(dynflow_actions_fname, newline='') as _file:
    for row in csv.reader(_file, delimiter=',', quotechar='"'):
        # ignore incomplete records
        if len(row) < 11:
            continue
        # ignore records for other dynflow UUID
        if row[0] != dynflow_uuid:
            continue
        try:
            data = json.loads(row[10])
        except json.decoder.JSONDecodeError:
            continue
        step_id = row[1]
        # if dynflow_steps are truncated, we might not know the [real/exec]time
        # then skip further calculation
        if step_id not in steps_times.keys():
            continue
        # pulp tasks
        if 'pulp_tasks' in data:
            for task in data['pulp_tasks']:
                process_external_task(step_id, 'pulp',
                                      task['pulp_created'][:23],
                                      task['started_at'][:23],
                                      task['finished_at'][:23])
        # pulp task groups
        if 'task_groups' in data and data['task_groups']:
            for group in data['task_groups']:
                for task in group["tasks"]:
                    process_external_task(step_id, 'pulp',
                                          task['pulp_created'][:23],
                                          task['started_at'][:23],
                                          task['finished_at'][:23])
        # candlepin tasks
        if 'task' in data:
            task = data['task']
            # time format is '2024-10-02T12:18:04+0000' so strip the trailing
            # timezone
            process_external_task(step_id, 'candle',
                                  task['created'].split('+')[0],
                                  task['startTime'].split('+')[0],
                                  task['endTime'].split('+')[0])

# for each action_intervals[step_id], distribute the execution time among
# partial intervals and store the final value in final intervals.
# Then sort action_intervals per time, to traverse it linearly in time
# to feed blame_periods
for step_id in action_intervals.keys():
    # insert one dummy record to prevent "is there a record .." tests
    action_intervals[step_id].append((now, 0, 'pulpexec'))
    action_intervals[step_id].sort(key=lambda x: x[0])
for step_id in steps_times.keys():
    for started_at, ended_at, realtime, exectime in steps_times[step_id]:
        # if whole interval was spent by sidekiq execution, skip finding any
        # external action, there won't be
        if realtime == exectime:
            blame_periods.add((started_at, ended_at-started_at, 'sideexec', 1))
            continue
        exec2real = exectime/realtime
        # while there is an action within this sidekiq interval..
        while ended_at > action_intervals[step_id][0][0]:
            # blame sidekiq for period prior the external action
            if started_at < action_intervals[step_id][0][0]:
                duration = action_intervals[step_id][0][0]-started_at
                blame_periods.add((started_at, duration, 'sidewait',
                                   1-exec2real))
                blame_periods.add((started_at, duration, 'sideexec',
                                   exec2real))
            # now blame pulp/candlepin and sideexec - nowadays, we treat both
            # of them fully concurrently, not interfering each other "blame"
            # or weight. This approach alone could mean simplier code but the
            # current code is prepared for a variant "blame them with proper"
            # weights" (since sideexec might affect pulp/candlepin..?)
            blame_periods.add((action_intervals[step_id][0][0],
                               action_intervals[step_id][0][1],
                               action_intervals[step_id][0][2], 1))
            blame_periods.add((action_intervals[step_id][0][0],
                               action_intervals[step_id][0][1],
                               'sideexec', exec2real))
            # move in time beyond the action
            started_at = action_intervals[step_id][0][0] + \
                action_intervals[step_id][0][1]
            action_intervals[step_id].pop(0)
        # if there is a trailing time spent by sidekiq, blame for it
        if started_at < ended_at:
            duration = ended_at-started_at
            blame_periods.add((started_at, duration, 'sidewait', 1-exec2real))
            blame_periods.add((started_at, duration, 'sideexec', exec2real))

if len(timestamps) == 0:
    print("No dynflow step found, nothing to blame.")
    exit()

# transform blame_periods into cumul_blame_intervals
# first initialise cumul_blame_intervals
next_ts = max(timestamps)
for ts in sorted(timestamps, reverse=True):
    cumul_blame_intervals[ts] = [next_ts-ts, deepcopy(zero_blame_times), 0]
    next_ts = ts
cumul_blame_intervals = dict(sorted(cumul_blame_intervals.items()))
# now, add each of blame_periods into corresponding cumul_blame_intervals
for ts, duration, who, weight in blame_periods:
    while duration > 0:
        interval = cumul_blame_intervals[ts]
        interval[1][who] += weight
        interval[2] += 1
        duration -= interval[0]
        ts += interval[0]

# summarize cumul_blame_intervals over time (absolute_blame_intervals) and
# also concurrency (relative_blame_intervals)
whos = absolute_blame_intervals.keys()
for duration, blame_times, concurrency in cumul_blame_intervals.values():
    if concurrency == 0:
        continue
    for who in whos:
        absolute_blame_intervals[who] += duration*blame_times[who]
        relative_blame_intervals[who] += duration*blame_times[who]/concurrency

# print header
blame_keys = zero_blame_times.keys()
keys_str = f"{'TOTAL':>12} {'pct.':>6}"
for key in blame_keys:
    keys_str = f"{keys_str}{key:>12} {'pct.':>6}"
print(f"{foreman_uuid:>36} :{keys_str}")
# for each metric, print its row
# ['absolute', 'absolute-blame', 'relative-blame', 'all']
for (metric, description, argvalue) in (
        (absolute_times, "absolute times", "absolute"),
        (absolute_blame_intervals, "abs.blame times", "absolute-blame"),
        (relative_blame_intervals, "relative blame times", "relative-blame")):
    if args.metric != 'all' and args.metric != argvalue:
        continue
    sumtime = sum(metric.values())
    vals = f"{sumtime:>12,.2f} {100:>5,.1f}%"
    # prevent division by zero - percentage would be zero instead of 0/0
    if sumtime == 0:
        sumtime = 1
    for key in blame_keys:
        vals = f"{vals}{metric[key]:>12,.2f} {metric[key]/sumtime*100:>5,.1f}%"
    print(f"{description:>36} :{vals}")
