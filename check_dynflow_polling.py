#!/usr/bin/python3 -s
#
# Is dynflow/sidekiq performing external tasks polling frequently enough?

import argparse
import re
from datetime import *
from os.path import isfile, isdir, join

parser = argparse.ArgumentParser(description="Dynflow polling checker against "
                                             "system logs")
parser.add_argument("sosreport_dir",
                    help="sosreport directory (preferred) or individual "
                         "logfile")
parser.add_argument("--poll-multiplier", "--multiplier", "-m",
                    type=int,
                    dest="multiplier",
                    help="Set or override foreman_tasks_polling_multiplier. "
                         "The value multiplied by 16s is the upper limit for "
                         "frequency of polling of the same pulp task in logs.")
parser.add_argument("--add-rounding-error", "-a",
                    type=int,
                    default=2,
                    help="Allow small delay in frequency caused by rounding "
                         "errors in stating timestamps in whole seconds. This "
                         "value should be at most a few seconds, in order to "
                         "catch border cases of imperformant dynflow.")

args = parser.parse_args()

# parse foreman_settings_table line like:
#  18 | foreman_tasks_polling_multiplier      | --- 5 ..
REGEXP_POLLING_MULTIPLIER = re.compile(
        r".* foreman_tasks_polling_multiplier.*--- (\d+)")

# extract timestamp and task id from log entries like:
# 1.2.3.4 - - [08/Oct/2024:10:04:49 +0200] "GET /pulp/api/v3/tasks/01926b28-cf33-7a80-afdc-3d0413d900f6/ HTTP/1.1" 200 559 "-" "OpenAPI-Generator/3.39.2/ruby"
REGEXP_GET_PULPTASK = re.compile(
        r".*\[(.*)\] \"GET /pulp/api/v3/tasks/(.*)/ .*")

# timestamp format in input data
TS_FORMAT = '%d/%b/%Y:%H:%M:%S'

multiplier = 1
if isdir(args.sosreport_dir):
    input_files = []
    for logfile in ['var/log/httpd/foreman-ssl_access_ssl.log',
                    'var/log/messages',
                    'sos_commands/logs/journalctl_--no-pager',
                    ]:
        fullpath = join(args.sosreport_dir, logfile)
        if isfile(fullpath):
            input_files.append(fullpath)
    # read foreman_tasks_polling_multiplier
    settings_file = join(args.sosreport_dir,
                         'sos_commands/foreman/foreman_settings_table')
    if isfile(settings_file):
        for line in open(settings_file, 'r').readlines():
            match = REGEXP_POLLING_MULTIPLIER.match(line)
            if match:
                multiplier = int(match.group(1))
                print(f"Found foreman_tasks_polling_multiplier={multiplier}")
                if args.multiplier:
                    print(f".. cmdline option --multiplier overrides it to "
                          f"{args.multiplier}!")
                print()
                break
else:
    input_files = [args.sosreport_dir]  # the argument sosreport_dir is a file

maxdelay = 16 * (args.multiplier or multiplier) + args.add_rounding_error

for _file in input_files:
    print(f"Processing file {_file}..")
    # last_seen: when polling status of a pulp task was last seen?
    # key: task UUID, value: datetime
    last_seen = dict()
    for line in open(_file, encoding="utf-8", errors="replace").readlines():
        match = REGEXP_GET_PULPTASK.match(line)
        if match:
            timestamp = match.group(1)[:20]
            task_id = match.group(2)
            now = datetime.strptime(timestamp, TS_FORMAT)
            prev = last_seen.get(task_id, now)
            diff = (now-prev).seconds
            if diff > maxdelay:
                print(f"Task '{task_id}' polled at "
                      f"'{prev.strftime(TS_FORMAT)}' and then at "
                      f"'{now.strftime(TS_FORMAT)}', delay {diff}s is bigger "
                      f"than maximum {maxdelay}s.")
            last_seen[task_id] = now
    print()

# vim:ts=4 et sw=4
