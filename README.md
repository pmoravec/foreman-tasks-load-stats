# foreman-tasks-load-stats
Scripts to analyze/blame load of foreman tasking system (dynflow/sidekiq and external tasks).

What makes a foreman task so slow? What is the most frequent or most busy dynflow step within some time? What was the load of sidekiq workers over the time? Were sidekiq workers polling external tasks frequently enough?

These scripts analyzing data from a [sosreport](https://github.com/sosreport/sos/) will help you finding those answers!

## Who made my task so slow? Was it sidekiq, pulp or candlepin?

Use `blame_foreman-task_execution.py` script:

```
./blame_foreman-task_execution.py /path/to/unpacked/sosreport/sos_commands/foreman --uuid 221a22c2-af88-433b-8414-ec43e6960800
687335a3-07e4-4b03-822b-504462fae378 :       TOTAL   pct.    sidewait   pct.    sideexec   pct.    pulpwait   pct.    pulpexec   pct.  candlewait   pct.  candleexec   pct.
                relative blame times :    1,834.30 100.0%        9.50   0.5%       45.97   2.5%        1.27   0.1%    1,777.57  96.9%        0.00   0.0%        0.00   0.0%
```

where `uuid` is either foreman task UUID or dynflow plan execution UUID. A path to unpacked sosreport directory with `foreman_tasks_tasks` + `dynflow_steps` + `dynflow_actions` CSV export is required. Columns explanation:

- `TOTAL`: evident
- `sidewait`: how much time sidekiq spent in waiting on something - either for idle sidekiq worer or between polling attempts (if no external task is still running). Roughly it is dynflow step's Real time minus execution time minus any external task times.
- `sideexec`: the execution time of the dynflow step(s), i.e. how much time a sidekiq worker was activelly working on the step(s). High values mean `sidekiq` is too busy (worth scaling them or using the other script to know what made sidekiq busy).
- `pulpwait`: times between creating a pulp task and starting to execute the task. High values mean too few pulp workers (hence big tasks queue) or some tasks scheduling issue.
- `pulpexec`: how much time pulp workers were activelly executing their tasks. High values mean slow pulp execution.
- `candlewait`: times between raising a job to candlepin and starting the job by candlepin. High value means candlepin's internal job scheduler/invocation has an issue.
- `candleexec`: how much time candlepin spent on active execution of the jobs. High values mean inefficient candlepin.

How to (fairly) blame concurrently running tasks, where e.g. a sidekiq worker is busy in one dynflow step while pulp worker is executing a task in another step, at the same time? The `relative blame times` approach splits the blame 50:50. Or in general, splits the blame evenly to all concurrently running actions at any time - and summarizes the durations over the task's lifetime.

Use option `--metric=all` to see other "metrics" / how to make the blame, with their description from `--help`:

```
  --metric {absolute,absolute-blame,relative-blame,all}
                        What metric to use for blaming.
                        'absolute': Sum of absolute values regardless of concurrency. 'sidewait' can be negative as it is realtime subsctracted by other possibly concurrent values.
                        'absolute-blame': Summarize blame times regardless of concurrency. I.e. when two dynflow steps run concurrently, count them both.
                        'relative-blame': Relativize blame times per concurrency. I.e. when two dynflow steps run concurrently, count half of each blame time from both.
                        'all': Show all three metrics.
```

## Where sidekiq workers spent the most time? What was their load over time?

Use `heat_stats_sidekiq_workers.py` script:

```
./heat_stats_sidekiq_workers.py /path/to/unpacked/sosreport/sos_commands/foreman/dynflow_steps --from '2024-09-04 07:45:00' --to '2024-09-04 10:15:00'
Processing '/path/to/unpacked/sosreport/sos_commands/foreman/dynflow_steps'..
Summarizing input data..

Top 5 dynflow step labels per count:
------------------------------------
steps   exec.time label
1222    266.63    Actions::Katello::Applicability::Hosts::BulkGenerate
682     1,004.44  Actions::Katello::Repository::IndexContent
682     633.12    Actions::Pulp3::Repository::SaveDistributionReferences
682     1,079.98  Actions::Pulp3::Repository::RefreshDistribution
679     219.27    Actions::Pulp3::Orchestration::Repository::GenerateMetadata

Top 5 dynflow step labels per execution time:
---------------------------------------------
steps   exec.time label
192     214,563.84Actions::Katello::CapsuleContent::UpdateContentCounts
192     6,341.53  Actions::Katello::CapsuleContent::SyncCapsule
48      5,908.29  Actions::BulkAction
192     5,511.68  Actions::Katello::CapsuleContent::Sync
384     2,675.58  Actions::Pulp3::CapsuleContent::Sync

Intervals with distinct sidekiq load
------------------------------------
.. in /path/to/unpacked/sosreport/sos_commands/foreman/dynflow_steps.sidekiq_load.csv

Generating heat graph of dynflow/sidekiq usage..
```

.. and if you have `python3-matplotlib` installed, you will also see a figure like:

![heat_graph_sidekiq](https://github.com/user-attachments/assets/8743508a-be5b-4460-b0ca-86691e953eca)

What the output says? First, there were many Hosts Applicability tasks within the period of interest, per the first table. They were quite short since they dont occur in the next table - here you see `UpdateContentCounts` was far far the most busy type of work for `sidekiq`, followed by a Capsule sync.

The graph shows peak times of concurrently running dynflow steps over the time, and namely the average load of sidekiq workers over time. In my example, there were altogether 30 sidekiq threads, so the almost-constant sidekiq load of 30+ means that sidekiq workers were all busy all the time, and they can easily miss some polling or cause delays in processing further work or elsewere.

Also, `dynflow_steps.sidekiq_load.csv` is generated with csv data from the graph - valuable if you need to run some further analysis over the data from the graph.

**Warning: all times are in GMT!** Since the data in all inputs are in GMT.

## Were sidekiq workers polling external pulp tasks frequently enough?

When sidekiq workers are under a heavy load, they can miss a polling attempt or forget polling a pulp task completely. That delays task execution or even makes a task stuck. `check_dynflow_polling.py` is a simple script that warns about this.

The script takes sosreport directory as an argument, and fetches three basic logfiles for frequency of `"GET /pulp/api/v3/tasks/../` requests. It honors `foreman_tasks_polling_multiplier` setting stored by sosreport, if present - or this multiplier value can be set or modified via command line option. The script can check a single logfile instead of sosreport directory as well. Example output:

```
Processing file sosreport-my-satellite-2023-08-30-haifamh/var/log/httpd/foreman-ssl_access_ssl.log..
Task '49edb99b-f18f-4fb3-8401-f974e96bbf51' polled at '29/Aug/2023:04:07:53' and then at '29/Aug/2023:04:08:12', delay 19s is bigger than maximum 18s.
..
Task 'fd099e8d-7c90-49ad-b69a-405de3eb0f30' polled at '30/Aug/2023:04:13:53' and then at '30/Aug/2023:04:14:49', delay 56s is bigger than maximum 18s.

Processing file sosreport-my-satellite-2023-08-30-haifamh/var/log/messages..
..

```
