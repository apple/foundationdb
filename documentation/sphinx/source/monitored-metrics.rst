**Monitored Metrics**
================================

**Database Availability**
-------------------------

*Database Availability Percentage*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - For our purposes, we’ve defined the database to be
unavailable if any operation (transaction start, read, or commit) cannot
be completed within 5 seconds. Currently, we only monitor transaction
start (get read version) and commit for this metric. We report the
percentage of each minute that the database is not unavailable according
to this definition.

**How to compute** - Because this is a metric where we value precision,
we compute it by running external processes that each start a new
transaction every 0.5 seconds at system immediate priority and then
committing them. Any time we have a delay exceeding 5 seconds, we
measure the duration of that downtime. We exclude the last 5 seconds of
this downtime, as operations performed during this period don’t satisfy
the definition of unavailability above.

**How we alert** - We do not alert on this metric.

*Database Available*
~~~~~~~~~~~~~~~~~~~~

**Explanation** - Reports as a point in time measurement once per minute
whether the database is available.

**How to compute** - This value begins reporting 0 anytime the process
described in ‘Database Availability Percentage’ detects an operation
taking longer than 5 seconds and resets to 1 whenever an operation
completes.

**How we alert** - We alert immediately whenever this value is 0.

*Max Unavailability Seconds*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - Reports the largest period of unavailability
overlapping a given minute. For example, a 3 minute unavailability that
started half way through a minute will report 30, 90, 150, 180 for the 4
minutes that overlap the unavailability period.

**How to compute** - The process described in ‘Database Availability
Percentage’ tracks and reports this data for each minute of the
unavailability period.

**How we alert** - We do not alert on this metric, though it could
possibly be combine with ‘Database Available’ to create an alert that
fires when unavailability reaches a minimum duration.

**Fault Tolerance**
-------------------

*Data Loss Margin*
~~~~~~~~~~~~~~~~~~

**Explanation** - Reports the number of fault tolerance domains (e.g.
separate Zone IDs) that can be safely lost without data loss. Fault
tolerance domains are typically assigned to correspond to something like
racks or machines, so for this metric you would be measuring something
like the number of racks that could be lost.

**How to compute** - From status:

cluster.fault_tolerance.max_machine_failures_without_losing_data

**How we alert** - We do not alert on this metric because the
Availability Loss Margin alert captures the same circumstances and more.

*Availability Loss Margin*
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - Reports the number of fault tolerance domains (e.g.
separate Zone IDs) that can be safely lost without indefinite
availability loss.

**How to compute** - From status:

cluster.fault_tolerance.max_machine_failures_without_losing_availability

**How we alert** - We have 3 different alerts on this metric, some based
on a relative measure with expected fault tolerance (e.g. 2 for triple,
1 for double):

1. Fault tolerance is 2 less than expected (only relevant with at least
   triple redundancy)

2. Fault tolerance is 1 less than expected for 3 hours (to allow for
   self-healing)

3. Fault tolerance decreases more than 4 times in 1 hour (may indicate
   flapping)

*Maintenance Mode*
~~~~~~~~~~~~~~~~~~

**Explanation** - Whether or not maintenance mode has been activated,
which treats a zone as failed but doesn’t invoke data movement for it.

**How to compute** - Maintenance mode is on if the following metric is
present in status:

cluster.maintenance_seconds_remaining

**How we alert** - We do not alert on this metric

**Process and Machine Count**
-----------------------------

*Process Count*
~~~~~~~~~~~~~~~

**Explanation** - The number of processes in the cluster, not counting
excluded processes.

**How to compute** - Count the number of entries in the
cluster.processes array where excluded is not true.

**How we alert** - We have 3 different alerts on this metric, some based
on a relative measure with the expected count:

1. The process count decreases 5 times in 60 minutes (may indicate
   flapping)

2. The process count is less that 70% of expected

3. The process count does not match expected (low severity notification)

*Excluded Process Count*
~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The number of processes in the cluster that are
excluded.

**How to compute** - Count the number of entries in the
cluster.processes array where excluded is true.

**How we alert** - We do not alert on this metric.

*Expected Process Count*
~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The expected number of non-excluded processes in the
cluster.

**How to compute** - We determine this number from how we’ve configured
the cluster.

**How we alert** - We do not alert on this metric.

*Machine Count*
~~~~~~~~~~~~~~~

**Explanation** - The number of machines in the cluster, not counting
excluded machines. This number may not be relevant depending on the
environment.

**How to compute** - Count the number of entries in the cluster.machines
array where excluded is not true.

**How we alert** - We have 3 different alerts on this metric, some based
on a relative measure with the expected count:

1. The machine count decreases 5 times in 60 minutes (may indicate
   flapping)

2. The machine count is less that 70% of expected

3. The machine count does not match expected (low severity notification)

*Excluded Machine Count*
~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The number of machines in the cluster that are
excluded.

**How to compute** - Count the number of entries in the cluster.machines
array where excluded is true.

**How we alert** - We do not alert on this metric.

*Expected Machine Count*
~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The expected number of non-excluded machines in the
cluster.

**How to compute** - We determine this number from how we’ve configured
the cluster.

**How we alert** - We do not alert on this metric.

**Latencies**
-------------

*GRV Probe Latency*
~~~~~~~~~~~~~~~~~~~

**Explanation** - The latency to get a read version as measured by the
cluster controller’s status latency probe.

**How to compute** - From status:

cluster.latency_probe.transaction_start_seconds

**How we alert** - We have multiple alerts at different severities
depending on the magnitude of the latency. The specific magnitudes
depend on the details of the cluster and the guarantees provided.
Usually, we require elevated latencies over multiple minutes (e.g. 2 out
of 3) to trigger an alert.

*Read Probe Latency*
~~~~~~~~~~~~~~~~~~~~

**Explanation** - The latency to read a key as measured by the cluster
controller’s status latency probe. Notably, this will only test a read
from a single storage server during any given probe and to only a single
team when measured over multiple probes. Data distribution could
sometimes change which team is responsible for the probed key.

**How to compute** - From status:

cluster.latency_probe.read_seconds

**How we alert** - We have multiple alerts at different severities
depending on the magnitude of the latency. The specific magnitudes
depend on the details of the cluster and the guarantees provided.
Usually, we require elevated latencies over multiple minutes (e.g. 2 out
of 3) to trigger an alert.

*Commit Probe Latency*
~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The latency to commit a transaction as measured by the
cluster controller’s status latency probe.

**How to compute** - From status:

cluster.latency_probe.commit_seconds

**How we alert** - We have multiple alerts at different severities
depending on the magnitude of the latency. The specific magnitudes
depend on the details of the cluster and the guarantees provided.
Usually, we require elevated latencies over multiple minutes (e.g. 2 out
of 3) to trigger an alert.

*Client GRV Latency*
~~~~~~~~~~~~~~~~~~~~

**Explanation** - A sampled distribution of get read version latencies
as measured on the clients.

**How to compute** - The use of this functionality is currently not well
documented.

**How we alert** - We do not alert on this metric.

*Client Read Latency*
~~~~~~~~~~~~~~~~~~~~~

**Explanation** - A sampled distribution of read latencies as measured
on the clients.

**How to compute** - The use of this functionality is currently not well
documented.

**How we alert** - We do not alert on this metric.

*Client Commit Latency*
~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - A sampled distribution of commit latencies as measured
on the clients.

**How to compute** - The use of this functionality is currently not well
documented.

**How we alert** - We do not alert on this metric.

**Workload**
------------

*Transaction Starts per Second*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The number of read versions issued per second.

**How to compute** - From status:

cluster.workload.transactions.started.hz

**How we alert** - We do not alert on this metric.

*Conflicts per Second*
~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The number of transaction conflicts per second.

**How to compute** - From status:

cluster.workload.transactions.conflicted.hz

**How we alert** - We do not alert on this metric.

*Commits per Second*
~~~~~~~~~~~~~~~~~~~~

**Explanation** - The number of transactions successfully committed per
second.

**How to compute** - From status:

cluster.workload.transactions.committed.hz

**How we alert** - We do not alert on this metric.

*Conflict Rate*
~~~~~~~~~~~~~~~

**Explanation** - The rate of conflicts relative to the total number of
committed and conflicted transactions.

**How to compute** - Derived from the conflicts and commits per second
metrics:

conflicts_per_second / (conflicts_per_second + commits_per_second)

**How we alert** - We do not alert on this metric.

*Reads per Second*
~~~~~~~~~~~~~~~~~~

**Explanation** - The total number of read operations issued per second
to storage servers.

**How to compute** - From status:

cluster.workload.operations.reads.hz

**How we alert** - We do not alert on this metric.

*Keys Read per Second*
~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The total number of keys read per second.

**How to compute** - From status:

cluster.workload.keys.read.hz

**How we alert** - We do not alert on this metric.

*Bytes Read per Second*
~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The total number of bytes read per second.

**How to compute** - From status:

cluster.workload.bytes.read.hz

**How we alert** - We do not alert on this metric.

*Writes per Second*
~~~~~~~~~~~~~~~~~~~

**Explanation** - The total number of mutations committed per second.

**How to compute** - From status:

cluster.workload.operations.writes.hz

**How we alert** - We do not alert on this metric.

*Bytes Written Per Second*
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The total number of mutation bytes committed per
second.

**How to compute** - From status:

cluster.workload.bytes.written.hz

**How we alert** - We do not alert on this metric.

**Recoveries**
--------------

*Cluster Generation*
~~~~~~~~~~~~~~~~~~~~

**Explanation** - The cluster generation increases when there is a
cluster recovery (i.e. the write subsystem gets restarted). For a
successful recovery, the generation usually increases by 2. If it only
increases by 1, that could indicate that a recovery is stalled. If it
increases by a lot, that might suggest that multiple recoveries are
taking place.

**How to compute** - From status:

cluster.generation

**How we alert** - We alert if the generation increases in 5 separate
minutes in a 60 minute window.

**Cluster Load**
----------------

*Ratekeeper Limit*
~~~~~~~~~~~~~~~~~~

**Explanation** - The number of transactions that the cluster is
allowing to start per second

**How to compute** - From status:

cluster.qos.transactions_per_second_limit

**How we alert** - We do not alert on this metric.

*Ratekeeper Batch Priority Limit*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The number of transactions that the cluster is
allowing to start per second above which batch priority transactions
will not be allowed to start.

**How to compute** - From status:

cluster.qos.batch_transactions_per_second_limit

**How we alert** - We do not alert on this metric.

*Ratekeeper Released Transactions*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The number of transactions that the cluster is
releasing per second. If this number is near or above the ratekeeper
limit, that would indicate that the cluster is saturated and you may see
an increase in the get read version latencies.

**How to compute** - From status:

cluster.qos.released_transactions_per_second

**How we alert** - We do not alert on this metric.

*Max Storage Queue*
~~~~~~~~~~~~~~~~~~~

**Explanation** - The largest write queue on a storage server, which
represents data being stored in memory that has not been persisted to
disk. With the default knobs, the target queue size is 1.0GB, and
ratekeeper will start trying to reduce the transaction rate when a
storage server’s queue size reaches 900MB. Depending on the replication
mode, the cluster allows all storage servers from one fault domain (i.e.
ZoneID) to exceed this limit without trying to adjust the transaction
rate in order to account for various failure scenarios. Storage servers
with a queue that reaches 1.5GB (the e-brake) will stop fetching
mutations from the transaction logs until they are able to flush some of
their data from memory. As of 6.1, batch priority transactions are
limited when the queue size reaches a smaller threshold (default target
queue size of 500MB).

**How to compute** - From status:

cluster.qos.worst_queue_bytes_storage_server

**How we alert** - We alert when the largest queue exceeds 500MB for 30
minutes in a 60 minute window.

*Limiting Storage Queue*
~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The largest write queue on a storage server that isn’t
being ignored for ratekeeper purposes (see max storage queue for
details). If this number is large, ratekeeper will start limiting the
transaction rate.

**How to compute** - From status:

cluster.qos.limiting_queue_bytes_storage_server

**How we alert** - We alert when the limiting queue exceeds 500MB for 10
consecutive minutes.

*Max Log Queue*
~~~~~~~~~~~~~~~

**Explanation** - The largest write queue on a transaction log, which
represents data that is being stored in memory on the transaction log
but has not yet been made durable on all applicable storage servers.
With the default knobs, the target queue size is 2.4GB, and ratekeeper
will start trying to reduce the transaction rate when a transaction
log’s queue size reaches 2.0GB. When the queue reaches 1.5GB, the
transaction log will start spilling mutations to a persistent structure
on disk, which allows the mutations to be flushed from memory and
reduces the queue size. During a storage server failure, you will see
the queue size grow to this spilling threshold and ideally hold steady
at that point. As of 6.1, batch priority transactions are limited when
the queue size reaches a smaller threshold (default target queue size of
1.0GB).

**How to compute** - From status:

cluster.qos.worst_queue_bytes_log_server

**How we alert** - We alert if the log queue is notably larger than the
spilling threshold (>1.6GB) for 3 consecutive minutes.

*Storage Read Queue*
~~~~~~~~~~~~~~~~~~~~

**Explanation** - The number of in flight read requests on a storage
server. We track the average and maximum of the queue size over all
storage processes in the cluster.

**How to compute** - From status (storage role only):

cluster.processes.<process_id>.roles[n].query_queue_max

**How we alert** - We do not alert on this metric.

*Storage and Log Input Rates*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The number of bytes being input to each storage server
or transaction log for writes as represented in memory. This includes
various overhead for the data structures required to store the data, and
the magnitude of this overhead is different on storage servers and logs.
This data lives in memory for at least 5 seconds, so if the rate is too
high it can result in large queues. We track the average and maximum
input rates over all storage processes in the cluster.

**How to compute** - From status (storage and log roles only):

cluster.processes.<process_id>.roles[n].input_bytes.hz

**How we alert** - We alert if the log input rate is larger than 80MB/s
for 20 out of 60 minutes, which can be an indication that we are using a
sizable fraction of our logs’ capacity.

*Storage Server Operations and Bytes Per Second*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - We track the number of mutations, mutation bytes,
reads, and read bytes per second on each storage server. We use this
primarily to track whether a single replica contains a hot shard
receiving an outsized number of reads or writes. To do so, we monitor
the maximum, average, and “2nd team” rate. Comparing the maximum and 2nd
team can sometimes indicate a hot shard.

**How to compute** - From status (storage roles only):

| cluster.processes.<process_id>.roles[n].mutations.hz
| cluster.processes.<process_id>.roles[n].mutation_bytes.hz
| cluster.processes.<process_id>.roles[n].finished_queries.hz
| cluster.processes.<process_id>.roles[n].bytes_queried.hz

To estimate the rate for the 2nd team (i.e the team that is the 2nd
busiest in the cluster), we ignore the top replication_factor storage
processes.

**How we alert** - We do not alert on these metrics.

*Transaction Log to Storage Server Lag*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - How far behind the latest mutations on the storage
servers are from those on the transaction logs, measured in seconds. In
addition to monitoring the average and maximum lag, we also measure what
we call the “worst replica lag”, which is an estimate of the worst lag
for a whole replica of data.

During recoveries of the write subsystem, this number can temporarily
increase because the database is advanced by many seconds worth of
versions.

When a missing storage server rejoins, if its data hasn’t been
re-replicated yet it will appear with a large lag that should steadily
decrease as it catches up.

A storage server that ratekeeper allows to exceed the target queue size
may eventually start lagging if it remains slow.

**How to compute** - From status (storage roles only):

cluster.processes.<process_id>.roles[n].data_lag.seconds

To compute the “worst replica lag”, we ignore the lag for all storage
servers in the first N-1 fault domains, where N is the minimum number of
replicas remaining across all data shards as reported by status at:

cluster.data.state.min_replicas_remaining

**How we alert** - We alert when the maximum lag exceeds 4 hours for a
duration of 2 minutes or if it exceeds 1000 seconds for a duration of 60
minutes. A more sophisticated alert may only alert if the lag is large
and not decreasing.

We also alert when the worst replica lag exceeds 15 seconds for 3
consecutive minutes.

*Storage Server Durability Lag*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - How far behind in seconds that the mutations on a
storage server’s disk are from the latest mutations in that storage
server’s memory. A large lag means can mean that the storage server
isn’t keeping up with the mutation rate, and the queue size can grow as
a result. We monitor the average and maximum durability lag for the
cluster.

**How to compute** - From status (storage roles only):

cluster.process.<process_id>.roles[n].durability_lag.seconds

**How we alert** - We do not alert on this metric.

**Other Cluster Metrics**
-------------------------

*Data Movement*
~~~~~~~~~~~~~~~

**Explanation** - How much data is actively being moved or queued to be
moved between shards in the cluster. There is often a small amount of
rebalancing movement happening to keep the cluster well distributed, but
certain failures and maintenance operations can cause a lot of movement.

**How to compute** - From status:

| cluster.data.moving_data.in_flight_bytes
| cluster.data.moving_data.in_queue_bytes

**How we alert** - We do not alert on this metric

*Coordinators*
~~~~~~~~~~~~~~

**Explanation** - The number of coordinators in the cluster, both as
configured and that are reachable from our monitoring agent.

**How to compute** - This list of coordinators can be found in status:

cluster.coordinators.coordinators

Each coordinator in the list also reports if it is reachable:

cluster.coordinators.coordinators.reachable

**How we alert** - We alert if there are any unreachable coordinators
for a duration of 3 hours or more.

*Clients*
~~~~~~~~~

**Explanation** - A count of connected clients and incompatible clients.
Currently, a large number of connected clients can be taxing for some
parts of the cluster. Having incompatible clients may indicate a
client-side misconfiguration somewhere.

**How to compute** - The connected client count can be obtained from
status directly:

cluster.clients.count 

To get the incompatible client count, we read the following list from
status and count the number of entries. Note that this is actually a
list of incompatible connections, which could theoretically include
incompatible server processes:

cluster.incompatible_connections

**How we alert** - We alert if the number of connected clients exceeds
1500 for 10 minutes. We also have a low priority alert if there are any
incompatible connections for a period longer than 3 hours.

**Resource Usage**
------------------

*CPU Usage*
~~~~~~~~~~~

**Explanation** - Percentage of available CPU resources being used. We
track the average and maximum values for each process (as a fraction of
1 core) and each machine (as a fraction of all logical cores). A useful
extension of this would be to track the average and/or max per cluster
role to highlight which parts of the cluster are heavily utilized.

**How to compute** - All of these metrics can be obtained from status.
For processes:

cluster.processes.<process_id>.cpu.usage_cores

For machines:

cluster.machines.<machine_id>.cpu.logical_core_utilization

To get the roles assigned to each process:

cluster.processes.<process_id>.roles[n].role

**How we alert** - We do not alert on this metric.

*Disk Activity*
~~~~~~~~~~~~~~~

**Explanation** - Various metrics for how the disks are being used. We
track averages and maximums for disk reads per second, disk writes per
second, and disk busyness percentage.

**How to compute** - All of these metrics can be obtained from status.
For reads:

cluster.processes.<process_id>.disk.reads.hz

For writes:

cluster.processes.<process_id>.disk.writes.hz

For busyness (as a fraction of 1):

cluster.processes.<process_id>.disk.busy

**How we alert** - We do not alert on this metric.

*Memory Usage*
~~~~~~~~~~~~~~

**Explanation** - How much memory is being used by each process and on
each machine. We track this in absolute numbers and as a percentage with
both averages and maximums.

**How to compute** - All of these metrics can be obtained from status.
For process absolute memory:

cluster.processes.<process_id>.memory.used_bytes

For process memory used percentage, divide used memory by available
memory:

cluster.processes.<process_id>.memory.available_bytes

For machine absolute memory:

cluster.machines.<machine_id>.memory.committed_bytes

For machine memory used percentage, divide used memory by free memory:

cluster.machines.<machine_id>.memory.free_bytes

**How we alert** - We do not alert on this metric.

*Network Activity*
~~~~~~~~~~~~~~~~~~

**Explanation** - Input and output network rates for processes and
machines in megabits per second (Mbps). We track averages and maximums
for each.

**How to compute** - All of these metrics can be obtained from status.
For process traffic:

| cluster.processes.<process_id>.network.megabits_received.hz
| cluster.processes.<process_id>.network.megabits_sent.hz

For machine traffic:

| cluster.machines.<machine_id>.network.megabits_received.hz
| cluster.machines.<machine_id>.network.megabits_sent.hz

**How we alert** - We do not alert on this metric.

*Network Connections*
~~~~~~~~~~~~~~~~~~~~~

**Explanation** - Statistics about open connection and connection
activity. For each process, we track the number of connections, the
number of connections opened per second, the number of connections
closed per second, and the number of connection errors per second.

**How to compute** - All of these metrics can be obtained from status:

| cluster.processes.<process_id>.network.current_connections
| cluster.processes.<process_id>.network.connections_established.hz
| cluster.processes.<process_id>.network.connections_closed.hz
| cluster.processes.<process_id>.network.connection_errors.hz

**How we alert** - We do not alert on this metric.

*Network Retransmits*
~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The number of TCP segments retransmitted per second
per machine.

**How to compute** - From status:

cluster.machines.<machine_id>.network.tcp_segments_retransmitted.hz

**How we alert** - We do not alert on this metric.

**Space Usage**
---------------

*Dataset Size*
~~~~~~~~~~~~~~

**Explanation** - The logical size of the database (i.e. the estimated
sum of key and value sizes) and the physical size of the database (bytes
used on disk). We also report an overhead factor, which is the physical
size divided by the logical size. Typically this is marginally larger
than the replication factor.

**How to compute** - From status:

| cluster.data.total_kv_size_bytes
| cluster.data.total_disk_used_bytes

**How we alert** - We do not alert on this metric.

*Process Space Usage*
~~~~~~~~~~~~~~~~~~~~~

**Explanation** - Various metrics relating to the space usage on each
process. We track the amount of space free on each process, reporting
minimums and averages for absolute bytes and as a percentage. We also
track the amount of space available to each process, which includes
space within data files that is reusable. For available space, we track
the minimum available to storage processes and the minimum available for
the transaction logs’ queues and kv-stores as percentages.

Running out of disk space can be a difficult situation to resolve, and
it’s important to be proactive about maintaining some buffer space.

**How to compute** - All of these metrics can be obtained from status.
For process free bytes:

cluster.processes.<process_id>.disk.free_bytes

For process free percentage, divide free bytes by total bytes:

cluster.processes.<process_id>.disk.total_bytes

For available percentage divide available bytes by total bytes. The
first is for kv-store data structures, present in storage and log roles:

| cluster.processes.<process_id>.roles[n].kvstore_available_bytes
| cluster.processes.<process_id>.roles[n].kvstore_total_bytes

The second is for the queue data structure, present only in log roles:

| cluster.processes.<process_id>.roles[n].queue_disk_available_bytes
| cluster.processes.<process_id>.roles[n].queue_disk_total_bytes

**How we alert** - We alert when free space on any process falls below
15%. We also alert with low severity when available space falls below
35% and with higher severity when it falls below 25%.

*Cluster Disk Space*
~~~~~~~~~~~~~~~~~~~~

**Explanation** - An accounting of the amount of space on all disks in
the cluster as well as how much of that space is free and available,
counted separately for storage and log processes. Available space has
the same meaning as described in the “Process Space Usage” section
above, as measured on each process’s kv-store.

**How to compute** - This needs to be aggregated from metrics in status.
For storage and log roles, the per-process values can be obtained from:

| cluster.processes.<process_id>.roles[n].kvstore_total_bytes
| cluster.processes.<process_id>.roles[n].kvstore_free_bytes
| cluster.processes.<process_id>.roles[n].kvstore_available_bytes

To compute totals for the cluster, these numbers would need to be summed
up across all processes in the cluster for each role. If you have
multiple processes sharing a single disk, then you can use the locality
API to tag each process with an identifier for its disk and then read
them back out with:

cluster.processes.<process_id>.locality.<identifier_name>

In this case, you would only count the total and free bytes once per
disk. For available bytes, you would add free bytes once per disk and
(available-free) for each process.

**How we alert** - We do not alert on this metric.

**Backup and DR**
-----------------

*Num Backup/DR Agents Running*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - A count of the number of backup and DR agents
currently connected to the cluster. For DR agents, we track the number
of DR agents where the cluster in question is the destination cluster,
but you could also count the number of agents using the cluster as a
source if needed.

**How to compute** - From status:

| cluster.layers.backup.instances_running
| cluster.layers.dr_backup.instances_running
| cluster.layers.dr_backup_dest.instances_running

**How we alert** - We have a low severity alert if this number differs
at all from the expected value. We have high severity alerts if the
number of running agents is less than half of what is expected or if the
count decreases 5 times in one hour.

*Num Backup/DR Agents Expected*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - The expected numbers of backup and DR agents in the
cluster.

**How to compute** - We determine this number from how we’ve configured
the cluster.

**How we alert** - We do not alert on this metric.

*Backup/DR Running*
~~~~~~~~~~~~~~~~~~~

**Explanation** - Tracks whether backup or DR is running on a cluster.
For our purposes, we only report DR is running on the primary cluster.

**How to compute** - From status:

| cluster.layers.backup.tags.default.running_backup
| cluster.layers.dr_backup.tags.default.running_backup

**How we alert** - We alert if backup is not running for 5 consecutive
minutes or DR is not running for 15 consecutive minutes. Because we only
run backup on primary clusters in a DR pair, we don’t have either of
these alerts on secondary clusters.

*Backup/DR Rate*
~~~~~~~~~~~~~~~~

**Explanation** - The rate at which backup and DR are processing data.
We report rates for both ranges (i.e. copying data at rest) and new
mutations.

**How to compute** - We can get the total number of bytes of each type
in status:

| cluster.layers.backup.tags.default.range_bytes_written
| cluster.layers.backup.tags.default.mutation_log_bytes_written
| cluster.layers.dr_backup.tags.default.range_bytes_written
| cluster.layers.dr_backup.tags.default.mutation_log_bytes_written

To compute a rate, it is necessary to query these values multiple times
and divide the number of bytes that each has increased by the time
elapsed between the queries.

**How we alert** - See Backup/DR Lag section, where we have an alert
that incorporates rate data.

*Backup/DR Lag*
~~~~~~~~~~~~~~~

**Explanation** - How many seconds behind the most recent mutations a
restorable backup or DR is. A backup or DR is restorable if it contains
a consistent snapshot of some version of the database. For a backup or
DR that is not running or restorable, we do not track lag.

**How to compute** - From status, you can get the lag from:

| cluster.layers.backup.tags.default.last_restorable_seconds_behind
| cluster.layers.dr_backup.tags.default.seconds_behind

This would then be combined with whether the backup or DR is running as
described above and whether it is restorable:

| cluster.layers.backup.tags.default.running_backup_is_restorable
| cluster.layers.dr_backup.tags.default.running_backup_is_restorable

**How we alert** - We have a low severity alert for a backup that is 30
minutes behind and a DR that is 5 minutes behind. We have high severity
alerts for a backup or DR that is 60 minutes behind.

We also have a high severity alert if a backup or DR is behind by at
least 5 minutes and the total backup/DR rate (combined range and
mutation bytes) is less than 1000 bytes/s. For backup, this alert occurs
after being in this state for 30 minutes, and for DR it is after 3
minutes.

*Backup Seconds Since Last Restorable*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - Measures how many seconds of data have not been backup
up and could not be restored.

**How to compute** - This uses the same source metric as in backup lag,
except that we also track it in cases where the backup is not running or
is not restorable:

cluster.layers.backup.tags.default.last_restorable_seconds_behind

**How we alert** - We do not alert on this metric.

*Datacenter Lag Seconds*
~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - When running a multi-DC cluster with async
replication, this tracks the lag in seconds between datacenters. It is
conceptually similar to DR lag when replication is done between 2
distinct clusters.

**How to compute** - This information can be obtained from status. The
metric used varies depending on the version. In 6.1 and older, use the
following metric and divide by 1,000,000:

cluster.datacenter_version_difference

In 6.2 and later, use:

cluster.datacenter_lag.seconds

**How we alert** - We have not yet defined alerts on this metric.

*Estimated Backup Size*
~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - This is not being tracked correctly.

*Process Uptime*
~~~~~~~~~~~~~~~~

**Explanation** - How long each process has been running.

**How to compute** - From status:

cluster.processes.<process_id>.uptime_seconds

**How we alert** - We do not alert on this metric.

*Cluster Health*
~~~~~~~~~~~~~~~~

**Explanation** - This is a complicated metric reported by status that
is used to indicate that something about the cluster is not in a desired
state. For example, a cluster will not be healthy if it is unavailable,
is missing replicas of some data, has any running processes with errors,
etc. If the metric indicates the cluster isn’t healthy, running status
in fdbcli can help determine what’s wrong.

**How to compute** - From status:

cluster.database_status.healthy

If the metric is missing, its value is presumed to be false.

**How we alert** - We do not alert on this metric.

*Layer Status*
~~~~~~~~~~~~~~

**Explanation** - Backup and DR report their statistics through a
mechanism called “layer status”. If this layer status is missing or
invalid, the state of backup and DR cannot be determined. This metric
can be used to track whether the layer status mechanism is working.

**How to compute** - From status:

cluster.layers._valid

If the metric is missing, its value is presumed to be false.

**How we alert** - We alert if the layer status is invalid for 10
minutes.

*Process Errors*
~~~~~~~~~~~~~~~~

**Explanation** - We track all errors logged by any process running in
the cluster (including the backup and DR agents).

**How to compute** - From process trace logs, look for events with
Severity=“40”

**How we alert** - We receive a daily summary of all errors.

*Process Notable Warnings*
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Explanation** - We track all notable warnings logged by any process
running in the cluster (including the backup and DR agents). Note that
there can be some noise in these events, so we heavily summarize the
results.

**How to compute** - From process trace logs, look for events with
Severity=“30”

**How we alert** - We receive a daily summary of all notable warnings.
