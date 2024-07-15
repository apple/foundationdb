# Dynamic Knobs

This document is largely adapted from original design documents by Markus
Pilman and Trevor Clinkenbeard.

## Background

FoundationDB parameters control the behavior of the database, including whether
certain features are available and the value of internal constants. Parameters
will be referred to as knobs for the remainder of this document. Currently,
these knobs are configured through arguments passed to `fdbserver` processes,
often controlled by `fdbmonitor`. This has a number of problems:

1. Updating knobs involves updating `foundationdb.conf` files on each host in a
   cluster. This has a lot of overhead and typically requires external tooling
   for large scale changes.
2. All knob changes require a process restart.
3. We can't easily track the history of knob changes.

## Overview

The dynamic knobs project creates a strictly serializable quorum-based
configuration database stored on the coordinators. Each `fdbserver` process
specifies a configuration path and applies knob overrides from the
configuration database for its specified classes.

### Caveats

The configuration database explicitly does not support the following:

1. A high load. The update rate, while not specified, should be relatively low.
2. A large amount of data. The database is meant to be relatively small (under
   one megabyte). Data is not sharded and every coordinator stores a complete
   copy.
3. Concurrent writes. At most one write can succeed at a time, and clients must
   retry their failed writes.

## Design

### Configuration Path

Each `fdbserver` process can now include a `--config_path` argument specifying
its configuration path. A configuration path is a hierarchical list of
configuration classes specifying which knob overrides the `fdbserver` process
should apply from the configuration database. For example:

```bash
$ fdbserver --config_path classA/classB/classC ...
```

Knob overrides follow descending priority:

1. Manually specified command line knobs.
2. Individual configuration class overrides.
  * Subdirectories override parent directories. For example, if the
    configuration path is `az-1/storage/gp3`, the `gp3` configuration takes
    priority over the `storage` configuration, which takes priority over the
    `az-1` configuration.
3. Global configuration knobs.
4. Default knob values.

#### Example

For example, imagine an `fdbserver` process run as follows:

```bash
$ fdbserver --datadir /mnt/fdb/storage/4500 --logdir /var/log/foundationdb --public_address auto:4500 --config_path az-1/storage/gp3 --knob_disable_asserts false
```

And the configuration database contains:

| ConfigClass | KnobName            | KnobValue |
|-------------|---------------------|-----------|
| az-2        | page_cache_4k       | 8e9       |
| storage     | min_trace_severity  | 20        |
| az-1        | compaction_interval | 280       |
| storage     | compaction_interval | 350       |
| az-1        | disable_asserts     | true      |
| \<global\>  | max_metric_size     | 5000      |
| gp3         | max_metric_size     | 1000      |

The final configuration for the process will be:

| KnobName            |  KnobValue  | Explanation |
|---------------------|-------------|-------------|
| page_cache_4k       | \<default\> | The configuration database knob override for `az-2` is ignored, so the compiled default is used |
| min_trace_severity  | 20          | Because the `storage` configuration class is part of the process’s configuration path, the corresponding knob override is applied from the configuration database |
| compaction_interval | 350         | The `storage` knob override takes precedence over the `az-1` knob override |
| disable_asserts     | false       | This knob is manually overridden, so all other overrides are ignored |
| max_metric_size     | 1000        | Knob overrides for specific configuration classes take precedence over global knob overrides, so the global override is ignored |

### Clients

Clients can write to the configuration database using transactions.
Configuration database transactions are differentiated from regular
transactions through specification of the `USE_CONFIG_DATABASE` database
option.

In configuration transactions, the client uses the tuple layer to interact with
the configuration database. Keys are tuples of size two, where the first item
is the configuration class being written, and the second item is the knob name.
The value should be specified as a string. It will be converted to the
appropriate type based on the declared type of the knob being set.

Below is a sample Python script to write to the configuration database.

```python
import fdb

fdb.api_version(730)

@fdb.transactional
def set_knob(tr, knob_name, knob_value, config_class, description):
        tr[b'\xff\xff/description'] = description
        tr[fdb.tuple.pack((config_class, knob_name,))] = knob_value

# This function performs two knob changes transactionally.
@fdb.transactional
def set_multiple_knobs(tr):
        tr[b'\xff\xff/description'] = b'description'
        tr[fdb.tuple.pack((None, b'min_trace_severity',))] = b'10'
        tr[fdb.tuple.pack((b'az-1', b'min_trace_severity',))] = b'20'

db = fdb.open()
db.options.set_use_config_database()

set_knob(db, b'min_trace_severity', b'10', None, b'description')
set_knob(db, b'min_trace_severity', b'20', 'az-1', b'description')
```

### CLI Usage

Users may also utilize `fdbcli` to set and update knobs dynamically. Usage is as follows
```
setknob <knob_name> <knob_value> [config_class]
getknob <knob_name> [config_class]
clearknob <knob_name> [config_class]
```
Where `knob_name` is an existing knob, `knob_value` is the desired value to set the knob and `config_class` is the optional configuration class. Furthermore, `setknob` may be combined within a `begin\commit` to update multiple knobs atomically. If using this option, a description must follow `commit` otherwise a prompt will be shown asking for a description. The description must be non-empty. An example follows.
```
begin
setknob min_trace_severity 30
setknob tracing_udp_listener_addr 192.168.0.1
commit "fdbcli change"
```
Users may only combine knob configuration changes with other knob configuration changes in the same transaction. For example, the following is not permitted and will raise an error.
```
begin
set foo bar
setknob max_metric_size 1000
commit "change"
```
Specifically, `set, clear, get, getrange, clearrange` cannot be combined in any transaction with a `setknob` or `getknob`.

If using an individual `setknob` or `clearknob` without being inside a `begin\commit` block, then `fdbcli` will prompt for a description as well.

#### Type checking
Knobs have implicit types attached to them when defined. For example, the knob `tracing_udp_listener_addr` is set to `"127.0.0.1"` as so the type is string. If a user invokes `setknob` on this knob with an incorrect value that is not a string, the transaction will fail. 


### Disable the Configuration Database

The configuration database includes both client and server changes and is
enabled by default. Thus, to disable the configuration database, changes must
be made to both.

#### Server

The configuration database can be disabled by specifying the ``fdbserver``
command line option ``--no-config-db``. Note that this option must be specified
for *every* ``fdbserver`` process.

#### Client

The only client change from the configuration database is as part of the change
coordinators command. The change coordinators command is not considered
successful until the configuration database is readable on the new
coordinators. If the configuration database has been disabled server-side via
the ``--no-config-db`` command line option, the coordinators will continue to
serve the configuration interface, but will reply to each request with an empty
response. Client-side changes are no longer necessary when disabling the
configuration database.

Optionally, the client liveness check of the configuration database can be
prevented by specifying the ``--no-config-db`` flag when changing the
coordinators. For example:

```
fdbcli> coordinators auto --no-config-db
```

## Status

The current state of the configuration database is output as part of `status
json`. The configuration path for each process can be determined from the
``command_line`` key associated with each process.

Sample from ``status json``:

```
"configuration_database" : {
    "commits" : [
        {
            "description" : "set some knobs",
            "timestamp" : 1659570000,
            "version" : 1
        },
        {
            "description" : "make some other changes",
            "timestamp" : 1659570000,
            "version" : 2
        }
    ],
    "last_compacted_version" : 0,
    "most_recent_version" : 2,
    "mutations" : [
        {
            "config_class" : "<global>",
            "knob_name" : "min_trace_severity",
            "knob_value" : "int:5",
            "type" : "set",
            "version" : 1
        },
        {
            "config_class" : "<global>",
            "knob_name" : "compaction_interval",
            "knob_value" : "double:30.000000",
            "type" : "set",
            "version" : 1
        },
        {
            "config_class" : "az-1",
            "knob_name" : "compaction_interval",
            "knob_value" : "double:60.000000",
            "type" : "set",
            "version" : 1
        },
        {
            "config_class" : "<global>",
            "knob_name" : "compaction_interval",
            "type" : "clear",
            "version" : 2
        },
        {
            "config_class" : "<global>",
            "knob_name" : "update_node_timeout",
            "knob_value" : "double:4.000000",
            "type" : "set",
            "version" : 2
        }
    ],
    "snapshot" : {
        "<global>" : {
            "min_trace_severity" : "int:5",
            "update_node_timeout" : "double:4.000000"
        },
        "az-1" : {
            "compaction_interval" : "double:60.000000"
        }
    }
}
```

After compaction, ``status json`` would show:

```
"configuration_database" : {
    "commits" : [
    ],
    "last_compacted_version" : 2,
    "most_recent_version" : 2,
    "mutations" : [
    ],
    "snapshot" : {
        "<global>" : {
            "min_trace_severity" : "int:5",
            "update_node_timeout" : "double:4.000000"
        },
        "az-1" : {
            "compaction_interval" : "double:60.000000"
        }
    }
}
```

## Detailed Implementation

The configuration database is implemented as a replicated state machine living
on the coordinators. This allows configuration database transactions to
continue to function in the event of a catastrophic loss of the transaction
subsystem.

To commit a transaction, clients run the two phase Paxos protocol. First, the
client asks for a live version from a quorum of coordinators. When a
coordinator receives a request for its live version, it increments its local
live version by one and returns it to the client. Then, the client submits its
writes at the live version it received in the previous step. A coordinator will
accept the commit if it is still on the same live version. If a majority of
coordinators accept the commit, it is considered committed.

### Coordinator

Each coordinator runs a ``ConfigNode`` which serves as a replica storing one
full copy of the configuration database. Coordinators never communicate with
other coordinators while processing configuration database transactions.
Instead, the client runs the transaction and determines when it has quorum
agreement.

Coordinators serve the following ``ConfigTransactionInterface`` to allow
clients to read from and write to the configuration database.

#### ``ConfigTransactionInterface``
| Request          | Request fields                                                 | Reply fields                                                                                  | Explanation                                                                                                      |
|------------------|----------------------------------------------------------------|-----------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| GetGeneration    | (coordinatorsHash)                                             | (generation) or (coordinators_changed error)                                                  | Get a new read version. This read version is used for all future requests in the transaction                     |
| Get              | (configuration class, knob name, coordinatorsHash, generation) | (knob value or empty) or (coordinators_changed error) or (transaction_too_old error)          | Returns the current value stored at the specified configuration class and knob name, or empty if no value exists |
| GetConfigClasses | (coordinatorsHash, generation)                                 | (configuration classes) or (coordinators_changed error) or (transaction_too_old error)        | Returns a list of all configuration classes stored in the configuration database                                 |
| GetKnobs         | (configuration class, coordinatorsHash, generation)            | (knob names) or (coordinators_changed error) or (transaction_too_old error)                   | Returns a list of all knob names stored for the provided configuration class                                     |
| Commit           | (mutation list, coordinatorsHash, generation)                  | ack or (coordinators_changed error) or (commit_unknown_result error) or (not_committed error) | Commit mutations set by the transaction                                                                          |

Coordinators also serve the following ``ConfigFollowerInterface`` to provide
access to (and modification of) their current state. Most interaction through
this interface is done by the cluster controller through its
``IConfigConsumer`` implementation living on the ``ConfigBroadcaster``.

#### ``ConfigFollowerInterface``
| Request               | Request fields                                                       | Reply fields                                                                            | Explanation                                                                                                         |
|-----------------------|----------------------------------------------------------------------|-----------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| GetChanges            | (lastSeenVersion, mostRecentVersion)                                 | (mutation list, version) or (version_already_compacted error) or (process_behind error) | Request changes since the last seen version, receive a new most recent version, as well as recent mutations         |
| GetSnapshotAndChanges | (mostRecentVersion)                                                  | (snapshot, snapshotVersion, changes)                                                    | Request the full configuration database, in the form of a base snapshot and changes to apply on top of the snapshot |
| Compact               | (version)                                                            | ack                                                                                     | Compact mutations up to the provided version                                                                        |
| Rollforward           | (rollbackTo, lastKnownCommitted, target, changes, specialZeroQuorum) | ack or (version_already_compacted error) or (transaction_too_old error)                 | Rollback/rollforward mutations on a node to catch it up with the majority                                           |
| GetCommittedVersion   | ()                                                                   | (registered, lastCompacted, lastLive, lastCommitted)                                    | Request version information from a ``ConfigNode``                                                                   |
| Lock                  | (coordinatorsHash)                                                   | ack                                                                                     | Lock a ``ConfigNode`` to prevent it from serving requests during a coordinator change                               |

### Cluster Controller

The cluster controller runs a singleton ``ConfigBroadcaster`` which is
responsible for periodically polling the ``ConfigNode``s for updates, then
broadcasting these updates to workers through the ``ConfigBroadcastInterface``.
When workers join the cluster, they register themselves and their
``ConfigBroadcastInterface`` with the broadcaster. The broadcaster then pushes
new updates to registered workers.

The ``ConfigBroadcastInterface`` is also used by ``ConfigNode``s to register
with the ``ConfigBroadcaster``. ``ConfigNode``s need to register with the
broadcaster because the broadcaster decides when the ``ConfigNode`` may begin
serving requests, based on global information about status of other
``ConfigNode``s. For example, if a system with three ``ConfigNode``s suffers a
fault where one ``ConfigNode`` loses data, the faulty ``ConfigNode``  should
not be allowed to begin serving requests again until it has been rolled forward
and is up to date with the latest state of the configuration database.

#### ``ConfigBroadcastInterface``

| Request    | Request fields                                             | Reply fields                  | Explanation                                                                                 |
|------------|------------------------------------------------------------|-------------------------------|---------------------------------------------------------------------------------------------|
| Snapshot   | (snapshot, version, restartDelay)                          | ack                           | A snapshot of the configuration database sent by the broadcaster to workers                 |
| Changes    | (changes, mostRecentVersion, restartDelay)                 | ack                           | A list of changes up to and including mostRecentVersion, sent by the broadcaster to workers |
| Registered | ()                                                         | (registered, lastSeenVersion) | Sent by the broadcaster to new ``ConfigNode``s to determine their registration status       |
| Ready      | (snapshot, snapshotVersion, liveVersion, coordinatorsHash) | ack                           | Sent by the broadcaster to new ``ConfigNode``s to allow them to start serving requests      |

### Worker

Each worker runs a ``LocalConfiguration`` instance which receives and applies
knob updates from the ``ConfigBroadcaster``. The local configuration maintains
a durable ``KeyValueStoreMemory`` containing the following:

* The latest known configuration version
* The most recently used configuration path
* All knob overrides corresponding to the configuration path at the latest known version

Once a worker starts, it will:

* Apply manually set knobs
* Read its local configuration file
  * If the stored configuration path does not match the configuration path
    specified on the command line, delete the local configuration file
  * Otherwise, apply knob updates from the local configuration file. Manually
    specified knobs will not be overridden
  * Register with the broadcaster to receive new updates for its configuration
    classes
    * Persist these updates when received and restart if necessary

### Knob Atomicity

All knobs are classified as either atomic or non-atomic. Atomic knobs require a
process restart when changed, while non-atomic knobs do not.

### Compaction

``ConfigNode``s store individual mutations in order to be able to update other,
out of date ``ConfigNode``s without needing to send a full snapshot. Each
configuration database commit also contains additional metadata such as a
timestamp and a text description of the changes being made. To keep the size of
the configuration database manageable, a compaction process runs periodically
(defaulting to every five minutes) which compacts individual mutations into a
simplified snapshot of key-value pairs. Compaction is controlled by the
``ConfigBroadcaster``, using information it peridiodically requests from
``ConfigNode``s. Compaction will only compact up to the minimum known version
across *all* ``ConfigNode``s. This means that if one ``ConfigNode`` is
permanently partitioned from the ``ConfigBroadcaster`` or from clients, no
compaction will ever take place.

### Rollback / Rollforward

It is necessary to be able to roll ``ConfigNode``s backward and forward with
respect to their committed versions due to the nature of quorum logic and
unreliable networks.

Consider a case where a client commit gets persisted durably on one out of
three ``ConfigNode``s (assume commit messages to the other two nodes are lost).
Since the value is not committed on a majority of ``ConfigNode``s, it cannot be
considered committed. But it is also incorrect to have the value persist on one
out of three nodes as future commits are made. In this case, the most common
result is that the ``ConfigNode`` will be rolled back when the next commit from
a different client is made, and then rolled forward to contain the data from
the commit. ``PaxosConfigConsumer`` contains logic to recognize ``ConfigNode``
minorities and update them to match the quorum.

### Changing Coordinators

Since the configuration database lives on the coordinators and the
[coordinators can be
changed](https://apple.github.io/foundationdb/configuration.html#configuration-changing-coordination-servers),
it is necessary to copy the configuration database from the old to the new
coordinators during such an event. A coordinator change performs the following
steps in regards to the configuration database:

1. Write ``\xff/coordinatorsKey`` with the new coordinators string. The key
   ``\xff/previousCoordinators`` contains the current (old) set of
   coordinators.
2. Lock the old ``ConfigNode``s so they can no longer serve client requests.
3. Start a recovery, causing a new cluster controller (and therefore
   ``ConfigBroadcaster``) to be selected.
4. Read ``\xff/previousCoordinators`` on the ``ConfigBroadcaster`` and, if
   present, read an up-to-date snapshot of the configuration database on the
   old coordinators.
5. Determine if each registering ``ConfigNode`` needs an up-to-date snapshot of
   the configuration database sent to it, based on its reported version and the
   snapshot version of the database received from the old coordinators.
   * Some new coordinators which were also coordinators in the previous
     configuration may not need a snapshot.
6. Send ready requests to new ``ConfigNode``s, including an up-to-date snapshot
   if necessary. This allows the new coordinators to begin serving
   configuration database requests from clients.

## Testing

The ``ConfigDatabaseUnitTests`` class unit test a number of different
configuration database dimensions.

The ``ConfigIncrement`` workload tests contention between clients attempting to
write to the configuration database, paired with machine failure and
coordinator changes.
