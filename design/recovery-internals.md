# FDB Recovery Internals

FDB uses recovery to handle various failures, such as hardware and network failures. When the current transaction system no longer works properly due to failures, recovery is automatically triggered to create a new generation of the transaction system.

This document explains at the high level how the recovery works in a single cluster. The audience of this document includes both FDB developers who want to have a basic understanding of the recovery process and database administrators who need to understand why a cluster fails to recover. This document does not discuss the complexity introduced to the recovery process by the multi-region configuration.

## Background

## `ServerDBInfo` data structure

This data structure contains transient information which is broadcast to all workers for a database, permitting them to communicate with each other. It contains, for example, the interfaces for cluster controller (CC), master, ratekeeper, and resolver, and holds the log system's configuration. Only part of the data structure, such as `ClientDBInfo` that contains the list of GRV proxies and commit proxies, is available to the client.

Whenever a field of the `ServerDBInfo`is changed, the new value of the field, say new master's interface, will be sent to the CC and CC will propagate the new `ServerDBInfo` to all workers in the cluster.

## When will recovery happen?
Failure of certain roles in FDB can cause recovery. Those roles are cluster controller, master, GRV proxy, commit proxy, transaction logs (tLog), resolvers, log router, and backup workers.

Network partition or failures can make CC unable to reach some roles, treating those roles as dead and causing recovery. If CC cannot connect to a majority of coordinators, it will be treated as dead by coordinators and recovery will happen.

Better master exists event can trigger recoveries. Better master exists event is the cluster changes such that there is a better location for some already recruited processes (say master role).

Configuration change, such as change of storage server type and excluding processes, can also trigger recovery.

Not every type of failure can trigger recovery. For example, storage server (SS) failure will not cause recovery. Data distributor, which is a role that is independent from the transaction system, will recruit a new storage server or simply move the failed server's data to other servers.

Failure of coordinators does not cause recovery. If more than a majority of coordinators fails, FDB will become unavailable. When the failed coordinators are replaced and rebooted, a recovery will happen.

## How to detect CC failure?

CC sends heart beat to all coordinators periodically. A CC will kill itself in the following conditions:

* The CC cannot  receive acknowledgement from a majority of coordinators due to network failure or death of coordinators; or

* A majority of coordinators reply that there exist another CC.

Once coordinators think there is no CC in a cluster, they will start leader election process to select a new CC.

## When will multiple CCs exist in a transient time period?

Although only one CC can succeed in recovery, which is guaranteed by Paxos algorithm, there exist scenarios when multiple CCs can exist in a  transient time period.

Scenario 1: A majority of coordinators reboot at the same time and the current running CC is still alive. When those coordinators reboot, they may likely choose a different process as CC. The new CC will start to recruit a new master and kicks off  `ClusterRecovery` actor that drives the recovery. The old CC will know the existence of the new CC when it sends heart-beat to coordinators periodically (in sub-seconds). The old CC will kill itself, once it was told by a majority of coordinators about the existence of the new CC. Old roles (say master) will commit suicide as well after the old CC dies. This prevents the cluster to have two sets of transaction systems. In summary, the cluster may have both the old CC and new CC alive in sub-seconds before the old CC confirms the existence of the new CC.

Scenario 2: Network partition makes the current running CC unable to connect to a majority of coordinators. Before the CC detects it, the coordinators can elect a new CC and recovery will happen. Typically, the old CC can quickly realize it cannot connect to a majority of coordinators and kill itself. In the rare situation when the old CC does not die within a short time period *and* the network partition is resolved before the old CC dies, the new CC can recruit a new master, which leads to two masters in the cluster. Only one master can succeed the recovery because only one master can lock the cstate (see Phase 2: LOCKING_CSTATE).

(The management of the CC's liveness is tricky to be implemented correctly. After four major revisions of the code, this functionality *should* be bug-free certified by Evan. ;))

## Overview

Cluster controller (CC) decides if recovery should be triggered. In case the current running CC crashes or cannot be reached by a majority of coordinators, coordinators will start leader election to select a CC. Stateless processes, which do not have a file behind it such as the processes that run master, are favored to run CC. In the rare situation when the majority of coordinators cannot be reached, say a majority of coordinators' machines crash, CC cannot be selected successfully and the recovery will get stuck.


Recovery has 9 phases, which are defined as the 9 states in the source code: READING_CSTATE = 1, LOCKING_CSTATE = 2, RECRUITING = 3, RECOVERY_TRANSACTION = 4, WRITING_CSTATE = 5, ACCEPTING_COMMITS = 6, ALL_LOGS_RECRUITED = 7, STORAGE_RECOVERED = 8, FULLY_RECOVERED = 9.

The recovery process is like a state machine, changing from one state to the next state. `ClusterRecovery.actor` implements the cluster recovery. We will describe in the rest of this document what each phase does to drive the recovery to the next state.

The system tracks the information of each recovery phase via trace events. In past recovery state machine was driven by the `Master` process, hence, the events used `Master` as event prefix name (for instance: `MasterRecoveryState`). Given the recovery state machine is currently driven by CC, updating name to `ClusterRecoveryState` would have been more appropriate, however, it breaks existing tooling scripts. For now, `ServerKnob::CLUSTER_RECOVERY_EVENT_NAME_PREFIX` determines the event name prefix, default prefix value is `Master`. Recovery tracks the information of each recovery phase in `<Prefix>RecoveryState` trace event. By checking the message, we can find which phase the recovery is stuck at. The status used in the `<Prefix>RecoveryState` trace event is defined as `RecoveryStatus` structure in `RecoveryState.h`. The status, instead of the name of the 9 phases, is typically used in diagnosing production issues.


## Phase 1: READING_CSTATE

This phase reads the coordinated state (cstate) from coordinators. The cstate includes the DBCoreState structure which describes the transaction systems (such as transaction logs (tLog) and tLogs’ configuration, logRouterTags (the number of log router tags), txsTags, old generations' tLogs, and recovery count) that exist before the recovery. The coordinated state can have multiple generations of tLogs.

The transaction system state before the recovery is the starting point for the current recovery to construct the configuration of the next-generation transaction system. Note FDB’s transaction system’s generation increases for each recovery.


## Phase 2: LOCKING_CSTATE

This phase locks the coordinated state (cstate) to make sure there is only one master who can change the cstate. Otherwise, we may end up with more than one master accepting commits after the recovery. To achieve that, the master needs to get currently alive tLogs’ interfaces and sends commands to tLogs to lock their states, preventing them from accepting any further writes.

Recall that `ServerDBInfo` has master's interface and is propagated by CC to every process in a cluster. The current running tLogs can use the master interface in its `ServerDBInfo` to send itself's interface to master.
Master simply waits on receiving the `TLogRejoinRequest` streams: for each tLog’s interface received, the master compares the interface ID with the tLog ID read from cstate. Once the master collects enough old tLog interfaces, it will use the interfaces to lock those tLogs.
The logic of collecting tLogs’ interfaces is implemented in `trackRejoins()` function.
The logic of locking the tLogs is implemented in `epochEnd()` function in [TagPartitionedLogSystems.actor.cpp](https://github.com/apple/foundationdb/blob/main/fdbserver/TagPartitionedLogSystem.actor.cpp).


Once we lock the cstate, we bump the `recoveryCount` by 1 and write the `recoveryCount` to the cstate. Each tLog in a recovery attempt records the `recoveryCount` and monitors the change of the variable. If the `recoveryCount` increases, becoming larger than the recorded value, the tLog will terminate itself. This mechanism makes sure that when multiple recovery attempts happen concurrently, only tLogs in the most recent recovery will be running. tLogs in other recovery attempts can release their memory earlier, reducing the memory pressure during recovery. This is an important memory optimization before shared tLogs, which allows tLogs in different generations to share the same memory, is introduced.


*How does each tLog know the current master’s interface?*

Master interface is stored in `serverDBInfo`. Once the CC recruits the master, it updates the `serverDBInfo` with the master’s interface. CC will send the updated `serverDBInfo`, which has the master’s interface, to all processes. tLog processes (i,e., tLog workers) monitor the `serverDBInfo` in an actor. when the `serverDBInfo` changes, it will register itself to the new master. The logic for a tLog worker to monitor `serverDBInfo` change is implemented in `monitorServerDBInfo()` actor.


*How does each role, such as tLog and data distributor (DD), register its interface to master and CC?*

* tLog monitors `serverDBInfo` change and sends its interface to the new master;

* Data distributor (DD) and Ratekeeper rejoin themselves to CC because they are no longer a part of the recovery process (they have been moved out of the master process since 6.2 release, before which they are part of the master process recovery in the FDB recovery procedure);

* Storage server (SS) does not rejoin. It waits for the tLogs to be ready and commit their interfaces into database with a special transaction.


## Phase 3: RECRUITING

Once the CC locks the cstate, it will recruit the still-alive tLogs from the previous generation for the benefit of faster recovery. The CC gets the old tLogs’ interfaces from the READING_CSTATE phase and uses those interfaces to track which old tLog are still alive, the implementation of which is in `trackRejoins()`.


Once the CC gets enough tLogs, it calculates the known committed version (i.e., `knownCommittedVersion` in code). `knownCommittedVersion` is the highest version that a commit proxy tells a given tLog that it had durably committed on *all* tLogs. The CC's is the maximum of all of that. `knownCommittedVersion` is  important, because it defines the lower bound of what version range of mutations need to be copied to the new generation. That is, any versions larger than the master's `knownCommittedVersion` is not guaranteed to persist on all replicas. The CC chooses a *recovery version*, which is the minimum of durable versions on all tLogs of the old generation, and recruits a new set of tLogs that copy all data between `knownCommittedVersion + 1` and `recoveryVersion` from old tLogs. This copy makes sure data within the range has enough replicas to satisfy the replication policy.

Later, the CC will use the recruited tLogs to create a new `TagPartitionedLogSystem` for the new generation.

**An example of `knownCommittedVersion` and `recoveryVersion`:**

Consider an old generation with three TLogs: `A, B, C`. Their durable versions are `100, 110, 120`, respectively, and their `knownCommittedVersion` are at `80, 90, 95`, respectively.

* If all of them are alive during recovery, CC will choose `max(80, 90, 95) = 95` as the last epoch's end version and `min(100, 110, 120)=100` as the recovery version. Versions between `96` and `100` will be copied to new generation's tLogs. Note some of them `101` to `120` are actually durable on one or two tLogs, but the CC chooses to discard them. If a storage server has peeked versions in the range of `[101, 120]`, these versions are in memory of the storage server and will be rolled back (i.e., discarded).

* Another scenario is `C` is down during recovery. In this case, the CC chooses `max(80, 90) = 90` as the last epoch's end version and `min(100, 110) = 100` as the recovery version. In this case, versions between `[91, 100]` will be copied to new generation's tLogs.

* If all `A, B, and C` are down. The operator can manually force recovery to any version, e.g., `98`. Then `99` to `120` are discarded, even though `99` to `100` are durable on the whole set.

**Two situations may invalidate the calculated knownCommittedVersion:**

* Situation 1: Too many tLogs in the previous generation permanently died, say due to hardware failure. If force recovery is allowed by system administrator, the CC can choose to force recovery, which can cause data loss; otherwise, to unblock the recovery, system administrator has to bring up those died tLogs, for example by copying their files onto new hardware.


* Situation 2: A tLog may die after it reports alive to the CC in the RECRUITING phase. This may cause the `knownCommittedVersion` calculated by the CC in this phase to no longer be valid in the next phases. When this happens, the CC will detect it, terminate the current recovery, and start a new recovery.


Once we have a `knownCommittedVersion`, the CC will reconstruct the [transaction state store](https://github.com/apple/foundationdb/blob/main/design/transaction-state-store.md) by peeking the txnStateTag in oldLogSystem.
Recall that the txnStateStore includes the transaction system’s configuration, such as the assignment of shards to SS and to tLogs and that the txnStateStore was durable on disk in the oldLogSystem.
Once we get the txnStateStore, we know the configuration of the transaction system, such as the number of GRV proxies and commit proxies. The CC recruits roles for the new generation in the `recruitEverything()` function. Those recruited roles includes GRV proxies, commit proxies, tLogs and seed SSes, which are the storage servers created for an empty database in the first generation to host the first shard and serve as the starting point of the bootstrap process to recruit more SSes. Once all roles are recruited, the CC starts a new epoch in `newEpoch()`.

At this point, we have recovered the txnStateStore, recruited new GRV proxies, commit proxies and tLogs, and copied data from old tLogs to new tLogs. We have a working transaction system in the new generation now.

### Where can the recovery get stuck in this phase?

Recovery can get stuck at the following two steps:

**Reading the txnStateStore step.**
Recovery typically won’t get stuck at reading the txnStateStore step because once the CC can lock tLogs, it should always be able to read the txnStateStore for the tLogs.


However, reading the txnStateStore can be slow because it needs to read from disk (through `openDiskQueueAdapter()` function) and the txnStateStore size increases as the cluster size increases. Recovery can take a long time if reading the txnStateStore is slow. To achieve faster recovery, we have improved the speed of reading the txnStateStore in FDB 6.2 by parallelly reading the txnStateStore on multiple tLogs based on tags.


**Recruiting roles step.**
There are cases where the recovery can get stuck at recruiting enough roles for the txn system configuration. For example, if a cluster with replica factor equal to three has only three tLogs and one of them dies during the recovery, the cluster will not succeed in recruiting 3 tLogs and the recovery will get stuck. Another example is when a new database is created and the cluster does not have a valid txnStateStore. To get out of this situation, the CC will use an emergency transaction to forcibly change the configuration such that the recruitment can succeed. This configuration change may temporarily violate the contract of the desired configuration, but it is only temporary.

ServerKnob::CLUSTER_RECOVERY_EVENT_NAME_PREFIX defines the prefix for cluster recovery trace events. Hereafter, referred as 'RecoveryEventPrefix' in this document.

We can use the trace event `<RecoveryEventPrefix>RecoveredConfig`, which dumps the information of the new transaction system’s configuration, to diagnose why the recovery is blocked in this phase.


## Phase 4: RECOVERY_TRANSACTION

Not every FDB role participates in the recovery phases 1-3. This phase tells the other roles about the recovery information and triggers the recovery of those roles when necessary.


Storage servers (SSes) are not involved in the recovery phase 1 - 3. To notify SSes about the recovery, the CC commits a recovery transaction, the first transaction in the new generation, which contains the txnStateStore information. Once storage servers receive the recovery transaction, it will compare its latest data version and the recovery version, and rollback to the recovery version if its data version is newer. Note that storage servers may have newer data than the recovery version because they pre-fetch mutations from tLogs before the mutations are durable to reduce the latency to read newly written data.


Commit proxies haven’t recovered the transaction system state and cannot accept transactions yet. The CC recovers proxies’ states by sending the txnStateStore to commit proxies through commit proxies’ (`txnState`) interfaces in `sendInitialCommitToResolvers()` function. Once commit proxies have recovered their states, they can start processing transactions. The recovery transaction that was waiting on commit proxies will be processed.


The resolvers haven’t known the recovery version either. The CC would update `recoveryTransactionVersion`, `lastEpochEnd` and elected `commitProxies` details to the master. The master needs to send the `lastEpochEnd` version (i.e., last commit of the previous generation) to resolvers via resolvers’ (`resolve`) interface.


At the end of this phase, every role should be aware of the recovery and start recovering their states.


## Phase 5: WRITING_CSTATE

Coordinators store the transaction systems’ information. The CC needs to write the new tLogs into coordinators’ states to achieve consensus and fault tolerance. Only when the coordinators’ states are updated with the new transaction system’s configuration will the cluster controller tell clients about the new transaction system (such as the new GRV proxies and commit proxies).

The CC only needs to write the new tLogs to a quorum of coordinators for a running cluster. The only time the CC has to write all coordinators is when creating a brand new database.


Once the cstate is written, the CC sets the `cstateUpdated` promise and moves to the ACCEPTING_COMMITS phase.


The cstate update is done in `trackTlogRecovery()` actor.
The actor keeps running until the recovery finishes the FULLY_RECOVERED phase.
The actor needs to update the cstates at the following phases:
ALL_LOGS_RECRUITED, STORAGE_RECOVERED, and FULLY_RECOVERED.
For example, when the old tLogs are no longer needed, the CC will write the coordinators’ state again.


Now the main steps in recovery have finished. The CC keeps waiting for all tLogs to join the system and for all storage servers to roll back their prefetched *uncommitted* data before claiming the system is fully recovered.


## Phase 6: ACCEPTING_COMMITS

The transaction system starts to accept new transactions. This doesn't mean that this committed data will be available for reading by clients, because storage servers are not guaranteed to be alive in the recovery process. In case storage servers have not been alive, write-only transactions can be committed and will be buffered in tLogs. If storage servers are unavailable for long enough, pushing tLogs' memory usage above a configurable threshold, ratekeeper will throttle all transactions.


## Phase 7: ALL_LOGS_RECRUITED

The CC sets the recovery phase to ALL_LOGS_RECRUITED when the number of new tLogs it receives is equal to the expected tLogs based on the cluster configuration. This is done in the `trackTlogRecovery()` actor.

The difference between this phase and getting to Phase 3 is that the CC is waiting for *older generations* of tLogs to be cleaned up at this phase.

## Phase 8: STORAGE_RECOVERED

Storage servers need old tLogs in previous generations to recover storage servers’ state. For example, a storage server may be offline for a long time, lagging behind in pulling mutations assigned to it. We have to keep the old tLogs who have those mutations until no storage server needs them.

When all tLogs are no longer needed and deleted, the CC moves to the STORAGE_RECOVERED phase. This is done by checking if oldTLogData is empty in the `trackTlogRecovery()` actor.


## Phase 9: FULLY_RECOVERED

When the CC has all new tLogs and has removed all old tLogs -- both STORAGE_RECOVERED and ALL_LOGS_RECRUITED have been satisfied -- the CC will mark the recovery state as FULLY_RECOVERED.
