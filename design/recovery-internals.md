# FDB Recovery Internals

FDB uses recovery to handle various failures, such as hardware and network failures. When the current transaction system no longer works properly due to failures, recovery is automatically triggered to create a new generation of the transaction system. 

This document explains at the high level how the recovery works in a single cluster. The audience of this document includes both FDB developers who want to have a basic understanding of the recovery process and database administrators who need to understand why a cluster fails to recover. This document does not discuss the complexity introduced to the recovery process by the multi-region configuration.

## Background

## `SeverDBInfo` data structure

This data structure contains transient information which is broadcast to all workers for a database, permitting them to communicate with each other. It contains, for example, the interfaces for cluster controller (CC), master, ratekeeper, and resolver, and holds the log system's configuration.  It also has a vector of all storage server's interfaces. The definition of the data structure is in `SeverDBInfo.h`. The data structure is not available to the client. 

Whenever a field of the `ServerDBInfo`is changed, the new value of the field, say new master's interface, will be sent to the CC and CC will propogate the new `ServerDBInfo` to all workers in the cluster.

## When will recovery happen?
Failures of roles in the transaction system and coordinators (?) can cause recovery. Transactoin system's roles include cluster controller, master, transaction logs (tLog), proxies, and resolvers.

[comment]: <> Can someone help define all situation that can trigger recovery?

Network partition or failures can make CC unable to reach some roles, treating those roles as dead and causing reocvery.

Not every type of failure can trigger recovery. For example, storage server (SS) failure will not cause recovery. Data distributor, which is a role that is independent from the transaction system, will recruit a new storage server or simply move the failed server's data to other servers.


## Overview

Cluster controller (CC) decides if recovery should be triggered. In case the current running CC crashes or cannot be reached by a majority of coordinators, coordinators will start leader election to select a CC among the stateless process -- the processes which do not have a file behind it, such as the processes that run master. In the rare situation when the majority of coordinators cannot be reached, say a majority of coordinators' machines crash, CC cannot be selected successfully and the recovery will get stuck.


Recovery has 9 phases, which are defined as the 9 states in the source code: READING_CSTATE = 1, LOCKING_CSTATE = 2, RECRUITING = 3, RECOVERY_TRANSACTION = 4, WRITING_CSTATE = 5, ACCEPTING_COMMITS = 6, ALL_LOGS_RECRUITED = 7, STORAGE_RECOVERED = 8, FULLY_RECOVERED = 9.

The recovery process is like a state machine, changing from one state to the next state. 
We will describe in the rest of this document what each phase does to drive the recovery to the next state.

Recovery tracks the information of each recovery phase in “MasterRecoveryState” trace event. By checking the message, we can find which phase the recovery is stuck at.


## Phase 1: READING_CSTATE

This phase reads the coordinated state (cstate) from coordinators. The cstate includes the DBCoreState structure which describes the transaction systems (such as transaction logs (tLog) and tLogs’ configuration, logRouterTags (the number of log router tags), txsTags, old generation's tLogs, and recovery count) that exist before the recovery. The coordinated state can have multiple generations of tLogs. 

The transaction system states before the recovery is the starting point for the current recovery to construct the configuration of the next-generation transaction system. Note FDB’s transaction system’s generation increases for each recovery.


## Phase 2: LOCKING_CSTATE

This phase locks the coordinated state (cstate) to make sure there is only one master who can change the cstate. Otherwise, we may end up with more than one master accepting commits after the recovery. To achieve that, the master needs to get currently alive tLogs’ interfaces and sends commands to tLogs to lock their states, preventing them from accepting any further writes. 


FDB lets the current alive tLogs to use the master’s interface to register each tLog’s interface to master. Master simply waits on receiving the TLogRejoinRequest streams: for each tLog’s interface received, the master compares the interface id with the tLog id read from cstate. Once the master collects enough old tLog interfaces, it will use the interfaces to lock those tLogs.
The logic of collecting tLogs’ interfaces is implemented in `trackRejoins()` function.
The logic of locking the tLogs is implemented in `epochEnd()` function in TagPartitionedLogSystems.actor.cpp.


Once we lock the cstate, we bump the recoveryCount and write the cstate to kill the other recovery attempts. This makes the other attempts know they are died and do not recruit more tLogs. If we do not do this, there will be many recovery attempts which recruit tLogs and can make system out of memory. This operation is the reason why every recovery bumps the recoveryCount (epoch) by 2 instead of 1.


*How does each tLog know the current master’s interface?*

Master interface is stored in `serverDBInfo`. Once the CC recruits the master, it updates the `serverDBInfo` with the master’s interface. CC will send the updated `serverDBInfo`, which has the master’s interface, to all processes. tLog processes (i,e., tLog workers) monitor the `serverDBInfo` in an actor. when the `serverDBInfo` changes, it will register itself to the new master. The logic for a tLog worker to monitor `serverDBInfo` change is implemented in `monitorServerDBInfo()` actor.


*How does each role, such as tLog and data distributor (DD), register its interface to master and CC?*

* tLog monitors `serverDBInfo` change and sends its interface to the new master; 

* Data distributor (DD) and ratekeeper rejoin themselves to CC because they are no longer a part of the recovery process;

* Storage server (SS) does not rejoin. It waits for the tLogs to be ready and commit their interfaces into database with a special transaction.


## Phase 3: RECRUITING

Once the master locks the cstate, it will recruit the still-alive tLogs from the previous generation for the benefit of faster recovery. The master gets the old tLogs’ interfaces from the READING_CSTATE phase and uses those interfaces to track which old tLog are still alive, the implementation of which is in `trackRejoins()`. 


Once the master gets enough tLogs, it calculates the knownCommittedVersion, which is the maximum durable version from the still-alive tLogs in the previous generation. The master will use the recruited tLogs to create a new TagPartitionedLogSystem for the new generation.


Two situations may invalidate the calculated knownCommittedVersion:
Situation 1: Too many tLogs in the previous generation permanently died, say due to hardware failure. The master can choose to force recovery, which can cause data loss. To avoid force recovery, database administrators can bring up those died tLogs, for example by copying their files onto new hardware. 


Situation 2: A tLog may die after it reports alive to the master in the RECRUITING phase. This may cause the knownCommittedVersion calculated by the master in this phase no longer valid in the next phases. When this happens, the master can detect it and will terminate the current recovery and start a new recovery. 


Then, the master will reconstruct the transaction state store (txnStateStore) by peeking the txnStateTag in oldLogSystem. 
Recall that the txnStateStore includes the transaction system’s configuration, such as the assignment of shards to SS and to tLogs and that the txnStateStore was durable on disk by the oldLogSystem.
Once we get the txnStateStore, we know the configuration of the txn system, such as the number of proxies. The master then can ask the CC to recruit roles for the new generation in the `recruitEverything()` function. Those recruited roles includes seed SS, proxies and tLogs. Once all roles are recruited, the master starts a new epoch in `newEpoch()`. 


At this point, we have recovered the txnStateStore and recruited new proxies and tLogs and copied data from old tLogs to new tLogs. We have a working transaction system in the new generation now.


**Where can the recovery get stuck in this phase?**


**Reading the txnStateStore step**. Recovery typically won’t get stuck at reading the txnStateStore step because once the master can lock tLogs, it should always be able to read the txnStateStore for the tLogs.


However, reading the txnStateStore can be slow because it needs to read from disk (through openDiskQueueAdapter() function) and the txnStateStore increases as the cluster size increases. Recovery can take a much longer time because of the slowness of reading the txnStateStore. To achieve faster recovery, we have improved the speed of reading the txnStateStore in FDB 6.2 by parallelly reading the txnStateStore on multiple tLogs based on tags. 


**Recruiting roles step**. There are cases the recovery can get stuck at recruiting enough roles for the txn system configuration. For example, a cluster with replica_factor equal to three may have only three tLog and one of them died during the recovery, the cluster will not succeed in recruiting 3 tLogs and will get stuck. Another example is when a new database is created and the cluster does not have a valid txnStateStore. To get out of the stuck situation, the master will use an emergency transaction to forcibly change the configuration such that the recruitment can succeed. This configuration change may temporarily violate the contract of the desired configuration but it is only temporary. 


We can use the trace event “MasterRecoveredConfig”, which dumps the information of the new transaction system’s configuration, to diagnose why the recovery is blocked in this phase. 


## Phase 4: RECOVERY_TRANSACTION

Not every FDB role participates in the recovery phases 1-3. This phase tells the other roles about the recovery information and triggers the recovery of those roles when necessary. 


Storage servers (SSes) are not involved in the recovery phase 1 - 3. To notify SSes about the recovery, the master commits a recovery transaction, the first transaction in the new generation, which contains the txnStateStore information. Once storage servers receive the recovery transaction, it will compare its latest data version and the recovery version, and roll-back its data whose version is newer than the recovery version. Note that storage servers may have newer data than the recovery version because they pre-fetch mutations from tLogs before the mutations are durable to reduce the read-your-write latency. 


Proxies haven’t recovered the transaction system states and cannot accept transactions yet. The master recovers proxies’ states by sending the txnStateStore to proxies through proxies’ (`txnState `) interfaces in `sendIntialCommitToResolvers()` function. Once proxies have recovered their states, they can start processing transactions. The recovery transaction that was waiting on proxies will be processed.


The resolvers haven’t known the recovery version either. The master needs to send the lastEpochEnd version (i.e., last commit of the previous generation) to resolvers via resolvers’ (resolve) interface.


At the end of this phase, every role should be aware of the recovery and start recovering its states.


## Phase 5: WRITING_CSTATE

Coordinators stores the transaction systems’ information. The master needs to write the new tLogs into coordinators’ states to achieve consensus and fault tolerance. Only when the coordinators’ state is updated with the new transaction system’s configuration, will the cluster controller tell clients about the new transaction system (such as the new proxies).


Once the cstate is written, the master sets the cstateUpdated promise and move to the ACCEPTING_COMMITS phase.


The cstate update is done in `trackTlogRecovery()` actor.
The actor keeps running until the recovery finishes the FULLY_RECOVERED phase. 
The actor needs to update the cstates at the following phases:
ALL_LOGS_RECRUITED, STORAGE_RECOVERED, and FULLY_RECOVERED. 
For example, when the old tLogs are no longer needed, the master will write the coordinators’ state again.


Now the main steps in recovery have finished. The master keeps waiting for all tLogs to join the system and for all storage servers to roll back their prefetched data before claiming the system is fully recovered. 


## Phase 6: ACCEPTING_COMMITS

The transaction system starts to accept new transactions.


## Phase 7: ALL_LOGS_RECRUITED

The master marks the recovery phase to ALL_LOGS_RECRUITED when the number of new tLogs it receives is equal to the configured expected tLogs. This is done in the `trackTlogRecovery()` actor.


## Phase 8: STORAGE_RECOVERED

Storage servers need old tLogs in previous generations to recover storage servers’ state. For example, a storage server may be offline for a long time, lagging behind in pulling mutations assigned to it. We have to keep the old tLogs who have those mutations until no storage server needs them. 

When all tLogs are no longer needed and deleted, the master move to the STORAGE_RECOVERED phase. This is done by checking if oldTLogData is empty in the `trackTlogRecovery()` actor.


## Phase 9: FULLY_RECOVERED

When the master has all new tLogs and removed all old tLogs -- both STORAGE_RECOVERED and ALL_LOGS_RECRUITED have been satisfied -- the master will mark the recovery state as FULLY_RECOVERED. 