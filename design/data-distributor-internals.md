# Data Distribution Internals

This document discusses how data distribution works in FDB.

Data distribution manages the lifetime of storage servers, decides which storage server is responsible for which data range, and ensures data is evenly distributed across all storage servers (SS). This document discusses the internals of data distribution (DD) from three perspectives: components that are the data structures of DD; operations that are the actors changing the states of DD; and mechanisms that realize functionalities of DD.

## Components

**Storage server (`class TCServerInfo`):** DD creates a TCServerInfo object for each storage server (SS). The TCServerInfo includes: (1) the SS’ locality, which includes the processID that is unique to ip:port, the zoneId that specifies which rack the SS is on, and the dcId that specifies which DC the SS is in; (2) the server’s teams, which will be discussed in the following paragraph; (3) the tracker that monitor the status of the server; and (4) extra information related to the server’s interface and preference. A server is healthy if its storage engine on the process is the same with the configured storage engine, and it is marked as desired by DD.

**Machine (`class TCMachineInfo`)**: A machine in FDB is considered as a rack, because a typical FDB cluster will only use one physical host from each rack in the datacenter to reduce the impact of regular rack-maintenance events on the cluster. All servers on the same rack belong to the same machine. A machine is healthy if there exists a healthy server on the machine.

**Server team (`class TCTeamInfo`)**: A server team is a group of *k* servers that host the same key ranges, where *k* is the replication factor that is usually three. A server team is healthy if every server in the team is healthy and those servers’ localities satisfy the replication requirement. Servers are grouped into server teams to reduce the possibility of data unavailability events at the event of *k* server failures.

**Machine team (`class TCMachineTeamInfo`)**: A machine team is a group of k machines, where k is the replication factor. Each server team must be on a machine team, meaning that each server in the server team is on a machine in the machine team and that no two servers are on the same machine. Similar to the purpose of server teams, machine teams are used to reduce the possibility of data unavailability events at the event of *k* machine failures. A machine team is healthy if every machine on the team is healthy and machines’ localities satisfy the replication policy.

**`TeamCollection`**: It has a global view of all servers and server teams, machines and machine teams. With the information, it creates server teams and machine teams. It also maintains the configuration settings for DD, which is used to create teams and decide which type of storage servers to recruit.

**Shard (`struct DDShardInfo`)**: A shard is a key range. A shard is maintained by a server team. A server team is responsible for many shards. Each shard has a similar amount of data. When a shard has too much data or has too much write traffic, it will be split into multiple shards and redistributed to server teams. Likewise, when a shard has too little data, it can be merged with its neighbors.

**RelocateShard (`struct RelocateShard`)**: A `RelocateShard` records the key range that need to be moved among servers and the data movement’s priority. DD always move shards with higher priorities first.

**Data distribution queue (`struct DDQueueData`)**: It receives shards to be relocated (i.e., RelocateShards), decides which shard should be moved to which server team, prioritizes the data movement based on relocate shard’s priority, and controls the progress of data movement based on servers’ workload.

**Special keys in the system keyspace**: DD saves its state in the system keyspace to recover from failure and to ensure every process (e.g., commit proxies, tLogs and storage servers) has a consistent view of which storage server is responsible for which key range.

*serverKeys* sub-space (`\xff/serverKeys/`): It records the start key of each shard a server is responsible for. The format is *\xff/serverKeys/[serverID]/[start_key]*. To get start keys of all shards for a server, DD can read the key range with prefix *\xff/serverKeys/[serverID]/* and decode the value of [start_key].

*keyServers* sub-space (`\xff/keyServers/`): It records each key’s source and destination server IDs. The format is *\xff/keyServers/[start_key]/[src_server][dst_server]*, where *[start_key]* is the start key of a shard, *[src_server]* are the servers responsible for the shard, *[dst_server]* are the new servers where the shard will be moved to when relocating shard request is initialized. To get all source and destination servers for the shard, DD can read the key range with the prefix `\xff/keyServers/[start_key]` and decode the value *[src_server][dst_server]*. To get each shard’s boundary, DD can read the key range with the prefix `\xff/keyServers/` and collect all *[start_key]*s. Two consecutive *[start_key]*, say *start_key1* and *start_key2*, construct the key range, say *[start_key1, start_key2)*, for a shard.

*`moveKeysLockOwnerKey`* (`\xff/moveKeysLock/Owner`) and *moveKeysLockWriteKey* (`\xff/moveKeysLock/Write`): When DD moves keys, it must grab the moveKeysLock, which consists of an owner key and a write key. The owner key (i.e., `moveKeysLockOwnerKey`) specifies which DD currently owns the lock. The write key (i.e., `moveKeysLockWriteKey`) specifies which DD is currently changing the mapping between keys and servers (i.e., operating on serverKeys and keyServers subspace). If DD finds it does not own both keys when it tries to move keys, it will kill itself by throwing an error. The cluster controller will recruit a new one.

When a new DD is initialized, it will set itself as the owner by setting its random UID to the `moveKeysLockOwnerKey`. Since the owner key has only one value, at most one DD can own the DD-related system subspace. This avoids the potential race condition between multiple DDs which may co-exist during DD recruitment.

**Transaction State Store (txnStateStore)**: It is a replica of the special keyspace that stores the cluster’s states, such as which SS is responsible for which shard. Because commit proxies use txnStateStore to decide which tLog and SS should receive a mutation, commit proxies must have a consistent view of txnStateStore. Therefore, changes to txnStateStore must be populated to all commit proxies in total order. To achieve that, we use the special transaction (`applyMetaMutations`) to update txnStateStore and use resolvers to ensure the total ordering (serializable snapshot isolation).

**Private mutation**: A private mutation is a mutation updating a special system key, such as keyServersKey (`\xff/keyServers/`) and serverKeysKey (`\xff/serverKeys/`). Like a normal mutation, a private mutation will be processed by the transaction systems (i.e., commit proxy, resolver and tLog) and be routed to a set of storage servers, based on the mutation’s tag, to update the key-value in the storage engine. Private mutations also keep the serializable snapshot isolation and consensus: The results of committed concurrent private mutations can be reproduced by sequentially executing the mutations, and all components in FDB have the same view of the mutations.


## Operations

Operations on the states (and data structure) of DD are done in actors. Each actor is responsible for only a specific task. We will describe the most important actors in this section.

**Storage server tracker (`storageServerTracker`)**: Whenever a storage server is created, a storage server tracker is created for the server. The tracker monitors the status (e.g., healthiness) of the server. When a server becomes unhealthy or the server’s process dies, the tracker issues the request to remove data on the server. Once all data are moved away from the server, the tracker remove the servers’ information from DD. When a server’s storage interface changes -- because the storage process reboots or moved -- the tracker updates the server’s information and changes the server’s teams accordingly to ensure the replication policy is always satisfied.

**Team tracker (`teamTracker`)**: Whenever a server team is created, a team tracker is created to monitor the healthiness of the team. When a healthy team becomes unhealthy, the team tracker will find all shards on the team, create the RelocateShard requests, and send the requests to the dataDistributionQueue.

**Team builder (`buildTeams`)**: Team builder is created when DD is initialized. It is invoked by the following events: (a) a new server is created and added to DD; (b) an existing server is removed from DD; (c) there is zero teams in the system.

Whenever the team builder is invoked, it aims to build the desired number of server teams. To ensure each server team belongs to a machine team, it first builds the desired number of machine teams; it then picks a machine team and picks a server from each machine in the machine team to form a server team.

**Data distribution queue server (`dataDistributionQueue` actor)**: It is created when DD is initialized. It behaves as a server to handle RelocateShard related requests. For example, it waits on the stream of RelocateShard. When a new RelocateShard is sent by teamTracker, it enqueues the new shard, and cancels the inflight shards that overlap with the new relocate shard.

**`applyMetaMutations`**: This is special logic to handle *private transactions* that modify txnStateStore and special system keys. Transaction systems (i.e., commit proxy, resolver and tLogs) and storage servers perform extra operations for the special transactions. For any update, it will be executed on all commit proxies in order so that all commit proxies have a consistent view of the txnStateStore. It will also send special keys to storage servers so that storage servers know the new keyspace they are now responsible for.

A storage server (SS) processes all requests sent to the server in its `storageServerCore` actor. When a (private) mutation request is sent to a SS, the server will call the `update()` function. Eventually, the `StorageUpdater` class will be invoked to apply the mutation in `applyMutation()` function, which handles private mutations `applyPrivateData()` function.

If a new key range is assigned to a storage server, the storage server will receive a private mutation that changes the *serverKeys* (`\xff/serverKeys/`) and *keyServers* (`\xff/keyServers/`). Then the server will create transactions, just like an FDB client, to read key-value pairs in the assigned key range and write the data into its local storage engine.

If a key range is removed from a storage server, similarly the storage server will receive a private mutation that changes the *serverKeys* and *keyServers*. Once the private mutation is processed by the SS, the SS removes data in its versioned data.


## Mechanisms

### How is data distribution initialized?

When a data distribution role is created, it recovers the states of the previous DD from the system keyspace. First, it sets itself as the owner of the `moveKeysLock`. Then it collects the information of servers and shards, the map between servers and shards, and the replication configuration by reading DD-related system keyspace (i.e., *serverKeys* sub-space). Based on the information, the new DD recreates its components (e.g., servers, teams, and trackers) that matches the states of the previous DD. Trackers will evaluate the healthiness of servers and teams based on the replication policy. Unhealthy servers and teams will be removed and new servers and teams will be created if the replication policy is changed.

### When to move keys?

Keys can be moved from a server to another for several reasons:
(1) DD moves keys from overutilized servers to underutilized servers, where a server’s utilization is defined as the server’s disk usage;
(2) DD splits or merges shards in order to rebalance the disk usage of servers;
(3) DD removes redundant teams when the team number is larger than the desired number;
(4) DD repairs the replication factor by duplicate shards from a server to another when servers in a team fail.

Actors are created to monitor the reasons of key movement:
(1) `MountainChopper` and `ValleyFiller` actors periodically measure a random server team’s utilization and rebalance the server’s keys among other servers;
(2) `shardMerger` and `shardSplitter` actors take a shard as input and respectively evaluates if the input shard can be merged with its neighboring shards without creating a too big shard and if the shard should be split. Once new shards are created, the actors create the shard’s tracker and send `RelocateShard` requests to DD’s queue;
(3) `serverTeamRemover` and `machineTeamRemover` actors periodically evaluate if the number of server teams and machine teams is larger than the desired number. If so, they respectively pick a server team or a machine team to remove based on predefined criteria;
(4) `teamTracker` actor monitors a team’s healthiness. When a server in the team becomes unhealthy, it issues the `RelocateShard` request to repair the replication factor. The less servers a team has, the higher priority the `RelocateShard` request will be.

### How to move keys?

A key range is a shard. A shard is the minimum unit of moving data. The storage server’s ownership of a shard -- which SS owns which shard -- is stored in the system keyspace *serverKeys* (`\xff/serverKeys/`) and *keyServers* (`\xff/keyServers/`). To simplify the explanation, we refer to the storage server’s ownership of a shard as a shard’s ownership.

A shard’s ownership is used in transaction systems (commit proxy and tLogs) to route mutations to tLogs and storage servers. When a commit proxy receives a mutation, it uses the shard’s ownership to decide which *k* tLogs receive the mutation, assuming *k* is the replias factor. When a storage server pulls mutations from tLogs, it uses the shard’s ownership to decide which shards the SS is responsible for and which tLog the SS should pull the data from.

A shard’s ownership must be consistent across transaction systems and SSes, so that mutations can be correctly routed to SSes. Moving keys from a SS to another requires changing the shard’s ownership under ACID property. The ACID property is achieved by using FDB transactions to change the *serverKeys *(`\xff/serverKeys/`) and *keyServers* (`\xff/keyServers/`). The mutation on the *serverKeys *and* keyServers *will be categorized as private mutations in transaction system. Compared to normal mutation, the private mutations will change the transaction state store (txnStateStore) that maintains the *serverKeys* and *keyServers* for transaction systems (commit proxy and tLog) when it arrives on each transaction component (e.g., tLog). Because mutations are processed in total order with the ACID guarantees, the change to the txnStateStore will be executed in total order on each node and the change on the shard’s ownership will also be consistent.

The data movement from one server (called source server) to another (called destination server) has four steps:
(1) DD adds the destination server as the shard’s new owner;
(2) The destination server will issue transactions to read the shard range and write the key-value pairs back. The key-value will be routed to the destination server and saved in the server’s storage engine;
(3) DD removes the source server from the shard’s ownership by modifying the system keyspace;
(4) DD removes the shard’s information owned by the source server from the server’s team information (i.e., *shardsAffectedByTeamFailure*).
