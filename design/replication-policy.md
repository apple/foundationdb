**FoundationDB Replication Policy System**

FoundationDB uses a composable **replication policy system** to determine how data replicas are distributed across physical infrastructure. This system enables 
the database to enforce constraints that ensure **fault tolerance**, **availability**, and **performance** while abstracting details from end users.

Shards, representing key ranges, are replicated across some number of storage servers (the shard's "team"). FDB maintains a "shard map" data structure to map 
shards to the team. More information on these terms can be found in [data-distributer-internals.md](http://data-distributer-internals.md)

The commit proxy uses the shard map to derive which log servers to send committed data to. Each SS has a "buddy" with a single corresponding log server. The 
buddy is found by taking the SS id modded by the number of tLogs in the dc.

When a batch of transactions is committed, the modified key(s) will logically reside within one or more shards, which physically are replicated over the SS in their team. Each such SS has a buddy
log server, and in the simple case, the CP forwards writes to those logs.

For example, suppose there were 4 log servers and 10 storage servers on a cluster configured with 3 replicas, and a shard's team resides on SS 0, 3, 6. Given 
there are 4 log servers total, the destination log servers would be {0%0, 3%4, 6%4}, or {0,3,2}.

Storage servers: **0** 1 2 **3** 4 5 **6** 7 8 9

TLogs: **0** 1 **2** **3**

The "buddy" associations between shards and log servers are dynamic:

- The number of SS in the cluster can change within an epoch.
- Shards move between SS as data is distributed.
- New shards can be created.

This means the set of log servers must be recalculated on each commit.

The team of log servers must meet the requirements of the replication policy. If it does not, additional logs are chosen. This is performed by "selectReplicas".

The selectReplicas function accepts a list of available servers and already chosen "also" servers (if any). It will check if the already chosen servers meet the 
replication policy. If they do not, the function will select additional servers at random out of the available servers in order to meet the policy. Because the 
shard map has replicas for all key ranges,the set of "also" servers passed to selectReplica usually already satisfies the policy, but there are exceptions.

- There are code paths where the shard map is not consulted, for example, when a tag is added to a transaction as a consequence of a metadata mutation.
- Two SS may have the same buddy, in which case a random server must be chosen.

Though users interact with only a few predefined redundancy modes (double, triple, etc.), FoundationDB internally supports a **rich set of compositional 
replication rules**. These are defined using basic building blocks that can be **composed recursively**. They are implemented in ReplicationPolicy.cpp as 
subclasses of IReplicationPolicy.

** Core Syntax Constructs**

| Syntax                  | Parameter 1                          | Parameter 2                            | Parameter 3                        | Description                                                                              |
| ----------------------- | ------------------------------------ | -------------------------------------- | ---------------------------------- | ---------------------------------------------------------------------------------------- |
| One()                   | —                                    | —                                      | —                                  | Select exactly one replica from the given server set                                    |
| Across(n, attribute, subpolicy) | n: number of distinct attribute values | attribute: locality key (e.g., zone\_id) | subpolicy: applied within each group | Select n groups partitioned by attribute, then apply subpolicy in each                     |
| PolicyAnd(p1, p2)       | p1: first policy                     | p2: second policy                      | —                                  | Both policies must be satisfied; results merged. This appears to be only used for testing. |

** Redundancy Modes and Policy Mapping**

User-facing redundancy modes map internally to replication policies as follows. Note the simulator tests a larger set of additional replication policies that are not exposed to the user.

| Mode                          | Internal Policy Expression                                                                              | Description                                                                           |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| single                        | PolicyOne()                                                                                                 | Single replica; no fault tolerance                                                     |
| double, fast\_recovery\_double | Across(2, zone\_id, One())                                                                                  | 2 replicas in 2 different zones; tolerates 1 zone failure                              |
| triple, fast\_recovery\_triple | Across(3, zone\_id, One())                                                                                  | 3 replicas in 3 different zones; tolerates 1–2 zone failures depending on config        |
| three\_datacenter, multi\_dc   | Storage: Across(3, dcid, Across(2, zone\_id, One())) TLog: Across(2, dcid, Across(2, zone\_id, One()))   | 6 replicas total:Storage: 3 DCs × 2 zonesTLogs: 2 DCs × 2 zones                        |
| three\_datacenter\_fallback    | Across(2, dcid, Across(2, zone\_id, One()))                                                                 | 4 replicas total in 2 DCs with 2 zones each; fallback configuration for multi-DC setups |
| three\_data\_hall              | Storage: Across(3, data\_hall, One()) TLog: Across(2, data\_hall, Across(2, zone\_id, One()))            | Storage: 1 replica in each of 3 data hallsTLogs: 2 data halls × 2 zones                |
| three\_data\_hall\_fallback   | Storage: Across(2, data\_hall, One()) TLog: Across(2, data\_hall, Across(2, zone\_id, One()))            | Fallback: 2 data halls for storage and logs, with diversity across zones               |
```
