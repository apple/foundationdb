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

**Core Syntax Constructs**

- **One()**
  - Description: Select exactly one replica from the given server set.

- **Across(n, attribute, subpolicy)**
  - Parameters:
    - n: Number of distinct attribute values
    - attribute: Locality key (e.g., `zone_id`)
    - subpolicy: Applied within each group
  - Description: Select `n` groups by attribute, apply subpolicy in each.

- **PolicyAnd(p1, p2)**
  - Parameters:
    - p1: First policy
    - p2: Second policy
  - Description: Both policies must be satisfied. Only used for testing.

**Redundancy Modes → Internal Policy Mapping**

- **single**
  - Policy: `One()`
  - Description: Single replica, no fault tolerance.

- **double**, **fast_recovery_double**
  - Policy: `Across(2, zone_id, One())`
  - Description: 2 replicas in 2 zones; tolerates 1 zone failure.

- **triple**, **fast_recovery_triple**
  - Policy: `Across(3, zone_id, One())`
  - Description: 3 replicas in 3 zones; tolerates up to 2 zone failures.

- **three_datacenter**, **multi_dc**
  - Policy:
    - Storage: `Across(3, dcid, Across(2, zone_id, One()))`
    - TLogs: `Across(2, dcid, Across(2, zone_id, One()))`
  - Description: 
    - Storage: 3 DCs × 2 zones = 6 replicas
    - TLogs: 2 DCs × 2 zones

- **three_datacenter_fallback**
  - Policy: `Across(2, dcid, Across(2, zone_id, One()))`
  - Description: 4 replicas total across 2 DCs × 2 zones.

- **three_data_hall**
  - Policy:
    - Storage: `Across(3, data_hall, One())`
    - TLogs: `Across(2, data_hall, Across(2, zone_id, One()))`
  - Description: 
    - Storage: 1 replica in each of 3 halls
    - TLogs: 2 halls × 2 zones

- **three_data_hall_fallback**
  - Policy: Same as above with fewer halls
  - Description: 2 data halls for storage and logs, with zone diversity.

