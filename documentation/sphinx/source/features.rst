########
Features
########

FoundationDB has an ordered transactional API with useful properties and strong guarantees. Features in the core are deliberately kept to a minimum; data models and other abilities are exposed :doc:`via layers <architecture>`.

The Foundation
==============

Scalable
--------

FoundationDB adapts to efficiently support applications with diverse performance requirements. By using a shared-nothing distributed architecture, FoundationDB *scales out* by adding more machines to a cluster rather than just *scaling up* by increasing the capacity of a single machine. Best of all, the hard work of managing data redundancy, partitioning, caching, etc., is all handled automatically. Read more about our :doc:`scalability <scalability>`.

ACID transactions
-----------------

All reads and writes in FoundationDB are accomplished using transactions. These transactions are fully ACID (Atomic, Consistent, Isolated, and Durable) and span multiple machines with high performance. FoundationDB's isolation is the highest available; transactions appear to occur sequentially. FoundationDB's durability is the strongest — all transactions are redundantly stored to disk before they are considered committed.

Fault tolerance
---------------

A system designed to be distributed across many machines must be highly fault tolerant because the likelihood of hardware and network failures increases with the number of machines involved. FoundationDB has been designed and relentlessly tested to provide exceptionally high levels of fault tolerance. We've gone much further than designing for "no single point of failure". FoundationDB has also been designed and tested to guarantee that all ACID properties are preserved, even under catastrophic failures. Read more about our :doc:`fault tolerance <fault-tolerance>`.

Replicated Storage
------------------

FoundationDB stores each piece of data on multiple servers. If a server containing one of the copies is lost, FoundationDB will automatically heal, finding a new location for the lost copy. For read operations, clients communicate directly to the servers with the replicas, requesting a specific version to ensure a consistent view of the data.

Ordered Key-Value API
---------------------

Simple can be powerful. FoundationDB uses an ordered key-value data model (and richer data models are exposed via :doc:`layers <layer-concept>`). Each "row" within the database consists of a key that is used to reference the row and a value which stores data associated with the key. No specific format for the keys or values is required; they are simply binary data. Because keys are kept in lexicographical (sorted) order, ranges of key-value pairs can be read efficiently.

Watches
-------

Clients can create transactional watches on keys to ensure that they are notified if the value changes. After a watch is registered, FoundationDB efficiently pushes change notifications to clients without polling.

Atomic Operations
-----------------

FoundationDB includes support for specific "atomic operations" (e.g. Add) within a transaction to manipulate the value of a key without requiring the client to actually read the value. This makes these operations low-latency and enables a variety of advanced data structures to be implemented more efficiently as layers.

OLTP and OLAP
-------------

FoundationDB is optimized for online transaction processing (OLTP) workloads consisting of many small reads and writes. However, because it is an *ordered* key-value store, FoundationDB can use range reads to efficiently scan large swaths of data. Thus, FoundationDB can be effectively used for online analytical processing (OLAP) workloads as well.

Performance
===========

Low, predictable latencies
--------------------------

FoundationDB provides predictable throughput and low-latency random IO, even under workloads with unusual or erratically changing access patterns. Further, both FoundationDB's API and implementation have been designed to make it possible to understand the costs of the operations being executed. For example, clearing any range, even the entire database, is a fast operation.
Read more about :doc:`performance <performance>`.

Load balancing
--------------

FoundationDB achieves full utilization of a cluster under variable real-world workloads by using two major techniques. First, individual chunks of data are continuously moved from machine to machine to balance the load minute-to-minute. Second, on a faster time scale, individual requests can be redirected from a busy machine to a less-busy peer that also has a copy of the data. These techniques work together to optimize both throughput and latency.

Bursting
--------

By deferring background work for later, FoundationDB can provide higher burst write speeds, often up to triple the steady-state speed. This ability to efficiently "absorb" work can last several minutes. The capability to buffer bursts of work allows FoundationDB to be provisioned without worrying about instantaneous load peaks, and to keep latencies low even when pushing "above 100%" load.

Distributed Caching
-------------------

When your database scales out, you don't need a separate distributed caching layer: you already have one. FoundationDB uses the aggregate memory of the entire cluster to cache commonly accessed data. Unlike a caching tier such as memcached, FoundationDB's cache is completely synchronized to the database and provides all ACID guarantees.

Concurrency
===========

Non-blocking
------------

FoundationDB uses multiversion concurrency control to provide transactionally isolated reads without locking data or blocking writes. Optimistic concurrency control ensures that deadlocks are impossible and that slow or failing clients cannot interfere with the operation of the database.

Concurrent Connections
----------------------

FoundationDB is able to handle large numbers of concurrent client connections. Because it uses a threadless communications and concurrency model, FoundationDB does not have to create a thread per connection. This allows full performance even with hundreds of thousands of in-flight requests.

Interactive Transactions
------------------------

FoundationDB transactions are true interactive sessions, unlike distributed databases that require stored procedures. This means that client code can make an iterative series of reads and writes over the network to execute complex transactions.

Operations
==========

Elastic
-------

A FoundationDB database can start on a single machine and be expanded to a cluster as load and circumstances require. Adding a machine is as easy as running another FoundationDB process, even during database operation and without any extra administration. Data is continuously re-partitioned in the background; no manual data distribution or sharding is required.

Datacenter Failover
-------------------

FoundationDB can be configured to run multiple geographically diverse datacenters through our Multi DC mode. Each piece of data is replicated into three data centers, and clients can read data from their local data center at low latencies. In the event of a data center failure, the two remaining data centers will continue accepting writes, allowing for minimal downtime.

Self Tuning
-----------

FoundationDB has been designed so that many functions (such as data distribution, fault tolerance, incorporation of new nodes, performance tuning, etc.) are done automatically and require minimal management. Management tools allow configuration of parameters like replication policy, cluster topology, and data directories. A status monitoring tool lets you monitor cluster health and utilization of the cluster's physical resources.

Deploy Anywhere
---------------

Because it can scale linearly by adding new machines, FoundationDB is an ideal database for deployment in public or private cloud environments. For best performance in cloud environments with limited I/O, FoundationDB can be configured to use a durable in-memory storage engine instead of its default SSD-optimized storage engine.

Backup
------

An integrated backup system provides a true "moment-in-time" snapshot backup of the entire distributed database stored to a remote file system on a schedule. Although FoundationDB itself is fault tolerant, this capability is useful for recovering from disasters or unintentional modification of the database.
