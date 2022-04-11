#########################
FoundationDB Architecture
#########################

Coordinators
============

All clients and servers connect to a FoundationDB cluster with a cluster file, which contains the IP:PORT of the coordinators. Both the clients and servers use the coordinators to connect with the cluster controller. The servers will attempt to become the cluster controller if one does not exist, and register with the cluster controller once one has been elected. Clients use the cluster controller to keep an up-to-date list of GRV proxies and commit proxies.

Cluster Controller
==================

The cluster controller is a singleton elected by a majority of coordinators. It is the entry point for all processes in the cluster. It is responsible for determining when a process has failed, telling processes which roles they should become, and passing system information between all of the processes.

Master
======

The master is responsible for coordinating the transition of the write sub-system from one generation to the next. The write sub-system includes the master, GRV proxies, commit proxies, resolvers, and transaction logs. The three roles are treated as a unit, and if any of them fail, we will recruit a replacement for all three roles. The master keeps commit proxies' committed version, provides read version for GRV proxies, provides the commit versions for batches of the mutations to the commit proxies, runs data distribution algorithm, and runs ratekeeper.

GRV Proxies and Commit Proxies
==============================

The GRV proxies are responsible for providing read versions. The commit proxies are responsible for committing transactions, and tracking the storage servers responsible for each range of keys. To provide a read version, a GRV proxy will ask master the largest committed version at this point in time, while simultaneously checking that the transaction logs have not been stopped. Ratekeeper will artificially slow down the rate at which the GRV proxy provides read versions.

Commits are accomplished by:

* Get a commit version from the master.
* Use the resolvers to determine if the transaction conflicts with previously committed transactions.
* Make the transaction durable on the transaction logs.

The key space starting with the '\xff' byte is reserved for system metadata. All mutations committed into this key space are distributed to all of the proxies through the resolvers. This metadata includes a mapping between key ranges and the storage servers which have the data for that range of keys. The proxies provide this information to clients on-demand. The clients cache this mapping; if they ask a storage server for a key it does not have, they will clear their cache and get a more up-to-date list of servers from the proxies.

Transaction Logs
================

The transaction logs make mutations durable to disk for fast commit latencies. The logs receive commits from the commit proxy in version order, and only respond to the commit proxy once the data has been written and fsync'ed to an append only mutation log on disk. Before the data is even written to disk we forward it to the storage servers responsible for that mutation. Once the storage servers have made the mutation durable, they pop it from the log. This generally happens roughly 6 seconds after the mutation was originally committed to the log. We only read from the log's disk when the process has been rebooted. If a storage server has failed, mutations bound for that storage server will build up on the logs. Once data distribution makes a different storage server responsible for all of the missing storage server's data we will discard the log data bound for the failed server.

Resolvers
=========

The resolvers are responsible determining conflicts between transactions. A read-write transaction conflicts if it reads a key that has been written between the transaction's read version and commit version. The resolver does this by holding the last 5 seconds of committed writes in memory, and comparing a new transaction's reads against this set of commits.

Storage Servers
===============

The vast majority of processes in a cluster are storage servers. Storage servers are assigned ranges of keys, and are responsible for storing all of the data for that range. They keep 5 seconds of mutations in memory, and an on disk copy of the data as of 5 second ago. Clients must read at a version within the last 5 seconds, or they will get a transaction_too_old error. The ssd storage engine stores the data in a b-tree. The memory storage engine stores the data in memory with an append only log that is only read from disk if the process is rebooted.

Clients
=======

Clients must get a read version at the start of every transaction. During the transaction all of the reads are done at that read version, and write are kept in memory until transaction is committed. When the transaction is committed, all of the reads and writes are sent to the commit proxy. If the transaction conflicts with another transaction the client is responsible for retrying the transaction. By default, reading a key that was written in the same transaction will return the newly written value.
