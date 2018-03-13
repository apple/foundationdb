######################
Transaction Processing
######################

We often get asked how FoundationDB can achieve high performance for transaction processing over a scalable, distributed cluster. Here is a brief overview.

The transactional authority
===========================

Because FoundationDB supports concurrent operations by clients, it uses a distributed set of nodes working together as a *transactional authority* to detect conflicting updates. Transactions execute using optimistic concurrency control, so they don't need to lock a key before updating it and unlock it afterward. Instead, when a transaction is submitted to the transactional authority, it will be rejected if there has been a conflicting transaction, requiring the client to retry the rejected transaction.

Transaction servers
===================

To maintain high performance, the transactional authority is implemented by a number of individual *transaction servers*, each of which manages a portion of the incoming stream of transactions. FoundationDB's design decomposes transaction processing into its individual functions and scales them independently. The separate functions are:

* batching incoming transactions;
* checking transaction conflicts;
* logging transactions;
* durably storing the data.

Of these functions, many people intuitively focus on transaction conflict checking stage as a potential bottleneck. Fortunately, it turns out that this function *is* scalable. FoundationDB uses a sophisticated data-parallel and multithreaded algorithm to optimize conflict-checking so that it requires only a small percentage of the system's total work. This optimization allows a few transaction servers to keep up with a large cluster of storage servers.