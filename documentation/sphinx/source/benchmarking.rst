############
Benchmarking
############

The goal of this guide is to help you understand how we approach testing FoundationDB using different client concurrencies and cluster sizes.

Single-core Write Test #1
=========================

FoundationDB can go :doc:`really fast <performance>`, but first let's take a look at how slow we can make it...


Let's set up a minimal FoundationDB database, using just a **single CPU core**. We'll load **1 million rows** with **16-byte keys** and **random 8-100 byte** values and check out how it performs.

We'll start with writes::

    Loop for 1 minute:
        Start a transaction
        Write a random key to a new random value
        Commit

    Result: 917 writes/second

If you do this, you'll find that FoundationDB is *quite slow*.

Why? The biggest reason is that FoundationDB is *ultra-safe* by default. When FoundationDB commits a transaction, it blocks until the transaction is durably logged and fsync'ed to disk. (In this case we are using a **single SATA SSD**.) FoundationDB is optimized for efficient processing of large, sustained workloads at millisecond latencies, rather than these type of synthetic tests that would benefit from ultra-low write latencies.

Single-core Write Test #2
=========================

So, can it go faster? Well, Test #1 only runs the FoundationDB process at about 3% CPU on a single core, so we might be able to pull something off. Let's try another strategy::

    Loop for 1 minute:
        Start a transaction
        Write 10 random keys to new random values
        Commit

    Result: 8,650 writes/second

But FoundationDB CPU utilization is still only at 5%! Looks like we can do even more.

Single-core Write Test #3
=========================

Here's the final strategy for testing writes against FoundationDB running on a single core::

    Start 100 parallel clients, each doing:
        Loop for 1 minute:
            Start a transaction
            Write 10 random keys to new random values
            Commit

    Result: 46,000 writes/second

This is 50x faster than our first simple loop! We have achieved this without sacrificing any safety, and still only using a single core. This shows how efficiently FoundationDB handles real-world concurrent workloads—in this case 100 parallel clients. A lot of this can be attributed to the :doc:`Flow programming language <flow>` that we developed specifically for these use cases.

Single-core Read Test
=====================

Let's take what we learned and test read speed::

    Start 100 parallel clients, each doing:
        Loop for 1 minute:
            Start a transaction
            Read 10 random keys

    Result: 305,000 reads/second

This test delivers an impressive result while achieving a 0.6 ms average read latency. Again, this is only using one core and doing transactional reads from a fully durable database (albeit without any concurrent writes). Note that a commit will have no impact for a read-only transaction and so is not necessary here.

Single-core 90/10 Test
======================

Let's put it all together into a test with 90% reads and 10% writes::

    Start 100 parallel clients, each doing:
        Loop for 1 minute:
            Start a transaction
            Read 9 random keys
            Write 1 random key
            Commit

    Result: 107,000 operations/second

This is all done from the **single core database**. Since FoundationDB has a lock-free design, performance remains high even with 100 concurrent clients doing ACID transactions on the same keys.

Single-core Range Read Test
===========================

Let's check a final performance figure before cranking up the cluster size. FoundationDB is an ordered datastore so let's see how fast we can read concurrent ranges::

    Start 100 parallel clients, each doing:
        Loop for 1 minute:
            Start a transaction
            Perform a random 1000-key range read

    Result: 3,600,000 keys/second

FoundationDB supports efficient range reads. Ordering keys in your data model to maximize range reads is obviously an important optimization you can use!

Now it's time to make our FoundationDB cluster a little bigger.

12-machine Write Test
=====================

We're going to move to using a **modest 12-machine cluster**. Each machine is pretty basic, with a 4-core processor and a single SATA SSD. We'll put a FoundationDB server process on each core, yielding a 48-core cluster. (You could also build something like this with just a couple modern dual-socket machines.) Finally, let's increase the database size to **100,000,000 key-value pairs** and raise the stakes by throwing **3,200 parallel clients (!)** at the cluster. Oh, and let's make it fault-tolerant by enabling **2x replication**.

We'll start with random writes::

    Start 3,200 parallel clients, each doing:
        Loop for 1 minute:
            Start a transaction
            Write 10 random keys
            Commit

    Result: 720,000 writes/second

12-machine Read Test
====================

Now let's turn to reads::

    Start 3,200 parallel clients, each doing:
        Loop for 1 minute:
            Start a transaction
            Read 10 random keys

    Result: 5,540,000 reads/second

This is exceptional performance for a random-read workload from a transactional database and about half the speed on the same hardware of a dedicated caching layer like memcached. Note that performance is significantly less than the linear extrapolation.

12-machine 90/10 Test
=====================

Let's put both together into a test with 90% reads and 10% writes::

    Start 3,200 parallel clients, each doing:
        Loop for 1 minute:
            Start a transaction
            Read 9 random keys
            Write 1 random key
            Commit

    Result: 2,390,000 operations/second

Next steps
==========

So how should you go about benchmarking FoundationDB for your own system?
    
Begin with the peak throughput your system needs to handle. From here, use the data on our :doc:`performance page <performance>` as a starting point for your cluster configuration and workload design. From our numbers for per-core throughput, you can derive an initial estimate of the number of cores you'll need. Construct a workload that reflects your pattern of reads and writes, making sure to use a large enough number of operations per transaction and/or clients to achieve high concurrency.

To find bottlenecks there are several tools at your disposal:

* You can find several useful metrics in the trace files that FoundationDB emits
* Your standard Linux operating system tools (like perf) might come in handy
* FoundationDB also emits dtrace probes (:doc:`dtrace-probes`) that you can look at using perf or System Tap

.. toctree::
    :maxdepth: 1
    :titlesonly:
    :hidden:

    dtrace-probes
