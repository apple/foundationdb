###########
Engineering
###########

When we built FoundationDB, we didn't just want it to make something that rivaled the competition, we wanted to go above and beyond. Below are examples of the extra lengths we've taken to make an incredible product.

Flow
====

FoundationDB began with ambitious goals for both :doc:`high performance <performance>` per node and :doc:`scalability <scalability>`. We knew that to achieve these goals we would face serious engineering challenges that would require tool breakthroughs. We'd need efficient asynchronous communicating processes like in Erlang or the Async in .NET, but we'd also need the raw speed, I/O efficiency, and control of C++. To meet these challenges, we developed several new tools, the most important of which is :doc:`flow`, a new programming language that brings actor-based concurrency to C++11. Flow adds about 10 keywords to C++11 and is technically a trans-compiler: the Flow compiler reads Flow code and compiles it down to raw C++11, which is then compiled to a native binary with a traditional toolchain. One of Flow’s most important job is enabling Simulation.

Simulation
==========

We wanted FoundationDB to survive failures of machines, networks, disks, clocks, racks, data centers, file systems, etc., so we created a simulation framework closely tied to Flow. By replacing physical interfaces with shims, replacing the main epoll-based run loop with a time-based simulation, and running multiple logical processes as concurrent Flow Actors, Simulation is able to conduct a deterministic simulation of an entire FoundationDB cluster within a single-thread! Even better, we are able to execute this simulation in a deterministic way, enabling us to reproduce problems and add instrumentation ex post facto. This incredible capability enabled us to build FoundationDB exclusively in simulation for the first 18 months and ensure exceptional fault tolerance long before it sent its first real network packet. For a database with as strong a contract as the FoundationDB, testing is crucial, and over the years we have run the equivalent of *a trillion CPU-hours* of simulated stress testing. Read more about our :doc:`Simulation and Testing <testing>`.

Ratekeeper
==========

FoundationDB uses an intelligent control algorithm called Ratekeeper to queue client transactions during heavy loads. Using principles from operational research and control theory, FoundationDB prevents system oscillation and reduces internal queue sizes by intelligently applying global backpressure. By handing out tickets and serving clients in order and at a controlled pace, latency is shifted from the read and commit operations to the transaction-creation line. This ensures continuous low-latency operation under all conditions and also allows transactions of different priorities to be queued separately, allowing concurrent batch and low-latency workloads.

Prioritization
==============

Every task inside of Flow, especially disk access and network use, has its own priority. Carefully controlling the order at which tasks are completed is crucial to shaping performance at both light and heavy workloads. As operations enter the system, their priority generally increases over time to ensure that all operations are completed within an equitable timeframe.

Range clears
============

Other ordered key-value stores frequently provide range clears that perform on the order of the number of keys cleared, much like range reads do. To provide FoundationDB with the most predictability and performance, we worked hard to make range clears ultra-efficient, taking only O(log N) time which is for all practical purposes instant even for very large N. One in-house test clears an entire 10TB+ database with a single range clear. This gives the least-surprising behavior for a clear operation, even when your dataset has grown very large.

Range reads
===========

Ordered key-value stores often provide range reads that perform on the order of the number of keys returned plus the number of keys recently cleared. The reason for this weakness is that it’s easier to use an asymptotically inefficient data structure that stores a large set of (data, version) pairs instead of a true multi-version representation. In FoundationDB, we use a persistent tree-type data structure to eliminate “tombstones” and similar hacks. This design allows range reads to truly be O(N) where N is the number of elements returned. For example, iterating over a recently cleared large range is efficient because the traversal skips the cleared range in a single step.

API versioning
==============

Since the very beginning, FoundationDB has completely encapsulated multiple versions of its interface by requiring an explicit call to the “api_version” function before invoking any APIs. The goal of this design is to allow the server, client libraries, or bindings to be upgraded without having to modify client code at all. The client libraries, building on our C bindings, support all previous versions of all APIs.