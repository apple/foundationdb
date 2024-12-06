##################
Technical Overview
##################

These documents explain the engineering design of FoundationDB, with detailed information on its features, architecture, and performance.

* :doc:`architecture` provides a diagrammatic description of FoundationDB's logical architecture.

* :doc:`layer-concept`: Applications use layers to add capabilities to FoundationDB's minimal key-value API. A layer can provide a new data model, encapsulate an algorithm, or even be an entire framework.

* :doc:`transaction-processing`: FoundationDB supports transaction processing over a distributed cluster. We use optimistic concurrency control with a transactional authority running on a small number of transaction servers.

* :doc:`performance` describes the scaling, latency, throughput, and concurrency behavior of FoundationDB under various workloads and configurations.

* :doc:`benchmarking` describes how we approach testing FoundationDB using different client concurrencies and cluster sizes.

* :doc:`features` describes the fundamental properties of FoundationDB, as well as those that relate to performance, concurrency, and operations.

* :doc:`anti-features`: What a system *isn't* is sometimes as important as what it is. FoundationDB explicitly excludes some features either because they are at odds with the overall system design, or because they are better implemented as :doc:`layers <layer-concept>`.

* :doc:`experimental-features` describes experimental features of FoundationDB, available for testing.

* :doc:`engineering` summarizes the unique tools and capabilities behind FoundationDB.

* :doc:`fault-tolerance`: FoundationDB provides fault tolerance by intelligently replicating data across a distributed cluster of machines. Our architecture is designed to minimize service interruption and data loss in the event of machine failures.

* :doc:`flow`: FoundationDB faces rigorous engineering challenges for high performance and scalability. To meet these challenges, we implemented Flow, an extension to C++ that supports actor-based concurrency with new keywords and control-flow primitives while retaining speed and I/O efficiency.

* :doc:`testing`: FoundationDB uses a combined regime of robust simulation, live performance testing, and hardware-based failure testing to meet exacting standards of correctness and performance.

* :doc:`kv-architecture` provides a description of every major role a process in FoundationDB can fulfill.

* :doc:`read-write-path` describes how FDB read and write path works.

* :doc:`ha-write-path` describes how FDB write path works in HA setting.

* :doc:`consistency-check-urgent` describes how to complete a consistency scan of the entire database in a fast way.

* :doc:`bulkdump` describes how to do snapshot data dump to blobstore or local file system.

.. toctree::
   :maxdepth: 1
   :titlesonly:
   :hidden:

   architecture
   performance
   benchmarking
   engineering
   features
   layer-concept
   anti-features
   experimental-features
   transaction-processing
   fault-tolerance
   flow
   testing
   kv-architecture
   read-write-path
   ha-write-path
   consistency-check-urgent
   bulkdump
