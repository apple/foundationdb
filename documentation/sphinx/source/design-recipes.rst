##############
Design Recipes
##############

Learn how to build new data models, indexes, and more on top of the FoundationDB API. For more background, check out the :doc:`client-design` documentation.

* :doc:`Blob <blob>`: Store binary large objects (blobs) in the database.

* :doc:`Hierarchical Documents <hierarchical-documents>`: Create a representation for hierarchical documents.

* :doc:`Multimaps <multimaps>`: Create a multimap data structure with multiset values.

* :doc:`Priority Queues <priority-queues>`: Create a data structure for priority queues supporting operations for push, pop_min, peek_min, pop_max, and peek_max.

* :doc:`Queues <queues>`: Create a queue data structure that supports FIFO operations.

* :doc:`Segmented Range Reads <segmented-range-reads>`: Perform range reads in calibrated batches.

* :doc:`Simple Indexes <simple-indexes>`: Add (one or more) indexes to allow efficient retrieval of data in multiple ways.

* :doc:`Spatial Indexing <spatial-indexing>`: Create a spatial index for the database.

* :doc:`Subspace Indirection <subspace-indirection>`: Employ subspace indirection to manage bulk inserts or similar long-running operations.

* :doc:`Tables <tables>`: Create a table data structure suitable for sparse data.

* :doc:`Vector <vector>`: Create a vector data structure.

.. toctree::
     :maxdepth: 1
     :titlesonly:
     :hidden:

     blob
     blob-java
     hierarchical-documents
     hierarchical-documents-java
     multimaps
     multimaps-java
     priority-queues
     priority-queues-java
     queues
     queues-java
     segmented-range-reads
     segmented-range-reads-java
     simple-indexes
     simple-indexes-java
     spatial-indexing
     spatial-indexing-java
     subspace-indirection
     subspace-indirection-java
     tables
     tables-java
     vector
     vector-java

