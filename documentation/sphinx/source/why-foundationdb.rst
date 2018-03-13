################
Why FoundationDB
################

NoSQL database design involves a number of fundamental technical alternatives. Among these alternatives, transactions yields significant advantages. FoundationDB is designed to perform transaction processing with high performance at scale.

* :doc:`transaction-manifesto`: Full ACID transactions are a powerful tool with significant advantages. To develop applications using modern distributed databases, transactions are simply essential. 

* The :doc:`cap-theorem` has been widely discussed in connection with NoSQL databases and is often invoked to justify weak models of data consistency. The truth is that the CAP theorem is widely misunderstood based on imprecise descriptions and actually constrains system design far less than is usually supposed.

* :doc:`consistency` is fundamental to the design of distributed databases. However, the term *consistency* is used with two different meanings in discussions of the ACID properties and the CAP theorem, and it's easy to confuse them.

* :doc:`scalability`: FoundationDB is horizontally scalable, achieving high performance per node in configurations from a single machine to a large cluster. We also offer elasticity, allowing machines to be provisioned or deprovisioned in a running cluster with automated load balancing and data distribution.

.. toctree::
   :maxdepth: 1
   :titlesonly:
   :hidden:

   transaction-manifesto
   cap-theorem
   consistency
   scalability
