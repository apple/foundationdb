###########
Scalability
###########

Scalability is widely recognized as an essential property for successful applications. Scalability is actually one of *three* properties that closely interact to shape a system's performance profile:

* High performance: the ability to achieve the highest performance levels in a given configuration;
* Scalability: the ability to efficiently deliver service at very different scales;
* Elasticity: the ability to adapt up and down in scale quickly.

The interaction between high performance and scalability, in particular, is often not understood. An ant colony moving dirt is scalable but not highly performant. A single bulldozer moving dirt is highly performant but not scalable.

All three properties are important to your business. High performance means that you won't need to redesign your architecture every time your traffic doubles in size. Scalability means that your expenses start out very small and grow with your business.  Elasticity means that you can gracefully scale up and down on a continuous basis in response to demand.

FoundationDB is Highly Performant
=================================

FoundationDB was built to optimize a range of critical performance metrics. This approach is an important differentiator among distributed databases, many of which optimize for the simplicity of their own product development effort over the performance of their product. At every level of our system, we evaluate potential designs for their real-world efficiency. We build our own benchmarks of CPUs, memory controllers, disks, networks, and SSDs. We perform modeling and simulation and change designs to maximize performance, even at the expense of the simplicity of our development. When we consider high performance, we don't just look at the theoretical scalability of various algorithms; we target and achieve :doc:`real-world numbers <performance>`: millions of operations per second.

FoundationDB is Scalable
========================

FoundationDB offers scalability from partial utilization of a single core on a single machine to full utilization of dozens of powerful multicore machines in a cluster.

FoundationDB is Elastic
=======================

FoundationDB allows hardware to be provisioned and deprovisioned on-the-fly in response to changing needs without interruption or degradation of service. As data is written to the database, each piece of data is automatically placed on several independent computers. This replication allows immediate load balancing, and data is automatically moved from computer to computer to balance load over a longer time period. Based on request load and data size, FoundationDB seamlessly redistributes data across its distributed servers. FoundationDB is completely elastic, responding within milliseconds to hot spots and within minutes to major changes in usage.
