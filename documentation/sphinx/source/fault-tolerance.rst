###############
Fault Tolerance
###############

What is fault tolerance?
========================

Many systems claim to be fault tolerant without really discussing the spectrum of failures they may face or their ability to maintain service in various cases. At one end of the spectrum, network packet collisions happen all the time with only minor impact; at the other end, if all the machines in a cluster lose power, the system will necessarily fail. The importance of fault tolerance lies in the middle range between these extremes. Fault tolerance is characterized by the *amount, duration, and likelihood* of data and service loss that may occur.

Distributed and replicated
==========================

FoundationDB is built on a distributed shared-nothing architecture. This design gives us a huge advantage over any system running on a single computer, which must fail when the machine fails. FoundationDB divides the data into chunks and distributes multiple copies of each chunk to different storage servers, each of which is a separate physical computer with its own local storage. The only connection between the computers is a switched Ethernet network.

FoundationDB can tolerate a single computer failure with service interruption of at most a few seconds and no data loss, although maximum system throughput is affected. Single machine failures are one of the most common types of failure, so tolerance to such failure is essential.

Data distribution strategy
==========================

If multiple machines fail, FoundationDB still deals gracefully with the possible service loss. Of course, data unavailability becomes a possibility because there are only a finite number of replicas of each chunk of data.

Any distributed system faces some basic probabilistic constraints. For example, take a system running on a 40-machine cluster. If each one of a million pieces of data is put on 4 random machines, and then 4 machines fail, unavailability of some data is almost certain. There are only about 100,000 possible combinations of 4 machines among 40, and with a million pieces of data, the failing combination of machines is almost certain to contain some of the million. (It will usually have about 10).

FoundationDB improves these probabilities by selecting "teams" of machines on which to distribute data. Instead of putting each chunk of data on a different set of machines, each machine can participate in multiple teams. In the above example, by selecting only 450 teams of 4 machines that each chunk of data can be on, the chance of data unavailability is reduced to about 0.5%.

The number of machines in each team is based on the replication mode; the total number of teams increases with the size of the cluster.

Independence assumptions
========================

As a further refinement, FoundationDB can be made aware that certain machines might tend to fail together by specifying the locality of each process. For example, every machine in a rack might share a network and power connection. If either failed, then the entire rack of machines would fail. We use this knowledge when choosing teams, taking care not to place any two machines in a team that would have a tendency to fail together. Pieces of data can then be intelligently distributed across racks or even datacenters, so that characteristic multimachine failures (for example, based on rack configuration) do not cause service interruption or data loss. Our ``three_data_hall`` and ``three_datacenter`` configurations use this technique to continuously operate through a failure of a data hall or datacenter respectively.

Other types of failure
======================

There are many different types of failures: drives filling up, network routing errors, machine performance degradation, "dead" machines coming back to life, OS faults, etc. FoundationDB has been built from the ground up on a framework that allows simulation of all these types of failures. We've run hundreds of millions of stress tests that fail machines at very short intervals, induce unusually severe loads, delay communications channels at the worst time, or all of the above at once.

We have worked hard to design FoundationDB to maximize fault tolerance, maintaining performance and availability in the face of worst-case scenarios. As a result, FoundationDB is a very safe system for managing your data.
