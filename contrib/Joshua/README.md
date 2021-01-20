# Overview

This directory provides the files needed to create a Joshua correctness bundle for testing FoundationDB.

Rigorous testing is central to our engineering process. The features of our core are challenging, requiring us to meet exacting standards of correctness and performance. Data guarantees and transactional integrity must be maintained not only during normal operations but over a broad range of failure scenarios. At the same time, we aim to achieve performance goals such as low latencies and near-linear scalability. To meet these challenges, we use a combined regime of robust simulation, live performance testing, and hardware-based failure testing.

# Joshua

Joshua is a powerful tool for testing system correctness. Our simulation technology, called Joshua, is enabled by and tightly integrated with `flow`, our programming language for actor-based concurrency. In addition to generating efficient production code, Flow works with Joshua for simulated execution.

The major goal of Joshua is to make sure that we find and diagnose issues in simulation rather than the real world. Joshua runs tens of thousands of simulations every night, each one simulating large numbers of component failures. Based on the volume of tests that we run and the increased intensity of the failures in our scenarios, we estimate that we have run the equivalent of roughly one trillion CPU-hours of simulation on FoundationDB.

Joshua is able to conduct a *deterministic* simulation of an entire FoundationDB cluster within a single-threaded process. Determinism is crucial in that it allows perfect repeatability of a simulated run, facilitating controlled experiments to home in on issues. The simulation steps through time, synchronized across the system, representing a larger amount of real time in a smaller amount of simulated time. In practice, our simulations usually have about a 10-1 factor of real-to-simulated time, which is advantageous for the efficiency of testing.

We run a broad range of simulations testing various aspects of the system. For example, we run a cycle test that uses key-values pairs arranged in a ring that executes transactions to change the values in a manner designed to maintain the ring's integrity, allowing a clear test of transactional isolation.

Joshua simulates all physical components of a FoundationDB system, beginning with the number and type of machines in the cluster. For example, Joshua models drive performance on each machine, including drive space and the possibility of the drive filling up. Joshua also models the network, allowing a small amount of code to specify delivery of packets.

We use Joshua to simulate failures modes at the network, machine, and datacenter levels, including connection failures, degradation of machine performance, machine shutdowns or reboots, machines coming back from the dead, etc. We stress-test all of these failure modes, failing machines at very short intervals, inducing unusually severe loads, and delaying communications channels.

For a while, there was an informal competition within the engineering team to design failures that found the toughest bugs and issues the most easily. After a period of one-upsmanship, the reigning champion is called "swizzle-clogging". To swizzle-clog, you first pick a random subset of nodes in the cluster. Then, you "clog" (stop) each of their network connections one by one over a few seconds. Finally, you unclog them in a random order, again one by one, until they are all up. This pattern seems to be particularly good at finding deep issues that only happen in the rarest real-world cases.

Joshua's success has surpassed our expectation and has been vital to our engineering team. It seems unlikely that we would have been able to build FoundationDB without this technology.
