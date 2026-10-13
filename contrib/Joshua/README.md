# Overview

This directory provides the files needed to create a Joshua correctness bundle for testing FoundationDB.

Rigorous testing is central to our engineering process. The features of our core are challenging, requiring us to meet exacting standards of correctness and performance. Data guarantees and transactional integrity must be maintained not only during normal operations but over a broad range of failure scenarios. At the same time, we aim to achieve performance goals such as low latencies and near-linear scalability. To meet these challenges, we use a combined regime of robust simulation, live performance testing, and hardware-based failure testing.

# Joshua

[Joshua](https://github.com/FoundationDB/fdb-joshua) is the framework FoundationDB uses to run correctness tests at scale. A test bundle (an *ensemble*) is submitted to a coordinating FoundationDB cluster, and Joshua agents claim individual runs from it in parallel and report their results back to that cluster.

Joshua does not perform the simulation itself. Each run invokes TestHarness2, which picks a test and executes it with `fdbserver`, in most cases as `fdbserver -r simulation`: a deterministic simulation of an entire FoundationDB cluster inside a single process, built on Flow, FoundationDB's asynchronous runtime for C++ coroutines. For background on simulation and fault injection, see [Simulation and Testing](../../documentation/sphinx/source/testing.rst).

To build the correctness bundle, run `ninja package_tests` in a configured build directory. This produces `packages/correctness-<version>.tar.gz`, whose top-level `joshua_test` and `joshua_timeout` scripts are the entry points Joshua invokes.

*   `scripts/`: This directory contains shell scripts that serve as entry points for running tests. Joshua invokes these scripts, which then set up the environment and execute the test runner.
    *   **`correctnessTest.sh`**: This is the primary script for running correctness tests (In the ensemble tarball, it is renamed `joshua_test`). It is responsible for invoking the Python-based `TestHarness2` and passing it the necessary configuration. It also handles the creation and cleanup of temporary output directories.
    *   Other scripts like `bindingTest.sh` and `valgrindTest.sh` are used for different, specialized test runs.

For detailed information on the operation of the Python test runner itself, including its configuration options and output structure, please see the **[TestHarness2 README](../TestHarness2/README.md)**.
