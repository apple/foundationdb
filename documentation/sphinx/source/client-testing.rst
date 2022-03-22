.. default-domain:: cpp
.. highlight:: cpp

###############
Client Testing
###############

###################################
Testing Error Handling with Buggify
###################################

FoundationDB clients need to handle errors correctly. Wrong error handling can lead to many bugs - in the worst case it can
lead to a corrupted database. Because of this it is important that an application or layer author tests properly their
application during failure scenarios. But this is non-trivial. In a development environment cluster failures are very
unlikely and it is therefore possible that certain types of exceptions are never tested in a controlled environment.

The simplest way of testing for these kind of errors is a simple mechanism called ``Buggify``. If this option is enabled
in the client, the client will randomly throw errors that an application might see in a production environment. Enable this
option in testing will greatly improve the probability that error handling is tested properly.

Options to Control Buggify
==========================

There are four network options to control the buggify behavior. By default, buggify is disabled (as it will behave in a way
that is not desirable in a production environment). The options to control buggify are:

- ``buggify_enable``
  This option takes no argument and will enable buggify.
- ``buggify_disable``
  This can be used to disable buggify again.
- ``client_buggify_section_activated_probability`` (default ``25``)
  A number between 0 and 100.
- ``client_buggify_section_fired_probability`` (default ``25``)
  A number between 0 and 100.

The way buggify works is by enabling sections in the code first that get only executed with a certain probability. Generally
these code sections will simply introduce a synthetic error.

When a section is passed for the first time, the client library will decide randomly whether that code section will be enabled
or not. It will be enabled with a probability of ``client_buggify_section_activated_probability``.

Whenever the client executes a buggify-enabled code-block, it will randomly execute it. This is to make sure that a certain
exception doesn't always fire. The probably for executing such a section is ``client_buggify_section_fired_probability``.

################################
Simulation and Cluster Workloads
################################


FoundationDB comes with its own testing framework. Tests are implemented as workloads. A workload is nothing more than a class
that gets called by server processes running the ``tester`` role. Additionally, a ``fdbserver`` process can run a simulator that
simulates a full fdb cluster with several machines and different configurations in one process. This simulator can run the same
workloads you can run on a real cluster. It will also inject random failures like network partitions and disk failures.

This tutorial explains how one can implement a workload, how one can orchestrate a workload on a cluster with multiple clients, and
how one can run a workload within a simulator. Running in a simulator is also useful as it does not require any setup: you can simply
run one command that will provide you with a fully functional FoundationDB cluster.

General Overview
================

Workloads in FoundationDB are generally compiled into the binary. However, FoundationDB also provides the ability to load workloads
dynamically. This is done through ``dlopen`` (on Unix like operating systems) or ``LoadLibrary`` (on Windows).

Parallelism and Determinism
===========================

A workload can run either in a simulation or on a real cluster. In simulation, ``fdbserver`` will simulate a whole cluster and will
use a deterministic random number generator to simulate random behavior and random failures. This random number generator is initialized
with a random seed. In case of a test failure, the user can reuse the given seed and rerun the same test in order to further observe
and debug the behavior.

However, this will only work as long as the workload doesn't introduce any non-deterministic behavior. One example of non-deterministic
behavior is the running multiple threads.

The workload is created in the main network thread and it will run in the main network thread. Because of this, using any blocking
function (for example ``blockUntilReady`` on a future object) will result in a deadlock. Using the callback API is therefore required
if one wants to keep the simulator's deterministic behavior.

For existing applications and layers, however, not using the blocking API might not be an option. For these use-cases, a user can chose
to start new threads and use the blocking API from within these threads. This will mean that test failures will be non-deterministic and
might be hard to reproduce.

To start a new thread, one has to "bind" operating system threads to their simulated processes. This can be done by setting the
``ProcessId`` in the child threads when they get created. In Java this is done by only starting new threads through the provided
``Executor``. In the C++ API one can use the ``FDBWorkloadContext`` to do that. For example:

.. code-block:: C++

   template<class Fun>
   std::thread startThread(FDBWorkloadContext* context, Fun fun) {
       auto processId = context->getProcessID();
       return std::thread([context, processID, fun](
           context->setProcessID(processID);
           fun();
       ));
   }

Finding the Shared Object
=========================

When the test starts, ``fdbserver`` needs to find the shared object to load. The name of this shared object has to be provided.

For Java, we provide an implementation in ``libjava_workloads.so`` which can be built out of the sources. The tester will look
for the key ``libraryName`` in the test file which should be the name of the library without extension and without the ``lib``
prefix (so ``java_workloads`` if you want to write a Java workload).

By default, the process will look for the library in the directory ``../shared/foundationdb/`` relative to the location of the
``fdbserver`` binary. If the library is somewhere else on the system, one can provide the absolute path to the library (only
the folder, not the file name) in the test file with the ``libraryPath`` option.

Implementing a C++ Workload
===========================

In order to implement a workload, one has to build a shared library that links against the fdb client library. This library has to
exppse a function (with C linkage) called workloadFactory which needs to return a pointer to an object of type ``FDBWorkloadFactory``.
This mechanism allows the other to implement as many workloads within one library as she wants. To do this the pure virtual classes
``FDBWorkloadFactory`` and ``FDBWorkload`` have to be implemented.

.. function:: FDBWorkloadFactory* workloadFactory(FDBLogger*)

   This function has to be defined within the shared library and will be called by ``fdbserver`` for looking up a specific workload.
   ``FDBLogger`` will be passed and is guaranteed to survive for the lifetime of the process. This class can be used to write to the
   FoundationDB traces. Logging anything with severity ``FDBSeverity::Error`` will result in a hard test failure. This function needs
   to have c-linkage, so define it in a ``extern "C"`` block.

.. function:: std::shared_ptr<FDBWorkload> FDBWorkload::create(const std::string& name)

   This is the only method to be implemented in ``FDBWorkloadFactory``. If the test file contains a key-value pair ``workloadName``
   the value will be passed to this method (empty string otherwise). This way, a library author can implement many workloads in one
   library and use the test file to chose which one to run (or run multiple workloads either concurrently or serially).

.. function:: std::string FDBWorkload::description() const

   This method has to return the name of the workload. This can be a static name and is primarily used for tracing.

.. function:: bool FDBWorkload::init(FDBWorkloadContext* context)

   Right after initialization

.. function:: void FDBWorkload::setup(FDBDatabase* db, GenericPromise<bool> done)

   This method will be called by the tester during the setup phase. It should be used to populate the database.

.. function:: void FDBWorkload::start(FDBDatabase* db, GenericPromise<bool> done)

   This method should run the actual test.

.. function:: void FDBWorkload::check(FDBDatabase* db, GenericPromise<bool> done)

   When the tester completes, this method will be called. A workload should run any consistency/correctness tests
   during this phase.

.. function:: void FDBWorkload::getMetrics(std::vector<FDBPerfMetric>& out) const

   If a workload collects metrics (like latencies or throughput numbers), these should be reported back here.
   The multitester (or test orchestrator) will collect all metrics from all test clients and it will aggregate them.

Implementing a Java Workload
============================

In order to implement your own workload in Java you can simply create an implementation of the abstract class ``AbstractWorkload``.
A minimal implementation will look like this:

.. code-block:: java

   package my.package;
   import com.apple.foundationdb.testing.Promise;
   import com.apple.foundationdb.testing.AbstractWorkload;
   import com.apple.foundationdb.testing.WorkloadContext;

   class MinimalWorkload extends AbstractWorkload {
       public MinimalWorkload(WorkloadContext ctx) {
           super(ctx);
       }

       @Override
       public void setup(Database db, Promise promise) {
           log(20, "WorkloadSetup", null);
           promise.send(true);
       }

       @Override
       public void start(Database db) {
           log(20, "WorkloadStarted", null);
           promise.send(true);
       }

       @Override
       public boolean check(Database db) {
           log(20, "WorkloadFailureCheck", null);
           promise.send(true);
       }
   }

The lifecycle of a test will look like this:

1. All testers will create an instance of the ``AbstractWorkload`` implementation.
2. All testers will (in parallel but not guaranteed exactly at the same time) call
   ``setup`` and they will wait for all of them to finish. This phase can be used to
   pre-populate data.
3. All tester will then call start (again, in parallel) and wait for all of them to
   finish.
4. All testers will then call ``check`` on all testers and use the returned boolean
   to determine whether the test succeeded.

All these methods take a ``Database`` object as an argument. This object can be used
to create and execute transactions against the cluster.

When implementing workloads, an author has to follow these rules:

- To write tracing to the trace-files one should use ``AbstractWorkload.log``. This
  Method takes three arguments: an integer for severity (5 means debug, 10 means log,
  20 means warning, 30 means warn always, and 40 is a severe error). If any tester
  logs something of severity 40, the test run is considered to have failed.
- In order to increase throughput on the cluster, an author might want to spawn several
  threads. However, threads *MUST* only be spawn through the ``Executor`` instance one
  can get from ``AbstractWorkload.getExecutor()``. Otherwise, a simulation test will
  probably segfault. The reason for this is that we need to keep track of which simulated
  machine a thread corresponds to internally.

Within a workload you have access to the ``WorkloadContext`` which provides additional
information about the current execution environment. The context can be accessed through
``this.context`` and provides the following methods:

- ``String getOption(String name, String defaultValue)``. A user can provide parameters to workloads
  through a configuration file (explained further down). These parameters are provided to
  all clients through the context and can be accessed with this method.
- ``int getClientId()`` and ``int getClientCount()``. An author can determine how many
  clients are running in the cluster and each of those will get a globally unique ID (a number
  between 0 and clientCount - 1). This is useful for example if you want to generate transactions
  that are guaranteed to not conflict with transactions from other clients.
- ``int getSharedRandomNumber()``. At startup a random number will be generated. This will allow for
  generating the same random numbers across several machines if this number is used as a seed.


Running a Workload in the Simulator
===================================

We'll first walk how one can run a workload in a simulator. FoundationDB comes already with a large number
of workloads. But some of them can't be run in simulation while other don't work on a real cluster. Most
will work on both though. To look for examples how these can be ran, you can find configuration files in
the ``tests`` directory in the FoundationDB source tree.

We will now go through an example how you can write a relatively complex test and run it in the simulator.
Writing and running tests in the simulator is a simple two-step process.

1. Write the test.
2. Run ``fdbserver`` in simulation mode and provide it with the test file.

Write the Test
--------------

A workload is not a test. A test is a simple test file that tells the test orchestrator which workloads it
should run and in which order. Additionally one can provide parameters to workloads through this file.

A test file might look like this:

.. code-block:: none

   testTitle=MyTest
     testName=External
     libraryName=java_workloads
     workloadName=my.package.MinimalWorkload
     classPath=PATH_TO_JAR_OR_DIR_CONTAINING_WORKLOAD,OTHER_DEPENDENCIES

     testName=Attrition
     testDuration=5.0
     reboot=true
     machinesToKill=3

   testTitle=AnotherTest
     testName=External
     libraryName=java_workloads
     workloadName=my.package.MinimalWorkload
     classPath=PATH_TO_JAR_OR_DIR_CONTAINING_WORKLOAD,OTHER_DEPENDENCIES
     someOption=foo

     testName=External
     libraryName=java_workloads
     workloadName=my.package.AnotherWorkload
     classPath=PATH_TO_JAR_OR_DIR_CONTAINING_WORKLOAD,OTHER_DEPENDENCIES
     anotherOption=foo

This test will do the following:

1. First it will run ``MinimalWorkload`` without any parameter.
2. After 5.0 seconds the simulator will reboot 3 random machines (this is what Attrition does
   and this workload is provided by FoundationDB. This is one of the few workloads that only
   work in the simulator).
3. When all workloads are finished, it will run ``MinimalWorkload``
   again. This time it will have the option ``someOption`` set to
   ``foo``. Additionally it will run ``AnotherWorkload`` in parallel.

How to set the Class Path correctly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As you can see from above example, we can set the classpath through two different mechanisms. However, one has
to be careful as they can't be used interchangeably.

- You can set a class path through the JVM argument ``-Djava.class.path=...``. This is how you have to pass the
  path to the FoundationDB client library (as the client library is needed during the initialization phase). However,
  only the first specified section will have any effect as the other Workloads will run in the same VM (and arguments,
  by nature, can only be passed once).
- The ``classPath`` option. This option will add all paths (directories or JAR-files) to the classPath of the JVM
  while it is running. Not being able to add the path will result in a test failure. This is useful to add different
  dependencies to different workloads. A path can appear more than once across sections. However, they must not
  conflict with each other as we never remove something from the classpath.

Run the simulator
-----------------

This step is very simple. You can simply run ``fdbserver`` with role simulator
and pass the test with ``-f``:

.. code-block:: sh

   fdbserver -r simulation -f testfile.txt


Running a Workload on an actual Cluster
=======================================

Running a workload on a cluster works basically the smae way. However, one must
actually setup a cluster first. This cluster must run between one and many server
processes with the class test. So above 2-step process becomes a bit more complex:

1. Write the test (same as above).
2. Set up a cluster with as many test clients as you want.
3. Run the orchestrator to actually execute the test.

Step 1. is explained further up. For step 2., please refer to the general FoundationDB
configuration. The main difference to a normal FoundationDB cluster is that some processes
must have a test class assigned to them. This can be done in the ``foundationdb.conf``. For
example this file would create a server with 8 processes of which 4 would act as test clients.

.. code-block:: ini

    [fdbmonitor]
    user = foundationdb
    group = foundationdb

    [general]
    restart-delay = 60
    cluster-file = /etc/foundationdb/fdb.cluster

    ## Default parameters for individual fdbserver processes
    [fdbserver]
    command = /usr/sbin/fdbserver
    public-address = auto:$ID
    listen-address = public
    datadir = /var/lib/foundationdb/data/$ID
    logdir = /var/log/foundationdb

    [fdbserver.4500]
    [fdbserver.4501]
    [fdbserver.4502]
    [fdbserver.4503]
    [fdbserver.4510]
    class = test
    [fdbserver.4511]
    class = test
    [fdbserver.4512]
    class = test
    [fdbserver.4513]
    class = test

Running the actual test can be done with ``fdbserver`` as well. For this you can call the process
with the ``multitest`` role:

.. code-block:: sh

   fdbserver -r multitest -f testfile.txt

This command will block until all tests are completed.
