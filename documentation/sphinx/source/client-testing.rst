###############
Client Testing
###############

FoundationDB comes with its own testing framework. Tests are implemented as workloads. A workload is nothing more than a class
that gets called by server processes running the ``tester`` role. Additionally, a ``fdbserver`` process can run a simulator that
simulates a full fdb cluster with several machines and different configurations in one process. This simulator can run the same
workloads you can run on a real cluster. It will also inject random failures like network partitions and disk failures.

Currently, workloads can only be implemented in Java, support for other languages might come later.

This tutorial explains how one can implement a workload, how one can orchestrate a workload on a cluster with multiple clients, and
how one can run a workload within a simulator. Running in a simulator is also useful as it does not require any setup: you can simply
run one command that will provide you with a fully functional FoundationDB cluster.

Preparing the fdbserver Binary
==============================

In order to run a Java workload, ``fdbserver`` needs to be able to embed a JVM. Because of that it needs to be linked against JNI.
The official FDB binaries do not link against JNI and therefore one can't use that to run a Java workload. Instead you need to
download the sources and build them. Make sure that ``cmake`` can find Java and pass ``-DWITH_JAVA_WORKLOAD=ON`` to cmake.

After FoundationDB was built, you can use ``bin/fdbserver`` to run the server. The jar file containing the client library can be
found in ``packages/fdb-VERSION.jar``. Both of these are in the build directory.

Implementing a Workload
=======================

In order to implement your own workload in Java you can simply create an implementation of the abstract class ``AbstractWorkload``.
A minimal implementation will look like this:

.. code-block:: java

   package my.package;
   import com.apple.foundationdb.testing.AbstractWorkload;
   import com.apple.foundationdb.testing.WorkloadContext;

   class MinimalWorkload extends AbstractWorkload {
       public MinimalWorkload(WorkloadContext ctx) {
           super(ctx);
       }

       @Override
       public void setup(Database db) {
           log(20, "WorkloadSetup", null);
       }

       @Override
       public void start(Database db) {
           log(20, "WorkloadStarted", null);
       }

       @Override
       public boolean check(Database db) {
           log(20, "WorkloadFailureCheck", null);
           return true;
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
     testName=JavaWorkload
     workloadClass=my.package.MinimalWorkload
     jvmOptions=-Djava.class.path=*PATH_TO_FDB_CLIENT_JAR*,*other options you want to pass to the JVM*
     classPath=PATH_TO_JAR_OR_DIR_CONTAINING_WORKLOAD,OTHER_DEPENDENCIES

     testName=Attrition
     testDuration=5.0
     reboot=true
     machinesToKill=3

   testTitle=AnotherTest
     workloadClass=my.package.MinimalWorkload
     workloadClass=my.package.MinimalWorkload
     jvmOptions=-Djava.class.path=*PATH_TO_FDB_CLIENT_JAR*,*other options you want to pass to the JVM*
     classPath=PATH_TO_JAR_OR_DIR_CONTAINING_WORKLOAD,OTHER_DEPENDENCIES
     someOpion=foo

     workloadClass=my.package.AnotherWorkload
     workloadClass=my.package.AnotherWorkload
     jvmOptions=-Djava.class.path=*PATH_TO_FDB_CLIENT_JAR*,*other options you want to pass to the JVM*
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

   fdbserver -r simulator -f testfile.txt


Running a Workload on an actual Cluster
=======================================

Running a workload on a cluster works basically the smae way. However, one must
actually setup a cluster first. This cluster must run between one and many server
processes with the class test. So above 2-step process becomes a bit more complex:

1. Write the test (same as above).
2. Set up a cluster with as many test clients as you want.
3. Run the orchestor to actually execute the test.

Step 1. is explained further up. For step 2., please refer to the general FoundationDB
configuration. The main difference to a normal FoundationDB cluster is that some processes
must have a test class assigned to them. This can be done in the ``foundationdb.conf``. For
example this file would create a server with 8 processes of which 4 would act as test clients.

.. code-block:: ini

    [fdbmonitor]
    user = foundationdb
    group = foundationdb

    [general]
    restart_delay = 60
    cluster_file = /etc/foundationdb/fdb.cluster

    ## Default parameters for individual fdbserver processes
    [fdbserver]
    command = /usr/sbin/fdbserver
    public_address = auto:$ID
    listen_address = public
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
