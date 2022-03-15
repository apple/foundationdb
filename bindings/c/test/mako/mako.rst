##############
🦈 Mako Benchmark
##############

| Mako (named after a very fast shark) is a micro-benchmark for FoundationDB
| which is designed to be very light and flexible
| so that you can stress a particular part of an FoundationDB cluster without introducing unnecessary overhead.


How to Build
============
| ``mako`` gets built automatically when you build FoundationDB.
| To build ``mako`` manually, simply build ``mako`` target in the FoundationDB build directory.
| e.g. If you're using Unix Makefiles, type:
| ``make mako``


Architecture
============
- mako is a stand-alone program written in C,
  which communicates to FoundationDB using C API (via ``libfdb_c.so``)
- It creates one master process, one stats emitter process, and one or more worker processes (multi-process)
- Each worker process creates one FDB network thread, and one or more worker threads (multi-thread)
- All worker threads within the same process share the same network thread


Data Specification
==================
- Key has a fixed prefix + sequential number + padding (e.g. ``mako000000xxxxxx``)
- Value is a random string (e.g. ``;+)Rf?H.DS&UmZpf``)


Arguments
=========
- | ``-m | --mode <mode>``
  | One of the following modes must be specified.  (Required)
  | - ``clean``:  Clean up existing data
  | - ``build``:  Populate data
  | - ``run``:  Run the benchmark

- | ``-c | --cluster <cluster file>``
  | FDB cluster files (Required, comma-separated)

- | ``-d | --num_databases <num_databases>``
  | Number of database objects (Default: 1)
  | If more than 1 cluster is provided, this value should be >= number of cluster

- | ``-a | --api_version <api_version>``
  | FDB API version to use (Default: Latest)

- | ``-p | --procs <procs>``
  | Number of worker processes (Default: 1)

- | ``-t | --threads <threads>``
  | Number of threads per worker process (Default: 1)

- | ``-r | --rows <rows>``
  | Number of rows initially populated (Default: 100000)

- | ``-s | --seconds <seconds>``
  | Test duration in seconds (Default: 30)
  | This option cannot be set with ``--iteration``.

- | ``-i | --iteration <iters>``
  | Specify the number of operations to be executed.
  | This option cannot be set with ``--seconds``.

- | ``--tps|--tpsmax <tps>``
  | Target total transaction-per-second (TPS) of all worker processes/threads.
  | When --tpsmin is also specified, this defines the upper-bound TPS.
  | (Default: Unset / Unthrottled)

- | ``--tpsmin <tps>``
  | Target total lower-bound TPS of all worker processes/threads
  | (Default: Unset / Unthrottled)

- | ``--tpsinterval <seconds>``
  | Time period TPS oscillates between --tpsmax and --tpsmin (Default: 10)

- | ``--tpschange <sin|square|pulse>``
  | Shape of the TPS change (Default: sin)

- | ``--keylen <num>``
  | Key string length in bytes (Default and Minimum: 32)

- | ``--vallen <num>``
  | Value string length in bytes (Default and Minimum: 16)

- | ``-x | --transaction <string>``
  | Transaction specification described in details in the following section.  (Default: ``g10``)

- | ``-z | --zipf``
  | Generate a skewed workload based on Zipf distribution (Default: Unset = Uniform)

- | ``--sampling <num>``
  | Sampling rate (1 sample / <num> ops) for latency stats (Default: 1000)

- | ``--trace``
  | Enable tracing.  The trace file will be created in the current directory.  (Default: Unset)

- | ``--tracepath <path>``
  | Enable tracing and set the trace file path.

- | ``--knobs <knobs>``
  | Set client knobs (comma-separated)

- | ``--commitget``
  | Force commit for read-only transactions (Default: Unset)

- | ``-v | --verbose <level>``
  | Set verbose level (Default: 1)
  | - 0 – Minimal
  | - 1 – Default
  | - 2 – Annoying
  | - 3 – Very Annoying (a.k.a. DEBUG)

- | ``--disable_ryw``
  | Disable snapshot read-your-writes

- | ``--json_report`` defaults to ``mako.json``
  | ``--json_report=PATH``
  | Output stats to the specified json file


Transaction Specification
=========================
| A transaction may contain multiple operations of various types.
| You can specify multiple operations for one operation type by specifying "Count".
| For RANGE operations, the "Range" needs to be specified in addition to "Count".
| Every transaction is committed unless the transaction is read-only.

Operation Types
---------------
- ``g`` – GET
- ``gr`` – GET RANGE
- ``sg`` – Snapshot GET
- ``sgr`` – Snapshot GET RANGE
- ``u`` – Update (= GET followed by SET)
- ``i`` – Insert (= SET with a new key)
- ``ir`` – Insert Range (Sequential)
- ``o`` – Overwrite (Blind write to existing keys)
- ``c`` – CLEAR
- ``sc`` – SET & CLEAR
- ``cr`` – CLEAR RANGE
- ``scr`` – SET & CLEAR RANGE
- ``grv`` – GetReadVersion()

Format
------
| One operation type is defined as ``<Type><Count>`` or ``<Type><Count>:<Range>``.
| When Count is omitted, it's equivalent to setting it to 1.  (e.g. ``g`` is equivalent to ``g1``)
| Multiple operation types within the same transaction can be concatenated.  (e.g. ``g9u1`` = 9 GETs and 1 update)

Transaction Specification Examples
----------------------------------
- | 100 GETs (Non-commited)
  | ``g100``

- | 10 GET RANGE with Range of 50 (Non-commited)
  | ``gr10:50``

- | 90 GETs and 10 Updates (Committed)
  | ``g90u10``

- | 70 GETs, 10 Updates and 10 Inserts (Committed)
  | ``g70u10i10``
  | This is 80-20.


Execution Examples
==================

Preparation
-----------
- Start the FoundationDB cluster and create a database
- Set ``LD_LIBRARY_PATH`` environment variable pointing to a proper ``libfdb_c.so`` shared library

Populate Initial Database
-------------------------
``mako --cluster /etc/foundationdb/fdb.cluster --mode build --rows 1000000 --procs 4``
Note: You may be able to speed up the data population by increasing the number of processes or threads.

Run
---
Run a mixed workload with a total of 8 threads for 60 seconds, keeping the throughput limited to 1000 TPS.
``mako --cluster /etc/foundationdb/fdb.cluster --mode run --rows 1000000 --procs 2 --threads 8 --transaction "g8ui" --seconds 60 --tps 1000``
