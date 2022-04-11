.. _transaction-profiler-analyzer:

###################################
Transaction profiling and analyzing
###################################

FoundationDB natively implements transaction profiling and analyzing. There are two ways to enable transaction profiling in FoundationDB. One is globally through the database, via ``fdbcli`` command which sets keys in the database and the clients pick it up. 
 
``e.g.  fdbcli> profile client set 0.01 100MB`` profiles 1% of transactions and maintains 100MB worth of history in the database.
 
Second way is through client side knobs ``CSI_SAMPLING_PROBABILITY`` and ``CSI_SIZE_LIMIT`` which have to be set at every client that you want to profile. Enabling transaction profiling through the database setting has higher precedence and overrides any client knob settings.
 
There are only two inputs for transaction profiling i.e. sampling rate and the size limit.

The transactions are sampled at the specified rate and all the events for that sampled transaction are recorded. Then at 30 second interval, the data for all the sampled transactions during that interval is flushed to the database. The sampled data is written into special key space ``“\xff\x02/fdbClientInfo/ - \xff\x02/fdbClientInfo0”``
 
The second part of transaction profiling involves deleting old sampled data to restrict the size. Retention is purely based on the input size limit. If the size of all the recorded data exceeds the input limit, then the old ones get deleted. But the limit is a soft limit, you could go over the limit temporarily.
 
There are many ways that this data can be exposed for analysis. One can imagine building a client that reads the data from the database and streams it to external tools such as Wavefront.
 
One such tool that’s available as part of open source FDB is a python script called ``transaction_profiling_analyzer.py`` that's available here on `GitHUb <https://github.com/apple/foundationdb/blob/main/contrib/transaction_profiling_analyzer/transaction_profiling_analyzer.py>`_. It reads the sampled data from the database and outputs it in a user friendly format. Currently it’s most useful in identifying hot key-ranges (for both reading and writing).
 
Prerequisites
=============

* ``python3``
* ``fdb python bindings`` - If you don't have the Python bindings installed, you can append $BUILDDIR/bindings/python to the PYTHONPATH  environment variable, then you should be able to import fdb

Additional packages
===================

* ``dateparser`` - for human date parsing
* ``sortedcontainers`` - for estimating key range read/write density

Sample usage
============

* ``$python3 transaction_profiling_analyzer.py --help`` - Shows the help message and exits
 
* ``python3 transaction_profiling_analyzer.py -C fdb.cluster --start-time "17:00 2020/07/07 PDT" --end-time "17:50 2020/07/07 PDT"`` - Analyzes and prints full information between a start and end time frame
 
Using filters:
==============

* ``python3 ~/transaction_profiling_analyzer.py -C fdb.cluster --filter-get --start-time "17:00 2020/07/07 PDT" --end-time "17:50 2020/07/07 PDT"`` - Analyzes and prints information about gets between a start and end time frame
 
* ``python3 ~/transaction_profiling_analyzer.py -C fdb.cluster --filter-get --start-time "17:00 2020/07/07 PDT" --end-time "17:50 2020/07/07 PDT" --top-requests 5`` - Analyzes and prints information about top 5 keys for gets between a start and end time frame
