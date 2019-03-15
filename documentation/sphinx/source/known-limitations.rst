
.. _known-limitations:

#################
Known Limitations
#################

.. include:: guide-common.rst.inc

FoundationDB has limitations, some of which may be addressed in future versions.

For related information, also see:
 * :doc:`platforms` that affect the operation of FoundationDB.
 * :ref:`system-requirements` for OS/hardware requirements.
 * :doc:`anti-features` for limitations of the scope of the FoundationDB core.
 * :ref:`developer-guide-peformance-considerations` for how different workloads can limit performance.
 
Design limitations
==================

These limitations come from fundamental design decisions and are unlikely to change in the short term. Applications using FoundationDB should plan to work around these limitations. See :doc:`anti-features` for related discussion of our design approach to the FoundationDB core.

.. _large-transactions:

Large transactions
------------------

Transaction size cannot exceed 10,000,000 bytes of affected data. Keys, values, and ranges that you write are included as affected data. Keys and ranges that you read are also included as affected data, but values that you read are not. Likewise, conflict ranges that you :ref:`add <api-python-conflict-ranges>` or remove (using a :ref:`snapshot read <api-python-snapshot-reads>` or a :ref:`transaction option <api-python-no-write-conflict-range>`) are also added or removed from the scope of affected data.

If any single transaction exceeds one megabyte of affected data, you should modify your design. In the current version, these large transactions can cause performance issues and database availability can (briefly) be impacted.

.. admonition:: Workarounds

    See the discussion in :ref:`long transactions <long-transactions>` for applicable workarounds.

.. _large-keys-and-values:

Large keys and values
---------------------

Keys cannot exceed 10,000 bytes in size. Values cannot exceed 100,000 bytes in size. Errors will be raised by the client if these limits are exceeded.

.. admonition:: Workarounds

    FoundationDB provides efficient ways to :doc:`design keys and values<largeval>` to work around this limitation.

.. _spinning-HDDs:

Spinning HDDs
-------------

FoundationDB is only designed for good performance with rotational disk drives when using the durable :ref:`memory <configuration-storage-engine-memory>` storage engine. It is not recommended that you run FoundationDB on rotational HDDs when using the :ref:`ssd <configuration-storage-engine-ssd>` storage engine. Many algorithms and optimizations have been made to specifically target good performance on solid-state storage that do not translate well to HDDs. Reduced performance and/or database availability issues can be expected.

.. admonition:: Recommendation

    Large disk arrays and abstracted storage subsystems with sufficient I/O performance may be able to overcome this limitation, but testing specific use cases will be required.

.. _dont-use-key-selectors-for-paging:

Key selectors with large offsets are slow
-----------------------------------------

The current version of FoundationDB resolves key selectors with large offsets in O(offset) time. A common misusage of key selectors is using offsets to page through a large range of data (i.e. reading 'a'+0 to 'a'+100, then 'a'+100 to 'a'+200, etc.) 

.. admonition:: Workarounds

    An efficient alternative is to use the limit parameter with range reads, starting subsequent reads at the key after the last one returned. You can also use the iterator functionality available in most of the language bindings, which uses this technique internally. 

    The RankedSet layer provides a data structure in which large offsets and counting operations require only O(log N) time. It is a good choice for applications such as large leaderboards that require such functionality.

Not a security boundary
-----------------------

Anyone who can connect to a FoundationDB cluster can read and write every key in the database. There is no user-level access control. External protections must be put into place to protect your database.

Current limitations
===================

These limitations do not reflect fundamental aspects of our design and are likely to be resolved or mitigated in future versions. Administrators should be aware of these issues, but longer-term application development should be less driven by them.

.. _long-transactions:

Long running transactions
-------------------------

FoundationDB currently does not support transactions running for over five seconds. In particular, after 5 seconds from the first read in a transaction:

* subsequent reads that go to the database will usually raise a ``transaction_too_old`` :doc:`error <api-error-codes>` (although reads cached by the client will not);
* a commit with any write will raise a ``transaction_too_old`` or ``not_committed`` :doc:`error <api-error-codes>`.

Long running read/write transactions are a design limitation, see the discussion in :doc:`anti-features`.

.. admonition:: Workarounds

    The effect of long and large transactions can be achieved using short and small transactions with a variety of techniques, depending on the desired behavior:

    * If an application wants long transactions because of an external process in the loop, it can perform optimistic validation itself at a higher layer.
    * If it needs long-running read snapshots, it can perform versioning in a layer.
    * If it needs large bulk inserts, it can use a level of indirection to swap in the inserted data quickly.

.. _cluster-size:

Cluster size
------------

FoundationDB has undergone performance testing and tuning with clusters of up to 500 cores/processes. Significantly larger clusters may experience performance bottlenecks leading to sub-linear scaling or related issues. 

Database size
-------------

FoundationDB has been tested with databases up to 100 TB (total size of key-value pairs -- required disk space will be significantly higher after replication and overhead).

Limited read load balancing
---------------------------

FoundationDB load balances reads across the servers with replicas of the data being read. However, it does not currently increase the replication factor of keys that are frequently read. As a result, the aggregate read performance of any given key or small contiguous set of keys in a triple-replicated system is limited to the total performance of three server processes (typically on the order of 100,000 reads per second).

.. admonition:: Workarounds

    If data is accessed exceptionally frequently, an application could avoid this limitation by storing such data in multiple subspaces, effectively increasing its replication factor.


