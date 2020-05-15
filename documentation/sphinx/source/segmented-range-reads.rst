#####################
Segmented Range Reads
#####################

**Python** :doc:`Java <segmented-range-reads-java>`

Goal
====

Perform range reads in calibrated batches.

Challenge
=========

Retrieve data in batches whose size you select based on your data model or application.

Explanation
===========

FoundationDB supports streaming modes that makes range reads efficient even for large amounts of data. You can usually get good performance by selecting the proper streaming mode. However, there are particular cases in which you may want to exercise finer grained control of data retrieval. You can exercise this control using the limit parameter.

Ordering
========

This approach works with arbitrary ranges, which are, by definition, ordered. The goal here is to be able to walk through sub-ranges in order.

Pattern
=======

A range read returns a container that issues asynchronous reads to the database. The client usually processes the data by iterating over the values returned by the container. The API balances latency and bandwidth by fetching data in batches as determined by the ``streaming_mode`` parameter. Streaming modes allow you to customize this balance based on how you intend to consume the data. The default streaming mode (iterator) is quite efficient. However, if you anticipate that your range read will retrieve a large amount of data, you should select a streaming mode to match your use case. For example, if you're iterating through a large range and testing against a condition that may result in early termination, you can use the ``small`` streaming mode::

    for k, v in tr.get_range('a', 'c', streaming_mode=fdb.StreamingMode.small):
        if halting_condition(k, v): break
        print(k,v)

However, in some situations, you may want to explicitly control the number of key-value pairs returned. This may be the case if your data model creates blocks of N key-value pairs, and you want to read M blocks at a time and therefore a sub-range of N x M key-value pairs. You can use the limit parameter for this purpose.

Extensions
==========

*Parallel retrieval*

For very large range reads, you can use multiple clients to perform reads in parallel. In this case, you'll want to estimate sub-ranges of roughly equal size based on the distribution of your keys. The :ref:`locality <api-python-locality>` functions can be used to find the partition boundaries used by the database, which will be roughly uniformly distributed in bytes of data. The partition boundaries can then be used to derive boundaries between sub-ranges for parallel reading.

Code
====

Here’s a basic function that successively reads sub-ranges of a size determined by the value of ``LIMIT``.
::

    def get_range_limited(tr, begin, end):
        keys_found = True
        while keys_found:
            keys_found = []
            for k, v in tr.get_range(begin, end, limit=LIMIT):
                keys_found.append(k)
            if keys_found:
                begin = fdb.KeySelector.first_greater_than(keys_found[-1])
                yield keys_found
