###############
Priority Queues
###############

**Python** :doc:`Java <priority-queues-java>`

Goal
====

Create a data structure for `priority queues <http://en.wikipedia.org/wiki/Priority_queue>`_ supporting operations for push, pop_min, peek_min, pop_max, and peek_max. You may find it helpful to review the :doc:`queues` recipe before this one.

Challenge
=========

Allow efficient operations on a shared priority queue by multiple clients acting concurrently.

Explanation
===========

We can model a priority queue using a key formed from a tuple of three elements: an item's priority, an increasing integer encoding the order in which the item was pushed, and a random element to make the key unique. By making keys unique, we can minimize conflicts for concurrent pushes.

Ordering
========

The ordering of keys will sort items first by priority, then by push order, then randomly (to break ties in concurrent pushes). The minimum and maximum priority items will always be at the beginning and end of the queue, respectively, allowing us to efficiently peek or pop them.

Pattern
=======

We create a subspace for the priority queue, which takes care of packing our tuples into byte strings.
::

 pq = fdb.Subspace(('P',))

Push operations will construct a key-value pair of the form::

 tr[ pq[ priority ][ count ][ random ] ] = value

where priority is supplied by the client, count is an integer that increases by 1 for each item pushed with priority, and random is a randomly generated integer.

Items of the same priority that are pushed concurrently may occasionally be assigned the same count, but their keys will still be distinct and ordered (in this case, randomly). The count is derived by reading and incrementing the highest count previously used for a given priority. By using a snapshot read, we guarantee that pushing is conflict-free.

To implement this model, we need an efficient way of finding the first and last key in the queue. (The ordering of keys guarantees that these will always be the proper keys to pop or peek.) FoundationDB's range reads have limit and reverse options that let us accomplish this. Given the range of the subspace::

 r = pq.range()

we can find the first and last key-value pairs in the range with::

 tr.get_range(r.start, r.stop, limit=1) # first
 tr.get_range(r.start, r.stop, limit=1, reverse=True) # last

Extensions
==========

*High-Contention Pop Operations*

To minimize conflicts during pop operations, we can use a staging technique to service the requests. If a pop operation doesn't initially succeed, it registers a pop request in a semi-ordered set of such requests. It then enters a retry loop in which it attempts to fulfill outstanding requests.

Code
====

Here's a basic implementation of the model::

    import os
     
    pq = fdb.Subspace(('P',))
     
    @fdb.transactional
    def push(tr, value, priority):
        tr[pq[priority][_next_count(tr, priority)][os.urandom(20)]] = value
     
    @fdb.transactional
    def _next_count(tr, priority):
        r = pq[priority].range()
        for key, value in tr.snapshot.get_range(r.start, r.stop, limit=1, reverse=True):
            return pq[priority].unpack(key)[0] + 1
        return 0
     
    @fdb.transactional
    def pop(tr, max=False):
        r = pq.range()
        for item in tr.get_range(r.start, r.stop, limit=1, reverse=max):
            del tr[item.key]
            return item.value
     
    @fdb.transactional
    def peek(tr, max=False):
        r = pq.range()
        for item in tr.get_range(r.start, r.stop, limit=1, reverse=max):
            return item.value
