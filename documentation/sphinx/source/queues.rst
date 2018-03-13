######
Queues
######

**Python** :doc:`Java <queues-java>`

Challenge
=========

Allow efficient operations on a shared queue by multiple clients acting concurrently.

Explanation
===========

We can model a queue by assigning increasing integers that encode the order of items. To minimize conflicts for concurrent operations, we combine the integers in a tuple with a random element to make the final key unique.

Ordering
========

The ordering of keys preserves the FIFO order of items and therefore lets us identify the next item to be dequeued without maintaining a pointer to it.

Pattern
=======

We store each item in the queue within a subspace, which takes care of packing our integer indexes into byte strings.
::

 queue = fdb.Subspace(('Q',))

As a first cut, we could store each item with a single key-value pair using increasing integer indexes for subsequent items:
::

 tr[queue[index]] = value

However, this would leave concurrent enqueue operations vulnerable to conflicts. To minimize these conflicts, we can add a random integer to the key.
::

 tr[queue[index][random_int]] = value

With this data model, items enqueued concurrently may be assigned the same index, but the keys as a whole will still be ordered (in this case, randomly). By using a :ref:`snapshot read <snapshot isolation>`, we guarantee that enqueuing will be conflict-free.

To implement this model, we need an efficient way of finding the first and last index presently in use. FoundationDB's range reads have limit and reverse options that let us accomplish this. Given the range of the subspace::

 r = queue.range()

we can find the first and last key-value pairs in the range with::

 tr.get_range(r.start, r.stop, limit=1) # first
 tr.get_range(r.start, r.stop, limit=1, reverse=True) # last

Extensions
==========

*High-Contention Dequeue Operations*

To minimize conflicts during dequeue operations, we can use a staging technique to service the requests. If a dequeue operation doesn't initially succeed, it registers a dequeue request in a semi-ordered set of such requests. It then enters a retry loop in which it attempts to fulfill outstanding requests.

Code
====

The following is a simple implementation of the basic pattern::

    import os

    queue = fdb.Subspace(('Q',))

    @fdb.transactional
    def dequeue(tr):
        item = first_item(tr)
        if item is None: return None
        del tr[item.key]
        return item.value

    @fdb.transactional
    def enqueue(tr, value):
        tr[queue[last_index(tr) + 1][os.urandom(20)]] = value

    @fdb.transactional
    def last_index(tr):
        r = queue.range()
        for key, _ in tr.snapshot.get_range(r.start, r.stop, limit=1, reverse=True):
            return queue.unpack(key)[0]
        return 0
        
    @fdb.transactional
    def first_item(tr):
        r = queue.range()
        for kv in tr.get_range(r.start, r.stop, limit=1): 
            return kv
