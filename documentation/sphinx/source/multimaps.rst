#########
Multimaps
#########

**Python** :doc:`Java <multimaps-java>`

Goal
====

Create an `multimap <http://en.wikipedia.org/wiki/Multimap>`_ data structure with `multiset <http://en.wikipedia.org/wiki/Multiset>`_ values.

Challenge
=========

Support efficient operations on multimaps, including random addition, removal, and retrieval of indexed values.

Explanation
===========

Multimaps are a generalization of dictionaries (a.k.a. maps or associative arrays) in which each index can have multiple values. Multimaps can be further generalized by allowing a value to be present more than once for a given index, so that each index is associated with a multiset. These structures have a simple and efficient representation as FoundationDB's key-value pairs.

Ordering
========

We store all values of a given index using adjacent key-value pairs. This allows all values of an index to be retrieved with a single range read.

Pattern
=======

We store all values in the multimap within a subspace, which takes care of packing our keys into byte strings.
::

    multi = fdb.Subspace(('M',))

Because we need to store multiple values per index, we'll store them within keys, with each (index, value) pair in its own key. To implement the multiset we’ll record the number of occurrences of each value. This is done by storing a positive integer with the key using an atomic addition. Each addition of a given value for an index will increment the count by 1:
::

    tr.add(multi[index][value], struct.pack('<q', 1))

By using a read-free atomic addition, FoundationDB guarantees that the addition operation will not conflict. As a result, values can be frequently added by multiple clients.

Subtracting values, on the other hand, requires a read to ensure that the value count does not fall below 0. (Hence, unlike additions, subtractions will be subject to conflicts.) We'll just delete the key if a subtraction reduces the count to 0 in order to keep the representation sparse.

Extensions
==========

*Negative value counts*

We can generalize the representation further by allowing the count to be an arbitrary integer rather than restricting it to a positive integer. This extension may be useful for applications that record a deficit or debt of some resource. In this case, the code becomes even simpler and more efficient: we can simply remove the read and test from subtraction, making it conflict-free like the addition operation.

Code
====

Here’s a simple implementation of multimaps with multisets as described::

    import struct
     
    multi = fdb.Subspace(('M',))
     
    # Multimaps with multiset values
    @fdb.transactional
    def multi_add(tr, index, value):
        tr.add(multi[index][value], struct.pack('<q', 1))
     
    @fdb.transactional
    def multi_subtract(tr, index, value):
        v = tr[multi[index][value]]
        if v.present() and struct.unpack('<q', str(v))[0] > 1:
            tr.add(multi[index][value], struct.pack('<q', -1))
        else:
            del tr[multi[index][value]]
     
    @fdb.transactional
    def multi_get(tr, index):
        return [multi.unpack(k)[1] for k, v in tr[multi[index].range()]]
     
    @fdb.transactional
    def multi_get_counts(tr, index):
        return {multi.unpack(k)[1]:struct.unpack('<q', v)[0]
                for k, v in tr[multi[index].range()]}
     
    @fdb.transactional
    def multi_is_element(tr, index, value):
        return tr[multi[index][value]].present()
