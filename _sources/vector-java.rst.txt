######
Vector
######

:doc:`Python <vector>` **Java**

Goal
====

Create a vector data structure.

Challenge
=========

Maintain the performance characteristics of vectors, including efficient random lookup, append, updates, scanning, and truncation.

Explanation
===========

Using the vector index to form a key allows efficient retrieval of individual vector elements. The ordering of keys further allows scanning of an entire vector.

Ordering
========

We can exploit the ordering of FoundationDB’s keys to place adjacent vector elements into adjacent keys. This approach supports efficient retrieval of an entire vector using a single range read.

Pattern
=======

We store each element of the vector as a single key-value pair. We store each item in the vector within a subspace, which takes care of packing our indexes into byte strings.

.. code-block:: java

 vector = new Subspace(Tuple.from("V"));

For each index in the vector, we store:

.. code-block:: java

 tr.set(vector.pack(Tuple.from(index)), Tuple.from(value).pack());

The tuple packing ensures that adjacent vector elements are stored as adjacent key values. This means that scanning an vector can be completed as a single range-read operation, which is very efficient. Likewise, looking up an individual vector element translates to a single random database read. Truncating the vector becomes a range-clear operation, which is O(log n) in FoundationDB.

Extensions
==========

Multi-dimensional
-----------------

This approach can easily be extended to multidimensional vectors by adding additional vector indexes to the tuples. Like in an in-memory vector, the ordering of the dimensions determine what kind of range operations are most efficient. With two dimensions (index1, index2), reads with a fixed index1 value and over a range of index2 values are most efficient.

Sparse
------

Since we are using the presence of a key-value pair to denote the presence of each individual vector element, we can efficiently represent sparse vectors by not storing key-value pairs for absent elements and ranges.

Multiple values
---------------

As described, each vector element stores only one value. An obvious extension would be to pack multiple logical values together in a single physical value for storage.

External values/composition
---------------------------

Another useful extension is to store a “pointer” in the value of the array to reference another source for the logical value. This allows you to compose an array with other design patterns and data structures. The advantage of transactions is that even data structures with indirection can maintain consistency with concurrent client modifications.

Code
====

Here’s the basic pattern:

.. code-block:: java

    public class MicroVector {
        private static final FDB fdb;
        private static final Database db;
        private static final Subspace vector;

        static {
            fdb = FDB.selectAPIVersion(800);
            db = fdb.open();
            vector = new Subspace(Tuple.from("V"));
        }

        public static Object get(TransactionContext tcx, final long index){
            return tcx.run(tr -> {
                return Tuple.fromBytes(tr.get(vector.pack(
                                Tuple.from(index))).get()).get(0);
            });
        }

        public static void set(TransactionContext tcx, final long index, final Object value){
            tcx.run(tr -> {
                tr.set(vector.pack(Tuple.from(index)), Tuple.from(value).pack());
                return null;
            });
        }
    }
