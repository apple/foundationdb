#########
Multimaps
#########

:doc:`Python <multimaps>` **Java**

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

.. code-block:: java

    static {
        multi = new Subspace(Tuple.from("M"));
    }

Because we need to store multiple values per index, we'll store them within keys, with each (index, value) pair in its own key. To implement the multiset we’ll record the number of occurrences of each value. This is done by storing a positive integer with the key using an atomic addition. Each addition of a given value for an index will increment the count by 1:

.. code-block:: java

    ByteBuffer b = ByteBuffer.allocate(8);
    b.order(ByteOrder.LITTLE_ENDIAN);
    b.putLong(1l);
    tr.mutate(MutationType.ADD, key, b.array());

By using a read-free atomic addition, FoundationDB guarantees that the addition operation will not conflict. As a result, values can be frequently added by multiple clients.

Subtracting values, on the other hand, requires a read to ensure that the value count does not fall below 0. (Hence, unlike additions, subtractions will be subject to conflicts.) We'll just delete the key if a subtraction reduces the count to 0 in order to keep the representation sparse.

Extensions
==========

*Negative value counts*

We can generalize the representation further by allowing the count to be an arbitrary integer rather than restricting it to a positive integer. This extension may be useful for applications that record a deficit or debt of some resource. In this case, the code becomes even simpler and more efficient: we can simply remove the read and test from subtraction, making it conflict-free like the addition operation.

Code
====

Here’s a simple implementation of multimaps with multisets as described:

.. code-block:: java

    import java.nio.ByteBuffer;
    import java.nio.ByteOrder;
    import java.util.ArrayList;
    import java.util.HashMap;
    import java.util.Map.Entry;
    public class MicroMulti {
        private static final FDB fdb;
        private static final Database db;
        private static final Subspace multi;
        private static final int N = 100;

        static {
            fdb = FDB.selectAPIVersion(730);
            db = fdb.open();
            multi = new Subspace(Tuple.from("M"));
        }

        private static void addHelp(TransactionContext tcx, final byte[] key, final long amount){
            tcx.run(tr -> {
                ByteBuffer b = ByteBuffer.allocate(8);
                b.order(ByteOrder.LITTLE_ENDIAN);
                b.putLong(amount);

                tr.mutate(MutationType.ADD, key, b.array());

                return null;
            });
        }

        private static long getLong(byte[] val){
            ByteBuffer b = ByteBuffer.allocate(8);
            b.order(ByteOrder.LITTLE_ENDIAN);
            b.put(val);
            return b.getLong(0);
        }

        public static void add(TransactionContext tcx, final String index,
                                final Object value){
            tcx.run(tr -> {
                addHelp(tr, multi.subspace(Tuple.from(index,value)).getKey(),1l);
                return null;
            });
        }

        public static void subtract(TransactionContext tcx, final String index,
                                    final Object value){
            tcx.run(tr -> {
                Future<byte[]> v = tr.get(multi.subspace(
                                        Tuple.from(index,value)).getKey());

                if(v.get() != null &&  getLong(v.get()) > 1l){
                    addHelp(tr, multi.subspace(Tuple.from(index,value)).getKey(), -1l);
                } else {
                    tr.clear(multi.subspace(Tuple.from(index,value)).getKey());
                }
                return null;
            });
        }

        public static ArrayList<Object> get(TransactionContext tcx, final String index){
            return tcx.run(tr -> {
                ArrayList<Object> vals = new ArrayList<Object>();
                for(KeyValue kv : tr.getRange(multi.subspace(
                                    Tuple.from(index)).range())){
                    vals.add(multi.unpack(kv.getKey()).get(1));
                }
                return vals;
            });
        }

        public static HashMap<Object,Long> getCounts(TransactionContext tcx,
                                                    final String index){
            return tcx.run(tr -> {
                HashMap<Object,Long> vals = new HashMap<Object,Long>();
                for(KeyValue kv : tr.getRange(multi.subspace(
                                        Tuple.from(index)).range())){
                    vals.put(multi.unpack(kv.getKey()).get(1),
                            getLong(kv.getValue()));
                }
                return vals;
            });
        }

        public static boolean isElement(TransactionContext tcx, final String index,
                                    final Object value){
            return tcx.run(tr -> {
                return tr.get(multi.subspace(
                        Tuple.from(index, value)).getKey()).get() != null;
            });
        }
    }
