###############
Priority Queues
###############

:doc:`Python <priority-queues>` **Java**

Goal
====

Create a data structure for `priority queues <http://en.wikipedia.org/wiki/Priority_queue>`_ supporting operations for push, pop_min, peek_min, pop_max, and peek_max. You may find it helpful to review the :doc:`Queues <queues-java>` recipe before this one.

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

.. code-block:: java

  pq = new Subspace(Tuple.from("P"));

Push operations will construct a key-value pair with the subspace pq of the form

.. code-block:: java

 // (priority, count, random) = value

where priority is supplied by the client, count is an integer that increases by 1 for each item pushed with priority, and random is a randomly generated integer.

Items of the same priority that are pushed concurrently may occasionally be assigned the same count, but their keys will still be distinct and ordered (in this case, randomly). The count is derived by reading and incrementing the highest count previously used for a given priority. By using a snapshot read, we guarantee that pushing is conflict-free.

To implement this model, we need an efficient way of finding the first and last key in the queue. (The ordering of keys guarantees that these will always be the proper keys to pop or peek.) FoundationDB's range reads have limit and reverse options that let us accomplish this. We can find the first and last key-value pairs in the range of the pq subspace with:

.. code-block:: java

 tr.getRange(pq.subspace(Tuple.from(priority)).range(),1)      //  first
 tr.getRange(pq.subspace(Tuple.from(priority)).range(),1,true) //  last

Extensions
==========

*High-Contention Pop Operations*

To minimize conflicts during pop operations, we can use a staging technique to service the requests. If a pop operation doesn't initially succeed, it registers a pop request in a semi-ordered set of such requests. It then enters a retry loop in which it attempts to fulfill outstanding requests.

Code
====

Here's a basic implementation of the model:

.. code-block:: java

    import java.util.Random;

    public class MicroPriority {

        private static final FDB fdb;
        private static final Database db;
        private static final Subspace pq;
        private static final Random randno;

        static{
            fdb = FDB.selectAPIVersion(730);
            db = fdb.open();
            pq = new Subspace(Tuple.from("P"));

            randno = new Random();
        }

        public static void push(TransactionContext tcx, final Object value, final int priority){
            tcx.run((Transaction tr) -> {
                byte[] rands = new byte[20];
                randno.nextBytes(rands);
                tr.set(pq.subspace(Tuple.from(priority, nextCount(tr,priority),rands)).pack(),
                        Tuple.from(value).pack());
                return null;
            });
        }

        private static long nextCount(TransactionContext tcx, final int priority){
            return tcx.run((Transaction tr) -> {
                for(KeyValue kv : tr.snapshot().getRange(pq.subspace(Tuple.from(priority)).range(),1,true)){
                    return 1l + (long)pq.subspace(Tuple.from(priority)).unpack(kv.getKey()).get(0);
                }

                return 0l; // None previously with this priority.
            });
        }

        // Pop--assumes min priority queue..
        public static Object pop(TransactionContext tcx){
            return pop(tcx,false);
        }

        // Pop--allows for either max or min priority queue.
        public static Object pop(TransactionContext tcx, final boolean max){
            return tcx.run((Transaction tr) -> {
                for(KeyValue kv : tr.getRange(pq.range(), 1, max)){
                    tr.clear(kv.getKey());
                    return Tuple.fromBytes(kv.getValue()).get(0);
                }

                return null;
            });
        }

        // Peek--assumes min priority queue.
        public static Object peek(TransactionContext tcx){
            return peek(tcx,false);
        }

        // Peek--allows for either max or min priority queue.
        public static Object peek(TransactionContext tcx, final boolean max){
            return tcx.run((Transaction tr) -> {
                Range r = pq.range();
                for(KeyValue kv : tr.getRange(r.begin, r.end, 1, max)){
                    return Tuple.fromBytes(kv.getValue()).get(0);
                }

                return null;
            });
        }
    }
