######
Queues
######

:doc:`Python <queues>` **Java**

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

.. code-block:: java

    queue = new Subspace(Tuple.from("Q"));

As a first cut, we could store each item with a single key-value pair using increasing integer indexes for subsequent items:

.. code-block:: java

     // (queue, index) = value

However, this would leave concurrent enqueue operations vulnerable to conflicts. To minimize these conflicts, we can add a random integer to the key.

.. code-block:: java

     // (queue, index, random) = value

With this data model, items enqueued concurrently may be assigned the same index, but the keys as a whole will still be ordered (in this case, randomly). By using a :ref:`snapshot read <snapshot isolation>`, we guarantee that enqueuing will be conflict-free.

To implement this model, we need an efficient way of finding the first and last index presently in use. FoundationDB's range reads have limit and reverse options that let us accomplish this. We can find the first and last key-value pairs in the range of the subspace with:

.. code-block:: java

    tr.getRange(queue.range(), 1)       // first
    tr.getRange(queue.range(), 1, true) // last

Extensions
==========

*High-Contention Dequeue Operations*

To minimize conflicts during dequeue operations, we can use a staging technique to service the requests. If a dequeue operation doesn't initially succeed, it registers a dequeue request in a semi-ordered set of such requests. It then enters a retry loop in which it attempts to fulfill outstanding requests.

Code
====

The following is a simple implementation of the basic pattern:

.. code-block:: java

    import java.util.Random

    public class MicroQueue {

        private static final FDB fdb;
        private static final Database db;
        private static final Subspace queue;
        private static final Random randno;

        static{
            fdb = FDB.selectAPIVersion(730);
            db = fdb.open();
            queue = new Subspace(Tuple.from("Q"));
            randno = new Random();
        }

        // Remove the top element from the queue.
        public static Object dequeue(TransactionContext tcx){
            // Remove from the top of the queue.
            return tcx.run((Transaction tr) -> {
                final KeyValue item = firstItem(tr);
                if(item == null){
                    return null;
                }

                tr.clear(item.getKey());
                // Return the old value.
                return Tuple.fromBytes(item.getValue()).get(0);
            });

        }

        // Add an element to the queue.
        public static void enqueue(TransactionContext tcx, final Object value){
            tcx.run((Transaction tr) -> {
                byte[] rands = new byte[20];
                randno.nextBytes(rands); // Create random seed to avoid conflicts.
                tr.set(queue.subspace(Tuple.from(lastIndex(tr)+1, rands)).pack(),
                        Tuple.from(value).pack());

                return null;
            });
        }

        // Get the top element of the queue.
        private static KeyValue firstItem(TransactionContext tcx){
            return tcx.run((Transaction tr) -> {
                for(KeyValue kv : tr.getRange(queue.range(), 1)){
                    return kv;
                }

                return null; // Empty queue. Should never be reached.
            });
        }

        // Get the last index in the queue.
        private static long lastIndex(TransactionContext tcx){
            return tcx.run((Transaction tr) -> {
                for(KeyValue kv : tr.snapshot().getRange(queue.range(), 1, true)){
                    return (long)queue.unpack(kv.getKey()).get(0);
                }
                return 0l;
            });
        }
    }
