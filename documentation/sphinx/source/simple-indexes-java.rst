##############
Simple Indexes
##############

:doc:`Python <simple-indexes>` **Java**

Goal
====

Add (one or more) indexes to allow efficient retrieval of data in multiple ways.

Challenges
==========

There are two big challenges with building indexes in a key-value store: 1) Storing the index so that multiple data elements “matching” the index read are returned as efficiently as possible, and 2) Keeping the indexes in sync with the data with concurrent readers and writers.

Strategy
========

By using the key ordering of FoundationDB, we can store indexes so that an index query can return multiple matches using a single efficient range read operation. By updating the data element and all of its associated indexes together within a single ACID transaction we can guarantee that the data and indexes stay in sync.

Pattern
=======

Let's say the primary copy of the data is stored with key-value pairs where the key has a tuple-structure consisting of a subspace and an ID:

.. code-block:: java

 // (main_subspace, ID) = value

This structure lets you lookup an “ID” easily and get its associated value. But, let’s say part of the value is a zipcode. You might be interested in all IDs that have a zipcode of 22182. You could answer that question, but it would require scanning every single ID. What we need to improve the efficiency is an “index on zipcode”.

An index is essentially another representation of the data, designed to be looked up in a different way:

.. code-block:: java

 // (index_subspace, zipcode, ID) = ''

To make the index, you store both the zipcode and the ID as parts of the key, but don’t store the whole value again. You also put the index in its own subspace to keep it separate from the primary data.

Now, to answer the question of what IDs match zipcode 22182, you can now restrict the search to all tuples matching ``(index_subspace, 22182, *)``. Happily, because of the way that ordered tuples get packed into ordered keys, all of the tuples matching this pattern can be retrieved using a single range-read operation on the database. This makes index queries blazing fast--requiring one database operation instead of a scan of the entire dataset.

You can use the pattern above in any ordered key-value store. But, as anyone who has tried it will tell you, the trick is dealing with maintaining these indexes during concurrent reads and writes. In most distributed databases, this is a nightmare of race conditions and extra logic to deal with the fact that, while the data and the indexes both get updated, they do not necessarily do so at the same time.

By contrast, FoundationDB’s ACID transactions completely handle the difficult concurrency problem automatically. This is accomplished by simply updating the data and the indexes in the same transaction. A good approach is to implement a transactional setter function that does nothing but perform a logical write to both the data record and its indexes. This approach keeps your code clean and makes it easier to add further indexes in the future.

Extensions
==========

Additional indexes
------------------

Of course, you can maintain as many indexes as you need. You are trading off write performance (and a bit of capacity usage) to speed up read performance. In general, you usually add indexes to support all of the access patterns that you actually use. For example, if we need fast access by both the "X" and "Y" properties, we could maintain three data representations (the main data plus two indexes):

.. code-block:: java

 // (main_subspace, ID) = value
 // (index_x, X, ID) = ''
 // (index_y, Y, ID) = ''

Covering indexes
----------------

In the above examples, the index gives you an entity ID or primary key with which the rest of the record can be retrieved. Sometimes might you want to retrieve the entire record from an index with a single read. In this case, you can store all data components in the key, possibly including the value.

.. code-block:: java

 // (main_subspace, ID) = value
 // (index_subspace, X, ID) = value

The obvious tradeoff is that you are storing another entire copy of the value.

Code
====

In this example, we’re storing user data based on user ID but sometimes need to retrieve users based on their zipcode. We use a transactional function to set user data and its index and another to retrieve data using the index.

.. code-block:: java

    import java.util.ArrayList;

    public class MicroIndexes {

        private static final FDB fdb;
        private static final Database db;
        private static final Subspace main;
        private static final Subspace index;

        static {
            fdb = FDB.selectAPIVersion(720);
            db = fdb.open();
            main = new Subspace(Tuple.from("user"));
            index = new Subspace(Tuple.from("zipcode_index"));
        }

        // TODO These three methods (setUser, getUser, and getUserIDsInRegion)
        // are all in the recipe book.
        public static void setUser(TransactionContext tcx, final String ID, final String name, final String zipcode){
            tcx.run(tr ->
                tr.set(main.pack(Tuple.from(ID,zipcode)), Tuple.from(name).pack());
                tr.set(index.pack(Tuple.from(zipcode,ID)), Tuple.from().pack());
                return null;
            });
        }

        // Normal lookup.
        public static String getUser(TransactionContext tcx, final String ID){
            return tcx.run(tr -> {
                for(KeyValue kv : tr.getRange(main.subspace(Tuple.from(ID)).range(), 1)){
                    // Return user with correct ID (if exists).
                    return Tuple.fromBytes(kv.getValue()).getString(0);
                }
                return "";
            });
        }

        // Index lookup.
        public static ArrayList<String> getUserIDsInRegion(TransactionContext tcx, final String zipcode){
            return tcx.run(tr -> {
                ArrayList<String> IDs = new ArrayList<String>();
                for(KeyValue kv : tr.getRange(index.subspace(Tuple.from(zipcode)).range())){
                    IDs.add(index.unpack(kv.getKey()).getString(1));
                }
                return IDs;
            });
        }
    }

That's just about all you need to create an index.
