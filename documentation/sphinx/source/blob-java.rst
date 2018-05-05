####
Blob
####

:doc:`Python <blob>` **Java**

Goal
====

Store binary large objects (blobs) in the database.

Challenge
=========

A blob is too large to be stored as a single key-value pair in FoundationDB. Values of key-value pairs are limited to 100 kB, and you’ll get the better performance by keeping them closer to 10 kb.

Explanation
===========

The core of the approach is simple: even though we can't store the blob in a single key-value pair, we can still store it by using multiple key-value pairs. We do this by splitting the blob into chunks and storing the chunks within a single subspace.

Ordering
========

Chunks are stored in order in adjacent key-value pairs. This approach allows the blob to be read with a single range read.

Pattern
=======

We create a subspace for a given blob. This subspace will hold the individual chunks of the blob as key-value pairs. In the key, we store a byte-offset into the blob; in the value, we store a range of the bytes starting at that offset. A constant ``CHUNK_SIZE`` establishes a maximum size for chunks of the blob (10 kb is a good starting point).

A simple transactional function to store a single blob with this strategy would look like:

.. code-block:: java

    static{
        blob = new Subspace(Tuple.from("B"));
    }
          
    public static void writeBlob(TransactionContext tcx, final String data){
        if(data.length() == 0) return;        
        tcx.run((Transaction tr) -> {
            int numChunks = (data.length() + CHUNK_SIZE - 1)/CHUNK_SIZE;
            int chunkSize = (data.length() + numChunks)/numChunks;
                
            for(int i = 0; i*chunkSize < data.length(); i++){
                int start = i*chunkSize;
                int end  = ((i+1)*chunkSize <= data.length()) ? ((i+1)*chunkSize) : (data.length());
                tr.set(blob.subspace(Tuple.from(start)).pack(),
                    Tuple.from(data.substring(start, end)).pack());
            }
 
            return null;
        });
    }

The blob can then be efficiently read with a single range read:

.. code-block:: java

    public static String readBlob(TransactionContext tcx){
        return tcx.run((Transaction tr) -> {
            StringBuilder value = new StringBuilder();
            for(KeyValue kv : tr.getRange(blob.range())){
                value.append(Tuple.fromBytes(kv.getValue()).getString(0));
            }
 
            return value.toString();
        });
    }

Extensions
==========

*A sparse representation for random reads and writes*

The simple model above lets us read and write a blob as a whole, but it can be extended to allow random reads and writes, allowing the blob to be partially accessed or streamed. At the same time, the representation can remain sparse, only consuming space for the data that is actually written.

For efficiency, we sometimes join chunks after a write to avoid the fragmentation than might result from small writes. Joining chunks is controlled by a constant ``CHUNK_SMALL`` (usually around 200 bytes), giving a lower limit for the sum of the size of adjacent chunks.