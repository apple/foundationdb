####
Blob
####

**Python** :doc:`Java <blob-java>`

Goal
====

Store binary large objects (blobs) in the database.

Challenge
=========

A blob is too large to be stored as a single key-value pair in FoundationDB. Values of key-value pairs are limited to 100 kB, and you’ll get the better performance by keeping them closer to 10 kb.

Explanation
===========

The core of the approach is simple: even though we can't store the blob in a single key-value pair, we can still store it by using *multiple* key-value pairs. We do this by splitting the blob into chunks and storing the chunks within a single subspace.

Ordering
========

Chunks are stored in order in adjacent key-value pairs. This approach allows the blob to be read with a single range read.

Pattern
=======

We create a subspace for a given blob. This subspace will hold the individual chunks of the blob as key-value pairs. In the key, we store a byte-offset into the blob; in the value, we store a range of the bytes starting at that offset. A constant ``CHUNK_SIZE`` establishes a maximum size for chunks of the blob (10 kb is a good starting point).

A simple transactional function to store a single blob with this strategy would look like::

    CHUNK_SIZE = 10000
     
    blob_subspace = fdb.Subspace(('myblob',))
     
    @fdb.transactional
    def write_blob(tr, blob_data):
        length = len(blob_data)
        if not length: return
        chunks = [(n, n+CHUNK_SIZE) for n in range(0, length, CHUNK_SIZE)]
        for start, end in chunks:
            tr[blob_subspace[start]] = blob_data[start:end]

The blob can then be efficiently read with a single range read::

    @fdb.transactional
    def read_blob(tr):
        blob_data = ''
        for k, v in tr[blob_subspace.range()]:
            blob_data += v
        return blob_data

Extensions
==========

*A sparse representation for random reads and writes*

The simple model above lets us read and write a blob as a whole, but it can be extended to allow random reads and writes, allowing the blob to be partially accessed or streamed. At the same time, the representation can remain sparse, only consuming space for the data that is actually written.

For efficiency, we sometimes join chunks after a write to avoid the fragmentation than might result from small writes. Joining chunks is controlled by a constant ``CHUNK_SMALL`` (usually around 200 bytes), giving a lower limit for the sum of the size of adjacent chunks.