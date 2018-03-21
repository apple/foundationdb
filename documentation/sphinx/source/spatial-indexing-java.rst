################
Spatial Indexing
################

:doc:`Python <spatial-indexing>` **Java**

Goal
====

Create a spatial index for the database.

Challenge
=========

For a data set of labeled points in two-dimensional (2D) space, support the efficient retrieval of all points within an axis-aligned rectangular region.

Explanation
===========

To achieve good performance, you encode each point as a binary key whose structure allows queries to efficiently find points in a specified region. The encoding uses a `Z-order curve <http://en.wikipedia.org/wiki/Z-order_curve>`_ that maps 2D data to one-dimensional (1D) keys suitable for an ordered key-value store.

Ordering
========

FoundationDB stores binary keys in their natural order. Z-order curves map 2D points to binary keys in a manner that preserves proximity, in the sense that nearby points end up in nearby binary keys. This property allows range-based queries to be `efficiently computed from the keys <http://en.wikipedia.org/wiki/Z-order_curve#Use_with_one-dimensional_data_structures_for_range_searching>`_.

Pattern
=======

The indexer conceptually traverses the 2D space in a systematic order, tracing out a 1D curve. Data points are mapped to their position along the curve as they’re encountered. Each step in the traversal is encoded as a bit indicating its direction.

The spatial index exploits the proximity preservation of Z-order curves to store spatial data in the ordered key-value store and support spatial queries.

Given a point p represented as pair of coordinates (x, y), the Z-order curve lets us encode p as a binary key z. In other words, you have a pair of functions:

.. code-block:: java

    public long xyToZ(long[] p){        
        long x,y,z;
        x = p[0]; y = p[1];
        // Encode (x,y) as a binary key z.
        return z;
    }

    public long[] zToXy(long z){
        long[] p = new long[2];
        long x, y;
        // Decode z to a pair of coordinates (x,y).
        p[0] = x; p[1] = y;
        return p;
    }

The spatial index will use a pair of subspaces: one, ``z_label``, to give us efficient access to labels by point; the other, ``label_z``, to give us efficient access to points by label. Storing two access paths for each item is an example of an inverse index (see the pattern for Simple Indexes). You set both parts of the index in a single transactional function, as follows: 

.. code-block:: java

    public void setLocation(TransactionContext tcx, final String label, final long[] pos){
        tcx.run(tr -> {
            long z = xyToZ(pos);
            long previous;
            // Read labelZ.subspace(Tuple.from(label)) to find previous z.
            if(/* there is a previous z */){
                tr.clear(labelZ.pack(Tuple.from(label,previous)));
                tr.clear(zLabel.pack(Tuple.from(previous,label)));
            }
            tr.set(labelZ.pack(Tuple.from(label,z)),Tuple.from().pack());
            tr.set(zLabel.pack(Tuple.from(z,label)),Tuple.from().pack());
            return null;
        });
    }

This representation gives the building blocks you need to efficiently find all the points in a given rectangle.

Extensions
==========

Higher dimensions
-----------------

Z-order curves can be straightforwardly applied to points in three or more dimensions. As in two dimensions, each point will be mapped to a binary key determined by its position along the curve.

Richer non-spatial data
-----------------------

We've assumed that the labels of our data items are strings or a similar primitive data type, but you can easily extend the technique to richer data records in which the spatial coordinates are one component among several.
