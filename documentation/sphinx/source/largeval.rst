###############################
Managing Large Values and Blobs
###############################

This tutorial illustrates techniques for storing and managing large values in FoundationDB. We'll look at using the blob (binary large object) layer, which provides a simple interface for storing unstructured data. We'll be drawing on :doc:`data-modeling` and :doc:`api-python`, so you should take a look at those documents if you're not familiar with them.

For an introductory tutorial that begins with "Hello world" and explains the basic concepts used in FoundationDB, take a look at our :doc:`class scheduling tutorial <class-scheduling>`.

Although we'll be using Python, the concepts in this tutorial are also applicable to the other :doc:`languages <api-reference>` supported by FoundationDB.

.. _largeval-modeling:

Modeling large values
=====================

For key-value pairs stored in FoundationDB, values are limited to a size of 100 kB (see :ref:`Known Limitations<large-keys-and-values>`). Furthermore, you'll usually get the best performance by keeping value sizes below 10 kb, as discussed in our :ref:`performance guidelines<data-modeling-performance-guidelines>`.

.. _largeval-splitting:

Splitting structured values
---------------------------

These factors lead to an obvious question: what should you do if your first cut at a data model results in values that are larger than those allowed by the above guidelines?

The answer depends on the nature and size of your values. If your values have some internal structure, consider revising your data model to split the values across multiple keys. For example, suppose you'd like to store a serialized JSON object. Instead of storing the object as the value of a single key, you could construct a key for each path in the object, as described for :ref:`documents <data-modeling-documents>`.

.. note:: In general, you should consider splitting your values if their sizes are above 10kb, or if they are above 1kb and you only use a part of each value after reading it.