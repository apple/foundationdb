######################
Hierarchical Documents
######################

**Python** :doc:`Java <hierarchical-documents-java>`

Goal
====

Create a representation for hierarchical `documents <http://en.wikipedia.org/wiki/Document-oriented_database>`_.

Challenge
=========

Support efficient storage and retrieval of documents, both as a whole and by subdocuments specified by paths.

Explanation
===========

A hierarchical document has a tree-like structure, with the document ID as the root. We'll map the hierarchy to a list of tuples in which each tuple corresponds to a path from the root to a leaf. These tuples will form keys, so each leaf is indexed by the path leading to it.

Ordering
========

Because each tuple represents a path from the document root to a leaf, the lexicographic ordering of tuples guarantees that adjacent paths will be stored in adjacent keys. Each tuple prefix will correspond to a subdocument that can be retrieved using a prefix range read. Likewise, a range read using the root as a prefix will retrieve the entire document.

Pattern
=======

A document will consist of a dictionary whose values may be simple data types (e.g., integers or strings), lists, or (nested) dictionaries. Each document will be stored under a unique ID. If a document ID has not already been supplied, we randomly generate one.

We convert the document to a list of tuples representing each path from the root to a leaf. Each tuple is used to construct a composite key within a subspace. The document ID becomes the first element after the subspace prefix, followed by the remainder of the path. We store the leaf (the last element of the tuple) as the value, which enables storage of larger data sizes (see :ref:`Key and value sizes <data-modeling-performance-guidelines>`).

If we're given a serialized JSON object to start with, we just deserialize it before converting it to tuples. To distinguish the list elements in the document (a.k.a. JSON arrays) from dictionary elements and preserve the order of the lists, we include the index of each list element before it in the tuple.

We can retrieve any subdocument based on the partial path to its root. The partial path will just be a tuple that the query function uses as a key prefix for a range read. The retrieved data will be a list of tuples. The final step before returning the data is to convert it back to a document.

Extensions
==========

Indexing
--------

We could extend the document model to allow selective indexing of keys or values at specified locations with a document.

Query language
--------------

We could extend the document to support more powerful query capabilities, either with query functions or a full query language. Either would be designed to take advantage of existing indexes.

Code
====

Here’s a basic implementation of the recipe.
::

    import itertools
    import json
    import random
     
    doc_space = fdb.Subspace(('D',))
     
    EMPTY_OBJECT = -2
    EMPTY_ARRAY = -1
     
    def to_tuples(item):
        if item == {}:
            return [(EMPTY_OBJECT, None)]
        elif item == []:
            return [(EMPTY_ARRAY, None)]
        elif type(item) == dict:
            return [(k,) + sub for k, v in item.iteritems() for sub in to_tuples(v)]
        elif type(item) == list:
            return [(k,) + sub for k, v in enumerate(item) for sub in to_tuples(v)]
        else:
            return [(item,)]
     
    def from_tuples(tuples):
        if not tuples: return {}
        first = tuples[0]  # Determine kind of object from first tuple
        if len(first) == 1: return first[0]  # Primitive value
        if first == (EMPTY_OBJECT,None): return {}
        if first == (EMPTY_ARRAY, None): return []
        # For an object or array, we need to group the tuples by their first element
        groups = [list(g) for k, g in itertools.groupby(tuples, lambda t:t[0])]
        if first[0] == 0:   # array
            return [from_tuples([t[1:] for t in g]) for g in groups]
        else:    # object
            return dict((g[0][0], from_tuples([t[1:] for t in g])) for g in groups)
     
    @fdb.transactional
    def insert_doc(tr, doc):
        if type(doc) == str:
            doc = json.loads(doc)
        if not 'doc_id' in doc:
            new_id = _get_new_id(tr)
            doc['doc_id'] = new_id
        for tup in to_tuples( doc ):
            tr[doc_space.pack((doc['doc_id'],) + tup[:-1])] = fdb.tuple.pack((tup[-1],))
        return doc['doc_id']
     
    @fdb.transactional
    def _get_new_id(tr):
        found = False
        while (not found):
            new_id = random.randint(0, 100000000)
            found = True
            for _ in tr[doc_space[new_id].range()]:
                found = False
                break
        return new_id
     
    @fdb.transactional
    def get_doc(tr, doc_id, prefix=()):
        v = tr[doc_space.pack((doc_id,) + prefix)]
        if v.present():
            return from_tuples([prefix + fdb.tuple.unpack(v)])
        else:
            return from_tuples([doc_space.unpack(k)[1:] + fdb.tuple.unpack(v)
                                for k, v in tr[doc_space.range((doc_id,)+prefix)]])
