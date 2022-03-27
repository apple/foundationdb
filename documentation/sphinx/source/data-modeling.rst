.. default-domain:: py
.. default-domain:: py
.. highlight:: python
.. module:: fdb

.. Required substitutions for api-common.rst.inc

.. |database-type| replace:: ``Database``
.. |database-class| replace:: ``Database``
.. |database-auto| replace:: FIXME
.. |tenant-type| replace:: FIXME
.. |transaction-class| replace:: ``Transaction``
.. |get-key-func| replace:: get_key()
.. |get-range-func| replace:: get_range()
.. |commit-func| replace:: FIXME
.. |open-func| replace:: FIXME
.. |set-cluster-file-func| replace:: FIXME
.. |set-local-address-func| replace:: FIXME
.. |on-error-func| replace:: FIXME
.. |null-type| replace:: FIXME
.. |error-type| replace:: FIXME
.. |error-raise-type| replace:: FIXME
.. |reset-func-name| replace:: FIXME
.. |reset-func| replace:: FIXME
.. |cancel-func| replace:: FIXME
.. |read-your-writes-disable-option| replace:: FIXME
.. |future-cancel| replace:: FIXME
.. |max-watches-database-option| replace:: FIXME
.. |future-type-string| replace:: FIXME
.. |lazy-iterator-object| replace:: FIXME
.. |key-meth| replace:: FIXME
.. |directory-subspace| replace:: FIXME
.. |directory-layer| replace:: FIXME
.. |subspace| replace:: FIXME
.. |subspace-api| replace:: FIXME
.. |as-foundationdb-key| replace:: FIXME
.. |as-foundationdb-value| replace:: FIXME
.. |tuple-layer| replace:: FIXME
.. |dir-path-type| replace:: FIXME
.. |node-subspace| replace:: FIXME
.. |content-subspace| replace:: FIXME
.. |allow-manual-prefixes| replace:: FIXME
.. |retry-limit-transaction-option| replace:: FIXME
.. |timeout-transaction-option| replace:: FIXME
.. |max-retry-delay-transaction-option| replace:: FIXME
.. |size-limit-transaction-option| replace:: FIXME
.. |snapshot-ryw-enable-transaction-option| replace:: FIXME
.. |snapshot-ryw-disable-transaction-option| replace:: FIXME
.. |snapshot-ryw-enable-database-option| replace:: FIXME
.. |snapshot-ryw-disable-database-option| replace:: FIXME
.. |retry-limit-database-option| replace:: FIXME
.. |max-retry-delay-database-option| replace:: FIXME
.. |transaction-size-limit-database-option| replace:: FIXME
.. |timeout-database-option| replace:: FIXME
.. |causal-read-risky-database-option| replace:: FIXME
.. |causal-read-risky-transaction-option| replace:: FIXME
.. |transaction-logging-max-field-length-transaction-option| replace:: FIXME
.. |transaction-logging-max-field-length-database-option| replace:: FIXME

.. include:: api-common.rst.inc

#############
Data Modeling
#############

FoundationDB's core provides a simple data model coupled with powerful transactions. This combination allows building richer data models and libraries that inherit the scalability, performance, and integrity of the database. The goal of data modeling is to design a mapping of data to keys and values that enables effective storage and retrieval. Good decisions will yield an extensible, efficient abstraction. This document covers the fundamentals of data modeling with FoundationDB.

* For general guidance on application development using FoundationDB, see :doc:`developer-guide`. 
* For detailed API documentation specific to each supported language, see :doc:`api-reference`.

The core data model
===================

FoundationDB's core data model is an ordered key-value store. Also known as an ordered associative array, map, or dictionary, this is a common data structure composed of a collection of key-value pairs in which all keys are unique. Starting with this simple model, an application can create higher-level data models by mapping their elements to individual keys and values.

In FoundationDB, both keys and values are simple byte strings. Apart from storage and retrieval, the database does not interpret or depend on the content of values. In contrast, keys are treated as members of a total order, the lexicographic order over the underlying bytes, in which keys are sorted by each byte in order. For example:

* ``'0'`` is sorted before ``'1'``
* ``'apple'`` is sorted before ``'banana'``
* ``'apple'`` is sorted before ``'apple123'``
* keys starting with ``'mytable\'`` are sorted together (e.g. ``'mytable\row1'``, ``'mytable\row2'``, ...)

The ordering of keys is especially relevant for range operations. An application should structure keys to produce an ordering that allows efficient data retrieval with range reads.

.. _encoding-data-types:

Encoding data types
===================

Because keys and values in FoundationDB are always byte strings, an application developer must serialize other data types (e.g., integers, floats, arrays) before storing them in the database. For values, the main concerns for serialization are simply CPU and space efficiency. For keys, there's an additional consideration: it's often important for keys to preserve the order of the data types (whether primitive or composite) they encode. For example:

Integers
--------

* The standard tuple layer provides an order-preserving, signed, variable length encoding.
* For positive integers, a big-endian fixed length encoding is order-preserving.
* For signed integers, a big-endian fixed length two's-complement encoding with the most significant (sign) bit inverted is order-preserving.

Unicode strings
---------------

* For unicode strings ordered lexicographically by unicode code point, use UTF-8 encoding.  (This approach is used by the tuple layer.)
* For unicode strings ordered by a particular collation (for example, a case insensitive ordering for a particular language), use an appropriate string collation transformation and then apply UTF-8 encoding.  Internationalization or "locale" libraries in most environments and programming languages provide a string collation transformation, for example `C <http://pubs.opengroup.org/onlinepubs/7908799/xsh/wcsxfrm.html>`_, `C++ <http://www.cplusplus.com/reference/std/locale/collate/transform/>`_, `Python <http://docs.python.org/py3k/library/locale.html#locale.strxfrm>`_, `Ruby <https://github.com/ninjudd/icunicode#readme>`_, `Java <http://docs.oracle.com/javase/1.5.0/docs/api/java/text/Collator.html#getCollationKey(java.lang.String)>`_, the `ICU <http://icu-project.org/apiref/icu4c/classCollator.html#ae524fd43a06d4429e2c76bef35874d4c>`_ library, etc.  Usually the output of this function is a unicode string, which needs to be further encoded in a code-point ordered encoding such as UTF-8 to get a byte string.

Floating point numbers
----------------------

The tuple layer provides an order-preserving, signed, fixed length encoding for both single- and double-precision floating point
numbers based off of the IEEE big-endian encoding with some modifications to make it correctly ordered. Within this representation,
-0 and +0 are not equal and negative NaN values will sort before all non-NaN values and positive NaN values will sort after
all non-NaN values. Otherwise, the representation is consistent with the mathematical ordering.

Composite types
---------------

An application's data is often represented using composite types, such as structures or records with multiple fields. It's very useful for the application to use *composite* keys to store such data. In FoundationDB, composite keys can be conveniently represented as *tuples* that are mapped to individual keys for storage.

.. note:: For the purpose of illustration, we'll use the FoundationDB's Python language binding, including the :py:func:`@fdb.transactional <fdb.transactional>` decorator described in :doc:`api-python`. The design patterns illustrated are applicable to all of the :doc:`languages <api-reference>` supported by FoundationDB.

.. _data-modeling-tuples:

Tuples
======

FoundationDB's keys are ordered, making tuples a particularly useful tool for data modeling. FoundationDB provides a :ref:`tuple layer <api-python-tuple-layer>` (available in each language binding) that encodes tuples into keys. This layer lets you store data using a tuple like ``(state, county)`` as a key. Later, you can perform reads using a prefix like ``(state,)``. The layer works by preserving the natural ordering of the tuples.

You could implement a naive encoding of tuples of strings into keys by using a tab character as a simple delimiter. You could do this with the following Python code::

    def tuple_to_key_with_tab(tup):
        return '\t'.join(str(i) for i in tup)

    # Example: Order first by state, then by county
    @fdb.transactional
    def set_county_population(tr, state, county, pop):
        tr[tuple_to_key_with_tab((state, county))] = str(pop)

In this example, population figures for the United States are stored using keys formed from the tuple of state and county.

Of course, this encoding would only work if all the bytes in the individual keys in the tuple were greater than the delimiter byte. Therefore, FoundationDB's built-in tuple layer implements a more robust encoding supporting elements of various data types: byte strings, unicode strings, signed integers, floating-point numbers, booleans, UUIDs, null values, and nested tuples.

.. note:: The tuple layer's encoding is compatible between languages, although some languages are limited in what data types they support. For language-specific documentation of the tuple layer, see the corresponding :doc:`api-reference` documentation.

Because of its ordering of keys, FoundationDB supports efficient range reads on any set of keys that share a prefix. The tuple layer preserves the ordering of tuples sorted by element from left to right; as a result, the leftmost elements of a tuple will always represent a prefix in keyspace and can be used for range reads. A basic principle of data modeling with the tuple layer is to order tuple elements to facilitate such range reads. The examples below illustrate this principle.

Sometimes data attributes will have a natural order of containment imposed by your domain. A common example is geographic attributes, such as state and county in the Unites States. By constructing keys from tuples of the form ``(state, county)``, where state is the first tuple element, all data for states will be stored in an adjacent range of keys. This ordering allows you to retrieve the populations for all counties in a given state with a single range read. You could use the tuple layer with the following functions::

    @fdb.transactional
    def set_county_population(tr, state, county, pop):
        tr[fdb.tuple.pack((state, county))] = str(pop)

    @fdb.transactional
    def get_county_populations_in_state(tr, state):
        return [int(pop) for k, pop in tr[fdb.tuple.range((state,))]]

Date/timestamp attributes form another example with a natural containment order. If you have attributes of year, month, day, hour, minute, and/or second, you can order them from larger to smaller units in your keys. As a result, you'll be able to retrieve temporally contiguous data with range reads, as above.

A few simple models
===================

Let's begin with a few examples of simple data models built on tuples with :ref:`subspaces <developer-guide-sub-keyspaces>`.

Arrays
------

You can easily map arrays to the key-value store using tuples. To model a named, one-dimensional array, you can construct a key for each array element using the array name as the subspace and the array index as the second tuple element.

For example, suppose you have a ``'temps2012'`` array containing a year's worth of daily temperature averages. The temperatures are indexed by an integer ranging from 1 to 365 representing the day. Your keys would then be constructed from tuples of the form ``('temps2012', day)``. 

To set and get array elements with this technique, you can use Python functions such as::

    @fdb.transactional
    def array_set(tr, array_space, index, value):
        tr[array_space[index]] = str(value)

    @fdb.transactional
    def array_get(tr, array_space, index):
        return tr[array_space[index]]

    temp_array = Subspace(('temps2012',))

    @fdb.transactional
    def add_temp(tr, day, temp):
        array_set(tr, temp_array, day, temp)

    @fdb.transactional
    def get_temp(tr, day):
        val = array_get(tr, temp_array, day)
        if val.present():
            return int(val)
        else:
            return None

This approach has a few nice properties:

* It can be extended to multidimensional arrays simply by adding additional array indexes to the tuples.
* Unassigned elements consume no storage, so sparse arrays are stored efficiently.

The tuple layer makes these properties easy to achieve, and most well-designed data models using tuples will share them.

An array can only have a single value for each index. Likewise, the key-value store can only have a single value for each key. The simple mapping above takes advantage of this correspondence to store the array value as a physical value. In contrast, some data structures are designed to store multiple values. In these cases, data models can store the logical values within the key itself, as illustrated next.

Multimaps
---------
A multimap is a generalization of an associative array in which each key may be associated with multiple values. Multimaps are often implemented as associative arrays in which the values are sets rather than primitive data types.

Suppose you have a multimap that records student enrollment in classes, with students as keys and classes as values. Each student can be enrolled in more than one class, so you need to map the key-value pairs of the multimap (with their multiple values) to the database. 

A simple approach is to use the multimap name (say, ``'enroll'``) as the subspace and construct a key from a tuple of the form ``('enroll', student, class_name)`` for each class in which a student is enrolled. Each class will generate a unique key, allowing as many classes as needed. Moreover, all the data in the multimap will be captured in the key, so you can just use an empty string for its value. Using this approach, you can add a class for a student or get all the student's classes with the Python functions::

    @fdb.transactional
    def multi_set(tr, multi_space, index, value):
        tr[multi_space.pack((index, value))] = ''

    @fdb.transactional
    def multi_get(tr, multi_space, index):
        pairs = tr[multi_space.range((index,))]
        return [multi_space.unpack(k)[-1] for k, v in pairs]

    @fdb.transactional
    def multi_is_element(tr, multi_space, index, value):
        val = tr[multi_space.pack((index, value))]
        return val.present()

    enroll_space = Subspace(('enroll',))

    @fdb.transactional
    def add_class(tr, student, class_name):
        multi_set(tr, enroll_space, student, class_name)

    @fdb.transactional
    def get_classes(tr, student):
        return multi_get(tr, enroll_space, student)

The ``range()`` method in :py:func:`multi_get` returns all keys in the subspace that encode tuples with the specified tuple as a prefix. The ``[-1]`` extracts the last element of the tuple unpacked from the key, which in this case will encode a class.

As this model for multimaps illustrates, data that is treated as a value at one level may be mapped to a key in the database. (The reverse may also occur, as shown in the discussion of indirection below.) Data modeling in FoundationDB is not dictated by how your data is represented in your programming language.

.. _data-modeling-tables:

Tables
------

You can easily use tuples to store data in tabular form with rows and columns. The simplest data model for a table is to make each cell in the table a key-value pair. To do this, you construct a key from a tuple containing the row and column identifiers. As with the array model, unassigned cells in tables constructed using this technique will consume no storage, so sparse tables can be stored efficiently. As a result, a table can safely have a very large number of columns.

You can make your model row-oriented or column-oriented by placing either the row or column first in the tuple, respectively. Because the lexicographic order sorts tuple elements from left to right, access is optimized for the element placed first. Placing the row first makes it efficient to read all the cells in a particular row; reversing the order makes reading a column more efficient.

Using the table name as the subspace, we could implement the common row-oriented version in Python as follows::

    @fdb.transactional
    def table_set_cell(tr, table_space, row, column, value):
        tr[table_space.pack((row, column))] = str(value)

    @fdb.transactional
    def table_get_cell(tr, table_space, row, column):
        return tr[table_space.pack((row, column))]
    
    @fdb.transactional
    def table_set_row(tr, table_space, row, cols):
        del tr[table_space.range((row,))]
        for c, v in cols.iteritems():
            table_set_cell(tr, table_space, row, c, v)

    @fdb.transactional
    def table_get_row(tr, table_space, row):
        cols = {}
        for k, v in tr[table_space.range((row,))]:
            _, c = table_space.unpack(k)
            cols[c] = v
        return cols


Versionstamps
-------------

A common data model is to index your data with a sequencing prefix to allow log scans or tails of recent data. This index requires a unique, monotonically increasing value, like an AUTO_INCREMENT PRIMARY KEY in SQL. This could be implemented at the client level by reading the value for conflict checks before every increment. A better solution is the versionstamp, which can be generated at commit-time with no read conflict ranges, providing a unique sequence ID in a single conflict-free write.

Versioning commits provides FoundationDB with MVCC guarantees and transactional integrity. Versionstamps write the transaction's commit version as a value to an arbitrary key as part of the same transaction, allowing the client to leverage the version's unique and serial properties.  Because the versionstamp is generated at commit-time, the versionstamped key cannot be read in the same transaction that it is written, and the versionstamp's value will be unknown until the transaction is committed. After the transaction is committed, the versionstamp can be obtained.

The versionstamp guarantees uniqueness and monotonically increasing values for the entire lifetime of a single FDB cluster. This is even true if the cluster is restored from a backup, as a restored cluster will begin at a higher version than when the backup was taken. Special care must be taken when moving data between two FoundationDB clusters containing versionstamps, as the differing cluster versions might break the monotonicity.

There are two concepts of versionstamp depending on your context. At the fdb_c client level, or any binding outside of the Tuple layer, the 'versionstamp' is 10 bytes: the transaction's commit version (8 bytes) and transaction batch order (2 bytes). The user can manually add 2 additional bytes to provide application level ordering. The tuple layer provides a useful api for getting and setting both the 10 byte system version and the 2 byte user version. In the context of the Tuple layer, the 'versionstamp' is all 12 bytes. For examples on how to use the versionstamp in the python binding, see the :doc:`api-python` documentation.

.. _data-modeling-entity-relationship:

Entity-relationship models
==========================

Entity-relationship models are often used to describe a database at various levels of abstraction. In this methodology, a *logical* data model consisting of entities, attributes, and relationships is defined before mapping it to a *physical* data models specifying keys and other implementation features. Entity-relationship models can be easily modeled in FoundationDB using tuples. 

Attributes
----------

Suppose you're storing entity-relationship data for users in an ``'ER'`` subspace. You might identify each entity with a unique identifier and define a key for each attribute with the tuple ``('ER', entity_ID, attribute)``. You could then store the user's region using the Python functions::

    ER_space = Subspace(('ER',))

    @fdb.transactional
    def add_attribute_value(tr, entity_ID, attribute, value):
        tr[ER_space.pack((entity_ID, attribute))] = str(value)

    @fdb.transactional
    def get_attribute_value(tr, entity_ID, attribute):
        return tr[ER_space.pack((entity_ID, attribute))]

    @fdb.transactional
    def add_user_region(tr, user_ID, region):
        add_attribute_value(tr, user_ID, 'region', region)

Relationships
-------------

Using the pattern we saw above with multimaps, you can store relationships and related entities as an element of the key and use an empty string as the physical value. Suppose your users can belong to one or more groups. To add a user to a group or retrieve all groups to which a user belongs, you can use the Python functions::

    @fdb.transactional
    def add_relationship(tr, relationship, primary_key, foreign_key):
        tr[ER_space.pack((relationship, primary_key, foreign_key))] = ''

    @fdb.transactional
    def get_relationships(tr, relationship):
        return [ER_space.unpack(k)[1:] 
                for k, v in tr.get_range_startswith(ER_space.pack((relationship,)), 
                                            streaming_mode=fdb.StreamingMode.want_all)]

    @fdb.transactional
    def get_related_entities(tr, relationship, primary_key):
        items = tr[ER_space.range((relationship, primary_key))]
        return [ER_space.unpack(k)[-1] for k, v in items]

    @fdb.transactional
    def is_related_entity(tr, relationship, primary_key, foreign_key):
        return tr[ER_space.pack((relationship, primary_key, foreign_key))].present()

    @fdb.transactional
    def add_user_to_group(tr, user_ID, group_name):
        add_relationship(tr, 'belongs_to', user_ID, group_name)

    @fdb.transactional
    def get_users_groups(tr, user_ID):
       return get_related_entities(tr, 'belongs_to', user_ID)

You can extend this code by adding indexes for the related entities (see below) and enforcement of relationship cardinalities (one-to-many, etc.).

.. _data-modeling-indexes:

Indexes
=======

A common technique is to store the same data in different ways to allow efficient retrieval for multiple use cases, creating indexes. This technique is especially useful when there are many more reads than writes. For example, you may find it most convenient to store user data based on ``user_ID`` but sometimes need to retrieve users based on their region. An index allows this retrieval to be performed efficiently.

An index can have a very simple tuple structure consisting of an unique subspace, the relationship being indexed, and a value: ``(subspace_for_index, relationship, value)``. Placing the relationship before the value is what allows efficient retrieval of all the associated values with a single range read.

With FoundationDB's transactions, you can easily build an index and guarantee that it stays in sync with the data: just update the index in the same transaction that updates the data.


For example, suppose you'd like to add an index to efficiently look up users by region. You can augment the Python function :py:func:`add_user` with the index and add a new function for retrieval::

    user_space = Subspace(('user',))
    region_index = Subspace(('region_idx',))

    @fdb.transactional
    def add_user(tr, user_ID, name, region):
        tr[user_space.pack((user_ID, region))] = str(name)
        tr[region_index.pack((region, user_ID))] = ''

    @fdb.transactional
    def get_users_in_region(tr, region):
        items = tr[region_index.range((region,))]
        return [region_index.unpack(k)[-1] for k, v in items]

To apply this technique to a real use case, you would add code to your update transaction to delete outdated index entries. Note that this approach lets you add as many indexes as desired by updating all the indexes in the same transaction.

Composite models
================

Most of the techniques we've discussed can be freely combined. Let's look at adding indexes to our basic data model for tables.

We've already seen a way to store tabular data in a row-oriented order using table names as subspaces. You can extend this model by simultaneously storing the table in both row-oriented and column-oriented layouts, allowing efficient retrieval of either an entire row or an entire column. We'll create nested subspaces for the indexes using the subscripting syntax we saw above::

    table_space = Subspace(('table',))
    row_index = table_space['row_idx']
    col_index = table_space['col_idx']

    @fdb.transactional
    def table_set_cell(tr, row_index, col_index, row, column, value):
        tr[row_index.pack((row, column))] = str(value)
        tr[col_index.pack((column, row))] = str(value)

    @fdb.transactional
    def table_get_cell(tr, row_index, row, column):
        return tr[row_index.pack((row, column))]

    @fdb.transactional
    def table_get_row(tr, row_index, row):
        cols = {}
        for k, v in tr[row_index.range((row,))]:
            r, c = row_index.unpack(k)
            cols[c] = v
        return cols

    @fdb.transactional
    def table_get_col(tr, col_index, col):
        rows = {}
        for k, v in tr[col_index.range((col,))]:
            c, r = col_index.unpack(k)
            rows[r] = v
        return rows


.. _data-modeling-hierarchies:

Hierarchies
===========

Many applications work with hierarchical data represented by nested dictionaries or similar composite data types. Such data is often serialized to or deserialized from a format such as JSON or XML. Looking at a hierarchical object as a tree, you can use a tuple to represent the full path to each leaf (sometimes called  a "materialized path"). By storing each full path as a key, you get an index for each leaf. FoundationDB can then efficiently retrieve any individual piece of data or entire sub-tree.

For example, suppose you have hierarchical data such as the following nested dictionaries and lists::

    {'user': {  'jones': 
                {   'friendOf': 'smith',
                    'group': ['sales', 'service']},
                'smith': 
                {   'friendOf': 'jones',
                    'group': ['dev', 'research']}}}

To distinguish the list elements from dictionary elements and preserve the order of the lists, you can just include the index of each list element before it in the tuple. Using this technique, the data above would be converted to the following tuples::

    [('user', 'jones', 'friendOf', 'smith'), 
    ('user', 'jones', 'group', 0, 'sales'), 
    ('user', 'jones', 'group', 1, 'service'), 
    ('user', 'smith', 'friendOf', 'jones'), 
    ('user', 'smith', 'group', 0, 'dev'), 
    ('user', 'smith', 'group', 1, 'research')]

Suppose you'd like to use this representation to implement a nested keyspace, i.e., a key-value store in which values can themselves be nested dictionaries or lists. Your application receives a stream of serialized JSON objects in which different objects may contain data about the same entities, so you'd like to store the data in a common nested keyspace.

You can deserialize the data using Python's standard ``json`` module, generate the corresponding set of paths as tuples, and store each tuple in a ``'hier'`` subspace::

    import json, itertools

    hier_space = Subspace(('hier',))

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

    @fdb.transactional
    def insert_hier(tr, hier):
        if type(hier) == str:
            hier = json.loads(hier)
        for tup in to_tuples(hier):
            tr[hier_space.pack(tup)] = ''

You can then retrieve any sub-tree from the nested keyspace by giving the partial path to its root. The partial path will just be a tuple that your query function uses as a key prefix for a range read. For example, to retrieve the data for ``'smith'`` from the hierarchy above, you would use ``('user', 'smith')``.

The retrieved data will be a list of tuples. The final step before returning the data is to convert it back to a nested data structure::

    def from_tuples(tuples):
        first = tuples[0]  # The first tuple will tell us what kind of object we have

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
    def get_sub_hier(tr, prefix):
        return from_tuples([hier_space.unpack(k)
                            for k, v in tr[hier_space.range(prefix)]])

.. _data-modeling-documents:

Documents
=========

Suppose you'd like to use the above representation to implement a simple document-oriented data model. As before, your application receives serialized data in JSON, only now you'd like to store each JSON object as an independent document. To do so, you just need to ensure that each tuple created for that object is stored with a unique identifier for the document. If a ``doc_id`` has not already been supplied, you can randomly generate one. 

To store a path, you can construct a composite key in a ``'doc'`` subspace, with the ``doc_id`` as the next element, followed by the remainder of the path. You can store the leaf (the last element of the tuple) as the value, which enables storage of larger data sizes (see :ref:`data-modeling-performance-guidelines`)::

    import random

    doc_space = Subspace(('doc',))

    @fdb.transactional
    def insert_doc(tr, doc):
        if type(doc) == str:
            doc = json.loads(doc)
        if not 'doc_id' in doc:
            doc['doc_id'] = random.randint(0, 100000000)
        for tup in to_tuples( doc ):
            tr[doc_space.pack((doc['doc_id'],) + tup[:-1])] = fdb.tuple.pack((tup[-1],))
        return doc['doc_id']

    @fdb.transactional
    def get_doc(tr, doc_id):
        return from_tuples([doc_space.unpack(k)[1:] + fdb.tuple.unpack(v)
                            for k, v in tr[doc_space.range((doc_id,))]])

.. _data-modeling-indirection:

Indirection
===========
It is sometimes beneficial to add a level of indirection to a data model. Instead of using key-value pairs to directly store application data, you can instead store a reference to that data. This approach can be used to model any data structure that would normally use references. You just need to perform any modifications to the data structure in a transaction that leaves it in a consistent state.

Suppose you want to maintain data in a singly linked list. The application data can use a tuple structure like those of single-valued relationships. Links will be similar but will use node identifiers as their values. Here is an example of removing the next node from the list::

    node_space = Subspace(('node',))

    @fdb.transactional
    def remove_next_node(tr, node_ID):
        next_ID = tr[node_space.pack((node_ID, 'next'))]
        if next_ID != '':
            next_next_ID = tr[node_space.pack((next_ID, 'next'))]
            tr[node_space.pack((node_ID, 'next'))] = next_next_ID
            del tr[node_space.range((next_ID,))]

FoundationDB's transactional guarantees ensure that, even when multiple clients are concurrently modifying the same linked list, the structure will be maintained in a consistent way.

.. _data-modeling-performance-guidelines:

Key and value sizes
===================

How you map your application data to keys and values can have a dramatic impact on performance. Below are some guidelines to consider as you design a data model. (For more general discussion of performance considerations, see :ref:`developer-guide-peformance-considerations`.)

* Structure keys so that range reads can efficiently retrieve the most frequently accessed data.

  * If you perform a range read that is, in total, much more than 1 kB, try to restrict your range as much as you can while still retrieving the needed data.

* Structure keys so that no single key needs to be updated too frequently, which can cause transaction conflicts.

  * If a key is updated more than 10-100 times per second, try to split it into multiple keys.
  * For example, if a key is storing a counter, split the counter into N separate counters that are randomly incremented by clients. The total value of the counter can then read by adding up the N individual ones.

* Keep key sizes small.

  * Try to keep key sizes below 1 kB. (Performance will be best with key sizes below 32 bytes and *cannot* be more than 10 kB.)
  * When using the tuple layer to encode keys (as is recommended), select short strings or small integers for tuple elements. Small integers will encode to just two bytes.
  * If your key sizes are above 1 kB, try either to move data from the key to the value, split the key into multiple keys, or encode the parts of the key more efficiently (remembering to preserve any important ordering).

* Keep value sizes moderate.

  * Try to keep value sizes below 10 kB. (Value sizes *cannot* be more than 100 kB.)
  * If your value sizes are above 10 kB, consider splitting the value across multiple keys.
  * If you read values with sizes above 1 kB but use only a part of each value, consider splitting the values using multiple keys.
  * If you frequently perform individual reads on a set of values that total to fewer than 200 bytes, try either to combine the values into a single value or to store the values in adjacent keys and use a range read.

Large Values and Blobs
----------------------

If your keys or values are much larger than the above guidelines, it may be difficult to find a data model that resizes them appropriately. Unstructured data, such as binary large objects, can be especially challenging to segment manually. In this case, a good option is to use our blob layer. See our tutorial on :doc:`Managing Large Values and Blobs <largeval>` for further discussion.


Data modeling examples: tutorials
=================================

The :doc:`tutorials` provide examples of data modeling using the tuple layer. They use techniques applicable to all of the :doc:`languages <api-reference>` supported by FoundationDB.
