.. default-domain:: py
.. default-domain:: py
.. highlight:: python

.. Required substitutions for api-common.rst.inc

.. |database-type| replace:: ``Database``
.. |database-class| replace:: ``Database``
.. |database-auto| replace:: FIXME
.. |tenant-type| replace:: FIXME
.. |transaction-class| replace:: ``Transaction``
.. |get-key-func| replace:: get_key()
.. |get-range-func| replace:: get_range()
.. |commit-func| replace:: ``commit()``
.. |open-func| replace:: FIXME
.. |set-cluster-file-func| replace:: FIXME
.. |set-local-address-func| replace:: FIXME
.. |on-error-func| replace:: ``on_error()``
.. |null-type| replace:: FIXME
.. |error-type| replace:: exception
.. |error-raise-type| replace:: raise
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

###############
Developer Guide
###############

FoundationDB's scalability and performance make it an ideal back end for supporting the operation of critical applications. FoundationDB provides a simple data model coupled with powerful transactional integrity. This document gives an overview of application development using FoundationDB, including use of the API, working with transactions, and performance considerations.

.. note:: For operational advice about how to setup and maintain a FoundationDB cluster, see :doc:`administration`.

Data model
==========

FoundationDB's core data model is an ordered key-value store. Also known as an ordered associative array, map, or dictionary, this is a data structure composed of a collection of key-value pairs in which all keys are unique and ordered. Both keys and values in FoundationDB are simple byte strings. Apart from storage and retrieval, the database does not interpret or depend on the content of values. In contrast, keys are treated as members of a total order, the lexicographic order over the underlying bytes, in which keys are sorted by each byte in order.

The combination of the core data model and multikey transactions allows an application to build richer data models and libraries that inherit FoundationDB's scalability, performance, and integrity. Richer data models are designed by mapping the application's data to keys and values in a way that yields an effective abstraction and enables efficient storage and retrieval.

.. note:: For guidance on mapping richer data models to FoundationDB's core, see :doc:`data-modeling`.

.. _developer-guide-namespace-management:

Namespace management
====================

The keys in FoundationDB's key-value store can be viewed as elements of a single, global keyspace. Your application will probably have multiple kinds of data to store, and it's a good idea to separate them into different `namespaces <http://en.wikipedia.org/wiki/Namespace_(computer_science)>`_. The use of distinct namespaces will allow you to avoid conflicts among keys as your application grows.

Because of the ordering of keys, a namespace in FoundationDB is defined by any prefix prepended to keys. For example, if we use a prefix ``'alpha'``, any key of the form ``'alpha' + remainder`` will be nested under ``'alpha'``. Although you can manually manage prefixes, it is more convenient to use the :ref:`tuple layer <data-modeling-tuples>`. To define a namespace with the tuple layer, just create a tuple ``(namespace_id)`` with an identifier for the namespace. You can add a new key ``(foo, bar)`` to the namespace by extending the tuple: ``(namespace_id, foo, bar)``. You can also create nested namespaces by extending your tuple with another namespace identifier: ``(namespace_id, nested_id)``. The tuple layer automatically encodes each of these tuple as a byte string that preserves its intended order.

.. note:: :ref:`Subspaces <developer-guide-sub-keyspaces>` employ the tuple layer to provide a convenient syntax for namespaces.

.. _developer-guide-sub-keyspaces:

Subspaces
---------

Subspaces provide the recommended way to define :ref:`namespaces <developer-guide-namespace-management>` for different categories of data. As a best practice, you should always use at least one subspace as a namespace for your application data. 

.. note:: While subspaces can be used directly, they are also returned when creating or opening a :ref:`directory <developer-guide-directories>`. Directories are designed to manage related subspaces.

Each FoundationDB language binding provides a :ref:`Subspace class <api-python-subspaces>` to help use subspaces uniformly. An instance of the class stores a prefix used to identify the namespace and automatically prepends it when encoding tuples into keys. Likewise, it removes the prefix when decoding keys. A subspace can be initialized by supplying it with a prefix tuple::

    my_space = Subspace(prefix_tuple)

It can also optionally take a byte string as a prefix element that will be prepended to all keys packed by the subspace::

    my_space = Subspace(prefix_tuple, prefix_element)

.. note:: A subspace formed with a byte string as a prefix element is not fully compatible with the tuple layer. Keys stored within it cannot be unpacked as tuples.

In many of the language bindings, index notation can be used on a subspace to create a nested subspace. The new subspace will have the same prefix extended by the index value::

    my_space = Subspace(('foo',))

    # new_space will have prefix tuple ('foo', 'bar')
    new_space = my_space['bar']

The :class:`Subspace` methods will then work as expected for ``new_space``, with its prefix nested within that of ``my_space``.

For example, suppose your application tracks profile data for your users. You could store the data in a ``user_space`` subspace that would make ``'user'`` the first element of each tuple. A back-end function might have the form::

    user_space = Subspace(('user',))

    @fdb.transactional
    def set_user_data(tr, key, value):
        tr[user_space.pack((key,))] = str(value)

Subspaces support the :ref:`as_foundationdb_key <api-python-keys>` method to implicitly pack keys, so you could also write the ``set_user_data`` as::

    @fdb.transactional
    def set_user_data(tr, key, value):
        tr[user_space[key]] = str(value)

Finally, you may want to clear a subspace before working with it (as long as you're sure it should be empty)::

    @fdb.transactional
    def clear_subspace(tr, subspace):
        tr.clear_range_startswith(subspace.key())

.. _developer-guide-directories:

Directories
-----------

FoundationDB provides :ref:`directories <api-python-directories>` (available in each language binding) as a tool for managing related :ref:`subspaces <developer-guide-sub-keyspaces>`. Directories are a recommended approach for administering applications. Each application should create or open at least one directory to manage its subspaces.

Directories are identified by hierarchical paths analogous to the paths in a Unix-like file system. A path is represented as a tuple of strings. Each directory has an associated subspace used to store its content. The directory layer maps each path to a short prefix used for the corresponding subspace. In effect, directories provide a level of indirection for access to subspaces.

This design has significant benefits: while directories are logically hierarchical as represented by their paths, their subspaces are not physically nested in a corresponding way. For example, suppose we create a few directories with increasing paths, such as::

  >>> a = fdb.directory.create(db, ('alpha',))
  >>> b = fdb.directory.create(db, ('alpha', 'bravo'))
  >>> c = fdb.directory.create(db, ('alpha', 'bravo', 'charlie'))

The prefixes of ``a``, ``b``, and ``c`` are allocated independently and will usually not increase in length. The indirection from paths to subspaces allows keys to be kept short and makes it fast to move directories (i.e., rename their paths).

Paths in the directory layer are always relative. In particular, paths are interpreted relative to the directory in which an operation is performed. For example, we could have created the directories ``a``, ``b``, and ``c`` as follows::

  >>> a = fdb.directory.create(db, ('alpha',))
  >>> b = a.create(db, ('bravo',))
  >>> c = b.create(db, ('charlie',))

We can easily check that the resulting paths are the same as before::

  >>> a.get_path()
  (u'alpha',)
  >>> b.get_path()
  (u'alpha', u'bravo')
  >>> c.get_path()
  (u'alpha', u'bravo', u'charlie')

Usage
~~~~~

The directory layer exposes a ``DirectoryLayer`` class and a ready-to-use instance of it that you can access as ``fdb.directory``. You can also create your own instance, which allows you to specify your own prefix for a subspace. For example, in Python, you could use::

  >>> dir_layer = DirectoryLayer(content_subspace = Subspace(rawPrefix='\x01'))

You can use a ``DirectoryLayer`` instance to create a new directory, specifying its path as a tuple. The ``create`` method returns a newly created directory::

  >>> users = fdb.directory.create(db, ('users',))
  >>> users
  DirectorySubspace(path=(u'users',), prefix='\x157')

A directory returned by ``create`` is a ``DirectorySubspace`` that fulfills the interface of both a ``DirectoryLayer`` and a ``Subspace``. Therefore, the directory can be used to access subdirectories recursively:

  >>> inactive = users.create(db, ('inactive',))
  >>> inactive
  DirectorySubspace(path=(u'users', u'inactive'), prefix='\x15&')

The directory can also be used as a subspace to store content::

  >>> db[users['Smith']] = '' 

The directory layer uses a high-contention allocator to efficiently map the path to a short prefix for the directory's subspace.

If the directory was created previously (e.g., in a prior session or by another client), you can open it via its path. Like ``create``, the ``open`` method returns a directory::

  >>> users = fdb.directory.open(db, ('users',))

It's often convenient to use a combined ``create_or_open`` method::

  >>> products = fdb.directory.create_or_open(db, ('products',))
  >>> orders = fdb.directory.create_or_open(db, ('orders',))

As noted above, all of the operations defined for a ``DirectoryLayer`` can also be called on a directory to operate on its subdirectories::

  >>> cancelled_orders = orders.create_or_open(db, ('cancelled',))

Once created, directory paths can be changed using ``move``::

  >>> store = fdb.directory.create_or_open(db, ('store',))
  >>> users = fdb.directory.move(db, ('users',), ('store', 'users'))
  >>> products = fdb.directory.move(db, ('products',), ('store', 'products'))

A directory path can also be changed via its subspace using ``move_to``::

  >>> orders = orders.move_to(db, ('store', 'orders'))

You can list the subdirectories of a directory. ``list`` returns directory names (the unicode string for the last component of the path), not subspaces or their contents::

  >>> fdb.directory.list(db)
  [u'store']
  >>> store.list(db) # or fdb.directory.list(db, ('store',))
  [u'orders', u'products', u'users']
  >>> store.list(db, ('orders'))
  [u'cancelled']

Directories can be removed, with or without a prior test for existence::

  >>> fdb.directory.remove_if_exists(db, ('store', 'temp'))
  >>> users.remove(db)

Although ``create_or_open`` and ``remove_if_exists`` cover the most common cases, you can also explicitly test for the existence of a directory::

  >>> users.exists(db)
  False
  >>> store.exists(db, ('products',))
  True

Subdirectories and nested subspaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It's common to have a subset of data that we want to arrange hierarchically under a directory. We can do this in two ways: by creating a subdirectory or a nested subspace. Let's return to our ``users`` directory for a closer look.

We may have data that is logically subordinate to our primary data but needs to be handled distinctly. For example, suppose we have inactive users that we want to store separately from our regular users. We could create a subdirectory for them::

  >>> inactive = users.create(db, ('inactive',))
  >>> inactive
  DirectorySubspace(path=(u'users', u'inactive'), prefix='\x15&')

A subdirectory's data is stored under a prefix unrelated to that of its parent, which allows the prefix to be kept short and makes the subdirectory fast and easy to move. The managed prefix also makes it harder to jointly access data across subdirectories (e.g., you cannot usefully perform a range read across subdirectories).

Conversely, we often have data that we want to access as part of a single data set. For example, suppose we want ``users`` to store a ``user_ID`` and related attributes. We could nest a subspace for ``'ID'`` under ``users`` and store each attribute beneath it::

  >>> db[ users['ID'][user_ID][lastname][firstname] ] = ''

Here, we're just employing ``users`` as a subspace, so all of the data modeling techniques using :ref:`subspaces <developer-guide-sub-keyspaces>` are available. Of course, data stored in a nested subspace cannot be moved as easily as a subdirectory.

Directory partitions
~~~~~~~~~~~~~~~~~~~~

Under normal operation, a directory does not share a common key prefix with its subdirectories. As a result, related directories are not necessarily located together in key-space. This means that you cannot use a single range query to read all contents of a directory and its descendants simultaneously, for example. 

For most applications this behavior is acceptable, but in some cases it may be useful to have a directory tree in your hierarchy where every directory shares a common prefix. For this purpose, the directory layer supports creating *partitions* within a directory. A partition is a directory whose prefix is prepended to all of its descendant directories' prefixes.

A partition can be created by specifying the byte string 'partition' for the layer parameter::

    >>> partition = fdb.directory.create(db, ('p1',), layer=b'partition')
    >>> users = partition.create_or_open(db, ('users',))

Directory partitions have the following drawbacks, and in general they should not be used unless specifically needed:

* Directories cannot be moved between different partitions.
* Directories in a partition have longer prefixes than their counterparts outside of partitions, which reduces performance. Nesting partitions inside of other partitions results in even longer prefixes.
* The root directory of a partition cannot be used to pack/unpack keys and therefore cannot be used to create subspaces. You must create at least one subdirectory of a partition in order to store content in it.

Tenants
-------

:doc:`tenants` in FoundationDB provide a way to divide the cluster key-space into named transaction domains. Each tenant has a byte-string name that can be used to open transactions on the tenant's data, and tenant transactions are not permitted to access data outside of the tenant. Tenants can be useful for enforcing separation between unrelated use-cases.

Tenants and directories
~~~~~~~~~~~~~~~~~~~~~~~

Because tenants enforce that transactions operate within the tenant boundaries, it is not recommended to use a global directory layer shared between tenants. It is possible, however, to use the directory layer within each tenant. To do so, simply use the directory layer as normal with tenant transactions.

Working with the APIs
=====================

FoundationDB supports client APIs for Python, Ruby, Node.js, Java, Go, and C. At a high level, each of these APIs support transactions allowing a client to:

* Set a key-value pair
* Get the value associated with a key
* Resolve a :ref:`key selector <system-keys>` to a key
* Get a range of key-value pairs (The range can be specified with keys or with key selectors)
* Clear a key (and its associated value)
* Clear a range of keys (and their values)

.. note:: For details on the language-specific APIs, see the corresponding document under :doc:`api-reference`.

.. _system-keys:

.. note:: All keys that start with the byte ``0xFF`` (255) are reserved for internal system use and should not be modified by the user. They cannot be read or written in a transaction unless it sets the ``ACCESS_SYSTEM_KEYS`` transaction option. Note that, among many other options, simply prepending a single zero-byte to all user-specified keys will avoid the reserved range and create a clean key space.

.. _key selectors:

Key selectors
-------------

|keysel-blurb1|

Each language API exposes constructors for four common forms of key selector. For example, in Python:

last_less_than(key)
    The lexicographically greatest key present in the database which is lexicographically strictly less than the given byte string key.

last_less_or_equal(key)
    The lexicographically greatest key present in the database which is lexicographically less than or equal to the given byte string key.

first_greater_than(key)
    The lexicographically least key present in the database which is lexicographically strictly greater than the given byte string key.

first_greater_or_equal(key)
    The lexicographically least key present in the database which is lexicographically greater than or equal to the given byte string key.

For example, suppose you want to read a range from a ``begin`` key to an ``end`` key *inclusive*. The :meth:`Transaction.get_range` method returns a range *exclusive* of its end key, so we can use a key selector to retrieve the desired range::

    X = tr.get_range(begin, fdb.KeySelector.first_greater_than(end))

You can add or subtract an offset to or from a key selector. For example, in Python::

	fdb.KeySelector.first_greater_than('apple') + 1

selects the second key following apple.

.. note:: The current version of FoundationDB *does not* resolve key selectors with large offsets in O(1) time. See the :ref:`dont-use-key-selectors-for-paging` known limitation.

All possible key selectors can be constructed using one of the four "common form" constructors and a positive or negative offset.  Alternatively, general key selectors can be manually constructed by specifying:

1. A reference key
2. An equality flag (boolean)
3. An offset (integer)

To "resolve" these key selectors FoundationDB first finds the last key less than the reference key (or equal to the reference key, if the equality flag is true), then moves forward a number of keys equal to the offset (or backwards, if the offset is negative). If a key selector would otherwise describe a key off the beginning of the database (before the first key), it instead resolves to the empty key ``''``. If it would otherwise describe a key off the end of the database (after the last key), it instead resolves to the key ``'\xFF'`` (or ``'\xFF\xFF'`` if the transaction has been granted access to the system keys).

Transaction basics
==================

Transactions in FoundationDB
----------------------------

FoundationDB provides concurrency control via transactions, allowing multiple clients to concurrently read and write data in the database with strong guarantees about how they affect each other. Specifically, FoundationDB provides global, ACID transactions with strict serializability using optimistic concurrency.

All reads and modifications of key-value pairs in FoundationDB are done within the context of a transaction. A transaction is a small unit of work that is both reliably performed and logically independent of other transactions.

In Python, a simple transaction looks like this::

	@fdb.transactional
	def example(tr):
	    # Read two values from the database
	    a = tr.get('a')
	    b = tr.get('b')
	    # Write two key-value pairs to the database
	    tr.set('c', a+b)
	    tr.set('d', a+b)

	example(db)

Once a call to the ``example()`` function returns, it is as if all "gets" and "sets" inside it happened at a single instant (at some point after ``example()`` was called and before it returned). If it *never* returns (suffers a power failure, raises an exception, etc.) then it is either as if the "get" and "set" methods all happened at an instantaneous point in time, or as if none of them happened.

.. _ACID:

This ensures the following properties, known collectively as "ACID":

* **Atomicity**: Either all of the writes in the transaction happen, or none of them happen.
* **Consistency**: If each individual transaction maintains a database invariant (for example, from above, that the ``'c'`` and ``'d'`` keys  always have the same value), then the invariant is maintained even when multiple transactions are modifying the database concurrently.
* **Isolation**: It is as if transactions executed one at a time (serializability).
* **Durability**: Once a transaction succeeds, its writes will not be lost despite any failures or network partitions.

An additional important property, though technically not part of ACID, is also guaranteed:

* **Causality**: A transaction is guaranteed to see the effects of all other transactions that commit before it begins.

FoundationDB implements these properties using multiversion concurrency control (MVCC) for reads and optimistic concurrency for writes. As a result, neither reads nor writes are blocked by other readers or writers. Instead, conflicting transactions will fail at commit time and will usually be retried by the client.

In particular, the reads in a transaction take place from an instantaneous snapshot of the database. From the perspective of the transaction this snapshot is not modified by the writes of other, concurrent transactions. When the read-write transaction is ready to be committed (read-only transactions don't get committed and therefore never conflict), the FoundationDB cluster checks that it does not conflict with any previously committed transaction (i.e. that no value read by a transaction has been modified by another transaction since the read occurred) and, if it does conflict, rejects it. Rejected conflicting transactions are usually retried by the client. Accepted transactions are written to disk on multiple cluster nodes and then reported accepted to the client.

* For more background on transactions, see Wikipedia articles for `Database transaction <http://en.wikipedia.org/wiki/Database_transaction>`_, `Atomicity (database systems) <http://en.wikipedia.org/wiki/Atomicity_(database_systems)>`_, and `Concurrency Control <http://en.wikipedia.org/wiki/Concurrency_control>`_.

Transaction retry loops
-----------------------

In most client APIs, there is a way of encapsulating a block of code as part of a transaction. For example, in Python, the ``@fdb.transactional`` decorator encapsulates retrying errors for the client. Here is a more detailed view of what the encapsulation does::

    def example_encapsulated(db):
        # make a new Transaction object on the database
        tr = db.create_transaction()

        while True:
            try:
                # Read two values from the transaction snapshot
                a = tr.get('a')
                b = tr.get('b')
                # Write two key-value pairs to the transaction snapshot
                tr.set('c', a+b)
                tr.set('d', a+b)

                # Try to commit the transaction, and wait for the result
                tr.commit().wait()

                # Success!
                return
            except fdb.FDBError as error:
                # The transaction conflicted or there was a transient failure.
                # Ask the FDB API whether and when to retry the error
                # (Also resets the tr object to be ready to use again)
                tr.on_error(error).wait()

Note that, while the ``@fdb.transactional`` decorator encapsulates an entire function, only the database operations it contains are part of the transaction and enjoy enforcement of ACID properties. Operations that mutate client memory (e.g., ordinary variable assignments or changes to data structures) are not "rolled back" when a transaction conflicts. You should place such operations outside of the retry loop unless it is acceptable for them to be executed when a transaction conflicts and is retried. See :ref:`developer-guide-unknown-results` for an example.

.. _developer-guide-programming-with-futures:

Programming with futures
------------------------

Many of FoundationDB's API functions are *asynchronous*: rather than blocking the calling thread until the result is available they immediately return a *future* object. A future represents a value (or error) to be available at some later time.

For languages in which it's supported, the simplest approach to futures is implicit blocking. This involves using the future as if it were an object of the result type. For example, the following Python code uses futures to do multiple read operations in parallel, thereby reducing transaction latency. The reads of the 'A' and 'B' keys will be done in parallel because ``a`` and ``b`` are futures::

    # tr is a transaction
    # Read two values from the database in parallel
    a = tr.get('A')
    b = tr.get('B')
    print a+b

The addition of ``a`` and ``b`` implicitly blocks. You can also explicitly block on a future
until it's ready.

By default, FoundationDB supports read-your-writes, meaning that reads reflect the results of prior writes within a transaction. FoundationDB maximizes parallelism of database operations within a transaction, subject to enforcing the sequential semantics of those operations, such as when a key is written before it is read.

Another approach to programming with futures in FoundationDB is to set a callback function to be invoked asynchronously when the future is ready.

.. note:: Be very careful when mixing callbacks with explicit or implicit blocking. Blocking in a callback on a non-ready future will cause a deadlock. Blocking on anything else or performing CPU intensive tasks will block the FoundationDB :ref:`client thread <client-network-thread>` and therefore all database access from that client.

For further details, see the :doc:`API reference <api-reference>` documentation for your language.

.. _developer-guide-range-reads:

Range reads
-----------

FoundationDB supports efficient range reads based on the lexicographic ordering of keys. Range reads are a powerful technique commonly used with FoundationDB. A recommended approach is to design your keys so that you can use range reads to retrieve your most frequently accessed data.

A range read within a transaction returns a container that issues asynchronous reads to the database. The client usually processes the data by iterating over the values returned by the container. Range reads can be specified explicitly by giving the ``begin`` and ``end`` of a range::

  for k, v in tr.get_range('a', 'm'):
      print(k, v)

To define ranges that extend from the beginning the database, you can use the empty string ``''``::

  for k, v in tr.get_range('', 'm'):
      print(k, v)

Likewise, to define ranges that extend to the end of the database, you can use the key ``'\xFF'``::

  for k, v in tr.get_range('m', '\xFF'):
      print(k, v)

A range read can also retrieve all keys starting with a given ``prefix``::

  for k, v in tr.get_range_startswith('a'):
      print(k, v)

The API balances latency and bandwidth by fetching data in batches as determined by the :ref:`streaming mode <streaming-mode-python>` parameter. Streaming modes allow you to customize this balance based on how you intend to consume the data. The default streaming mode (``iterator``) is quite efficient. However, if you anticipate that your range read will retrieve a large amount of data, you should select a streaming mode to match your use case. For example, if you're iterating through a large range and testing against a condition that may result in early termination, you may want to use the ``small`` streaming mode::

  for k, v in tr.get_range_startswith('a', streaming_mode=fdb.StreamingMode.small):
      if halting_condition(k, v): break
      print(k, v)

In some situations, you may want to explicitly control the number of key-value pairs returned. You can use the ``limit`` parameter for this purpose. For example, suppose you want to iterate through a range while retrieving blocks of a predetermined size::

  LIMIT = 100 # adjust according to data characteristics

  def get_range_limited(tr, begin, end):
      keys_found = True
      while keys_found:
          keys_found = []
          for k, v in tr.get_range(begin, end, limit=LIMIT):
              keys_found.append(k)
          if keys_found:
              begin = fdb.KeySelector.first_greater_than(keys_found[-1])
              yield keys_found

For very large range reads, you can use multiple clients to perform reads concurrently. In this case, you'll want to estimate sub-ranges of roughly equal size based on the distribution of your keys. The :ref:`locality <api-python-locality>` functions can be used to derive estimates for the boundaries between sub-ranges.

.. _developer-guide-long-transactions:

Long-running transactions
-------------------------

FoundationDB does not support long-running transactions, currently defined as
those lasting over five seconds. The reasons for this design limitation relate
to multiversion concurrency control and are discussed in :doc:`anti-features`.

You may have certain large operations that you're accustomed to implementing as
long-running transactions with another database. How should you approach implementing
your operation in FoundationDB?

The key consideration is whether your operation requires global consistency over
all its data elements. In many cases, some smaller scope of consistency is acceptable.
For example, many analytic computations are defined over a set of entities, such
as users, and require consistency only for each entity, not globally across them.
In this case, you can decompose the operation into multiple transactions, one for
each entity. More generally, the strategy for operations requiring local consistency
is to decompose them into a set of short transactions.

If your operation really does require global consistency, you can often use
an indirection strategy. Here, you write a separate version of the data during
the course of the operation and switch to the new version when
the operation is done. The switch can be performed transactionally with a single
change of a reference.

For example, you can store the version reference using a ``'mostRecentVersion'``
key::

    @fdb.transactional
    def setMostRecentVersion(tr, versionNumber):
        tr[fdb.tuple.pack(('mostRecentVersion',))] = str(versionNumber)

    @fdb.transactional
    def getMostRecentVersion(tr):
        return tr[fdb.tuple.pack(('mostRecentVersion',))]

Your application would then store the relevant data using keys that encode the
version number. The application would read data with a transaction that reads the
most recent version number and uses it to reference the correct data. This
strategy has the advantage of allowing consistent access to the current version
of the data while concurrently writing the new version.

Working with transactions
=========================

.. _developer-guide-atomic-operations:

Atomic operations
-----------------

|atomic-ops-blurb1|

Atomic operations are ideal for operating on keys that multiple clients modify frequently. For example, you can use a key as a counter and increment/decrement it with atomic :func:`add`::

  @fdb.transactional
  def increment(tr, counter):
      tr.add(counter, struct.pack('<i', 1))

  @fdb.transactional
  def decrement(tr, counter):
      tr.add(counter, struct.pack('<i', -1))

When the counter value is decremented down to 0, you may want to clear the key from database. An easy way to do that is to use :func:`compare_and_clear`, which atomically compares the value against given parameter and clears it without issuing a read from client::

  @fdb.transactional
  def decrement(tr, counter):
      tr.add(counter, struct.pack('<i', -1))
      tr.compare_and_clear(counter, struct.pack('<i', 0))

Similarly, you can use a key as a flag and toggle it with atomic :func:`xor`::

  @fdb.transactional
  def toggle(tr, flag):
      tr.xor(flag, struct.pack('=?',1))

Each atomic operation takes a packed string as an argument (as detailed in the :ref:`API reference <api-python-transaction-atomic-operations>`).

|atomic-ops-blurb2| By combining its logical steps into a single, read-free operation, FoundationDB can guarantee that the transaction performing the atomic operation will not conflict due to that operation.

.. note :: |atomic-ops-warning|

.. _developer-guide-unknown-results:

Transactions with unknown results
---------------------------------

|unknown-result-blurb|

.. note :: The Python ``@fdb.transactional`` decorator and its counterparts in the other language APIs do not check for :ref:`commit_unknown_result <developer-guide-error-codes>`.

An *idempotent* transaction is one that has the same effect when committed twice as when committed once. If your transaction is already idempotent, there is nothing more to worry about. Otherwise, you should consider whether it is acceptable for your transaction to be committed twice. The following suggestions are useful for making your transaction idempotent:

* Avoid generating IDs within the retry loop. Instead, create them prior to the loop and pass them in. For example, if your transaction records an account deposit with a deposit ID, generate the deposit ID outside of the loop.

* Within the retry loop, check for the completion of one of the transaction's unique side effects to determine if the whole transaction has previously completed. If the transaction doesn't naturally have such a side effect, you can create one by setting a unique key.

The following example illustrates both techniques. Together, they make a transaction idempotent that otherwise would not have been::

    # Increases account balance and stores a record of the deposit with a unique depositId
    @fdb.transactional
    def deposit(tr, acctId, depositId, amount):

        # If the deposit record exists, the deposit already succeeded, and we can quit
        depositKey = fdb.tuple.pack(('account', acctId, depositId))
        if tr[depositKey].present(): return

        amount = struct.pack('<i', amount)
        tr[depositKey] = amount

        # The above check ensures that the balance update is executed only once
        balanceKey = fdb.tuple.pack(('account', acctId))
        tr.add(balanceKey, amount)

There is experimental support for preventing ``commit_unknown_result`` altogether using a transaction option. See :doc:`automatic-idempotency` for more details. Note: there are other errors which indicate an unknown commit status. See :ref:`non-retryable errors`.

.. _conflict-ranges:

Conflict ranges
---------------

By default, FoundationDB transactions guarantee :ref:`strict serializability <ACID>`, which results in a state that *could* have been produced by executing transactions one at a time, even though they may actually have been executed concurrently. FoundationDB maintains strict serializability by detecting conflicts among concurrent transactions and allowing only a non-conflicting subset of them to succeed. Two concurrent transactions conflict if the first to commit writes a value that the second reads. In this case, the second transaction will fail. Clients will usually retry failed transactions.

To detect conflicts, FoundationDB tracks the ranges of keys each transaction reads and writes. While most applications will use the strictly serializable isolation that transactions provide by default, FoundationDB also provides several API features that manipulate conflict ranges to allow more precise control.

Conflicts can be *avoided*, reducing isolation, in two ways:

* Instead of ordinary (strictly serializable) reads, you can perform :ref:`snapshot reads <snapshot isolation>`, which do not add read conflict ranges.
* You can use :ref:`transaction options <api-python-transaction-options>` to disable conflict ranges for writes.

Conflicts can be *created*, increasing isolation, by :ref:`explicitly adding <api-python-conflict-ranges>` read or write conflict ranges. 

.. note:: *add read conflict range* behaves as if the client is reading the range. This means *add read conflict range* will not add conflict ranges for keys that have been written earlier in the same transaction. This is the intended behavior, as it allows users to compose transactions together without introducing unnecessary conflicts.

For example, suppose you have a transactional function that increments a set of counters using atomic addition. :ref:`developer-guide-atomic-operations` do not add read conflict ranges and so cannot cause the transaction in which they occur to fail. More precisely, the read version for an atomic operation is the same as transaction's commit version, and thus no conflicting write from other transactions could be serialized between the read and write of the key. Most of the time, this is exactly what we want. However, suppose there is another transaction that (infrequently) resets one or more counters, and our contract requires that we must advance all specified counters in unison. We want to guarantee that if a counter is reset during an incrementing transaction, then the incrementing transaction will conflict. We can selectively add read conflicts ranges for this purpose::

  @fdb.transactional
  def guarded_increment(tr, counters):
      for counter in counters:
          tr.add(counter, struct.pack('<i', 1))
          if reset(counter): tr.add_read_conflict_key(counter)

.. _snapshot isolation:

Snapshot reads
--------------

|snapshot-blurb1|

The strictly serializable isolation that transactions maintain by default has little performance cost when there are few conflicts but can be expensive when there are many. FoundationDB therefore also permits individual reads within a transaction to be done as snapshot reads. Snapshot reads differ from ordinary (strictly serializable) reads by permitting the values they read to be modified by concurrent transactions, whereas strictly serializable reads cause conflicts in that case.

Consider a transaction which needs to remove and return an arbitrary value from a small range of keys.  The simplest implementation (using strictly serializable isolation) would be::

    @fdb.transactional
    def remove_one(tr, range):
        all_kv = tr[range]
        key, value = random.choice(list(all_kv))
        del tr[key]
        return value

Unfortunately, if a concurrent transaction happens to insert a new key anywhere in the range, our transaction will conflict with it and fail (resulting in a retry) because seeing the other transaction's write would change the result of the range read.  FoundationDB is enforcing a stronger contract than we actually need.  A snapshot read allows us to weaken the contract, but we don't want to weaken it too far: it's still important to us that the actual value we returned existed in the database at the time of our transaction. Adding a :ref:`conflict range <conflict-ranges>` for our key ensures that we will fail if someone else modifies the key simultaneously::

    @fdb.transactional
    def remove_one(tr, range):
        all_kv = tr.snapshot[range]              # Snapshot read
        key, value = random.choice(list(all_kv))
        tr.add_read_conflict_key(key)            # Add conflict range
        del tr[key]
        return value

This transaction accomplishes the same task but won't conflict with the insert of a key elsewhere in the range. It will only conflict with a modification to the key it actually returns.

By default, snapshot reads see the effects of prior writes in the same transaction. (This read-your-writes behavior is the same as for ordinary, strictly serializable reads.) Read-your-writes allows transactional functions (such as the above example) to be easily composed within a single transaction because each function will see the writes of previously invoked functions.

.. note::
  | The default read-your-writes behavior of snapshot reads is well-suited to the large majority of use cases. In less frequent cases, you may want to read from only a single version of the database. This behavior can be achieved through the appropriate :ref:`transaction options <api-python-snapshot-ryw>`. Transaction options are an advanced feature of the API and should be used with caution.
  | 
  | Read-your-writes can be disabled (and re-enabled) within a transaction by using the options:
  |
  | * :meth:`Transaction.options.set_snapshot_ryw_disable`
  | * :meth:`Transaction.options.set_snapshot_ryw_enable`.
  |
  | A given snapshot read gets read-your-writes behavior unless the disable option has been previously set more times than the enable option in that transaction.
  |

Using snapshot reads is appropriate when the following conditions all hold:

* A particular read of frequently written values causes too many conflicts.
* There isn't an easy way to reduce conflicts by splitting up data more granularly.
* Any necessary invariants can be validated with added conflict ranges or more narrowly targeted strictly serializable reads.

Transaction cancellation
------------------------

The FoundationDB language bindings all provide a mechanism for cancelling an outstanding transaction. However there are also special transaction options for specifying the conditions under which a transaction should automatically be cancelled.

In the following example, a retry loop is combined with transaction options that ensure that the operation will not be attempted more than 6 times or for longer than 3 seconds::

    @fdb.transactional
    def example_with_cancellation(tr):
        # Set maximum number of times that on_error() can be called (implicitly by the decorator).
        # On the 6th time, on_error() will throw retry_limit_exceeded rather than resetting and retrying.
        tr.options.set_retry_limit(5)
        # Cancel transaction with transaction_timed_out after 3 seconds
        tr.options.set_timeout(3000)
                
        # Read two values from the transaction snapshot
        a = tr.get('a')
        b = tr.get('b')
        # Write two key-value pairs to the transaction snapshot
        tr.set('c', a+b)
        tr.set('d', a+b)
                
.. note:: The ``set_retry_limit()`` option sets a maximum number of *retries*, not tries. So the transaction above will at most be attempted a total of six times.

Watches
-------

Sometimes you want a client to monitor one or more keys for updates to their values by other clients. An obvious way to implement monitoring is by polling, i.e., periodically reading the key-values to check them for a change. FoundationDB provides watches to monitor keys more efficiently. Watches are created for a specified key and return a :ref:`future <developer-guide-programming-with-futures>` that becomes ready when there's a change to the key's value.

For example, suppose you have a polling loop that checks keys for changes once a second::

    def polling_loop(db, keys):

        @fdb.transactional
        def read_keys(tr):
            return {k:tr[k] for k in keys}
    
        cache = {k:object() for k in keys}
        while True:
            value = read_keys(db)
            for k in keys:
                if cache[k] != value[k]:
                    yield (k, value[k])
                    cache[k] = value[k]
            time.sleep(1)

You can use the loop to dispatch changes to a handler with something like::

    for k, v in polling_loop(db, ['foo','bar','bat']): handle(k,v)

With watches, you can eliminate the sleep and perform new reads only after a change to one of the keys::

    def watching_read_loop(db, keys):

        @fdb.transactional
        def watch_keys(tr):
            return {k:tr[k] for k in keys}, [tr.watch(k) for k in keys]
    
        cache = {k:object() for k in keys}
        while True:
            value, watches = watch_keys(db)
            for k in keys:
                if cache[k] != value[k]:
                    yield (k, value[k])
                    cache[k] = value[k]
            fdb.Future.wait_for_any(*watches)

The version with watches will perform fewer unnecessary reads and detect changes with better resolution than by polling.

.. note :: Watches guarantee only that a value was changed; they make no guarantee about values you may subsequently read. In particular, there is no guarantee that a value you read will correspond to the change that triggered the watch. Another client may have changed a key back to its original value or to some third value between a watch becoming ready and your subsequent read. For further details, see the discussion of watches in the language-specific document of your choice under :doc:`api-reference`.

If you only need to detect the *fact* of a change, and your response doesn't depend on the new *value*, then you can eliminate the reads altogether::

    def watching_loop(db, keys):

        @fdb.transactional
        def watch_keys(tr):
            return [tr.watch(k) for k in keys]
    
        while True:
            fdb.Future.wait_for_any(*watch_keys(db))
            yield

.. _developer-guide-peformance-considerations:


Performance considerations
==========================

Latency
-------

Like all systems, FoundationDB operates at a low latency while under low load and an increasing latency as the load approaches the saturation point. We have made efforts to allow FoundationDB to operate at a low latency even at moderate loads. However, if FoundationDB is being driven with a "saturating" load (e.g. batch processing), latencies can be become very high as a line forms for requests. In this case, the transactions generating the saturating load should be run with a lower priority, allowing other transactions to skip ahead in line.

* For more information on setting transaction priorities, see the discussion of Transaction Options in the language-specific document of your choice under :doc:`api-reference`.

There are several places in a typical transaction that can experience database latency:

* **Starting the transaction**. This delay will be experienced as part of your first read (or part of ``getReadVersion()`` if using that API call). It will typically be a few milliseconds under moderate load, but under high write loads FoundationDB tries to concentrate most transaction latency here. This latency does not increase transaction conflicts (see :ref:`developer-guide-transaction-conflicts` below) since the transaction has not yet started.
* **Individual reads**. These should take about 1 ms under moderate load on appropriate hardware. If a transaction performs many reads by waiting for each to complete before starting the next, however, these small latencies can add up. You can thus reduce transaction latency (and potentially conflicts) by doing as many of your reads as possible in parallel (i.e. by starting several reads before waiting on their results). See the :ref:`developer-guide-programming-with-futures` section of this document for an elegant way to achieve this.
* **Committing the transaction**. Transactions that are not read-only must be committed, and the commit will not succeed until the transaction is fully (redundantly) durable. This takes time: averaging about 10 ms under normal loads with SSD hardware. This latency will be increased further in a geographically distributed system (in order to confirm that the transaction is durable in multiple datacenters). Only a small part of this latency impacts transaction conflicts.

Throughput requires concurrency
-------------------------------

FoundationDB will only reach its maximum performance with a highly concurrent workload. This is a practical consideration that derives mathematically from the ratio of system throughput to system latency (known in queuing theory as `Little's Law <http://en.wikipedia.org/wiki/Little%27s_law>`_). For FoundationDB, a cluster might have a read latency of 1ms and be capable of millions of reads per second. To achieve such a rate, there must therefore be thousands of read requests happening concurrently. *Not having enough outstanding requests is the single biggest reason for low performance when using FoundationDB.*

There are several important techniques for achieving high concurrency:

* Whether your application does FoundationDB transactions in response to requests (as in web applications) or simply does transactions as fast as it can (as in a batch workload), make sure to run it with enough concurrent threads or processes---perhaps more than you would expect to provide optimal performance from experience with other database systems.
* In many environments, there are cheaper (and sometimes less dangerous) alternatives to operating system threads for workloads that are bound by network latency. For example, in Python, the `gevent library <http://gevent.org/>`_ provides "coroutines" that have a simple thread-like programming model but are scheduled asynchronously in a single thread. FoundationDB's :doc:`Python API <api-python>` integrates with gevent and other language APIs have similar integrations. This can make it practical to run hundreds or thousands of concurrent transactions per core without much overhead. (If FoundationDB doesn't integrate with your favorite asynchronous programming tool, please let us know about it.)
* Whenever possible, do multiple reads within a single transaction in parallel rather than sequentially. This reduces latency, and consequently reduces the number of concurrent transactions required to sustain a given throughput. See the :ref:`developer-guide-programming-with-futures` section of this document for an elegant way to achieve this.

.. _developer-guide-transaction-conflicts:

Minimizing conflicts
---------------------

Frequent conflicts make FoundationDB operate inefficiently and should be minimized. They result from multiple clients trying to update the same keys at a high rate. Developers need to avoid this condition by spreading frequently updated data over a large set of keys.

.. note :: As a rule of thumb, if a key will be modified *more than 10-100 times per second*, a different data model should be considered.

In these situations:

* If the data stored in the key is large, consider :ref:`splitting it among multiple keys<largeval-splitting>` that can each be modified separately.
* For a data structure like a counter, consider using :ref:`atomic operations <developer-guide-atomic-operations>` so that the write-only transactions do not conflict with each other. FoundationDB supports atomic operations for *addition*, *min*, *max*, bitwise *and*, bitwise *or*, and bitwise *xor*.
* Consider performing selected reads as :ref:`snapshot reads <snapshot isolation>`, which eliminate conflicts with those reads but weaken transactional isolation.
* For associative, commutative operations not supported as atomic operations, consider using adaptive sharding.
* For operations that are order-dependent, consider inserting operations into a database queue prior to their execution by a subsequent transaction. The general pattern is to convert a read/modify/write operation on a logical value into an insertion into a list of changes to the value. Any client can then transactionally replace a portion of the list with its evaluated result. This pattern allows insertions to be decoupled from subsequent evaluations, which can take place in separate transactions. You can reach out on the `community forums <https://forums.foundationdb.org>`_ for help with finding the simplest solution to your actual use case.

.. _developer-guide-key-and-value-sizes:

Key and value sizes
-------------------

Maintaining efficient key and value sizes is essential to getting maximum performance with FoundationDB. As a rule, smaller key sizes are better, as they can be more efficiently transmitted over the network and compared. In concrete terms, the highest performance applications will keep key sizes below 32 bytes. Key sizes above 10 kB are not allowed, and sizes above 1 kB should be avoided---store the data in the value if possible.

Value sizes are more flexible, with 0-10 kB normal. Value sizes cannot exceed 100 kB. Like any similar system, FoundationDB has a "characteristic value size" where the fixed costs of the random read roughly equal the marginal costs of the actual bytes. For FoundationDB running on SSD hardware, this characteristic size is roughly 1 kB for randomly accessed data and roughly 100 bytes for frequently accessed (cached) data.

If your keys or values are initially too large, try to revise your :doc:`data model <data-modeling>` to make them smaller.

Loading data
------------

Loading data is a common task in any database. Loading data in FoundationDB will be most efficiently accomplished if a few guidelines are followed:

* Load small sequential chunks of the data set from random positions in the data set (to allow the system to efficiently distribute different data to different servers).
* Do about 10KB of data in total writes per transaction.
* Use about 50 concurrent transactions per loading process to allow efficient pipelining. You can increase the number of concurrent transactions as long as transaction latencies remain under about 1 second.
* Use multiple processes loading in parallel if a single one is CPU-bound.

Using these techniques, our cluster of 24 nodes and 48 SSDs loads about 3 billion (100 byte) key-value pairs per hour.

Implementation Details
======================

These following sections go into some of the gritty details of FoundationDB. Most users don't need to read or understand this in order to use FoundationDB efficiently.

How FoundationDB Detects Conflicts
----------------------------------

As written above, FoundationDB implements serializable transactions with external consistency. The underlying algorithm uses multi-version concurrency control. At commit time, each transaction is checked for read-write conflicts.

Conceptually this algorithm is quite simple. Each transaction will get a read version assigned when it issues the first read or before it tries to commit. All reads that happen during that transaction will be read as of that version. Writes will go into a local cache and will be sent to FoundationDB during commit time. The transaction can successfully commit if it is conflict free; it will then get a commit-version assigned. A transaction is conflict free if and only if there have been no writes to any key that was read by that transaction between the time the transaction started and the commit time. This is true if there was no transaction with a commit version larger than our read version but smaller than our commit version that wrote to any of the keys that we read.

This form of conflict detection, while simple, can often be confusing for people who are familiar with databases that check for write-write conflicts.

Some interesting properties of FoundationDB transactions are:

* FoundationDB transactions are optimistic: we never block on reads or writes (there are no locks), instead we abort transactions at commit time.
* Read-only transactions will never conflict and never cause conflicts with other transactions.
* Write-only transactions will never conflict but might cause future transactions to conflict.
* For read-write transactions: A read will never cause any other transaction to be aborted - but reading a key might result in the current transaction being aborted at commit time. A write will never cause a conflict in the current transaction but might cause conflicts in transactions that try to commit in the future.
* FoundationDB only uses the read conflict set and the write conflict set to resolve transactions. A user can read from and write to FoundationDB without adding entries to these sets. If not done carefully, this can cause non-serializable executions (see :ref:`Snapshot Reads <api-python-snapshot-reads>` and the :ref:`no-write-conflict-range option <api-python-no-write-conflict-range>` option).

How Versions are Generated and Assigned
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Versions are generated by the process that runs the *master* role. FoundationDB guarantees that no version will be generated twice and that the versions are monotonically increasing.

In order to assign read and commit versions to transactions, a client will never talk to the master. Instead it will get them from a GRV proxy and a commit proxy. Getting a read version is more complex than a commit version. Let's first look at commit versions:

#. The client will send a commit message to a commit proxy.
#. The commit proxy will put this commit message in a queue in order to build a batch.
#. In parallel, the commit proxy will ask for a new version from the master (note that this means that only commit proxies will ever ask for new versions - which scales much better as it puts less stress on the network).
#. The commit proxy will then resolve all transactions within that batch (discussed later) and assign the version it got from the master to *all* transactions within that batch. It will then write the transactions to the transaction log system to make it durable.
#. If the transaction succeeded, it will send back the version as commit version to the client. Otherwise it will send back an error.

As mentioned before, the algorithm to assign read versions is a bit more complex. At the start of a transaction, a client will ask a GRV proxy server for a read version. The GRV proxy will reply with the last committed version as of the time it received the request - this is important to guarantee external consistency. This is how this is achieved:

#. The client will send a GRV (get read version) request to a GRV proxy.
#. The GRV proxy will batch GRV requests for a short amount of time (it depends on load and configuartion how big these batches will be).
#. The proxy will do the following steps in parallel:
   * Ask master for their most recent committed version (the largest version of proxies' committed version for which the transactions are successfully written to the transaction log system).
   * Send a message to the transaction log system to verify that it is still writable. This is to prevent that we fetch read versions from a GRV proxy that has been declared to be dead.

Checking whether the log-system is still writeable can be especially expensive if a clusters runs in a multi-region configuration. If a user is fine to sacrifice strict serializability they can use :ref:`option-causal-read-risky <api-python-option-set-causal-read-risky>`.

Conflict Detection
~~~~~~~~~~~~~~~~~~

This section will only explain conceptually how transactions are resolved in FoundationDB. The implementation will use multiple servers running the *Resolver* role and the keyspace will be sharded across them. It will also only allow resolving transactions whose read versions are less than 5 million versions older than their commit version (around 5 seconds).

A resolver will keep a map in memory which stores the written keys of each commit version. A simpified resolver state could look like this:

=======    =======
Version    Keys
=======    =======
1000       a, b
1200       f, q, c
1210       a
1340       t, u, x
=======    =======

Now let's assume we have a transaction with read version *1200* and the assigned commit version will be something larger than 1340 - let's say it is *1450*. In that transaction we read keys ``b, m, s`` and we want to write to ``a``. Note that we didn't read ``a`` - so we will issue a blind write. The resolver will check whether any of the read keys (``b, m, or s``) appers in any line between version *1200* and the most recent version, *1450*. The last write to ``b`` was at version 1000 which was before the read version. This means that transaction read the most recent value. We don't know about any recent writes to the other keys. Therefore the resolver will decide that this transaction does *NOT* conflict and it can be committed. It will then add this new write set to its internal state so that it can resolve future transactions. The new state will look like this:

=======    =======
Version    Keys
=======    =======
1000       a, b
1200       f, q, c
1210       a
1340       t, u, x
1450       a
=======    =======

Note that the resolver didn't use the write set at all in order to make a decision whether the transaction can commit or not. This means that blind writes (writes to keys without reading them first) will never cause a conflict. But since the resolver will then remember these writes, blind writes can cause future transactions to conflict.

Error Handling
--------------

When using FoundationDB we strongly recommend users to use the retry-loop. In Python the retry loop would look this this:

.. code-block:: python

   tr = tr.create_transaction()
   while True:
       try:
           # execute reads and writes on FDB using the tr object
           tr.commit().wait()
           break
       except FDBError as e:
           tr.on_error(e.code).wait()

This is also what the transaction decoration in python does, if you pass a ``Database`` object to a decorated function. There are some interesting properties of this retry loop:

* We never create a new transaction within that loop. Instead ``tr.on_error`` will create a soft reset on the transaction.
* ``tr.on_error`` returns a future. This is because ``on_error`` will do back off to make sure we don't overwhelm the cluster.
* If ``tr.on_error`` throws an error, we exit the retry loop.

If you use this retry loop, there are very few caveats. If you write your own and you are not careful, some things might behave differently than you would expect. The following sections will go over the most common errors you will see, the guarantees FoundationDB provides during failures, and common caveats. This retry loop will take care of most of these errors, but it might still be beneficial to understand those.

Errors where we know the State of the Transaction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most common errors you will see are errors where we know that the transaction failed to commit. In this case, we're guaranteed that nothing that we attempted to write was written to the database. The most common error codes for this are:

* ``not_committed`` is thrown whenever there was a conflict. This will only be thrown by a ``commit``, read and write operations won't generate this error.
* ``transaction_too_old`` is thrown if your transaction runs for more than five seconds. If you see this error often, you should try to make your transactions shorter.
* ``future_version`` is one of the slightly more complex errors. There are a couple ways this error could be generated: if you set the read version of your transaction manually to something larger than exists or if the storage servers are falling behind. The second case should be more common. This is usually caused by a write load that is too high for FoundationDB to handle or by faulty/slow disks.

The good thing about these errors is that retrying is simple: you know that the transaction didn't commit and therefore you can retry even without thinking much about weird corner cases.

The ``commit_unknown_result`` Error
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``commit_unknown_result`` can be thrown during a commit. This error is difficult to handle as you won't know whether your transaction was committed or not. There are mostly two reasons why you might see this error:

#. The client lost the connection to the commit proxy to which it did send the commit. So it never got a reply and therefore can't know whether the commit was successful or not.
#. There was a FoundationDB failure - for example a commit proxy failed during the commit. In that case there is no way for the client know whether the transaction succeeded or not.

However, there is one guarantee FoundationDB gives to the caller: at the point of time where you receive this error, the transaction either committed or not and if it didn't commit, it will never commit in the future. Or: it is guaranteed that the transaction is not in-flight anymore. This is an important guarantee as it means that if your transaction is idempotent you can simply retry. For more explanations see developer-guide-unknown-results_.

There is experimental support for preventing ``commit_unknown_result`` altogether using a transaction option. See :doc:`automatic-idempotency` for more details. Note: there are other errors which indicate an unknown commit status. See :ref:`non-retryable errors`.

.. _non-retryable errors:

Non-Retryable Errors
~~~~~~~~~~~~~~~~~~~~

The trickiest errors are non-retryable errors. ``Transaction.on_error`` will rethrow these. Some examples of non-retryable errors are:

#. ``transaction_timed_out``. If you set a timeout for a transaction, the transaction will throw this error as soon as that timeout occurs.
#. ``operation_cancelled``. This error is thrown if you call ``cancel()`` on any future returned by a transaction. So if this future is shared by multiple threads or coroutines, all other waiters will see this error.

If you see one of those errors, the best way of action is to fail the client.

At a first glance this looks very similar to an ``commit_unknown_result``. However, these errors lack the one guarantee ``commit_unknown_result`` still gives to the user: if the commit has already been sent to the database, the transaction could get committed at a later point in time. This means that if you retry the transaction, your new transaction might race with the old transaction. While this technically doesn't violate any consistency guarantees, abandoning a transaction means that there are no causality guaranatees.
