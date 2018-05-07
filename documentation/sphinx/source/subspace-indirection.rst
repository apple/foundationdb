####################
Subspace Indirection
####################

**Python** :doc:`Java <subspace-indirection-java>`

Goal
====

Employ subspace indirection to manage bulk inserts or similar long-running operations.

Challenge
=========

Some large or long operations, such as bulk inserts, cannot be handled in a single FoundationDB transaction because the database does not support transactions over five seconds.

Explanation
===========

We can handle long-running operations using a distinct subspace to hold a temporary copy of the data. The new subspace is transactionally moved into place upon completion of the writes. We can perform the move quickly because the client accesses the subspaces through managed references.

FoundationDB :ref:`directories <developer-guide-directories>` provide a convenient method to indirectly reference subspaces. Each directory is identified by a path that is mapped to a prefix for a subspace. The indirection from paths to subspaces makes it fast to move directories by renaming their paths.

Ordering
========

The ordering of keys applies within each directory subspace for whatever data model is used by the application. However, directory subspaces are independent of each other, and there is no meaningful ordering between them.

Pattern
=======

For a single client, we can use a simple context manager to handle creation of a new subspace and transactionally swapping it into place. Rather than working with a subspace directly, a client accesses the current subspace through a managed reference as follows::

 db = fdb.open()
  
 working_dir = fdb.directory.create_or_open(db, (u'working',))
 workspace = Workspace(working_dir, db)
 current = workspace.current

The client performs transactions on data in the subspace current in the usual manner. When we want a new workspace for a bulk load or other long-running operation, we create one with a context manager::

 with workspace as newspace:
     # . . .     
     # perform long-running operation using newspace here
     # . . .
 # current workspace has now been replaced by the new one
 current = workspace.current

When the context manager completes, it transactionally replaces the current workspace with the new one.

Extensions
==========

*Multiple Clients*

Beyond the ability to load and transactionally swap in a single new data set, an application may want to support multiple clients concurrently performing long-running operations on a data set. In this case, the application could perform optimistic validation of an operation before accepting it.

Code
====

Here's a simple context manager for swapping in a new workspace for the basic usage above.

::

    class Workspace(object):
     
        def __init__(self, directory, db):
            self.dir = directory
            self.db = db
     
        def __enter__(self):
            return self.dir.create_or_open(self.db, (u'new',))
     
        def __exit__(self, *exc):
            self._update(self.db)
     
        @fdb.transactional
        def _update(self, tr):
            self.dir.remove(tr, (u'current'))
            self.dir.move(tr, (u'new'), (u'current'))
     
        @property
        def current(self):
            return self.dir.create_or_open(self.db, (u'current',))
