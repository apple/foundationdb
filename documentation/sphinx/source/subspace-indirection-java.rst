####################
Subspace Indirection
####################

:doc:`Python <subspace-indirection>` **Java**

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

For a single client, we can use a simple Workspace class to handle creation of a new subspace and transactionally swapping it into place. Rather than working with a subspace directly, a client accesses the current subspace through a managed reference as follows:

.. code-block:: java

    Database db = fdb.open();
    Future<DirectorySubspace> workingDir = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from("working"));
    Workspace workspace = new Workspace(workingDir.get(), db);
    final DirectorySubspace current = workspace.getCurrent().get();

The client performs transactions on data in the subspace current in the usual manner. When we want a new workspace for a bulk load or other long-running operation, we create one with a workspace:

.. code-block:: java

    final DirectorySubspace newspace = workspace.getNew().get();
    try {
        clearSubspace(db, newspace);
        db.run(tr -> {
            tr.set(newspace.pack(Tuple.from(3)),Tuple.from("c").pack());
            tr.set(newspace.pack(Tuple.from(4)), Tuple.from("d").pack());
            return null;
        });
    } finally {
        // Asynchronous operation--wait until result is reached.
        workspace.replaceWithNew().blockUntilReady();
    }

When the workspace completes, it transactionally replaces the current subspace with the new one.

Extensions
==========

*Multiple Clients*

Beyond the ability to load and transactionally swap in a single new data set, an application may want to support multiple clients concurrently performing long-running operations on a data set. In this case, the application could perform optimistic validation of an operation before accepting it.

Code
====

Here's a simple Workspace class for swapping in a new workspace supporting the basic usage above.

.. code-block:: java

    public static class Workspace {
        private final Database db;
        private final DirectorySubspace dir;
        public Workspace(DirectorySubspace directory, Database db){
            this.dir = directory;
            this.db = db;
        }
        public Future<DirectorySubspace> getCurrent() {
            return dir.createOrOpen(this.db, PathUtil.from("current"));
        }
        public Future<DirectorySubspace> getNew() {
            return dir.createOrOpen(this.db, PathUtil.from("new"));
        }
        public Future<DirectorySubspace> replaceWithNew() {
            return this.db.runAsync(tr -> {
                return dir.remove(tr, PathUtil.from("current")) // Clear the old current.
                .flatMap(() -> {
                    // Replace the old directory with the new one.
                    return dir.move(tr, PathUtil.from("new"), PathUtil.from("current"));
                });
            });
        }
    }
