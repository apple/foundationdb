#####################
Automatic Idempotency
#####################

.. automatic-idempotency:

.. warning :: Automatic idempotency is currently experimental and not recommended for use in production.

Synopsis
~~~~~~~~

Use the ``automatic_idempotency`` transaction option to prevent commits from
failing with ``commit_unknown_result`` at a small performance cost.
``transaction_timed_out`` and ``cluster_version_changed`` still indicate an
unknown commit status.

Details
~~~~~~~

Transactions are generally run in retry loops that retry the error
``commit_unknown_result``. When an attempt fails with
``commit_unknown_result`` it's possible that the attempt succeeded, and the
retry will perform the effect of the transaction twice!  This behavior can be
surprising at first, and difficult to reason
about.

As an example, consider this simple transaction::

    @fdb.transactional
    def atomic_increment(tr, key):
        tr.add(key, struct.pack("<q", 1))

This transaction appears to be correct, and behaves as expected most of the
time, but incorrectly adds to the key multiple times if
``commit_unknown_result`` is thrown.

To mitigate this, it's common to write something unique to a transaction, so
that retries can detect whether or not a commit succeeded by reading the
database and looking for the unique change.

For example::

    def atomic_increment(db, key):
        tr_id = uuid.UUID(int=random.getrandbits(128)).bytes
        # tr_id can be used to detect whether or not this transaction has already committed
        @fdb.transactional
        def do_it(tr):
            if tr[b"ids/" + tr_id] == None:
                tr[b"ids/" + tr_id] = b""
                tr.add(key, struct.pack("<q", 1))
        do_it(db)
        # Clean up unique state
        del db[b"ids/" + tr_id]

This approach has several problems

#. We're now doing about twice the work as before, and we need an extra transaction to clean up.
#. This will slowly leak space over time if clients fail before cleaning up their unique state.

Automatic Idempotency is an implementation of a conceptually similar pattern,
but taking advantage of fdb implementation details to provide better
performance and clean up after itself. Here's our atomic increment with Automatic Idempotency::

    @fdb.transactional
    def atomic_increment(tr, key):
        tr.options.set_automatic_idempotency()
        tr.add(key, struct.pack("<q", 1))

This will correctly add exactly once, provided you allow it to retry until success.

Caveats
~~~~~~~

Automatic Idempotency only prevents ``commit_unknown_result``, and it does not
prevent ``transaction_timed_out`` or ``cluster_version_changed``.
Each of these can indicate an unknown commit status. To mitigate this, the
current recommendation is *do not* retry transactions that fail with
``transaction_timed_out``. The default retry loop does not retry
``transaction_timed_out``, so this is mostly relevant if you have a custom retry
loop. This feature is not recommended for users of the :ref:`multi-version-client-api` at
this time since it does not prevent ``cluster_version_changed``. Support may be added in the future.