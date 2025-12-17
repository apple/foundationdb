.. _restore-validation-testing:

##################################
Restore Validation Testing Guide
##################################

This guide provides step-by-step instructions for testing the restore validation feature in FoundationDB.

Quick Setup
===========

1. Build Required Binaries
---------------------------

::

    cd ~/build_output
    cmake --build . --target fdbserver fdbcli fdbbackup backup_agent -j4

2. Start Test Cluster
---------------------

::

    # Create directories and cluster file
    mkdir -p ~/fdb_test_data ~/fdb_backup
    echo "test:test@127.0.0.1:4500" > ~/fdb_test.cluster

    # Start fdbserver
    ~/build_output/bin/fdbserver -p 127.0.0.1:4500 -d ~/fdb_test_data \
      -C ~/fdb_test.cluster &

    # Configure database
    sleep 3
    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster --exec "configure new single memory"

    # Start backup agent (required for backups)
    sleep 2
    ~/build_output/bin/backup_agent -C ~/fdb_test.cluster &

    sleep 2
    echo "Cluster ready"

Testing Workflow
================

Phase 1: Set Up Test Data
--------------------------

Write Some Test Data
^^^^^^^^^^^^^^^^^^^^

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    # In fdbcli:
    fdb> writemode on
    fdb> set testkey1 testvalue1
    fdb> set testkey2 testvalue2
    fdb> set testkey3 testvalue3
    fdb> set mykey myvalue

    # Verify data
    fdb> getrange "" "\xff"

Phase 2: Backup
---------------

Start Backup
^^^^^^^^^^^^

::

    # Start backup (directory already created in setup)
    ~/build_output/bin/fdbbackup start -C ~/fdb_test.cluster \
      -d file:///Users/stack/fdb_backup -z

    # Check status - wait until backup is restorable
    ~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster

Wait approximately 15-30 seconds until status shows backup is "restorable". The output should include:

- ``BackupURL: file:///Users/stack/fdb_backup/backup-<timestamp>``
- Status showing "completed" or "running differential"

Verify Backup is Ready
^^^^^^^^^^^^^^^^^^^^^^

::

    # Get the specific backup URL from the status output
    ~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster

    # Verify it's restorable (use the actual backup URL from status)
    ~/build_output/bin/fdbbackup describe \
      -d file:///Users/stack/fdb_backup/backup-<timestamp>

Look for ``Restorable: true`` and ``MaxRestorableVersion`` in the output.

Stop the Backup
^^^^^^^^^^^^^^^

Before proceeding with restore validation, stop the backup to ensure no more differential data is being captured:

::

    # Stop the backup (use discontinue to keep the backup data)
    ~/build_output/bin/fdbbackup discontinue -C ~/fdb_test.cluster

    # Verify backup is stopped
    ~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster

.. note::
   Use ``discontinue`` rather than ``abort`` to preserve the backup data. The ``discontinue`` command stops the backup gracefully while keeping it restorable.

Phase 3: Restore to Validation Prefix
--------------------------------------

Lock the Database
^^^^^^^^^^^^^^^^^

Before starting the restore, lock the database to prevent any writes during validation:

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    # Lock the database - save the UID returned!
    fdb> lock

The lock command returns a UID that you'll need to unlock the database later.

Restore with Prefix
^^^^^^^^^^^^^^^^^^^

**Important**: Use the SPECIFIC backup URL from the previous step (not the parent directory):

::

    # Use fdbrestore with the actual backup URL
    ~/build_output/bin/fdbrestore start \
      -r file:///Users/stack/fdb_backup/backup-<timestamp> \
      --dest-cluster-file ~/fdb_test.cluster \
      --add-prefix "\xff\x02/rlog/" \
      -w

.. warning::
   Using ``file:///Users/stack/fdb_backup`` (parent dir) instead of the full backup path will fail with "not restorable to any version".

The ``\xff\x02/rlog/`` prefix is the ``validateRestoreLogKeys`` range where validation looks for restored data.

.. important::
   **Restore will clear and overwrite any existing data at the destination range.** This is standard restore behavior. The restore process explicitly clears the destination range before writing restored data, so any pre-existing keys in ``\xff\x02/rlog/`` will be replaced.

Verify Restored Data Exists
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    # In fdbcli, enable system keys to see restored data
    fdb> option on ACCESS_SYSTEM_KEYS
    fdb> getrange "\xff\x02/rlog/" "\xff\x02/rlog0"

    # You should see your keys with the prefix
    # e.g., "\xff\x02/rlog/testkey1" -> "testvalue1"

Phase 4: Run Validation
------------------------

Start Validation Audit
^^^^^^^^^^^^^^^^^^^^^^

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    # Start validation for entire key range
    fdb> audit_storage validate_restore "" "\xff"

This returns an Audit ID. **Save this ID!**

Example output::

    Audit ID: 12345678-1234-5678-1234-567812345678

Monitor Progress
^^^^^^^^^^^^^^^^

::

    # Check overall status
    fdb> get_audit_status validate_restore id <AuditID>

    # Check detailed progress (shows which ranges are complete)
    fdb> get_audit_status validate_restore progress <AuditID>

    # Check for any errors
    fdb> get_audit_status validate_restore phase error

Wait for Completion
^^^^^^^^^^^^^^^^^^^

Keep checking status until the audit completes. For a small dataset, this should take seconds to minutes.

Phase 5: Verify Results
------------------------

Check Audit Status
^^^^^^^^^^^^^^^^^^

::

    fdb> get_audit_status validate_restore id <AuditID>

**Expected Output (Success)**::

    Audit result is:
    AuditStorageState: [ID]: <AuditID>, [Range]: ["","\\xff"), [Type]: 5, [Phase]: 2

Where:

- Type: 5 = ValidateRestore
- Phase: 2 = Complete (no errors)

**If Phase: 3 = Error**, there were validation failures!

Check Trace Logs
^^^^^^^^^^^^^^^^

Look for validation events in the server logs::

    grep "AuditRestore" ~/fdb_test_data/*.log | tail -20

Look for:

- ``SSAuditRestoreBegin`` - Validation started
- ``SSAuditRestoreComplete`` - Validation finished successfully
- ``SSAuditRestoreError`` - Validation found errors (check details!)

Phase 6: Testing a Failed Audit
--------------------------------

To verify that the audit correctly detects mismatches, you can intentionally modify the source data and rerun the audit.

Modify Source Data
^^^^^^^^^^^^^^^^^^

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    fdb> writemode on

    # Modify one of the original source values to create a mismatch
    fdb> set testkey1 "modified_value"

    # Verify the change
    fdb> get testkey1

This creates a mismatch because:

- Current source data: ``testkey1`` = ``modified_value`` (modified after backup)
- Restored data: ``\xff\x02/rlog/testkey1`` = ``testvalue1`` (from backup)

Run Audit Again
^^^^^^^^^^^^^^^

::

    # Start a new validation audit
    fdb> audit_storage validate_restore "" "\xff"

Save the new Audit ID returned.

Check for Expected Failure
^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    # Monitor the audit status
    fdb> get_audit_status validate_restore id <NewAuditID>

**Expected Output (Failure)**::

    Audit result is:
    AuditStorageState: [ID]: <AuditID>, [Range]: ["","\\xff"), [Type]: 5, [Phase]: 3

Where:

- Phase: 3 = Error (validation found mismatches)

Check Error Details
^^^^^^^^^^^^^^^^^^^

::

    # Check detailed error information
    fdb> get_audit_status validate_restore phase error

    # Check trace logs for specific error details
    grep "SSAuditRestoreError" ~/fdb_test_data/*.log | tail -20

The logs should show which key had a mismatch and what the differing values were.

Restore Correct Source Data for Next Tests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

    # Restore the original source value to match the backup
    fdb> writemode on
    fdb> set testkey1 testvalue1

    # Verify the restoration
    fdb> get testkey1

Phase 7: Understanding Audit Design and Limitations
----------------------------------------------------

.. important::
   The restore validation audit is designed to run immediately after a restore operation to verify the restore process didn't corrupt data. It compares the current database state with the restored data at the time of the audit.

.. important::
   **Lock the database before starting restore validation.** To avoid false positive audit errors caused by writes occurring between the backup snapshot and audit completion, you should lock the database before getting the restorableVersion and keep it locked until the audit completes. Any writes to the source data during this window will cause audit mismatches that are difficult to distinguish from actual restore corruption issues.

   **Both restore and audit operations will continue to work on a locked database.** They use ``LOCK_AWARE`` transactions internally, which allows system operations to proceed while the lock prevents regular application writes. This is the recommended approach for accurate validation.

   ::

       # Lock the database before restore
       fdb> lock

       # Restore and audit will work - they use LOCK_AWARE transactions
       # Regular application writes are blocked, preventing false positives

       # Proceed with restore and validation
       # ... perform restore ...
       # ... run audit ...

       # Unlock after audit completes
       fdb> unlock <UID from lock command>

What the Audit Validates
^^^^^^^^^^^^^^^^^^^^^^^^^

The audit ensures:

✅ The restore process worked without corruption during the restore operation

✅ Current source data matches the restored data at the time of audit

✅ No data corruption occurred between source and restored locations

What the Audit Does NOT Validate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The audit has these design limitations:

❌ It does NOT detect if source data changed after the backup was created

❌ It compares current source data to restored data, not backup data to restored data

**Why This Matters**:

If you:

1. Create a backup with ``testkey1=value1``
2. Modify source data to ``testkey1=value2``
3. Restore the backup (restores ``testkey1=value1`` to ``\xff\x02/rlog/``)
4. Run the audit

The audit will report an ERROR because current source (``value2``) doesn't match restored (``value1``). This is expected behavior - the audit validates restore integrity by comparing current state to restored state.

When to Use Restore Validation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use restore validation:

✅ Immediately after completing a restore operation

✅ To verify the restore process worked correctly

✅ To ensure no corruption during data transfer

Do NOT use restore validation:

❌ To verify backup data integrity (use backup verification tools instead)

❌ To check if source data matches the backup (they're expected to diverge)

❌ As a long-term consistency check between source and restored data

Phase 8: Cleanup
-----------------

Unlock the Database
^^^^^^^^^^^^^^^^^^^

After validation is complete, unlock the database to allow normal operations:

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    # Unlock using the UID from the lock command
    fdb> unlock <UID from lock command>

Clear Restored Data
^^^^^^^^^^^^^^^^^^^

::

    ~/build_output/bin/fdbcli -C ~/fdb_test.cluster

    fdb> option on ACCESS_SYSTEM_KEYS
    fdb> writemode on
    fdb> clearrange "\xff\x02/rlog/" "\xff\x02/rlog0"

Verify Cleanup
^^^^^^^^^^^^^^

::

    fdb> getrange "\xff\x02/rlog/" "\xff\x02/rlog0"
    # Should return empty

Troubleshooting
===============

"restore_destination_not_empty" Error
--------------------------------------

**Symptom**: Restore fails saying destination is not empty

**Cause**: The restore is not using a prefix (addPrefix parameter)

**Fix**: When restoring for validation, you must use the ``addPrefix`` parameter to restore 
to a different keyspace (e.g., ``\xff\x02/rlog/``). This bypasses the empty destination check 
that's enforced for regular restores (which protect against accidental data loss).

**Note**: All restores (with or without prefix) will clear and overwrite the destination range. 
The difference is that regular restores (no prefix) check for existing data and fail to prevent 
accidents, while validation restores (with prefix) proceed because they're intentionally writing 
to a dedicated validation keyspace.

"No backup agents are responding"
----------------------------------

**Symptom**: After running ``fdbbackup start``, you see this message

**Cause**: No backup agent process running to execute the backup

**Fix**::

    # Start backup agent daemon
    ~/build_output/bin/backup_agent -C ~/fdb_test.cluster &

    # Wait a moment, then check backup status
    sleep 5
    ~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster

"The specified backup is not restorable to any version"
--------------------------------------------------------

**Symptom**: Restore fails immediately with this error

**Causes**:

1. **Wrong backup URL**: Using parent directory instead of specific backup path
2. **Backup not complete**: Backup hasn't finished creating a restorable snapshot

**Fix**::

    # 1. Get the correct backup URL from status
    ~/build_output/bin/fdbbackup status -C ~/fdb_test.cluster
    # Look for: BackupURL: file:///.../backup-<timestamp>

    # 2. Use that EXACT URL in restore command
    ~/build_output/bin/fdbrestore start \
      -r file:///Users/stack/fdb_backup/backup-2025-11-18-09-36-09.156836 \
      --dest-cluster-file ~/fdb_test.cluster \
      --add-prefix "\xff\x02/rlog/" -w

Backup Not Restorable
----------------------

**Symptom**: Restore fails with "not restorable to any version"

**Cause**: Backup hasn't completed or saved a snapshot yet

**Fix**:

- Wait longer (10-30 seconds minimum)
- Check ``fdbbackup status`` for restorable version
- Ensure backup agent is running

No Progress Updates
-------------------

**Symptom**: Audit status stays in "Running" phase forever

**Cause**: Storage servers may not have the shard containing your key range

**Fix**::

    # Check if data distribution is working
    fdbcli> status details
    # Look for storage servers and their shard assignments

Cannot See Restored Data
-------------------------

**Symptom**: ``getrange "\xff\x02/rlog/"`` returns empty

**Cause**: Need to enable system keys access

**Fix**::

    fdb> option on ACCESS_SYSTEM_KEYS

Validation Completes but No Logs
---------------------------------

**Symptom**: Can't find trace events

**Cause**: Logs may be in different location

**Fix**::

    # Find log location
    ps aux | grep fdbserver
    # Look for -L or --logdir parameter

    # Or check default locations
    ls -ltr /var/log/foundationdb/
    ls -ltr ~/fdb_test_data/*.log

Expected Performance
====================

- **Small dataset (100s of keys)**: Seconds
- **Medium dataset (10K keys)**: 1-5 minutes
- **Large dataset (1M+ keys)**: Hours (rate limited)

Rate limiting is controlled by ``AUDIT_STORAGE_RATE_PER_SERVER_MAX`` (default: 50MB/s per server).
