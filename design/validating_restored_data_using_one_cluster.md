# Validating restored data with source in the same cluster

Author: Neethu Haneeshi Bingi

## Goal:

The goal is to verify the restored data from a backup with the original source data to ensure the end-to-end correctness of backup and restore flow. While this validation is currently performed in simulation, it is limited in scale and differs from the production backup environment.
This new validation flow should:

* Run **quickly as possible and automatically**, enabling execution at regular intervals like bi-weekly or with every patch release.
* Increase confidence in **backup/restore reliability** by testing this in production-like clusters especially as upcoming backup and restore using bulk-loading projects evolve over the next year.
* Should be able to validate data for smaller key ranges for faster turn around time and be able to test the entire database data.



## Usage:

* Workflow running for smaller keyRanges once per week. Workflow can take multiple keyRange boundaries from getLocation fdbcli api output and will run the rest of the workflow steps. Should be able to complete within 3-4 hours.
* Workflow running for entire DB data every patch release/once bi-monthly. This has a constraint for the cluster to be only 35% filled, so target to be able to complete in 5 days. 

By doing this, we can validate data reliability in this path and also the restore speed for any regressions.


## **High-Level Idea**

Store both **source** and **restored** data within the same cluster to eliminate the need for a separate validation cluster.
Every key-value pair will be compared directly, and exact key/value **corruptions or mismatches** will be reported.



## **Solution:**

More detailed steps followed by the diagram
[Image: Screenshot 2025-11-12 at 7.53.04 PM.png]
The validation consists of **three main steps** — **Backup**, **Restore**, and **Compare** — followed by a **Cleanup** phase.

* Backup: 
    * Run workload → Start Backup → Stop load  → NoteDown ReadVersion → Lock cluster for writes → Wait until (ReadVersion saved > MaxRestorableVersion) → Stop Backup 
    * Constraint: If you running this validate entire DB data, ensure cluster has >65% available space in this phase. If space drops below this, stop/clear the load.
        Note: Locking the cluster still allows backup and restore operations
* Restore:
    * Setup: Use the `addPrefix` parameter to restore into a validation keyspace. When restoring with a prefix, the restore destination empty check is automatically bypassed.
    * GetMaxRestoreVersion from backup → Start restore with add-prefix option → Wait for restore completion
    * Note: 
        * Restore writes into a predefined restore_data_prefix (**/xff/x02/rlog**). Restore does lock-aware transactions to bypass the lock. Restore already supports option to add prefix to data.
        * **Important**: Restore will clear and overwrite any existing data at the destination range. This is standard restore behavior.
        * The restore should work for smaller key ranges or for entire db data.
* Compare:
    * Run fdbcli ‘audit_storage’ command with AuditType=validate_restore
        * `audit_storage validate_restore [BeginKey] [EndKey]`  (beginKey,endKey should be within userKeyRange)
            * BeginKey and Endkey should be same range given to restore command.
        * DD dispatches validation tasks to SS and monitors the progress
        * Each SS compares data and updates the results in the DB.
    * Monitor the audit storage status, fdbcli command
        *  `get_audit_status validate_restore progress [AuditID]`
    * Once complete/error, check for corruptions
* Clean up:
    * Clear up the restored data in restored_data_prefix
    * Clear off the setup in restore phase
    * Start backup
    * Unlock cluster for writes.

**Note:** 

* Backup–Restore–Compare steps can be automated as a single script/workflow, while Cleanup can be managed via a separate script/workflow.
* The validate_restore process compares user keys against the restored data, but not the other way around. As a result, it can confirm that all user keys were successfully restored, but cannot detect any extra keys that may exist in the restored data

## 
Alternative Design Considerations

* Validate two databases content are same:  Might be slow. Needs an external process like Consistency Checker to compare two databases. With our availability infra, difficult to have two databases
* Validate checksum of two databases: [Comparing FDB Contents Using Merkle-Tree MD5](https://quip-apple.com/1x2jAMZJP5YB). Detects inconsistency but cannot pinpoint which key/value differs.



## Implementation Details

* **Locking/Unlocking database:** Use the similar api's restore uses locking and unlocking database. [Lock](https://github.com/apple/foundationdb/blob/release-7.4/fdbclient/FileBackupAgent.actor.cpp#L6754) [Unlock](https://github.com/apple/foundationdb/blob/release-7.4/fdbclient/FileBackupAgent.actor.cpp#L4286). Don't allow the restore to unlockDB as we want lock the database until the comparison is done. Restore does lock-aware transactions to bypass this lock.
* **Wait until (ReadVersion saved > MaxRestorableVersion) step** in Backup phase: There might be small gap before we save readVersion and lock the DB. Ensure to wait for at least backup_lag_seconds not to miss any mutations in the backup
* **Restore destination check:** The restore empty destination check is bypassed when `addPrefix.size() > 0` (indicating a validation restore to a prefixed keyspace). For regular restores without a prefix, the check remains enforced to prevent accidental data loss. Note that all restores (validation or regular) will clear and overwrite any existing data at the destination range - this is standard restore behavior.
* **Audit Storage:**
    * Default value of BeginKey and EndKey is normalKeys.begin and normalKeys.end. Validate both keys are in normalKeys/userKeys range, systemKeys should be not included as they are in the backup.
    * Add new AuditType **ValidateRestore.**
    * New audit actor auditRestoreQ() in StorageServer similar to auditStorageShardReplicaQ()
        * Change the code to read the range appended with the prefix restored_data_prefix. Can compare, update metadata and error mechanism in the same way. [Range](https://github.com/apple/foundationdb/blob/release-7.4/fdbserver/storageserver.actor.cpp#L5671)


