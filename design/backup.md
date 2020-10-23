### File Backup Design

 This implementation of `BackupAgentBase` implements mechanism to backup keyspace into file. File could be on the local
 file system or a remote blob store. Basic idea is backup task would record KV ranges and mutation logs into a
 container(directory!) to multiple files. Restore would read files from a container and apply KV Ranges and then
 mutation logs.

 Just like any thing else in FoundationDB, Backup-Restore also depend on monotonically increasing versions. Simplest way
 to describe backup would be record KVRanges at a version v0 and record mutation logs from v1 to vn. That gives us the
 capability to restore to any version between v0 and vn. Restore to version vk (v0 <= vk <= vn) will be done by
 * just load KV ranges recorded at v0.
 * if (vk == v0) then no need to replay mutation logs else replay mutation logs from v1 to vk.

 There is a small flaw in the above simplistic design, versions in FoundationDB are short lived (expiring in 5 seconds).
 There is no way to take snapshot. There is no way to record KV Ranges for the complete key space at a given version. For
 a keyspace a-z, its not possible to record KV range (a-z, v0), if keyspace a-z is not small enough. Instead, we can record
 KV ranges {(a-b, v0), (c-d, v1), (e-f, v2) ... (y-z, v10)}. With mutation log recorded all along, we can still use
 the simple backup-restore scheme described above on sub keyspaces seperately. Assuming we did record mutation log from
 v0 to vn, that allows us to restore

* Keyspace a-b to any version between v0 and vn
* Keyspace c-d to any version between v1 and vn
* Keyspace y-z to any version between v10 and vn

But, we are not interested in restoring sub keyspaces, we want to restore a-z. Well, we can restore a-z, to any
version between v10 and vn by restoring individual sub spaces seperately.

#### Key Value Ranges

Backing up KV ranges and restoring them just uses standard client API, tr->getRange() to read KV ranges and tr->set() to
restore them.

####  Mutation Logs

For a backup enabled cluster, proxy keeps a copy of all mutations in system keyspace for backup task to pick them
up and record. During restore, restore task puts the mutations in system keyspace, then proxy reads them and applies
to the database.

#### Restore

As we discussed above KV ranges are for subspaces, where as mutation log is combined for the full keyspace. If we
take the same example

* KV ranges => {(a-b, v0), (c-d, v1), (e-f, v2) ... (y-z, v10)}
* Mutation log => [(a-z, v0), (a-z, v1), .. , (a-z, v10), .. , (a-z, vn)]
  						=> (a-z, vx) represents all mutations on keys a-z at version x.

Restoring to version vk (v10 < vk <= vn), needs KV ranges to be restored first and then replaying mutation logs. For
each KV range (k1-k2, vx) that is restored we need to replay mutation log [(k1-k2, vx+1), .., (k1-k2, vk)]. But, this
needs scanning complete mutation log to get mutations for k1-k2, that is sub-optimal, for any decent sized database
this will take forever.

Instead looking at restore on key space, we can replay events on version space, that way we need to scan
mutation log only once. At each version vx,
* Wait for all the KV ranges recorded before vx to restore first.
* Replay (a-z, vx), but ignore mutations for the keys that are not yet restored by KV ranges.
  Because the keys that are not yet restored by KV ranges will eventually be restored at a later version,
  the mutations on those keys can be safely ignored. 

For the above example, it would look like
* KV ranges:
    * Restore KV ranges in parallel
* Mutation Logs:
    * At v0:
  		* Wait for all KV ranges recorded before v0 to restore - nothing
  		* Nothing to replay as no keys restored by KV ranges yet.
    * At v1:
  		* Wait for all KV ranges recorded before v1 to restore - {(a-b, v0)}
  		* Replay (a-b, v1)
    * At v10:
  		* Wait for all KV ranges recorded before v10 to restore - Everything except (y-z, v10)
  		* Replay (a-x, v10)
    * At v11:
  		* Wait for key range (y-z, v10) to restore.
  		* Replay (a-z, v11)
    * At vk:
  		* Replay (a-z, vk)

Even though, in the above description logging mutations is shown as continuous task, this is actually divided into
  two logical parts. During KV ranges are being backed up, we start a mutation log backup in parallel. Until, we complete
  KV range backup and the parallel mutation logs backup, cluster is not in restorable phase. Once these two tasks are
  completed, now cluster can be restored back to the last version. To be able to restore even after the KV range
  backup, we continue to backup mutation logs called differential log backup. With differential backup, we can restore
  to any version since KV range backup completed. It would look like below

```
   Backup Too large                           x                              x
   Diff backup                 ----------------              -----------------
   Mutations           --------                      ---------
   KV Range            --------                      ---------
   Start             x                            x
   Version space     ---------------------------------------------------------------
                     a b      c               d   e  f       g               h
```

   To explain sequence of events here

|Version|Event|
|-----------|----------------------------------------------------------------------------------|
|           a  |   Asked to start backup|
|           b  |   Started backing up KV ranges and mutation logs|
|           c  |   Completed backing up KV ranges and mutation logs also mark backup restorable|
|          c+1 |   Started differential backup of logs|
|           d  |   Decided backup is too large already and discontinued differential backup|
|           e  |   Asked to start backup|
|           f  |   Started backing up KV ranges and mutation logs|
|           g  |   Completed backing up KV ranges and mutation logs also mark backup restorable|
|          g+1 |   Started differential backup of logs|
|           h  |   Decided backup is too large already and discontinued differential backup|

With the above backup scenario, we could restore to any version from c to d or g to h. No other versions are
   restorable.

#### Continuous Backup

Instead of going through the pain of monitoring for backup becoming too large and restarting backup, we could just
have backup running continuously with KVrange task restarting once in a while (with some policy of course). 
The code for continuous has already committed. The document will be added in the future.
