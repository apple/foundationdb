# File-Level Encryption in FoundationDB Backups

This document describes the design and usage of file-level encryption for
FoundationDB backups and restores.

---

## 1. Overview

File-level encryption protects backup data files written to local
directories or object stores (e.g., S3) by encrypting each file with a
user-supplied key before it is uploaded. Restore requires the same key to
be available to all participating processes.

**What is encrypted:**

- All **snapshot**, **range**, and **log** files.

**What is *not* encrypted:**

- The metadata files in the `properties/` folder of the backup container,
  so tools like `fdbbackup describe` and `fdbbackup expire` can work
  without holding the key.
- Anything inside the live cluster — mutations, keys, and values are
  unchanged on the wire and in storage.

**Cipher:** AES-256-GCM. The same key must be supplied to backup, restore,
modify (when re-encrypting), and decode operations.

---

## 2. Encryption Key

The encryption key is a **32-byte (256-bit)** value in raw binary format —
the exact key length AES-256 requires.

The key value itself is **never** passed on the command line. The user
provides the **path** to a file holding the 32 raw bytes via
`--encryption-key-file <PATH>`, and the CLI reads the file to load the key.
The file must contain exactly 32 bytes of raw binary data — no headers, no
encoding, no trailing newline.

For example, a key file can be generated using OpenSSL:

```
openssl rand 32 > key_file
```

---

## 3. CLI Arguments

Two command-line arguments drive the encrypted-backup workflow:

### `--encryption-key-file <PATH>`

Path to the file holding the AES-256 key. Encryption is opt-in: a backup
is encrypted only when this argument is supplied to `fdbbackup start`. The
same argument must be supplied to `fdbrestore` and `fdbdecode` so they can
read the ciphertext.

### `--encryption-block-size <BYTES>`

Block size used to encrypt each data file. Optional on `fdbbackup start`,
defaults to `1048576` (1 MB). The chosen value is recorded in the backup's
metadata so that restore and decode pick it up automatically — those tools
do not accept the flag themselves.

Validation rules on `fdbbackup start`:

- Must be a positive integer; non-numeric or non-positive values are
  rejected with `ERROR: Invalid encryption block size`.
- Must be combined with `--encryption-key-file`. Supplying it without a
  key file is rejected with
  `ERROR: --encryption-block-size option requires --encryption-key-file to be set`.

### Per-command summary

| Command            | `--encryption-key-file` | `--encryption-block-size` | Notes                                                                                  |
| ------------------ | :---------------------: | :-----------------------: | -------------------------------------------------------------------------------------- |
| `fdbbackup start`  | required for encryption | optional, default 1 MB    | Starts a new encrypted backup.                                                         |
| `fdbbackup modify` | conditional             | n/a                       | Required only when the destination URL changes and future files must be re-encrypted. |
| `fdbrestore start` | required if encrypted   | n/a                       | Block size is read from the backup's metadata file, not the CLI.                       |
| `fdbdecode`        | required if encrypted   | n/a                       | Decodes encrypted log files with the supplied key.                                     |

---

## 4. Usage

The same workflow applies whether the destination is S3 or a local file
directory.

### 4.1 Start a backup

`--encryption-block-size` is optional; it defaults to 1 MB (`1048576`)
when omitted. The example below sets it explicitly to half a megabyte
(`524288`):

```
fdbbackup start \
    -C   $CLUSTER_FILE \
    -t   test_backup \
    -w \
    -d   $BACKUP_URL \
    -k   '"" \xff' \
    --log --logdir=$LOG_DIR \
    --blob-credentials $BLOB_CREDENTIALS_FILE \
    --knob_blobstore_encryption_type=aws:kms \
    --encryption-key-file   /root/backup_testing/test_encryption_key_file \
    --encryption-block-size 524288
```

### 4.2 Stop a backup

```
fdbbackup discontinue -C $CLUSTER_FILE -t test_backup
```

### 4.3 Restore a backup

Restore must use the **same key** that was used at backup time:

```
fdbrestore start \
    --dest-cluster-file $CLUSTER_FILE \
    -t   test_backup \
    -w \
    -r   $BACKUP_URL \
    --log --logdir=$LOG_DIR \
    --blob-credentials $BLOB_CREDENTIALS_FILE \
    --knob_blobstore_encryption_type=aws:kms \
    --encryption-key-file /root/backup_testing/test_encryption_key_file
```

`fdbrestore` does **not** take `--encryption-block-size`; the block size
is read from the backup's `file_level_encryption` metadata file.

### 4.4 Modify a backup destination

```
fdbbackup modify \
    -C   $CLUSTER_FILE \
    -t   mybackup \
    -d   $BACKUP_URL \
    --encryption-key-file /root/local_testing/key_file \
    --log --logdir=$LOG_DIR
```

Caveats:

- If the **same URL** is provided with a **different** key, `modify`
  rejects the change to avoid mixing differently-encrypted files at the
  same URL.
- If a **different URL** is provided, the encryption key may be different,
  the same, or omitted entirely.

### 4.5 Decode encrypted log files

```
fdbdecode \
    -r   $BACKUP_URL \
    --encryption-key-file /root/local_testing/key_file
```

`--encryption-key-file` is required only when the backup at `BACKUP_URL`
is encrypted.

---

## 5. Verifying Encryption Status

### 5.1 `fdbbackup describe`

`fdbbackup describe` reports whether a backup is encrypted and the block
size used. **No encryption key is required.**

```
fdbbackup describe \
    -C   $CLUSTER_FILE \
    -d   $BACKUP_URL \
    --blob-credentials $BLOB_CREDENTIALS_FILE
```

Text output:

```
URL: ...
Restorable: true
Partitioned logs: false
File-level encryption: true
Encryption block size: 1048576
Snapshot: ...
```

JSON output (`--json`) adds `FileLevelEncryption` and
`EncryptionBlockSize`:

```json
{
  "SchemaVersion": "1.0.0",
  "URL": "file:///root/local_testing/backup_before/backup-2026-01-06-04-03-08.320838/",
  "Restorable": true,
  "Partitioned": false,
  "FileLevelEncryption": true,
  "EncryptionBlockSize": 1048576,
  "Snapshots": [ ... ]
}
```

### 5.2 `fdbcli` status JSON

Each backup agent reports an array of encryption key file paths along
with read success status, so distribution failures are visible at a
glance:

```json
"encryption_keys": [
  { "path": "/root/local_testing/key1",     "success": false },
  { "path": "/root/local_testing/key_file", "success": true  }
]
```

Each backup tag reports its key file path and encryption status:

```json
"encryption_key_file": "/root/local_testing/key1",
"file_level_encryption": true
```

A complete fragment:

```json
"instances": {
  "4747c54caca01d682a2780d27b7dc9f6": {
    "blob_stats": { ... },
    "configured_workers": 10,
    "encryption_keys": [
      { "path": "/root/local_testing/key1",     "success": false },
      { "path": "/root/local_testing/key_file", "success": true  }
    ],
    "id": "4747c54caca01d682a2780d27b7dc9f6"
  }
},
"tags": {
  "test_backup5": {
    "current_container": "file:///root/backup_testing/backupfolder/backup-2026-01-21-22-47-47.952405",
    "current_status": "has been completed",
    "encryption_key_file": "/root/local_testing/key1",
    "file_level_encryption": true,
    ...
  },
  "test_backup8": {
    "current_container": "file:///root/backup_testing/backupfolder/backup-2026-01-21-22-57-08.832311",
    "current_status": "has been completed",
    "file_level_encryption": false,
    ...
  }
}
```

---

## 6. Errors and Conflicting Scenarios

| Scenario                                                  | Error                                                                                                                            |
| --------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Restoring with a key when backup is **not** encrypted     | `ERROR: Backup is not encrypted, please remove the encryption key file path.`                                                |
| Restoring **without** a key when backup **is** encrypted  | `ERROR: Backup is encrypted, please provide the encryption key file path.`                                                   |
| Restoring with the **wrong** key                          | `ERROR: Failed to read data. Verify that backup and restore encryption keys match (if provided) or the data is corrupted.` |
| `modify` with same URL and different key                  | `Destination URL matches the existing backup URL for tag '...', but the encryption key file does not match. Fatal Error: Backup error` |
| `--encryption-block-size` without `--encryption-key-file` | `ERROR: --encryption-block-size option requires --encryption-key-file to be set`                                                 |
| Invalid `--encryption-block-size` value                   | `ERROR: Invalid encryption block size '<value>'`                                                                                 |

---

## 7. Key Distribution Requirements

The encryption key file **path** is added to the backup container and
persisted in `BackupConfig`. That same path is then propagated to every
process that touches the encrypted files:

- Every **`backup_agent`** host participating in the backup.
- Every **`backup_agent`** host participating in a restore (restore
  workers read the encrypted files and must decrypt them).
- The **`fdbrestore`** client process that drives the restore — it opens
  the backup container directly to read metadata and validate the
  configuration.

Each of these hosts **must** have the **same** encryption key file
stored locally **at the exact same path**. Otherwise individual agents or
the `fdbrestore` process will fail to read or write encrypted files and
the backup/restore will be unable to make progress.

`fdbcli status json` (Section 5.2) is the operational signal for whether
the key file is present and readable on every agent.

---

## 8. Design Internals

### 8.1 What "file-level" means

"File-level" refers to the **scope** of encryption: each whole backup
file (snapshot, range, or log) is the unit that gets encrypted, as
opposed to encrypting individual mutations, keys/values, or live cluster
storage. Mutations and the live cluster are unchanged; only the on-disk
backup artifacts are ciphertext. Filenames, directory layout, and files
under `properties/` stay plaintext so `fdbbackup describe` and
`fdbbackup expire` can work without the key.

### 8.2 Block layout inside a file

Inside each file, the data is split into fixed-size blocks (the
**encryption block size**) and each block is encrypted independently with
AES-256-GCM using the 32-byte key from `--encryption-key-file`. The
blocks are an implementation detail of the AES-GCM stream cipher — not a
change in scope — and they exist so restore can read a small byte range
without decrypting the entire file.

Per-block IVs are derived deterministically: the first 12 bytes come from
a hash of the filename, and the last 4 bytes are the block index. This
gives every block a unique IV while keeping reads seekable. On restore,
the `file_level_encryption` metadata file is read first to recover the
enable flag and the block size, so the reader splits each file into the
exact same blocks that were produced at backup time.

### 8.3 Metadata files

Metadata lives in the `properties/` folder of the backup container and
stays unencrypted.

| File                       | Contents                            | Purpose                                                                                |
| -------------------------- | ----------------------------------- | -------------------------------------------------------------------------------------- |
| `file_level_encryption`    | JSON object (see below)             | Indicates whether the backup is encrypted and the encryption block size used.          |
| `mutation_log_type`        | `"1"` or `"0"` (1 byte)             | Whether the backup uses partitioned mutation logs.                                     |
| `log_begin_version`        | Version number as text (10 bytes)   | Earliest mutation-log version present in the backup.                                   |
| `log_end_version`          | Version number as text (10 bytes)   | Latest mutation-log version present in the backup.                                     |

`file_level_encryption` is a JSON object containing both the on/off flag
and the block size used to encrypt the data files:

```json
{
  "is_encryption_enabled": true,
  "encryption_block_size": 1048576
}
```

- `is_encryption_enabled` — `true` if all snapshot/range/log files are
  encrypted, `false` otherwise.
- `encryption_block_size` — block size in bytes used to encrypt the data
  files, or `0` if the backup is not encrypted. The block size must match
  what restore/decode tools use for the same backup, so it is persisted
  with the backup itself.

The `file_level_encryption` metadata file is created immediately when a
backup starts. The other metadata files are generated only when
`fdbrestore` or `fdbbackup describe` runs. Those operations scan
filenames only, not file contents, when reconstructing the on-disk view.

Example container layout:

```
ls -ltrh properties/
total 16K
-rw-r--r-- 1 root root  74 Sep 15 21:04 file_level_encryption
-rw-r--r-- 1 root root   1 Sep 15 21:06 mutation_log_type
-rw-r--r-- 1 root root  10 Sep 15 21:41 log_end_version
-rw-r--r-- 1 root root  10 Sep 15 21:41 log_begin_version
```
