## FDB Backup Data Format

### Introduction
This document describes the data format of the files generated by FoundationDB (FDB) backup procedure.
The target readers who may benefit from reading this document are:
* who make changes on the current backup or restore procedure;
* who writes tools to digest the backup data for analytical purpose;
* who wants to understand the internals of how backup and restore works.

The description of the backup data format is based on FDB 5.2 to FDB 6.1. The backup data format may (although unlikely) change after FDB 6.1.


### Files generated by backup
The backup procedure generates two types of files: range files and log files.
* A range file  describes key-value pairs in a range at the version when the backup process takes a snapshot of the range. Different range files have data for different ranges at different versions.
* A log file describes the mutations taken from a version v<sub>1</sub> to v<sub>2</sub>  during the backup procedure.

With the key-value pairs in range file and the mutations in log file, the restore procedure can restore the database into a consistent state at a user-provided version v<sub>k</sub>  if the backup data is claimed by the restore as restorable at  v<sub>k</sub>. (The details of determining if a set of backup data is restorable at a version is out of scope of this document and can be found at [backup.md](https://github.com/xumengpanda/foundationdb/blob/cd873831ecd18653c5bf459d6f72d14a99b619c4/design/backup.md).


### Filename conventions
The backup files will be saved in a directory (i.e., url) specified by users. Under the directory, the range files are in the `snapshots` folder. The log files are in the `logs` folder.

The convention of the range filename is ` snapshots/snapshot,beginVersion,beginVersion,blockSize`, where `beginVersion` is the version when the key-values in the range file are recorded, and blockSize is the size of data blocks in the range file.

The convention of the log filename is  `logs/,versionPrefix/log,beginVersion,endVersion,randomUID, blockSize`, where the versionPrefix is a 2-level path (`x/y`) where beginVersion should go such that `x/y/*` contains (10^smallestBucket) possible versions; the randomUID is a random UID, the `beginVersion` and `endVersion` are the version range (left inclusive, right exclusive) when the mutations are recorded; and the `blockSize` is the data block size in the log file.

We will use an example to explain what each field in the range and log filename means.
Suppose under the backup directory, we have a range file `snapshots/snapshot,78994177,78994177,97` and a log file `logs/0000/0000/log,78655645,98655645,149a0bdfedecafa2f648219d5eba816e,1048576`.
The range file’s filename tells us that all key-value pairs decoded from the file are the KV value in DB at the version `78994177`.  The data block size is `97` bytes.
The log file’s filename tells us that the mutations in the log file were the mutations in the DB during the version range `[78655645,98655645)`, and the data block size is `1048576` bytes.


### Data format in a range file
A range file can have one to many data blocks. Each data block has a set of key-value pairs.
A data block is encoded as follows: `Header startKey k1v1 k2v2 Padding`.


    Example:

    The client code writes keys in this sequence:
             a c d e f g h i j z
    The backup procedure records the key-value pairs in the database into range file.

    H = header   P = padding   a...z = keys  v = value | = block boundary

    Encoded file:  H a cv dv P | H e ev fv gv hv P | H h hv iv jv z
    Decoded in blocks yields:
               Block 1: range [a, e) with kv pairs cv, dv
               Block 2: range [e, h) with kv pairs ev, fv, gv
               Block 3: range [h, z) with kv pairs hv, iv, jv

NOTE: All blocks except for the final block will have one last  value which will not be used.  This isn't actually a waste since if the next KV pair wouldn't fit within the block after the value  then the space after the final key to the next 1MB boundary would just be padding anyway.

The code related to how a range file is written is in the `struct RangeFileWriter` in `namespace fileBackup`.

The code that decodes a range block is in `ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file, int64_t offset, int len, Database cx)`.


### Data format in a log file
A log file can have one to many data blocks.
Each block is encoded as  `Header, [Param1, Param2]... padding`.
The first 32bits in `Param1` and `Param2` specifies the length of the `Param1` and `Param2`.
`Param1` specifies the version when the mutations happened;
`Param2` encodes the group of mutations happened at the version.

Note that if the group of mutations is bigger than the block size, the mutation group will be split across multiple data blocks.
For example, we may get `[Param1, Param2_part0]`, `[Param1, Param2_part1]`. By concatenating the `Param2_part0` and `Param2_part1`, we can get the group of all mutations happened in the version specified in `Param1`.

The encoding format for `Param1` is as follows:
`hashValue|commitVersion|part`,
where `hashValue` is the hash of the commitVersion, `commitVersion` is the version when the mutations in `Param2`(s) are taken, and `part` is the part number in case we need to concatenate the `Param2` to get the group of all mutations.
 `hashValue` takes 8bits,  `commitVersion` takes 64bits, and `part` takes 32bits.

Note that in case of concatenating the partial group of mutations in `Param2` to get the full group of all mutations, the part number should be continuous.

The encoding format for the group of mutations, which is Param2 or the concatenated Param2 in case of partial group of mutations in a block,  is as follows:
`length_of_the_mutation_group | encoded_mutation_1 | … | encoded_mutation_k`.
The `encoded_mutation_i` is encoded as follows
      `type|kLen|vLen|Key|Value`
where type is the mutation type, such as Set or Clear, `kLen` and `vLen` respectively are the length of the key and value in the mutation. `Key` and `Value` are the serialized value of the Key and Value in the mutation.

The code related to how a log file is written is in the `struct LogFileWriter` in `namespace fileBackup`.

The code that decodes a mutation block is in `ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeLogFileBlock(Reference<IAsyncFile> file, int64_t offset, int len)`.


### Endianness
When the restore decodes a serialized integer from the backup file, it needs to convert the serialized value from big endian to little endian.

The reason is as follows: When the backup procedure transfers the data to remote blob store, the backup data is encoded in big endian. However, FoundationDB currently only run on little endian machines. The endianness affects the interpretation of an integer, so we must perform the endianness conversion.