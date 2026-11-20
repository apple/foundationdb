/*
 * StorageServerRangeTransfer.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "StorageServerRangeTransfer.h"

#include "flow/Coroutines.h"
#include "flow/Trace.h"
/*
    Akanksha: SSTFilePartitioner is provided by RocksDB which calls plugin function whenver compaction occurs.
    An optional plug-in that lets the application add one more rule: "also start a new file here".
    For each key it calls partition plugged in if file needs to be closed or start a new file.
    It's not used for flushes, i.e. memtable → L0.

    In our case, we would use this mechanism to determine when to close the current SST file and start a new one based
on:

    Let's say L6 one File F has no keys between apple and lime.
    But F's range runs from aardvark to zebra, so the new file would overlap it, and files in L6 can't overlap.
    So the new file doesn't fit.
    To reduce compaction, we want to fit in L6 so we split the file into two.



    1. Stage the copied blocks in an SST file instead of writing each key into RocksDB.
        Use RocksDB's SSTFileWriter to write a SST file.
        It just produces a file on disk in the same format RocksDB uses for its own files.
        build SST files ourselves with RocksDB's SstFileWriter.
        folder is <SS data folder>/fetchKeysSstFiles/<fetchKeysID>-0.sst.0
    2. Skip the clear when the range is empty. ????
    3. Decide when to ingest:
        when the staged file reaches 512 MB, or the copy ends.
        Currently it reads block by block and clear that block, and
        add that block to commit write batch until a limit is reached.
        With this change:
        - Add blocks to SST files and close SST file when it reaches 64MB or shard ends.
        - Ingest the SST files into RocksDB when total size is 512MB. (keeping a cap of 512MB)
        - Repeat until all blocks are processed or the copy ends.
    4. Before ingesting, wait until earlier clears are on disk.
    5. Ask RocksDB for "L6 only".
    6. If L6 has no room, make a gap (the split).
    7. If it still doesn't fit, retry for up to 30 seconds.
    8. Fallback: ingest wherever it fits, one at a time, after L0 is small enough.
    9. Clean up staged files if the copy is cancelled, and when the server starts.
    10. Knobs and a test.


    RocksDB compares the new file's first and last key against what each level already has, going from the top down.
    For the new file apple … lime, at each level:
    1. Is a compaction writing into this level, in apple … lime, right now?
        -  If yes, stop: the file must go above this level - 1.
    2. Does this level have any actual keys (or delete markers) between apple and lime?
        - If yes, stop: the new file is newer, so it must sit above them (level - 1)
    3. Is there a gap for it? Do the level's files leave room between apple and lime, with no file's own range covering
that area?
       - If yes, this level is a possible place. Then go on to the next, deeper level.


Example A: it lands in L6

L5:  [aardvark … ant]                 [orange … pear]
L6:  [aardvark … ant]                 [orange … zebra]
new:                 [apple … lime]
- L5: no keys between apple and lime, and there's a gap → possible.
- L6: no keys, and there's a gap → possible.
- → L6 ✓

Example B: L6 has no gap, so it goes to L5

L5:  [aardvark … ant]                 [orange … pear]
L6:  [aardvark ………………………………………………… zebra]      ← no keys in apple … lime, but its range covers it
new:                 [apple … lime]
- L5: possible.
- L6: no keys, but no gap → not possible. It's the last level, so stop.
- → L5. With "L6 or nothing", RocksDB returns TryAgain. This is the case the PR's split fixes.

Example C: old keys in L5, so it goes above L5

L4:  [aardvark … ant]                 [orange … zebra]
L5:  [banana(old) … cherry(old)]                       ← real keys inside apple … lime
L6:  [aardvark … ant]                 [orange … zebra]
new:                 [apple … lime]
- L4: no keys, and a gap → possible.
- L5: has keys in the range → stop. The new file must be above L5.
- → L4. With "L6 or nothing", TryAgain. A split can't help here: those keys really are in the range.

*/

Future<Void> deleteFilesInRemovedRange(IKeyValueStore* kvStore,
                                       KeyRange range,
                                       Future<Void> clearDurable,
                                       std::function<bool()> stillUnassigned,
                                       UID ssId) {
	// Older reads may still be in progress, wait for the clear to complete before deleting files.
	co_await clearDurable;
	// DD may have moved the range back to this server while we waited. If so, it may now hold newly
	// fetched data, so don't delete its files.
	if (!stillUnassigned()) {
		co_return;
	}
	try {
		co_await kvStore->deleteFilesInRange(range);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		// This is only an optimization: the range's clear already removed the data, and compaction reclaims the space.
		TraceEvent(SevWarnAlways, "DeleteFilesInRemovedRangeFailed", ssId).error(e).detail("Range", range);
	}
}
