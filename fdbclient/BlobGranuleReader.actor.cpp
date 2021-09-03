/*
 * BlobGranuleReader.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/AsyncFileS3BlobStore.actor.h"
#include "fdbclient/Atomic.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/SystemData.h" // for allKeys unit test - could remove
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// TODO more efficient data structure besides std::map? PTree is unecessary since this isn't versioned, but some other
// sorted thing could work. And if it used arenas it'd probably be more efficient with allocations, since everything
// else is in 1 arena and discarded at the end.

// TODO could refactor the file reading code from here and the delta file function into another actor,
// then this part would also be testable? but meh

#define BG_READ_DEBUG false

ACTOR Future<Arena> readSnapshotFile(Reference<BackupContainerFileSystem> bstore,
                                     BlobFilenameRef f,
                                     KeyRangeRef keyRange,
                                     std::map<KeyRef, ValueRef>* dataMap) {
	try {
		state Arena arena;
		// printf("Starting read of snapshot file %s\n", filename.c_str());
		state Reference<IAsyncFile> reader = wait(bstore->readFile(f.filename.toString()));
		// printf("Got snapshot file size %lld\n", size);
		state uint8_t* data = new (arena) uint8_t[f.length];
		// printf("Reading %lld bytes from snapshot file %s\n", size, filename.c_str());
		int readSize = wait(reader->read(data, f.length, f.offset));
		// printf("Read %lld bytes from snapshot file %s\n", readSize, filename.c_str());
		ASSERT(f.length == readSize);

		// weird stuff for deserializing vector and arenas
		Arena parseArena;
		GranuleSnapshot snapshot;
		StringRef dataRef(data, f.length);
		ArenaObjectReader rdr(arena, dataRef, Unversioned());
		rdr.deserialize(FileIdentifierFor<GranuleSnapshot>::value, snapshot, parseArena);
		arena.dependsOn(parseArena);

		// GranuleSnapshot snapshot = ObjectReader::fromStringRef<GranuleSnapshot>(dataRef, Unversioned();)
		// printf("Parsed %d rows from snapshot file %s\n", snapshot.size(), filename.c_str());

		// TODO REMOVE sanity check eventually
		for (int i = 0; i < snapshot.size() - 1; i++) {
			if (snapshot[i].key >= snapshot[i + 1].key) {
				printf("BG SORT ORDER VIOLATION IN SNAPSHOT FILE: '%s', '%s'\n",
				       snapshot[i].key.printable().c_str(),
				       snapshot[i + 1].key.printable().c_str());
			}
			ASSERT(snapshot[i].key < snapshot[i + 1].key);
		}

		int i = 0;
		while (i < snapshot.size() && snapshot[i].key < keyRange.begin) {
			/*if (snapshot.size() < 10) { // debug
			    printf("  Pruning %s < %s\n", snapshot[i].key.printable().c_str(), keyRange.begin.printable().c_str());
			}*/
			i++;
		}
		while (i < snapshot.size() && snapshot[i].key < keyRange.end) {
			dataMap->insert({ snapshot[i].key, snapshot[i].value });
			/*if (snapshot.size() < 10) { // debug
			    printf("  Including %s\n", snapshot[i].key.printable().c_str());
			}*/
			i++;
		}
		/*if (snapshot.size() < 10) { // debug
		    while (i < snapshot.size()) {
		        printf("  Pruning %s >= %s\n", snapshot[i].key.printable().c_str(), keyRange.end.printable().c_str());
		        i++;
		    }
		}*/
		if (BG_READ_DEBUG) {
			printf("Started with %d rows from snapshot file %s after pruning to [%s - %s)\n",
			       dataMap->size(),
			       f.toString().c_str(),
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str());
		}

		return arena;
	} catch (Error& e) {
		printf("Reading snapshot file %s got error %s\n", f.toString().c_str(), e.name());
		throw e;
	}
}

ACTOR Future<Standalone<GranuleDeltas>> readDeltaFile(Reference<BackupContainerFileSystem> bstore,
                                                      BlobFilenameRef f,
                                                      KeyRangeRef keyRange,
                                                      Version readVersion) {
	try {
		// printf("Starting read of delta file %s\n", filename.c_str());
		state Standalone<GranuleDeltas> result;
		state Reference<IAsyncFile> reader = wait(bstore->readFile(f.filename.toString()));
		// printf("Got delta file size %lld\n", size);
		state uint8_t* data = new (result.arena()) uint8_t[f.length];
		// printf("Reading %lld bytes from delta file %s into %p\n", size, filename.c_str(), data);
		int readSize = wait(reader->read(data, f.length, f.offset));
		// printf("Read %d bytes from delta file %s\n", readSize, filename.c_str());
		ASSERT(f.length == readSize);

		// Don't do range or version filtering in here since we'd have to copy/rewrite the deltas and it might starve
		// snapshot read task, do it in main thread

		// weirdness with vector refs and arenas here
		Arena parseArena;
		StringRef dataRef(data, f.length);
		ArenaObjectReader rdr(result.arena(), dataRef, Unversioned());
		rdr.deserialize(FileIdentifierFor<GranuleDeltas>::value, result.contents(), parseArena);
		result.arena().dependsOn(parseArena);

		if (BG_READ_DEBUG) {
			printf("Parsed %d deltas from delta file %s\n", result.size(), f.toString().c_str());
		}

		// TODO REMOVE sanity check
		for (int i = 0; i < result.size() - 1; i++) {
			if (result[i].version > result[i + 1].version) {
				printf("BG VERSION ORDER VIOLATION IN DELTA FILE: '%lld', '%lld'\n",
				       result[i].version,
				       result[i + 1].version);
			}
			ASSERT(result[i].version <= result[i + 1].version);
		}

		return result;
	} catch (Error& e) {
		printf("Reading delta file %s got error %s\n", f.toString().c_str(), e.name());
		throw e;
	}
}

// TODO this giant switch is mostly lifted from storage server.
// Could refactor atomics to have a generic "handle this atomic mutation" thing instead of having to duplicate code with
// the switch statement everywhere?
static void applyDelta(std::map<KeyRef, ValueRef>* dataMap, Arena& ar, KeyRangeRef keyRange, MutationRef m) {
	if (m.type == MutationRef::ClearRange) {
		if (m.param2 <= keyRange.begin || m.param1 >= keyRange.end) {
			return;
		}
		// keyRange is inclusive on start, lower_bound is inclusive with the argument, and erase is inclusive for the
		// begin. So if lower bound didn't find the exact key, we need to go up one so it doesn't erase an extra key
		// outside the range.
		std::map<KeyRef, ValueRef>::iterator itStart = dataMap->lower_bound(m.param1);
		if (itStart != dataMap->end() && itStart->first < m.param1) {
			itStart++;
		}

		// keyRange is exclusive on end, lower bound is inclusive with the argument, and erase is exclusive for the end
		// key. So if lower bound didn't find the exact key, we need to go up one so it doesn't skip the last key it
		// should erase
		std::map<KeyRef, ValueRef>::iterator itEnd = dataMap->lower_bound(m.param2);
		if (itEnd != dataMap->end() && itEnd->first < m.param2) {
			itEnd++;
		}
		dataMap->erase(itStart, itEnd);
	} else {
		if (m.param1 < keyRange.begin || m.param1 >= keyRange.end) {
			return;
		}
		std::map<KeyRef, ValueRef>::iterator it = dataMap->find(m.param1);
		if (m.type != MutationRef::SetValue) {
			Optional<StringRef> oldVal;
			if (it != dataMap->end()) {
				oldVal = it->second;
			}

			switch (m.type) {
			case MutationRef::AddValue:
				m.param2 = doLittleEndianAdd(oldVal, m.param2, ar);
				break;
			case MutationRef::And:
				m.param2 = doAnd(oldVal, m.param2, ar);
				break;
			case MutationRef::Or:
				m.param2 = doOr(oldVal, m.param2, ar);
				break;
			case MutationRef::Xor:
				m.param2 = doXor(oldVal, m.param2, ar);
				break;
			case MutationRef::AppendIfFits:
				m.param2 = doAppendIfFits(oldVal, m.param2, ar);
				break;
			case MutationRef::Max:
				m.param2 = doMax(oldVal, m.param2, ar);
				break;
			case MutationRef::Min:
				m.param2 = doMin(oldVal, m.param2, ar);
				break;
			case MutationRef::ByteMin:
				m.param2 = doByteMin(oldVal, m.param2, ar);
				break;
			case MutationRef::ByteMax:
				m.param2 = doByteMax(oldVal, m.param2, ar);
				break;
			case MutationRef::MinV2:
				m.param2 = doMinV2(oldVal, m.param2, ar);
				break;
			case MutationRef::AndV2:
				m.param2 = doAndV2(oldVal, m.param2, ar);
				break;
			case MutationRef::CompareAndClear:
				if (oldVal.present() && m.param2 == oldVal.get()) {
					m.type = MutationRef::ClearRange;
					m.param2 = keyAfter(m.param1, ar);
					applyDelta(dataMap, ar, keyRange, m);
				};
				return;
			}
		}
		if (it == dataMap->end()) {
			dataMap->insert({ m.param1, m.param2 });
		} else {
			it->second = m.param2;
		}
	}
}

// TODO might want to change this to an actor so it can yield periodically?
static void applyDeltas(std::map<KeyRef, ValueRef>* dataMap,
                        Arena& arena,
                        GranuleDeltas deltas,
                        KeyRangeRef keyRange,
                        Version readVersion) {
	for (MutationsAndVersionRef& delta : deltas) {
		if (delta.version > readVersion) {
			break;
		}
		for (auto& m : delta.mutations) {
			applyDelta(dataMap, arena, keyRange, m);
		}
	}
}

ACTOR Future<RangeResult> readBlobGranule(BlobGranuleChunkRef chunk,
                                          KeyRangeRef keyRange,
                                          Version readVersion,
                                          Reference<BackupContainerFileSystem> bstore,
                                          Optional<BlobWorkerStats *> stats) {

	// TODO REMOVE with V2 of protocol
	ASSERT(readVersion == chunk.includedVersion);
	// Arena to hold all allocations for applying deltas. Most of it, and the arenas produced by reading the files,
	// will likely be tossed if there are a significant number of mutations, so we copy at the end instead of doing a
	// dependsOn.
	// FIXME: probably some threshold of a small percentage of the data is actually changed, where it makes sense to
	// just to dependsOn instead of copy, to use a little extra memory footprint to help cpu?
	state Arena arena;

	try {
		state std::map<KeyRef, ValueRef> dataMap;

		Future<Arena> readSnapshotFuture;
		if (chunk.snapshotFile.present()) {
			readSnapshotFuture = readSnapshotFile(bstore, chunk.snapshotFile.get(), keyRange, &dataMap);
			if (stats.present()) {
				++stats.get()->s3GetReqs;
			}
		} else {
			readSnapshotFuture = Future<Arena>(Arena());
		}

		state std::vector<Future<Standalone<GranuleDeltas>>> readDeltaFutures;
		readDeltaFutures.reserve(chunk.deltaFiles.size());
		for (BlobFilenameRef deltaFile : chunk.deltaFiles) {
			readDeltaFutures.push_back(readDeltaFile(bstore, deltaFile, keyRange, readVersion));
			if (stats.present()) {
				++stats.get()->s3GetReqs;
			}
		}

		Arena snapshotArena = wait(readSnapshotFuture);
		arena.dependsOn(snapshotArena);

		if (BG_READ_DEBUG) {
			printf("Applying %d delta files\n", readDeltaFutures.size());
		}
		for (Future<Standalone<GranuleDeltas>> deltaFuture : readDeltaFutures) {
			Standalone<GranuleDeltas> result = wait(deltaFuture);
			arena.dependsOn(result.arena());
			applyDeltas(&dataMap, arena, result, keyRange, readVersion);
			wait(yield());
		}
		if (BG_READ_DEBUG) {
			printf("Applying %d memory deltas\n", chunk.newDeltas.size());
		}
		applyDeltas(&dataMap, arena, chunk.newDeltas, keyRange, readVersion);
		wait(yield());

		RangeResult ret;
		for (auto& it : dataMap) {
			ret.push_back_deep(ret.arena(), KeyValueRef(it.first, it.second));
			// TODO for large reads, probably wait to yield periodically here for SlowTask
		}

		return ret;
	} catch (Error& e) {
		printf("Reading blob granule got error %s\n", e.name());
		throw e;
	}
}

// TODO probably should add things like limit/bytelimit at some point?
ACTOR Future<Void> readBlobGranules(BlobGranuleFileRequest request,
                                    BlobGranuleFileReply reply,
                                    Reference<BackupContainerFileSystem> bstore,
                                    PromiseStream<RangeResult> results) {
	// TODO for large amount of chunks, this should probably have some sort of buffer limit like ReplyPromiseStream.
	// Maybe just use ReplyPromiseStream instead of PromiseStream?
	try {
		state int i;
		for (i = 0; i < reply.chunks.size(); i++) {
			/*printf("ReadBlobGranules processing chunk %d [%s - %s)\n",
			       i,
			       reply.chunks[i].keyRange.begin.printable().c_str(),
			       reply.chunks[i].keyRange.end.printable().c_str());*/
			RangeResult chunkResult =
			    wait(readBlobGranule(reply.chunks[i], request.keyRange, request.readVersion, bstore));
			results.send(std::move(chunkResult));
		}
		// printf("ReadBlobGranules done, sending EOS\n");
		results.sendError(end_of_stream());
	} catch (Error& e) {
		printf("ReadBlobGranules got error %s\n", e.name());
		results.sendError(e);
	}

	return Void();
}

TEST_CASE("/blobgranule/reader/applyDelta") {
	printf("Testing blob granule deltas\n");
	Arena a;

	// do this 2 phase arena creation of string refs instead of LiteralStringRef because there is no char* StringRef
	// constructor, and valgrind might complain if the stringref data isn't in the arena
	std::string sk_a = "A";
	std::string sk_ab = "AB";
	std::string sk_b = "B";
	std::string sk_c = "C";
	std::string sk_z = "Z";
	std::string sval1 = "1";
	std::string sval2 = "2";

	StringRef k_a = StringRef(a, sk_a);
	StringRef k_ab = StringRef(a, sk_ab);
	StringRef k_b = StringRef(a, sk_b);
	StringRef k_c = StringRef(a, sk_c);
	StringRef k_z = StringRef(a, sk_z);
	StringRef val1 = StringRef(a, sval1);
	StringRef val2 = StringRef(a, sval2);

	std::map<KeyRef, ValueRef> data;
	data.insert({ k_a, val1 });
	data.insert({ k_ab, val1 });
	data.insert({ k_b, val1 });

	std::map<KeyRef, ValueRef> correctData = data;
	std::map<KeyRef, ValueRef> originalData = data;

	ASSERT(data == correctData);

	// test all clear permutations

	MutationRef mClearEverything(MutationRef::ClearRange, allKeys.begin, allKeys.end);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mClearEverything);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearEverything2(MutationRef::ClearRange, allKeys.begin, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mClearEverything2);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearEverything3(MutationRef::ClearRange, k_a, allKeys.end);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mClearEverything3);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearEverything4(MutationRef::ClearRange, k_a, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mClearEverything4);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearFirst(MutationRef::ClearRange, k_a, k_ab);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mClearFirst);
	correctData.erase(k_a);
	ASSERT(data == correctData);

	MutationRef mClearSecond(MutationRef::ClearRange, k_ab, k_b);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mClearSecond);
	correctData.erase(k_ab);
	ASSERT(data == correctData);

	MutationRef mClearThird(MutationRef::ClearRange, k_b, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mClearThird);
	correctData.erase(k_b);
	ASSERT(data == correctData);

	MutationRef mClearFirst2(MutationRef::ClearRange, k_a, k_b);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mClearFirst2);
	correctData.erase(k_a);
	correctData.erase(k_ab);
	ASSERT(data == correctData);

	MutationRef mClearLast2(MutationRef::ClearRange, k_ab, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mClearLast2);
	correctData.erase(k_ab);
	correctData.erase(k_b);
	ASSERT(data == correctData);

	// test set data
	MutationRef mSetA(MutationRef::SetValue, k_a, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mSetA);
	correctData[k_a] = val2;
	ASSERT(data == correctData);

	MutationRef mSetAB(MutationRef::SetValue, k_ab, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mSetAB);
	correctData[k_ab] = val2;
	ASSERT(data == correctData);

	MutationRef mSetB(MutationRef::SetValue, k_b, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mSetB);
	correctData[k_b] = val2;
	ASSERT(data == correctData);

	MutationRef mSetC(MutationRef::SetValue, k_c, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mSetC);
	correctData[k_c] = val2;
	ASSERT(data == correctData);

	// test pruning deltas that are outside of the key range

	MutationRef mSetZ(MutationRef::SetValue, k_z, val2);
	data = originalData;
	applyDelta(&data, a, KeyRangeRef(k_a, k_c), mSetZ);
	ASSERT(data == originalData);

	applyDelta(&data, a, KeyRangeRef(k_ab, k_c), mSetA);
	ASSERT(data == originalData);

	applyDelta(&data, a, KeyRangeRef(k_ab, k_c), mClearFirst);
	ASSERT(data == originalData);

	applyDelta(&data, a, KeyRangeRef(k_a, k_ab), mClearThird);
	ASSERT(data == originalData);

	// Could test all other atomic ops, but if set, max, and compare+clear works, and the others all just directly call
	// the atomics, there is little to test

	MutationRef mCAndC1(MutationRef::CompareAndClear, k_a, val1);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mCAndC1);
	correctData.erase(k_a);
	ASSERT(data == correctData);

	MutationRef mCAndC2(MutationRef::CompareAndClear, k_a, val2);
	data = originalData;
	applyDelta(&data, a, allKeys, mCAndC2);
	ASSERT(data == originalData);

	MutationRef mCAndCZ(MutationRef::CompareAndClear, k_z, val2);
	data = originalData;
	applyDelta(&data, a, allKeys, mCAndCZ);
	ASSERT(data == originalData);

	MutationRef mMaxA(MutationRef::ByteMax, k_a, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mMaxA);
	correctData[k_a] = val2;
	ASSERT(data == correctData);

	MutationRef mMaxC(MutationRef::ByteMax, k_c, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(&data, a, allKeys, mMaxC);
	correctData[k_c] = val2;
	ASSERT(data == correctData);

	return Void();
}
