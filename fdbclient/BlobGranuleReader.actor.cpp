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

// TODO could refactor the like 6 lines of file reading code from here and the delta file function into another actor,
// then this part would also be testable? but meh
ACTOR Future<Void> readSnapshotFile(Reference<S3BlobStoreEndpoint> bstore,
                                    std::string bucket,
                                    std::string filename,
                                    Arena arena,
                                    KeyRangeRef keyRange,
                                    std::map<KeyRef, ValueRef>* dataMap) {
	state AsyncFileS3BlobStoreRead reader(bstore, bucket, filename);
	state int64_t size = wait(reader.size());
	state uint8_t* data = new (arena) uint8_t[size];
	printf("Reading %d bytes from snapshot file %s\n", size, filename.c_str());
	int readSize = wait(reader.read(data, size, 0));
	printf("Read %d bytes from snapshot file %s\n", readSize, filename.c_str());
	ASSERT(size == readSize);

	StringRef dataRef(data, size);

	GranuleSnapshot snapshot = ObjectReader::fromStringRef<GranuleSnapshot>(dataRef, Unversioned());
	printf("Parsed %d rows from snapshot file %s\n", snapshot.size(), filename.c_str());
	int i = 0;
	while (i < snapshot.size() && snapshot[i].key < keyRange.begin) {
		i++;
	}
	while (i < snapshot.size() && snapshot[i].key < keyRange.end) {
		dataMap->insert({ snapshot[i].key, snapshot[i].value });
	}
	printf("Started with %d rows from snapshot file %s\n", dataMap->size(), filename.c_str());

	return Void();
}

ACTOR Future<GranuleDeltas> readDeltaFile(Reference<S3BlobStoreEndpoint> bstore,
                                          std::string bucket,
                                          std::string filename,
                                          Arena arena,
                                          KeyRangeRef keyRange,
                                          Version readVersion) {
	state AsyncFileS3BlobStoreRead reader(bstore, bucket, filename);
	state int64_t size = wait(reader.size());
	state uint8_t* data = new (arena) uint8_t[size];
	printf("Reading %d bytes from delta file %s\n", size, filename.c_str());
	int readSize = wait(reader.read(data, size, 0));
	printf("Read %d bytes from delta file %s\n", readSize, filename.c_str());
	ASSERT(size == readSize);

	// Don't do range or version filtering in here since we'd have to copy/rewrite the deltas and it might starve
	// snapshot read task, do it in main thread

	StringRef dataRef(data, size);

	GranuleDeltas deltas = ObjectReader::fromStringRef<GranuleDeltas>(dataRef, Unversioned());
	printf("Parsed %d deltas from delta file %s\n", deltas.size(), filename.c_str());
	return deltas;
}

// TODO unit test this!

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
	for (MutationAndVersion& delta : deltas) {
		if (delta.v > readVersion) {
			break;
		}
		applyDelta(dataMap, arena, keyRange, delta.m);
	}
}

ACTOR Future<RangeResult> readBlobGranule(BlobGranuleChunk chunk,
                                          KeyRangeRef keyRange,
                                          Version readVersion,
                                          Reference<S3BlobStoreEndpoint> bstore,
                                          std::string bucket) {
	// arena to hold all stuff for parsing and updaing mutations. Most of it will likely be tossed, so we copy at the
	// end instead of doing a dependsOn
	state Arena arena;

	state std::map<KeyRef, ValueRef> dataMap;
	Future<Void> readSnapshotFuture =
	    readSnapshotFile(bstore, bucket, chunk.snapshotFileName.toString(), arena, keyRange, &dataMap);
	state std::vector<Future<GranuleDeltas>> readDeltaFutures;
	readDeltaFutures.resize(chunk.deltaFileNames.size());
	for (StringRef deltaFileName : chunk.deltaFileNames) {
		readDeltaFutures.push_back(
		    readDeltaFile(bstore, bucket, deltaFileName.toString(), arena, keyRange, readVersion));
	}

	wait(readSnapshotFuture);
	for (Future<GranuleDeltas> deltaFuture : readDeltaFutures) {
		GranuleDeltas result = wait(deltaFuture);
		applyDeltas(&dataMap, arena, result, keyRange, readVersion);
		wait(yield());
	}
	printf("Applying %d memory deltas\n", chunk.newDeltas.size());
	applyDeltas(&dataMap, arena, chunk.newDeltas, keyRange, readVersion);
	wait(yield());

	RangeResult ret;
	for (auto& it : dataMap) {
		ret.push_back_deep(ret.arena(), KeyValueRef(it.first, it.second));
		// TODO for large reads, probably wait to yield periodically here for slowTask
	}
	printf("Final processing ended up with %d rows\n", ret.size());

	return ret;
}

// TODO probably should add things like limit/bytelimit at some point?
ACTOR Future<Void> readBlobGranules(BlobGranuleFileRequest request,
                                    BlobGranuleFileReply reply,
                                    Reference<S3BlobStoreEndpoint> bstore,
                                    std::string bucket,
                                    PromiseStream<RangeResult> results) {
	// TODO for large amount of chunks, this should probably have some sort of buffer limit like ReplyPromiseStream.
	// Maybe just use ReplyPromiseStream instead of PromiseStream?
	try {
		state int i;
		for (i = 0; i < reply.chunks.size(); i++) {
			printf("ReadBlobGranules processing chunk %d [%s - %s)\n",
			       i,
			       reply.chunks[i].keyRange.begin.printable().c_str(),
			       reply.chunks[i].keyRange.end.printable().c_str());
			RangeResult chunkResult =
			    wait(readBlobGranule(reply.chunks[i], request.keyRange, request.readVersion, bstore, bucket));
			results.send(std::move(chunkResult));
		}
		printf("ReadBlobGranules done, sending EOS\n");
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