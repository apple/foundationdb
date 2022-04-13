/*
 * BlobGranuleFiles.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include <vector>

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "flow/serialize.h"
#include "fdbclient/BlobGranuleFiles.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/SystemData.h" // for allKeys unit test - could remove
#include "flow/UnitTest.h"

#define BG_READ_DEBUG false

// FIXME: implement actual proper file format for this

// Implements granule file parsing and materialization with normal c++ functions (non-actors) so that this can be used
// outside the FDB network thread.

static Arena loadSnapshotFile(const StringRef& snapshotData,
                              KeyRangeRef keyRange,
                              std::map<KeyRef, ValueRef>& dataMap) {

	Arena parseArena;
	GranuleSnapshot snapshot;
	ObjectReader reader(snapshotData.begin(), Unversioned());
	reader.deserialize(FileIdentifierFor<GranuleSnapshot>::value, snapshot, parseArena);

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
		dataMap.insert({ snapshot[i].key, snapshot[i].value });
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
		fmt::print("Started with {0} rows from snapshot after pruning to [{1} - {2})\n",
		           dataMap.size(),
		           keyRange.begin.printable(),
		           keyRange.end.printable());
	}

	return parseArena;
}

static void applyDelta(KeyRangeRef keyRange, MutationRef m, std::map<KeyRef, ValueRef>& dataMap) {
	if (m.type == MutationRef::ClearRange) {
		if (m.param2 <= keyRange.begin || m.param1 >= keyRange.end) {
			return;
		}
		// keyRange is inclusive on start, lower_bound is inclusive with the argument, and erase is inclusive for the
		// begin. So if lower bound didn't find the exact key, we need to go up one so it doesn't erase an extra key
		// outside the range.
		std::map<KeyRef, ValueRef>::iterator itStart = dataMap.lower_bound(m.param1);
		if (itStart != dataMap.end() && itStart->first < m.param1) {
			itStart++;
		}

		// keyRange is exclusive on end, lower bound is inclusive with the argument, and erase is exclusive for the end
		// key. So if lower bound didn't find the exact key, we need to go up one so it doesn't skip the last key it
		// should erase
		std::map<KeyRef, ValueRef>::iterator itEnd = dataMap.lower_bound(m.param2);
		if (itEnd != dataMap.end() && itEnd->first < m.param2) {
			itEnd++;
		}
		dataMap.erase(itStart, itEnd);
	} else {
		// We don't need atomics here since eager reads handles it
		ASSERT(m.type == MutationRef::SetValue);
		if (m.param1 < keyRange.begin || m.param1 >= keyRange.end) {
			return;
		}

		std::map<KeyRef, ValueRef>::iterator it = dataMap.find(m.param1);
		if (it == dataMap.end()) {
			dataMap.insert({ m.param1, m.param2 });
		} else {
			it->second = m.param2;
		}
	}
}

static void applyDeltas(const GranuleDeltas& deltas,
                        KeyRangeRef keyRange,
                        Version beginVersion,
                        Version readVersion,
                        Version& lastFileEndVersion,
                        std::map<KeyRef, ValueRef>& dataMap) {
	if (deltas.empty()) {
		return;
	}
	// check that consecutive delta file versions are disjoint
	ASSERT(lastFileEndVersion < deltas.front().version);

	const MutationsAndVersionRef* mutationIt = deltas.begin();
	// prune beginVersion if necessary
	if (beginVersion > deltas.front().version) {
		ASSERT(beginVersion <= deltas.back().version);
		// binary search for beginVersion
		mutationIt = std::lower_bound(deltas.begin(),
		                              deltas.end(),
		                              MutationsAndVersionRef(beginVersion, 0),
		                              MutationsAndVersionRef::OrderByVersion());
	}

	while (mutationIt != deltas.end()) {
		if (mutationIt->version > readVersion) {
			lastFileEndVersion = readVersion;
			return;
		}
		for (auto& m : mutationIt->mutations) {
			applyDelta(keyRange, m, dataMap);
		}
		mutationIt++;
	}
	lastFileEndVersion = deltas.back().version;
}

static Arena loadDeltaFile(StringRef deltaData,
                           KeyRangeRef keyRange,
                           Version beginVersion,
                           Version readVersion,
                           Version& lastFileEndVersion,
                           std::map<KeyRef, ValueRef>& dataMap) {
	Arena parseArena;
	GranuleDeltas deltas;
	ObjectReader reader(deltaData.begin(), Unversioned());
	reader.deserialize(FileIdentifierFor<GranuleDeltas>::value, deltas, parseArena);

	if (BG_READ_DEBUG) {
		fmt::print("Parsed {} deltas from file\n", deltas.size());
	}

	// TODO REMOVE sanity check
	for (int i = 0; i < deltas.size() - 1; i++) {
		if (deltas[i].version > deltas[i + 1].version) {
			fmt::print(
			    "BG VERSION ORDER VIOLATION IN DELTA FILE: '{0}', '{1}'\n", deltas[i].version, deltas[i + 1].version);
		}
		ASSERT(deltas[i].version <= deltas[i + 1].version);
	}

	applyDeltas(deltas, keyRange, beginVersion, readVersion, lastFileEndVersion, dataMap);
	return parseArena;
}

RangeResult materializeBlobGranule(const BlobGranuleChunkRef& chunk,
                                   KeyRangeRef keyRange,
                                   Version beginVersion,
                                   Version readVersion,
                                   Optional<StringRef> snapshotData,
                                   StringRef deltaFileData[]) {
	// TODO REMOVE with early replying
	ASSERT(readVersion == chunk.includedVersion);

	// Arena to hold all allocations for applying deltas. Most of it, and the arenas produced by reading the files,
	// will likely be tossed if there are a significant number of mutations, so we copy at the end instead of doing a
	// dependsOn.
	// FIXME: probably some threshold of a small percentage of the data is actually changed, where it makes sense to
	// just to dependsOn instead of copy, to use a little extra memory footprint to help cpu?
	Arena arena;
	std::map<KeyRef, ValueRef> dataMap;
	Version lastFileEndVersion = invalidVersion;

	if (snapshotData.present()) {
		Arena snapshotArena = loadSnapshotFile(snapshotData.get(), keyRange, dataMap);
		arena.dependsOn(snapshotArena);
	}

	if (BG_READ_DEBUG) {
		fmt::print("Applying {} delta files\n", chunk.deltaFiles.size());
	}
	for (int deltaIdx = 0; deltaIdx < chunk.deltaFiles.size(); deltaIdx++) {
		Arena deltaArena =
		    loadDeltaFile(deltaFileData[deltaIdx], keyRange, beginVersion, readVersion, lastFileEndVersion, dataMap);
		arena.dependsOn(deltaArena);
	}
	if (BG_READ_DEBUG) {
		fmt::print("Applying {} memory deltas\n", chunk.newDeltas.size());
	}
	applyDeltas(chunk.newDeltas, keyRange, beginVersion, readVersion, lastFileEndVersion, dataMap);

	RangeResult ret;
	for (auto& it : dataMap) {
		ret.push_back_deep(ret.arena(), KeyValueRef(it.first, it.second));
	}

	return ret;
}

struct GranuleLoadIds {
	Optional<int64_t> snapshotId;
	std::vector<int64_t> deltaIds;
};

static void startLoad(const ReadBlobGranuleContext granuleContext,
                      const BlobGranuleChunkRef& chunk,
                      GranuleLoadIds& loadIds) {

	// Start load process for all files in chunk
	if (chunk.snapshotFile.present()) {
		std::string snapshotFname = chunk.snapshotFile.get().filename.toString();
		// FIXME: remove when we implement file multiplexing
		ASSERT(chunk.snapshotFile.get().offset == 0);
		ASSERT(chunk.snapshotFile.get().length == chunk.snapshotFile.get().fullFileLength);
		loadIds.snapshotId = granuleContext.start_load_f(snapshotFname.c_str(),
		                                                 snapshotFname.size(),
		                                                 chunk.snapshotFile.get().offset,
		                                                 chunk.snapshotFile.get().length,
		                                                 chunk.snapshotFile.get().fullFileLength,
		                                                 granuleContext.userContext);
	}
	loadIds.deltaIds.reserve(chunk.deltaFiles.size());
	for (int deltaFileIdx = 0; deltaFileIdx < chunk.deltaFiles.size(); deltaFileIdx++) {
		std::string deltaFName = chunk.deltaFiles[deltaFileIdx].filename.toString();
		// FIXME: remove when we implement file multiplexing
		ASSERT(chunk.deltaFiles[deltaFileIdx].offset == 0);
		ASSERT(chunk.deltaFiles[deltaFileIdx].length == chunk.deltaFiles[deltaFileIdx].fullFileLength);
		int64_t deltaLoadId = granuleContext.start_load_f(deltaFName.c_str(),
		                                                  deltaFName.size(),
		                                                  chunk.deltaFiles[deltaFileIdx].offset,
		                                                  chunk.deltaFiles[deltaFileIdx].length,
		                                                  chunk.deltaFiles[deltaFileIdx].fullFileLength,
		                                                  granuleContext.userContext);
		loadIds.deltaIds.push_back(deltaLoadId);
	}
}

ErrorOr<RangeResult> loadAndMaterializeBlobGranules(const Standalone<VectorRef<BlobGranuleChunkRef>>& files,
                                                    const KeyRangeRef& keyRange,
                                                    Version beginVersion,
                                                    Version readVersion,
                                                    ReadBlobGranuleContext granuleContext) {
	int64_t parallelism = granuleContext.granuleParallelism;
	if (parallelism < 1) {
		parallelism = 1;
	}
	if (parallelism >= CLIENT_KNOBS->BG_MAX_GRANULE_PARALLELISM) {
		parallelism = CLIENT_KNOBS->BG_MAX_GRANULE_PARALLELISM;
	}

	GranuleLoadIds loadIds[files.size()];

	// Kick off first file reads if parallelism > 1
	for (int i = 0; i < parallelism - 1 && i < files.size(); i++) {
		startLoad(granuleContext, files[i], loadIds[i]);
	}

	try {
		RangeResult results;
		for (int chunkIdx = 0; chunkIdx < files.size(); chunkIdx++) {
			// Kick off files for this granule if parallelism == 1, or future granule if parallelism > 1
			if (chunkIdx + parallelism - 1 < files.size()) {
				startLoad(granuleContext, files[chunkIdx + parallelism - 1], loadIds[chunkIdx + parallelism - 1]);
			}

			RangeResult chunkRows;

			// once all loads kicked off, load data for chunk
			Optional<StringRef> snapshotData;
			if (files[chunkIdx].snapshotFile.present()) {
				snapshotData =
				    StringRef(granuleContext.get_load_f(loadIds[chunkIdx].snapshotId.get(), granuleContext.userContext),
				              files[chunkIdx].snapshotFile.get().length);
				if (!snapshotData.get().begin()) {
					return ErrorOr<RangeResult>(blob_granule_file_load_error());
				}
			}

			StringRef deltaData[files[chunkIdx].deltaFiles.size()];
			for (int i = 0; i < files[chunkIdx].deltaFiles.size(); i++) {
				deltaData[i] =
				    StringRef(granuleContext.get_load_f(loadIds[chunkIdx].deltaIds[i], granuleContext.userContext),
				              files[chunkIdx].deltaFiles[i].length);
				// null data is error
				if (!deltaData[i].begin()) {
					return ErrorOr<RangeResult>(blob_granule_file_load_error());
				}
			}

			// materialize rows from chunk
			chunkRows =
			    materializeBlobGranule(files[chunkIdx], keyRange, beginVersion, readVersion, snapshotData, deltaData);

			results.arena().dependsOn(chunkRows.arena());
			results.append(results.arena(), chunkRows.begin(), chunkRows.size());

			if (loadIds[chunkIdx].snapshotId.present()) {
				granuleContext.free_load_f(loadIds[chunkIdx].snapshotId.get(), granuleContext.userContext);
			}
			for (int i = 0; i < loadIds[chunkIdx].deltaIds.size(); i++) {
				granuleContext.free_load_f(loadIds[chunkIdx].deltaIds[i], granuleContext.userContext);
			}
		}
		return ErrorOr<RangeResult>(results);
	} catch (Error& e) {
		return ErrorOr<RangeResult>(e);
	}
}

TEST_CASE("/blobgranule/files/applyDelta") {
	printf("Testing blob granule delta applying\n");
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
	applyDelta(allKeys, mClearEverything, data);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearEverything2(MutationRef::ClearRange, allKeys.begin, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearEverything2, data);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearEverything3(MutationRef::ClearRange, k_a, allKeys.end);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearEverything3, data);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearEverything4(MutationRef::ClearRange, k_a, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearEverything, data);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearFirst(MutationRef::ClearRange, k_a, k_ab);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearFirst, data);
	correctData.erase(k_a);
	ASSERT(data == correctData);

	MutationRef mClearSecond(MutationRef::ClearRange, k_ab, k_b);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearSecond, data);
	correctData.erase(k_ab);
	ASSERT(data == correctData);

	MutationRef mClearThird(MutationRef::ClearRange, k_b, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearThird, data);
	correctData.erase(k_b);
	ASSERT(data == correctData);

	MutationRef mClearFirst2(MutationRef::ClearRange, k_a, k_b);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearFirst2, data);
	correctData.erase(k_a);
	correctData.erase(k_ab);
	ASSERT(data == correctData);

	MutationRef mClearLast2(MutationRef::ClearRange, k_ab, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearLast2, data);
	correctData.erase(k_ab);
	correctData.erase(k_b);
	ASSERT(data == correctData);

	// test set data
	MutationRef mSetA(MutationRef::SetValue, k_a, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mSetA, data);
	correctData[k_a] = val2;
	ASSERT(data == correctData);

	MutationRef mSetAB(MutationRef::SetValue, k_ab, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mSetAB, data);
	correctData[k_ab] = val2;
	ASSERT(data == correctData);

	MutationRef mSetB(MutationRef::SetValue, k_b, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mSetB, data);
	correctData[k_b] = val2;
	ASSERT(data == correctData);

	MutationRef mSetC(MutationRef::SetValue, k_c, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mSetC, data);
	correctData[k_c] = val2;
	ASSERT(data == correctData);

	// test pruning deltas that are outside of the key range

	MutationRef mSetZ(MutationRef::SetValue, k_z, val2);
	data = originalData;
	applyDelta(KeyRangeRef(k_a, k_c), mSetZ, data);
	ASSERT(data == originalData);

	applyDelta(KeyRangeRef(k_ab, k_c), mSetA, data);
	ASSERT(data == originalData);

	applyDelta(KeyRangeRef(k_ab, k_c), mClearFirst, data);
	ASSERT(data == originalData);

	applyDelta(KeyRangeRef(k_a, k_ab), mClearThird, data);
	ASSERT(data == originalData);

	return Void();
}