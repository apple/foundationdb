/*
 * BlobGranuleValidation.actor.cpp
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

#include "fdbserver/BlobGranuleValidation.actor.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<std::pair<RangeResult, Version>> readFromFDB(Database cx, KeyRange range) {
	state bool first = true;
	state Version v;
	state RangeResult out;
	state Transaction tr(cx);
	state KeyRange currentRange = range;
	loop {
		try {
			state RangeResult r = wait(tr.getRange(currentRange, CLIENT_KNOBS->TOO_MANY));
			Version grv = wait(tr.getReadVersion());
			// need consistent version snapshot of range
			if (first) {
				v = grv;
				first = false;
			} else if (v != grv) {
				// reset the range and restart the read at a higher version
				first = true;
				out = RangeResult();
				currentRange = range;
				tr.reset();
				continue;
			}
			out.arena().dependsOn(r.arena());
			out.append(out.arena(), r.begin(), r.size());
			if (r.more) {
				currentRange = KeyRangeRef(keyAfter(r.back().key), currentRange.end);
			} else {
				break;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return std::pair(out, v);
}

// FIXME: typedef this pair type and/or chunk list
ACTOR Future<std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>>> readFromBlob(
    Database cx,
    Reference<BackupContainerFileSystem> bstore,
    KeyRange range,
    Version beginVersion,
    Version readVersion) {
	state RangeResult out;
	state Standalone<VectorRef<BlobGranuleChunkRef>> chunks;
	state Transaction tr(cx);

	loop {
		try {
			Standalone<VectorRef<BlobGranuleChunkRef>> chunks_ =
			    wait(tr.readBlobGranules(range, beginVersion, readVersion));
			chunks = chunks_;
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	for (const BlobGranuleChunkRef& chunk : chunks) {
		RangeResult chunkRows = wait(readBlobGranule(chunk, range, beginVersion, readVersion, bstore));
		out.arena().dependsOn(chunkRows.arena());
		out.append(out.arena(), chunkRows.begin(), chunkRows.size());
	}
	return std::pair(out, chunks);
}

bool compareFDBAndBlob(RangeResult fdb,
                       std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob,
                       KeyRange range,
                       Version v,
                       bool debug) {
	bool correct = fdb == blob.first;
	if (!correct) {
		TraceEvent ev(SevError, "GranuleMismatch");
		ev.detail("RangeStart", range.begin)
		    .detail("RangeEnd", range.end)
		    .detail("Version", v)
		    .detail("FDBSize", fdb.size())
		    .detail("BlobSize", blob.first.size());

		if (debug) {
			fmt::print("\nMismatch for [{0} - {1}) @ {2} ({3}). F({4}) B({5}):\n",
			           range.begin.printable(),
			           range.end.printable(),
			           v,
			           fdb.size(),
			           blob.first.size());

			Optional<KeyValueRef> lastCorrect;
			for (int i = 0; i < std::max(fdb.size(), blob.first.size()); i++) {
				if (i >= fdb.size() || i >= blob.first.size() || fdb[i] != blob.first[i]) {
					printf("  Found mismatch at %d.\n", i);
					if (lastCorrect.present()) {
						printf("    last correct: %s=%s\n",
						       lastCorrect.get().key.printable().c_str(),
						       lastCorrect.get().value.printable().c_str());
					}
					if (i < fdb.size()) {
						printf("    FDB: %s=%s\n", fdb[i].key.printable().c_str(), fdb[i].value.printable().c_str());
					} else {
						printf("    FDB: <missing>\n");
					}
					if (i < blob.first.size()) {
						printf("    BLB: %s=%s\n",
						       blob.first[i].key.printable().c_str(),
						       blob.first[i].value.printable().c_str());
					} else {
						printf("    BLB: <missing>\n");
					}
					printf("\n");
					break;
				}
				if (i < fdb.size()) {
					lastCorrect = fdb[i];
				} else {
					lastCorrect = blob.first[i];
				}
			}

			printf("Chunks:\n");
			for (auto& chunk : blob.second) {
				printf("[%s - %s)\n", chunk.keyRange.begin.printable().c_str(), chunk.keyRange.end.printable().c_str());

				printf("  SnapshotFile:\n    %s\n",
				       chunk.snapshotFile.present() ? chunk.snapshotFile.get().toString().c_str() : "<none>");
				printf("  DeltaFiles:\n");
				for (auto& df : chunk.deltaFiles) {
					printf("    %s\n", df.toString().c_str());
				}
				printf("  Deltas: (%d)", chunk.newDeltas.size());
				if (chunk.newDeltas.size() > 0) {
					fmt::print(" with version [{0} - {1}]",
					           chunk.newDeltas[0].version,
					           chunk.newDeltas[chunk.newDeltas.size() - 1].version);
				}
				fmt::print("  IncludedVersion: {}\n", chunk.includedVersion);
			}
			printf("\n");
		}
	}
	return correct;
}