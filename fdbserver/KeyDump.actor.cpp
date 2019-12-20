/*
 * KeyDump.actor.cpp
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

#include "fdbserver/IKeyValueStore.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "flow/ActorCollection.h"
#include "fdbserver/KeyDump.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Void> traceDumpKeysProgress(int* keysCopied, int* bytesCopied, UID debugID) {
	loop {
		wait(delay(5.0));
		TraceEvent("DumpKeysProgress", debugID).detail("KeysCopied", *keysCopied).detail("BytesCopied", *bytesCopied);
	}
}

ACTOR Future<Void> dumpKeysToRemoteCluster(IKeyValueStore* kvStore, std::string destCluster, int* keysCopied,
                                           int* bytesCopied, UID debugID) {
	TraceEvent("DumpKeysBegin", debugID);
	try {
		state Database cx = Database::createDatabase(destCluster, Database::API_VERSION_LATEST);
		TraceEvent("DumpKeysGotCluster").detail("Cluster", destCluster);
		state Key begin = normalKeys.begin;
		loop {
			state Standalone<VectorRef<KeyValueRef>> kvs = wait(kvStore->readRange(
			    KeyRangeRef(begin, normalKeys.end), CLIENT_KNOBS->TOO_MANY, CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT / 2));
			TraceEvent(SevDebug, "DumpKeysGotKeys", debugID).detail("Size", kvs.size());
			if (kvs.size() == 0) {
				break;
			}
			begin = kvs.back().key;
			state Transaction tr(cx);
			state int numBytes;
			loop {
				numBytes = 0;
				try {
					for (const auto& kv : kvs) {
						tr.set(kv.key, kv.value);
						numBytes += kv.key.size() + kv.value.size();
					}
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			*keysCopied += kvs.size();
			*bytesCopied += numBytes;
			if (kvs.size() <= 1) {
				break;
			}
		}
		TraceEvent("DumpKeysComplete", debugID);
		kvStore->close();
	} catch (Error& e) {
		TraceEvent(SevError, "DumpKeysFailed", debugID).error(e, /*includeCancelled*/ true);
		kvStore->close();
		throw;
	}
	return Void();
}

ACTOR Future<Void> keyDump(std::string destCluster, std::string dataDir) {
	state UID debugID = deterministicRandom()->randomUniqueID();
	state std::vector<IKeyValueStore*> kvStores = getStorageKeyValueStores(dataDir);
	state ActorCollection errors(true);
	state std::vector<Future<Void>> futures;
	state int keysCopied = 0;
	state int bytesCopied = 0;
	state Future<Void> tracer = traceDumpKeysProgress(&keysCopied, &bytesCopied, debugID);
	for (const auto kv : kvStores) {
		errors.add(kv->getError());
		futures.push_back(kv->onClosed());
		futures.push_back(dumpKeysToRemoteCluster(kv, destCluster, &keysCopied, &bytesCopied, debugID));
	}
	choose {
		when(wait(waitForAll(futures))) { return Void(); }
		when(wait(errors.getResult())) { throw internal_error(); }
	}
}
