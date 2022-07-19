/*
 * StorageQuotaTracker.actor.cpp
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

#include "fdbclient/SystemData.h"
#include "fdbserver/StorageQuota.actor.h"
#include "flow/Arena.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct QuotaMap {
	std::map<StringRef, uint64_t> map;
};

ACTOR Future<Void> storageQuotaTracker(Database cx) {
	state QuotaMap quotaMap;
	loop {
		state Transaction tr(cx);
		loop {
			try {
				state RangeResult currentQuotas = wait(tr.getRange(storageQuotaKeys, CLIENT_KNOBS->TOO_MANY));
				TraceEvent("StorageQuota_ReadCurrentQuotas").detail("Size", currentQuotas.size());
				for (auto const kv : currentQuotas) {
					StringRef const key = kv.key.removePrefix(storageQuotaPrefix);
					uint64_t const quota = BinaryReader::fromStringRef<uint64_t>(kv.value, Unversioned());
					quotaMap.map[key] = quota;
				}
				wait(delay(5.0));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}
