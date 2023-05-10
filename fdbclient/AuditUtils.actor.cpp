/*
 * AuditUtils.actor.cpp
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

#include "fdbclient/AuditUtils.actor.h"

#include "fdbclient/Audit.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/ClientKnobs.h"
#include <fmt/format.h>

#include "flow/actorcompiler.h" // has to be last include

ACTOR static Future<std::vector<AuditStorageState>> getLatestAuditStatesImpl(Transaction* tr, AuditType type, int num) {
	state std::vector<AuditStorageState> auditStates;

	loop {
		auditStates.clear();
		try {
			RangeResult res = wait(tr->getRange(auditKeyRange(type), num, Snapshot::False, Reverse::True));
			for (int i = 0; i < res.size(); ++i) {
				auditStates.push_back(decodeAuditStorageState(res[i].value));
			}
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	return auditStates;
}

ACTOR Future<UID> persistNewAuditState(Database cx, AuditStorageState auditState) {
	ASSERT(!auditState.id.isValid());
	state Transaction tr(cx);
	state UID auditId;

	loop {
		try {
			std::vector<AuditStorageState> auditStates = wait(getLatestAuditStatesImpl(&tr, auditState.getType(), 1));
			uint64_t nextId = 1;
			if (!auditStates.empty()) {
				nextId = auditStates.front().id.first() + 1;
			}
			auditId = UID(nextId, 0LL);
			auditState.id = auditId;
			tr.set(auditKey(auditState.getType(), auditId), auditStorageStateValue(auditState));
			wait(tr.commit());
			TraceEvent("PersistedNewAuditState", auditId).detail("AuditKey", auditKey(auditState.getType(), auditId));
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return auditId;
}

ACTOR Future<Void> persistAuditState(Database cx, AuditStorageState auditState) {
	state Transaction tr(cx);

	loop {
		try {
			tr.set(auditKey(auditState.getType(), auditState.id), auditStorageStateValue(auditState));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return Void();
}

ACTOR Future<AuditStorageState> getAuditState(Database cx, AuditType type, UID id) {
	state Transaction tr(cx);
	state Optional<Value> res;

	loop {
		try {
			Optional<Value> res_ = wait(tr.get(auditKey(type, id)));
			res = res_;
			TraceEvent("ReadAuditState", id).detail("AuditKey", auditKey(type, id));
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	if (!res.present()) {
		throw key_not_found();
	}

	return decodeAuditStorageState(res.get());
}

ACTOR Future<std::vector<AuditStorageState>> getLatestAuditStates(Database cx, AuditType type, int num) {
	Transaction tr(cx);
	std::vector<AuditStorageState> auditStates = wait(getLatestAuditStatesImpl(&tr, type, num));
	return auditStates;
}

ACTOR Future<std::string> checkMigrationProgress(Database cx) {
	state Key begin = allKeys.begin;
	state int numShards = 0;
	state int numPhysicalShards = 0;

	while (begin < allKeys.end) {
		// RYW to optimize re-reading the same key ranges
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

				state RangeResult UIDtoTagMap = wait(tr->getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				KeyRangeRef currentKeys(begin, allKeys.end);
				RangeResult shards = wait(
				    krmGetRanges(tr, keyServersPrefix, currentKeys, CLIENT_KNOBS->TOO_MANY, CLIENT_KNOBS->TOO_MANY));

				for (int i = 0; i < shards.size() - 1; ++i) {
					std::vector<UID> src;
					std::vector<UID> dest;
					UID srcId;
					UID destId;
					decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest, srcId, destId);
					if (srcId != anonymousShardId) {
						++numPhysicalShards;
					}
				}

				begin = shards.back().key;
				numShards += shards.size() - 1;
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	return fmt::format("Total number of shards: {}, number of physical shards: {}", numShards, numPhysicalShards);
}

ACTOR Future<Void> persistAuditStateMap(Database cx, AuditStorageState auditState) {
	state Transaction tr(cx);

	loop {
		try {
			wait(krmSetRange(
			    &tr, auditRangePrefixFor(auditState.id), auditState.range, auditStorageStateValue(auditState)));
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return Void();
}

ACTOR Future<std::vector<AuditStorageState>> getAuditStateForRange(Database cx, UID id, KeyRange range) {
	state RangeResult auditStates;
	state Transaction tr(cx);

	loop {
		try {
			RangeResult res_ = wait(krmGetRanges(&tr,
			                                     auditRangePrefixFor(id),
			                                     range,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
			auditStates = res_;
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	std::vector<AuditStorageState> res;
	for (int i = 0; i < auditStates.size() - 1; ++i) {
		KeyRange currentRange = KeyRangeRef(auditStates[i].key, auditStates[i + 1].key);
		AuditStorageState auditState;
		if (!auditStates[i].value.empty()) {
			AuditStorageState auditState = decodeAuditStorageState(auditStates[i].value);
		}
		auditState.range = currentRange;
		res.push_back(auditState);
	}

	return res;
}