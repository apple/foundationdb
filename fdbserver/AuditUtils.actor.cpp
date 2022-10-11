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

#include "fdbserver/AuditUtils.actor.h"

#include "fdbclient/Audit.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/Knobs.h"

#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<Void> persistAuditStorage(Database cx, AuditStorageState auditState) {
	state Transaction tr(cx);

	loop {
		try {
			tr.set(auditKey(auditState.id), auditStorageStateValue(auditState));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return Void();
}

ACTOR Future<AuditStorageState> getAuditStorage(Database cx, UID id) {
	state Transaction tr(cx);
	state Optional<Value> res;

	loop {
		try {
			Optional<Value> res_ = wait(tr.get(auditKey(id)));
			res = res_;
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

ACTOR Future<Void> persistAuditStorageMap(Database cx, AuditStorageState auditState) {
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

ACTOR Future<std::vector<AuditStorageState>> getAuditStorageFroRange(Database cx, UID id, KeyRange range) {
	state RangeResult auditStates;
	state Transaction tr(cx);

	loop {
		try {
			RangeResult res_ = wait(krmGetRanges(&tr,
			                                     auditRangePrefixFor(id),
			                                     range,
			                                     SERVER_KNOBS->MOVE_SHARD_KRM_ROW_LIMIT,
			                                     SERVER_KNOBS->MOVE_SHARD_KRM_BYTE_LIMIT));
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