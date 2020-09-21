/*
 * LockRange.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include <cinttypes>
#include <vector>

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/LockRange.actor.h"
#include "fdbclient/ReadYourWrites.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

ACTOR Future<Void> lockRange(Transaction* tr, KeyRangeRef range, bool checkDBLock, LockMode mode) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	if (checkDBLock) {
		Optional<Value> val = wait(tr->get(databaseLockedKey));

		if (val.present()) {
			throw database_locked();
		}
	}

	tr->atomicOp(rangeLockKey, encodeRangeLock(LockRequest(range, mode)), MutationRef::LockRange);
	if (checkDBLock) {
		tr->atomicOp(rangeLockVersionKey, rangeLockVersionRequiredValue, MutationRef::SetVersionstampedValue);
	}
	tr->addWriteConflictRange(range);
	return Void();
}

ACTOR Future<Void> lockRange(Database cx, KeyRangeRef range, LockMode mode) {
	state Transaction tr(cx);
	loop {
		try {
			wait(lockRange(&tr, range, true, mode));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked) throw e;
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> lockRanges(Database cx, std::vector<KeyRangeRef> ranges, LockMode mode) {
	if (ranges.size() > 1) {
		// Check ranges are disjoint
		std::sort(ranges.begin(), ranges.end(), [](const KeyRangeRef& a, const KeyRangeRef& b) {
			return a.begin == b.begin ? a.end < b.end : a.begin < b.begin;
		});
		for (int i = 1; i < ranges.size(); i++) {
			if (ranges[i - 1].intersects(ranges[i])) {
				throw client_invalid_operation();
			}
		}
	}

	state Transaction tr(cx);
	loop {
		try {
			state bool checkDBLock = true;
			for (const auto& range : ranges) {
				wait(lockRange(&tr, range, checkDBLock, mode));
				checkDBLock = false;
			}
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked) throw e;
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> unlockRange(Transaction* tr, KeyRangeRef range, bool checkDBLock, LockMode mode) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	if (checkDBLock) {
		Optional<Value> val = wait(tr->get(databaseLockedKey));

		if (val.present()) {
			throw database_locked();
		}
	}

	tr->atomicOp(rangeLockKey, encodeRangeLock(LockRequest(range, mode)), MutationRef::UnlockRange);
	if (checkDBLock) {
		tr->atomicOp(rangeLockVersionKey, rangeLockVersionRequiredValue, MutationRef::SetVersionstampedValue);
	}
	tr->addWriteConflictRange(range);
	return Void();
}

ACTOR Future<Void> unlockRange(Database cx, KeyRangeRef range, LockMode mode) {
	state Transaction tr(cx);
	loop {
		try {
			wait(unlockRange(&tr, range, true, mode));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked) throw e;
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> unlockRange(Database cx, std::vector<KeyRangeRef> ranges, LockMode mode) {
	if (ranges.size() > 1) {
		// Check ranges are disjoint
		std::sort(ranges.begin(), ranges.end(), [](const KeyRangeRef& a, const KeyRangeRef& b) {
			return a.begin == b.begin ? a.end < b.end : a.begin < b.begin;
		});
		for (int i = 1; i < ranges.size(); i++) {
			if (ranges[i - 1].intersects(ranges[i])) {
				throw client_invalid_operation();
			}
		}
	}

	state Transaction tr(cx);
	loop {
		try {
			state bool checkDBLock = true;
			for (const auto& range : ranges) {
				wait(unlockRange(&tr, range, checkDBLock, mode));
				checkDBLock = false;
			}
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_database_locked) throw e;
			wait(tr.onError(e));
		}
	}
}
