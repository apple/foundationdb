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
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

const char* getLockModeText(LockMode mode) {
	switch (mode) {
	case LockMode::LOCK_EXCLUSIVE:
		return "LOCK_EXCLUSIVE";

	case LockMode::LOCK_READ_SHARED:
		return "LOCK_READ_SHARED";

	case LockMode::UNLOCK_EXCLUSIVE:
		return "UNLOCK_EXCLUSIVE";

	case LockMode::UNLOCK_READ_SHARED:
		return "UNLOCK_READ_SHARED";

	default:
		return "<undefined>";
	}
}

ACTOR Future<Void> lockRange(Transaction* tr, KeyRangeRef range, bool checkDBLock, LockMode mode) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	if (checkDBLock) {
		Optional<Value> val = wait(tr->get(databaseLockedKey));

		if (val.present()) {
			throw database_locked();
		}
	}

	BinaryWriter wr(IncludeVersion(ProtocolVersion::withLockRangeValue()));
	wr << LockRequest(range, mode);
	tr->atomicOp(rangeLockKey, wr.toValue(), MutationRef::LockRange);
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
	ASSERT(mode == LockMode::UNLOCK_EXCLUSIVE || mode == LockMode::UNLOCK_READ_SHARED);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	if (checkDBLock) {
		Optional<Value> val = wait(tr->get(databaseLockedKey));

		if (val.present()) {
			throw database_locked();
		}
	}

	BinaryWriter wr(IncludeVersion(ProtocolVersion::withLockRangeValue()));
	wr << LockRequest(range, mode);
	tr->atomicOp(rangeLockKey, wr.toValue(), MutationRef::LockRange);
	if (checkDBLock) {
		tr->atomicOp(rangeLockVersionKey, rangeLockVersionRequiredValue, MutationRef::SetVersionstampedValue);
	}
	tr->addWriteConflictRange(range);
	return Void();
}

ACTOR Future<Void> unlockRange(Database cx, KeyRangeRef range, LockMode mode) {
	ASSERT(mode == LockMode::UNLOCK_EXCLUSIVE || mode == LockMode::UNLOCK_READ_SHARED);
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

RangeLockCache::RangeLockCache(Snapshot snapshot, Version version) {
	snapshots[version] = snapshot;
}

void RangeLockCache::add(Version version, const Requests& more) {
	auto& current = requests[version];
	current.append(current.arena(), more.begin(), more.size());
	current.arena().dependsOn(more.arena());
}

void RangeLockCache::add(Version version, const LockRequest& request) {
	auto& current = requests[version];
	current.push_back_deep(current.arena(), request);
}

// PRE-CONDITION: there are lock versions larger than "upTo".
void RangeLockCache::expire(Version upTo) {
	// Make sure we have a snapshot right after "upTo" version
	auto snapIter = snapshots.upper_bound(upTo);
	auto reqIter = requests.upper_bound(upTo);
	if (snapIter != snapshots.end()) {
		if (reqIter != requests.end() && reqIter->first < snapIter->first) {
			ensureSnapshot(reqIter->first);
		}
	} else if (reqIter != requests.end()) {
		ensureSnapshot(reqIter->first);
	} else {
		TraceEvent(SevError, "RLCExpireNone");
		return;
	}

	TraceEvent("RLCExpire").detail("Version", upTo);
	for (auto it = snapshots.begin(); it != snapshots.end() && it->first <= upTo;) {
		it = snapshots.erase(it);
	}

	for (auto it = requests.begin(); it != requests.end() && it->first <= upTo;) {
		it = requests.erase(it);
	}
}

bool RangeLockCache::hasVersion(Version version) {
	return snapshots.find(version) != snapshots.end() || requests.find(version) != requests.end();
}

RangeLockCache::Reason RangeLockCache::check(KeyRef key, Version version, bool write) {
	Key end = keyAfter(key);
	return check(KeyRangeRef(key, end), version, write);
}

RangeLockCache::Reason RangeLockCache::check(KeyRangeRef range, Version version, bool write) {
	if (!hasVersion(version)) return DENIED_OLD_VERSION;

	ensureSnapshot(version);
	auto& snapshot = snapshots[version];
	// TODO: change snapshot for O(log N) cost
	for (const auto& lock : snapshot) {
		if (range.end <= lock.range.begin) break;
		if (range.begin >= lock.range.end) continue;

		if (lock.mode == LockMode::LOCK_EXCLUSIVE) {
			return DENIED_EXCLUSIVE_LOCK;
		} else if (lock.mode == LockMode::LOCK_READ_SHARED && write) {
			return DENIED_READ_LOCK;
		}
	}
	return OK;
}

Value RangeLockCache::getChanges(Version from) {
	ASSERT(false);
	return Value();
}

RangeLockCache::Snapshot RangeLockCache::getSnapshot(Version at) {
	ASSERT(hasVersion(at));
	ensureSnapshot(at);
	return snapshots[at];
}

std::string RangeLockCache::toString(Version version, Snapshot snapshot) {
	std::string s;
	char buf[16];
	sprintf(buf, "%" PRId64, version);
	s.append("Version: ").append(buf);
	s.append("\n");
	for (const auto& req : snapshot) {
		s.append("  Range: " + printable(req.range));
		s.append(", mode: ").append(getLockModeText(req.mode));
		s.append("\n");
	}
	return s;
}

std::string RangeLockCache::toString() {
	std::string s;
	bool first = true;
	for (const auto& [version, snapshot] : snapshots) {
		if (first) {
			s.append("=== Snapshots ===\n");
			first = false;
		} else {
			s.append("\n");
		}
		s.append(toString(version, snapshot));
	}

	first = true;
	for (const auto& [version, snapshot] : requests) {
		if (first) {
			s.append("=== Requests ===\n");
			first = false;
		} else {
			s.append("\n");
		}
		s.append(toString(version, snapshot));
	}
	return s;
}

void RangeLockCache::ensureSnapshot(Version version) {
	auto snapIter = snapshots.lower_bound(version);
	if (snapIter != snapshots.end() && snapIter->first == version) return;

	ASSERT(requests.find(version) != requests.end());

	if (snapIter != snapshots.begin()) {
		snapIter--;
	}
	if (snapIter == snapshots.end()) {
		// No snapshots before, build first one.
		const auto reqIter = requests.begin();
		ASSERT(reqIter != requests.end());

		std::vector<LockRequest> fLocks;
		for (const auto& req : reqIter->second) {
			ASSERT(req.mode == LockMode::LOCK_EXCLUSIVE || req.mode == LockMode::LOCK_READ_SHARED);
			fLocks.push_back(req);
		}
		std::sort(fLocks.begin(), fLocks.end(), RangeLockCache::lockLess);

		auto& snap = snapshots[reqIter->first];
		for (const auto& req : fLocks) {
			snap.push_back_deep(snap.arena(), req);
		}
		snapIter = snapshots.begin();
	}

	// Start building snapshots till version
	while (snapIter->first < version) {
		snapIter = buildSnapshot(snapIter);
	}
	ASSERT(snapIter->first == version);
}

RangeLockCache::SnapshotIterator RangeLockCache::buildSnapshot(RangeLockCache::SnapshotIterator it) {
	const auto reqIter = requests.lower_bound(it->first + 1);
	ASSERT(reqIter != requests.end());

	std::map<KeyRangeRef, LockRequest, bool (*)(const KeyRangeRef&, const KeyRangeRef&)> locks(rangeLess);
	for (const auto& req : it->second) {
		locks.insert(std::make_pair(req.range, req));
	}

	for (const auto& req : reqIter->second) {
		auto it = locks.lower_bound(req.range);

		switch (req.mode) {
		case LockMode::LOCK_EXCLUSIVE:
		case LockMode::LOCK_READ_SHARED:
			if (it == locks.end()) {
				if (!locks.empty()) {
					ASSERT(rangeLess(locks.rbegin()->first, req.range));
				}
			} else {
				ASSERT(req.range.end <= it->first.begin); // No overlap
			}
			locks.insert(std::make_pair(req.range, req));
			break;

		case LockMode::UNLOCK_EXCLUSIVE:
		case LockMode::UNLOCK_READ_SHARED:
			ASSERT(it != locks.end());
			ASSERT(it->first == req.range);
			locks.erase(it);
			break;

		default:
			ASSERT(false);
		}
	}

	Snapshot& snapshot = snapshots[reqIter->first];
	ASSERT_EQ(snapshot.size(), 0);
	for (const auto& [_, req] : locks) {
		snapshot.push_back_deep(snapshot.arena(), req);
	}
	it++;
	return it;
}

namespace {
TEST_CASE("/fdbclient/LockRange/Cache") {
	RangeLockCache cache;

	RangeLockCache::Requests requests;
	const Version v1 = 100;
	requests.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("a"_sr, "d"_sr), LockMode::LOCK_EXCLUSIVE));
	cache.add(v1, requests);

	ASSERT_EQ(cache.hasVersion(v1), true);
	ASSERT_EQ(cache.check("a"_sr, v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("b"_sr, v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("d"_sr, v1), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("c"_sr, "e"_sr), v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), v1), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("0"_sr, "9"_sr), v1), RangeLockCache::OK);

	RangeLockCache::Requests requests2;
	const Version v2 = 200;
	requests2.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("d"_sr, "e"_sr), LockMode::LOCK_EXCLUSIVE));
	requests2.push_back_deep(requests.arena(),
	                         LockRequest(KeyRangeRef("ee"_sr, "exxx"_sr), LockMode::LOCK_READ_SHARED));
	cache.add(v2, requests2);

	ASSERT_EQ(cache.hasVersion(v1), true);
	ASSERT_EQ(cache.hasVersion(v2), true);
	ASSERT_EQ(cache.check("a"_sr, v1), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), v2), RangeLockCache::DENIED_READ_LOCK);

	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), v2), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ey"_sr, "g"_sr), v2), RangeLockCache::OK);

	std::cout << cache.toString() << "\n";
	return Void();
}

TEST_CASE("/fdbclient/LockRange/CacheExpire") {
	RangeLockCache cache;

	RangeLockCache::Requests requests;
	const Version v1 = 100;
	requests.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("a"_sr, "d"_sr), LockMode::LOCK_EXCLUSIVE));
	cache.add(v1, requests);

	RangeLockCache::Requests requests2;
	const Version v2 = 200;
	requests2.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("d"_sr, "e"_sr), LockMode::LOCK_EXCLUSIVE));
	requests2.push_back_deep(requests.arena(),
	                         LockRequest(KeyRangeRef("ee"_sr, "exxx"_sr), LockMode::LOCK_READ_SHARED));
	cache.add(v2, requests2);

	ASSERT_EQ(cache.hasVersion(v1), true);
	ASSERT_EQ(cache.hasVersion(v1 - 1), false);
	ASSERT_EQ(cache.hasVersion(v1 + 1), false);
	ASSERT_EQ(cache.hasVersion(v2), true);
	ASSERT_EQ(cache.hasVersion(v2 - 10), false);
	ASSERT_EQ(cache.hasVersion(v2 + 100), false);

	cache.expire(v1);
	ASSERT_EQ(cache.check("a"_sr, v1), RangeLockCache::DENIED_OLD_VERSION);
	ASSERT_EQ(cache.check(KeyRangeRef("0"_sr, "6"_sr), v1, /*write=*/false), RangeLockCache::DENIED_OLD_VERSION);

	ASSERT_EQ(cache.check("a"_sr, v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("b"_sr, v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("d"_sr, v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);

	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("c"_sr, "e"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("0"_sr, "9"_sr), v2), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), v2), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), v2), RangeLockCache::DENIED_READ_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), v2, /*write=*/false), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), v2), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), v2, /*write=*/false), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ey"_sr, "g"_sr), v2), RangeLockCache::OK);
	std::cout << cache.toString() << "\n";

	return Void();
}

TEST_CASE("/fdbclient/LockRange/Unlock") {
	RangeLockCache cache;

	RangeLockCache::Requests requests;
	const Version v1 = 100;
	requests.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("a"_sr, "d"_sr), LockMode::LOCK_EXCLUSIVE));
	cache.add(v1, requests);

	RangeLockCache::Requests requests2;
	const Version v2 = 200;
	requests2.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("d"_sr, "e"_sr), LockMode::LOCK_EXCLUSIVE));
	requests2.push_back_deep(requests.arena(),
	                         LockRequest(KeyRangeRef("ee"_sr, "exxx"_sr), LockMode::LOCK_READ_SHARED));
	cache.add(v2, requests2);

	RangeLockCache::Requests requests3; // Unlocks [d, e), Lock [p, t)
	const Version v3 = 300;
	requests3.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("d"_sr, "e"_sr), LockMode::UNLOCK_EXCLUSIVE));
	requests3.push_back_deep(requests.arena(), LockRequest(KeyRangeRef("p"_sr, "t"_sr), LockMode::LOCK_EXCLUSIVE));
	cache.add(v3, requests3);

	ASSERT_EQ(cache.hasVersion(v1), true);
	ASSERT_EQ(cache.hasVersion(v2), true);
	ASSERT_EQ(cache.hasVersion(v3), true);

	std::cout << "Before expire: \n" << cache.toString() << "\n";
	cache.expire(v2);

	ASSERT_EQ(cache.check("a"_sr, v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("b"_sr, v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	std::cout << cache.toString() << "\n";
	ASSERT_EQ(cache.check("d"_sr, v3), RangeLockCache::OK);
	ASSERT_EQ(cache.check("p"_sr, v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check("q"_sr, v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);

	ASSERT_EQ(cache.check(KeyRangeRef("b"_sr, "d"_sr), v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("c"_sr, "e"_sr), v3), RangeLockCache::DENIED_EXCLUSIVE_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("d"_sr, "e"_sr), v3), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("0"_sr, "9"_sr), v3), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), v3), RangeLockCache::DENIED_READ_LOCK);
	ASSERT_EQ(cache.check(KeyRangeRef("ef"_sr, "eg"_sr), v3, /*write=*/false), RangeLockCache::OK);

	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), v3), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ea"_sr, "ee"_sr), v3, /*write=*/false), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("ey"_sr, "g"_sr), v3), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("f"_sr, "p"_sr), v3), RangeLockCache::OK);
	ASSERT_EQ(cache.check(KeyRangeRef("t"_sr, "ta"_sr), v3), RangeLockCache::OK);

	return Void();
}

} // anonymous namespace
