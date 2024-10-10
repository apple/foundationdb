/*
 * RangeLock.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_RANGELOCK_H
#define FDBCLIENT_RANGELOCK_H
#include "flow/Error.h"
#include "flow/IRandom.h"
#include <string>
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

enum class RangeLockType : uint8_t {
	Invalid = 0,
	RejectCommits = 1, // reject all commits to the locked range
};

// Owner who owns the lock
// A lock can be only removed by the owner
struct RangeLockOwner {
	constexpr static FileIdentifier file_identifier = 1384408;

public:
	RangeLockOwner() = default;
	RangeLockOwner(const std::string& uniqueId, const std::string& description)
	  : uniqueId(uniqueId), description(description), logId(deterministicRandom()->randomUniqueID()),
	    creationTime(now()) {
		if (!isValid()) {
			throw range_lock_failed();
		}
	}

	bool isValid() const { return !uniqueId.empty() && !description.empty(); }

	std::string toString() const {
		return "RangeLockOwner: [UniqueId]: " + uniqueId + ", [Description]: " + description +
		           ", [LogId]: " + logId.toString(),
		       ", [CreationTime]: " + std::to_string(creationTime);
	}

	bool operator==(RangeLockOwner const& r) const { return uniqueId == r.uniqueId; }

	std::string getUniqueId() const { return uniqueId; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, uniqueId, description, logId, creationTime);
	}

private:
	std::string uniqueId; // Unique globally
	std::string description; // More details
	UID logId; // For logging purpose
	double creationTime; // Indicate when the data structure is created
};

// Metadata of a lock on a range
struct RangeLockState {
	constexpr static FileIdentifier file_identifier = 1384409;

public:
	RangeLockState() = default;

	RangeLockState(RangeLockType type, const std::string& ownerUniqueId)
	  : lockType(type), ownerUniqueId(ownerUniqueId) {
		ASSERT(isValid());
	}

	bool isValid() const { return lockType != RangeLockType::Invalid && !ownerUniqueId.empty(); }

	std::string rangeLockTypeString() const {
		if (lockType == RangeLockType::Invalid) {
			return "invalid";
		} else if (lockType == RangeLockType::RejectCommits) {
			return "rejectCommit";
		} else {
			UNREACHABLE();
		}
	}

	std::string toString() const {
		return "RangeLockState: [lockType]: " + rangeLockTypeString() + " [Owner]: " + ownerUniqueId;
	}

	bool isLockedFor(RangeLockType inputLockType) const { return lockType == inputLockType; }

	bool operator==(RangeLockState const& r) const {
		return lockType == r.lockType && ownerUniqueId == r.ownerUniqueId;
	}

	std::string getLockUniqueString() const { return ownerUniqueId + rangeLockTypeString(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ownerUniqueId, lockType);
	}

private:
	std::string ownerUniqueId;
	RangeLockType lockType;
};

// Persisted state on a range. A range can have multiple locks distinguishing by owner and lockType.
// For each combination of owner and lockType, there is an unique lock for the combination
// RangeLockSetState tracks all those unique locks
struct RangeLockSetState {
	constexpr static FileIdentifier file_identifier = 1384410;

public:
	RangeLockSetState() = default;

	bool empty() const { return locks.empty(); }

	std::vector<RangeLockState> getAllLockStats() const {
		std::vector<RangeLockState> res;
		for (const auto& [name, lock] : locks) {
			res.push_back(lock);
		}
		return res;
	}

	bool isValid() const {
		for (const auto& [owner, lock] : locks) {
			if (!lock.isValid()) {
				return false; // Any invalid makes this set invalid
			}
		}
		return true;
	}

	std::string toString() const { return "RangeLockSetState: " + describe(getAllLockStats()); }

	bool operator==(RangeLockSetState const& r) const { return getAllLockStats() == r.getAllLockStats(); }

	void insert(const RangeLockState& inputLock) {
		ASSERT(inputLock.isValid());
		locks[inputLock.getLockUniqueString()] = inputLock;
		return;
	}

	void remove(const RangeLockState& inputLock) {
		ASSERT(inputLock.isValid());
		locks.erase(inputLock.getLockUniqueString());
		return;
	}

	bool isLockedFor(RangeLockType lockType) const {
		for (const auto& [owner, lock] : locks) {
			ASSERT(lock.isValid());
			if (lock.isLockedFor(lockType)) {
				return true;
			}
		}
		return false;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, locks);
	}

private:
	std::map<std::string, RangeLockState> locks;
};

#endif
