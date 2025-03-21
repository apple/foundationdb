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

using RangeLockOwnerName = std::string;
using RangeLockUniqueString = std::string;
using RangeLockID = std::string;

enum class RangeLockType : uint8_t {
	Invalid = 0,
	ExclusiveReadLock = 1, // reject all commits to the locked range
};

// The app/user that owns the lock.
// A lock can be only removed by the owner
struct RangeLockOwner {
	constexpr static FileIdentifier file_identifier = 1384408;

public:
	RangeLockOwner() = default;
	RangeLockOwner(const std::string& ownerUniqueId, const std::string& description)
	  : ownerUniqueId(ownerUniqueId), description(description), logId(deterministicRandom()->randomUniqueID()),
	    creationTime(now()) {
		if (!isValid()) {
			throw range_lock_failed();
		}
	}

	bool isValid() const { return !ownerUniqueId.empty() && !description.empty(); }

	std::string toString() const {
		return "RangeLockOwner: [OwnerUniqueId]: " + ownerUniqueId + ", [Description]: " + description +
		       ", [LogId]: " + logId.toString() + ", [CreationTime]: " + std::to_string(creationTime);
	}

	bool operator==(RangeLockOwner const& r) const { return ownerUniqueId == r.ownerUniqueId; }

	RangeLockOwnerName getOwnerUniqueId() const { return ownerUniqueId; }

	void setDescription(const std::string& inputDescription) {
		description = inputDescription;
		return;
	}

	std::string getDescription() const { return description; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ownerUniqueId, description, logId, creationTime);
	}

private:
	RangeLockOwnerName ownerUniqueId; // The owner's unique ID and the owner is free to use as many times as needed.
	std::string description; // More details about the owner
	UID logId; // For logging purpose
	double creationTime; // Indicate when the data structure is created
};

// Metadata of a lock on a range
struct RangeLockState {
	constexpr static FileIdentifier file_identifier = 1384409;

public:
	RangeLockState() = default;

	RangeLockState(RangeLockType type, const RangeLockOwnerName& ownerUniqueId, const KeyRange& range)
	  : lockType(type), ownerUniqueId(ownerUniqueId), range(range) {
		ASSERT(isValid());
	}

	bool isValid() const { return lockType != RangeLockType::Invalid && !ownerUniqueId.empty(); }

	static std::string rangeLockTypeString(const RangeLockType& type) {
		if (type == RangeLockType::Invalid) {
			return "invalid";
		} else if (type == RangeLockType::ExclusiveReadLock) {
			return "ExclusiveReadLock";
		} else {
			UNREACHABLE();
		}
	}

	KeyRange getRange() const { return range; }

	std::string toString() const {
		return "RangeLockState: [LockType]: " + rangeLockTypeString(lockType) + ", [Owner]: " + ownerUniqueId +
		       ", [Range]: " + range.toString() + ", [RangeLockID]: " + lockId;
	}

	bool isLockedFor(RangeLockType inputLockType) const { return lockType == inputLockType; }

	bool operator==(RangeLockState const& r) const {
		return lockType == r.lockType && ownerUniqueId == r.ownerUniqueId && range == r.range;
	}

	// TODO: use lockId
	RangeLockUniqueString getLockUniqueString() const {
		return ownerUniqueId + rangeLockTypeString(lockType) + range.toString();
	}

	RangeLockOwnerName getOwnerUniqueId() const { return ownerUniqueId; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ownerUniqueId, lockType, range, lockId);
	}

private:
	RangeLockOwnerName ownerUniqueId; // The app/user that owns the lock.
	RangeLockType lockType;
	KeyRange range;
	RangeLockID lockId; // Reserved for physical lock. Is not used now.
};

// Persisted state on a range. A range can have multiple locks distinguishing by owner and lockType.
// For each combination of owner and lockType, there is an unique lock for the combination
// RangeLockStateSet tracks all those unique locks
struct RangeLockStateSet {
	constexpr static FileIdentifier file_identifier = 1384410;

public:
	RangeLockStateSet() = default;

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

	std::string toString() const { return "RangeLockStateSet: " + describe(getAllLockStats()); }

	const std::map<RangeLockUniqueString, RangeLockState>& getLocks() const { return locks; }

	bool operator==(RangeLockStateSet const& r) const {
		auto rLocks = r.getLocks();
		if (locks.size() != rLocks.size()) {
			return false;
		}
		std::map<RangeLockUniqueString, RangeLockState>::const_iterator iterator = locks.begin();
		std::map<RangeLockUniqueString, RangeLockState>::const_iterator rIterator = rLocks.begin();
		while (iterator != locks.end() && rIterator != rLocks.end()) {
			if (iterator->first != rIterator->first || iterator->second != rIterator->second) {
				return false;
			}
			++iterator;
			++rIterator;
		}
		return true;
	}

	void insertIfNotExist(const RangeLockState& inputLock) {
		ASSERT(inputLock.isValid());
		if (inputLock.isLockedFor(RangeLockType::ExclusiveReadLock) && !locks.empty() &&
		    locks.find(inputLock.getLockUniqueString()) == locks.end()) {
			throw range_lock_failed();
		}
		locks.insert({ inputLock.getLockUniqueString(), inputLock });
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
	std::map<RangeLockUniqueString, RangeLockState> locks;
};

#endif
