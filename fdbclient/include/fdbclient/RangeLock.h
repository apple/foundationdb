/*
 * RangeLock.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include <utility>
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

using RangeLockOwnerName = std::string;
using RangeLockUniqueString = std::string;
using RangeLockID = std::string;

class Transaction;

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
	UID getGeneration() const { return logId; }

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
	UID logId; // Identifies this registration, including after an owner name is reused.
	double creationTime; // Indicate when the data structure is created
};

// Metadata of a lock on a range
struct RangeLockState {
	constexpr static FileIdentifier file_identifier = 1384409;

public:
	RangeLockState() = default;

	RangeLockState(RangeLockType type,
	               const RangeLockOwnerName& ownerUniqueId,
	               const KeyRange& range,
	               RangeLockID lockId = RangeLockID())
	  : ownerUniqueId(ownerUniqueId), lockType(type), range(range), lockId(std::move(lockId)) {
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

	bool hasSameLogicalIdentity(RangeLockState const& r) const {
		return lockType == r.lockType && ownerUniqueId == r.ownerUniqueId && range == r.range;
	}

	bool hasSameAcquisition(RangeLockState const& r) const { return hasSameLogicalIdentity(r) && lockId == r.lockId; }

	bool operator==(RangeLockState const& r) const { return hasSameAcquisition(r); }

	// This legacy map key is persisted and can collide. Compare the lock fields for identity;
	// changing this encoding requires migrating existing range-lock metadata.
	RangeLockUniqueString getLockUniqueString() const {
		return ownerUniqueId + rangeLockTypeString(lockType) + range.toString();
	}

	RangeLockOwnerName getOwnerUniqueId() const { return ownerUniqueId; }
	const RangeLockID& getLockId() const { return lockId; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ownerUniqueId, lockType, range, lockId);
	}

private:
	RangeLockOwnerName ownerUniqueId; // The app/user that owns the lock.
	RangeLockType lockType = RangeLockType::Invalid;
	KeyRange range;
	RangeLockID lockId; // Empty only for legacy, unfenced acquisitions.
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

	bool containsLogicalLock(const RangeLockState& inputLock) const {
		for (const auto& [name, lock] : locks) {
			if (lock.hasSameLogicalIdentity(inputLock)) {
				return true;
			}
		}
		return false;
	}

	bool containsExactLock(const RangeLockState& inputLock) const {
		for (const auto& [name, lock] : locks) {
			if (lock.hasSameAcquisition(inputLock)) {
				return true;
			}
		}
		return false;
	}

	bool operator==(RangeLockStateSet const& r) const {
		auto rLocks = r.getLocks();
		if (locks.size() != rLocks.size()) {
			return false;
		}
		auto iterator = locks.begin();
		auto rIterator = rLocks.begin();
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
		if (containsExactLock(inputLock)) {
			return;
		}
		if (inputLock.isLockedFor(RangeLockType::ExclusiveReadLock) && !locks.empty()) {
			throw range_lock_failed();
		}
		if (!locks.insert({ inputLock.getLockUniqueString(), inputLock }).second) {
			throw range_lock_failed();
		}
	}

	void remove(const RangeLockState& inputLock) {
		ASSERT(inputLock.isValid());
		for (auto it = locks.begin(); it != locks.end(); ++it) {
			if (it->second.hasSameAcquisition(inputLock)) {
				locks.erase(it);
				return;
			}
		}
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

// Persist a rangeLock owner to database metadata.
// A range can only be locked by a registered owner.
Future<Void> registerRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID, std::string description);

// Remove an owner only when it holds no locks. The expected-owner overload also fences owner-name reuse.
Future<Void> removeRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID);
Future<Void> removeRangeLockOwner(Database cx, RangeLockOwner expectedOwner);

// Get all registered rangeLock owners.
AsyncResult<std::vector<RangeLockOwner>> getAllRangeLockOwners(Database cx);

// Get a rangeLock owner by ownerUniqueID.
Future<Optional<RangeLockOwner>> getRangeLockOwner(Database cx, RangeLockOwnerName ownerUniqueID);

// Block write traffic to a non-empty user range within normalKeys; otherwise throw range_lock_failed.
// One transaction can call takeExclusiveReadLockOnRange at most one time.
Future<Void> takeExclusiveReadLockOnRange(Transaction* tr, KeyRange range, RangeLockOwnerName ownerUniqueID);
Future<Void> takeExclusiveReadLockOnRange(Database cx, KeyRange range, RangeLockOwnerName ownerUniqueID);

// Fenced operations require a nonempty, never-reused acquisition ID. Retrying a take is idempotent only for
// the same acquisition. Owner generation is the value returned by RangeLockOwner::getGeneration().
Future<Void> takeExclusiveReadLockOnRange(Transaction* tr, RangeLockState requestedLock, UID expectedOwnerGeneration);
Future<Void> takeExclusiveReadLockOnRange(Database cx, RangeLockState requestedLock, UID expectedOwnerGeneration);

// Administrative, unfenced release of a non-empty range in normalKeys. One transaction can call this at most
// once. A live BulkLoad job must be cancelled through its job API unless allowActiveBulkLoad explicitly authorizes
// emergency fence removal.
Future<Void> releaseExclusiveReadLockOnRange(Transaction* tr,
                                             KeyRange range,
                                             RangeLockOwnerName ownerUniqueID,
                                             bool allowActiveBulkLoad = false);
Future<Void> releaseExclusiveReadLockOnRange(Database cx,
                                             KeyRange range,
                                             RangeLockOwnerName ownerUniqueID,
                                             bool allowActiveBulkLoad = false);

// Release only this acquisition. An empty range is already released; a replacement acquisition is rejected.
Future<Void> releaseExclusiveReadLockOnRange(Transaction* tr, RangeLockState expectedLock);
Future<Void> releaseExclusiveReadLockOnRange(Database cx, RangeLockState expectedLock);

// Check the whole original range at the transaction's read version. Used to fence work before it mutates metadata.
Future<bool> isExclusiveReadLockHeld(Transaction* tr, RangeLockState expectedLock);

// Upgrade an exact legacy acquisition in the same transaction as its caller's durable fence metadata.
Future<Void> adoptExclusiveReadLockOnRange(Transaction* tr, RangeLockState requestedLock, UID expectedOwnerGeneration);

// Get locked ranges within a non-empty range in normalKeys; otherwise throw range_lock_failed.
Future<std::vector<std::pair<KeyRange, RangeLockState>>> findExclusiveReadLockOnRange(
    Database cx,
    KeyRange range,
    Optional<RangeLockOwnerName> ownerName = Optional<RangeLockOwnerName>());

// Administrative, unfenced release of all locks owned by the input user. Not transactional.
Future<Void> releaseExclusiveReadLockByUser(Database cx,
                                            RangeLockOwnerName ownerUniqueID,
                                            bool allowActiveBulkLoad = false);

#endif
