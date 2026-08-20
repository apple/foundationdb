/*
 * RangeLockConfiguration.h
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

#ifndef FDBCLIENT_RANGELOCKCONFIGURATION_H
#define FDBCLIENT_RANGELOCKCONFIGURATION_H
#pragma once

#include "fdbclient/FDBTypes.h"

class Transaction;

// This key is deliberately outside rangeLockPrefix: resetting the lock KRM
// must not erase the record that keeps an incomplete migration fail-closed.
extern const KeyRef rangeLockConfigurationKey;

enum class RangeLockReadiness : uint8_t { Unknown, Migrating, Ready };
enum class RangeLockConfigurationTransition : uint8_t { Invalid, Initialize, Begin, Replay, Finish };

// An absent key is Unknown. Only a newly created database or a complete,
// database-lock-protected reconciliation may publish Ready.
class RangeLockConfiguration {
public:
	constexpr static FileIdentifier file_identifier = 1384411;
	constexpr static uint32_t currentFormatRevision = 1;

	RangeLockConfiguration() = default;
	static RangeLockConfiguration ready(UID completedMigration = UID());
	static RangeLockConfiguration migrating(UID databaseLockId, const KeyRef& nextKey);
	RangeLockConfiguration advance(const KeyRef& nextKey) const;

	RangeLockReadiness readiness() const { return readiness_; }
	uint32_t formatRevision() const { return formatRevision_; }
	bool isReady() const { return readiness_ == RangeLockReadiness::Ready; }
	bool isMigrating() const { return readiness_ == RangeLockReadiness::Migrating; }
	UID migrationId() const { return migrationId_; }
	const Key& nextKey() const { return nextKey_; }
	bool completedBy(UID id) const { return isReady() && id.isValid() && migrationId_ == id; }
	bool isValid() const;
	bool operator==(const RangeLockConfiguration&) const = default;
	std::string toString() const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, formatRevision_, readiness_, migrationId_, nextKey_);
	}

private:
	RangeLockConfiguration(RangeLockReadiness readiness, UID migrationId, const KeyRef& nextKey);

	uint32_t formatRevision_ = currentFormatRevision;
	RangeLockReadiness readiness_ = RangeLockReadiness::Unknown;
	UID migrationId_;
	Key nextKey_;
};

Value rangeLockConfigurationValue(const RangeLockConfiguration& configuration);
RangeLockConfiguration decodeRangeLockConfiguration(const ValueRef& value);
RangeLockConfigurationTransition classifyRangeLockConfigurationTransition(const RangeLockConfiguration& previous,
                                                                          const RangeLockConfiguration& next,
                                                                          bool initializingDatabase = false);
Optional<UID> decodeRangeLockDatabaseLock(const Optional<Value>& value);

// These are current proxy capabilities, not proof that every resolver or
// cluster controller has been upgraded. Reconciliation requires a homogeneous
// upgraded transaction system as an operator-enforced precondition.
struct RangeLockAdmissionStatus {
	bool allProxiesEnableAcquisition = false;
	bool allProxiesEncodeShardLocations = false;
	bool allProxiesHaveValidState = false;
	Optional<bool> dataDistributorEncodesShardLocations;
};

Future<RangeLockAdmissionStatus> getRangeLockAdmissionStatus(Database cx, bool includeBulkLoadConfiguration = false);
Future<RangeLockConfiguration> getRangeLockConfiguration(Transaction* tr);
Future<RangeLockConfiguration> getRangeLockConfiguration(Database cx);
Future<Void> requireRangeLockReadyForAcquisition(Transaction* tr);

// Transaction-level steps do not commit. The database lock must already be
// present and owned by exactly databaseLockId. An interrupted migration keeps
// that lock and its durable cursor, and can be resumed with the same UID.
Future<RangeLockConfiguration> beginRangeLockReconciliation(Transaction* tr, UID databaseLockId);
Future<RangeLockConfiguration> reconcileRangeLockBatch(Transaction* tr, UID databaseLockId, int rowLimit = 1000);
Future<Void> finishRangeLockReconciliation(Transaction* tr, UID databaseLockId);
Future<Void> reconcileRangeLocks(Database cx, UID databaseLockId);

#endif
