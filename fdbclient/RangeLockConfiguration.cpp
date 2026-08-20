/*
 * RangeLockConfiguration.cpp
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

#include "fdbclient/RangeLockConfiguration.h"

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "flow/UnitTest.h"

const KeyRef rangeLockConfigurationKey = "\xff/rangeLockConfiguration"_sr;

namespace {

// Fixed-width discriminator/revision/state/UID/length, followed by cursor
// bytes. Unlike ObjectReader, this new on-disk format can be fully bounded
// before any deserialization. Its revision is independent of wire protocol.
constexpr int rangeLockConfigurationHeaderBytes =
    sizeof(FileIdentifier) + sizeof(uint32_t) + sizeof(uint8_t) + 2 * sizeof(uint64_t) + sizeof(int32_t);

} // namespace

RangeLockConfiguration::RangeLockConfiguration(RangeLockReadiness readiness, UID migrationId, const KeyRef& nextKey)
  : readiness_(readiness), migrationId_(migrationId), nextKey_(nextKey) {
	if (!isValid()) {
		throw range_lock_not_ready();
	}
}

RangeLockConfiguration RangeLockConfiguration::ready(UID completedMigration) {
	return RangeLockConfiguration(RangeLockReadiness::Ready, completedMigration, normalKeys.end);
}

RangeLockConfiguration RangeLockConfiguration::migrating(UID databaseLockId, const KeyRef& nextKey) {
	return RangeLockConfiguration(RangeLockReadiness::Migrating, databaseLockId, nextKey);
}

RangeLockConfiguration RangeLockConfiguration::advance(const KeyRef& nextKey) const {
	if (!isMigrating() || nextKey <= nextKey_ || nextKey > normalKeys.end) {
		throw range_lock_not_ready();
	}
	return migrating(migrationId_, nextKey);
}

bool RangeLockConfiguration::isValid() const {
	if (formatRevision_ != currentFormatRevision) {
		return false;
	}
	switch (readiness_) {
	case RangeLockReadiness::Unknown:
		return !migrationId_.isValid() && nextKey_.empty();
	case RangeLockReadiness::Migrating:
		return migrationId_.isValid() && nextKey_ <= normalKeys.end;
	case RangeLockReadiness::Ready:
		return nextKey_ == normalKeys.end;
	default:
		return false;
	}
}

std::string RangeLockConfiguration::toString() const {
	const char* state = isReady() ? "ready" : isMigrating() ? "migrating" : "unknown";
	return format("v%u %s migration=%s next=%s",
	              formatRevision_,
	              state,
	              migrationId_.toString().c_str(),
	              printable(nextKey_).c_str());
}

Value rangeLockConfigurationValue(const RangeLockConfiguration& configuration) {
	if (!configuration.isValid() || configuration.readiness() == RangeLockReadiness::Unknown) {
		throw range_lock_not_ready();
	}
	BinaryWriter writer(Unversioned());
	writer << RangeLockConfiguration::file_identifier << configuration.formatRevision()
	       << static_cast<uint8_t>(configuration.readiness()) << configuration.migrationId()
	       << static_cast<int32_t>(configuration.nextKey().size());
	writer.serializeBytes(configuration.nextKey());
	return writer.toValue();
}

RangeLockConfiguration decodeRangeLockConfiguration(const ValueRef& value) {
	if (value.size() < rangeLockConfigurationHeaderBytes) {
		throw range_lock_not_ready();
	}
	BinaryReader reader(value, Unversioned());
	FileIdentifier fileIdentifier;
	uint32_t revision;
	uint8_t readiness;
	UID migrationId;
	int32_t cursorBytes;
	reader >> fileIdentifier >> revision >> readiness >> migrationId >> cursorBytes;
	if (fileIdentifier != RangeLockConfiguration::file_identifier ||
	    revision != RangeLockConfiguration::currentFormatRevision || cursorBytes < 0 ||
	    static_cast<size_t>(cursorBytes) != reader.remainingBytes()) {
		throw range_lock_not_ready();
	}
	const KeyRef cursor = value.substr(rangeLockConfigurationHeaderBytes, cursorBytes);
	if (readiness == static_cast<uint8_t>(RangeLockReadiness::Ready) && cursor == normalKeys.end) {
		return RangeLockConfiguration::ready(migrationId);
	}
	if (readiness == static_cast<uint8_t>(RangeLockReadiness::Migrating)) {
		return RangeLockConfiguration::migrating(migrationId, cursor);
	}
	throw range_lock_not_ready();
}

RangeLockConfigurationTransition classifyRangeLockConfigurationTransition(const RangeLockConfiguration& previous,
                                                                          const RangeLockConfiguration& next,
                                                                          bool initializingDatabase) {
	if (!previous.isValid() || !next.isValid()) {
		return RangeLockConfigurationTransition::Invalid;
	}
	if (initializingDatabase && next == RangeLockConfiguration::ready()) {
		return RangeLockConfigurationTransition::Initialize;
	}
	if (!previous.isMigrating() && next.isMigrating() && next.nextKey() == normalKeys.begin &&
	    !previous.completedBy(next.migrationId())) {
		return RangeLockConfigurationTransition::Begin;
	}
	if (previous.isMigrating() && next.migrationId() == previous.migrationId()) {
		if (next.isMigrating() && next.nextKey() > previous.nextKey()) {
			return RangeLockConfigurationTransition::Replay;
		}
		if (next.isReady() && previous.nextKey() == normalKeys.end) {
			return RangeLockConfigurationTransition::Finish;
		}
	}
	return RangeLockConfigurationTransition::Invalid;
}

Optional<UID> decodeRangeLockDatabaseLock(const Optional<Value>& value) {
	// lockDatabase stores a ten-byte versionstamp followed by an unversioned UID.
	if (!value.present() || value.get().size() != 26) {
		return Optional<UID>();
	}
	return BinaryReader::fromStringRef<UID>(value.get().substr(10), Unversioned());
}

namespace {

void setRangeLockManagementOptions(Transaction* tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
}

Future<Void> requireExactDatabaseLock(Transaction* tr, UID databaseLockId) {
	setRangeLockManagementOptions(tr);
	if (!databaseLockId.isValid() ||
	    decodeRangeLockDatabaseLock(co_await tr->get(databaseLockedKey)) != Optional<UID>(databaseLockId)) {
		throw database_locked();
	}
}

ValueRef rangeLockReplayValue(const KeyValueRef& row) {
	// The terminal boundary covers no normal keys. krmDecodeRanges may carry
	// an unrelated predecessor's value here when the source KRM is empty.
	return row.key == normalKeys.end ? ValueRef() : row.value;
}

RangeLockAdmissionStatus aggregateRangeLockAdmissionStatus(const std::vector<RangeLockProxyStatusReply>& replies,
                                                           bool includeBulkLoadConfiguration) {
	if (replies.empty()) {
		throw range_lock_not_ready();
	}
	RangeLockAdmissionStatus result{ true, true, true, Optional<bool>() };
	Optional<UID> dataDistributorId;
	if (includeBulkLoadConfiguration) {
		result.dataDistributorEncodesShardLocations = true;
	}
	for (const auto& reply : replies) {
		if (reply.formatRevision != RangeLockConfiguration::currentFormatRevision) {
			throw range_lock_not_ready();
		}
		result.allProxiesEnableAcquisition &= reply.admissionEnabled;
		result.allProxiesEncodeShardLocations &= reply.shardEncodeLocationMetadata;
		result.allProxiesHaveValidState &= reply.enforcementStateValid;
		if (includeBulkLoadConfiguration) {
			if (!reply.dataDistributorId.present() || !reply.dataDistributorEncodesShardLocations.present() ||
			    (dataDistributorId.present() && dataDistributorId.get() != reply.dataDistributorId.get())) {
				throw range_lock_not_ready();
			}
			dataDistributorId = reply.dataDistributorId;
			result.dataDistributorEncodesShardLocations =
			    result.dataDistributorEncodesShardLocations.get() && reply.dataDistributorEncodesShardLocations.get();
		}
	}
	return result;
}

Future<Void> requireValidCompletedRangeLockMigration(Database cx) {
	// A completed ID is an idempotent retry, not permission to reset a later
	// poisoned Ready map. Repairing that state requires a fresh migration ID.
	if (!(co_await getRangeLockAdmissionStatus(cx)).allProxiesHaveValidState) {
		throw range_lock_not_ready();
	}
}

} // namespace

Future<RangeLockAdmissionStatus> getRangeLockAdmissionStatus(Database cx, bool includeBulkLoadConfiguration) {
	while (true) {
		co_await cx->getCommitProxiesFuture(UseProvisionalProxies::False);
		const UID clientInfoId = cx->clientInfo->get().id;
		const std::vector<CommitProxyInterface> proxies = cx->clientInfo->get().commitProxies;
		if (proxies.empty() ||
		    std::any_of(proxies.begin(), proxies.end(), [](const auto& proxy) { return proxy.provisional; })) {
			co_await cx->onProxiesChanged();
			continue;
		}
		std::vector<Future<RangeLockProxyStatusReply>> requests;
		for (const auto& proxy : proxies) {
			requests.push_back(
			    proxy.rangeLockStatus.getReply(RangeLockProxyStatusRequest(includeBulkLoadConfiguration)));
		}
		try {
			// A pre-upgrade proxy does not have this endpoint. This is only a
			// fail-closed capability probe, not mixed-version upgrade support.
			co_await timeoutError(waitForAll(requests), includeBulkLoadConfiguration ? 10.0 : 5.0);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			if (clientInfoId != cx->clientInfo->get().id) {
				continue;
			}
			throw range_lock_not_ready();
		}
		if (clientInfoId != cx->clientInfo->get().id) {
			continue;
		}
		std::vector<RangeLockProxyStatusReply> replies;
		replies.reserve(requests.size());
		for (const auto& request : requests) {
			replies.push_back(request.get());
		}
		co_return aggregateRangeLockAdmissionStatus(replies, includeBulkLoadConfiguration);
	}
}

Future<RangeLockConfiguration> getRangeLockConfiguration(Transaction* tr) {
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	Optional<Value> value = co_await tr->get(rangeLockConfigurationKey);
	co_return value.present() ? decodeRangeLockConfiguration(value.get()) : RangeLockConfiguration();
}

Future<RangeLockConfiguration> getRangeLockConfiguration(Database cx) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			setRangeLockManagementOptions(&tr);
			co_return co_await getRangeLockConfiguration(&tr);
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Void> requireRangeLockReadyForAcquisition(Transaction* tr) {
	if (!(co_await getRangeLockConfiguration(tr)).isReady()) {
		throw range_lock_not_ready();
	}
	const auto status = co_await getRangeLockAdmissionStatus(tr->getDatabase());
	if (!status.allProxiesEnableAcquisition || !status.allProxiesHaveValidState) {
		throw range_lock_not_ready();
	}
}

Future<RangeLockConfiguration> beginRangeLockReconciliation(Transaction* tr, UID databaseLockId) {
	co_await requireExactDatabaseLock(tr, databaseLockId);
	RangeLockConfiguration configuration = co_await getRangeLockConfiguration(tr);
	if (configuration.isMigrating()) {
		if (configuration.migrationId() != databaseLockId) {
			throw database_locked();
		}
		co_return configuration;
	}
	if (configuration.completedBy(databaseLockId)) {
		co_return configuration;
	}
	configuration = RangeLockConfiguration::migrating(databaseLockId, normalKeys.begin);
	tr->set(rangeLockConfigurationKey, rangeLockConfigurationValue(configuration));
	co_return configuration;
}

Future<RangeLockConfiguration> reconcileRangeLockBatch(Transaction* tr, UID databaseLockId, int rowLimit) {
	co_await requireExactDatabaseLock(tr, databaseLockId);
	RangeLockConfiguration configuration = co_await getRangeLockConfiguration(tr);
	if (!configuration.isMigrating() || configuration.migrationId() != databaseLockId || rowLimit < 2) {
		throw range_lock_not_ready();
	}
	if (configuration.nextKey() == normalKeys.end) {
		co_return configuration;
	}
	RangeResult ranges = co_await krmGetRanges(tr,
	                                           rangeLockPrefix,
	                                           KeyRangeRef(configuration.nextKey(), normalKeys.end),
	                                           rowLimit,
	                                           CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES);
	if (ranges.size() < 2 || ranges.front().key != configuration.nextKey() ||
	    ranges.back().key <= configuration.nextKey() || ranges.back().key > normalKeys.end) {
		throw range_lock_not_ready();
	}
	const Key nextKey = ranges.back().key;
	// Rewrite the same logical KRM. krmGetRanges can synthesize cursor/end
	// boundaries; retain every returned value, including empty intervals.
	tr->clear(KeyRangeRef(configuration.nextKey().withPrefix(rangeLockPrefix), nextKey.withPrefix(rangeLockPrefix)));
	for (const auto& row : ranges) {
		tr->set(row.key.withPrefix(rangeLockPrefix), rangeLockReplayValue(row));
	}
	configuration = configuration.advance(nextKey);
	tr->set(rangeLockConfigurationKey, rangeLockConfigurationValue(configuration));
	co_return configuration;
}

Future<Void> finishRangeLockReconciliation(Transaction* tr, UID databaseLockId) {
	co_await requireExactDatabaseLock(tr, databaseLockId);
	RangeLockConfiguration configuration = co_await getRangeLockConfiguration(tr);
	if (!configuration.isMigrating() || configuration.migrationId() != databaseLockId ||
	    configuration.nextKey() != normalKeys.end) {
		throw range_lock_not_ready();
	}
	tr->set(rangeLockConfigurationKey, rangeLockConfigurationValue(RangeLockConfiguration::ready(databaseLockId)));
	tr->clear(databaseLockedKey);
}

Future<Void> reconcileRangeLocks(Database cx, UID databaseLockId) {
	if (!databaseLockId.isValid()) {
		throw range_lock_not_ready();
	}
	if ((co_await getRangeLockConfiguration(cx)).completedBy(databaseLockId)) {
		co_await requireValidCompletedRangeLockMigration(cx);
		co_return;
	}
	// Probe before taking the database offline. The eligibility booleans are
	// intentionally ignored: reconciliation is also supported with admission
	// disabled. This cannot prove that resolver/CC binaries are homogeneous.
	co_await getRangeLockAdmissionStatus(cx);
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			setRangeLockManagementOptions(&tr);
			// This read and lock acquisition must commit together. Otherwise a
			// concurrent same-ID finisher could unlock between them and leave
			// this retry holding a redundant database lock forever.
			if ((co_await getRangeLockConfiguration(&tr)).completedBy(databaseLockId)) {
				co_await requireValidCompletedRangeLockMigration(cx);
				co_return;
			}
			co_await lockDatabase(&tr, databaseLockId);
			co_await tr.commit();
			tr.reset();
			break;
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_database_locked || err.code() == error_code_range_lock_not_ready) {
			throw err;
		}
		co_await tr.onError(err);
	}
	while (true) {
		Error err;
		try {
			setRangeLockManagementOptions(&tr);
			RangeLockConfiguration configuration = co_await getRangeLockConfiguration(&tr);
			if (configuration.completedBy(databaseLockId)) {
				co_await requireValidCompletedRangeLockMigration(cx);
				co_return;
			}
			if (!configuration.isMigrating()) {
				co_await beginRangeLockReconciliation(&tr, databaseLockId);
			} else if (configuration.nextKey() != normalKeys.end) {
				co_await reconcileRangeLockBatch(&tr, databaseLockId);
			} else {
				co_await finishRangeLockReconciliation(&tr, databaseLockId);
			}
			co_await tr.commit();
			tr.reset();
			continue;
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_database_locked || err.code() == error_code_range_lock_not_ready) {
			throw err;
		}
		co_await tr.onError(err);
	}
}

TEST_CASE("/RangeLock/ConfigurationTransitions") {
	const UID owner(1, 2);
	const UID other(3, 4);
	const RangeLockConfiguration unknown;
	const auto begin = RangeLockConfiguration::migrating(owner, normalKeys.begin);
	const auto middle = begin.advance("m"_sr);
	const auto end = middle.advance(normalKeys.end);
	const auto ready = RangeLockConfiguration::ready(owner);
	ASSERT(unknown.isValid());
	ASSERT(!unknown.isReady());
	ASSERT_EQ(classifyRangeLockConfigurationTransition(unknown, RangeLockConfiguration::ready()),
	          RangeLockConfigurationTransition::Invalid);
	ASSERT_EQ(classifyRangeLockConfigurationTransition(unknown, RangeLockConfiguration::ready(), true),
	          RangeLockConfigurationTransition::Initialize);
	ASSERT_EQ(classifyRangeLockConfigurationTransition(unknown, begin), RangeLockConfigurationTransition::Begin);
	ASSERT_EQ(classifyRangeLockConfigurationTransition(begin, middle), RangeLockConfigurationTransition::Replay);
	ASSERT_EQ(classifyRangeLockConfigurationTransition(middle, end), RangeLockConfigurationTransition::Replay);
	ASSERT_EQ(classifyRangeLockConfigurationTransition(middle, ready), RangeLockConfigurationTransition::Invalid);
	ASSERT_EQ(classifyRangeLockConfigurationTransition(end, ready), RangeLockConfigurationTransition::Finish);
	ASSERT_EQ(classifyRangeLockConfigurationTransition(middle, begin), RangeLockConfigurationTransition::Invalid);
	ASSERT_EQ(classifyRangeLockConfigurationTransition(middle, RangeLockConfiguration::migrating(other, "z"_sr)),
	          RangeLockConfigurationTransition::Invalid);
	ASSERT(decodeRangeLockConfiguration(rangeLockConfigurationValue(middle)) == middle);
	ASSERT(ready.completedBy(owner));
	ASSERT(!ready.completedBy(other));
	ASSERT(!decodeRangeLockDatabaseLock(Optional<Value>()).present());
	ASSERT(!decodeRangeLockDatabaseLock(Optional<Value>("short"_sr)).present());
	ASSERT(decodeRangeLockDatabaseLock(Optional<Value>(
	           BinaryWriter::toValue(owner, Unversioned()).withPrefix("0123456789"_sr))) == Optional<UID>(owner));
	co_return;
}

TEST_CASE("/RangeLock/ConfigurationEncoding") {
	const auto configuration = RangeLockConfiguration::migrating(UID(1, 2), "cursor"_sr);
	const Value encoded = rangeLockConfigurationValue(configuration);
	ASSERT(decodeRangeLockConfiguration(encoded) == configuration);
	auto rejected = [](const ValueRef& value) {
		try {
			decodeRangeLockConfiguration(value);
			return false;
		} catch (Error& e) {
			return e.code() == error_code_range_lock_not_ready;
		}
	};
	auto encodeHeader = [&](FileIdentifier identifier, uint32_t revision, uint8_t state, int32_t cursorLength) {
		BinaryWriter writer(Unversioned());
		writer << identifier << revision << state << configuration.migrationId() << cursorLength;
		writer.serializeBytes(configuration.nextKey());
		return writer.toValue();
	};
	for (int length = 0; length < encoded.size(); ++length) {
		ASSERT(rejected(encoded.substr(0, length)));
	}
	ASSERT(rejected(encoded.withSuffix("trailing"_sr)));
	ASSERT(rejected(encodeHeader(0,
	                             RangeLockConfiguration::currentFormatRevision,
	                             static_cast<uint8_t>(RangeLockReadiness::Migrating),
	                             configuration.nextKey().size())));
	ASSERT(rejected(encodeHeader(RangeLockConfiguration::file_identifier,
	                             RangeLockConfiguration::currentFormatRevision + 1,
	                             static_cast<uint8_t>(RangeLockReadiness::Migrating),
	                             configuration.nextKey().size())));
	ASSERT(rejected(encodeHeader(RangeLockConfiguration::file_identifier,
	                             RangeLockConfiguration::currentFormatRevision,
	                             255,
	                             configuration.nextKey().size())));
	ASSERT(rejected(encodeHeader(RangeLockConfiguration::file_identifier,
	                             RangeLockConfiguration::currentFormatRevision,
	                             static_cast<uint8_t>(RangeLockReadiness::Migrating),
	                             -1)));
	co_return;
}

TEST_CASE("/RangeLock/ReconciliationEmptyKrm") {
	RangeResult unrelatedPredecessor;
	unrelatedPredecessor.push_back_deep(unrelatedPredecessor.arena(),
	                                    KeyValueRef(databaseLockedKey, "not a range-lock value"_sr));
	const RangeResult ranges = krmDecodeRanges(rangeLockPrefix, normalKeys, unrelatedPredecessor);
	ASSERT_EQ(ranges.size(), 2);
	ASSERT(ranges.front().key == normalKeys.begin);
	ASSERT(rangeLockReplayValue(ranges.front()).empty());
	ASSERT(ranges.back().key == normalKeys.end);
	ASSERT(!ranges.back().value.empty());
	ASSERT(rangeLockReplayValue(ranges.back()).empty());
	co_return;
}

TEST_CASE("/RangeLock/AdmissionStatusAggregation") {
	std::vector<RangeLockProxyStatusReply> replies(2);
	for (auto& reply : replies) {
		reply.admissionEnabled = true;
		reply.shardEncodeLocationMetadata = true;
		reply.enforcementStateValid = true;
	}
	auto rejected = [&](bool includeBulkLoadConfiguration) {
		try {
			aggregateRangeLockAdmissionStatus(replies, includeBulkLoadConfiguration);
			return false;
		} catch (Error& e) {
			return e.code() == error_code_range_lock_not_ready;
		}
	};
	const auto generic = aggregateRangeLockAdmissionStatus(replies, false);
	ASSERT(generic.allProxiesEnableAcquisition);
	ASSERT(generic.allProxiesEncodeShardLocations);
	ASSERT(generic.allProxiesHaveValidState);
	ASSERT(!generic.dataDistributorEncodesShardLocations.present());
	replies[1].enforcementStateValid = false;
	ASSERT(!aggregateRangeLockAdmissionStatus(replies, false).allProxiesHaveValidState);
	replies[1].enforcementStateValid = true;
	ASSERT(rejected(true));
	for (auto& reply : replies) {
		reply.dataDistributorId = UID(1, 2);
		reply.dataDistributorEncodesShardLocations = true;
	}
	ASSERT(aggregateRangeLockAdmissionStatus(replies, true).dataDistributorEncodesShardLocations.get());
	replies[1].dataDistributorEncodesShardLocations = false;
	ASSERT(!aggregateRangeLockAdmissionStatus(replies, true).dataDistributorEncodesShardLocations.get());
	replies[1].dataDistributorEncodesShardLocations.reset();
	ASSERT(rejected(true));
	replies[1].dataDistributorEncodesShardLocations = true;
	replies[1].dataDistributorId = UID(3, 4);
	ASSERT(rejected(true));
	ASSERT(!aggregateRangeLockAdmissionStatus(replies, false).dataDistributorEncodesShardLocations.present());
	co_return;
}
