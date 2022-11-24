/*
 * Tenant.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "fdbrpc/TenantInfo.h"
#include "flow/BooleanParam.h"
#include "flow/IRandom.h"
#include "libb64/decode.h"
#include "libb64/encode.h"
#include "flow/ApiVersion.h"
#include "flow/UnitTest.h"

namespace TenantAPI {
Key idToPrefix(int64_t id) {
	int64_t swapped = bigEndian64(id);
	return StringRef(reinterpret_cast<const uint8_t*>(&swapped), TenantAPI::PREFIX_SIZE);
}

int64_t prefixToId(KeyRef prefix, EnforceValidTenantId enforceValidTenantId) {
	ASSERT(prefix.size() == TenantAPI::PREFIX_SIZE);
	int64_t id = *reinterpret_cast<const int64_t*>(prefix.begin());
	id = bigEndian64(id);
	ASSERT(id >= 0);
	return id;
}
}; // namespace TenantAPI

std::string TenantAPI::TenantStateToString(TenantState tenantState) {
	switch (tenantState) {
	case TenantState::REGISTERING:
		return "registering";
	case TenantState::READY:
		return "ready";
	case TenantState::REMOVING:
		return "removing";
	case TenantState::UPDATING_CONFIGURATION:
		return "updating configuration";
	case TenantState::RENAMING:
		return "renaming";
	case TenantState::ERROR:
		return "error";
	default:
		UNREACHABLE();
	}
}

TenantAPI::TenantState TenantAPI::stringToTenantState(std::string stateStr) {
	std::transform(stateStr.begin(), stateStr.end(), stateStr.begin(), [](unsigned char c) { return std::tolower(c); });
	if (stateStr == "registering") {
		return TenantState::REGISTERING;
	} else if (stateStr == "ready") {
		return TenantState::READY;
	} else if (stateStr == "removing") {
		return TenantState::REMOVING;
	} else if (stateStr == "updating configuration") {
		return TenantState::UPDATING_CONFIGURATION;
	} else if (stateStr == "renaming") {
		return TenantState::RENAMING;
	} else if (stateStr == "error") {
		return TenantState::ERROR;
	}

	throw invalid_option();
}

std::string TenantAPI::tenantLockStateToString(TenantLockState tenantState) {
	switch (tenantState) {
	case TenantLockState::UNLOCKED:
		return "unlocked";
	case TenantLockState::READ_ONLY:
		return "read only";
	case TenantLockState::LOCKED:
		return "locked";
	default:
		UNREACHABLE();
	}
}

TenantAPI::TenantLockState TenantAPI::stringToTenantLockState(std::string stateStr) {
	std::transform(stateStr.begin(), stateStr.end(), stateStr.begin(), [](unsigned char c) { return std::tolower(c); });
	if (stateStr == "unlocked") {
		return TenantLockState::UNLOCKED;
	} else if (stateStr == "read only") {
		return TenantLockState::READ_ONLY;
	} else if (stateStr == "locked") {
		return TenantLockState::LOCKED;
	}

	UNREACHABLE();
}

MetaclusterTenantMapEntry::MetaclusterTenantMapEntry() {}
MetaclusterTenantMapEntry::MetaclusterTenantMapEntry(int64_t id, TenantName tenantName, TenantAPI::TenantState tenantState)
  : tenantName(tenantName), tenantState(tenantState) {
	setId(id);
}
MetaclusterTenantMapEntry::MetaclusterTenantMapEntry(int64_t id,
                                                     TenantName tenantName,
                                                     TenantAPI::TenantState tenantState,
                                                     Optional<TenantGroupName> tenantGroup)
  : tenantName(tenantName), tenantState(tenantState), tenantGroup(tenantGroup) {
	setId(id);
}

void MetaclusterTenantMapEntry::setId(int64_t id) {
	ASSERT(id >= 0);
	this->id = id;
	prefix = TenantAPI::idToPrefix(id);
}

json_spirit::mObject binaryToJson(StringRef bytes) {
	json_spirit::mObject obj;
	std::string encodedBytes = base64::encoder::from_string(bytes.toString());
	// Remove trailing newline
	encodedPrefix.resize(encodedBytes.size() - 1);

	obj["base64"] = encodedBytes;
	obj["printable"] = printable(bytes);

	return obj;
}

std::string MetaclusterTenantMapEntry::toJson() const {
	json_spirit::mObject tenantEntry;
	tenantEntry["id"] = id;

	tenantEntry["name"] = binaryToJson(tenantName);
	tenantEntry["prefix"] = binaryToJson(prefix);

	tenantEntry["tenant_state"] = TenantAPI::TenantStateToString(tenantState);
	tenantEntry["assigned_cluster"] = binaryToJson(assignedCluster);

	if (tenantGroup.present()) {
		tenantEntry["tenant_group"] = binaryToJson(tenantGroup.get());
	}

	tenantEntry["lock_state"] = TenantAPI::tenantLockStateToString(tenantLockState);
	if (tenantState == TenantAPI::TenantState::RENAMING) {
		ASSERT(renameDestination.present());
		tenantEntry["rename_destination"] = binaryToJson(renameDestination.get());
	} else if (tenantState == TenantAPI::TenantState::ERROR) {
		tenantEntry["error"] = error;
	}

	return json_spirit::write_string(json_spirit::mValue(tenantEntry));
}

bool MetaclusterTenantMapEntry::matchesConfiguration(MetaclusterTenantMapEntry const& other) const {
	return tenantName == other.tenantName && tenantLockState == other.tenantLockState &&
	       tenantGroup == other.tenantGroup && assignedCluster == other.assignedCluster;
}

void MetaclusterTenantMapEntry::configure(Standalone<StringRef> parameter, Optional<Value> value) {
	if (parameter == "tenant_group"_sr) {
		tenantGroup = value;
	} else if (parameter == "assigned_cluster"_sr && value.present()) {
		assignedCluster = value.get();
	} else {
		TraceEvent(SevWarnAlways, "UnknownTenantConfigurationParameter").detail("Parameter", parameter);
		throw invalid_tenant_configuration();
	}
}

TenantMapEntry::TenantMapEntry() {}
TenantMapEntry::TenantMapEntry(int64_t id, TenantName tenantName) : tenantName(tenantName) {
	setId(id);
}
TenantMapEntry::TenantMapEntry(int64_t id, TenantName tenantName, Optional<TenantGroupName> tenantGroup)
  : tenantName(tenantName), tenantGroup(tenantGroup) {
	setId(id);
}
TenantMapEntry::TenantMapEntry(MetaclusterTenantMapEntry metaclusterEntry)
  : tenantName(metaclusterEntry.tenantName), tenantLockState(metaclusterEntry.tenantLockState),
    tenantGroup(metaclusterEntry.tenantGroup), configurationSequenceNum(metaclusterEntry.configurationSequenceNum) {
	setId(id);
}

void TenantMapEntry::setId(int64_t id) {
	ASSERT(id >= 0);
	this->id = id;
	prefix = TenantAPI::idToPrefix(id);
}

std::string TenantMapEntry::toJson() const {
	json_spirit::mObject tenantEntry;
	tenantEntry["id"] = id;

	tenantEntry["name"] = binaryToJson(tenantName);
	tenantEntry["prefix"] = binaryToJson(prefix);

	if (tenantGroup.present()) {
		tenantEntry["tenant_group"] = binaryToJson(tenantGroup.get());
	}

	tenantEntry["lock_state"] = TenantAPI::tenantLockStateToString(tenantLockState);

	return json_spirit::write_string(json_spirit::mValue(tenantEntry));
}

bool TenantMapEntry::matchesConfiguration(TenantMapEntry const& other) const {
	return tenantName == other.tenantName && tenantGroup == other.tenantGroup &&
	       tenantLockState == other.tenantLockState;
}

void TenantMapEntry::configure(Standalone<StringRef> parameter, Optional<Value> value) {
	if (parameter == "tenant_group"_sr) {
		tenantGroup = value;
	} else {
		TraceEvent(SevWarnAlways, "UnknownTenantConfigurationParameter").detail("Parameter", parameter);
		throw invalid_tenant_configuration();
	}
}

TenantTxnStateStoreEntry::TenantTxnStateStoreEntry() {}
TenantTxnStateStoreEntry::TenantTxnStateStoreEntry(TenantName tenantName, TenantAPI::TenantLockState tenantLockState)
  : tenantName(tenantName), tenantLockState(tenantLockState) {}

json_spirit::mObject TenantGroupEntry::toJson() const {
	json_spirit::mObject tenantGroupEntry;
	if (assignedCluster.present()) {
		tenantGroupEntry["assigned_cluster"] = binaryToJson(assignedCluster.get());
	}

	return tenantGroupEntry;
}

TenantMetadataSpecification<TenantMapEntry>& TenantMetadata::instance() {
	static TenantMetadataSpecification<TenantMapEntry> _instance =
	    TenantMetadataSpecification<TenantMapEntry>("\xff/"_sr);
	return _instance;
}

Key TenantMetadata::tenantMapPrivatePrefix() {
	static Key _prefix = "\xff"_sr.withSuffix(tenantMap().subspace.begin);
	return _prefix;
}

TEST_CASE("/fdbclient/libb64/base64decoder") {
	Standalone<StringRef> buf = makeString(100);
	for (int i = 0; i < 1000; ++i) {
		int length = deterministicRandom()->randomInt(0, 100);
		deterministicRandom()->randomBytes(mutateString(buf), length);

		StringRef str = buf.substr(0, length);
		std::string encodedStr = base64::encoder::from_string(str.toString());
		// Remove trailing newline
		encodedStr.resize(encodedStr.size() - 1);

		std::string decodedStr = base64::decoder::from_string(encodedStr);
		ASSERT(decodedStr == str.toString());
	}

	return Void();
}

TEST_CASE("/fdbclient/TenantMapEntry/Serialization") {
	TenantMapEntry entry1(1, "tenant1"_sr);
	ASSERT(entry1.prefix == "\x00\x00\x00\x00\x00\x00\x00\x01"_sr);
	TenantMapEntry entry2 = TenantMapEntry::decode(entry1.encode());
	ASSERT(entry1.id == entry2.id && entry1.prefix == entry2.prefix);

	TenantMapEntry entry3(std::numeric_limits<int64_t>::max(), "tenant3"_sr);
	ASSERT(entry3.prefix == "\x7f\xff\xff\xff\xff\xff\xff\xff"_sr);
	TenantMapEntry entry4 = TenantMapEntry::decode(entry3.encode());
	ASSERT(entry3.id == entry4.id && entry3.prefix == entry4.prefix);

	for (int i = 0; i < 100; ++i) {
		int bits = deterministicRandom()->randomInt(1, 64);
		int64_t min = bits == 1 ? 0 : (UINT64_C(1) << (bits - 1));
		int64_t maxPlusOne = std::min<uint64_t>(UINT64_C(1) << bits, std::numeric_limits<int64_t>::max());
		int64_t id = deterministicRandom()->randomInt64(min, maxPlusOne);

		TenantMapEntry entry(id, "tenant"_sr);
		int64_t bigEndianId = bigEndian64(id);
		ASSERT(entry.id == id && entry.prefix == StringRef(reinterpret_cast<uint8_t*>(&bigEndianId), 8));

		TenantMapEntry decodedEntry = TenantMapEntry::decode(entry.encode());
		ASSERT(decodedEntry.id == entry.id && decodedEntry.prefix == entry.prefix);
	}

	return Void();
}
