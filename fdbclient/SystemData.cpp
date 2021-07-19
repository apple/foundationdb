/*
 * SystemData.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/SystemData.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/Arena.h"
#include "flow/TDMetric.actor.h"
#include "flow/serialize.h"
#include "flow/UnitTest.h"

const KeyRef systemKeysPrefix = "\xff"_sr;
const KeyRangeRef normalKeys(KeyRef(), systemKeysPrefix);
const KeyRangeRef systemKeys(systemKeysPrefix, "\xff\xff"_sr);
const KeyRangeRef nonMetadataSystemKeys("\xff\x02"_sr, "\xff\x03"_sr);
const KeyRangeRef allKeys = KeyRangeRef(normalKeys.begin, systemKeys.end);
const KeyRef afterAllKeys = "\xff\xff\x00"_sr;
const KeyRangeRef specialKeys = KeyRangeRef("\xff\xff"_sr, "\xff\xff\xff"_sr);

// keyServersKeys.contains(k) iff k.startsWith(keyServersPrefix)
const KeyRangeRef keyServersKeys("\xff/keyServers/"_sr, "\xff/keyServers0"_sr);
const KeyRef keyServersPrefix = keyServersKeys.begin;
const KeyRef keyServersEnd = keyServersKeys.end;
const KeyRangeRef keyServersKeyServersKeys("\xff/keyServers/\xff/keyServers/"_sr,
                                           "\xff/keyServers/\xff/keyServers0"_sr);
const KeyRef keyServersKeyServersKey = keyServersKeyServersKeys.begin;

const Key keyServersKey(const KeyRef& k) {
	return k.withPrefix(keyServersPrefix);
}
const KeyRef keyServersKey(const KeyRef& k, Arena& arena) {
	return k.withPrefix(keyServersPrefix, arena);
}
const Value keyServersValue(RangeResult result, const std::vector<UID>& src, const std::vector<UID>& dest) {
	if (!CLIENT_KNOBS->TAG_ENCODE_KEY_SERVERS) {
		BinaryWriter wr(IncludeVersion(ProtocolVersion::withKeyServerValue()));
		wr << src << dest;
		return wr.toValue();
	}

	std::vector<Tag> srcTag;
	std::vector<Tag> destTag;

	bool foundOldLocality = false;
	for (const KeyValueRef& kv : result) {
		UID uid = decodeServerTagKey(kv.key);
		if (std::find(src.begin(), src.end(), uid) != src.end()) {
			srcTag.push_back(decodeServerTagValue(kv.value));
			if (srcTag.back().locality == tagLocalityUpgraded) {
				foundOldLocality = true;
				break;
			}
		}
		if (std::find(dest.begin(), dest.end(), uid) != dest.end()) {
			destTag.push_back(decodeServerTagValue(kv.value));
			if (destTag.back().locality == tagLocalityUpgraded) {
				foundOldLocality = true;
				break;
			}
		}
	}

	if (foundOldLocality || src.size() != srcTag.size() || dest.size() != destTag.size()) {
		ASSERT_WE_THINK(foundOldLocality);
		BinaryWriter wr(IncludeVersion(ProtocolVersion::withKeyServerValue()));
		wr << src << dest;
		return wr.toValue();
	}

	return keyServersValue(srcTag, destTag);
}
const Value keyServersValue(const std::vector<Tag>& srcTag, const std::vector<Tag>& destTag) {
	// src and dest are expected to be sorted
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withKeyServerValueV2()));
	wr << srcTag << destTag;
	return wr.toValue();
}

void decodeKeyServersValue(RangeResult result,
                           const ValueRef& value,
                           std::vector<UID>& src,
                           std::vector<UID>& dest,
                           bool missingIsError) {
	if (value.size() == 0) {
		src.clear();
		dest.clear();
		return;
	}

	BinaryReader rd(value, IncludeVersion());
	if (!rd.protocolVersion().hasKeyServerValueV2()) {
		rd >> src >> dest;
		return;
	}

	std::vector<Tag> srcTag, destTag;
	rd >> srcTag >> destTag;

	src.clear();
	dest.clear();

	for (const KeyValueRef& kv : result) {
		Tag tag = decodeServerTagValue(kv.value);
		if (std::find(srcTag.begin(), srcTag.end(), tag) != srcTag.end()) {
			src.push_back(decodeServerTagKey(kv.key));
		}
		if (std::find(destTag.begin(), destTag.end(), tag) != destTag.end()) {
			dest.push_back(decodeServerTagKey(kv.key));
		}
	}
	std::sort(src.begin(), src.end());
	std::sort(dest.begin(), dest.end());
	if (missingIsError && (src.size() != srcTag.size() || dest.size() != destTag.size())) {
		TraceEvent(SevError, "AttemptedToDecodeMissingTag");
		for (const KeyValueRef& kv : result) {
			Tag tag = decodeServerTagValue(kv.value);
			UID serverID = decodeServerTagKey(kv.key);
			TraceEvent("TagUIDMap").detail("Tag", tag.toString()).detail("UID", serverID.toString());
		}
		for (auto& it : srcTag) {
			TraceEvent("SrcTag").detail("Tag", it.toString());
		}
		for (auto& it : destTag) {
			TraceEvent("DestTag").detail("Tag", it.toString());
		}
		ASSERT(false);
	}
}

void decodeKeyServersValue(std::map<Tag, UID> const& tag_uid,
                           const ValueRef& value,
                           std::vector<UID>& src,
                           std::vector<UID>& dest) {
	static std::vector<Tag> srcTag, destTag;
	src.clear();
	dest.clear();
	if (value.size() == 0) {
		return;
	}

	BinaryReader rd(value, IncludeVersion());
	rd.checkpoint();
	int srcLen, destLen;
	rd >> srcLen;
	rd.readBytes(srcLen * sizeof(Tag));
	rd >> destLen;
	rd.rewind();

	if (value.size() !=
	    sizeof(ProtocolVersion) + sizeof(int) + srcLen * sizeof(Tag) + sizeof(int) + destLen * sizeof(Tag)) {
		rd >> src >> dest;
		rd.assertEnd();
		return;
	}

	srcTag.clear();
	destTag.clear();
	rd >> srcTag >> destTag;

	for (auto t : srcTag) {
		auto itr = tag_uid.find(t);
		if (itr != tag_uid.end()) {
			src.push_back(itr->second);
		} else {
			TraceEvent(SevError, "AttemptedToDecodeMissingSrcTag").detail("Tag", t.toString());
			ASSERT(false);
		}
	}

	for (auto t : destTag) {
		auto itr = tag_uid.find(t);
		if (itr != tag_uid.end()) {
			dest.push_back(itr->second);
		} else {
			TraceEvent(SevError, "AttemptedToDecodeMissingDestTag").detail("Tag", t.toString());
			ASSERT(false);
		}
	}

	std::sort(src.begin(), src.end());
	std::sort(dest.begin(), dest.end());
}

const KeyRangeRef conflictingKeysRange =
    KeyRangeRef("\xff\xff/transaction/conflicting_keys/"_sr, "\xff\xff/transaction/conflicting_keys/\xff\xff"_sr);
const ValueRef conflictingKeysTrue = "1"_sr;
const ValueRef conflictingKeysFalse = "0"_sr;

const KeyRangeRef readConflictRangeKeysRange =
    KeyRangeRef("\xff\xff/transaction/read_conflict_range/"_sr, "\xff\xff/transaction/read_conflict_range/\xff\xff"_sr);

const KeyRangeRef writeConflictRangeKeysRange = KeyRangeRef("\xff\xff/transaction/write_conflict_range/"_sr,
                                                            "\xff\xff/transaction/write_conflict_range/\xff\xff"_sr);

// "\xff/cacheServer/[[UID]] := StorageServerInterface"
const KeyRangeRef storageCacheServerKeys("\xff/cacheServer/"_sr, "\xff/cacheServer0"_sr);
const KeyRef storageCacheServersPrefix = storageCacheServerKeys.begin;
const KeyRef storageCacheServersEnd = storageCacheServerKeys.end;

const Key storageCacheServerKey(UID id) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(storageCacheServersPrefix);
	wr << id;
	return wr.toValue();
}

const Value storageCacheServerValue(const StorageServerInterface& ssi) {
	BinaryWriter wr(IncludeVersion());
	wr << ssi;
	return wr.toValue();
}

const KeyRangeRef ddStatsRange =
    KeyRangeRef("\xff\xff/metrics/data_distribution_stats/"_sr, "\xff\xff/metrics/data_distribution_stats/\xff\xff"_sr);

//    "\xff/storageCache/[[begin]]" := "[[vector<uint16_t>]]"
const KeyRangeRef storageCacheKeys("\xff/storageCache/"_sr, "\xff/storageCache0"_sr);
const KeyRef storageCachePrefix = storageCacheKeys.begin;

const Key storageCacheKey(const KeyRef& k) {
	return k.withPrefix(storageCachePrefix);
}

const Value storageCacheValue(const vector<uint16_t>& serverIndices) {
	BinaryWriter wr((IncludeVersion(ProtocolVersion::withStorageCacheValue())));
	wr << serverIndices;
	return wr.toValue();
}

void decodeStorageCacheValue(const ValueRef& value, vector<uint16_t>& serverIndices) {
	serverIndices.clear();
	if (value.size()) {
		BinaryReader rd(value, IncludeVersion());
		rd >> serverIndices;
	}
}

const Value logsValue(const vector<std::pair<UID, NetworkAddress>>& logs,
                      const vector<std::pair<UID, NetworkAddress>>& oldLogs) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withLogsValue()));
	wr << logs;
	wr << oldLogs;
	return wr.toValue();
}
std::pair<vector<std::pair<UID, NetworkAddress>>, vector<std::pair<UID, NetworkAddress>>> decodeLogsValue(
    const ValueRef& value) {
	vector<std::pair<UID, NetworkAddress>> logs;
	vector<std::pair<UID, NetworkAddress>> oldLogs;
	BinaryReader reader(value, IncludeVersion());
	reader >> logs;
	reader >> oldLogs;
	return std::make_pair(logs, oldLogs);
}

const KeyRef serverKeysPrefix = "\xff/serverKeys/"_sr;
const ValueRef serverKeysTrue = "1"_sr, // compatible with what was serverKeysTrue
    serverKeysFalse;

const Key serverKeysKey(UID serverID, const KeyRef& key) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(serverKeysPrefix);
	wr << serverID;
	wr.serializeBytes("/"_sr);
	wr.serializeBytes(key);
	return wr.toValue();
}
const Key serverKeysPrefixFor(UID serverID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(serverKeysPrefix);
	wr << serverID;
	wr.serializeBytes("/"_sr);
	return wr.toValue();
}
UID serverKeysDecodeServer(const KeyRef& key) {
	UID server_id;
	BinaryReader rd(key.removePrefix(serverKeysPrefix), Unversioned());
	rd >> server_id;
	return server_id;
}
bool serverHasKey(ValueRef storedValue) {
	return storedValue == serverKeysTrue;
}

const KeyRef cacheKeysPrefix = "\xff\x02/cacheKeys/"_sr;

const Key cacheKeysKey(uint16_t idx, const KeyRef& key) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(cacheKeysPrefix);
	wr << idx;
	wr.serializeBytes("/"_sr);
	wr.serializeBytes(key);
	return wr.toValue();
}
const Key cacheKeysPrefixFor(uint16_t idx) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(cacheKeysPrefix);
	wr << idx;
	wr.serializeBytes("/"_sr);
	return wr.toValue();
}
uint16_t cacheKeysDecodeIndex(const KeyRef& key) {
	uint16_t idx;
	BinaryReader rd(key.removePrefix(cacheKeysPrefix), Unversioned());
	rd >> idx;
	return idx;
}
KeyRef cacheKeysDecodeKey(const KeyRef& key) {
	return key.substr(cacheKeysPrefix.size() + sizeof(uint16_t) + 1);
}

const KeyRef cacheChangeKey = "\xff\x02/cacheChangeKey"_sr;
const KeyRangeRef cacheChangeKeys("\xff\x02/cacheChangeKeys/"_sr, "\xff\x02/cacheChangeKeys0"_sr);
const KeyRef cacheChangePrefix = cacheChangeKeys.begin;
const Key cacheChangeKeyFor(uint16_t idx) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(cacheChangePrefix);
	wr << idx;
	return wr.toValue();
}
uint16_t cacheChangeKeyDecodeIndex(const KeyRef& key) {
	uint16_t idx;
	BinaryReader rd(key.removePrefix(cacheChangePrefix), Unversioned());
	rd >> idx;
	return idx;
}

const KeyRangeRef tssMappingKeys("\xff/tss/"_sr, "\xff/tss0"_sr);

const KeyRangeRef tssQuarantineKeys("\xff/tssQ/"_sr, "\xff/tssQ0"_sr);

const Key tssQuarantineKeyFor(UID serverID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(tssQuarantineKeys.begin);
	wr << serverID;
	return wr.toValue();
}

UID decodeTssQuarantineKey(KeyRef const& key) {
	UID serverID;
	BinaryReader rd(key.removePrefix(tssQuarantineKeys.begin), Unversioned());
	rd >> serverID;
	return serverID;
}

const KeyRangeRef tssMismatchKeys("\xff/tssMismatch/"_sr, "\xff/tssMismatch0"_sr);

const KeyRangeRef serverTagKeys("\xff/serverTag/"_sr, "\xff/serverTag0"_sr);

const KeyRef serverTagPrefix = serverTagKeys.begin;
const KeyRangeRef serverTagConflictKeys("\xff/serverTagConflict/"_sr, "\xff/serverTagConflict0"_sr);
const KeyRef serverTagConflictPrefix = serverTagConflictKeys.begin;
// serverTagHistoryKeys is the old tag a storage server uses before it is migrated to a different location.
// For example, we can copy a SS file to a remote DC and start the SS there;
//   The new SS will need to consume the last bits of data from the old tag it is responsible for.
const KeyRangeRef serverTagHistoryKeys("\xff/serverTagHistory/"_sr, "\xff/serverTagHistory0"_sr);
const KeyRef serverTagHistoryPrefix = serverTagHistoryKeys.begin;

const Key serverTagKeyFor(UID serverID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(serverTagKeys.begin);
	wr << serverID;
	return wr.toValue();
}

const Key serverTagHistoryKeyFor(UID serverID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(serverTagHistoryKeys.begin);
	wr << serverID;
	return addVersionStampAtEnd(wr.toValue());
}

const KeyRange serverTagHistoryRangeFor(UID serverID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(serverTagHistoryKeys.begin);
	wr << serverID;
	return prefixRange(wr.toValue());
}

const KeyRange serverTagHistoryRangeBefore(UID serverID, Version version) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(serverTagHistoryKeys.begin);
	wr << serverID;
	version = bigEndian64(version);

	Key versionStr = makeString(8);
	uint8_t* data = mutateString(versionStr);
	memcpy(data, &version, 8);

	return KeyRangeRef(wr.toValue(), versionStr.withPrefix(wr.toValue()));
}

const Value serverTagValue(Tag tag) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withServerTagValue()));
	wr << tag;
	return wr.toValue();
}

UID decodeServerTagKey(KeyRef const& key) {
	UID serverID;
	BinaryReader rd(key.removePrefix(serverTagKeys.begin), Unversioned());
	rd >> serverID;
	return serverID;
}

Version decodeServerTagHistoryKey(KeyRef const& key) {
	Version parsedVersion;
	memcpy(&parsedVersion, key.substr(key.size() - 10).begin(), sizeof(Version));
	parsedVersion = bigEndian64(parsedVersion);
	return parsedVersion;
}

Tag decodeServerTagValue(ValueRef const& value) {
	Tag s;
	BinaryReader reader(value, IncludeVersion());
	if (!reader.protocolVersion().hasTagLocality()) {
		int16_t id;
		reader >> id;
		if (id == invalidTagOld) {
			s = invalidTag;
		} else if (id == txsTagOld) {
			s = txsTag;
		} else {
			ASSERT(id >= 0);
			s.id = id;
			s.locality = tagLocalityUpgraded;
		}
	} else {
		reader >> s;
	}
	return s;
}

const Key serverTagConflictKeyFor(Tag tag) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(serverTagConflictKeys.begin);
	wr << tag;
	return wr.toValue();
}

const KeyRangeRef tagLocalityListKeys("\xff/tagLocalityList/"_sr, "\xff/tagLocalityList0"_sr);
const KeyRef tagLocalityListPrefix = tagLocalityListKeys.begin;

const Key tagLocalityListKeyFor(Optional<Value> dcID) {
	BinaryWriter wr(AssumeVersion(currentProtocolVersion));
	wr.serializeBytes(tagLocalityListKeys.begin);
	wr << dcID;
	return wr.toValue();
}

const Value tagLocalityListValue(int8_t const& tagLocality) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withTagLocalityListValue()));
	wr << tagLocality;
	return wr.toValue();
}
Optional<Value> decodeTagLocalityListKey(KeyRef const& key) {
	Optional<Value> dcID;
	BinaryReader rd(key.removePrefix(tagLocalityListKeys.begin), AssumeVersion(currentProtocolVersion));
	rd >> dcID;
	return dcID;
}
int8_t decodeTagLocalityListValue(ValueRef const& value) {
	int8_t s;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	return s;
}

const KeyRangeRef datacenterReplicasKeys("\xff\x02/datacenterReplicas/"_sr, "\xff\x02/datacenterReplicas0"_sr);
const KeyRef datacenterReplicasPrefix = datacenterReplicasKeys.begin;

const Key datacenterReplicasKeyFor(Optional<Value> dcID) {
	BinaryWriter wr(AssumeVersion(currentProtocolVersion));
	wr.serializeBytes(datacenterReplicasKeys.begin);
	wr << dcID;
	return wr.toValue();
}

const Value datacenterReplicasValue(int const& replicas) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withDatacenterReplicasValue()));
	wr << replicas;
	return wr.toValue();
}
Optional<Value> decodeDatacenterReplicasKey(KeyRef const& key) {
	Optional<Value> dcID;
	BinaryReader rd(key.removePrefix(datacenterReplicasKeys.begin), AssumeVersion(currentProtocolVersion));
	rd >> dcID;
	return dcID;
}
int decodeDatacenterReplicasValue(ValueRef const& value) {
	int s;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	return s;
}

//    "\xff\x02/tLogDatacenters/[[datacenterID]]"
extern const KeyRangeRef tLogDatacentersKeys;
extern const KeyRef tLogDatacentersPrefix;
const Key tLogDatacentersKeyFor(Optional<Value> dcID);

const KeyRangeRef tLogDatacentersKeys("\xff\x02/tLogDatacenters/"_sr, "\xff\x02/tLogDatacenters0"_sr);
const KeyRef tLogDatacentersPrefix = tLogDatacentersKeys.begin;

const Key tLogDatacentersKeyFor(Optional<Value> dcID) {
	BinaryWriter wr(AssumeVersion(currentProtocolVersion));
	wr.serializeBytes(tLogDatacentersKeys.begin);
	wr << dcID;
	return wr.toValue();
}
Optional<Value> decodeTLogDatacentersKey(KeyRef const& key) {
	Optional<Value> dcID;
	BinaryReader rd(key.removePrefix(tLogDatacentersKeys.begin), AssumeVersion(currentProtocolVersion));
	rd >> dcID;
	return dcID;
}

const KeyRef primaryDatacenterKey = "\xff/primaryDatacenter"_sr;

// serverListKeys.contains(k) iff k.startsWith( serverListKeys.begin ) because '/'+1 == '0'
const KeyRangeRef serverListKeys("\xff/serverList/"_sr, "\xff/serverList0"_sr);
const KeyRef serverListPrefix = serverListKeys.begin;

const Key serverListKeyFor(UID serverID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(serverListKeys.begin);
	wr << serverID;
	return wr.toValue();
}

// TODO use flatbuffers depending on version
const Value serverListValue(StorageServerInterface const& server) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withServerListValue()));
	wr << server;
	return wr.toValue();
}
UID decodeServerListKey(KeyRef const& key) {
	UID serverID;
	BinaryReader rd(key.removePrefix(serverListKeys.begin), Unversioned());
	rd >> serverID;
	return serverID;
}
StorageServerInterface decodeServerListValue(ValueRef const& value) {
	StorageServerInterface s;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	return s;
}

const Value serverListValueFB(StorageServerInterface const& server) {
	return ObjectWriter::toValue(server, IncludeVersion());
}

StorageServerInterface decodeServerListValueFB(ValueRef const& value) {
	StorageServerInterface s;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(s);
	return s;
}

// processClassKeys.contains(k) iff k.startsWith( processClassKeys.begin ) because '/'+1 == '0'
const KeyRangeRef processClassKeys("\xff/processClass/"_sr, "\xff/processClass0"_sr);
const KeyRef processClassPrefix = processClassKeys.begin;
const KeyRef processClassChangeKey = "\xff/processClassChanges"_sr;
const KeyRef processClassVersionKey = "\xff/processClassChangesVersion"_sr;
const ValueRef processClassVersionValue = "1"_sr;

const Key processClassKeyFor(StringRef processID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(processClassKeys.begin);
	wr << processID;
	return wr.toValue();
}

const Value processClassValue(ProcessClass const& processClass) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withProcessClassValue()));
	wr << processClass;
	return wr.toValue();
}

Key decodeProcessClassKey(KeyRef const& key) {
	StringRef processID;
	BinaryReader rd(key.removePrefix(processClassKeys.begin), Unversioned());
	rd >> processID;
	return processID;
}

UID decodeProcessClassKeyOld(KeyRef const& key) {
	UID processID;
	BinaryReader rd(key.removePrefix(processClassKeys.begin), Unversioned());
	rd >> processID;
	return processID;
}

ProcessClass decodeProcessClassValue(ValueRef const& value) {
	ProcessClass s;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	return s;
}

const KeyRangeRef configKeys("\xff/conf/"_sr, "\xff/conf0"_sr);
const KeyRef configKeysPrefix = configKeys.begin;

const KeyRef perpetualStorageWiggleKey("\xff/conf/perpetual_storage_wiggle"_sr);
const KeyRef wigglingStorageServerKey("\xff/storageWigglePID"_sr);

const KeyRef triggerDDTeamInfoPrintKey("\xff/triggerDDTeamInfoPrint"_sr);

const KeyRangeRef excludedServersKeys("\xff/conf/excluded/"_sr, "\xff/conf/excluded0"_sr);
const KeyRef excludedServersPrefix = excludedServersKeys.begin;
const KeyRef excludedServersVersionKey = "\xff/conf/excluded"_sr;
AddressExclusion decodeExcludedServersKey(KeyRef const& key) {
	ASSERT(key.startsWith(excludedServersPrefix));
	// Returns an invalid NetworkAddress if given an invalid key (within the prefix)
	// Excluded servers have IP in x.x.x.x format, port optional, and no SSL suffix
	// Returns a valid, public NetworkAddress with a port of 0 if the key represents an IP address alone (meaning all
	// ports) Returns a valid, public NetworkAddress with nonzero port if the key represents an IP:PORT combination

	return AddressExclusion::parse(key.removePrefix(excludedServersPrefix));
}
std::string encodeExcludedServersKey(AddressExclusion const& addr) {
	// FIXME: make sure what's persisted here is not affected by innocent changes elsewhere
	return excludedServersPrefix.toString() + addr.toString();
}

const KeyRangeRef excludedLocalityKeys("\xff/conf/excluded_locality/"_sr, "\xff/conf/excluded_locality0"_sr);
const KeyRef excludedLocalityPrefix = excludedLocalityKeys.begin;
const KeyRef excludedLocalityVersionKey = "\xff/conf/excluded_locality"_sr;
std::string decodeExcludedLocalityKey(KeyRef const& key) {
	ASSERT(key.startsWith(excludedLocalityPrefix));
	return key.removePrefix(excludedLocalityPrefix).toString();
}
std::string encodeExcludedLocalityKey(std::string const& locality) {
	return excludedLocalityPrefix.toString() + locality;
}

const KeyRangeRef failedServersKeys("\xff/conf/failed/"_sr, "\xff/conf/failed0"_sr);
const KeyRef failedServersPrefix = failedServersKeys.begin;
const KeyRef failedServersVersionKey = "\xff/conf/failed"_sr;
AddressExclusion decodeFailedServersKey(KeyRef const& key) {
	ASSERT(key.startsWith(failedServersPrefix));
	// Returns an invalid NetworkAddress if given an invalid key (within the prefix)
	// Excluded servers have IP in x.x.x.x format, port optional, and no SSL suffix
	// Returns a valid, public NetworkAddress with a port of 0 if the key represents an IP address alone (meaning all
	// ports) Returns a valid, public NetworkAddress with nonzero port if the key represents an IP:PORT combination

	return AddressExclusion::parse(key.removePrefix(failedServersPrefix));
}
std::string encodeFailedServersKey(AddressExclusion const& addr) {
	// FIXME: make sure what's persisted here is not affected by innocent changes elsewhere
	return failedServersPrefix.toString() + addr.toString();
}

const KeyRangeRef failedLocalityKeys("\xff/conf/failed_locality/"_sr, "\xff/conf/failed_locality0"_sr);
const KeyRef failedLocalityPrefix = failedLocalityKeys.begin;
const KeyRef failedLocalityVersionKey = "\xff/conf/failed_locality"_sr;
std::string decodeFailedLocalityKey(KeyRef const& key) {
	ASSERT(key.startsWith(failedLocalityPrefix));
	return key.removePrefix(failedLocalityPrefix).toString();
}
std::string encodeFailedLocalityKey(std::string const& locality) {
	return failedLocalityPrefix.toString() + locality;
}

// const KeyRangeRef globalConfigKeys( "\xff/globalConfig/"_sr, "\xff/globalConfig0"_sr );
// const KeyRef globalConfigPrefix = globalConfigKeys.begin;

const KeyRangeRef globalConfigDataKeys("\xff/globalConfig/k/"_sr, "\xff/globalConfig/k0"_sr);
const KeyRef globalConfigKeysPrefix = globalConfigDataKeys.begin;

const KeyRangeRef globalConfigHistoryKeys("\xff/globalConfig/h/"_sr, "\xff/globalConfig/h0"_sr);
const KeyRef globalConfigHistoryPrefix = globalConfigHistoryKeys.begin;

const KeyRef globalConfigVersionKey = "\xff/globalConfig/v"_sr;

const KeyRangeRef workerListKeys("\xff/worker/"_sr, "\xff/worker0"_sr);
const KeyRef workerListPrefix = workerListKeys.begin;

const Key workerListKeyFor(StringRef processID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(workerListKeys.begin);
	wr << processID;
	return wr.toValue();
}

const Value workerListValue(ProcessData const& processData) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withWorkerListValue()));
	wr << processData;
	return wr.toValue();
}

Key decodeWorkerListKey(KeyRef const& key) {
	StringRef processID;
	BinaryReader rd(key.removePrefix(workerListKeys.begin), Unversioned());
	rd >> processID;
	return processID;
}

ProcessData decodeWorkerListValue(ValueRef const& value) {
	ProcessData s;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	return s;
}

const KeyRangeRef backupProgressKeys("\xff\x02/backupProgress/"_sr, "\xff\x02/backupProgress0"_sr);
const KeyRef backupProgressPrefix = backupProgressKeys.begin;
const KeyRef backupStartedKey = "\xff\x02/backupStarted"_sr;
extern const KeyRef backupPausedKey = "\xff\x02/backupPaused"_sr;

const Key backupProgressKeyFor(UID workerID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(backupProgressPrefix);
	wr << workerID;
	return wr.toValue();
}

const Value backupProgressValue(const WorkerBackupStatus& status) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBackupProgressValue()));
	wr << status;
	return wr.toValue();
}

UID decodeBackupProgressKey(const KeyRef& key) {
	UID serverID;
	BinaryReader rd(key.removePrefix(backupProgressPrefix), Unversioned());
	rd >> serverID;
	return serverID;
}

WorkerBackupStatus decodeBackupProgressValue(const ValueRef& value) {
	WorkerBackupStatus status;
	BinaryReader reader(value, IncludeVersion());
	reader >> status;
	return status;
}

Value encodeBackupStartedValue(const std::vector<std::pair<UID, Version>>& ids) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBackupStartValue()));
	wr << ids;
	return wr.toValue();
}

std::vector<std::pair<UID, Version>> decodeBackupStartedValue(const ValueRef& value) {
	std::vector<std::pair<UID, Version>> ids;
	BinaryReader reader(value, IncludeVersion());
	if (value.size() > 0)
		reader >> ids;
	return ids;
}

const KeyRef coordinatorsKey = "\xff/coordinators"_sr;
const KeyRef logsKey = "\xff/logs"_sr;
const KeyRef minRequiredCommitVersionKey = "\xff/minRequiredCommitVersion"_sr;

const KeyRef globalKeysPrefix = "\xff/globals"_sr;
const KeyRef lastEpochEndKey = "\xff/globals/lastEpochEnd"_sr;
const KeyRef lastEpochEndPrivateKey = "\xff\xff/globals/lastEpochEnd"_sr;
const KeyRef killStorageKey = "\xff/globals/killStorage"_sr;
const KeyRef killStoragePrivateKey = "\xff\xff/globals/killStorage"_sr;
const KeyRef rebootWhenDurableKey = "\xff/globals/rebootWhenDurable"_sr;
const KeyRef rebootWhenDurablePrivateKey = "\xff\xff/globals/rebootWhenDurable"_sr;
const KeyRef primaryLocalityKey = "\xff/globals/primaryLocality"_sr;
const KeyRef primaryLocalityPrivateKey = "\xff\xff/globals/primaryLocality"_sr;
const KeyRef fastLoggingEnabled = "\xff/globals/fastLoggingEnabled"_sr;
const KeyRef fastLoggingEnabledPrivateKey = "\xff\xff/globals/fastLoggingEnabled"_sr;

// Whenever configuration changes or DD related system keyspace is changed(e.g.., serverList),
// actor must grab the moveKeysLockOwnerKey and update moveKeysLockWriteKey.
// This prevents concurrent write to the same system keyspace.
// When the owner of the DD related system keyspace changes, DD will reboot
const KeyRef moveKeysLockOwnerKey = "\xff/moveKeysLock/Owner"_sr;
const KeyRef moveKeysLockWriteKey = "\xff/moveKeysLock/Write"_sr;

const KeyRef dataDistributionModeKey = "\xff/dataDistributionMode"_sr;
const UID dataDistributionModeLock = UID(6345, 3425);

// Keys to view and control tag throttling
const KeyRangeRef tagThrottleKeys = KeyRangeRef("\xff\x02/throttledTags/tag/"_sr, "\xff\x02/throttledTags/tag0"_sr);
const KeyRef tagThrottleKeysPrefix = tagThrottleKeys.begin;
const KeyRef tagThrottleAutoKeysPrefix = "\xff\x02/throttledTags/tag/\x01"_sr;
const KeyRef tagThrottleSignalKey = "\xff\x02/throttledTags/signal"_sr;
const KeyRef tagThrottleAutoEnabledKey = "\xff\x02/throttledTags/autoThrottlingEnabled"_sr;
const KeyRef tagThrottleLimitKey = "\xff\x02/throttledTags/manualThrottleLimit"_sr;
const KeyRef tagThrottleCountKey = "\xff\x02/throttledTags/manualThrottleCount"_sr;

// Client status info prefix
const KeyRangeRef fdbClientInfoPrefixRange("\xff\x02/fdbClientInfo/"_sr, "\xff\x02/fdbClientInfo0"_sr);
// See remaining fields in GlobalConfig.actor.h

// ConsistencyCheck settings
const KeyRef fdbShouldConsistencyCheckBeSuspended = "\xff\x02/ConsistencyCheck/Suspend"_sr;

// Request latency measurement key
const KeyRef latencyBandConfigKey = "\xff\x02/latencyBandConfig"_sr;

// Keyspace to maintain wall clock to version map
const KeyRangeRef timeKeeperPrefixRange("\xff\x02/timeKeeper/map/"_sr, "\xff\x02/timeKeeper/map0"_sr);
const KeyRef timeKeeperVersionKey = "\xff\x02/timeKeeper/version"_sr;
const KeyRef timeKeeperDisableKey = "\xff\x02/timeKeeper/disable"_sr;

// Backup Log Mutation constant variables
const KeyRef backupEnabledKey = "\xff/backupEnabled"_sr;
const KeyRangeRef backupLogKeys("\xff\x02/blog/"_sr, "\xff\x02/blog0"_sr);
const KeyRangeRef applyLogKeys("\xff\x02/alog/"_sr, "\xff\x02/alog0"_sr);
// static_assert( backupLogKeys.begin.size() == backupLogPrefixBytes, "backupLogPrefixBytes incorrect" );
const KeyRef backupVersionKey = "\xff/backupDataFormat"_sr;
const ValueRef backupVersionValue = "4"_sr;
const int backupVersion = 4;

// Log Range constant variables
// \xff/logRanges/[16-byte UID][begin key] := serialize( make_pair([end key], [destination key prefix]),
// IncludeVersion() )
const KeyRangeRef logRangesRange("\xff/logRanges/"_sr, "\xff/logRanges0"_sr);

// Layer status metadata prefix
const KeyRangeRef layerStatusMetaPrefixRange("\xff\x02/status/"_sr, "\xff\x02/status0"_sr);

// Backup agent status root
const KeyRangeRef backupStatusPrefixRange("\xff\x02/backupstatus/"_sr, "\xff\x02/backupstatus0"_sr);

// Restore configuration constant variables
const KeyRangeRef fileRestorePrefixRange("\xff\x02/restore-agent/"_sr, "\xff\x02/restore-agent0"_sr);

// Backup Agent configuration constant variables
const KeyRangeRef fileBackupPrefixRange("\xff\x02/backup-agent/"_sr, "\xff\x02/backup-agent0"_sr);

// DR Agent configuration constant variables
const KeyRangeRef databaseBackupPrefixRange("\xff\x02/db-backup-agent/"_sr, "\xff\x02/db-backup-agent0"_sr);

// \xff\x02/sharedLogRangesConfig/destUidLookup/[keyRange]
const KeyRef destUidLookupPrefix = "\xff\x02/sharedLogRangesConfig/destUidLookup/"_sr;
// \xff\x02/sharedLogRangesConfig/backuplatestVersions/[destUid]/[logUid]
const KeyRef backupLatestVersionsPrefix = "\xff\x02/sharedLogRangesConfig/backupLatestVersions/"_sr;

// Returns the encoded key comprised of begin key and log uid
Key logRangesEncodeKey(KeyRef keyBegin, UID logUid) {
	return keyBegin.withPrefix(uidPrefixKey(logRangesRange.begin, logUid));
}

// Returns the start key and optionally the logRange Uid
KeyRef logRangesDecodeKey(KeyRef key, UID* logUid) {
	if (key.size() < logRangesRange.begin.size() + sizeof(UID)) {
		TraceEvent(SevError, "InvalidDecodeKey").detail("Key", key);
		ASSERT(false);
	}

	if (logUid) {
		*logUid = BinaryReader::fromStringRef<UID>(key.removePrefix(logRangesRange.begin), Unversioned());
	}

	return key.substr(logRangesRange.begin.size() + sizeof(UID));
}

// Returns the encoded key value comprised of the end key and destination path
Key logRangesEncodeValue(KeyRef keyEnd, KeyRef destPath) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withLogRangeEncodeValue()));
	wr << std::make_pair(keyEnd, destPath);
	return wr.toValue();
}

// \xff/logRanges/[16-byte UID][begin key] := serialize( make_pair([end key], [destination key prefix]),
// IncludeVersion() )
Key logRangesDecodeValue(KeyRef keyValue, Key* destKeyPrefix) {
	std::pair<KeyRef, KeyRef> endPrefixCombo;

	BinaryReader rd(keyValue, IncludeVersion());
	rd >> endPrefixCombo;

	if (destKeyPrefix) {
		*destKeyPrefix = endPrefixCombo.second;
	}

	return endPrefixCombo.first;
}

// Returns a key prefixed with the specified key with
// the uid encoded at the end
Key uidPrefixKey(KeyRef keyPrefix, UID logUid) {
	BinaryWriter bw(Unversioned());
	bw.serializeBytes(keyPrefix);
	bw << logUid;
	return bw.toValue();
}

// Apply mutations constant variables
// \xff/applyMutationsEnd/[16-byte UID] := serialize( endVersion, Unversioned() )
// This indicates what is the highest version the mutation log can be applied
const KeyRangeRef applyMutationsEndRange("\xff/applyMutationsEnd/"_sr, "\xff/applyMutationsEnd0"_sr);

// \xff/applyMutationsBegin/[16-byte UID] := serialize( beginVersion, Unversioned() )
const KeyRangeRef applyMutationsBeginRange("\xff/applyMutationsBegin/"_sr, "\xff/applyMutationsBegin0"_sr);

// \xff/applyMutationsAddPrefix/[16-byte UID] := addPrefix
const KeyRangeRef applyMutationsAddPrefixRange("\xff/applyMutationsAddPrefix/"_sr, "\xff/applyMutationsAddPrefix0"_sr);

// \xff/applyMutationsRemovePrefix/[16-byte UID] := removePrefix
const KeyRangeRef applyMutationsRemovePrefixRange("\xff/applyMutationsRemovePrefix/"_sr,
                                                  "\xff/applyMutationsRemovePrefix0"_sr);

const KeyRangeRef applyMutationsKeyVersionMapRange("\xff/applyMutationsKeyVersionMap/"_sr,
                                                   "\xff/applyMutationsKeyVersionMap0"_sr);
const KeyRangeRef applyMutationsKeyVersionCountRange("\xff\x02/applyMutationsKeyVersionCount/"_sr,
                                                     "\xff\x02/applyMutationsKeyVersionCount0"_sr);

const KeyRef systemTuplesPrefix = "\xff/a/"_sr;
const KeyRef metricConfChangeKey = "\x01TDMetricConfChanges\x00"_sr;

const KeyRangeRef metricConfKeys("\x01TDMetricConf\x00\x01"_sr, "\x01TDMetricConf\x00\x02"_sr);
const KeyRef metricConfPrefix = metricConfKeys.begin;

/*
const Key metricConfKey( KeyRef const& prefix, MetricNameRef const& name, KeyRef const& key  ) {
    BinaryWriter wr(Unversioned());
    wr.serializeBytes( prefix );
    wr.serializeBytes( metricConfPrefix );
    wr.serializeBytes( name.type );
    wr.serializeBytes( "\x00\x01"_sr );
    wr.serializeBytes( name.name );
    wr.serializeBytes( "\x00\x01"_sr );
    wr.serializeBytes( name.address );
    wr.serializeBytes( "\x00\x01"_sr );
    wr.serializeBytes( name.id );
    wr.serializeBytes( "\x00\x01"_sr );
    wr.serializeBytes( key );
    wr.serializeBytes( "\x00"_sr );
    return wr.toValue();
}

std::pair<MetricNameRef, KeyRef> decodeMetricConfKey( KeyRef const& prefix, KeyRef const& key ) {
    MetricNameRef result;
    KeyRef withoutPrefix = key.removePrefix( prefix );
    withoutPrefix = withoutPrefix.removePrefix( metricConfPrefix );
    int pos = std::find(withoutPrefix.begin(), withoutPrefix.end(), '\x00') - withoutPrefix.begin();
    result.type = withoutPrefix.substr(0,pos);
    withoutPrefix = withoutPrefix.substr(pos+2);
    pos = std::find(withoutPrefix.begin(), withoutPrefix.end(), '\x00') - withoutPrefix.begin();
    result.name = withoutPrefix.substr(0,pos);
    withoutPrefix = withoutPrefix.substr(pos+2);
    pos = std::find(withoutPrefix.begin(), withoutPrefix.end(), '\x00') - withoutPrefix.begin();
    result.address = withoutPrefix.substr(0,pos);
    withoutPrefix = withoutPrefix.substr(pos+2);
    pos = std::find(withoutPrefix.begin(), withoutPrefix.end(), '\x00') - withoutPrefix.begin();
    result.id = withoutPrefix.substr(0,pos);
    return std::make_pair( result, withoutPrefix.substr(pos+2,withoutPrefix.size()-pos-3) );
}
*/

const KeyRef maxUIDKey = "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"_sr;

const KeyRef databaseLockedKey = "\xff/dbLocked"_sr;
const KeyRef databaseLockedKeyEnd = "\xff/dbLocked\x00"_sr;
const KeyRef metadataVersionKey = "\xff/metadataVersion"_sr;
const KeyRef metadataVersionKeyEnd = "\xff/metadataVersion\x00"_sr;
const KeyRef metadataVersionRequiredValue = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"_sr;
const KeyRef mustContainSystemMutationsKey = "\xff/mustContainSystemMutations"_sr;

const KeyRangeRef monitorConfKeys("\xff\x02/monitorConf/"_sr, "\xff\x02/monitorConf0"_sr);

const KeyRef restoreRequestDoneKey = "\xff\x02/restoreRequestDone"_sr;

const KeyRef healthyZoneKey = "\xff\x02/healthyZone"_sr;
const StringRef ignoreSSFailuresZoneString = "IgnoreSSFailures"_sr;
const KeyRef rebalanceDDIgnoreKey = "\xff\x02/rebalanceDDIgnored"_sr;

const Value healthyZoneValue(StringRef const& zoneId, Version version) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withHealthyZoneValue()));
	wr << zoneId;
	wr << version;
	return wr.toValue();
}
std::pair<Key, Version> decodeHealthyZoneValue(ValueRef const& value) {
	Key zoneId;
	Version version;
	BinaryReader reader(value, IncludeVersion());
	reader >> zoneId;
	reader >> version;
	return std::make_pair(zoneId, version);
}

const KeyRangeRef testOnlyTxnStateStorePrefixRange("\xff/TESTONLYtxnStateStore/"_sr, "\xff/TESTONLYtxnStateStore0"_sr);

const KeyRef writeRecoveryKey = "\xff/writeRecovery"_sr;
const ValueRef writeRecoveryKeyTrue = "1"_sr;
const KeyRef snapshotEndVersionKey = "\xff/snapshotEndVersion"_sr;

const KeyRef configTransactionDescriptionKey = "\xff\xff/description"_sr;
const KeyRange globalConfigKnobKeys = singleKeyRange("\xff\xff/globalKnobs"_sr);
const KeyRangeRef configKnobKeys("\xff\xff/knobs/"_sr, "\xff\xff/knobs0"_sr);
const KeyRangeRef configClassKeys("\xff\xff/configClasses/"_sr, "\xff\xff/configClasses0"_sr);

// for tests
void testSSISerdes(StorageServerInterface const& ssi, bool useFB) {
	printf("ssi=\nid=%s\nlocality=%s\nisTss=%s\ntssId=%s\naddress=%s\ngetValue=%s\n\n\n",
	       ssi.id().toString().c_str(),
	       ssi.locality.toString().c_str(),
	       ssi.isTss() ? "true" : "false",
	       ssi.isTss() ? ssi.tssPairID.get().toString().c_str() : "",
	       ssi.address().toString().c_str(),
	       ssi.getValue.getEndpoint().token.toString().c_str());

	StorageServerInterface ssi2 =
	    (useFB) ? decodeServerListValueFB(serverListValueFB(ssi)) : decodeServerListValue(serverListValue(ssi));

	printf("ssi2=\nid=%s\nlocality=%s\nisTss=%s\ntssId=%s\naddress=%s\ngetValue=%s\n\n\n",
	       ssi2.id().toString().c_str(),
	       ssi2.locality.toString().c_str(),
	       ssi2.isTss() ? "true" : "false",
	       ssi2.isTss() ? ssi2.tssPairID.get().toString().c_str() : "",
	       ssi2.address().toString().c_str(),
	       ssi2.getValue.getEndpoint().token.toString().c_str());

	ASSERT(ssi.id() == ssi2.id());
	ASSERT(ssi.locality == ssi2.locality);
	ASSERT(ssi.isTss() == ssi2.isTss());
	if (ssi.isTss()) {
		ASSERT(ssi2.tssPairID.get() == ssi2.tssPairID.get());
	}
	ASSERT(ssi.address() == ssi2.address());
	ASSERT(ssi.getValue.getEndpoint().token == ssi2.getValue.getEndpoint().token);
}

// unit test for serialization since tss stuff had bugs
TEST_CASE("/SystemData/SerDes/SSI") {
	printf("testing ssi serdes\n");
	LocalityData localityData(Optional<Standalone<StringRef>>(),
	                          Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
	                          Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
	                          Optional<Standalone<StringRef>>());

	// non-tss
	StorageServerInterface ssi;
	ssi.uniqueID = UID(0x1234123412341234, 0x5678567856785678);
	ssi.locality = localityData;
	ssi.initEndpoints();

	testSSISerdes(ssi, false);
	testSSISerdes(ssi, true);

	ssi.tssPairID = UID(0x2345234523452345, 0x1238123812381238);

	testSSISerdes(ssi, false);
	testSSISerdes(ssi, true);
	printf("ssi serdes test complete\n");

	return Void();
}
