/*
 * SystemData.cpp
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

#include "fdbclient/SystemData.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/Arena.h"
#include "flow/TDMetric.actor.h"
#include "flow/serialize.h"
#include "flow/UnitTest.h"

const KeyRef systemKeysPrefix = LiteralStringRef("\xff");
const KeyRangeRef normalKeys(KeyRef(), systemKeysPrefix);
const KeyRangeRef systemKeys(systemKeysPrefix, LiteralStringRef("\xff\xff"));
const KeyRangeRef nonMetadataSystemKeys(LiteralStringRef("\xff\x02"), LiteralStringRef("\xff\x03"));
const KeyRangeRef allKeys = KeyRangeRef(normalKeys.begin, systemKeys.end);
const KeyRef afterAllKeys = LiteralStringRef("\xff\xff\x00");
const KeyRangeRef specialKeys = KeyRangeRef(LiteralStringRef("\xff\xff"), LiteralStringRef("\xff\xff\xff"));

// keyServersKeys.contains(k) iff k.startsWith(keyServersPrefix)
const KeyRangeRef keyServersKeys(LiteralStringRef("\xff/keyServers/"), LiteralStringRef("\xff/keyServers0"));
const KeyRef keyServersPrefix = keyServersKeys.begin;
const KeyRef keyServersEnd = keyServersKeys.end;
const KeyRangeRef keyServersKeyServersKeys(LiteralStringRef("\xff/keyServers/\xff/keyServers/"),
                                           LiteralStringRef("\xff/keyServers/\xff/keyServers0"));
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
		TraceEvent(SevError, "AttemptedToDecodeMissingTag").log();
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
    KeyRangeRef(LiteralStringRef("\xff\xff/transaction/conflicting_keys/"),
                LiteralStringRef("\xff\xff/transaction/conflicting_keys/\xff\xff"));
const ValueRef conflictingKeysTrue = LiteralStringRef("1");
const ValueRef conflictingKeysFalse = LiteralStringRef("0");

const KeyRangeRef readConflictRangeKeysRange =
    KeyRangeRef(LiteralStringRef("\xff\xff/transaction/read_conflict_range/"),
                LiteralStringRef("\xff\xff/transaction/read_conflict_range/\xff\xff"));

const KeyRangeRef writeConflictRangeKeysRange =
    KeyRangeRef(LiteralStringRef("\xff\xff/transaction/write_conflict_range/"),
                LiteralStringRef("\xff\xff/transaction/write_conflict_range/\xff\xff"));

const KeyRef clusterIdKey = LiteralStringRef("\xff/clusterId");

const KeyRef checkpointPrefix = "\xff/checkpoint/"_sr;

const Key checkpointKeyFor(UID checkpointID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(checkpointPrefix);
	wr << checkpointID;
	return wr.toValue();
}

const Value checkpointValue(const CheckpointMetaData& checkpoint) {
	return ObjectWriter::toValue(checkpoint, IncludeVersion());
}

UID decodeCheckpointKey(const KeyRef& key) {
	UID checkpointID;
	BinaryReader rd(key.removePrefix(checkpointPrefix), Unversioned());
	rd >> checkpointID;
	return checkpointID;
}

CheckpointMetaData decodeCheckpointValue(const ValueRef& value) {
	CheckpointMetaData checkpoint;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(checkpoint);
	return checkpoint;
}

// "\xff/cacheServer/[[UID]] := StorageServerInterface"
const KeyRangeRef storageCacheServerKeys(LiteralStringRef("\xff/cacheServer/"), LiteralStringRef("\xff/cacheServer0"));
const KeyRef storageCacheServersPrefix = storageCacheServerKeys.begin;
const KeyRef storageCacheServersEnd = storageCacheServerKeys.end;

const Key storageCacheServerKey(UID id) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(storageCacheServersPrefix);
	wr << id;
	return wr.toValue();
}

const Value storageCacheServerValue(const StorageServerInterface& ssi) {
	auto protocolVersion = currentProtocolVersion;
	protocolVersion.addObjectSerializerFlag();
	return ObjectWriter::toValue(ssi, IncludeVersion(protocolVersion));
}

const KeyRangeRef ddStatsRange = KeyRangeRef(LiteralStringRef("\xff\xff/metrics/data_distribution_stats/"),
                                             LiteralStringRef("\xff\xff/metrics/data_distribution_stats/\xff\xff"));

//    "\xff/storageCache/[[begin]]" := "[[vector<uint16_t>]]"
const KeyRangeRef storageCacheKeys(LiteralStringRef("\xff/storageCache/"), LiteralStringRef("\xff/storageCache0"));
const KeyRef storageCachePrefix = storageCacheKeys.begin;

const Key storageCacheKey(const KeyRef& k) {
	return k.withPrefix(storageCachePrefix);
}

const Value storageCacheValue(const std::vector<uint16_t>& serverIndices) {
	BinaryWriter wr((IncludeVersion(ProtocolVersion::withStorageCacheValue())));
	wr << serverIndices;
	return wr.toValue();
}

void decodeStorageCacheValue(const ValueRef& value, std::vector<uint16_t>& serverIndices) {
	serverIndices.clear();
	if (value.size()) {
		BinaryReader rd(value, IncludeVersion());
		rd >> serverIndices;
	}
}

const Value logsValue(const std::vector<std::pair<UID, NetworkAddress>>& logs,
                      const std::vector<std::pair<UID, NetworkAddress>>& oldLogs) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withLogsValue()));
	wr << logs;
	wr << oldLogs;
	return wr.toValue();
}
std::pair<std::vector<std::pair<UID, NetworkAddress>>, std::vector<std::pair<UID, NetworkAddress>>> decodeLogsValue(
    const ValueRef& value) {
	std::vector<std::pair<UID, NetworkAddress>> logs;
	std::vector<std::pair<UID, NetworkAddress>> oldLogs;
	BinaryReader reader(value, IncludeVersion());
	reader >> logs;
	reader >> oldLogs;
	return std::make_pair(logs, oldLogs);
}

const KeyRef serverKeysPrefix = "\xff/serverKeys/"_sr;
const ValueRef serverKeysTrue = "1"_sr, // compatible with what was serverKeysTrue
    serverKeysTrueEmptyRange = "3"_sr, // the server treats the range as empty.
    serverKeysFalse;

const Key serverKeysKey(UID serverID, const KeyRef& key) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(serverKeysPrefix);
	wr << serverID;
	wr.serializeBytes(LiteralStringRef("/"));
	wr.serializeBytes(key);
	return wr.toValue();
}
const Key serverKeysPrefixFor(UID serverID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(serverKeysPrefix);
	wr << serverID;
	wr.serializeBytes(LiteralStringRef("/"));
	return wr.toValue();
}
UID serverKeysDecodeServer(const KeyRef& key) {
	UID server_id;
	BinaryReader rd(key.removePrefix(serverKeysPrefix), Unversioned());
	rd >> server_id;
	return server_id;
}
bool serverHasKey(ValueRef storedValue) {
	return storedValue == serverKeysTrue || storedValue == serverKeysTrueEmptyRange;
}

const KeyRef cacheKeysPrefix = LiteralStringRef("\xff\x02/cacheKeys/");

const Key cacheKeysKey(uint16_t idx, const KeyRef& key) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(cacheKeysPrefix);
	wr << idx;
	wr.serializeBytes(LiteralStringRef("/"));
	wr.serializeBytes(key);
	return wr.toValue();
}
const Key cacheKeysPrefixFor(uint16_t idx) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(cacheKeysPrefix);
	wr << idx;
	wr.serializeBytes(LiteralStringRef("/"));
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

const KeyRef cacheChangeKey = LiteralStringRef("\xff\x02/cacheChangeKey");
const KeyRangeRef cacheChangeKeys(LiteralStringRef("\xff\x02/cacheChangeKeys/"),
                                  LiteralStringRef("\xff\x02/cacheChangeKeys0"));
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

const KeyRangeRef tssMappingKeys(LiteralStringRef("\xff/tss/"), LiteralStringRef("\xff/tss0"));

const KeyRangeRef tssQuarantineKeys(LiteralStringRef("\xff/tssQ/"), LiteralStringRef("\xff/tssQ0"));

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

const KeyRangeRef tssMismatchKeys(LiteralStringRef("\xff/tssMismatch/"), LiteralStringRef("\xff/tssMismatch0"));

const KeyRangeRef serverMetadataKeys(LiteralStringRef("\xff/serverMetadata/"),
                                     LiteralStringRef("\xff/serverMetadata0"));

const KeyRangeRef serverTagKeys(LiteralStringRef("\xff/serverTag/"), LiteralStringRef("\xff/serverTag0"));

const KeyRef serverTagPrefix = serverTagKeys.begin;
const KeyRangeRef serverTagConflictKeys(LiteralStringRef("\xff/serverTagConflict/"),
                                        LiteralStringRef("\xff/serverTagConflict0"));
const KeyRef serverTagConflictPrefix = serverTagConflictKeys.begin;
// serverTagHistoryKeys is the old tag a storage server uses before it is migrated to a different location.
// For example, we can copy a SS file to a remote DC and start the SS there;
//   The new SS will need to consume the last bits of data from the old tag it is responsible for.
const KeyRangeRef serverTagHistoryKeys(LiteralStringRef("\xff/serverTagHistory/"),
                                       LiteralStringRef("\xff/serverTagHistory0"));
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

const KeyRangeRef tagLocalityListKeys(LiteralStringRef("\xff/tagLocalityList/"),
                                      LiteralStringRef("\xff/tagLocalityList0"));
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

const KeyRangeRef datacenterReplicasKeys(LiteralStringRef("\xff\x02/datacenterReplicas/"),
                                         LiteralStringRef("\xff\x02/datacenterReplicas0"));
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

const KeyRangeRef tLogDatacentersKeys(LiteralStringRef("\xff\x02/tLogDatacenters/"),
                                      LiteralStringRef("\xff\x02/tLogDatacenters0"));
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

const KeyRef primaryDatacenterKey = LiteralStringRef("\xff/primaryDatacenter");

// serverListKeys.contains(k) iff k.startsWith( serverListKeys.begin ) because '/'+1 == '0'
const KeyRangeRef serverListKeys(LiteralStringRef("\xff/serverList/"), LiteralStringRef("\xff/serverList0"));
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
const KeyRangeRef processClassKeys(LiteralStringRef("\xff/processClass/"), LiteralStringRef("\xff/processClass0"));
const KeyRef processClassPrefix = processClassKeys.begin;
const KeyRef processClassChangeKey = LiteralStringRef("\xff/processClassChanges");
const KeyRef processClassVersionKey = LiteralStringRef("\xff/processClassChangesVersion");
const ValueRef processClassVersionValue = LiteralStringRef("1");

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

const KeyRangeRef configKeys(LiteralStringRef("\xff/conf/"), LiteralStringRef("\xff/conf0"));
const KeyRef configKeysPrefix = configKeys.begin;

const KeyRef perpetualStorageWiggleKey(LiteralStringRef("\xff/conf/perpetual_storage_wiggle"));
const KeyRef perpetualStorageWiggleLocalityKey(LiteralStringRef("\xff/conf/perpetual_storage_wiggle_locality"));
const KeyRef perpetualStorageWiggleIDPrefix(
    LiteralStringRef("\xff/storageWiggleID/")); // withSuffix /primary or /remote
const KeyRef perpetualStorageWiggleStatsPrefix(
    LiteralStringRef("\xff/storageWiggleStats/")); // withSuffix /primary or /remote

const KeyRef triggerDDTeamInfoPrintKey(LiteralStringRef("\xff/triggerDDTeamInfoPrint"));

const KeyRangeRef excludedServersKeys(LiteralStringRef("\xff/conf/excluded/"), LiteralStringRef("\xff/conf/excluded0"));
const KeyRef excludedServersPrefix = excludedServersKeys.begin;
const KeyRef excludedServersVersionKey = LiteralStringRef("\xff/conf/excluded");
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

const KeyRangeRef excludedLocalityKeys(LiteralStringRef("\xff/conf/excluded_locality/"),
                                       LiteralStringRef("\xff/conf/excluded_locality0"));
const KeyRef excludedLocalityPrefix = excludedLocalityKeys.begin;
const KeyRef excludedLocalityVersionKey = LiteralStringRef("\xff/conf/excluded_locality");
std::string decodeExcludedLocalityKey(KeyRef const& key) {
	ASSERT(key.startsWith(excludedLocalityPrefix));
	return key.removePrefix(excludedLocalityPrefix).toString();
}
std::string encodeExcludedLocalityKey(std::string const& locality) {
	return excludedLocalityPrefix.toString() + locality;
}

const KeyRangeRef failedServersKeys(LiteralStringRef("\xff/conf/failed/"), LiteralStringRef("\xff/conf/failed0"));
const KeyRef failedServersPrefix = failedServersKeys.begin;
const KeyRef failedServersVersionKey = LiteralStringRef("\xff/conf/failed");
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

const KeyRangeRef failedLocalityKeys(LiteralStringRef("\xff/conf/failed_locality/"),
                                     LiteralStringRef("\xff/conf/failed_locality0"));
const KeyRef failedLocalityPrefix = failedLocalityKeys.begin;
const KeyRef failedLocalityVersionKey = LiteralStringRef("\xff/conf/failed_locality");
std::string decodeFailedLocalityKey(KeyRef const& key) {
	ASSERT(key.startsWith(failedLocalityPrefix));
	return key.removePrefix(failedLocalityPrefix).toString();
}
std::string encodeFailedLocalityKey(std::string const& locality) {
	return failedLocalityPrefix.toString() + locality;
}

// const KeyRangeRef globalConfigKeys( LiteralStringRef("\xff/globalConfig/"), LiteralStringRef("\xff/globalConfig0") );
// const KeyRef globalConfigPrefix = globalConfigKeys.begin;

const KeyRangeRef globalConfigDataKeys(LiteralStringRef("\xff/globalConfig/k/"),
                                       LiteralStringRef("\xff/globalConfig/k0"));
const KeyRef globalConfigKeysPrefix = globalConfigDataKeys.begin;

const KeyRangeRef globalConfigHistoryKeys(LiteralStringRef("\xff/globalConfig/h/"),
                                          LiteralStringRef("\xff/globalConfig/h0"));
const KeyRef globalConfigHistoryPrefix = globalConfigHistoryKeys.begin;

const KeyRef globalConfigVersionKey = LiteralStringRef("\xff/globalConfig/v");

const KeyRangeRef workerListKeys(LiteralStringRef("\xff/worker/"), LiteralStringRef("\xff/worker0"));
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

const KeyRangeRef backupProgressKeys(LiteralStringRef("\xff\x02/backupProgress/"),
                                     LiteralStringRef("\xff\x02/backupProgress0"));
const KeyRef backupProgressPrefix = backupProgressKeys.begin;
const KeyRef backupStartedKey = LiteralStringRef("\xff\x02/backupStarted");
extern const KeyRef backupPausedKey = LiteralStringRef("\xff\x02/backupPaused");

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

const KeyRef coordinatorsKey = LiteralStringRef("\xff/coordinators");
const KeyRef logsKey = LiteralStringRef("\xff/logs");
const KeyRef minRequiredCommitVersionKey = LiteralStringRef("\xff/minRequiredCommitVersion");

const KeyRef globalKeysPrefix = LiteralStringRef("\xff/globals");
const KeyRef lastEpochEndKey = LiteralStringRef("\xff/globals/lastEpochEnd");
const KeyRef lastEpochEndPrivateKey = LiteralStringRef("\xff\xff/globals/lastEpochEnd");
const KeyRef killStorageKey = LiteralStringRef("\xff/globals/killStorage");
const KeyRef killStoragePrivateKey = LiteralStringRef("\xff\xff/globals/killStorage");
const KeyRef rebootWhenDurableKey = LiteralStringRef("\xff/globals/rebootWhenDurable");
const KeyRef rebootWhenDurablePrivateKey = LiteralStringRef("\xff\xff/globals/rebootWhenDurable");
const KeyRef primaryLocalityKey = LiteralStringRef("\xff/globals/primaryLocality");
const KeyRef primaryLocalityPrivateKey = LiteralStringRef("\xff\xff/globals/primaryLocality");
const KeyRef fastLoggingEnabled = LiteralStringRef("\xff/globals/fastLoggingEnabled");
const KeyRef fastLoggingEnabledPrivateKey = LiteralStringRef("\xff\xff/globals/fastLoggingEnabled");

// Whenever configuration changes or DD related system keyspace is changed(e.g.., serverList),
// actor must grab the moveKeysLockOwnerKey and update moveKeysLockWriteKey.
// This prevents concurrent write to the same system keyspace.
// When the owner of the DD related system keyspace changes, DD will reboot
const KeyRef moveKeysLockOwnerKey = LiteralStringRef("\xff/moveKeysLock/Owner");
const KeyRef moveKeysLockWriteKey = LiteralStringRef("\xff/moveKeysLock/Write");

const KeyRef dataDistributionModeKey = LiteralStringRef("\xff/dataDistributionMode");
const UID dataDistributionModeLock = UID(6345, 3425);

// Keys to view and control tag throttling
const KeyRangeRef tagThrottleKeys =
    KeyRangeRef(LiteralStringRef("\xff\x02/throttledTags/tag/"), LiteralStringRef("\xff\x02/throttledTags/tag0"));
const KeyRef tagThrottleKeysPrefix = tagThrottleKeys.begin;
const KeyRef tagThrottleAutoKeysPrefix = LiteralStringRef("\xff\x02/throttledTags/tag/\x01");
const KeyRef tagThrottleSignalKey = LiteralStringRef("\xff\x02/throttledTags/signal");
const KeyRef tagThrottleAutoEnabledKey = LiteralStringRef("\xff\x02/throttledTags/autoThrottlingEnabled");
const KeyRef tagThrottleLimitKey = LiteralStringRef("\xff\x02/throttledTags/manualThrottleLimit");
const KeyRef tagThrottleCountKey = LiteralStringRef("\xff\x02/throttledTags/manualThrottleCount");

// Client status info prefix
const KeyRangeRef fdbClientInfoPrefixRange(LiteralStringRef("\xff\x02/fdbClientInfo/"),
                                           LiteralStringRef("\xff\x02/fdbClientInfo0"));
// See remaining fields in GlobalConfig.actor.h

// ConsistencyCheck settings
const KeyRef fdbShouldConsistencyCheckBeSuspended = LiteralStringRef("\xff\x02/ConsistencyCheck/Suspend");

// Request latency measurement key
const KeyRef latencyBandConfigKey = LiteralStringRef("\xff\x02/latencyBandConfig");

// Keyspace to maintain wall clock to version map
const KeyRangeRef timeKeeperPrefixRange(LiteralStringRef("\xff\x02/timeKeeper/map/"),
                                        LiteralStringRef("\xff\x02/timeKeeper/map0"));
const KeyRef timeKeeperVersionKey = LiteralStringRef("\xff\x02/timeKeeper/version");
const KeyRef timeKeeperDisableKey = LiteralStringRef("\xff\x02/timeKeeper/disable");

// Backup Log Mutation constant variables
const KeyRef backupEnabledKey = LiteralStringRef("\xff/backupEnabled");
const KeyRangeRef backupLogKeys(LiteralStringRef("\xff\x02/blog/"), LiteralStringRef("\xff\x02/blog0"));
const KeyRangeRef applyLogKeys(LiteralStringRef("\xff\x02/alog/"), LiteralStringRef("\xff\x02/alog0"));
// static_assert( backupLogKeys.begin.size() == backupLogPrefixBytes, "backupLogPrefixBytes incorrect" );
const KeyRef backupVersionKey = LiteralStringRef("\xff/backupDataFormat");
const ValueRef backupVersionValue = LiteralStringRef("4");
const int backupVersion = 4;

// Log Range constant variables
// \xff/logRanges/[16-byte UID][begin key] := serialize( make_pair([end key], [destination key prefix]),
// IncludeVersion() )
const KeyRangeRef logRangesRange(LiteralStringRef("\xff/logRanges/"), LiteralStringRef("\xff/logRanges0"));

// Layer status metadata prefix
const KeyRangeRef layerStatusMetaPrefixRange(LiteralStringRef("\xff\x02/status/"),
                                             LiteralStringRef("\xff\x02/status0"));

// Backup agent status root
const KeyRangeRef backupStatusPrefixRange(LiteralStringRef("\xff\x02/backupstatus/"),
                                          LiteralStringRef("\xff\x02/backupstatus0"));

// Restore configuration constant variables
const KeyRangeRef fileRestorePrefixRange(LiteralStringRef("\xff\x02/restore-agent/"),
                                         LiteralStringRef("\xff\x02/restore-agent0"));

// Backup Agent configuration constant variables
const KeyRangeRef fileBackupPrefixRange(LiteralStringRef("\xff\x02/backup-agent/"),
                                        LiteralStringRef("\xff\x02/backup-agent0"));

// DR Agent configuration constant variables
const KeyRangeRef databaseBackupPrefixRange(LiteralStringRef("\xff\x02/db-backup-agent/"),
                                            LiteralStringRef("\xff\x02/db-backup-agent0"));

// \xff\x02/sharedLogRangesConfig/destUidLookup/[keyRange]
const KeyRef destUidLookupPrefix = LiteralStringRef("\xff\x02/sharedLogRangesConfig/destUidLookup/");
// \xff\x02/sharedLogRangesConfig/backuplatestVersions/[destUid]/[logUid]
const KeyRef backupLatestVersionsPrefix = LiteralStringRef("\xff\x02/sharedLogRangesConfig/backupLatestVersions/");

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
const KeyRangeRef applyMutationsEndRange(LiteralStringRef("\xff/applyMutationsEnd/"),
                                         LiteralStringRef("\xff/applyMutationsEnd0"));

// \xff/applyMutationsBegin/[16-byte UID] := serialize( beginVersion, Unversioned() )
const KeyRangeRef applyMutationsBeginRange(LiteralStringRef("\xff/applyMutationsBegin/"),
                                           LiteralStringRef("\xff/applyMutationsBegin0"));

// \xff/applyMutationsAddPrefix/[16-byte UID] := addPrefix
const KeyRangeRef applyMutationsAddPrefixRange(LiteralStringRef("\xff/applyMutationsAddPrefix/"),
                                               LiteralStringRef("\xff/applyMutationsAddPrefix0"));

// \xff/applyMutationsRemovePrefix/[16-byte UID] := removePrefix
const KeyRangeRef applyMutationsRemovePrefixRange(LiteralStringRef("\xff/applyMutationsRemovePrefix/"),
                                                  LiteralStringRef("\xff/applyMutationsRemovePrefix0"));

const KeyRangeRef applyMutationsKeyVersionMapRange(LiteralStringRef("\xff/applyMutationsKeyVersionMap/"),
                                                   LiteralStringRef("\xff/applyMutationsKeyVersionMap0"));
const KeyRangeRef applyMutationsKeyVersionCountRange(LiteralStringRef("\xff\x02/applyMutationsKeyVersionCount/"),
                                                     LiteralStringRef("\xff\x02/applyMutationsKeyVersionCount0"));

const KeyRef systemTuplesPrefix = LiteralStringRef("\xff/a/");
const KeyRef metricConfChangeKey = LiteralStringRef("\x01TDMetricConfChanges\x00");

const KeyRangeRef metricConfKeys(LiteralStringRef("\x01TDMetricConf\x00\x01"),
                                 LiteralStringRef("\x01TDMetricConf\x00\x02"));
const KeyRef metricConfPrefix = metricConfKeys.begin;

/*
const Key metricConfKey( KeyRef const& prefix, MetricNameRef const& name, KeyRef const& key  ) {
    BinaryWriter wr(Unversioned());
    wr.serializeBytes( prefix );
    wr.serializeBytes( metricConfPrefix );
    wr.serializeBytes( name.type );
    wr.serializeBytes( LiteralStringRef("\x00\x01") );
    wr.serializeBytes( name.name );
    wr.serializeBytes( LiteralStringRef("\x00\x01") );
    wr.serializeBytes( name.address );
    wr.serializeBytes( LiteralStringRef("\x00\x01") );
    wr.serializeBytes( name.id );
    wr.serializeBytes( LiteralStringRef("\x00\x01") );
    wr.serializeBytes( key );
    wr.serializeBytes( LiteralStringRef("\x00") );
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

const KeyRef maxUIDKey = LiteralStringRef("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff");

const KeyRef databaseLockedKey = LiteralStringRef("\xff/dbLocked");
const KeyRef databaseLockedKeyEnd = LiteralStringRef("\xff/dbLocked\x00");
const KeyRef metadataVersionKey = LiteralStringRef("\xff/metadataVersion");
const KeyRef metadataVersionKeyEnd = LiteralStringRef("\xff/metadataVersion\x00");
const KeyRef metadataVersionRequiredValue =
    LiteralStringRef("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
const KeyRef mustContainSystemMutationsKey = LiteralStringRef("\xff/mustContainSystemMutations");

const KeyRangeRef monitorConfKeys(LiteralStringRef("\xff\x02/monitorConf/"), LiteralStringRef("\xff\x02/monitorConf0"));

const KeyRef restoreRequestDoneKey = LiteralStringRef("\xff\x02/restoreRequestDone");

const KeyRef healthyZoneKey = LiteralStringRef("\xff\x02/healthyZone");
const StringRef ignoreSSFailuresZoneString = LiteralStringRef("IgnoreSSFailures");
const KeyRef rebalanceDDIgnoreKey = LiteralStringRef("\xff\x02/rebalanceDDIgnored");

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

const KeyRangeRef testOnlyTxnStateStorePrefixRange(LiteralStringRef("\xff/TESTONLYtxnStateStore/"),
                                                   LiteralStringRef("\xff/TESTONLYtxnStateStore0"));

const KeyRef writeRecoveryKey = LiteralStringRef("\xff/writeRecovery");
const ValueRef writeRecoveryKeyTrue = LiteralStringRef("1");
const KeyRef snapshotEndVersionKey = LiteralStringRef("\xff/snapshotEndVersion");

const KeyRangeRef changeFeedKeys(LiteralStringRef("\xff\x02/feed/"), LiteralStringRef("\xff\x02/feed0"));
const KeyRef changeFeedPrefix = changeFeedKeys.begin;
const KeyRef changeFeedPrivatePrefix = LiteralStringRef("\xff\xff\x02/feed/");

const Value changeFeedValue(KeyRangeRef const& range, Version popVersion, ChangeFeedStatus status) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withChangeFeed()));
	wr << range;
	wr << popVersion;
	wr << status;
	return wr.toValue();
}

std::tuple<KeyRange, Version, ChangeFeedStatus> decodeChangeFeedValue(ValueRef const& value) {
	KeyRange range;
	Version version;
	ChangeFeedStatus status;
	BinaryReader reader(value, IncludeVersion());
	reader >> range;
	reader >> version;
	reader >> status;
	return std::make_tuple(range, version, status);
}

const KeyRangeRef changeFeedDurableKeys(LiteralStringRef("\xff\xff/cf/"), LiteralStringRef("\xff\xff/cf0"));
const KeyRef changeFeedDurablePrefix = changeFeedDurableKeys.begin;

const Value changeFeedDurableKey(Key const& feed, Version version) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withChangeFeed()));
	wr.serializeBytes(changeFeedDurablePrefix);
	wr << feed;
	wr << bigEndian64(version);
	return wr.toValue();
}
std::pair<Key, Version> decodeChangeFeedDurableKey(ValueRef const& key) {
	Key feed;
	Version version;
	BinaryReader reader(key.removePrefix(changeFeedDurablePrefix), AssumeVersion(ProtocolVersion::withChangeFeed()));
	reader >> feed;
	reader >> version;
	return std::make_pair(feed, bigEndian64(version));
}
const Value changeFeedDurableValue(Standalone<VectorRef<MutationRef>> const& mutations, Version knownCommittedVersion) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withChangeFeed()));
	wr << mutations;
	wr << knownCommittedVersion;
	return wr.toValue();
}
std::pair<Standalone<VectorRef<MutationRef>>, Version> decodeChangeFeedDurableValue(ValueRef const& value) {
	Standalone<VectorRef<MutationRef>> mutations;
	Version knownCommittedVersion;
	BinaryReader reader(value, IncludeVersion());
	reader >> mutations;
	reader >> knownCommittedVersion;
	return std::make_pair(mutations, knownCommittedVersion);
}

const KeyRef configTransactionDescriptionKey = "\xff\xff/description"_sr;
const KeyRange globalConfigKnobKeys = singleKeyRange("\xff\xff/globalKnobs"_sr);
const KeyRangeRef configKnobKeys("\xff\xff/knobs/"_sr, "\xff\xff/knobs0"_sr);
const KeyRangeRef configClassKeys("\xff\xff/configClasses/"_sr, "\xff\xff/configClasses0"_sr);

// key to watch for changes in active blob ranges + KeyRangeMap of active blob ranges
// Blob Manager + Worker stuff is all \xff\x02 to avoid Transaction State Store
const KeyRef blobRangeChangeKey = LiteralStringRef("\xff\x02/blobRangeChange");
const KeyRangeRef blobRangeKeys(LiteralStringRef("\xff\x02/blobRange/"), LiteralStringRef("\xff\x02/blobRange0"));
const KeyRef blobManagerEpochKey = LiteralStringRef("\xff\x02/blobManagerEpoch");

const Value blobManagerEpochValueFor(int64_t epoch) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << epoch;
	return wr.toValue();
}

int64_t decodeBlobManagerEpochValue(ValueRef const& value) {
	int64_t epoch;
	BinaryReader reader(value, IncludeVersion());
	reader >> epoch;
	return epoch;
}

// blob granule data
const KeyRangeRef blobGranuleFileKeys(LiteralStringRef("\xff\x02/bgf/"), LiteralStringRef("\xff\x02/bgf0"));
const KeyRangeRef blobGranuleMappingKeys(LiteralStringRef("\xff\x02/bgm/"), LiteralStringRef("\xff\x02/bgm0"));
const KeyRangeRef blobGranuleLockKeys(LiteralStringRef("\xff\x02/bgl/"), LiteralStringRef("\xff\x02/bgl0"));
const KeyRangeRef blobGranuleSplitKeys(LiteralStringRef("\xff\x02/bgs/"), LiteralStringRef("\xff\x02/bgs0"));
const KeyRangeRef blobGranuleHistoryKeys(LiteralStringRef("\xff\x02/bgh/"), LiteralStringRef("\xff\x02/bgh0"));
const KeyRangeRef blobGranulePruneKeys(LiteralStringRef("\xff\x02/bgp/"), LiteralStringRef("\xff\x02/bgp0"));
const KeyRangeRef blobGranuleVersionKeys(LiteralStringRef("\xff\x02/bgv/"), LiteralStringRef("\xff\x02/bgv0"));
const KeyRef blobGranulePruneChangeKey = LiteralStringRef("\xff\x02/bgpChange");

const uint8_t BG_FILE_TYPE_DELTA = 'D';
const uint8_t BG_FILE_TYPE_SNAPSHOT = 'S';

const Key blobGranuleFileKeyFor(UID granuleID, Version fileVersion, uint8_t fileType) {
	ASSERT(fileType == 'D' || fileType == 'S');
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withBlobGranule()));
	wr.serializeBytes(blobGranuleFileKeys.begin);
	wr << granuleID;
	wr << bigEndian64(fileVersion);
	wr << fileType;
	return wr.toValue();
}

std::tuple<UID, Version, uint8_t> decodeBlobGranuleFileKey(KeyRef const& key) {
	UID granuleID;
	Version fileVersion;
	uint8_t fileType;
	BinaryReader reader(key.removePrefix(blobGranuleFileKeys.begin), AssumeVersion(ProtocolVersion::withBlobGranule()));
	reader >> granuleID;
	reader >> fileVersion;
	reader >> fileType;
	ASSERT(fileType == 'D' || fileType == 'S');
	return std::tuple(granuleID, bigEndian64(fileVersion), fileType);
}

const KeyRange blobGranuleFileKeyRangeFor(UID granuleID) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withBlobGranule()));
	wr.serializeBytes(blobGranuleFileKeys.begin);
	wr << granuleID;
	Key startKey = wr.toValue();
	return KeyRangeRef(startKey, strinc(startKey));
}

const Value blobGranuleFileValueFor(StringRef const& filename, int64_t offset, int64_t length, int64_t fullFileLength) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << filename;
	wr << offset;
	wr << length;
	wr << fullFileLength;
	return wr.toValue();
}

std::tuple<Standalone<StringRef>, int64_t, int64_t, int64_t> decodeBlobGranuleFileValue(ValueRef const& value) {
	StringRef filename;
	int64_t offset;
	int64_t length;
	int64_t fullFileLength;
	BinaryReader reader(value, IncludeVersion());
	reader >> filename;
	reader >> offset;
	reader >> length;
	reader >> fullFileLength;
	return std::tuple(filename, offset, length, fullFileLength);
}

const Value blobGranulePruneValueFor(Version version, KeyRange range, bool force) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << version;
	wr << range;
	wr << force;
	return wr.toValue();
}

std::tuple<Version, KeyRange, bool> decodeBlobGranulePruneValue(ValueRef const& value) {
	Version version;
	KeyRange range;
	bool force;
	BinaryReader reader(value, IncludeVersion());
	reader >> version;
	reader >> range;
	reader >> force;
	return std::tuple(version, range, force);
}

const Value blobGranuleMappingValueFor(UID const& workerID) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << workerID;
	return wr.toValue();
}

UID decodeBlobGranuleMappingValue(ValueRef const& value) {
	UID workerID;
	BinaryReader reader(value, IncludeVersion());
	reader >> workerID;
	return workerID;
}

const Key blobGranuleLockKeyFor(KeyRangeRef const& keyRange) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withBlobGranule()));
	wr.serializeBytes(blobGranuleLockKeys.begin);
	wr << keyRange;
	return wr.toValue();
}

const Value blobGranuleLockValueFor(int64_t epoch, int64_t seqno, UID changeFeedId) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << epoch;
	wr << seqno;
	wr << changeFeedId;
	return wr.toValue();
}

std::tuple<int64_t, int64_t, UID> decodeBlobGranuleLockValue(const ValueRef& value) {
	int64_t epoch, seqno;
	UID changeFeedId;
	BinaryReader reader(value, IncludeVersion());
	reader >> epoch;
	reader >> seqno;
	reader >> changeFeedId;
	return std::make_tuple(epoch, seqno, changeFeedId);
}

const Key blobGranuleSplitKeyFor(UID const& parentGranuleID, UID const& granuleID) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withBlobGranule()));
	wr.serializeBytes(blobGranuleSplitKeys.begin);
	wr << parentGranuleID;
	wr << granuleID;
	return wr.toValue();
}

std::pair<UID, UID> decodeBlobGranuleSplitKey(KeyRef const& key) {
	UID parentGranuleID;
	UID granuleID;
	BinaryReader reader(key.removePrefix(blobGranuleSplitKeys.begin),
	                    AssumeVersion(ProtocolVersion::withBlobGranule()));

	reader >> parentGranuleID;
	reader >> granuleID;
	return std::pair(parentGranuleID, granuleID);
}

const KeyRange blobGranuleSplitKeyRangeFor(UID const& parentGranuleID) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withBlobGranule()));
	wr.serializeBytes(blobGranuleSplitKeys.begin);
	wr << parentGranuleID;

	Key startKey = wr.toValue();
	return KeyRangeRef(startKey, strinc(startKey));
}

const Value blobGranuleSplitValueFor(BlobGranuleSplitState st) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << st;
	return addVersionStampAtEnd(wr.toValue());
}

std::pair<BlobGranuleSplitState, Version> decodeBlobGranuleSplitValue(const ValueRef& value) {
	BlobGranuleSplitState st;
	Version v;
	BinaryReader reader(value, IncludeVersion());
	reader >> st;
	reader >> v;

	return std::pair(st, bigEndian64(v));
}

const Key blobGranuleHistoryKeyFor(KeyRangeRef const& range, Version version) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withBlobGranule()));
	wr.serializeBytes(blobGranuleHistoryKeys.begin);
	wr << range;
	wr << bigEndian64(version);
	return wr.toValue();
}

std::pair<KeyRange, Version> decodeBlobGranuleHistoryKey(const KeyRef& key) {
	KeyRangeRef keyRange;
	Version version;
	BinaryReader reader(key.removePrefix(blobGranuleHistoryKeys.begin),
	                    AssumeVersion(ProtocolVersion::withBlobGranule()));
	reader >> keyRange;
	reader >> version;
	return std::make_pair(keyRange, bigEndian64(version));
}

const KeyRange blobGranuleHistoryKeyRangeFor(KeyRangeRef const& range) {
	return KeyRangeRef(blobGranuleHistoryKeyFor(range, 0), blobGranuleHistoryKeyFor(range, MAX_VERSION));
}

const Value blobGranuleHistoryValueFor(Standalone<BlobGranuleHistoryValue> const& historyValue) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << historyValue;
	return wr.toValue();
}

Standalone<BlobGranuleHistoryValue> decodeBlobGranuleHistoryValue(const ValueRef& value) {
	Standalone<BlobGranuleHistoryValue> historyValue;
	BinaryReader reader(value, IncludeVersion());
	reader >> historyValue;
	return historyValue;
}

const KeyRangeRef blobWorkerListKeys(LiteralStringRef("\xff\x02/bwList/"), LiteralStringRef("\xff\x02/bwList0"));

const Key blobWorkerListKeyFor(UID workerID) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withBlobGranule()));
	wr.serializeBytes(blobWorkerListKeys.begin);
	wr << workerID;
	return wr.toValue();
}

UID decodeBlobWorkerListKey(KeyRef const& key) {
	UID workerID;
	BinaryReader reader(key.removePrefix(blobWorkerListKeys.begin), AssumeVersion(ProtocolVersion::withBlobGranule()));
	reader >> workerID;
	return workerID;
}

const Value blobWorkerListValue(BlobWorkerInterface const& worker) {
	return ObjectWriter::toValue(worker, IncludeVersion(ProtocolVersion::withBlobGranule()));
}

BlobWorkerInterface decodeBlobWorkerListValue(ValueRef const& value) {
	BlobWorkerInterface interf;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(interf);
	return interf;
}

Value encodeTenantEntry(TenantMapEntry const& tenantEntry) {
	return ObjectWriter::toValue(tenantEntry, IncludeVersion());
}

TenantMapEntry decodeTenantEntry(ValueRef const& value) {
	TenantMapEntry entry;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(entry);
	return entry;
}

const KeyRangeRef tenantMapKeys("\xff/tenantMap/"_sr, "\xff/tenantMap0"_sr);
const KeyRef tenantMapPrefix = tenantMapKeys.begin;
const KeyRef tenantMapPrivatePrefix = "\xff\xff/tenantMap/"_sr;
const KeyRef tenantLastIdKey = "\xff/tenantLastId/"_sr;
const KeyRef tenantDataPrefixKey = "\xff/tenantDataPrefix"_sr;

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
