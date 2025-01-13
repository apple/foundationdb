/*
 * SystemData.cpp
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

#include "fdbclient/SystemData.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/FDBTypes.h"
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

SystemKey::SystemKey(Key const& k) : Key(k) {
	// In simulation, if k is not in the known key set then make sure no known key is a prefix of it, then add it to the
	// known set.
	if (g_network->isSimulated()) {
		static std::unordered_set<Key> knownKeys;

		if (!knownKeys.contains(k)) {
			for (auto& known : knownKeys) {
				if (k.startsWith(known) || known.startsWith(k)) {
					TraceEvent(SevError, "SystemKeyPrefixConflict").detail("NewKey", k).detail("ExistingKey", known);
					UNSTOPPABLE_ASSERT(false);
				}
			}
			knownKeys.insert(k);
		}
	}
}

// keyServersKeys.contains(k) iff k.startsWith(keyServersPrefix)
const KeyRangeRef keyServersKeys("\xff/keyServers/"_sr, "\xff/keyServers0"_sr);
const KeyRef keyServersPrefix = keyServersKeys.begin;
const KeyRef keyServersEnd = keyServersKeys.end;
const KeyRangeRef keyServersKeyServersKeys("\xff/keyServers/\xff/keyServers/"_sr,
                                           "\xff/keyServers/\xff/keyServers0"_sr);
const KeyRef keyServersKeyServersKey = keyServersKeyServersKeys.begin;

// These constants are selected to be easily recognized during debugging.
// Note that the last bit of the following constants is 0, indicating that physical shard move is disabled.
const UID anonymousShardId = UID(0x666666, 0x88888888);
const uint64_t emptyShardId = 0x2222222;

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

	for (const KeyValueRef& kv : result) {
		UID uid = decodeServerTagKey(kv.key);
		if (std::find(src.begin(), src.end(), uid) != src.end()) {
			srcTag.push_back(decodeServerTagValue(kv.value));
		}
		if (std::find(dest.begin(), dest.end(), uid) != dest.end()) {
			destTag.push_back(decodeServerTagValue(kv.value));
		}
	}

	ASSERT_WE_THINK(src.size() == srcTag.size() && dest.size() == destTag.size());

	return keyServersValue(srcTag, destTag);
}

const Value keyServersValue(const std::vector<UID>& src,
                            const std::vector<UID>& dest,
                            const UID& srcID,
                            const UID& destID) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withShardEncodeLocationMetaData()));
	if (dest.empty()) {
		ASSERT(!destID.isValid());
		wr << src << dest << srcID;
	} else {
		wr << src << dest << srcID << destID;
	}
	return wr.toValue();
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
	if (rd.protocolVersion().hasShardEncodeLocationMetaData()) {
		UID srcId, destId;
		decodeKeyServersValue(result, value, src, dest, srcId, destId);
		return;
	}
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

void decodeKeyServersValue(RangeResult result,
                           const ValueRef& value,
                           std::vector<UID>& src,
                           std::vector<UID>& dest,
                           UID& srcID,
                           UID& destID,
                           bool missingIsError) {
	src.clear();
	dest.clear();
	srcID = UID();
	destID = UID();

	if (value.size() == 0) {
		return;
	}

	BinaryReader rd(value, IncludeVersion());
	if (rd.protocolVersion().hasShardEncodeLocationMetaData()) {
		rd >> src >> dest >> srcID;
		if (rd.empty()) {
			ASSERT(dest.empty());
		} else {
			rd >> destID;
			rd.assertEnd();
		}
	} else {
		decodeKeyServersValue(result, value, src, dest, missingIsError);
		if (!src.empty()) {
			srcID = anonymousShardId;
		}
		if (!dest.empty()) {
			destID = anonymousShardId;
		}
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
		if (rd.protocolVersion().hasShardEncodeLocationMetaData()) {
			UID srcId, destId;
			rd >> srcId;
			if (rd.empty()) {
				ASSERT(dest.empty());
				destId = UID();
			} else {
				rd >> destId;
			}
		}
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

bool isSystemKey(KeyRef key) {
	return key.size() && key[0] == systemKeys.begin[0];
}

const KeyRangeRef conflictingKeysRange =
    KeyRangeRef("\xff\xff/transaction/conflicting_keys/"_sr, "\xff\xff/transaction/conflicting_keys/\xff\xff"_sr);
const ValueRef conflictingKeysTrue = "1"_sr;
const ValueRef conflictingKeysFalse = "0"_sr;

const KeyRangeRef readConflictRangeKeysRange =
    KeyRangeRef("\xff\xff/transaction/read_conflict_range/"_sr, "\xff\xff/transaction/read_conflict_range/\xff\xff"_sr);

const KeyRangeRef writeConflictRangeKeysRange = KeyRangeRef("\xff\xff/transaction/write_conflict_range/"_sr,
                                                            "\xff\xff/transaction/write_conflict_range/\xff\xff"_sr);

const KeyRef accumulativeChecksumKey = "\xff\xff/accumulativeChecksum"_sr;

const Value accumulativeChecksumValue(const AccumulativeChecksumState& acsState) {
	return ObjectWriter::toValue(acsState, IncludeVersion());
}

AccumulativeChecksumState decodeAccumulativeChecksum(const ValueRef& value) {
	AccumulativeChecksumState acsState;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(acsState);
	return acsState;
}

const KeyRangeRef auditKeys = KeyRangeRef("\xff/audits/"_sr, "\xff/audits0"_sr);
const KeyRef auditPrefix = auditKeys.begin;
const KeyRangeRef auditRanges = KeyRangeRef("\xff/auditRanges/"_sr, "\xff/auditRanges0"_sr);
const KeyRef auditRangePrefix = auditRanges.begin;
const KeyRangeRef auditServers = KeyRangeRef("\xff/auditServers/"_sr, "\xff/auditServers0"_sr);
const KeyRef auditServerPrefix = auditServers.begin;

const Key auditKey(const AuditType type, const UID& auditId) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(auditPrefix);
	wr << static_cast<uint8_t>(type);
	wr.serializeBytes("/"_sr);
	wr << bigEndian64(auditId.first());
	return wr.toValue();
}

const KeyRange auditKeyRange(const AuditType type) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(auditPrefix);
	wr << static_cast<uint8_t>(type);
	wr.serializeBytes("/"_sr);
	return prefixRange(wr.toValue());
}

const Key auditRangeBasedProgressPrefixFor(const AuditType type, const UID& auditId) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(auditRangePrefix);
	wr << static_cast<uint8_t>(type);
	wr.serializeBytes("/"_sr);
	wr << bigEndian64(auditId.first());
	wr.serializeBytes("/"_sr);
	return wr.toValue();
}

const KeyRange auditRangeBasedProgressRangeFor(const AuditType type, const UID& auditId) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(auditRangePrefix);
	wr << static_cast<uint8_t>(type);
	wr.serializeBytes("/"_sr);
	wr << bigEndian64(auditId.first());
	wr.serializeBytes("/"_sr);
	return prefixRange(wr.toValue());
}

const KeyRange auditRangeBasedProgressRangeFor(const AuditType type) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(auditRangePrefix);
	wr << static_cast<uint8_t>(type);
	wr.serializeBytes("/"_sr);
	return prefixRange(wr.toValue());
}

const Key auditServerBasedProgressPrefixFor(const AuditType type, const UID& auditId, const UID& serverId) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(auditServerPrefix);
	wr << static_cast<uint8_t>(type);
	wr.serializeBytes("/"_sr);
	wr << bigEndian64(auditId.first());
	wr.serializeBytes("/"_sr);
	wr << bigEndian64(serverId.first());
	wr.serializeBytes("/"_sr);
	return wr.toValue();
}

const KeyRange auditServerBasedProgressRangeFor(const AuditType type, const UID& auditId) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(auditServerPrefix);
	wr << static_cast<uint8_t>(type);
	wr.serializeBytes("/"_sr);
	wr << bigEndian64(auditId.first());
	wr.serializeBytes("/"_sr);
	return prefixRange(wr.toValue());
}

const KeyRange auditServerBasedProgressRangeFor(const AuditType type) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(auditServerPrefix);
	wr << static_cast<uint8_t>(type);
	wr.serializeBytes("/"_sr);
	return prefixRange(wr.toValue());
}

const Value auditStorageStateValue(const AuditStorageState& auditStorageState) {
	return ObjectWriter::toValue(auditStorageState, IncludeVersion());
}

AuditStorageState decodeAuditStorageState(const ValueRef& value) {
	AuditStorageState auditState;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(auditState);
	return auditState;
}

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

// "\xff/dataMoves/[[UID]] := [[DataMoveMetaData]]"
const KeyRangeRef dataMoveKeys("\xff/dataMoves/"_sr, "\xff/dataMoves0"_sr);
const Key dataMoveKeyFor(UID dataMoveId) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(dataMoveKeys.begin);
	wr << dataMoveId;
	return wr.toValue();
}

const Value dataMoveValue(const DataMoveMetaData& dataMoveMetaData) {
	return ObjectWriter::toValue(dataMoveMetaData, IncludeVersion());
}

UID decodeDataMoveKey(const KeyRef& key) {
	UID id;
	BinaryReader rd(key.removePrefix(dataMoveKeys.begin), Unversioned());
	rd >> id;
	return id;
}

DataMoveMetaData decodeDataMoveValue(const ValueRef& value) {
	DataMoveMetaData dataMove;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(dataMove);
	return dataMove;
}

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
	auto protocolVersion = currentProtocolVersion();
	protocolVersion.addObjectSerializerFlag();
	return ObjectWriter::toValue(ssi, IncludeVersion(protocolVersion));
}

const KeyRangeRef ddStatsRange =
    KeyRangeRef("\xff\xff/metrics/data_distribution_stats/"_sr, "\xff\xff/metrics/data_distribution_stats/\xff\xff"_sr);

//    "\xff/storageCache/[[begin]]" := "[[vector<uint16_t>]]"
const KeyRangeRef storageCacheKeys("\xff/storageCache/"_sr, "\xff/storageCache0"_sr);
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

const KeyRangeRef serverKeysRange = KeyRangeRef("\xff/serverKeys/"_sr, "\xff/serverKeys0"_sr);
const KeyRef serverKeysPrefix = serverKeysRange.begin;
const ValueRef serverKeysTrue = "1"_sr, // compatible with what was serverKeysTrue
    serverKeysTrueEmptyRange = "3"_sr, // the server treats the range as empty.
    serverKeysFalse;

const UID newDataMoveId(const uint64_t physicalShardId,
                        AssignEmptyRange assignEmptyRange,
                        const DataMoveType type,
                        const DataMovementReason reason,
                        UnassignShard unassignShard) {
	uint64_t split = 0;
	if (assignEmptyRange) {
		split = emptyShardId;
	} else if (unassignShard) {
		split = 0;
	} else {
		do {
			split = deterministicRandom()->randomUInt64();
			// Clear the lower 16 bits
			split = (~0xFFFF) & split;
			// Set DataMoveType to the lower [0, 8) bits
			split = split | static_cast<uint64_t>(type);
			// Set DataMovementReason to the lower [8, 16) bits
			split = split | (static_cast<uint64_t>(reason) << 8);
		} while (split == anonymousShardId.second() || split == 0 || split == emptyShardId);
	}
	return UID(physicalShardId, split);
}

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

std::pair<UID, Key> serverKeysDecodeServerBegin(const KeyRef& key) {
	UID server_id;
	BinaryReader rd(key.removePrefix(serverKeysPrefix), Unversioned());
	rd >> server_id;
	rd.readBytes(1); // skip "/"
	const auto remainingBytes = rd.remainingBytes();
	KeyRef ref = KeyRef(rd.arenaRead(remainingBytes), remainingBytes);
	// std::cout << ref.size() << " " << ref.toString() << std::endl;
	return std::make_pair(server_id, Key(ref));
}

bool serverHasKey(ValueRef storedValue) {
	UID shardId;
	bool assigned, emptyRange;
	DataMoveType dataMoveType = DataMoveType::LOGICAL;
	DataMovementReason dataMoveReason = DataMovementReason::INVALID;
	decodeServerKeysValue(storedValue, assigned, emptyRange, dataMoveType, shardId, dataMoveReason);
	return assigned;
}

const Value serverKeysValue(const UID& id) {
	if (!id.isValid()) {
		return serverKeysFalse;
	}

	BinaryWriter wr(IncludeVersion(ProtocolVersion::withShardEncodeLocationMetaData()));
	wr << id;
	return wr.toValue();
}

void decodeDataMoveId(const UID& id,
                      bool& assigned,
                      bool& emptyRange,
                      DataMoveType& dataMoveType,
                      DataMovementReason& dataMoveReason) {
	dataMoveType = DataMoveType::LOGICAL;
	dataMoveReason = DataMovementReason::INVALID;
	assigned = id.second() != 0LL;
	emptyRange = id.second() == emptyShardId;
	if (assigned && !emptyRange && id != anonymousShardId) {
		dataMoveType = static_cast<DataMoveType>(0xFF & id.second());
		if (dataMoveType >= DataMoveType::NUMBER_OF_TYPES || dataMoveType < DataMoveType::LOGICAL) {
			TraceEvent(SevWarnAlways, "DecodeDataMoveIdError")
			    .detail("Reason", "DataMoveTypeOutScope")
			    .detail("Value", dataMoveType)
			    .detail("DataMoveID", id)
			    .detail("SplitIDToDecode", id.second());
			dataMoveType = DataMoveType::LOGICAL;
			// When upgrade from a release 7.3.x where dataMoveType is not encoded in
			// datamove id, the decoded dataMoveType can be out of scope.
			// For this case, we set it to DataMoveType::LOGICAL.
			// It is possible that the new binary decodes a wrong data move type.
			// However, it only affects whether dest SSes use physical shard move
			// to get the data from the source server.
			// When SS decodes a data move type, SS checks whether its KVStore supports
			// the data move type. If no, SS will use DataMoveType::LOGICAL by default.
		}
		dataMoveReason = static_cast<DataMovementReason>(0xFF & (id.second() >> 8));
		if (dataMoveReason >= DataMovementReason::NUMBER_OF_REASONS || dataMoveReason < DataMovementReason::INVALID) {
			TraceEvent(SevWarnAlways, "DecodeDataMoveIdError")
			    .detail("Reason", "DataMoveReasonOutScope")
			    .detail("Value", dataMoveReason)
			    .detail("DataMoveID", id)
			    .detail("SplitIDToDecode", id.second());
			dataMoveReason = DataMovementReason::INVALID;
			// When upgrade from release-7.3 where dataMoveReason is not encoded in
			// datamove id, the decoded reason can be out of scope.
			// For this case, we set it to DataMovementReason::INVALID.
			// Currently, this is only used by priority-based fetchKeys throttling.
			// It is possible that the new binary decodes a wrong data move reason.
			// However, it only effects the throttling decison made by the fetchKeys.
			// If the fetchKeys throttling is enabled and it misbehaves after the upgrading
			// from release-7.3, users can temporarily disable the feature until the old data moves
			// have been consumed.
		}
	}
}

void decodeServerKeysValue(const ValueRef& value,
                           bool& assigned,
                           bool& emptyRange,
                           DataMoveType& dataMoveType,
                           UID& id,
                           DataMovementReason& dataMoveReason) {
	dataMoveType = DataMoveType::LOGICAL;
	dataMoveReason = DataMovementReason::INVALID;
	if (value.size() == 0) {
		assigned = false;
		emptyRange = false;
		id = UID();
	} else if (value == serverKeysTrue) {
		assigned = true;
		emptyRange = false;
		id = anonymousShardId;
	} else if (value == serverKeysTrueEmptyRange) {
		assigned = true;
		emptyRange = true;
		id = anonymousShardId;
	} else if (value == serverKeysFalse) {
		assigned = false;
		emptyRange = false;
		id = UID();
	} else {
		BinaryReader rd(value, IncludeVersion());
		ASSERT(rd.protocolVersion().hasShardEncodeLocationMetaData());
		rd >> id;
		decodeDataMoveId(id, assigned, emptyRange, dataMoveType, dataMoveReason);
	}
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

const KeyRef serverMetadataChangeKey = "\xff\x02/serverMetadataChanges"_sr;
const KeyRangeRef serverMetadataKeys("\xff/serverMetadata/"_sr, "\xff/serverMetadata0"_sr);

UID decodeServerMetadataKey(const KeyRef& key) {
	// Key is packed by KeyBackedObjectMap::packKey
	return TupleCodec<UID>::unpack(key.removePrefix(serverMetadataKeys.begin));
}

StorageMetadataType decodeServerMetadataValue(const KeyRef& value) {
	StorageMetadataType type;
	ObjectReader rd(value.begin(), IncludeVersion());
	rd.deserialize(type);
	return type;
}

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
	ASSERT_WE_THINK(reader.protocolVersion().hasTagLocality());
	reader >> s;
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
	BinaryWriter wr(AssumeVersion(currentProtocolVersion()));
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
	BinaryReader rd(key.removePrefix(tagLocalityListKeys.begin), AssumeVersion(currentProtocolVersion()));
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
	BinaryWriter wr(AssumeVersion(currentProtocolVersion()));
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
	BinaryReader rd(key.removePrefix(datacenterReplicasKeys.begin), AssumeVersion(currentProtocolVersion()));
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
	BinaryWriter wr(AssumeVersion(currentProtocolVersion()));
	wr.serializeBytes(tLogDatacentersKeys.begin);
	wr << dcID;
	return wr.toValue();
}
Optional<Value> decodeTLogDatacentersKey(KeyRef const& key) {
	Optional<Value> dcID;
	BinaryReader rd(key.removePrefix(tLogDatacentersKeys.begin), AssumeVersion(currentProtocolVersion()));
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

const Value serverListValue(StorageServerInterface const& server) {
	auto protocolVersion = currentProtocolVersion();
	protocolVersion.addObjectSerializerFlag();
	return ObjectWriter::toValue(server, IncludeVersion(protocolVersion));
}

UID decodeServerListKey(KeyRef const& key) {
	UID serverID;
	BinaryReader rd(key.removePrefix(serverListKeys.begin), Unversioned());
	rd >> serverID;
	return serverID;
}

StorageServerInterface decodeServerListValueFB(ValueRef const& value) {
	StorageServerInterface s;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(s);
	return s;
}

StorageServerInterface decodeServerListValue(ValueRef const& value) {
	StorageServerInterface s;
	BinaryReader reader(value, IncludeVersion());

	ASSERT_WE_THINK(reader.protocolVersion().hasStorageInterfaceReadiness());

	return decodeServerListValueFB(value);
}

Value swVersionValue(SWVersion const& swversion) {
	auto protocolVersion = currentProtocolVersion();
	protocolVersion.addObjectSerializerFlag();
	return ObjectWriter::toValue(swversion, IncludeVersion(protocolVersion));
}

SWVersion decodeSWVersionValue(ValueRef const& value) {
	SWVersion s;
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
const KeyRef perpetualStorageWiggleLocalityKey("\xff/conf/perpetual_storage_wiggle_locality"_sr);
// The below two are there for compatible upgrade and downgrade. After 7.3, the perpetual wiggle related keys should use
// format "\xff/storageWiggle/[primary | remote]/[fieldName]". See class StorageWiggleData for the data schema.
const KeyRef perpetualStorageWiggleIDPrefix("\xff/storageWiggleID/"_sr); // withSuffix /primary/ or /remote/
const KeyRef perpetualStorageWiggleStatsPrefix("\xff/storageWiggleStats/"_sr); // withSuffix /primary or /remote
const KeyRef perpetualStorageWigglePrefix("\xff/storageWiggle/"_sr);

const KeyRef triggerDDTeamInfoPrintKey("\xff/triggerDDTeamInfoPrint"_sr);

const KeyRef encryptionAtRestModeConfKey("\xff/conf/encryption_at_rest_mode"_sr);
const KeyRef tenantModeConfKey("\xff/conf/tenant_mode"_sr);

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

bool mutationForKey(const MutationRef& m, const KeyRef& key) {
	return isSingleKeyMutation((MutationRef::Type)m.type) && m.param1 == key;
}

const KeyRef previousCoordinatorsKey = "\xff/previousCoordinators"_sr;
const KeyRef coordinatorsKey = "\xff/coordinators"_sr;
const KeyRef logsKey = "\xff/logs"_sr;
const KeyRef minRequiredCommitVersionKey = "\xff/minRequiredCommitVersion"_sr;
const KeyRef versionEpochKey = "\xff/versionEpoch"_sr;

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
const KeyRef constructDataKey = "\xff/globals/constructData"_sr;

// Whenever configuration changes or DD related system keyspace is changed(e.g.., serverList),
// actor must grab the moveKeysLockOwnerKey and update moveKeysLockWriteKey.
// This prevents concurrent write to the same system keyspace.
// When the owner of the DD related system keyspace changes, DD will reboot
const KeyRef moveKeysLockOwnerKey = "\xff/moveKeysLock/Owner"_sr;
const KeyRef moveKeysLockWriteKey = "\xff/moveKeysLock/Write"_sr;

const KeyRef dataDistributionModeKey = "\xff/dataDistributionMode"_sr;
const UID dataDistributionModeLock = UID(6345, 3425);

// Bulk loading keys
const KeyRef bulkLoadModeKey = "\xff/bulkLoadMode"_sr;
const KeyRangeRef bulkLoadTaskKeys = KeyRangeRef("\xff/bulkLoadTask/"_sr, "\xff/bulkLoadTask0"_sr);
const KeyRef bulkLoadTaskPrefix = bulkLoadTaskKeys.begin;

const Value bulkLoadTaskStateValue(const BulkLoadTaskState& bulkLoadTaskState) {
	return ObjectWriter::toValue(bulkLoadTaskState, IncludeVersion());
}

BulkLoadTaskState decodeBulkLoadTaskState(const ValueRef& value) {
	BulkLoadTaskState bulkLoadTaskState;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(bulkLoadTaskState);
	return bulkLoadTaskState;
}

const KeyRangeRef bulkLoadJobKeys = KeyRangeRef("\xff/bulkLoadJob/"_sr, "\xff/bulkLoadJob0"_sr);
const KeyRef bulkLoadJobPrefix = bulkLoadJobKeys.begin;

const Value bulkLoadJobValue(const BulkLoadJobState& bulkLoadJobState) {
	return ObjectWriter::toValue(bulkLoadJobState, IncludeVersion());
}

BulkLoadJobState decodeBulkLoadJobState(const ValueRef& value) {
	BulkLoadJobState bulkLoadJobState;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(bulkLoadJobState);
	return bulkLoadJobState;
}

// Bulk dumping keys
const KeyRef bulkDumpModeKey = "\xff/bulkDumpMode"_sr;
const KeyRangeRef bulkDumpKeys = KeyRangeRef("\xff/bulkDump/"_sr, "\xff/bulkDump0"_sr);
const KeyRef bulkDumpPrefix = bulkDumpKeys.begin;

const Value bulkDumpStateValue(const BulkDumpState& bulkDumpState) {
	return ObjectWriter::toValue(bulkDumpState, IncludeVersion());
}

BulkDumpState decodeBulkDumpState(const ValueRef& value) {
	BulkDumpState bulkDumpState;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(bulkDumpState);
	return bulkDumpState;
}

// Range Lock
const std::string rangeLockNameForBulkLoad = "BulkLoad";

const KeyRangeRef rangeLockKeys = KeyRangeRef("\xff/rangeLock/"_sr, "\xff/rangeLock0"_sr);
const KeyRef rangeLockPrefix = rangeLockKeys.begin;

const Value rangeLockStateSetValue(const RangeLockStateSet& rangeLockStateSet) {
	return ObjectWriter::toValue(rangeLockStateSet, IncludeVersion());
}

RangeLockStateSet decodeRangeLockStateSet(const ValueRef& value) {
	RangeLockStateSet rangeLockStateSet;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(rangeLockStateSet);
	return rangeLockStateSet;
}

const KeyRangeRef rangeLockOwnerKeys = KeyRangeRef("\xff/rangeLockOwner/"_sr, "\xff/rangeLockOwner0"_sr);
const KeyRef rangeLockOwnerPrefix = rangeLockOwnerKeys.begin;

const Key rangeLockOwnerKeyFor(const RangeLockOwnerName& ownerUniqueID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(rangeLockOwnerPrefix);
	wr.serializeBytes(StringRef(ownerUniqueID));
	return wr.toValue();
}

const RangeLockOwnerName decodeRangeLockOwnerKey(const KeyRef& key) {
	std::string ownerUniqueID;
	BinaryReader rd(key.removePrefix(rangeLockOwnerPrefix), Unversioned());
	rd >> ownerUniqueID;
	return ownerUniqueID;
}

const Value rangeLockOwnerValue(const RangeLockOwner& rangeLockOwner) {
	return ObjectWriter::toValue(rangeLockOwner, IncludeVersion());
}

RangeLockOwner decodeRangeLockOwner(const ValueRef& value) {
	RangeLockOwner rangeLockOwner;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(rangeLockOwner);
	return rangeLockOwner;
}

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

// Durable cluster ID key. Added "Key" to the end to differentiate from the key
// "\xff/clusterId" which was stored in the txnStateStore in FDB 7.1, whereas
// this key is stored in the database in 7.2+.
const KeyRef clusterIdKey = "\xff/clusterIdKey"_sr;

// Backup Log Mutation constant variables
const KeyRef backupEnabledKey = "\xff/backupEnabled"_sr;
const KeyRangeRef backupLogKeys("\xff\x02/blog/"_sr, "\xff\x02/blog0"_sr);
const KeyRangeRef applyLogKeys("\xff\x02/alog/"_sr, "\xff\x02/alog0"_sr);
bool isBackupLogMutation(const MutationRef& m) {
	return isSingleKeyMutation((MutationRef::Type)m.type) &&
	       (backupLogKeys.contains(m.param1) || applyLogKeys.contains(m.param1));
}
bool isAccumulativeChecksumMutation(const MutationRef& m) {
	return m.type == MutationRef::SetValue && m.param1 == accumulativeChecksumKey;
}
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

std::tuple<Standalone<StringRef>, uint64_t, uint64_t, uint64_t> decodeConstructKeys(ValueRef value) {
	StringRef keyStart;
	uint64_t valSize, keyCount, seed;
	BinaryReader rd(value, Unversioned());
	rd >> keyStart;
	rd >> valSize;
	rd >> keyCount;
	rd >> seed;
	return std::make_tuple(keyStart, valSize, keyCount, seed);
}

Value encodeConstructValue(StringRef keyStart, uint64_t valSize, uint64_t keyCount, uint64_t seed) {
	BinaryWriter wr(Unversioned());
	wr << keyStart;
	wr << valSize;
	wr << keyCount;
	wr << seed;
	return wr.toValue();
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

const KeyRangeRef changeFeedKeys("\xff\x02/feed/"_sr, "\xff\x02/feed0"_sr);
const KeyRef changeFeedPrefix = changeFeedKeys.begin;
const KeyRef changeFeedPrivatePrefix = "\xff\xff\x02/feed/"_sr;

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

const KeyRangeRef changeFeedDurableKeys("\xff\xff/cf/"_sr, "\xff\xff/cf0"_sr);
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

const KeyRangeRef changeFeedCacheKeys("\xff\xff/cc/"_sr, "\xff\xff/cc0"_sr);
const KeyRef changeFeedCachePrefix = changeFeedCacheKeys.begin;

const Value changeFeedCacheKey(Key const& prefix, Key const& feed, KeyRange const& range, Version version) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withChangeFeed()));
	wr.serializeBytes(prefix);
	wr.serializeBytes(changeFeedCachePrefix);
	wr << feed;
	wr << range;
	wr << bigEndian64(version);
	return wr.toValue();
}
std::tuple<Key, KeyRange, Version> decodeChangeFeedCacheKey(KeyRef const& prefix, ValueRef const& key) {
	Key feed;
	KeyRange range;
	Version version;
	BinaryReader reader(key.removePrefix(prefix).removePrefix(changeFeedCachePrefix),
	                    AssumeVersion(ProtocolVersion::withChangeFeed()));
	reader >> feed;
	reader >> range;
	reader >> version;
	return std::make_tuple(feed, range, bigEndian64(version));
}

// The versions of these mutations must be less than or equal to the version in the changeFeedCacheKey
const Value changeFeedCacheValue(Standalone<VectorRef<MutationsAndVersionRef>> const& mutations) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withChangeFeed()));
	wr << mutations;
	return wr.toValue();
}
Standalone<VectorRef<MutationsAndVersionRef>> decodeChangeFeedCacheValue(ValueRef const& value) {
	Standalone<VectorRef<MutationsAndVersionRef>> mutations;
	BinaryReader reader(value, IncludeVersion());
	reader >> mutations;
	return mutations;
}

const KeyRangeRef changeFeedCacheFeedKeys("\xff\xff/ccd/"_sr, "\xff\xff/ccd0"_sr);
const KeyRef changeFeedCacheFeedPrefix = changeFeedCacheFeedKeys.begin;

const Value changeFeedCacheFeedKey(Key const& prefix, Key const& feed, KeyRange const& range) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withChangeFeed()));
	wr.serializeBytes(changeFeedCacheFeedPrefix);
	wr << prefix;
	wr << feed;
	wr << range;
	return wr.toValue();
}
std::tuple<Key, Key, KeyRange> decodeChangeFeedCacheFeedKey(ValueRef const& key) {
	Key prefix;
	Key feed;
	KeyRange range;
	BinaryReader reader(key.removePrefix(changeFeedCacheFeedPrefix), AssumeVersion(ProtocolVersion::withChangeFeed()));
	reader >> prefix;
	reader >> feed;
	reader >> range;
	return std::make_tuple(prefix, feed, range);
}
const Value changeFeedCacheFeedValue(Version const& version, Version const& popped) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withChangeFeed()));
	wr << version;
	wr << popped;
	return wr.toValue();
}
std::pair<Version, Version> decodeChangeFeedCacheFeedValue(ValueRef const& value) {
	Version version;
	Version popped;
	BinaryReader reader(value, IncludeVersion());
	reader >> version;
	reader >> popped;
	return std::make_pair(version, popped);
}

const KeyRef configTransactionDescriptionKey = "\xff\xff/description"_sr;
const KeyRange globalConfigKnobKeys = singleKeyRange("\xff\xff/globalKnobs"_sr);
const KeyRangeRef configKnobKeys("\xff\xff/knobs/"_sr, "\xff\xff/knobs0"_sr);
const KeyRangeRef configClassKeys("\xff\xff/configClasses/"_sr, "\xff\xff/configClasses0"_sr);

// key to watch for changes in active blob ranges + KeyRangeMap of active blob ranges
// Blob Manager + Worker stuff is all \xff\x02 to avoid Transaction State Store
const KeyRef blobRangeChangeKey = "\xff\x02/blobRangeChange"_sr;
const KeyRangeRef blobRangeKeys("\xff\x02/blobRange/"_sr, "\xff\x02/blobRange0"_sr);
const KeyRangeRef blobRangeChangeLogKeys("\xff\x02/blobRangeLog/"_sr, "\xff\x02/blobRangeLog0"_sr);
const KeyRef blobManagerEpochKey = "\xff\x02/blobManagerEpoch"_sr;

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
const KeyRef blobRangeActive = "1"_sr;
const KeyRef blobRangeInactive = StringRef();

bool isBlobRangeActive(const ValueRef& blobRangeValue) {
	// Empty or "0" is inactive
	// "1" is active
	// Support future change where serialized metadata struct is also active
	return !blobRangeValue.empty() && blobRangeValue != blobRangeInactive;
}

const Key blobRangeChangeLogReadKeyFor(Version version) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withBlobRangeChangeLog()));
	wr.serializeBytes(blobRangeChangeLogKeys.begin);
	wr << bigEndian64(version);
	return wr.toValue();
}

const Value blobRangeChangeLogValueFor(const Standalone<BlobRangeChangeLogRef>& value) {
	return ObjectWriter::toValue(value, IncludeVersion(ProtocolVersion::withBlobRangeChangeLog()));
}

Standalone<BlobRangeChangeLogRef> decodeBlobRangeChangeLogValue(ValueRef const& value) {
	Standalone<BlobRangeChangeLogRef> result;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(result);
	return result;
}

const KeyRangeRef blobGranuleFileKeys("\xff\x02/bgf/"_sr, "\xff\x02/bgf0"_sr);
const KeyRangeRef blobGranuleMappingKeys("\xff\x02/bgm/"_sr, "\xff\x02/bgm0"_sr);
const KeyRangeRef blobGranuleLockKeys("\xff\x02/bgl/"_sr, "\xff\x02/bgl0"_sr);
const KeyRangeRef blobGranuleSplitKeys("\xff\x02/bgs/"_sr, "\xff\x02/bgs0"_sr);
const KeyRangeRef blobGranuleMergeKeys("\xff\x02/bgmerge/"_sr, "\xff\x02/bgmerge0"_sr);
const KeyRangeRef blobGranuleMergeBoundaryKeys("\xff\x02/bgmergebounds/"_sr, "\xff\x02/bgmergebounds0"_sr);
const KeyRangeRef blobGranuleHistoryKeys("\xff\x02/bgh/"_sr, "\xff\x02/bgh0"_sr);
const KeyRangeRef blobGranulePurgeKeys("\xff\x02/bgp/"_sr, "\xff\x02/bgp0"_sr);
const KeyRangeRef blobGranuleForcePurgedKeys("\xff\x02/bgpforce/"_sr, "\xff\x02/bgpforce0"_sr);
const KeyRef blobGranulePurgeChangeKey = "\xff\x02/bgpChange"_sr;

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

const Value blobGranuleFileValueFor(StringRef const& filename,
                                    int64_t offset,
                                    int64_t length,
                                    int64_t fullFileLength,
                                    int64_t logicalSize,
                                    Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta) {
	auto protocolVersion = CLIENT_KNOBS->ENABLE_BLOB_GRANULE_FILE_LOGICAL_SIZE
	                           ? ProtocolVersion::withBlobGranuleFileLogicalSize()
	                           : ProtocolVersion::withBlobGranule();
	BinaryWriter wr(IncludeVersion(protocolVersion));
	wr << filename;
	wr << offset;
	wr << length;
	wr << fullFileLength;
	wr << cipherKeysMeta;
	if (CLIENT_KNOBS->ENABLE_BLOB_GRANULE_FILE_LOGICAL_SIZE) {
		wr << logicalSize;
	}
	return wr.toValue();
}

std::tuple<Standalone<StringRef>, int64_t, int64_t, int64_t, int64_t, Optional<BlobGranuleCipherKeysMeta>>
decodeBlobGranuleFileValue(ValueRef const& value) {
	StringRef filename;
	int64_t offset;
	int64_t length;
	int64_t fullFileLength;
	int64_t logicalSize;
	Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta;

	BinaryReader reader(value, IncludeVersion());
	reader >> filename;
	reader >> offset;
	reader >> length;
	reader >> fullFileLength;
	reader >> cipherKeysMeta;
	if (reader.protocolVersion().hasBlobGranuleFileLogicalSize()) {
		reader >> logicalSize;
	} else {
		// fall back to estimating logical size as physical size
		logicalSize = length;
	}
	return std::tuple(filename, offset, length, fullFileLength, logicalSize, cipherKeysMeta);
}

const Value blobGranulePurgeValueFor(Version version, KeyRange range, bool force) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << version;
	wr << range;
	wr << force;
	return wr.toValue();
}

std::tuple<Version, KeyRange, bool> decodeBlobGranulePurgeValue(ValueRef const& value) {
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

const Key blobGranuleMergeKeyFor(UID const& mergeGranuleID) {
	// TODO should we bump this assumed version to 7.2 as blob granule merging is not in 7.1? 7.1 won't try to read this
	// data though since it didn't exist before
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withBlobGranule()));
	wr.serializeBytes(blobGranuleMergeKeys.begin);
	wr << mergeGranuleID;

	return wr.toValue();
}

UID decodeBlobGranuleMergeKey(KeyRef const& key) {
	UID mergeGranuleID;
	BinaryReader reader(key.removePrefix(blobGranuleMergeKeys.begin),
	                    AssumeVersion(ProtocolVersion::withBlobGranule()));

	reader >> mergeGranuleID;
	return mergeGranuleID;
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

const Value blobGranuleMergeValueFor(KeyRange mergeKeyRange,
                                     std::vector<UID> parentGranuleIDs,
                                     std::vector<Key> parentGranuleRanges,
                                     std::vector<Version> parentGranuleStartVersions) {
	ASSERT(parentGranuleIDs.size() == parentGranuleRanges.size() - 1);
	ASSERT(parentGranuleIDs.size() == parentGranuleStartVersions.size());

	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << mergeKeyRange;
	wr << parentGranuleIDs;
	wr << parentGranuleRanges;
	wr << parentGranuleStartVersions;
	return addVersionStampAtEnd(wr.toValue());
}
std::tuple<KeyRange, Version, std::vector<UID>, std::vector<Key>, std::vector<Version>> decodeBlobGranuleMergeValue(
    ValueRef const& value) {
	KeyRange range;
	Version v;
	std::vector<UID> parentGranuleIDs;
	std::vector<Key> parentGranuleRanges;
	std::vector<Version> parentGranuleStartVersions;

	BinaryReader reader(value, IncludeVersion());
	reader >> range;
	reader >> parentGranuleIDs;
	reader >> parentGranuleRanges;
	reader >> parentGranuleStartVersions;
	reader >> v;

	ASSERT(parentGranuleIDs.size() == parentGranuleRanges.size() - 1);
	ASSERT(parentGranuleIDs.size() == parentGranuleStartVersions.size());
	ASSERT(bigEndian64(v) >= 0);

	return std::tuple(range, bigEndian64(v), parentGranuleIDs, parentGranuleRanges, parentGranuleStartVersions);
}

const Key blobGranuleMergeBoundaryKeyFor(const KeyRef& key) {
	return key.withPrefix(blobGranuleMergeBoundaryKeys.begin);
}

const Value blobGranuleMergeBoundaryValueFor(BlobGranuleMergeBoundary const& boundary) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << boundary;
	return wr.toValue();
}

Standalone<BlobGranuleMergeBoundary> decodeBlobGranuleMergeBoundaryValue(const ValueRef& value) {
	Standalone<BlobGranuleMergeBoundary> boundaryValue;
	BinaryReader reader(value, IncludeVersion());
	reader >> boundaryValue;
	return boundaryValue;
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
	ASSERT(historyValue.parentVersions.empty() ||
	       historyValue.parentBoundaries.size() - 1 == historyValue.parentVersions.size());
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranule()));
	wr << historyValue;
	return wr.toValue();
}

Standalone<BlobGranuleHistoryValue> decodeBlobGranuleHistoryValue(const ValueRef& value) {
	Standalone<BlobGranuleHistoryValue> historyValue;
	BinaryReader reader(value, IncludeVersion());
	reader >> historyValue;
	ASSERT(historyValue.parentVersions.empty() ||
	       historyValue.parentBoundaries.size() - 1 == historyValue.parentVersions.size());
	return historyValue;
}

const KeyRangeRef blobWorkerListKeys("\xff\x02/bwList/"_sr, "\xff\x02/bwList0"_sr);

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

const KeyRangeRef blobWorkerAffinityKeys("\xff\x02/bwa/"_sr, "\xff\x02/bwa0"_sr);

const Key blobWorkerAffinityKeyFor(UID workerID) {
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withBlobGranule()));
	wr.serializeBytes(blobWorkerAffinityKeys.begin);
	wr << workerID;
	return wr.toValue();
}

UID decodeBlobWorkerAffinityKey(KeyRef const& key) {
	UID workerID;
	BinaryReader reader(key.removePrefix(blobWorkerAffinityKeys.begin),
	                    AssumeVersion(ProtocolVersion::withBlobGranule()));
	reader >> workerID;
	return workerID;
}

const Value blobWorkerAffinityValue(UID const& id) {
	return ObjectWriter::toValue(id, IncludeVersion(ProtocolVersion::withBlobGranuleFile()));
}

UID decodeBlobWorkerAffinityValue(ValueRef const& value) {
	UID id;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(id);
	return id;
}

const Key blobManifestVersionKey = "\xff\x02/blobManifestVersion"_sr;

const KeyRangeRef idempotencyIdKeys("\xff\x02/idmp/"_sr, "\xff\x02/idmp0"_sr);
const KeyRef idempotencyIdsExpiredVersion("\xff\x02/idmpExpiredVersion"_sr);

// for tests
void testSSISerdes(StorageServerInterface const& ssi) {
	printf("ssi=\nid=%s\nlocality=%s\nisTss=%s\ntssId=%s\nacceptingRequests=%s\naddress=%s\ngetValue=%s\n\n\n",
	       ssi.id().toString().c_str(),
	       ssi.locality.toString().c_str(),
	       ssi.isTss() ? "true" : "false",
	       ssi.isTss() ? ssi.tssPairID.get().toString().c_str() : "",
	       ssi.isAcceptingRequests() ? "true" : "false",
	       ssi.address().toString().c_str(),
	       ssi.getValue.getEndpoint().token.toString().c_str());

	StorageServerInterface ssi2 = decodeServerListValue(serverListValue(ssi));

	printf("ssi2=\nid=%s\nlocality=%s\nisTss=%s\ntssId=%s\nacceptingRequests=%s\naddress=%s\ngetValue=%s\n\n\n",
	       ssi2.id().toString().c_str(),
	       ssi2.locality.toString().c_str(),
	       ssi2.isTss() ? "true" : "false",
	       ssi2.isTss() ? ssi2.tssPairID.get().toString().c_str() : "",
	       ssi2.isAcceptingRequests() ? "true" : "false",
	       ssi2.address().toString().c_str(),
	       ssi2.getValue.getEndpoint().token.toString().c_str());

	ASSERT(ssi.id() == ssi2.id());
	ASSERT(ssi.locality == ssi2.locality);
	ASSERT(ssi.isTss() == ssi2.isTss());
	ASSERT(ssi.isAcceptingRequests() == ssi2.isAcceptingRequests());
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

	testSSISerdes(ssi);

	ssi.tssPairID = UID(0x2345234523452345, 0x1238123812381238);

	testSSISerdes(ssi);
	printf("ssi serdes test complete\n");

	return Void();
}

// Tests compatibility of different keyServersValue() and decodeKeyServersValue().
TEST_CASE("noSim/SystemData/compat/KeyServers") {
	printf("testing keyServers serdes\n");
	std::vector<UID> src, dest;
	std::map<Tag, UID> tag_uid;
	std::map<UID, Tag> uid_tag;
	std::vector<Tag> srcTag, destTag;
	const int n = 3;
	int8_t locality = 1;
	uint16_t id = 1;
	UID srcId = deterministicRandom()->randomUniqueID(), destId = deterministicRandom()->randomUniqueID();
	for (int i = 0; i < n; ++i) {
		src.push_back(deterministicRandom()->randomUniqueID());
		tag_uid.emplace(Tag(locality, id), src.back());
		uid_tag.emplace(src.back(), Tag(locality, id++));
		dest.push_back(deterministicRandom()->randomUniqueID());
		tag_uid.emplace(Tag(locality, id), dest.back());
		uid_tag.emplace(dest.back(), Tag(locality, id++));
	}
	std::sort(src.begin(), src.end());
	std::sort(dest.begin(), dest.end());
	RangeResult idTag;
	for (int i = 0; i < src.size(); ++i) {
		idTag.push_back_deep(idTag.arena(), KeyValueRef(serverTagKeyFor(src[i]), serverTagValue(uid_tag[src[i]])));
	}
	for (int i = 0; i < dest.size(); ++i) {
		idTag.push_back_deep(idTag.arena(), KeyValueRef(serverTagKeyFor(dest[i]), serverTagValue(uid_tag[dest[i]])));
	}

	auto decodeAndVerify =
	    [&src, &dest, &tag_uid, &idTag](ValueRef v, const UID expectedSrcId, const UID expectedDestId) {
		    std::vector<UID> resSrc, resDest;
		    UID resSrcId, resDestId;

		    decodeKeyServersValue(idTag, v, resSrc, resDest, resSrcId, resDestId);
		    TraceEvent("VerifyKeyServersSerDes")
		        .detail("ExpectedSrc", describe(src))
		        .detail("ActualSrc", describe(resSrc))
		        .detail("ExpectedDest", describe(dest))
		        .detail("ActualDest", describe(resDest))
		        .detail("ExpectedDestID", expectedDestId)
		        .detail("ActualDestID", resDestId)
		        .detail("ExpectedSrcID", expectedSrcId)
		        .detail("ActualSrcID", resSrcId);
		    ASSERT(std::equal(src.begin(), src.end(), resSrc.begin()));
		    ASSERT(std::equal(dest.begin(), dest.end(), resDest.begin()));
		    ASSERT(resSrcId == expectedSrcId);
		    ASSERT(resDestId == expectedDestId);

		    resSrc.clear();
		    resDest.clear();
		    decodeKeyServersValue(idTag, v, resSrc, resDest);
		    ASSERT(std::equal(src.begin(), src.end(), resSrc.begin()));
		    ASSERT(std::equal(dest.begin(), dest.end(), resDest.begin()));

		    resSrc.clear();
		    resDest.clear();
		    decodeKeyServersValue(tag_uid, v, resSrc, resDest);
		    ASSERT(std::equal(src.begin(), src.end(), resSrc.begin()));
		    ASSERT(std::equal(dest.begin(), dest.end(), resDest.begin()));
	    };

	Value v = keyServersValue(src, dest, srcId, destId);
	decodeAndVerify(v, srcId, destId);

	printf("ssi serdes test part.1 complete\n");

	v = keyServersValue(idTag, src, dest);
	decodeAndVerify(v, anonymousShardId, anonymousShardId);

	printf("ssi serdes test part.2 complete\n");

	dest.clear();
	destId = UID();

	v = keyServersValue(src, dest, srcId, destId);
	decodeAndVerify(v, srcId, destId);

	printf("ssi serdes test part.3 complete\n");

	v = keyServersValue(idTag, src, dest);
	decodeAndVerify(v, anonymousShardId, UID());

	printf("ssi serdes test complete\n");

	return Void();
}

TEST_CASE("noSim/SystemData/DataMoveId") {
	printf("testing data move ID encoding/decoding\n");
	const uint64_t physicalShardId = deterministicRandom()->randomUInt64();
	const DataMoveType type =
	    static_cast<DataMoveType>(deterministicRandom()->randomInt(0, static_cast<int>(DataMoveType::NUMBER_OF_TYPES)));
	const DataMovementReason reason = static_cast<DataMovementReason>(
	    deterministicRandom()->randomInt(1, static_cast<int>(DataMovementReason::NUMBER_OF_REASONS)));
	const UID dataMoveId = newDataMoveId(physicalShardId, AssignEmptyRange(false), type, reason, UnassignShard(false));

	bool assigned, emptyRange;
	DataMoveType decodeType;
	DataMovementReason decodeReason = DataMovementReason::INVALID;
	decodeDataMoveId(dataMoveId, assigned, emptyRange, decodeType, decodeReason);

	ASSERT(type == decodeType && reason == decodeReason);

	printf("testing data move ID encoding/decoding complete\n");

	return Void();
}
