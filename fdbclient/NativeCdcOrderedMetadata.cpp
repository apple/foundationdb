/*
 * NativeCdcOrderedMetadata.cpp
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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>

#include "NativeCdcOrderedMetadata.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/SystemData.h"
#include "flow/CodeProbe.h"
#include "flow/UnitTest.h"
#include "flow/serialize.h"

const KeyRangeRef cdcOrderedStreamKeys("\xff\x02/cdc/orderedStream/"_sr, "\xff\x02/cdc/orderedStream0"_sr);
const KeyRangeRef cdcOrderedParentKeys("\xff\x02/cdc/orderedParent/"_sr, "\xff\x02/cdc/orderedParent0"_sr);

namespace {

constexpr uint32_t orderedMetadataSchemaVersion = 1;
constexpr size_t maxOrderedMetadataBytes = 100000;
constexpr size_t maxOrderedKeyBytes = 10000;

template <class T>
T readScalar(BinaryReader& reader) {
	if (reader.remainingBytes() < sizeof(T)) {
		throw serialization_failed();
	}
	T value;
	reader >> value;
	return value;
}

void writeKey(BinaryWriter& writer, KeyRef key) {
	writer << static_cast<uint32_t>(key.size());
	writer.serializeBytes(key);
}

Key readKey(BinaryReader& reader) {
	const uint32_t size = readScalar<uint32_t>(reader);
	if (size > maxOrderedKeyBytes || size > reader.remainingBytes()) {
		throw serialization_failed();
	}
	return Key(KeyRef(static_cast<const uint8_t*>(reader.readBytes(size)), size));
}

void readSchema(BinaryReader& reader) {
	if (readScalar<uint32_t>(reader) != orderedMetadataSchemaVersion) {
		throw serialization_failed();
	}
}

Key orderedKeyFor(KeyRef prefix, CDCStreamId streamId) {
	if (streamId == 0) {
		throw client_invalid_operation();
	}
	BinaryWriter writer(Unversioned());
	writer.serializeBytes(prefix);
	writer << streamId;
	return writer.toValue();
}

CDCStreamId decodeOrderedKey(KeyRef prefix, KeyRef key) {
	if (!key.startsWith(prefix) || key.size() != prefix.size() + sizeof(CDCStreamId)) {
		throw serialization_failed();
	}
	BinaryReader reader(key.removePrefix(prefix), Unversioned());
	const auto streamId = readScalar<CDCStreamId>(reader);
	if (streamId == 0) {
		throw serialization_failed();
	}
	return streamId;
}

size_t encodedMetadataSize(const NativeCdcOrderedMetadata& metadata) {
	size_t size = 4 * sizeof(uint32_t) + metadata.partitions().size() * sizeof(CDCStreamId);
	for (const auto& range : metadata.ranges()) {
		size += 2 * sizeof(uint32_t) + range.begin.size() + range.end.size();
	}
	for (const auto& splitPoint : metadata.splitPoints()) {
		size += sizeof(uint32_t) + splitPoint.size();
	}
	return size;
}

template <class Operation>
void expectError(Operation operation, int expectedCode) {
	try {
		operation();
	} catch (Error& error) {
		ASSERT_EQ(error.code(), expectedCode);
		return;
	}
	ASSERT(false);
}

} // namespace

std::vector<std::vector<KeyRange>> nativeCdcOrderedPartitionRanges(const std::vector<KeyRange>& canonicalRanges,
                                                                   const std::vector<Key>& splitPoints) {
	if (canonicalRanges.empty() || canonicalRanges.size() > maxNativeCdcOrderedRanges ||
	    splitPoints.size() >= maxNativeCdcOrderedPartitions) {
		throw client_invalid_operation();
	}
	for (size_t i = 0; i < canonicalRanges.size(); ++i) {
		const auto& range = canonicalRanges[i];
		if (range.empty() || !normalKeys.contains(range) || range.begin.size() > maxOrderedKeyBytes ||
		    range.end.size() > maxOrderedKeyBytes || (i > 0 && canonicalRanges[i - 1].end >= range.begin)) {
			throw client_invalid_operation();
		}
	}
	for (size_t i = 0; i < splitPoints.size(); ++i) {
		const auto& splitPoint = splitPoints[i];
		if (splitPoint.size() > maxOrderedKeyBytes || splitPoint <= canonicalRanges.front().begin ||
		    splitPoint >= canonicalRanges.back().end || (i > 0 && splitPoints[i - 1] >= splitPoint)) {
			throw client_invalid_operation();
		}
	}

	std::vector<std::vector<KeyRange>> result(splitPoints.size() + 1);
	for (size_t i = 0; i < result.size(); ++i) {
		const KeyRef begin = i == 0 ? canonicalRanges.front().begin : splitPoints[i - 1];
		const KeyRef end = i == splitPoints.size() ? canonicalRanges.back().end : splitPoints[i];
		for (const auto& range : canonicalRanges) {
			const KeyRef intersectionBegin = std::max(begin, range.begin);
			const KeyRef intersectionEnd = std::min(end, range.end);
			if (intersectionBegin < intersectionEnd) {
				result[i].emplace_back(KeyRangeRef(intersectionBegin, intersectionEnd));
			}
		}
		if (result[i].empty()) {
			throw client_invalid_operation();
		}
	}
	return result;
}

NativeCdcOrderedMetadata::NativeCdcOrderedMetadata(std::vector<KeyRange> ranges,
                                                   std::vector<Key> splitPoints,
                                                   std::vector<CDCStreamId> partitions)
  : registeredRanges(std::move(ranges)), partitionSplitPoints(std::move(splitPoints)),
    partitionStreamIds(std::move(partitions)) {
	if (partitionStreamIds.size() != partitionSplitPoints.size() + 1) {
		throw client_invalid_operation();
	}
	nativeCdcOrderedPartitionRanges(registeredRanges, partitionSplitPoints);
	auto sortedIds = partitionStreamIds;
	std::sort(sortedIds.begin(), sortedIds.end());
	if (sortedIds.front() == 0 || std::adjacent_find(sortedIds.begin(), sortedIds.end()) != sortedIds.end() ||
	    encodedMetadataSize(*this) > maxOrderedMetadataBytes) {
		throw client_invalid_operation();
	}
}

Key cdcOrderedStreamKeyFor(CDCStreamId logicalId) {
	return orderedKeyFor(cdcOrderedStreamKeys.begin, logicalId);
}

Key cdcOrderedParentKeyFor(CDCStreamId childId) {
	return orderedKeyFor(cdcOrderedParentKeys.begin, childId);
}

CDCStreamId decodeCDCOrderedStreamKey(KeyRef key) {
	return decodeOrderedKey(cdcOrderedStreamKeys.begin, key);
}

CDCStreamId decodeCDCOrderedParentKey(KeyRef key) {
	return decodeOrderedKey(cdcOrderedParentKeys.begin, key);
}

Value cdcOrderedStreamValue(const NativeCdcOrderedMetadata& metadata) {
	BinaryWriter writer(Unversioned());
	writer << orderedMetadataSchemaVersion << static_cast<uint32_t>(metadata.ranges().size());
	for (const auto& range : metadata.ranges()) {
		writeKey(writer, range.begin);
		writeKey(writer, range.end);
	}
	writer << static_cast<uint32_t>(metadata.splitPoints().size());
	for (const auto& splitPoint : metadata.splitPoints()) {
		writeKey(writer, splitPoint);
	}
	writer << static_cast<uint32_t>(metadata.partitions().size());
	for (const CDCStreamId partition : metadata.partitions()) {
		writer << partition;
	}
	return writer.toValue();
}

NativeCdcOrderedMetadata decodeCDCOrderedStreamValue(ValueRef value) {
	if (value.size() > maxOrderedMetadataBytes) {
		throw serialization_failed();
	}
	BinaryReader reader(value, Unversioned());
	readSchema(reader);
	const uint32_t rangeCount = readScalar<uint32_t>(reader);
	if (rangeCount == 0 || rangeCount > maxNativeCdcOrderedRanges) {
		throw serialization_failed();
	}
	std::vector<KeyRange> ranges;
	ranges.reserve(rangeCount);
	for (uint32_t i = 0; i < rangeCount; ++i) {
		Key begin = readKey(reader);
		Key end = readKey(reader);
		if (begin >= end) {
			throw serialization_failed();
		}
		ranges.emplace_back(KeyRangeRef(begin, end));
	}
	const uint32_t splitCount = readScalar<uint32_t>(reader);
	if (splitCount >= maxNativeCdcOrderedPartitions) {
		throw serialization_failed();
	}
	std::vector<Key> splitPoints;
	splitPoints.reserve(splitCount);
	for (uint32_t i = 0; i < splitCount; ++i) {
		splitPoints.push_back(readKey(reader));
	}
	const uint32_t partitionCount = readScalar<uint32_t>(reader);
	if (partitionCount != splitCount + 1 || reader.remainingBytes() != partitionCount * sizeof(CDCStreamId)) {
		throw serialization_failed();
	}
	std::vector<CDCStreamId> partitions;
	partitions.reserve(partitionCount);
	for (uint32_t i = 0; i < partitionCount; ++i) {
		partitions.push_back(readScalar<CDCStreamId>(reader));
	}
	try {
		return NativeCdcOrderedMetadata(std::move(ranges), std::move(splitPoints), std::move(partitions));
	} catch (Error& error) {
		if (error.code() == error_code_client_invalid_operation) {
			throw serialization_failed();
		}
		throw;
	}
}

Value cdcOrderedParentValue(CDCStreamId logicalId) {
	if (logicalId == 0) {
		throw client_invalid_operation();
	}
	BinaryWriter writer(Unversioned());
	writer << orderedMetadataSchemaVersion << logicalId;
	return writer.toValue();
}

CDCStreamId decodeCDCOrderedParentValue(ValueRef value) {
	if (value.size() != sizeof(uint32_t) + sizeof(CDCStreamId)) {
		throw serialization_failed();
	}
	BinaryReader reader(value, Unversioned());
	readSchema(reader);
	const auto logicalId = readScalar<CDCStreamId>(reader);
	if (logicalId == 0) {
		throw serialization_failed();
	}
	return logicalId;
}

namespace {

void validateVersionedScalar(ValueRef value, ValueRef reference, int scalarBytes) {
	if (value.size() != reference.size() || !value.startsWith(reference.substr(0, reference.size() - scalarBytes))) {
		throw serialization_failed();
	}
}

Version readMinVersion(ValueRef value) {
	if (value.size() != sizeof(Version) + sizeof(uint16_t)) {
		validateVersionedScalar(value, cdcMinVersionValue(0), sizeof(Version));
	}
	const Version minVersion = decodeCDCMinVersionValue(value);
	if (minVersion < 0 || minVersion == std::numeric_limits<Version>::max()) {
		throw serialization_failed();
	}
	return minVersion;
}

} // namespace

Version validateNativeCdcOrderedPartition(CDCStreamId logicalId,
                                          CDCStreamId childId,
                                          const std::vector<KeyRange>& expectedRanges,
                                          ValueRef ranges,
                                          ValueRef parent,
                                          ValueRef minimum,
                                          Version commonMinimum) {
	if (childId == logicalId || ranges != cdcStreamKeysValue(expectedRanges) ||
	    decodeCDCOrderedParentValue(parent) != logicalId) {
		throw serialization_failed();
	}
	const Version minVersion = readMinVersion(minimum);
	if (commonMinimum != invalidVersion && minVersion != commonMinimum) {
		throw serialization_failed();
	}
	return minVersion;
}

Future<Optional<NativeCdcOrderedSnapshot>> readNativeCdcOrderedSnapshot(Transaction* tr, CDCStreamId logicalId) {
	if (logicalId == 0) {
		throw client_invalid_operation();
	}
	const Optional<Value> groupValue = co_await tr->get(cdcOrderedStreamKeyFor(logicalId));
	if (!groupValue.present()) {
		co_return Optional<NativeCdcOrderedSnapshot>();
	}
	NativeCdcOrderedMetadata metadata = decodeCDCOrderedStreamValue(groupValue.get());
	const auto partitionRanges = nativeCdcOrderedPartitionRanges(metadata.ranges(), metadata.splitPoints());
	std::vector<Future<Optional<Value>>> rangeReads;
	std::vector<Future<Optional<Value>>> parentReads;
	std::vector<Future<Optional<Value>>> minimumReads;
	for (const CDCStreamId child : metadata.partitions()) {
		rangeReads.push_back(tr->get(cdcStreamKeyFor(child)));
		parentReads.push_back(tr->get(cdcOrderedParentKeyFor(child)));
		minimumReads.push_back(tr->get(cdcMinVersionKeyFor(child)));
	}
	Version commonMinVersion = invalidVersion;
	for (size_t i = 0; i < metadata.partitions().size(); ++i) {
		const Optional<Value> ranges = co_await rangeReads[i];
		const Optional<Value> parent = co_await parentReads[i];
		const Optional<Value> minimum = co_await minimumReads[i];
		commonMinVersion = validateNativeCdcOrderedPartition(logicalId,
		                                                     metadata.partitions()[i],
		                                                     partitionRanges[i],
		                                                     ranges.orDefault(Value()),
		                                                     parent.orDefault(Value()),
		                                                     minimum.orDefault(Value()),
		                                                     commonMinVersion);
	}
	co_return Optional<NativeCdcOrderedSnapshot>(NativeCdcOrderedSnapshot{ std::move(metadata), commonMinVersion });
}

Future<Optional<NativeCdcOrderedSnapshot>> readNativeCdcOrderedSnapshot(Database cx, CDCStreamId logicalId) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			co_return co_await readNativeCdcOrderedSnapshot(&tr, logicalId);
		} catch (Error& error) {
			err = error;
		}
		co_await tr.onError(err);
	}
}

Future<Version> acknowledgeNativeCdcOrderedStream(Database cx,
                                                  CDCStreamId logicalId,
                                                  NativeCdcOrderedMetadata expectedMetadata,
                                                  Version consumedThrough,
                                                  Version knownAvailableThrough) {
	if (logicalId == 0 || consumedThrough < 0 || consumedThrough >= std::numeric_limits<Version>::max() - 1 ||
	    knownAvailableThrough < invalidVersion) {
		throw client_invalid_operation();
	}
	const Version minUnpoppedVersion = consumedThrough + 1;
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			const Optional<NativeCdcOrderedSnapshot> snapshot = co_await readNativeCdcOrderedSnapshot(&tr, logicalId);
			if (!snapshot.present() || snapshot.get().metadata != expectedMetadata) {
				throw client_invalid_operation();
			}
			if (minUnpoppedVersion <= snapshot.get().minVersion) {
				CODE_PROBE(true, "Ordered native CDC preserves a duplicate acknowledgement");
				co_return snapshot.get().minVersion;
			}
			const Version readVersion = co_await tr.getReadVersion();
			if (consumedThrough > readVersion && consumedThrough > knownAvailableThrough) {
				throw client_invalid_operation();
			}
			const Value minimum = cdcMinVersionValue(minUnpoppedVersion);
			for (const CDCStreamId child : expectedMetadata.partitions()) {
				tr.set(cdcMinVersionKeyFor(child), minimum);
			}
			co_await tr.commit();
			CODE_PROBE(true, "Ordered native CDC advances all partition acknowledgements atomically");
			co_return minUnpoppedVersion;
		} catch (Error& error) {
			err = error;
		}
		co_await tr.onError(err);
	}
}

void forceLinkNativeCdcOrderedMetadataTests() {}

TEST_CASE("/NativeCDC/OrderedMetadata/PartitionRanges") {
	const std::vector<KeyRange> ranges{ KeyRangeRef("a"_sr, "f"_sr), KeyRangeRef("m"_sr, "z"_sr) };
	const auto partitions = nativeCdcOrderedPartitionRanges(ranges, { "c"_sr, "t"_sr });
	ASSERT_EQ(partitions.size(), 3);
	ASSERT(partitions[0] == std::vector<KeyRange>({ KeyRangeRef("a"_sr, "c"_sr) }));
	ASSERT(partitions[1] == std::vector<KeyRange>({ KeyRangeRef("c"_sr, "f"_sr), KeyRangeRef("m"_sr, "t"_sr) }));
	ASSERT(partitions[2] == std::vector<KeyRange>({ KeyRangeRef("t"_sr, "z"_sr) }));
	const auto gapSplit = nativeCdcOrderedPartitionRanges(ranges, { "h"_sr });
	ASSERT(gapSplit[0] == std::vector<KeyRange>({ KeyRangeRef("a"_sr, "f"_sr) }));
	ASSERT(gapSplit[1] == std::vector<KeyRange>({ KeyRangeRef("m"_sr, "z"_sr) }));
	ASSERT(nativeCdcOrderedPartitionRanges(ranges, { "f"_sr }) == gapSplit);
	ASSERT(nativeCdcOrderedPartitionRanges(ranges, { "m"_sr }) == gapSplit);
	ASSERT(nativeCdcOrderedPartitionRanges(ranges, {}).front() == ranges);
	for (const auto& invalidSplits : std::vector<std::vector<Key>>{
	         { "a"_sr }, { "z"_sr }, { "t"_sr, "c"_sr }, { "c"_sr, "c"_sr }, { "f"_sr, "m"_sr } }) {
		expectError([&] { nativeCdcOrderedPartitionRanges(ranges, invalidSplits); },
		            error_code_client_invalid_operation);
	}
	expectError([&] { nativeCdcOrderedPartitionRanges({}, {}); }, error_code_client_invalid_operation);
	expectError(
	    [&] { nativeCdcOrderedPartitionRanges({ KeyRangeRef("a"_sr, "f"_sr), KeyRangeRef("f"_sr, "z"_sr) }, {}); },
	    error_code_client_invalid_operation);
	expectError(
	    [&] { nativeCdcOrderedPartitionRanges(ranges, std::vector<Key>(maxNativeCdcOrderedPartitions, "c"_sr)); },
	    error_code_client_invalid_operation);
	expectError(
	    [&] { nativeCdcOrderedPartitionRanges(std::vector<KeyRange>(maxNativeCdcOrderedRanges + 1, ranges[0]), {}); },
	    error_code_client_invalid_operation);
	std::vector<Key> maximumSplits;
	for (size_t i = 1; i < maxNativeCdcOrderedPartitions; ++i) {
		const uint8_t splitByte = static_cast<uint8_t>(i);
		maximumSplits.emplace_back(KeyRef(&splitByte, 1));
	}
	ASSERT_EQ(nativeCdcOrderedPartitionRanges({ normalKeys }, maximumSplits).size(), maxNativeCdcOrderedPartitions);
	return Void();
}

TEST_CASE("/NativeCDC/OrderedMetadata/Codec") {
	const NativeCdcOrderedMetadata metadata(
	    { KeyRangeRef("a"_sr, "f"_sr), KeyRangeRef("m"_sr, "z"_sr) }, { "c"_sr, "t"_sr }, { 7, 8, 9 });
	const Value encoded = cdcOrderedStreamValue(metadata);
	const auto decoded = decodeCDCOrderedStreamValue(encoded);
	ASSERT(decoded.ranges() == metadata.ranges());
	ASSERT(decoded.splitPoints() == metadata.splitPoints());
	ASSERT(decoded.partitions() == metadata.partitions());
	ASSERT_EQ(encoded.size(), encodedMetadataSize(metadata));
	ASSERT(nonMetadataSystemKeys.contains(cdcOrderedStreamKeyFor(4)));
	ASSERT(nonMetadataSystemKeys.contains(cdcOrderedParentKeyFor(7)));
	ASSERT_EQ(decodeCDCOrderedStreamKey(cdcOrderedStreamKeyFor(4)), 4);
	ASSERT_EQ(decodeCDCOrderedParentKey(cdcOrderedParentKeyFor(7)), 7);
	ASSERT_EQ(decodeCDCOrderedParentValue(cdcOrderedParentValue(4)), 4);
	for (int length = 0; length < encoded.size(); ++length) {
		expectError([&] { decodeCDCOrderedStreamValue(encoded.substr(0, length)); }, error_code_serialization_failed);
	}
	BinaryWriter trailing(Unversioned());
	trailing.serializeBytes(encoded);
	trailing << uint8_t{ 0 };
	expectError([&] { decodeCDCOrderedStreamValue(trailing.toValue()); }, error_code_serialization_failed);
	BinaryWriter unknownSchema(Unversioned());
	unknownSchema << orderedMetadataSchemaVersion + 1;
	unknownSchema.serializeBytes(encoded.substr(sizeof(uint32_t)));
	expectError([&] { decodeCDCOrderedStreamValue(unknownSchema.toValue()); }, error_code_serialization_failed);
	BinaryWriter oversizedCount(Unversioned());
	oversizedCount << orderedMetadataSchemaVersion << std::numeric_limits<uint32_t>::max();
	expectError([&] { decodeCDCOrderedStreamValue(oversizedCount.toValue()); }, error_code_serialization_failed);
	BinaryWriter oversizedKey(Unversioned());
	oversizedKey << orderedMetadataSchemaVersion << uint32_t{ 1 } << std::numeric_limits<uint32_t>::max();
	expectError([&] { decodeCDCOrderedStreamValue(oversizedKey.toValue()); }, error_code_serialization_failed);
	BinaryWriter duplicatePartitions(Unversioned());
	duplicatePartitions.serializeBytes(encoded.substr(0, encoded.size() - 3 * sizeof(CDCStreamId)));
	duplicatePartitions << CDCStreamId{ 7 } << CDCStreamId{ 7 } << CDCStreamId{ 9 };
	expectError([&] { decodeCDCOrderedStreamValue(duplicatePartitions.toValue()); }, error_code_serialization_failed);
	BinaryWriter overlappingRanges(Unversioned());
	overlappingRanges << orderedMetadataSchemaVersion << uint32_t{ 2 };
	for (const auto& key : { "a"_sr, "f"_sr, "c"_sr, "z"_sr }) {
		writeKey(overlappingRanges, key);
	}
	overlappingRanges << uint32_t{ 0 } << uint32_t{ 1 } << CDCStreamId{ 7 };
	expectError([&] { decodeCDCOrderedStreamValue(overlappingRanges.toValue()); }, error_code_serialization_failed);
	BinaryWriter emptyPartition(Unversioned());
	emptyPartition << orderedMetadataSchemaVersion << uint32_t{ 2 };
	for (const auto& key : { "a"_sr, "f"_sr, "m"_sr, "z"_sr }) {
		writeKey(emptyPartition, key);
	}
	emptyPartition << uint32_t{ 2 };
	writeKey(emptyPartition, "f"_sr);
	writeKey(emptyPartition, "m"_sr);
	emptyPartition << uint32_t{ 3 } << CDCStreamId{ 7 } << CDCStreamId{ 8 } << CDCStreamId{ 9 };
	expectError([&] { decodeCDCOrderedStreamValue(emptyPartition.toValue()); }, error_code_serialization_failed);
	expectError([&] { decodeCDCOrderedParentKey(cdcOrderedStreamKeyFor(4)); }, error_code_serialization_failed);
	expectError([&] { decodeCDCOrderedParentValue(cdcOrderedParentValue(4).substr(1)); },
	            error_code_serialization_failed);
	BinaryWriter unknownParentSchema(Unversioned());
	unknownParentSchema << orderedMetadataSchemaVersion + 1 << CDCStreamId{ 4 };
	expectError([&] { decodeCDCOrderedParentValue(unknownParentSchema.toValue()); }, error_code_serialization_failed);
	BinaryWriter zeroParent(Unversioned());
	zeroParent << orderedMetadataSchemaVersion << CDCStreamId{ 0 };
	expectError([&] { decodeCDCOrderedParentValue(zeroParent.toValue()); }, error_code_serialization_failed);
	expectError([&] { NativeCdcOrderedMetadata(metadata.ranges(), metadata.splitPoints(), { 7, 7, 9 }); },
	            error_code_client_invalid_operation);
	expectError([&] { NativeCdcOrderedMetadata(metadata.ranges(), metadata.splitPoints(), { 7, 8 }); },
	            error_code_client_invalid_operation);
	return Void();
}
