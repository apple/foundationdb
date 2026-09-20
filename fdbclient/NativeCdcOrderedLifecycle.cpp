/*
 * NativeCdcOrderedLifecycle.cpp
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
#include <limits>
#include <set>
#include <utility>
#include <vector>

#include "NativeCdcOrderedLifecycle.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/SystemData.h"
#include "flow/CodeProbe.h"
#include "flow/Trace.h"
#include "flow/serialize.h"

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
		if (child == logicalId) {
			throw serialization_failed();
		}
		rangeReads.push_back(tr->get(cdcStreamKeyFor(child)));
		parentReads.push_back(tr->get(cdcOrderedParentKeyFor(child)));
		minimumReads.push_back(tr->get(cdcMinVersionKeyFor(child)));
	}
	Version commonMinVersion = invalidVersion;
	for (size_t i = 0; i < metadata.partitions().size(); ++i) {
		const Optional<Value> ranges = co_await rangeReads[i];
		const Optional<Value> parent = co_await parentReads[i];
		const Optional<Value> minimum = co_await minimumReads[i];
		if (!ranges.present() || !parent.present() || !minimum.present() ||
		    ranges.get() != cdcStreamKeysValue(partitionRanges[i]) ||
		    decodeCDCOrderedParentValue(parent.get()) != logicalId) {
			throw serialization_failed();
		}
		const Version minVersion = readMinVersion(minimum.get());
		if (commonMinVersion != invalidVersion && minVersion != commonMinVersion) {
			throw serialization_failed();
		}
		commonMinVersion = minVersion;
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
