/*
 * ClientLocationCache.h
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

#pragma once

#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbrpc/MultiInterface.h"
#include "flow/FastAlloc.h"

#include <unordered_set>
#include <vector>

struct LocationInfo : MultiInterface<ReferencedInterface<StorageServerInterface>>, FastAllocated<LocationInfo> {
	using Locations = MultiInterface<ReferencedInterface<StorageServerInterface>>;
	explicit LocationInfo(const std::vector<Reference<ReferencedInterface<StorageServerInterface>>>& v)
	  : Locations(v) {}
	LocationInfo(const LocationInfo&) = delete;
	LocationInfo(LocationInfo&&) = delete;
	LocationInfo& operator=(const LocationInfo&) = delete;
	LocationInfo& operator=(LocationInfo&&) = delete;
	Reference<Locations> locations() { return Reference<Locations>::addRef(this); }
};

struct KeyRangeLocationInfo {
	KeyRange range;
	Reference<LocationInfo> locations;

	KeyRangeLocationInfo() = default;
	KeyRangeLocationInfo(KeyRange range, Reference<LocationInfo> locations) : range(range), locations(locations) {}
};

class ClientLocationCache : NonCopyable {
public:
	Optional<KeyRangeLocationInfo> get(const KeyRef& key, Reverse isBackward) const;
	bool getRange(const KeyRangeRef& range,
	              std::vector<KeyRangeLocationInfo>& result,
	              int limit,
	              Reverse reverse) const;
	void insert(const KeyRangeRef& range, const Reference<LocationInfo>& location);

	// Point invalidation retains range boundaries; range invalidation removes whole intersecting entries.
	void invalidate(const KeyRef& key, Reverse isBackward);
	void invalidateIntersecting(const KeyRangeRef& range);
	// Preserve the supplied boundaries even if the cache has changed since the range was selected.
	void invalidateExact(const KeyRangeRef& range);
	void clear();

	// Empty ranges count toward the size and eviction threshold.
	int size() const { return cache.size(); }
	int capacity() const { return sizeLimit; }
	// Eviction is attempted before insertion; changing the threshold does not evict immediately.
	void setCapacity(int capacity) { sizeLimit = capacity; }

	struct AddressScanResult {
		int rangesScanned;
		bool more;
	};
	// Count empty ranges toward the budget and advance the owning cursor past every scanned range.
	AddressScanResult scanAddresses(const std::unordered_set<NetworkAddress>& addresses,
	                                Key& cursor,
	                                int rangeLimit,
	                                std::vector<KeyRange>& matches) const;

private:
	int sizeLimit = 0;
	CoalescedKeyRangeMap<Reference<LocationInfo>> cache;
};
