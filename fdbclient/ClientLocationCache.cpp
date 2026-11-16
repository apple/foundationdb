/*
 * ClientLocationCache.cpp
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

#include "fdbclient/ClientLocationCache.h"
#include "flow/CodeProbe.h"

Optional<KeyRangeLocationInfo> ClientLocationCache::get(const KeyRef& key, Reverse isBackward) const {
	Arena arena;

	auto range = isBackward ? cache.rangeContainingKeyBefore(key) : cache.rangeContaining(key);
	if (range->value()) {
		return KeyRangeLocationInfo(range->range(), range->value());
	}

	return Optional<KeyRangeLocationInfo>();
}

bool ClientLocationCache::getRange(const KeyRangeRef& range,
                                   std::vector<KeyRangeLocationInfo>& result,
                                   int limit,
                                   Reverse reverse) const {
	result.clear();

	Arena arena;

	auto begin = cache.rangeContaining(range.begin);
	auto end = cache.rangeContainingKeyBefore(range.end);

	while (true) {
		auto r = reverse ? end : begin;
		if (!r->value()) {
			CODE_PROBE(!result.empty(), "had some but not all cached locations");
			result.clear();
			return false;
		}
		result.emplace_back((r->range() & range), r->value());
		if (result.size() == limit || begin == end) {
			break;
		}

		if (reverse)
			--end;
		else
			++begin;
	}

	return true;
}

void ClientLocationCache::insert(const KeyRangeRef& range, const Reference<LocationInfo>& location) {
	int maxEvictionAttempts = 100, attempts = 0;
	while (cache.size() > sizeLimit && attempts < maxEvictionAttempts) {
		CODE_PROBE(true, "NativeAPI storage server locationCache entry evicted");
		attempts++;
		auto r = cache.randomRange();
		Key begin = r.begin(), end = r.end(); // insert invalidates r, so can't be passed a mere reference into it
		cache.insert(KeyRangeRef(begin, end), Reference<LocationInfo>());
	}
	cache.insert(range, location);
}

void ClientLocationCache::invalidate(const KeyRef& key, Reverse isBackward) {
	Arena arena;
	KeyRef resolvedKey = key;

	if (isBackward) {
		cache.rangeContainingKeyBefore(resolvedKey)->value() = Reference<LocationInfo>();
	} else {
		cache.rangeContaining(resolvedKey)->value() = Reference<LocationInfo>();
	}
}

void ClientLocationCache::invalidateIntersecting(const KeyRangeRef& keys) {
	Arena arena;

	auto rs = cache.intersectingRanges(keys);
	Key begin = rs.begin().begin(),
	    end = rs.end().begin(); // insert invalidates rs, so can't be passed a mere reference into it
	cache.insert(KeyRangeRef(begin, end), Reference<LocationInfo>());
}

void ClientLocationCache::invalidateExact(const KeyRangeRef& range) {
	cache.insert(range, Reference<LocationInfo>());
}

void ClientLocationCache::clear() {
	cache.insert(allKeys, Reference<LocationInfo>());
}

ClientLocationCache::AddressScanResult ClientLocationCache::scanAddresses(
    const std::unordered_set<NetworkAddress>& addresses,
    Key& cursor,
    int rangeLimit,
    std::vector<KeyRange>& matches) const {
	auto iter = cache.rangeContaining(cursor);
	auto endIter = cache.ranges().end();
	bool rangesRemaining = false;
	int currRangesScanned = 0;
	for (; iter != endIter; ++iter) {
		if (currRangesScanned >= rangeLimit) {
			rangesRemaining = true;
			break;
		}
		++currRangesScanned;
		cursor = iter->end();
		if (!iter->value()) {
			continue;
		}
		auto& loc = iter->value();
		for (int i = 0; i < loc->size(); ++i) {
			if (addresses.contains(loc->getInterface(i).address())) {
				matches.push_back(KeyRange(KeyRangeRef(iter->begin(), iter->end())));
				break;
			}
		}
	}
	return { currRangesScanned, rangesRemaining };
}
