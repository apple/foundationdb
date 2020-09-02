/**
 * PartMerger.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_PARTMERGER_H
#define FDBSERVER_PARTMERGER_H

#pragma once

#include <algorithm>
#include <vector>

#include "fdbserver/TimedKVCache.h"

template <typename PART>
class PartMerger {
public:
	using value_t = PART;

	PartMerger(const int totalParts) : partsIncluded(totalParts, false) {}
	PartMerger(const int totalParts, const value_t& initialPart)
	  : partsIncluded(totalParts, false), merged(initialPart) {}

	/**
	 * Insert part of the value of a given key
	 * @param partIndex the index of the part
	 * @param v Part of the value, must be copy-constructable
	 */
	void insert(const int partIndex, const value_t& v) {
		if (!partsIncluded[partIndex]) {
			if (!firstPartReceived) {
				mergeFirstPart(v);
				firstPartReceived = true;
			} else {
				merge(v);
			}

			partsIncluded[partIndex] = true;
		}
	}

	/**
	 * Return if the value is ready-to-use
	 * @return bool
	 */
	bool ready() const { return checkIsReady(); }

	/**
	 * Return if all parts received
	 * @return bool
	 */
	bool allPartsReceived() const {
		return std::all_of(partsIncluded.begin(), partsIncluded.end(), [](bool v) { return v; });
	}

	/**
	 * Return the merged value for a given key
	 * @warning get will NOT check if the data is ready/complete
	 * @return value
	 */
	const value_t& get() const { return merged; }

protected:
	/**
	 * Merge the first part
	 */
	virtual void mergeFirstPart(const value_t& part) {
		// By default, merging the first part will just merge it to a trivially initialized/pre-initialized empty trunk
		merge(part);
	}
	/**
	 * Merge a new part
	 */
	virtual void merge(const value_t&) = 0;

	/**
	 * Check if the value of key is ready to use. By default it just verifies if all the parts are inserted; otherwise
	 * override this function.
	 * @return bool
	 */
	virtual bool checkIsReady() const {
		// By default, it is ready only if all parts are received, however, it is possible to override in case not all
		// parts are needed.
		return allPartsReceived();
	}

	value_t merged;

private:
	bool firstPartReceived = false;
	std::vector<bool> partsIncluded;
};

template <typename K, typename Merger>
class TimedMergingKVCache : private TimedKVCache<K, Merger> {
	using base = TimedKVCache<K, Merger>;

public:
	using base::TimedKVCache;

	using key_t = typename base::key_t;
	using value_t = typename Merger::value_t;

	/**
	 * Add a part into the cache
	 * @param k
	 * @param part
	 * @param partIndex
	 * @param totalParts
	 */
	void add(const key_t& k, const value_t& part, const int partIndex, const int totalParts) {
		if (!exists(k)) {
			base::add(k, Merger(totalParts));
		}
		get(k).insert(partIndex, part);
	}

	using base::exists;
	using base::get;
};

#endif // FDBSERVER_PARTMERGER_H
