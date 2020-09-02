/**
 * TimedKVCache.h
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

#ifndef FDBSERVER_TIMED_KVCACHE_H
#define FDBSERVER_TIMED_KVCACHE_H

#pragma once

#include <list>
#include <memory>
#include <unordered_map>

#include "flow/flow.h"
#include "flow/Error.h"

/**
 * @class TimedKVCache
 * @brief A key/value based cache that expires its key after a given time
 */
template <typename K, typename V>
class TimedKVCache {
public:
	using key_t = K;
	using value_t = V;

	const double CACHE_EXPIRING_TIME_LENGTH;

	explicit TimedKVCache(const double& expiringTimeLength) : CACHE_EXPIRING_TIME_LENGTH(expiringTimeLength) {}

	/**
	 * Add a new key/value pair to the cache
	 * @param key
	 * @param value
	 */
	void add(const key_t& key, const value_t& value) {
		double now = g_network->now();
		timestampedKey.emplace_back(std::make_pair(now, key));
		kvMapper[key] = value;
	}

	/**
	 * Add a new key/value pair to the cache
	 * @param key
	 * @param value
	 */
	void add(const key_t& key, value_t&& value) {
		double now = g_network->now();
		timestampedKey.emplace_back(std::make_pair(now, key));
		kvMapper.emplace(key, std::move(value));
	}

	/**
	 * Remove a key from the cache
	 * @param key
	 */
	void erase(const key_t& key) {
		// We do not erase the key in the time range as it would be done by sweep
		kvMapper.erase(key);
	}

	/**
	 * Check if a given key exists, remove the expired keys
	 * @param key
	 * @return bool
	 */
	bool exists(const key_t& key) {
		sweep();
		return kvMapper.find(key) != kvMapper.end();
	}

	/**
	 * Get the value of a key, use exists to check if it is expired/removed first
	 * @param key
	 * @return value of the key
	 */
	value_t& get(const key_t& key) {
		try {
			return kvMapper.at(key);
		} catch (std::out_of_range&) {
			// Translate std error to FDB error
			ASSERT(exists(key));
			// This throw is to mute the control reaches end of non-void function warning
			throw;
		}
	}

	const value_t& get(const key_t& key) const { return const_cast<TimedKVCache*>(this)->get(key); }

private:
	std::list<std::pair<double, key_t>> timestampedKey;
	std::unordered_map<key_t, value_t> kvMapper;

protected:
	void sweep() {
		auto now = g_network->now();
		while (!timestampedKey.empty() && (now - timestampedKey.front().first) > CACHE_EXPIRING_TIME_LENGTH) {
			const auto& key = timestampedKey.front().second;
			kvMapper.erase(key);
			timestampedKey.pop_front();
		}
	}
};

#endif // FDBSERVER_TIMED_KVCACHE_H
