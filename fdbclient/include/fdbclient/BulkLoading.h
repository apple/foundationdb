/*
 * BulkLoading.h
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

#ifndef FDBCLIENT_BULKLOADING_H
#define FDBCLIENT_BULKLOADING_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

enum class BulkLoadPhase : uint8_t {
	Invalid = 0,
	Running = 1,
	Complete = 2,
	Error = 3,
};

enum class BulkLoadType : uint8_t {
	Invalid = 0,
	SQLite = 1,
	RocksDB = 2,
	ShardedRocksDB = 3,
};

struct BulkLoadState {
	constexpr static FileIdentifier file_identifier = 1384499;

	BulkLoadState() = default;

	BulkLoadState(BulkLoadType loadType, std::set<std::string> filePaths) : loadType(loadType), filePaths(filePaths) {}

	BulkLoadState(KeyRange range, BulkLoadType loadType, std::set<std::string> filePaths)
	  : range(range), loadType(loadType), filePaths(filePaths) {}

	bool isValid() const { return loadType != BulkLoadType::Invalid; }

	std::string toString() const {
		return "BulkLoadState: [Range]: " + Traceable<KeyRangeRef>::toString(range) +
		       ", [Type]: " + std::to_string(static_cast<uint8_t>(loadType)) + ", [FilePath]: " + describe(filePaths);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, loadType, filePaths);
	}

	KeyRange range;
	BulkLoadType loadType;
	std::set<std::string> filePaths;
};

#endif
