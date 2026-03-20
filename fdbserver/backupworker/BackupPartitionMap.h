/*
 * BackupPartitionMap.h
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

#include "fdbclient/FDBTypes.h"
#include "fdbserver/core/ShardMetrics.h"
#include <unordered_map>
#include <vector>

struct Partition {
	int32_t partitionId;
	KeyRange ranges;

	Partition() : partitionId(-1) {}
	Partition(int32_t id, KeyRange r) : partitionId(id), ranges(r) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, partitionId, ranges);
	}
};

typedef std::vector<Partition> PartitionList;
typedef std::unordered_map<Tag, PartitionList> PartitionMap;

std::string serializePartitionListJSON(PartitionMap const& PartitionMap);
