/*
 * ReadLatencySamples.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include <memory>

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Stats.h"

class ReadLatencySamples {
public:
	enum SampleType {
		READ,
		READ_KEY,
		READ_VALUE,
		READ_RANGE,
		READ_VERSION_WAIT,
		READ_QUEUE_WAIT,
		KV_READ_RANGE,
		MAPPED_RANGE,
		MAPPED_RANGE_REMOTE,
		MAPPED_RANGE_LOCAL,
		END,
	};

private:
	struct Entry {
		std::array<std::unique_ptr<LatencySample>, SampleType::END> samples;
		Entry(std::string_view prefix, UID serverId);
	};

	Entry aggregate;
	std::array<Entry, ReadType::MAX + 1> perType;

public:
	explicit ReadLatencySamples(UID serverId);

	void sample(double latency, SampleType, Optional<ReadType> = {});
};
