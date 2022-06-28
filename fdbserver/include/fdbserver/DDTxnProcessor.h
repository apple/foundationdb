/*
 * DDTxnProcessor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#ifndef FOUNDATIONDB_DDTXNPROCESSOR_H
#define FOUNDATIONDB_DDTXNPROCESSOR_H

#include "fdbserver/Knobs.h"
#include "fdbserver/DataDistribution.actor.h"

class IDDTxnProcessor {
public:
	struct SourceServers {
		std::vector<UID> srcServers, completeSources; // the same as RelocateData.src, RelocateData.completeSources;
	};
	// get the source server list and complete source server list for range
	virtual Future<SourceServers> getSourceServersForRange(const KeyRangeRef range) = 0;

	// get the storage server list and Process class
	virtual Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> getServerListAndProcessClasses() = 0;

	virtual ~IDDTxnProcessor() = default;
};

class DDTxnProcessorImpl;

// run transactions over real database
class DDTxnProcessor : public IDDTxnProcessor {
	friend class DDTxnProcessorImpl;
	Database cx;

public:
	DDTxnProcessor() = default;
	explicit DDTxnProcessor(Database cx) : cx(cx) {}

	Future<SourceServers> getSourceServersForRange(const KeyRangeRef range) override;

	// Call NativeAPI implementation directly
	Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> getServerListAndProcessClasses() override;
};

// run mock transaction
class DDMockTxnProcessor : public IDDTxnProcessor {};

#endif // FOUNDATIONDB_DDTXNPROCESSOR_H
