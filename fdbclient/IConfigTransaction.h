/*
 * IConfigTransaction.h
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

#pragma once

#include <memory>

#include "fdbclient/ConfigKnobs.h"
#include "fdbclient/ConfigTransactionInterface.h"
#include "fdbclient/ISingleThreadTransaction.h"
#include "fdbclient/NativeAPI.actor.h"

/*
 * Configuration transactions are used by clients to update the configuration database, in order
 * to dynamically update knobs. The interface is similar to that of regular transactions, but simpler, and
 * many virtual methods of ISingleThreadTransaction are disallowed here
 */
class IConfigTransaction : public ISingleThreadTransaction {
protected:
	IConfigTransaction() = default;

public:
	virtual ~IConfigTransaction() = default;

	static Reference<IConfigTransaction> createTestSimple(ConfigTransactionInterface const&);
	static Reference<IConfigTransaction> createTestPaxos(std::vector<ConfigTransactionInterface> const&);

	// Not implemented:
	void setVersion(Version) override { throw client_invalid_operation(); }
	VersionVector getVersionVector() const override { throw client_invalid_operation(); }
	UID getSpanID() const override { throw client_invalid_operation(); }
	Future<Key> getKey(KeySelector const& key, Snapshot snapshot = Snapshot::False) override {
		throw client_invalid_operation();
	}
	Future<Standalone<VectorRef<const char*>>> getAddressesForKey(Key const& key) override {
		throw client_invalid_operation();
	}
	Future<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(KeyRange const& range, int64_t chunkSize) override {
		throw client_invalid_operation();
	}
	Future<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRanges(KeyRange const& range) override {
		throw client_invalid_operation();
	}
	Future<Standalone<VectorRef<BlobGranuleChunkRef>>> readBlobGranules(KeyRange const& range,
	                                                                    Version begin,
	                                                                    Optional<Version> readVersion,
	                                                                    Version* readVersionOut) override {
		throw client_invalid_operation();
	}
	Future<int64_t> getEstimatedRangeSizeBytes(KeyRange const& keys) override { throw client_invalid_operation(); }
	void addReadConflictRange(KeyRangeRef const& keys) override { throw client_invalid_operation(); }
	void makeSelfConflicting() override { throw client_invalid_operation(); }
	void atomicOp(KeyRef const& key, ValueRef const& operand, uint32_t operationType) override {
		throw client_invalid_operation();
	}
	Future<Void> watch(Key const& key) override { throw client_invalid_operation(); }
	void addWriteConflictRange(KeyRangeRef const& keys) override { throw client_invalid_operation(); }
	Future<Standalone<StringRef>> getVersionstamp() override { throw client_invalid_operation(); }

	// Implemented:
	void getWriteConflicts(KeyRangeMap<bool>* result) override{};
};
