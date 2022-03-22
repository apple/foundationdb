/*
 * SimpleConfigTransaction.h
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

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/ConfigTransactionInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IConfigTransaction.h"
#include "fdbclient/PImpl.h"
#include "flow/Error.h"
#include "flow/flow.h"

/*
 * A configuration transaction implementation that interacts with a simple (test-only) implementation of
 * the configuration database. All configuration database data is assumed to live on a single node
 * (the lowest coordinator by IP address), so there is no fault tolerance.
 */
class SimpleConfigTransaction final : public IConfigTransaction, public FastAllocated<SimpleConfigTransaction> {
	PImpl<class SimpleConfigTransactionImpl> impl;

public:
	SimpleConfigTransaction(ConfigTransactionInterface const&);
	SimpleConfigTransaction(Database const&);
	SimpleConfigTransaction();
	void construct(Database const&) override;
	~SimpleConfigTransaction();
	Future<Version> getReadVersion() override;
	Optional<Version> getCachedReadVersion() const override;

	Future<Optional<Value>> get(Key const& key, Snapshot = Snapshot::False) override;
	Future<RangeResult> getRange(KeySelector const& begin,
	                             KeySelector const& end,
	                             int limit,
	                             Snapshot = Snapshot::False,
	                             Reverse = Reverse::False) override;
	Future<RangeResult> getRange(KeySelector begin,
	                             KeySelector end,
	                             GetRangeLimits limits,
	                             Snapshot = Snapshot::False,
	                             Reverse = Reverse::False) override;
	Future<MappedRangeResult> getMappedRange(KeySelector begin,
	                                         KeySelector end,
	                                         Key mapper,
	                                         GetRangeLimits limits,
	                                         Snapshot = Snapshot::False,
	                                         Reverse = Reverse::False) override {
		throw client_invalid_operation();
	}
	Future<Void> commit() override;
	Version getCommittedVersion() const override;
	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	Future<Void> onError(Error const& e) override;
	void cancel() override;
	void reset() override;
	void debugTransaction(UID dID) override;
	void checkDeferredError() const override;
	int64_t getApproximateSize() const override;
	void set(KeyRef const&, ValueRef const&) override;
	void clear(KeyRangeRef const&) override { throw client_invalid_operation(); }
	void clear(KeyRef const&) override;

	void fullReset();
};
