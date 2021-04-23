/*
 * IConfigTransaction.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
#include "flow/flow.h"

class IConfigTransaction {
public:
	virtual void set(KeyRef key, ValueRef value) = 0;
	virtual void clearRange(KeyRef begin, KeyRef end) = 0;
	virtual Future<Optional<Value>> get(KeyRef) = 0;
	virtual Future<Standalone<RangeResultRef>> getRange(KeyRangeRef) = 0;
	virtual Future<Void> commit() = 0;
	virtual Future<Void> onError(Error const&) = 0;
	virtual Future<Version> getVersion() = 0;
	virtual void reset() = 0;
	virtual void fullReset() = 0;
};

class SimpleConfigTransaction : public IConfigTransaction {
	std::unique_ptr<class SimpleConfigTransactionImpl> impl;

public:
	SimpleConfigTransaction(ClusterConnectionString const&);
	~SimpleConfigTransaction();
	void set(KeyRef begin, KeyRef end) override;
	void clearRange(KeyRef begin, KeyRef end) override;
	Future<Optional<Value>> get(KeyRef) override;
	Future<Standalone<RangeResultRef>> getRange(KeyRangeRef) override;
	Future<Void> commit() override;
	Future<Void> onError(Error const&) override;
	Future<Version> getVersion() override;
	void reset() override;
	void fullReset() override;
};
