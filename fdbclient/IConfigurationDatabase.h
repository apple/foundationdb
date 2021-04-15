/*
 * IConfigurationDatabase.h
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
#include "fdbclient/ConfigDatabaseInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "flow/flow.h"

class IConfigurationTransaction {
public:
	virtual void set(KeyRef key, ValueRef value) = 0;
	virtual void clearRange(KeyRef begin, KeyRef end) = 0;
	virtual Future<Optional<Value>> get(KeyRef) = 0;
	virtual Future<Void> commit() = 0;
	virtual Future<Void> onError(Error const&) = 0;
};

class SimpleConfigurationTransaction : public IConfigurationTransaction {
	std::unique_ptr<class SimpleConfigurationTransactionImpl> impl;

public:
	SimpleConfigurationTransaction(ClusterConnectionString const&);
	~SimpleConfigurationTransaction();
	void set(KeyRef begin, KeyRef end) override;
	void clearRange(KeyRef begin, KeyRef end) override;
	Future<Optional<Value>> get(KeyRef) override;
	Future<Void> commit() override;
	Future<Void> onError(Error const&) override;
};
