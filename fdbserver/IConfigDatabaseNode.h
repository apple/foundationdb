/*
 * IConfigDatabaseNode.h
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

#include "fdbclient/ConfigTransactionInterface.h"
#include "fdbserver/ConfigFollowerInterface.h"
#include "flow/FastRef.h"
#include "flow/flow.h"

#include <memory>

class IConfigDatabaseNode : public ReferenceCounted<IConfigDatabaseNode> {
public:
	virtual Future<Void> serve(ConfigTransactionInterface&) = 0;
	virtual Future<Void> serve(ConfigFollowerInterface&) = 0;
	virtual Future<Void> initialize(std::string const& dataFolder, UID id) = 0;
};

class SimpleConfigDatabaseNode : public IConfigDatabaseNode {
	std::unique_ptr<class SimpleConfigDatabaseNodeImpl> impl;

public:
	SimpleConfigDatabaseNode();
	~SimpleConfigDatabaseNode();
	Future<Void> serve(ConfigTransactionInterface&) override;
	Future<Void> serve(ConfigFollowerInterface&) override;
	Future<Void> initialize(std::string const& dataFolder, UID id) override;
};
