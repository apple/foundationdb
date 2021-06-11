/*
 * SimpleConfigDatabaseNode.h
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

#include "fdbserver/IConfigDatabaseNode.h"

/*
 * A test-only configuration database node implementation that assumes all data is stored on a single coordinator.
 * As such, there is no need to handle rolling forward or rolling back mutations, because this one node is considered
 * the source of truth.
 */
class SimpleConfigDatabaseNode : public IConfigDatabaseNode {
	std::unique_ptr<class SimpleConfigDatabaseNodeImpl> _impl;
	SimpleConfigDatabaseNodeImpl const& impl() const { return *_impl; }
	SimpleConfigDatabaseNodeImpl& impl() { return *_impl; }

public:
	SimpleConfigDatabaseNode(std::string const& folder);
	~SimpleConfigDatabaseNode();
	Future<Void> serve(ConfigTransactionInterface const&) override;
	Future<Void> serve(ConfigFollowerInterface const&) override;
};
