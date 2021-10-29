/*
 * ConfigNode.h
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

#include <string>

#include "fdbclient/ConfigTransactionInterface.h"
#include "fdbserver/ConfigFollowerInterface.h"

class ConfigNode : public ReferenceCounted<ConfigNode> {
	std::unique_ptr<class ConfigNodeImpl> _impl;
	ConfigNodeImpl const& impl() const { return *_impl; }
	ConfigNodeImpl& impl() { return *_impl; }

public:
	ConfigNode(std::string const& folder);
	~ConfigNode();
	Future<Void> serve(ConfigTransactionInterface const&);
	Future<Void> serve(ConfigFollowerInterface const&);
};
