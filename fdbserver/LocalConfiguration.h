/*
 * LocalConfiguration.h
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

#include "fdbclient/ConfigKnobs.h"
#include "fdbserver/ConfigFollowerInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/Arena.h"
#include "flow/Knobs.h"

class TestKnobs : public Knobs {
public:
	int64_t TEST;
	void initialize();
};

class LocalConfiguration {
	std::unique_ptr<class LocalConfigurationImpl> impl;

public:
	LocalConfiguration(ConfigClassSet const& configClasses,
	                   std::string const& dataFolder,
	                   std::map<Key, Value>&& manuallyOverriddenKnobs);
	~LocalConfiguration();
	Future<Void> init();
	TestKnobs const &getKnobs() const;
	// TODO: Only one field of serverDBInfo is required, so improve encapsulation
	Future<Void> consume(Reference<AsyncVar<ServerDBInfo> const> const&);
};
