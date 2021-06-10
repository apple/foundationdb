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
#include "fdbclient/IKnobCollection.h"
#include "fdbserver/ConfigBroadcastFollowerInterface.h"
#include "fdbserver/Knobs.h"
#include "flow/Arena.h"
#include "flow/Knobs.h"

enum class IsTest { NO, YES };

class LocalConfiguration {
	std::unique_ptr<class LocalConfigurationImpl> impl;

public:
	LocalConfiguration(std::string const& dataFolder,
	                   std::string const& configPath,
	                   std::map<std::string, std::string> const& manualKnobOverrides,
	                   IsTest isTest = IsTest::NO);
	LocalConfiguration(LocalConfiguration&&);
	LocalConfiguration& operator=(LocalConfiguration&&);
	~LocalConfiguration();
	Future<Void> initialize();
	FlowKnobs const& getFlowKnobs() const;
	ClientKnobs const& getClientKnobs() const;
	ServerKnobs const& getServerKnobs() const;
	TestKnobs const& getTestKnobs() const;
	Future<Void> consume(Reference<IDependentAsyncVar<ConfigBroadcastFollowerInterface> const> const& broadcaster);
	UID getID() const;

public: // Testing
	Future<Void> addChanges(Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations,
	                        Version mostRecentVersion);
};
