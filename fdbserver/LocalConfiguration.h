/*
 * LocalConfiguration.h
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

#include <string>

#include "fdbclient/ConfigKnobs.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/PImpl.h"
#include "fdbserver/ConfigBroadcastInterface.h"
#include "fdbserver/Knobs.h"
#include "flow/Arena.h"
#include "flow/Knobs.h"

FDB_DECLARE_BOOLEAN_PARAM(IsTest);

/*
 * Each worker maintains a LocalConfiguration object used to update its knob collection.
 * When a worker starts, the following steps are executed:
 *    - Apply manual knob updates
 *    - Read the local configuration file (with "localconf" prefix)
 *      - If the stored configuration path does not match the current configuration path, delete the local configuration
 * file
 *      - Otherwise, apply knob updates from the local configuration file (without overriding manual knob overrides)
 *    - Register with the broadcaster to receive new updates for the relevant configuration classes
 *      - Persist these updates when received, and restart if necessary
 */
class LocalConfiguration : public ReferenceCounted<LocalConfiguration> {
	PImpl<class LocalConfigurationImpl> impl;

public:
	LocalConfiguration(std::string const& dataFolder,
	                   std::string const& configPath,
	                   std::map<std::string, std::string> const& manualKnobOverrides,
	                   IsTest = IsTest::False);
	~LocalConfiguration();
	FlowKnobs const& getFlowKnobs() const;
	ClientKnobs const& getClientKnobs() const;
	ServerKnobs const& getServerKnobs() const;
	TestKnobs const& getTestKnobs() const;
	Future<Void> consume(ConfigBroadcastInterface const& broadcastInterface);
	UID getID() const;
	Version lastSeenVersion() const;
	ConfigClassSet configClassSet() const;
	Future<Void> initialize();

public: // Testing
	Future<Void> addChanges(Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations,
	                        Version mostRecentVersion);
	void close();
	Future<Void> onClosed();
};
