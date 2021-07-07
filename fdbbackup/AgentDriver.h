/*
 * AgentDriver.h
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

#include "fdbclient/ReadYourWrites.h"
#include "flow/flow.h"
#include "flow/SimpleOpt.h"

#include <string>

enum class AgentType {
	FILE,
	DB,
};

// TODO: Add comment here
Future<Void> statusUpdateActor(Database statusUpdateDest,
                               std::string const& name,
                               AgentType,
                               double& pollDelay,
                               Database taskDest = Database(),
                               std::string const& id = nondeterministicRandom()->randomUniqueID().toString());

Optional<Database> initCluster(std::string const& clusterFile, LocalityData const& localities, bool isQuiet);
