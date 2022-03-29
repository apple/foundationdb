/*
 * IConfigConsumer.h
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

#include "fdbclient/CoordinationInterface.h"
#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/ConfigFollowerInterface.h"
#include "flow/flow.h"
#include <memory>

/*
 * An IConfigConsumer instantiation is used by the ConfigBroadcaster to consume
 * updates from the configuration database nodes. A consumer sends mutations to a broadcaster
 * once they are confirmed to be committed.
 */
class IConfigConsumer {
public:
	virtual ~IConfigConsumer() = default;
	virtual Future<Void> consume(ConfigBroadcaster& broadcaster) = 0;
	virtual UID getID() const = 0;

	static std::unique_ptr<IConfigConsumer> createTestSimple(ConfigFollowerInterface const& cfi,
	                                                         double pollingInterval,
	                                                         Optional<double> compactionInterval);
	static std::unique_ptr<IConfigConsumer> createSimple(ServerCoordinators const& coordinators,
	                                                     double pollingInterval,
	                                                     Optional<double> compactionInterval);
	static std::unique_ptr<IConfigConsumer> createPaxos(ServerCoordinators const& coordinators,
	                                                    double pollingInterval,
	                                                    Optional<double> compactionInterval);
};
