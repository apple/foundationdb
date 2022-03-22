/*
 * PaxosConfigConsumer.h
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

#include "fdbclient/PImpl.h"
#include "fdbserver/IConfigConsumer.h"

/*
 * A fault-tolerant configuration database consumer implementation
 */
class PaxosConfigConsumer : public IConfigConsumer {
	PImpl<class PaxosConfigConsumerImpl> impl;

public:
	PaxosConfigConsumer(ServerCoordinators const& coordinators,
	                    double pollingInterval,
	                    Optional<double> compactionInterval);
	~PaxosConfigConsumer();
	Future<Void> consume(ConfigBroadcaster& broadcaster) override;
	UID getID() const override;

public: // Testing
	PaxosConfigConsumer(std::vector<ConfigFollowerInterface> const& cfis,
	                    double pollingInterval,
	                    Optional<double> compactionInterval);
};
