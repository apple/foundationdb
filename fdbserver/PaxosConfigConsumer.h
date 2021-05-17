/*
 * PaxosConfigConsumer.h
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

#include "fdbserver/IConfigConsumer.h"

class PaxosConfigConsumer : public IConfigConsumer {
	std::unique_ptr<class PaxosConfigConsumer> impl;

public:
	PaxosConfigConsumer(ConfigFollowerInterface const& cfi,
	                    Optional<double> const& pollingInterval,
	                    Optional<double> const& compactionInterval,
	                    UID id);
	~PaxosConfigConsumer();
	Future<Void> getInitialSnapshot(ConfigBroadcaster& broadcaster) override;
	Future<Void> consume(ConfigBroadcaster& broadcaster) override;
};
