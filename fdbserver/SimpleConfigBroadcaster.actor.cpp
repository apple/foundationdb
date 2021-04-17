/*
 * SimpleConfigBroadcaster.actor.cpp
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

#include "fdbserver/IConfigBroadcaster.h"

class SimpleConfigBroadcasterImpl {
	ConfigFollowerInterface subscriber;

public:
	SimpleConfigBroadcasterImpl(ClusterConnectionString const& ccs) {
		auto coordinators = ccs.coordinators();
		std::sort(coordinators.begin(), coordinators.end());
		subscriber = ConfigFollowerInterface(coordinators[0]);
	}

	Future<Void> serve(ConfigFollowerInterface& publisher) {
		// TODO: Implement
		wait(Never());
		return Void();
	}
};

SimpleConfigBroadcaster::SimpleConfigBroadcaster(ClusterConnectionString const& ccs)
  : impl(std::make_unique<SimpleConfigBroadcasterImpl>(ccs)) {}

SimpleConfigBroadcaster::~SimpleConfigBroadcaster() = default;

Future<Void> SimpleConfigBroadcaster::serve(ConfigFollowerInterface& publisher) {
	return impl->serve(publisher);
}
