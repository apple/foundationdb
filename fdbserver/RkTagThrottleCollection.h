/*
 * RkTagThrottleCollection.h
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
 *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "fdbclient/Knobs.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbrpc/Smoother.h"

class RkTagThrottleCollection : NonCopyable {
	struct RkTagData {
		Smoother requestRate;
		RkTagData() : requestRate(CLIENT_KNOBS->TAG_THROTTLE_SMOOTHING_WINDOW) {}
	};

	struct RkTagThrottleData {
		ClientTagThrottleLimits limits;
		Smoother clientRate;

		// Only used by auto-throttles
		double created = now();
		double lastUpdated = 0;
		double lastReduced = now();
		bool rateSet = false;

		RkTagThrottleData() : clientRate(CLIENT_KNOBS->TAG_THROTTLE_SMOOTHING_WINDOW) {}
		double getTargetRate(Optional<double> requestRate);
		Optional<double> updateAndGetClientRate(Optional<double> requestRate);
	};

	TransactionTagMap<RkTagThrottleData> autoThrottledTags;
	TransactionTagMap<std::map<TransactionPriority, RkTagThrottleData>> manualThrottledTags;
	TransactionTagMap<RkTagData> tagData;
	uint32_t busyReadTagCount = 0, busyWriteTagCount = 0;

	void initializeTag(TransactionTag const& tag) { tagData.try_emplace(tag); }
	static double computeTargetTpsRate(double currentBusyness, double targetBusyness, double requestRate);
	Optional<double> getRequestRate(TransactionTag const& tag);

public:
	RkTagThrottleCollection() = default;
	RkTagThrottleCollection(RkTagThrottleCollection&& other);
	RkTagThrottleCollection& operator=(RkTagThrottleCollection&& other);

	// Set or update an auto throttling limit for the specified tag and priority combination.
	// Returns the TPS rate if the throttle is updated, otherwise returns an empty optional
	Optional<double> autoThrottleTag(UID id,
	                                 TransactionTag const& tag,
	                                 double fractionalBusyness,
	                                 Optional<double> tpsRate = Optional<double>(),
	                                 Optional<double> expiration = Optional<double>());

	// Set or update a manual tps rate limit for the specified tag and priority combination
	void manualThrottleTag(UID id,
	                       TransactionTag const& tag,
	                       TransactionPriority priority,
	                       double tpsRate,
	                       double expiration,
	                       Optional<ClientTagThrottleLimits> const& oldLimits);

	Optional<ClientTagThrottleLimits> getManualTagThrottleLimits(TransactionTag const& tag,
	                                                             TransactionPriority priority);

	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates(bool autoThrottlingEnabled);
	void addRequests(TransactionTag const& tag, int requests);
	int64_t autoThrottleCount() const { return autoThrottledTags.size(); }
	int64_t manualThrottleCount() const;
	void updateBusyTagCount(TagThrottledReason);
	auto getBusyReadTagCount() const { return busyReadTagCount; }
	auto getBusyWriteTagCount() const { return busyWriteTagCount; }
};
