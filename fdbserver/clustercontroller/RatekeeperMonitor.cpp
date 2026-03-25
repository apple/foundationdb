/*
 * RatekeeperMonitor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "RatekeeperMonitor.h"
#include "flow/UnitTest.h"

bool RatekeeperMonitor::hasSustainedZeroRatekeeperTpsLimit(double tpsLimit,
                                                           double currentTime,
                                                           double zeroTpsLimitDuration) {
	if (zeroTpsLimitDuration <= 0 || tpsLimit > 0) {
		resetZeroRatekeeperTpsLimitObservation();
		return false;
	}

	if (!zeroRatekeeperTpsLimitStartTime.present()) {
		zeroRatekeeperTpsLimitStartTime = currentTime;
		return false;
	}

	return currentTime - zeroRatekeeperTpsLimitStartTime.get() >= zeroTpsLimitDuration;
}

TEST_CASE("/fdbserver/clustercontroller/hasSustainedZeroRatekeeperTpsLimit") {
	RatekeeperMonitor monitor;

	constexpr double duration = 5.0;

	ASSERT(!monitor.hasSustainedZeroRatekeeperTpsLimit(0.0, 100.0, duration));
	ASSERT(monitor.getZeroRatekeeperTpsLimitStartTime().present());
	ASSERT_EQ(monitor.getZeroRatekeeperTpsLimitStartTime().get(), 100.0);
	ASSERT(!monitor.hasSustainedZeroRatekeeperTpsLimit(0.0, 104.9, duration));
	ASSERT(monitor.hasSustainedZeroRatekeeperTpsLimit(0.0, 105.0, duration));

	ASSERT(!monitor.hasSustainedZeroRatekeeperTpsLimit(1.0, 106.0, duration));
	ASSERT(!monitor.getZeroRatekeeperTpsLimitStartTime().present());

	ASSERT(!monitor.hasSustainedZeroRatekeeperTpsLimit(0.0, 107.0, duration));
	ASSERT(monitor.getZeroRatekeeperTpsLimitStartTime().present());
	ASSERT_EQ(monitor.getZeroRatekeeperTpsLimitStartTime().get(), 107.0);

	ASSERT(!monitor.hasSustainedZeroRatekeeperTpsLimit(0.0, 200.0, 0.0));
	ASSERT(!monitor.getZeroRatekeeperTpsLimitStartTime().present());

	return Void();
}
