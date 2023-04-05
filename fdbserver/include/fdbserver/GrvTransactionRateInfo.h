/*
 * GrvTransactionRateInfo.h
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

#include "fdbrpc/Smoother.h"

// Used by GRV Proxy to enforce rate limits received from the Ratekeeper.
//
// Between waits, the GrvTransactionRateInfo executes a "release window" starting
// with a call to the startReleaseWindow method. Within this release window, transactions are
// released while canStart returns true. At the end of the release window, the
// endReleaseWindow method is called, and the budget is updated to add or
// remove capacity.
//
// Meanwhile, the desired rate is updated through the setRate method.
//
// Smoothers are used to avoid turbulent throttling behaviour.
class GrvTransactionRateInfo {
	double rate = 0.0;
	double limit{ 0.0 };
	double budget{ 0.0 };
	bool disabled{ true };
	Smoother smoothRate;
	Smoother smoothReleased;

public:
	explicit GrvTransactionRateInfo(double rate = 0.0);

	// Determines the number of transactions that this proxy is allowed to release
	// in this release window.
	void startReleaseWindow();

	// Checks if a "count" new transactions can be released, given that
	// "numAlreadyStarted" transactions have already been released in the
	// current release window.
	bool canStart(int64_t numAlreadyStarted, int64_t count) const;

	// Updates the budget to accumulate any extra capacity available or remove any excess that was used.
	// Call at the end of a release window.
	void endReleaseWindow(int64_t numStarted, bool queueEmpty, double elapsed);

	// Smoothly sets rate. If currently disabled, reenable
	void setRate(double rate);

	// Smoothly sets transaction rate to 0. Call disable when new rates have not been
	// set for a sufficiently long period of time.
	void disable();

	double getRate() const { return rate; }
	double getLimit() const { return limit; }
};
