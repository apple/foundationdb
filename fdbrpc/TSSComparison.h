/*
 * TSSComparison.h
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

/*
 * This header is to declare the tss comparison function that LoadBalance.Actor.h needs to be aware of to call,
 * But StorageServerInterface.h needs to implement on the types defined in SSI.h.
 */
#ifndef FDBRPC_TSS_COMPARISON_H
#define FDBRPC_TSS_COMPARISON_H

#include "fdbrpc/ContinuousSample.h"
#include "fdbrpc/Stats.h"

// refcounted + noncopyable because both DatabaseContext and individual endpoints share ownership
struct TSSMetrics : ReferenceCounted<TSSMetrics>, NonCopyable {
	CounterCollection cc;
	Counter requests;
	Counter ssErrors;
	Counter tssErrors;
	Counter tssTimeouts;
	Counter mismatches;

	// We could probably just ignore getKey as it's seldom used?
	ContinuousSample<double> SSgetValueLatency;
	ContinuousSample<double> SSgetKeyLatency;
	ContinuousSample<double> SSgetKeyValuesLatency;

	ContinuousSample<double> TSSgetValueLatency;
	ContinuousSample<double> TSSgetKeyLatency;
	ContinuousSample<double> TSSgetKeyValuesLatency;

	std::unordered_map<int, uint64_t> ssErrorsByCode;
	std::unordered_map<int, uint64_t> tssErrorsByCode;

	void ssError(int code) {
		++ssErrors;
		ssErrorsByCode[code]++;
	}

	void tssError(int code) {
		++tssErrors;
		tssErrorsByCode[code]++;
	}

	template <class Req>
	void recordLatency(const Req& req, double ssLatency, double tssLatency);

	void clear() {
		SSgetValueLatency.clear();
		SSgetKeyLatency.clear();
		SSgetKeyValuesLatency.clear();

		TSSgetValueLatency.clear();
		TSSgetKeyLatency.clear();
		TSSgetKeyValuesLatency.clear();

		tssErrorsByCode.clear();
		ssErrorsByCode.clear();
	}

	TSSMetrics()
	  : cc("TSSClientMetrics"), requests("Requests", cc), ssErrors("SSErrors", cc), tssErrors("TSSErrors", cc),
	    tssTimeouts("TSSTimeouts", cc), mismatches("Mismatches", cc), SSgetValueLatency(1000), SSgetKeyLatency(1000),
	    SSgetKeyValuesLatency(1000), TSSgetValueLatency(1000), TSSgetKeyLatency(1000), TSSgetKeyValuesLatency(1000) {}
};

// part of the contract of this function is that if there is a mismatch, the implementation needs to record a trace
// event with the specified severity and tssId in the event.
template <class Req, class Rep>
bool TSS_doCompare(const Req& req, const Rep& src, const Rep& tss, Severity traceSeverity, UID tssId);

#endif
