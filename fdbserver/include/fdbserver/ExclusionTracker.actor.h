/*
 * ExclusionTracker.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(EXCLUSION_TRACKER_ACTOR_G_H)
#define EXCLUSION_TRACKER_ACTOR_G_H
#include "fdbserver/ExclusionTracker.actor.g.h"
#elif !defined(EXCLUSION_TRACKER_ACTOR_H)
#define EXCLUSION_TRACKER_ACTOR_H

#include <set>
#include "flow/flow.h"
#include "flow/Trace.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ExclusionTracker {
	std::set<AddressExclusion> excluded;
	std::set<AddressExclusion> failed;

	AsyncTrigger changed;

	Database db;
	Future<Void> trackerFuture;

	ExclusionTracker() {}
	ExclusionTracker(Database db) : db(db) { trackerFuture = tracker(this); }

	bool isFailedOrExcluded(NetworkAddress addr) {
		AddressExclusion addrExclusion(addr.ip, addr.port);
		return excluded.contains(addrExclusion) || failed.contains(addrExclusion);
	}

	ACTOR static Future<Void> tracker(ExclusionTracker* self) {
		// Fetch the list of excluded servers
		state ReadYourWritesTransaction tr(self->db);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				state Future<RangeResult> fresultsExclude = tr.getRange(excludedServersKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> fresultsFailed = tr.getRange(failedServersKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> flocalitiesExclude =
				    tr.getRange(excludedLocalityKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<RangeResult> flocalitiesFailed = tr.getRange(failedLocalityKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<std::vector<ProcessData>> fworkers = getWorkers(&tr.getTransaction());
				wait(success(fresultsExclude) && success(fresultsFailed) && success(flocalitiesExclude) &&
				     success(flocalitiesFailed));

				state RangeResult excludedResults = fresultsExclude.get();
				ASSERT(!excludedResults.more && excludedResults.size() < CLIENT_KNOBS->TOO_MANY);

				state RangeResult failedResults = fresultsFailed.get();
				ASSERT(!failedResults.more && failedResults.size() < CLIENT_KNOBS->TOO_MANY);

				state RangeResult excludedLocalityResults = flocalitiesExclude.get();
				ASSERT(!excludedLocalityResults.more && excludedLocalityResults.size() < CLIENT_KNOBS->TOO_MANY);

				state RangeResult failedLocalityResults = flocalitiesFailed.get();
				ASSERT(!failedLocalityResults.more && failedLocalityResults.size() < CLIENT_KNOBS->TOO_MANY);

				state std::set<AddressExclusion> newExcluded;
				state std::set<AddressExclusion> newFailed;
				for (const auto& r : excludedResults) {
					AddressExclusion addr = decodeExcludedServersKey(r.key);
					if (addr.isValid()) {
						newExcluded.insert(addr);
					}
				}
				for (const auto& r : failedResults) {
					AddressExclusion addr = decodeFailedServersKey(r.key);
					if (addr.isValid()) {
						newFailed.insert(addr);
					}
				}

				wait(success(fworkers));
				std::vector<ProcessData> workers = fworkers.get();
				for (const auto& r : excludedLocalityResults) {
					std::string locality = decodeExcludedLocalityKey(r.key);
					std::set<AddressExclusion> localityExcludedAddresses = getAddressesByLocality(workers, locality);
					newExcluded.insert(localityExcludedAddresses.begin(), localityExcludedAddresses.end());
					if (localityExcludedAddresses.empty()) {
						TraceEvent(SevWarn, "ExclusionTrackerLocalityNotFound").detail("Locality", locality);
					}
				}
				for (const auto& r : failedLocalityResults) {
					std::string locality = decodeFailedLocalityKey(r.key);
					std::set<AddressExclusion> localityFailedAddresses = getAddressesByLocality(workers, locality);
					newFailed.insert(localityFailedAddresses.begin(), localityFailedAddresses.end());
					if (localityFailedAddresses.empty()) {
						TraceEvent(SevWarn, "ExclusionTrackerFailedLocalityNotFound").detail("Locality", locality);
					}
				}

				bool foundChange = false;
				if (self->excluded != newExcluded) {
					self->excluded = newExcluded;
					foundChange = true;
				}
				if (self->failed != newFailed) {
					self->failed = newFailed;
					foundChange = true;
				}

				if (foundChange) {
					self->changed.trigger();
				}

				state Future<Void> watchFuture =
				    tr.watch(excludedServersVersionKey) || tr.watch(failedServersVersionKey) ||
				    tr.watch(excludedLocalityVersionKey) || tr.watch(failedLocalityVersionKey);
				wait(tr.commit());
				if (excludedLocalityResults.size() > 0 || failedLocalityResults.size() > 0) {
					// when there are excluded localities we need to monitor for when the worker list changes, so we
					// must poll
					watchFuture = watchFuture || delay(10.0);
				}
				wait(watchFuture);
				tr.reset();
			} catch (Error& e) {
				TraceEvent("ExclusionTrackerError").error(e);
				wait(tr.onError(e));
			}
		}
	}
};

#include "flow/unactorcompiler.h"
#endif