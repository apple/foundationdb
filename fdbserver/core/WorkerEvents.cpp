/*
 * WorkerEvents.cpp
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

#include "fdbserver/core/WorkerEvents.h"

AsyncResult<Optional<std::pair<WorkerEvents, std::set<std::string>>>> latestEventOnWorkers(
    std::vector<WorkerDetails> workers,
    std::string eventName) {
	try {
		std::vector<Future<ErrorOr<TraceEventFields>>> eventTraces;
		for (int c = 0; c < workers.size(); c++) {
			EventLogRequest req =
			    !eventName.empty() ? EventLogRequest(Standalone<StringRef>(eventName)) : EventLogRequest();
			eventTraces.push_back(errorOr(timeoutError(workers[c].interf.eventLogRequest.getReply(req), 2.0)));
		}

		co_await waitForAll(eventTraces);

		std::set<std::string> failed;
		WorkerEvents results;

		for (int i = 0; i < eventTraces.size(); i++) {
			const ErrorOr<TraceEventFields>& v = eventTraces[i].get();
			if (v.isError()) {
				failed.insert(workers[i].interf.address().toString());
				results[workers[i].interf.address()] = TraceEventFields();
			} else {
				results[workers[i].interf.address()] = v.get();
			}
		}

		co_return std::make_pair(std::move(results), std::move(failed));
	} catch (Error& e) {
		ASSERT(e.code() ==
		       error_code_actor_cancelled); // All errors should be filtering through the errorOr actor above
		throw;
	}
}
