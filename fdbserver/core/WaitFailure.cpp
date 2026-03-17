/*
 * WaitFailure.cpp
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

#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/fdbrpc.h"
#include "flow/Deque.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/WaitFailure.h"

Future<Void> waitFailureServer(FutureStream<ReplyPromise<Void>> waitFailure) {
	// when this actor is cancelled, the promises in the queue will send broken_promise
	Deque<ReplyPromise<Void>> queue;
	while (true) {
		ReplyPromise<Void> P = co_await waitFailure;
		queue.push_back(P);
		if (queue.size() > SERVER_KNOBS->MAX_OUTSTANDING_WAIT_FAILURE_REQUESTS) {
			CODE_PROBE(true, "wait server queue full");
			queue.front().send(Void());
			queue.pop_front();
		}
	}
}

Future<Void> waitFailureClient(RequestStream<ReplyPromise<Void>> waitFailure,
                               double reactionTime,
                               double reactionSlope,
                               bool trace,
                               Optional<Standalone<StringRef>> traceMsg,
                               TaskPriority taskID) {
	while (true) {
		try {
			double start = now();
			ErrorOr<Void> x =
			    co_await waitFailure.getReplyUnlessFailedFor(ReplyPromise<Void>(), reactionTime, reactionSlope, taskID);
			if (!x.present()) {
				if (trace) {
					TraceEvent te("WaitFailureClient");
					te.detail("FailedEndpoint", waitFailure.getEndpoint().getPrimaryAddress().toString())
					    .detail("Token", waitFailure.getEndpoint().token);
					if (traceMsg.present()) {
						te.detail("Context", traceMsg.get());
					}
				}
				co_return;
			}
			double w = start + SERVER_KNOBS->WAIT_FAILURE_DELAY_LIMIT - now();
			if (w > 0) {
				co_await delay(w, taskID);
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			TraceEvent(SevError, "WaitFailureClientError").error(e);
			ASSERT(false); // unknown error from waitFailureServer
		}
	}
}

Future<Void> waitFailureClientStrict(RequestStream<ReplyPromise<Void>> waitFailure,
                                     double failureReactionTime,
                                     TaskPriority taskID) {
	while (true) {
		co_await waitFailureClient(waitFailure,
		                           /* failureReactionTime */ 0,
		                           /* failureReactionSlope */ 0,
		                           /* trace */ false,
		                           /* traceMsg */ Optional<Standalone<StringRef>>(),
		                           taskID);
		co_await (delay(failureReactionTime, taskID) ||
		          IFailureMonitor::failureMonitor().onStateEqual(waitFailure.getEndpoint(), FailureStatus(false)));
		if (IFailureMonitor::failureMonitor().getState(waitFailure.getEndpoint()).isFailed()) {
			co_return;
		}
	}
}

Future<Void> waitFailureTracker(RequestStream<ReplyPromise<Void>> waitFailure,
                                Reference<AsyncVar<bool>> failed,
                                double reactionTime,
                                double reactionSlope,
                                TaskPriority taskID) {
	while (true) {
		try {
			failed->set(IFailureMonitor::failureMonitor().getState(waitFailure.getEndpoint()).isFailed());
			if (failed->get()) {
				co_await IFailureMonitor::failureMonitor().onStateChanged(waitFailure.getEndpoint());
			} else {
				double start = now();
				ErrorOr<Void> x = co_await waitFailure.getReplyUnlessFailedFor(
				    ReplyPromise<Void>(), reactionTime, reactionSlope, taskID);
				if (x.present()) {
					double w = start + SERVER_KNOBS->WAIT_FAILURE_DELAY_LIMIT - now();
					if (w > 0) {
						co_await delay(w, taskID);
					}
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			TraceEvent(SevError, "WaitFailureClientError").error(e);
			ASSERT(false); // unknown error from waitFailureServer
		}
	}
}
