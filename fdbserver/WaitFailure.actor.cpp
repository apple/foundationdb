/*
 * WaitFailure.actor.cpp
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

#include "fdbrpc/FailureMonitor.h"
#include "flow/Deque.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Void> waitFailureServer(FutureStream<ReplyPromise<Void>> waitFailure) {
	// when this actor is cancelled, the promises in the queue will send broken_promise
	state Deque<ReplyPromise<Void>> queue;
	loop {
		ReplyPromise<Void> P = waitNext(waitFailure);
		queue.push_back(P);
		if (queue.size() > SERVER_KNOBS->MAX_OUTSTANDING_WAIT_FAILURE_REQUESTS) {
			TEST(true); // wait server queue full
			queue.front().send(Void());
			queue.pop_front();
		}
	}
}

ACTOR Future<Void> waitFailureClient(RequestStream<ReplyPromise<Void>> waitFailure,
                                     double reactionTime,
                                     double reactionSlope,
                                     bool trace,
                                     TaskPriority taskID) {
	loop {
		try {
			state double start = now();
			ErrorOr<Void> x =
			    wait(waitFailure.getReplyUnlessFailedFor(ReplyPromise<Void>(), reactionTime, reactionSlope, taskID));
			if (!x.present()) {
				if (trace) {
					TraceEvent("WaitFailureClient")
					    .detail("FailedEndpoint", waitFailure.getEndpoint().getPrimaryAddress().toString())
					    .detail("Token", waitFailure.getEndpoint().token);
				}
				return Void();
			}
			double w = start + SERVER_KNOBS->WAIT_FAILURE_DELAY_LIMIT - now();
			if (w > 0)
				wait(delay(w, taskID));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			TraceEvent(SevError, "WaitFailureClientError").error(e);
			ASSERT(false); // unknown error from waitFailureServer
		}
	}
}

ACTOR Future<Void> waitFailureClientStrict(RequestStream<ReplyPromise<Void>> waitFailure,
                                           double failureReactionTime,
                                           TaskPriority taskID) {
	loop {
		wait(waitFailureClient(waitFailure, 0, 0, false, taskID));
		wait(delay(failureReactionTime, taskID) ||
		     IFailureMonitor::failureMonitor().onStateEqual(waitFailure.getEndpoint(), FailureStatus(false)));
		if (IFailureMonitor::failureMonitor().getState(waitFailure.getEndpoint()).isFailed()) {
			return Void();
		}
	}
}

ACTOR Future<Void> waitFailureTracker(RequestStream<ReplyPromise<Void>> waitFailure,
                                      Reference<AsyncVar<bool>> failed,
                                      double reactionTime,
                                      double reactionSlope,
                                      TaskPriority taskID) {
	loop {
		try {
			failed->set(IFailureMonitor::failureMonitor().getState(waitFailure.getEndpoint()).isFailed());
			if (failed->get()) {
				wait(IFailureMonitor::failureMonitor().onStateChanged(waitFailure.getEndpoint()));
			} else {
				state double start = now();
				ErrorOr<Void> x = wait(
				    waitFailure.getReplyUnlessFailedFor(ReplyPromise<Void>(), reactionTime, reactionSlope, taskID));
				if (x.present()) {
					double w = start + SERVER_KNOBS->WAIT_FAILURE_DELAY_LIMIT - now();
					if (w > 0)
						wait(delay(w, taskID));
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
