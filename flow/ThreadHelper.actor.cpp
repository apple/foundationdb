/*
 * ThreadHelper.actor.cpp
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

#include <string>

#include "flow/flow.h"
#include "flow/network.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/Error.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ThreadCallback* ThreadCallback::addCallback(ThreadCallback* cb) {
	return (new ThreadMultiCallback())->addCallback(this)->addCallback(cb);
}

// A simple thread object that sends the result
struct ThreadFutureSendObj {
	void operator()() { tsav->send(Void()); }
	ThreadSingleAssignmentVar<Void>* tsav;
};

// A simple thread object that cancels the threadFuture
struct ThreadFutureCancelObj {
	ThreadFutureCancelObj(ThreadFuture<Void> f) : f(f) {}
	void operator()() { f.cancel(); }
	ThreadFuture<Void> f;
};

// This unit test should be running with TSAN enabled binary
TEST_CASE("/flow/safeThreadFutureToFuture/Send") {
	// std::thread is not working in simulation at present, disable this in simulation
	if (g_network->isSimulated())
		return Void();
	auto* tsav = new ThreadSingleAssignmentVar<Void>;
	state std::thread thread = std::thread{ ThreadFutureSendObj{ tsav } };
	ThreadFuture<Void> f(tsav);
	// change this to unsafeThreadFutureToFuture will get a data-race failure
	wait(safeThreadFutureToFuture(f));
	thread.join();
	return Void();
}

// Test the case where the underlying threadFuture is cancelled
TEST_CASE("/flow/safeThreadFutureToFuture/Cancel") {
	// std::thread is not working in simulation at present, disable this in simulation
	if (g_network->isSimulated())
		return Void();
	ThreadFuture<Void> f = onMainThread([]() -> Future<Void> { return Never(); });
	state std::thread thread = std::thread{ ThreadFutureCancelObj(f) };
	try {
		wait(safeThreadFutureToFuture(f)); // this actor should get actor_cancelled
		ASSERT(false);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_actor_cancelled);
	}
	thread.join();
	return Void();
}
