/*
 * AsyncTaskThread.actor.cpp
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

#include <atomic>

#include "fdbclient/AsyncTaskThread.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

class TerminateTask final : public IAsyncTask {
public:
	void operator()() override { ASSERT(false); }
	bool isTerminate() const override { return true; }
};

ACTOR Future<Void> asyncTaskThreadClient(AsyncTaskThread* asyncTaskThread,
                                         std::atomic<int>* sum,
                                         int count,
                                         int clientId,
                                         double meanSleep) {
	state int i = 0;
	state double randomSleep = 0.0;
	for (; i < count; ++i) {
		randomSleep = deterministicRandom()->random01() * 2 * meanSleep;
		wait(delay(randomSleep));
		wait(asyncTaskThread->execAsync([sum = sum] {
			sum->fetch_add(1);
			return Void();
		}));
		TraceEvent("AsyncTaskThreadIncrementedSum")
		    .detail("Index", i)
		    .detail("Sum", sum->load())
		    .detail("ClientId", clientId)
		    .detail("RandomSleep", randomSleep)
		    .detail("MeanSleep", meanSleep);
	}
	return Void();
}

} // namespace

const double AsyncTaskThread::meanDelay = 0.01;

AsyncTaskThread::AsyncTaskThread() : thread([this] { run(this); }) {}

AsyncTaskThread::~AsyncTaskThread() {
	bool wakeUp = false;
	{
		std::lock_guard<std::mutex> g(m);
		wakeUp = queue.push(std::make_unique<TerminateTask>());
	}
	if (wakeUp) {
		cv.notify_one();
	}
	thread.join();
}

void AsyncTaskThread::run(AsyncTaskThread* self) {
	while (true) {
		std::unique_ptr<IAsyncTask> task;
		{
			std::unique_lock<std::mutex> lk(self->m);
			self->cv.wait(lk, [self] { return !self->queue.canSleep(); });
			task = self->queue.pop().get();
			if (task->isTerminate()) {
				return;
			}
		}
		(*task)();
	}
}

TEST_CASE("/asynctaskthread/add") {
	state std::atomic<int> sum = 0;
	state AsyncTaskThread asyncTaskThread;
	state int numClients = 10;
	state int incrementsPerClient = 100;
	std::vector<Future<Void>> clients;
	clients.reserve(numClients);
	for (int clientId = 0; clientId < numClients; ++clientId) {
		clients.push_back(asyncTaskThreadClient(
		    &asyncTaskThread, &sum, incrementsPerClient, clientId, deterministicRandom()->random01() * 0.01));
	}
	wait(waitForAll(clients));
	ASSERT_EQ(sum.load(), numClients * incrementsPerClient);
	return Void();
}

TEST_CASE("/asynctaskthread/error") {
	state AsyncTaskThread asyncTaskThread;
	try {
		wait(asyncTaskThread.execAsync([] {
			throw operation_failed();
			return Void();
		}));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}
	return Void();
}
