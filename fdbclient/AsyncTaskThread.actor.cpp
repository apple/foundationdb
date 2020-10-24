/*
 * AsyncTaskThread.actor.cpp
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

#include "fdbclient/AsyncTaskThread.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

class TerminateTask final : public IAsyncTask {
public:
	void operator()() override { ASSERT(false); }
	bool isTerminate() const override { return true; }
};

ACTOR Future<Void> asyncTaskThreadClient(AsyncTaskThread* asyncTaskThread, int* sum, int count) {
	state int i = 0;
	for (; i < count; ++i) {
		wait(asyncTaskThread->execAsync([sum = sum] {
			++(*sum);
			return Void();
		}));
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
		wakeUp = queue.push(std::make_shared<TerminateTask>());
	}
	if (wakeUp) {
		cv.notify_one();
	}
	thread.join();
}

void AsyncTaskThread::run(AsyncTaskThread* self) {
	while (true) {
		std::shared_ptr<IAsyncTask> task;
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
	state int sum = 0;
	state AsyncTaskThread asyncTaskThread;
	std::vector<Future<Void>> clients;
	clients.reserve(10);
	for (int i = 0; i < 10; ++i) {
		clients.push_back(asyncTaskThreadClient(&asyncTaskThread, &sum, 100));
	}
	wait(waitForAll(clients));
	ASSERT(sum == 1000);
	return Void();
}
