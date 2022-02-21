/*
 * SysTestScheduler.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "SysTestScheduler.h"

#include "flow/Arena.h"
#include "flow/ThreadPrimitives.h"
#include "flow/ThreadSafeQueue.h"
#include "flow/IRandom.h"
#include <iostream>
#include <thread>
#include <cassert>

namespace FDBSystemTester {

class SingleThreadedScheduler : public IScheduler {
public:
	SingleThreadedScheduler() : stopRequested(false), sleeping(false), thr(nullptr) {}

	~SingleThreadedScheduler() override {
		if (thr) {
			delete thr;
		}
	}

	void start() override {
		assert(thr == nullptr);
		assert(!stop_);
		thr = new std::thread([this]() { this->threadMain(); });
	}

	void schedule(TTaskFct task) override {
		taskQueue.push(task);
		wake();
	}

	void stop() override {
		if (stopRequested.exchange(true) == false) {
			if (thr) {
				wake();
			}
		}
	}

	void join() override {
		assert(thr);
		thr->join();
	}

private:
	void threadMain() {
		while (!stopRequested) {
			Optional<TTaskFct> t = taskQueue.pop();
			if (t.present()) {
				t.get()();
				continue;
			}
			sleeping = true;
			wakeEvent.block();
			sleeping = false;
			continue;
		}
	}

	void wake() {
		while (sleeping) {
			wakeEvent.set();
		}
	}

	ThreadSafeQueue<TTaskFct> taskQueue;
	std::atomic<bool> stopRequested;
	std::atomic<bool> sleeping;
	Event wakeEvent;
	std::thread* thr;
};

class MultiThreadedScheduler : public IScheduler {
public:
	MultiThreadedScheduler(int numThreads) : numThreads(numThreads) {
		for (int i = 0; i < numThreads; i++) {
			schedulers.push_back(new SingleThreadedScheduler());
		}
	}

	~MultiThreadedScheduler() override {
		for (auto sch : schedulers) {
			delete sch;
		}
	}

	void start() override {
		for (auto sch : schedulers) {
			sch->start();
		}
	}

	void schedule(TTaskFct task) override {
		int idx = deterministicRandom()->randomInt(0, numThreads);
		schedulers[idx]->schedule(task);
	}

	void stop() override {
		for (auto sch : schedulers) {
			sch->stop();
		}
	}

	void join() override {
		for (auto sch : schedulers) {
			sch->join();
		}
	}

private:
	std::vector<IScheduler*> schedulers;
	int numThreads;
};

IScheduler* createScheduler(int numThreads) {
	assert(numThreads > 0 && numThreads <= 1000);
	if (numThreads == 1) {
		return new SingleThreadedScheduler();
	} else {
		return new MultiThreadedScheduler(numThreads);
	}
}

} // namespace FDBSystemTester