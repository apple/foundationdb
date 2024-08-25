/*
 * ThreadSafety.actor.cpp
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

#include "fdbrpc/simulator.h"
#include "flow/DeterministicRandom.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ThreadSafetyWorkload;

// Parameters passed to each thread
struct ThreadInfo {
	int id;
	ThreadSafetyWorkload* self;

	Promise<Void> done;
	DeterministicRandom random;

	ThreadInfo(int id, ThreadSafetyWorkload* self)
	  : id(id), self(self), random(deterministicRandom()->randomInt(1, 1e9)) {}
};

// A thread barrier implementation. Reached() method blocks until the required number of threads reach it.
struct Barrier {
	Mutex mutex;
	std::vector<Event*> events;

	int numRequired;
	int numReached;

	Barrier() : numRequired(0), numReached(0) {}

	~Barrier() { fire(); }

	void decrementNumRequired() {
		mutex.enter();
		if (--numRequired == numReached)
			fire();
		mutex.leave();
	}

	void setNumRequired(int numRequired) {
		mutex.enter();
		this->numRequired = numRequired;
		if (numRequired > 0 && numRequired <= numReached)
			fire();
		mutex.leave();
	}

	// Called by each thread to signal that the barrier has been reached.
	// Blocks until <numRequired> threads have called this function.
	void reached() {
		mutex.enter();
		bool ready = (++numReached == numRequired);
		Event* myEvent = nullptr;

		if (ready)
			fire();
		else {
			myEvent = new Event();
			events.push_back(myEvent);
		}
		mutex.leave();

		if (!ready) {
			myEvent->block();
			delete myEvent;
		}
	}

private:
	void fire() {
		numReached = 0;
		for (int i = 0; i < events.size(); ++i)
			events[i]->set();

		events.clear();
	}
};

// A workload which uses the thread safe API from multiple threads
struct ThreadSafetyWorkload : TestWorkload {
	static constexpr auto NAME = "ThreadSafety";

	int threadsPerClient;
	double threadDuration;

	// Used to generate keys for the workload. This is the number of keys that will be available for operations.
	int numKeys;

	bool success;
	bool stopped;

	Mutex mutex;
	Barrier commitBarrier;

	Reference<IDatabase> db;

	// ThreadFutures are not thread safe, so they must be copied in other threads inside of mutexes
	ThreadFuture<Void> commitFuture;

	Reference<ITransaction> tr;

	ThreadSafetyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), stopped(false) {

		threadsPerClient = getOption(options, "threadsPerClient"_sr, 3);
		threadDuration = getOption(options, "threadDuration"_sr, 60.0);
		numKeys = getOption(options, "numKeys"_sr, 100);

		commitBarrier.setNumRequired(threadsPerClient);

		success = true;

		// This test is not deterministic
		noUnseed = true;
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	ACTOR Future<Void> _start(Database cx, ThreadSafetyWorkload* self) {
		state std::vector<ThreadInfo*> threadInfo;

		Reference<IDatabase> dbRef =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));
		self->db = dbRef;

		if (deterministicRandom()->coinflip()) {
			MultiVersionApi::api->selectApiVersion(cx->apiVersion.version());
			self->db = MultiVersionDatabase::debugCreateFromExistingDatabase(dbRef);
		}

		state int i;
		for (i = 0; i < self->threadsPerClient; ++i) {
			threadInfo.push_back(new ThreadInfo(i, self));
			g_network->startThread(self->threadStart, threadInfo[i]);
		}

		wait(delay(self->threadDuration));

		// Signals the threads to stop
		self->mutex.enter();
		self->stopped = true;
		self->mutex.leave();

		for (i = 0; i < threadInfo.size(); ++i) {
			try {
				wait(threadInfo[i]->done.getFuture());
			} catch (Error& e) {
				self->success = false;
				printf("Thread %d.%d failed: %s\n", self->clientId, i, e.name());
				TraceEvent(SevError, "ThreadSafety_ThreadFailed").error(e);
			}

			delete threadInfo[i];
		}

		return Void();
	}

	THREAD_FUNC threadStart(void* arg) {
		ThreadInfo* info = (ThreadInfo*)arg;

		Error error(error_code_success);
		try {
			info->self->runTest(info);
		} catch (Error& e) {
			error = e;
		}

		info->self->commitBarrier.decrementNumRequired();

		// Signal completion back to the main thread
		onMainThreadVoid([=]() {
			if (error.code() != error_code_success)
				info->done.sendError(error);
			else
				info->done.send(Void());
		});

		THREAD_RETURN;
	}

	Key getRandomKey(DeterministicRandom& random) {
		return StringRef(format("ThreadSafetyKey%010d", random.randomInt(0, numKeys)));
	}

	void runTest(ThreadInfo* info) {
		// Create a new transaction
		mutex.enter();
		if (!tr) {
			try {
				tr = db->createTransaction();
			} catch (Error&) {
				mutex.leave();
				throw;
			}
		}
		mutex.leave();

		loop {
			// Perform a sequence of random operations
			for (int i = 0; i < info->random.randomInt(1, 10); ++i) {
				int operation = info->random.randomInt(0, 6);

				try {
					if (operation == 0)
						tr->set(getRandomKey(info->random),
						        StringRef(std::string(info->random.randomInt(0, 100), 'x')));
					else if (operation == 1)
						tr->get(getRandomKey(info->random)).getBlocking();
					else if (operation == 2)
						tr->getKey(KeySelectorRef(getRandomKey(info->random),
						                          info->random.randomInt(0, 2) == 1,
						                          info->random.randomInt(-10, 11)))
						    .getBlocking();
					else if (operation == 3) {
						Key key1 = getRandomKey(info->random);
						Key key2 = getRandomKey(info->random);
						GetRangeLimits limits(info->random.randomInt(1, 1000), info->random.randomInt(1, 1e6));
						tr->getRange(KeyRangeRef(std::min(key1, key2), std::max(key1, key2)),
						             limits,
						             info->random.randomInt(0, 2) != 0,
						             info->random.randomInt(0, 2) != 0)
						    .getBlocking();
					} else if (operation == 4)
						tr->clear(getRandomKey(info->random));
					else if (operation == 5) {
						Key key1 = getRandomKey(info->random);
						Key key2 = getRandomKey(info->random);
						tr->clear(KeyRangeRef(std::min(key1, key2), std::max(key1, key2)));
					}
				} catch (Error&) {
					break;
				}
			}

			commitBarrier.reached();

			// One thread starts a commit, and all threads wait on that commit
			mutex.enter();
			if (!commitFuture.isValid())
				commitFuture = tr->commit();
			ThreadFuture<Void> commit = commitFuture;
			mutex.leave();

			try {
				commit.getBlocking();
			} catch (Error&) {
			}

			commitBarrier.reached();

			mutex.enter();
			if (commitFuture.isValid())
				commitFuture = ThreadFuture<Void>();

			if (stopped) {
				mutex.leave();
				break;
			}

			mutex.leave();
		}
	}

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ThreadSafetyWorkload> ThreadSafetyWorkloadFactory;
