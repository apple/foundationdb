/*
 * PriorityMultiLock.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_PRIORITYMULTILOCK_ACTOR_G_H)
#define FLOW_PRIORITYMULTILOCK_ACTOR_G_H
#include "flow/PriorityMultiLock.actor.g.h"
#elif !defined(PRIORITYMULTILOCK_ACTOR_H)
#define PRIORITYMULTILOCK_ACTOR_H

#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A multi user lock with a concurrent holder limit where waiters are granted the lock according to
// an integer priority from 0 to maxPriority, inclusive, where higher integers are given priority.
//
// The interface is similar to FlowMutex except that lock holders can drop the lock to release it.
//
// Usage:
//   Lock lock = wait(prioritylock.lock(priorityLevel));
//   lock.release();  // Explicit release, or
//   // let lock and all copies of lock go out of scope to release
class PriorityMultiLock {

public:
	// Waiting on the lock returns a Lock, which is really just a Promise<Void>
	// Calling release() is not necessary, it exists in case the Lock holder wants to explicitly release
	// the Lock before it goes out of scope.
	struct Lock {
		void release() { promise.send(Void()); }

		// This is exposed in case the caller wants to use/copy it directly
		Promise<Void> promise;
	};

	PriorityMultiLock(int concurrency, std::string launchLimits)
	  : PriorityMultiLock(concurrency, parseStringToVector<int>(launchLimits, ',')) {}

	PriorityMultiLock(int concurrency, std::vector<int> launchLimitsByPriority)
	  : concurrency(concurrency), available(concurrency), waiting(0), launchLimits(launchLimitsByPriority) {

		waiters.resize(launchLimits.size());
		runnerCounts.resize(launchLimits.size(), 0);
		fRunner = runner(this);
	}

	~PriorityMultiLock() {}

	Future<Lock> lock(int priority = 0) {

		// This shortcut may enable a waiter to jump the line when the releaser loop yields
		if (available > 0) {
			Lock p;
			addRunner(p, priority);
			return p;
		}

		Waiter w;
		waiters[priority].push_back(w);
		++waiting;
		return w.lockPromise.getFuture();
	}

	void kill() {
		for (int i = 0; i < runners.size(); ++i) {
			if (!runners[i].isReady()) {
				runners[i].cancel();
			}
		}
		runners.clear();
		brokenOnDestruct.sendError(broken_promise());
		waiting = 0;
		waiters.clear();
	}

	std::string toString() const {
		int runnersDone = 0;
		for (int i = 0; i < runners.size(); ++i) {
			if (runners[i].isReady()) {
				++runnersDone;
			}
		}

		std::string s =
		    format("{ ptr=%p concurrency=%d available=%d running=%d waiting=%d runnersQueue=%d runnersDone=%d ",
		           this,
		           concurrency,
		           available,
		           concurrency - available,
		           waiting,
		           runners.size(),
		           runnersDone);

		for (int i = 0; i < waiters.size(); ++i) {
			s += format("p%d_waiters=%u ", i, waiters[i].size());
		}

		s += "}";
		return s;
	}
	int maxPriority() const { return launchLimits.size() - 1; }

	int totalWaiters() const { return waiting; }

	int numWaiters(const unsigned int priority) const {
		ASSERT(priority < waiters.size());
		return waiters[priority].size();
	}

	int totalRunners() const { return concurrency - available; }

	int numRunners(const unsigned int priority) const {
		ASSERT(priority < waiters.size());
		return runnerCounts[priority];
	}

private:
	struct Waiter {
		Waiter() : queuedTime(now()) {}
		Promise<Lock> lockPromise;
		double queuedTime;
	};

	int concurrency;
	int available;
	int waiting;
	typedef Deque<Waiter> Queue;
	std::vector<int> launchLimits;
	std::vector<Queue> waiters;
	std::vector<int> runnerCounts;
	Deque<Future<Void>> runners;
	Future<Void> fRunner;
	AsyncTrigger release;
	Promise<Void> brokenOnDestruct;

	void addRunner(Lock& lock, int priority) {
		runnerCounts[priority] += 1;
		--available;
		runners.push_back(map(ready(lock.promise.getFuture()), [=](Void) {
			++available;
			runnerCounts[priority] -= 1;
			if (waiting > 0 || runners.size() > 100) {
				release.trigger();
			}
			return Void();
		}));
	}

	ACTOR static Future<Void> runner(PriorityMultiLock* self) {
		state int sinceYield = 0;
		state Future<Void> error = self->brokenOnDestruct.getFuture();
		state int maxPriority = self->waiters.size() - 1;

		// Priority to try to run tasks from next
		state int priority = maxPriority;
		state int ioLaunchLimit = self->launchLimits[priority];
		state Queue* pQueue = &self->waiters[maxPriority];

		// Track the number of waiters unlocked at the same priority in a row
		state int lastPriorityCount = 0;

		loop {
			// Cleanup finished runner futures at the front of the runner queue.
			while (!self->runners.empty() && self->runners.front().isReady()) {
				self->runners.pop_front();
			}

			// Wait for a runner to release its lock
			wait(self->release.onTrigger());

			if (++sinceYield == 1000) {
				sinceYield = 0;
				wait(delay(0));
			}

			// While there are available slots and there are waiters, launch tasks
			while (self->available > 0 && self->waiting > 0) {

				while (!pQueue->empty() && lastPriorityCount++ < ioLaunchLimit) {
					Waiter w = pQueue->front();
					pQueue->pop_front();
					--self->waiting;
					Lock lock;

					w.lockPromise.send(lock);

					// Self may have been destructed during the lock callback
					if (error.isReady()) {
						throw error.getError();
					}

					// If the lock was not already released, add it to the runners future queue
					if (lock.promise.canBeSet()) {
						self->addRunner(lock, priority);

						// A slot has been consumed, so stop reading from this queue if there aren't any more
						if (self->available == 0) {
							break;
						}
					}
				}

				// If there are no more slots available, then don't move to the next priority
				if (self->available == 0) {
					break;
				}

				// Decrease priority, wrapping around to max from 0
				if (priority == 0) {
					priority = maxPriority;
				} else {
					--priority;
				}

				// Set launch limit to configured limit for the new priority to launch from
				ioLaunchLimit = self->launchLimits[priority];

				// If there are waiters at other priority levels, then reduce the launch limit by the number of
				// runners for priority, possibly reducing it all the way to 0.
				if (self->numWaiters(priority) < self->waiting) {
					ioLaunchLimit = std::max(0, self->numRunners(priority));
				}

				pQueue = &self->waiters[priority];
				lastPriorityCount = 0;
			}
		}
	}
};

#include "flow/unactorcompiler.h"

#endif
