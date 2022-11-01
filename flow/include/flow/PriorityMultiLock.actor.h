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

#define PRIORITYMULTILOCK_DEBUG 0

#if PRIORITYMULTILOCK_DEBUG || !defined(NO_INTELLISENSE)
#define pml_debug_printf(...)                                                                                          \
	if (now() > 0)                                                                                                     \
	printf(__VA_ARGS__)
#else
#define pml_debug_printf(...)
#endif

// A multi user lock with a concurrent holder limit where waiters request a lock with a priority
// id and are granted locks based on a total concurrency and relative weights of the current active
// priorities.  Priority id's must start at 0 and are sequential integers.
//
// Scheduling logic
// Let
// 	 weights[n] = configured weight for priority n
//   waiters[n] = the number of waiters for priority n
//   runnerCounts[n] = number of runners at priority n
//
//   totalPendingWeights = sum of weights for all priorities with waiters[n] > 0
//   When waiters[n] becomes == 0, totalPendingWeights -= weights[n]
//   When waiters[n] becomes  > 0, totalPendingWeights += weights[n]
//
//   The total capacity of a priority to be considered when launching tasks is
//     ceil(weights[n] / totalPendingWeights * concurrency)
//
// For improved memory locality the properties mentioned above are stored as priorities[n].<property>
// in the actual implementation.
//
// The interface is similar to FlowMutex except that lock holders can just drop the lock to release it.
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

	PriorityMultiLock(int concurrency, std::string weights)
	  : PriorityMultiLock(concurrency, parseStringToVector<int>(weights, ',')) {}

	PriorityMultiLock(int concurrency, std::vector<int> weightsByPriority)
	  : concurrency(concurrency), available(concurrency), waiting(0), totalPendingWeights(0), releaseDebugID(0) {

		priorities.resize(weightsByPriority.size());
		for (int i = 0; i < priorities.size(); ++i) {
			priorities[i].weight = weightsByPriority[i];
		}

		fRunner = runner(this);
	}

	~PriorityMultiLock() { kill(); }

	Future<Lock> lock(int priority = 0,
	                  TaskPriority flowDelayPriority = TaskPriority::DefaultEndpoint,
	                  int64_t userTag = 0) {
		Priority& p = priorities[priority];
		Queue& q = p.queue;
		Waiter w({ flowDelayPriority, userTag });

		// If this priority currently has no waiters
		if (q.empty()) {
			// Add this priority's weight to the total for priorities with pending work
			totalPendingWeights += p.weight;

			// If there are slots available and the priority has capacity then don't make the caller wait
			if (available > 0 && p.runners < currentCapacity(p.weight)) {
				// Remove this priority's weight from the total since it will remain empty
				totalPendingWeights -= p.weight;

				// Return a Lock to the caller
				Lock lock;
				addRunner(lock, w.task, &p);

				pml_debug_printf("lock nowait line %d priority %d  %s\n", __LINE__, priority, toString().c_str());
				return lock;
			}
		}

		q.push_back(w);
		++waiting;

		pml_debug_printf("lock wait line %d priority %d  %s\n", __LINE__, priority, toString().c_str());
		return w.lockPromise.getFuture();
	}

	void kill() {
		brokenOnDestruct.reset();
		// handleRelease will not free up any execution slots when it ends via cancel
		fRunner.cancel();
		available = 0;
		runners.clear();
		priorities.clear();
	}

	std::string toString() const {
		int runnersDone = 0;
		for (int i = 0; i < runners.size(); ++i) {
			if (runners[i].taskFuture.isReady()) {
				++runnersDone;
			}
		}

		std::string s = format("{ ptr=%p concurrency=%d available=%d running=%d waiting=%d runnersQueue=%d "
		                       "runnersDone=%d pendingWeights=%d ",
		                       this,
		                       concurrency,
		                       available,
		                       concurrency - available,
		                       waiting,
		                       runners.size(),
		                       runnersDone,
		                       totalPendingWeights);

		for (int i = 0; i < priorities.size(); ++i) {
			s += format("p%d:{%s} ", i, priorities[i].toString(this).c_str());
		}

		s += "}";

		if (concurrency - available != runners.size() - runnersDone) {
			pml_debug_printf("%s\n", s.c_str());
			ASSERT_EQ(concurrency - available, runners.size() - runnersDone);
		}

		return s;
	}

	int maxPriority() const { return priorities.size() - 1; }

	int totalWaiters() const { return waiting; }

	int numWaiters(const unsigned int priority) const {
		ASSERT(priority < priorities.size());
		return priorities[priority].queue.size();
	}

	int totalRunners() const { return concurrency - available; }

	int numRunners(const unsigned int priority) const {
		ASSERT(priority < priorities.size());
		return priorities[priority].runners;
	}

private:
	struct Task {
		TaskPriority flowDelayPriority;
		int64_t userTag;
	};

	struct Waiter {
		Waiter(Task task) : task(task) {}
		Promise<Lock> lockPromise;
		Task task;
	};

	struct Runner {
		Task task;
		Future<Void> taskFuture;
	};

	// Total execution slots allowed across all priorities
	int concurrency;
	// Current available execution slots
	int available;
	// Total waiters across all priorities
	int waiting;
	// Sum of weights for all priorities with 1 or more waiters
	int totalPendingWeights;

	typedef Deque<Waiter> Queue;

	struct Priority {
		Priority() : runners(0), weight(0) {}

		// Queue of waiters at this priority
		Queue queue;
		// Number of runners at this priority
		int runners;
		// Configured weight for this priority
		int weight;

		std::string toString(const PriorityMultiLock* pml) const {
			return format("weight=%d run=%d wait=%d cap=%d",
			              weight,
			              runners,
			              queue.size(),
			              queue.empty() ? 0 : pml->currentCapacity(weight));
		}
	};

	std::vector<Priority> priorities;

	// Current or recent (ended) runners
	Deque<Runner> runners;

	Future<Void> fRunner;
	AsyncTrigger wakeRunner;
	Promise<Void> brokenOnDestruct;

	// Used for debugging, can roll over without issue
	unsigned int releaseDebugID;

	ACTOR static Future<Void> handleRelease(PriorityMultiLock* self, Future<Void> f, Priority* priority) {
		state [[maybe_unused]] unsigned int id = self->releaseDebugID++;

		pml_debug_printf("%f handleRelease self=%p id=%u start \n", now(), self, id);
		try {
			wait(f);
			pml_debug_printf("%f handleRelease self=%p id=%u success\n", now(), self, id);
		} catch (Error& e) {
			pml_debug_printf("%f handleRelease self=%p id=%u error %s\n", now(), self, id, e.what());
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
		}

		pml_debug_printf("lock release line %d priority %d  %s\n",
		                 __LINE__,
		                 (int)(priority - &self->priorities.front()),
		                 self->toString().c_str());

		pml_debug_printf("%f handleRelease self=%p id=%u releasing\n", now(), self, id);
		++self->available;
		priority->runners -= 1;

		// If there are any waiters or if the runners array is getting large, trigger the runner loop
		if (self->waiting > 0 || self->runners.size() > 1000) {
			self->wakeRunner.trigger();
		}
		return Void();
	}

	void addRunner(Lock& lock, const Task& task, Priority* p) {
		p->runners += 1;
		--available;
		runners.push_back({ task, handleRelease(this, lock.promise.getFuture(), p) });
	}

	// Current maximum running tasks for the specified priority, which must have waiters
	// or the result is undefined
	int currentCapacity(int weight) const {
		// The total concurrency allowed for this priority at present is the total concurrency times
		// priority's weight divided by the total weights for all priorities with waiters.
		return ceil((float)weight / totalPendingWeights * concurrency);
	}

	ACTOR static Future<Void> runner(PriorityMultiLock* self) {
		state Future<Void> error = self->brokenOnDestruct.getFuture();

		// Priority to try to run tasks from next
		state int priority = 0;

		loop {
			pml_debug_printf(
			    "runner loop start line %d  priority=%d  %s\n", __LINE__, priority, self->toString().c_str());

			// Cleanup finished runner futures at the front of the runner queue.
			while (!self->runners.empty() && self->runners.front().taskFuture.isReady()) {
				self->runners.pop_front();
			}

			// Wait for a runner to release its lock
			pml_debug_printf(
			    "runner loop waitTrigger line %d  priority=%d  %s\n", __LINE__, priority, self->toString().c_str());
			wait(self->wakeRunner.onTrigger());
			pml_debug_printf(
			    "%f runner loop wake line %d  priority=%d  %s\n", now(), __LINE__, priority, self->toString().c_str());

			// While there are available slots and there are waiters, launch tasks
			while (self->available > 0 && self->waiting > 0) {
				pml_debug_printf(
				    "  launch loop start line %d  priority=%d  %s\n", __LINE__, priority, self->toString().c_str());

				Priority* pPriority;

				// Find the next priority with waiters and capacity.  There must be at least one.
				loop {
					// Rotate to next priority
					if (++priority == self->priorities.size()) {
						priority = 0;
					}

					pPriority = &self->priorities[priority];

					pml_debug_printf("    launch loop scan line %d  priority=%d  %s\n",
					                 __LINE__,
					                 priority,
					                 self->toString().c_str());

					if (!pPriority->queue.empty() && pPriority->runners < self->currentCapacity(pPriority->weight)) {
						break;
					}
				}

				Queue& queue = pPriority->queue;

				Waiter w = queue.front();
				queue.pop_front();

				// If this priority is now empty, subtract its weight from the total pending weights
				if (queue.empty()) {
					self->totalPendingWeights -= pPriority->weight;

					pml_debug_printf("      emptied priority line %d  priority=%d  %s\n",
					                 __LINE__,
					                 priority,
					                 self->toString().c_str());
				}

				--self->waiting;
				Lock lock;

				w.lockPromise.send(lock);

				// Self may have been destructed during the lock callback
				if (error.isReady()) {
					throw error.getError();
				}

				// If the lock was not already released, add it to the runners future queue
				if (lock.promise.canBeSet()) {
					self->addRunner(lock, w.task, pPriority);
				}

				pml_debug_printf("    launched line %d alreadyDone=%d priority=%d  %s\n",
				                 __LINE__,
				                 !lock.promise.canBeSet(),
				                 priority,
				                 self->toString().c_str());

				// If the task returned the lock immediately and did not wait, then delay to let the Flow run loop
				// schedule other tasks if needed
				if (!lock.promise.canBeSet()) {
					wait(delay(0, w.task.flowDelayPriority));
				}
			}
		}
	}
};

#include "flow/unactorcompiler.h"

#endif
