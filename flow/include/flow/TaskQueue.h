/*
 * TaskQueue.h
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

#ifndef FLOW_TASK_QUEUE_H
#define FLOW_TASK_QUEUE_H
#pragma once

#include <queue>
#include <vector>
#include "flow/TDMetric.h"
#include "flow/network.h"
#include "flow/ThreadSafeQueue.h"

template <typename Task>
// A queue of ordered tasks, both ready to execute, and delayed for later execution.
// All functions must be called on the main thread, except for addReadyThreadSafe() which can be called from any thread.
class TaskQueue {
public:
	TaskQueue() : tasksIssued(0), ready(FLOW_KNOBS->READY_QUEUE_RESERVED_SIZE) {}

	// Add a task that is ready to be executed.
	void addReady(TaskPriority taskId, Task* t) { this->ready.push(OrderedTask(getFIFOPriority(taskId), taskId, t)); }
	// Add a task to be executed at a given future time instant (a "timer").
	void addTimer(double at, TaskPriority taskId, Task* t) {
		this->timers.push(DelayedTask(at, getFIFOPriority(taskId), taskId, t));
	}
	// Add a task that is ready to be executed, potentially called from a thread that is different from main.
	// Returns true iff the main thread need to be woken up to execute this task.
	bool addReadyThreadSafe(bool isMainThread, TaskPriority taskID, Task* t) {
		if (isMainThread) {
			processThreadReady();
			addReady(taskID, t);
		} else {
			if (threadReady.push(std::make_pair(taskID, t)))
				return true;
		}
		return false;
	}
	// Returns true if the there are no tasks that are ready to be executed.
	bool canSleep() {
		bool b = ready.empty();
		if (b) {
			b = threadReady.canSleep();
			if (!b)
				++countCantSleep;
		} else {
			++countWontSleep;
		}
		return b;
	}
	// Returns a time interval a caller should sleep from now until the next timer.
	double getSleepTime(double now) const {
		if (!timers.empty()) {
			return timers.top().at - now;
		}
		return 0;
	}

	// Moves all timers that are scheduled to be executed at or before now to the ready queue.
	void processReadyTimers(double now) {
		[[maybe_unused]] int numTimers = 0;
		while (!timers.empty() && timers.top().at <= now + INetwork::TIME_EPS) {
			++numTimers;
			++countTimers;
			ready.push(timers.top());
			timers.pop();
		}
		FDB_TRACE_PROBE(run_loop_ready_timers, numTimers);
	}

	// Moves all tasks scheduled from a different thread to the ready queue.
	void processThreadReady() {
		[[maybe_unused]] int numReady = 0;
		while (true) {
			Optional<std::pair<TaskPriority, Task*>> t = threadReady.pop();
			if (!t.present())
				break;
			ASSERT(t.get().second != nullptr);
			addReady(t.get().first, t.get().second);
			++numReady;
		}
		FDB_TRACE_PROBE(run_loop_thread_ready, numReady);
	}

	bool hasReadyTask() const { return !ready.empty(); }
	size_t getNumReadyTasks() const { return ready.size(); }
	TaskPriority getReadyTaskID() const { return ready.top().taskID; }
	int64_t getReadyTaskPriority() const { return ready.top().priority; }
	Task* getReadyTask() const { return ready.top().task; }
	void popReadyTask() { ready.pop(); }

	void initMetrics() {
		countTimers.init("Net2.CountTimers"_sr);
		countCantSleep.init("Net2.CountCantSleep"_sr);
		countWontSleep.init("Net2.CountWontSleep"_sr);
	}

	void clear() {
#ifdef ADDRESS_SANITIZER
		// Under ASAN, properly free pending tasks so LeakSanitizer doesn't report
		// them. Destroying a PromiseTask fires broken_promise on waiting futures,
		// which cancels associated actors and frees their coroutine frames.
		// Only done in ASAN builds because broken_promise during shutdown can
		// trigger SevError logging in server-side actors that don't expect it.

		// Swap out queues first so any callbacks triggered during cleanup
		// add new tasks to the live (empty) queues, not the ones we're draining.
		// This prevents infinite loops from actors that catch broken_promise and retry.
		ReadyQueue<OrderedTask> oldReady(0);
		ready.swap(oldReady);
		decltype(timers) oldTimers;
		timers.swap(oldTimers);

		while (!oldTimers.empty()) {
			delete oldTimers.top().task;
			oldTimers.pop();
		}
		while (!oldReady.empty()) {
			delete oldReady.top().task;
			oldReady.pop();
		}
		// Drain cross-thread queue. ThreadSafeQueue::pop() can transiently return
		// empty if a producer is between head.exchange() and prev->next.store() in
		// push(). We drain in a loop: if we got at least one item, retry in case
		// more are in-flight. Exit only after a full pass finds nothing.
		bool found;
		do {
			found = false;
			while (true) {
				Optional<std::pair<TaskPriority, Task*>> t = threadReady.pop();
				if (!t.present())
					break;
				delete t.get().second;
				found = true;
			}
		} while (found);
#else
		decltype(ready) _1;
		ready.swap(_1);
		decltype(timers) _2;
		timers.swap(_2);
#endif
	}

private:
	struct OrderedTask {
		int64_t priority;
		TaskPriority taskID;
		Task* task;
		OrderedTask(int64_t priority, TaskPriority taskID, Task* task)
		  : priority(priority), taskID(taskID), task(task) {}
		bool operator<(OrderedTask const& rhs) const { return priority < rhs.priority; }
	};

	struct DelayedTask : OrderedTask {
		double at;
		DelayedTask(double at, int64_t priority, TaskPriority taskID, Task* task)
		  : OrderedTask(priority, taskID, task), at(at) {}
		bool operator<(DelayedTask const& rhs) const { return at > rhs.at; } // Ordering is reversed for priority_queue
	};

	template <class T>
	class ReadyQueue : public std::priority_queue<T, std::vector<T>> {
	public:
		using size_type = typename std::priority_queue<T, std::vector<T>>::size_type;
		explicit ReadyQueue(size_type capacity = 0) { reserve(capacity); };
		void reserve(size_type capacity) { this->c.reserve(capacity); }
	};

	// Returns a unique priority value for a task which preserves FIFO ordering
	// for tasks with the same priority.
	int64_t getFIFOPriority(TaskPriority taskId) { return (int64_t(taskId) << 32) - (++tasksIssued); }
	uint64_t tasksIssued;

	ReadyQueue<OrderedTask> ready;
	ThreadSafeQueue<std::pair<TaskPriority, Task*>> threadReady;

	std::priority_queue<DelayedTask, std::vector<DelayedTask>> timers;

	Int64MetricHandle countTimers;
	Int64MetricHandle countCantSleep;
	Int64MetricHandle countWontSleep;
};

#endif /* FLOW_TASK_QUEUE_H */
