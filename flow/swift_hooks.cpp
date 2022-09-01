/*
 * swift_hooks.cpp
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

#include "flow/swift.h"
#include "flow/swift_hooks.h"

// ==== ----------------------------------------------------------------------------------------------------------------

void installGlobalSwiftConcurrencyHooks(void* net2) {
}



// ==== ----------------------------------------------------------------------------------------------------------------
// ==== ----------------------------------------------------------------------------------------------------------------


// TODO: Bridge the scheduled task from Swift into DelayedTask
//struct SwiftDelayedOrderedTask : OrderedTask {
//    double at;
//    SwiftJobTask(double at, int64_t priority, TaskPriority taskID, Task* task)
//            : OrderedTask(priority, taskID, task), at(at) {}
//
//    static DelayedTask *make(double at, int64_t priority, TaskPriority taskID, Job* swiftJob) {
//        new DelayedTask(at, priority, taskID, )
//    }
//
//    bool operator<(DelayedTask const& rhs) const { return at > rhs.at; } // Ordering is reversed for priority_queue
//};

struct SwiftJobTask final : public N2::Task, public FastAllocated<SwiftJobTask> {
	Job *job;
	explicit SwiftJobTask(Job* job) noexcept : job(job) {}

	void operator()() override {
		job->runInFullyEstablishedContext(); // FIXME: not right; how to actually "just run"
		delete this;
	}
};


// SWIFT_CC(swift)
// void ((* _Nullable))(Job *, swift_task_enqueueGlobal_original _Nonnull) __attribute__((swiftcall))'
// AKA
// void (*)(Job *, void (* _Nonnull)(Job *) __attribute__((swiftcall))) __attribute__((swiftcall))

// void (Job *, swift_task_enqueueGlobal_original _Nonnull)
// AKA
// void (Job *, void (* _Nonnull)(Job *) __attribute__((swiftcall)))

void net2_swift_task_enqueueGlobal(Job *job,
                                   swift_task_enqueueGlobal_original _Nonnull original) {
	N2::Net2 *net = N2::g_net2;
	ASSERT(net);

	double at = net->now();
	int64_t priority = 1; // FIXME: how to determine
	TaskPriority taskID; // FIXME: how to determine

	SwiftJobTask *jobTask = new SwiftJobTask(job);
	N2::OrderedTask orderedTask = N2::OrderedTask(priority, taskID, jobTask);
	//    net->threadReady.push(orderedTask);

	net->ready.push(orderedTask);

	assert(false && "just mocking out APIs");
}

void net2_swift_task_enqueueGlobalWithDelay(JobDelay delay, Job *job) {
	N2::Net2 *net2 = N2::g_net2;
	ASSERT(net2);
	//
	//    N2::Task *taskPtr;
	//
	//    double at = net2->now() + 0.0; // FIXME, instead add the JobDelay here
	//    int64_t priority = 0; // FIXME: how to set this
	//    int64_t taskID = 111; // FIXME: how to set this
	//    auto delayedTask = N2::Net2::DelayedTask(
	//            /*at=*/at,
	//            /*priority=*/priority,
	//            /*taskID=*/taskID,
	//            taskPtr);

	ASSERT(false && "just mocking out APIs");
}

void N2::Net2::installSwiftConcurrencyHooks() {
	// swift_task_enqueueGlobal_hook = net2_swift_task_enqueueGlobal; // FIXME: slight type issues still
}