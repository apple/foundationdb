/*
 * swift_concurrency_hooks.cpp
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

#include "flow/swift_concurrency_hooks.h"
#include "flow/swift.h"
#include "flow/swift/ABI/Task.h"
#include "flow/TLSConfig.actor.h"

// ==== ----------------------------------------------------------------------------------------------------------------

// TODO: Bridge the scheduled task from Swift into DelayedTask
// struct SwiftDelayedOrderedTask : OrderedTask {
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
	swift::Job* job;
	explicit SwiftJobTask(swift::Job* job) noexcept : job(job) { printf("[c++][job:%p] prepare job\n", job); }

	void operator()() override {
		printf("[c++][job:%p] run job (priority: %zu)\n", job, job->getPriority());

		swift_job_run(job, ExecutorRef::generic());
		delete this;
	}
};

// ==== ----------------------------------------------------------------------------------------------------------------

double flow_gNetwork_now() {
	return g_network->now();
}

Future<class Void> flow_gNetwork_delay(double seconds, TaskPriority taskID) {
	return g_network->delay(seconds, taskID);
}


// ==== ----------------------------------------------------------------------------------------------------------------
// ==== Net2 hooks

//void net2_swift_task_enqueueGlobal(swift::Job* job, swift_task_enqueueGlobal_original _Nonnull original) {
// ...
//}

//void net2_swift_task_enqueueGlobalWithDelay(JobDelay delay, swift::Job* job) {
// ...
//}

SWIFT_CC(swift)
void net2_enqueueGlobal_hook_impl(swift::Job* _Nonnull job,
                                  //                              swift_task_enqueueGlobal_original _Nonnull original) {
                                  void (*_Nonnull)(swift::Job*) __attribute__((swiftcall))) {
	// auto net = N2::g_net2; // TODO: can't access Net2 since it's incomplete here, would be nicer to not expose API on
	// INetwork I suppose
	auto net = g_network; // TODO: can't access Net2 since it's incomplete here, would be nicer to not expose API on
	                      // INetwork I suppose
	ASSERT(net);

	printf("[c++][%s:%d](%s) intercepted job enqueue: %p to g_network (%p)\n", __FILE_NAME__, __LINE__, __FUNCTION__, job, net);

	auto swiftPriority = job->getPriority();
	printf("[c++][%s:%d](%s) net2_enqueueGlobal_hook_impl - swift task priority: %d\n", __FILE_NAME__, __LINE__, __FUNCTION__, static_cast<std::underlying_type<TaskPriority>::type>(swiftPriority));
	int64_t priority = swift_priority_to_net2(swiftPriority); // default to lowest "Min"
	printf("[c++][%s:%d](%s) net2_enqueueGlobal_hook_impl - swift task priority: %d -> %d\n", __FILE_NAME__, __LINE__, __FUNCTION__, static_cast<std::underlying_type<TaskPriority>::type>(swiftPriority), static_cast<std::underlying_type<TaskPriority>::type>(priority));

	TaskPriority taskID = TaskPriority::DefaultOnMainThread; // FIXME: how to determine

	SwiftJobTask* jobTask = new SwiftJobTask(job);
	N2::OrderedTask* orderedTask = new N2::OrderedTask(priority, taskID, jobTask);

	net->_swiftEnqueue(orderedTask);
}

void swift_job_run_generic(swift::Job* _Nonnull job) {
	swift_job_run(job, ExecutorRef::generic());
}

