/*
 * sim2.actor.cpp
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

//#include <cinttypes>
//#include <memory>
//#include <string>

#include "fdbrpc/simulator.h"
//#include "flow/Arena.h"
//#define BOOST_SYSTEM_NO_LIB
//#define BOOST_DATE_TIME_NO_LIB
//#define BOOST_REGEX_NO_LIB
//#include "fdbrpc/SimExternalConnection.h"
//#include "flow/ActorCollection.h"
//#include "flow/IRandom.h"
//#include "flow/IThreadPool.h"
//#include "flow/ProtocolVersion.h"
//#include "flow/Util.h"
//#include "flow/WriteOnlySet.h"
//#include "flow/IAsyncFile.h"
//#include "fdbrpc/AsyncFileCached.actor.h"
//#include "fdbrpc/AsyncFileEncrypted.h"
//#include "fdbrpc/AsyncFileNonDurable.actor.h"
//#include "fdbrpc/AsyncFileChaos.h"
//#include "crc32/crc32c.h"
//#include "fdbrpc/TraceFileIO.h"
//#include "flow/FaultInjection.h"
#include "flow/flow.h"
//#include "flow/genericactors.actor.h"
#include "flow/network.h"
#include "flow/TLSConfig.actor.h"
//#include "fdbrpc/Net2FileSystem.h"
//#include "fdbrpc/Replication.h"
//#include "fdbrpc/ReplicationUtils.h"
//#include "fdbrpc/AsyncFileWriteChecker.h"
#include "flow/swift_net2_hooks.h"
#include "flow/swift.h"
#include "flow/swift/ABI/Task.h"
//#include "flow/actorcompiler.h" // This must be the last #include.
//
//
//// ==== ----------------------------------------------------------------------------------------------------------------
//// ==== Sim2 hooks
//
////struct SwiftSim2JobTask final : public Sim2::Task {
////  swift::Job* job;
////  explicit SwiftJobTask(swift::Job* job) noexcept : job(job) {
////    printf("[c++][sim][job:%p] prepare job\n", job);
////  }
////
////  void operator()() override {
////    printf("[c++][sim][job:%p] run job (priority: %zu)\n", job, job->getPriority());
////
////    swift_job_run(job, ExecutorRef::generic());
////    delete this;
////  }
////};
//
SWIFT_CC(swift)
void sim2_enqueueGlobal_hook_impl(swift::Job* _Nonnull job,
                                  void (*_Nonnull)(swift::Job*) __attribute__((swiftcall))) {
  ISimulator* sim = g_pSimulator;
  ASSERT(sim);

  printf("[c++][sim2][%s:%d](%s) intercepted job enqueue: %p to g_network (%p)\n", __FILE_NAME__, __LINE__, __FUNCTION__, job, sim);

//  auto swiftPriority = job->getPriority();
//  printf("[c++][%s:%d](%s) sim2_enqueueGlobal_hook_impl - swift task priority: %d\n", __FILE_NAME__, __LINE__, __FUNCTION__, static_cast<std::underlying_type<TaskPriority>::type>(swiftPriority));
//  int64_t priority = swift_priority_to_net2(swiftPriority); // default to lowest "Min"
//  printf("[c++][%s:%d](%s) sim2_enqueueGlobal_hook_impl - swift task priority: %d -> %d\n", __FILE_NAME__, __LINE__, __FUNCTION__, static_cast<std::underlying_type<TaskPriority>::type>(swiftPriority), static_cast<std::underlying_type<TaskPriority>::type>(priority));
//
//  TaskPriority taskID = TaskPriority::DefaultOnMainThread; // FIXME: how to determine
//
//  SwiftJobTask* jobTask = new SwiftJobTask(job);
//  N2::OrderedTask* orderedTask = new N2::OrderedTask(priority, taskID, jobTask);

  sim->_swiftEnqueue(job);
}
