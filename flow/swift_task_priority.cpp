/*
 * swift_concurrency_hooks.cpp
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

#include "flow/swift.h"
#include "flow/swift_concurrency_hooks.h"
#include "flow/swift/ABI/Task.h"
#include "flow/TLSConfig.actor.h"

// FIXME: surely there must be some more automatic way to maintain the mappings with Swift/C++ interop.
TaskPriority swift_priority_to_net2(swift::JobPriority p) {
	// printf("[c++][%s:%d](%s) converting a priority (priority: %zu)\n", __FILE_NAME__, __LINE__, __FUNCTION__, p);

	TaskPriority fp = TaskPriority::Zero;
	switch (static_cast<std::underlying_type<swift::JobPriority>::type>(p)) {
	case 255:
		fp = TaskPriority::Max;
		break;
	case 200:
		fp = TaskPriority::RunLoop;
		break;
	case 173:
		fp = TaskPriority::ASIOReactor;
		break;
	case 73:
		fp = TaskPriority::RunCycleFunction;
		break;
	case 72:
		fp = TaskPriority::FlushTrace;
		break;
	case 71:
		fp = TaskPriority::WriteSocket;
		break;
	case 70:
		fp = TaskPriority::PollEIO;
		break;
	case 69:
		fp = TaskPriority::DiskIOComplete;
		break;
	case 68:
		fp = TaskPriority::LoadBalancedEndpoint;
		break;
	case 67:
		fp = TaskPriority::ReadSocket;
		break;
	case 66:
		fp = TaskPriority::AcceptSocket;
		break;
	case 65:
		fp = TaskPriority::Handshake;
		break;
	case 64:
		fp = TaskPriority::CoordinationReply;
		break;
	case 63:
		fp = TaskPriority::Coordination;
		break;
	case 62:
		fp = TaskPriority::FailureMonitor;
		break;
	case 61:
		fp = TaskPriority::ResolutionMetrics;
		break;
	case 60:
		fp = TaskPriority::Worker;
		break;
	case 59:
		fp = TaskPriority::ClusterControllerWorker;
		break;
	case 58:
		fp = TaskPriority::ClusterControllerRecruit;
		break;
	case 57:
		fp = TaskPriority::ClusterControllerRegister;
		break;
	case 56:
		fp = TaskPriority::ClusterController;
		break;
	case 55:
		fp = TaskPriority::MasterTLogRejoin;
		break;
	case 54:
		fp = TaskPriority::ProxyStorageRejoin;
		break;
	case 53:
		fp = TaskPriority::TLogQueuingMetrics;
		break;
	case 52:
		fp = TaskPriority::TLogPop;
		break;
	case 51:
		fp = TaskPriority::TLogPeekReply;
		break;
	case 50:
		fp = TaskPriority::TLogPeek;
		break;
	case 49:
		fp = TaskPriority::TLogCommitReply;
		break;
	case 48:
		fp = TaskPriority::TLogCommit;
		break;
	case 47:
		fp = TaskPriority::ReportLiveCommittedVersion;
		break;
	case 46:
		fp = TaskPriority::ProxyGetRawCommittedVersion;
		break;
	case 45:
		fp = TaskPriority::ProxyMasterVersionReply;
		break;
	case 44:
		fp = TaskPriority::ProxyCommitYield2;
		break;
	case 43:
		fp = TaskPriority::ProxyTLogCommitReply;
		break;
	case 42:
		fp = TaskPriority::ProxyCommitYield1;
		break;
	case 41:
		fp = TaskPriority::ProxyResolverReply;
		break;
	case 40:
		fp = TaskPriority::ProxyCommit;
		break;
	case 39:
		fp = TaskPriority::ProxyCommitBatcher;
		break;
	case 38:
		fp = TaskPriority::TLogConfirmRunningReply;
		break;
	case 37:
		fp = TaskPriority::TLogConfirmRunning;
		break;
	case 36:
		fp = TaskPriority::ProxyGRVTimer;
		break;
	case 35:
		fp = TaskPriority::GetConsistentReadVersion;
		break;
	case 34:
		fp = TaskPriority::GetLiveCommittedVersionReply;
		break;
	case 33:
		fp = TaskPriority::GetLiveCommittedVersion;
		break;
	case 32:
		fp = TaskPriority::GetTLogPrevCommitVersion;
		break;
	case 31:
		fp = TaskPriority::UpdateRecoveryTransactionVersion;
		break;
	case 30:
		fp = TaskPriority::DefaultPromiseEndpoint;
		break;
	case 29:
		fp = TaskPriority::DefaultOnMainThread;
		break;
	case 28:
		fp = TaskPriority::DefaultDelay;
		break;
	case 27:
		fp = TaskPriority::DefaultYield;
		break;
	case 26:
		fp = TaskPriority::DiskRead;
		break;
	case 25:
		fp = TaskPriority::DefaultEndpoint;
		break;
	case 24:
		fp = TaskPriority::UnknownEndpoint;
		break;
	case 23:
		fp = TaskPriority::MoveKeys;
		break;
	case 22:
		fp = TaskPriority::DataDistributionLaunch;
		break;
	case 21:
		fp = TaskPriority::Ratekeeper;
		break;
	case 20:
		fp = TaskPriority::DataDistribution;
		break;
	case 19:
		fp = TaskPriority::DataDistributionLow;
		break;
	case 18:
		fp = TaskPriority::DataDistributionVeryLow;
		break;
	case 17:
		fp = TaskPriority::BlobManager;
		break;
	case 16:
		fp = TaskPriority::DiskWrite;
		break;
	case 15:
		fp = TaskPriority::UpdateStorage;
		break;
	case 14:
		fp = TaskPriority::CompactCache;
		break;
	case 13:
		fp = TaskPriority::TLogSpilledPeekReply;
		break;
	case 12:
		fp = TaskPriority::BlobWorkerReadChangeFeed;
		break;
	case 11:
		fp = TaskPriority::BlobWorkerUpdateFDB;
		break;
	case 10:
		fp = TaskPriority::BlobWorkerUpdateStorage;
		break;
	case 9:
		fp = TaskPriority::FetchKeys;
		break;
	case 8:
		fp = TaskPriority::RestoreApplierWriteDB;
		break;
	case 7:
		fp = TaskPriority::RestoreApplierReceiveMutations;
		break;
	case 6:
		fp = TaskPriority::RestoreLoaderFinishVersionBatch;
		break;
	case 5:
		fp = TaskPriority::RestoreLoaderSendMutations;
		break;
	case 4:
		fp = TaskPriority::RestoreLoaderLoadFiles;
		break;
	case 3:
		fp = TaskPriority::LowPriorityRead;
		break;
	case 2:
		fp = TaskPriority::Low;
		break;
	case 1:
		fp = TaskPriority::Min;
		break;
	case 0:
		fp = TaskPriority::Zero;
		break;
	default: {
		printf("[c++]Unknown priority: %zu\n", p);
		abort();
	}
	}
	// return static_cast<std::underlying_type<TaskPriority>::type>(fp);
	return fp;
}
