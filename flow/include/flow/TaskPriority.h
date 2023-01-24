/*
 * TaskPriority.h
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

#ifndef FLOW_TASKPRIORITY_H
#define FLOW_TASKPRIORITY_H

enum class TaskPriority {
	Max = 1000000,
	RunLoop = 30000,
	ASIOReactor = 20001,
	RunCycleFunction = 20000,
	FlushTrace = 10500,
	WriteSocket = 10000,
	PollEIO = 9900,
	DiskIOComplete = 9150,
	LoadBalancedEndpoint = 9000,
	ReadSocket = 9000,
	AcceptSocket = 8950,
	Handshake = 8900,
	CoordinationReply = 8810,
	Coordination = 8800,
	FailureMonitor = 8700,
	ResolutionMetrics = 8700,
	Worker = 8660,
	ClusterControllerWorker = 8656,
	ClusterControllerRecruit = 8654,
	ClusterControllerRegister = 8652,
	ClusterController = 8650,
	MasterTLogRejoin = 8646,
	ProxyStorageRejoin = 8645,
	TLogQueuingMetrics = 8620,
	TLogPop = 8610,
	TLogPeekReply = 8600,
	TLogPeek = 8590,
	TLogCommitReply = 8580,
	TLogCommit = 8570,
	ReportLiveCommittedVersion = 8567,
	ProxyGetRawCommittedVersion = 8565,
	ProxyMasterVersionReply = 8560,
	ProxyCommitYield2 = 8557,
	ProxyTLogCommitReply = 8555,
	ProxyCommitYield1 = 8550,
	ProxyResolverReply = 8547,
	ProxyCommit = 8545,
	ProxyCommitBatcher = 8540,
	TLogConfirmRunningReply = 8530,
	TLogConfirmRunning = 8520,
	ProxyGRVTimer = 8510,
	GetConsistentReadVersion = 8500,
	GetLiveCommittedVersionReply = 8490,
	GetLiveCommittedVersion = 8480,
	GetTLogPrevCommitVersion = 8400,
	UpdateRecoveryTransactionVersion = 8470,
	DefaultPromiseEndpoint = 8000,
	DefaultOnMainThread = 7500,
	DefaultDelay = 7010,
	DefaultYield = 7000,
	DiskRead = 5010,
	DefaultEndpoint = 5000,
	UnknownEndpoint = 4000,
	MoveKeys = 3550,
	DataDistributionLaunch = 3530,
	Ratekeeper = 3510,
	DataDistribution = 3502,
	DataDistributionLow = 3501,
	DataDistributionVeryLow = 3500,
	BlobManager = 3490,
	DiskWrite = 3010,
	UpdateStorage = 3000,
	CompactCache = 2900,
	TLogSpilledPeekReply = 2800,
	SSSpilledChangeFeedReply = 2730,
	BlobWorkerReadChangeFeed = 2720,
	BlobWorkerUpdateFDB = 2710,
	BlobWorkerUpdateStorage = 2700,
	FetchKeys = 2500,
	RestoreApplierWriteDB = 2310,
	RestoreApplierReceiveMutations = 2300,
	RestoreLoaderFinishVersionBatch = 2220,
	RestoreLoaderSendMutations = 2210,
	RestoreLoaderLoadFiles = 2200,
	LowPriorityRead = 2100,
	Low = 2000,

	Min = 1000,
	Zero = 0
};

// These have been given long, annoying names to discourage their use.

inline TaskPriority incrementPriority(TaskPriority p) {
	return static_cast<TaskPriority>(static_cast<uint64_t>(p) + 1);
}

inline TaskPriority decrementPriority(TaskPriority p) {
	return static_cast<TaskPriority>(static_cast<uint64_t>(p) - 1);
}

inline TaskPriority incrementPriorityIfEven(TaskPriority p) {
	return static_cast<TaskPriority>(static_cast<uint64_t>(p) | 1);
}

#endif // FLOW_TASKPRIORITY_H
