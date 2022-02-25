/*
 * TesterWorkload.h
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

#pragma once

#ifndef APITESTER_WORKLOAD_H
#define APITESTER_WORKLOAD_H

#include "TesterTransactionExecutor.h"
#include <atomic>
#include <unordered_map>
#include <mutex>

namespace FdbApiTester {

class WorkloadManager;

class IWorkload {
public:
	virtual ~IWorkload() {}
	virtual void init(WorkloadManager* manager) = 0;
	virtual void start() = 0;
};

class WorkloadBase : public IWorkload {
public:
	WorkloadBase() : manager(nullptr), tasksScheduled(0), txRunning(0) {}
	void init(WorkloadManager* manager) override;

protected:
	void schedule(TTaskFct task);
	void execTransaction(std::shared_ptr<ITransactionActor> tx, TTaskFct cont);
	void execTransaction(TTxStartFct start, TTaskFct cont) {
		execTransaction(std::make_shared<TransactionFct>(start), cont);
	}
	void checkIfDone();

private:
	WorkloadManager* manager;
	std::atomic<int> tasksScheduled;
	std::atomic<int> txRunning;
};

class WorkloadManager {
public:
	WorkloadManager(ITransactionExecutor* txExecutor, IScheduler* scheduler)
	  : txExecutor(txExecutor), scheduler(scheduler) {}

	void add(std::shared_ptr<IWorkload> workload, TTaskFct cont = NO_OP_TASK);
	void run();

private:
	friend WorkloadBase;

	struct WorkloadInfo {
		std::shared_ptr<IWorkload> ref;
		TTaskFct cont;
	};

	void workloadDone(IWorkload* workload);

	ITransactionExecutor* txExecutor;
	IScheduler* scheduler;

	std::mutex mutex;
	std::unordered_map<IWorkload*, WorkloadInfo> workloads;
};

} // namespace FdbApiTester

#endif