/*
 * TesterWorkload.h
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

#include <memory>
#ifndef APITESTER_WORKLOAD_H
#define APITESTER_WORKLOAD_H

#include "TesterTransactionExecutor.h"
#include "TesterUtil.h"
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

struct WorkloadConfig {
	std::string name;
	int clientId;
	int numClients;
	std::unordered_map<std::string, std::string> options;

	int getIntOption(const std::string& name, int defaultVal) const;
	double getFloatOption(const std::string& name, double defaultVal) const;
};

// A base class for test workloads
// Tracks if workload is active, notifies the workload manager when the workload completes
class WorkloadBase : public IWorkload {
public:
	WorkloadBase(const WorkloadConfig& config);
	void init(WorkloadManager* manager) override;

protected:
	// Schedule the a task as a part of the workload
	void schedule(TTaskFct task);

	// Execute a transaction within the workload
	void execTransaction(std::shared_ptr<ITransactionActor> tx, TTaskFct cont, bool failOnError = true);

	// Execute a transaction within the workload, a convenience method for tranasactions defined by a single lambda
	void execTransaction(TTxStartFct start, TTaskFct cont, bool failOnError = true) {
		execTransaction(std::make_shared<TransactionFct>(start), cont, failOnError);
	}

	// Log an error message
	void error(const std::string& msg);

	// Log an info message
	void info(const std::string& msg);

private:
	WorkloadManager* manager;

	// Check if workload is done and notify the workload manager
	void checkIfDone();

	// Keep track of tasks scheduled by the workload
	// End workload when this number falls to 0
	std::atomic<int> tasksScheduled;
	std::atomic<int> numErrors;

protected:
	int clientId;
	int numClients;
	int maxErrors;
	std::string workloadId;
	std::atomic<bool> failed;
};

// Workload manager
// Keeps track of active workoads, stops the scheduler after all workloads complete
class WorkloadManager {
public:
	WorkloadManager(ITransactionExecutor* txExecutor, IScheduler* scheduler)
	  : txExecutor(txExecutor), scheduler(scheduler), numWorkloadsFailed(0) {}

	// Add a workload
	// A continuation is to be specified for subworkloads
	void add(std::shared_ptr<IWorkload> workload, TTaskFct cont = NO_OP_TASK);

	// Run all workloads. Blocks until all workloads complete
	void run();

	bool failed() {
		std::unique_lock<std::mutex> lock(mutex);
		return numWorkloadsFailed > 0;
	}

private:
	friend WorkloadBase;

	struct WorkloadInfo {
		std::shared_ptr<IWorkload> ref;
		TTaskFct cont;
	};

	void workloadDone(IWorkload* workload, bool failed);

	ITransactionExecutor* txExecutor;
	IScheduler* scheduler;

	std::mutex mutex;
	std::unordered_map<IWorkload*, WorkloadInfo> workloads;
	int numWorkloadsFailed;
};

struct IWorkloadFactory {
	static std::shared_ptr<IWorkload> create(std::string const& name, const WorkloadConfig& config);
	static std::unordered_map<std::string, IWorkloadFactory*>& factories();

	virtual ~IWorkloadFactory() = default;
	virtual std::shared_ptr<IWorkload> create(const WorkloadConfig& config) = 0;
};

template <class WorkloadType>
struct WorkloadFactory : IWorkloadFactory {
	WorkloadFactory(const char* name) { factories()[name] = this; }
	std::shared_ptr<IWorkload> create(const WorkloadConfig& config) override {
		return std::make_shared<WorkloadType>(config);
	}
};

} // namespace FdbApiTester

#endif