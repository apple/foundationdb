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

// Workoad interface
class IWorkload {
public:
	virtual ~IWorkload() {}

	// Intialize the workload
	virtual void init(WorkloadManager* manager) = 0;

	// Start executing the workload
	virtual void start() = 0;
};

// Workload configuration
struct WorkloadConfig {
	// Workoad name
	std::string name;

	// Client ID assigned to the workload (a number from 0 to numClients-1)
	int clientId;

	// Total number of clients
	int numClients;

	// Workload options: as key-value pairs
	std::unordered_map<std::string, std::string> options;

	// Get option of a certain type by name. Throws an exception if the values is of a wrong type
	int getIntOption(const std::string& name, int defaultVal) const;
	double getFloatOption(const std::string& name, double defaultVal) const;
};

// A base class for test workloads
// Tracks if workload is active, notifies the workload manager when the workload completes
class WorkloadBase : public IWorkload {
public:
	WorkloadBase(const WorkloadConfig& config);

	// Initialize the workload
	void init(WorkloadManager* manager) override;

protected:
	// Schedule the a task as a part of the workload
	void schedule(TTaskFct task);

	// Execute a transaction within the workload
	void execTransaction(std::shared_ptr<ITransactionActor> tx, TTaskFct cont, bool failOnError = true);

	// Execute a transaction within the workload, a convenience method for a tranasaction defined by a lambda function
	void execTransaction(TTxStartFct start, TTaskFct cont, bool failOnError = true) {
		execTransaction(std::make_shared<TransactionFct>(start), cont, failOnError);
	}

	// Log an error message, increase error counter
	void error(const std::string& msg);

	// Log an info message
	void info(const std::string& msg);

private:
	WorkloadManager* manager;

	// Decrease scheduled task counter, notify the workload manager
	// that the task is done if no more tasks schedule
	void scheduledTaskDone();

	// Keep track of tasks scheduled by the workload
	// End workload when this number falls to 0
	std::atomic<int> tasksScheduled;

	// Number of errors logged
	std::atomic<int> numErrors;

protected:
	// Client ID assigned to the workload (a number from 0 to numClients-1)
	int clientId;

	// Total number of clients
	int numClients;

	// The maximum number of errors before stoppoing the workload
	int maxErrors;

	// Workload identifier, consisting of workload name and client ID
	std::string workloadId;

	// Workload is failed, no further transactions or continuations will be scheduled by the workload
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

	// True if at least one workload has failed
	bool failed() {
		std::unique_lock<std::mutex> lock(mutex);
		return numWorkloadsFailed > 0;
	}

private:
	friend WorkloadBase;

	// Info about a running workload
	struct WorkloadInfo {
		// Reference to the workoad for ownership
		std::shared_ptr<IWorkload> ref;
		// Continuation to be executed after completing the workload
		TTaskFct cont;
	};

	// To be called by a workload to notify that it is done
	void workloadDone(IWorkload* workload, bool failed);

	// Transaction executor to be used by the workloads
	ITransactionExecutor* txExecutor;

	// A scheduler to be used by the workloads
	IScheduler* scheduler;

	// Mutex protects access to workloads & numWorkloadsFailed
	std::mutex mutex;

	// A map of currently running workloads
	std::unordered_map<IWorkload*, WorkloadInfo> workloads;

	// Number of workloads failed
	int numWorkloadsFailed;
};

// A workload factory
struct IWorkloadFactory {
	// create a workload by name
	static std::shared_ptr<IWorkload> create(std::string const& name, const WorkloadConfig& config);

	// a singleton registry of workload factories
	static std::unordered_map<std::string, IWorkloadFactory*>& factories();

	// Interface to be implemented by a workload factory
	virtual ~IWorkloadFactory() = default;
	virtual std::shared_ptr<IWorkload> create(const WorkloadConfig& config) = 0;
};

/**
 * A template for a workload factory for creating workloads of a certain type
 *
 * Declare a global instance of the factory for a workload type as follows:
 * WorkloadFactory<MyWorkload> MyWorkloadFactory("myWorkload");
 */
template <class WorkloadType>
struct WorkloadFactory : IWorkloadFactory {
	WorkloadFactory(const char* name) { factories()[name] = this; }
	std::shared_ptr<IWorkload> create(const WorkloadConfig& config) override {
		return std::make_shared<WorkloadType>(config);
	}
};

} // namespace FdbApiTester

#endif