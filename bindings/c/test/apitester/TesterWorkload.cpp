/*
 * TesterWorkload.cpp
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

#include "TesterWorkload.h"
#include "TesterUtil.h"
#include "fdb_c_options.g.h"
#include "fmt/core.h"
#include "test/apitester/TesterScheduler.h"
#include <cstdlib>
#include <memory>
#include <fmt/format.h>
#include <vector>
#include <iostream>
#include <cstdio>

namespace FdbApiTester {

int WorkloadConfig::getIntOption(const std::string& name, int defaultVal) const {
	auto iter = options.find(name);
	if (iter == options.end()) {
		return defaultVal;
	} else {
		char* endptr;
		int intVal = strtol(iter->second.c_str(), &endptr, 10);
		if (*endptr != '\0') {
			throw TesterError(
			    fmt::format("Invalid workload configuration. Invalid value {} for {}", iter->second, name));
		}
		return intVal;
	}
}

double WorkloadConfig::getFloatOption(const std::string& name, double defaultVal) const {
	auto iter = options.find(name);
	if (iter == options.end()) {
		return defaultVal;
	} else {
		char* endptr;
		double floatVal = strtod(iter->second.c_str(), &endptr);
		if (*endptr != '\0') {
			throw TesterError(
			    fmt::format("Invalid workload configuration. Invalid value {} for {}", iter->second, name));
		}
		return floatVal;
	}
}

bool WorkloadConfig::getBoolOption(const std::string& name, bool defaultVal) const {
	auto iter = options.find(name);
	if (iter == options.end()) {
		return defaultVal;
	} else {
		std::string val(fdb::toCharsRef(lowerCase(fdb::toBytesRef(iter->second))));
		if (val == "true") {
			return true;
		} else if (val == "false") {
			return false;
		} else {
			throw TesterError(
			    fmt::format("Invalid workload configuration. Invalid value {} for {}", iter->second, name));
		}
	}
}

WorkloadBase::WorkloadBase(const WorkloadConfig& config)
  : manager(nullptr), tasksScheduled(0), numErrors(0), clientId(config.clientId), numClients(config.numClients),
    failed(false), numTxCompleted(0), numTxStarted(0), inProgress(false) {
	maxErrors = config.getIntOption("maxErrors", 10);
	minTxTimeoutMs = config.getIntOption("minTxTimeoutMs", 0);
	maxTxTimeoutMs = config.getIntOption("maxTxTimeoutMs", 0);
	workloadId = fmt::format("{}{}", config.name, clientId);
}

void WorkloadBase::init(WorkloadManager* manager) {
	this->manager = manager;
	inProgress = true;
}

void WorkloadBase::printStats() {
	info(fmt::format("{} transactions completed", numTxCompleted.load()));
}

void WorkloadBase::schedule(TTaskFct task) {
	ASSERT(inProgress);
	if (failed) {
		return;
	}
	tasksScheduled++;
	manager->scheduler->schedule([this, task]() {
		task();
		scheduledTaskDone();
	});
}

void WorkloadBase::execTransaction(TOpStartFct startFct,
                                   TTaskFct cont,
                                   std::optional<fdb::BytesRef> tenant,
                                   bool failOnError) {
	doExecute(startFct, cont, tenant, failOnError, true);
}

// Execute a non-transactional database operation within the workload
void WorkloadBase::execOperation(TOpStartFct startFct,
                                 TTaskFct cont,
                                 std::optional<fdb::BytesRef> tenant,
                                 bool failOnError) {
	doExecute(startFct, cont, tenant, failOnError, false);
}

void WorkloadBase::doExecute(TOpStartFct startFct,
                             TTaskFct cont,
                             std::optional<fdb::BytesRef> tenant,
                             bool failOnError,
                             bool transactional) {
	ASSERT(inProgress);
	if (failed) {
		return;
	}
	tasksScheduled++;
	numTxStarted++;
	manager->txExecutor->execute( //
	    [this, transactional, cont, startFct](auto ctx) {
		    if (transactional && maxTxTimeoutMs > 0) {
			    int timeoutMs = Random::get().randomInt(minTxTimeoutMs, maxTxTimeoutMs);
			    ctx->tx().setOption(FDB_TR_OPTION_TIMEOUT, timeoutMs);
		    }
		    startFct(ctx);
	    },
	    [this, cont, failOnError](fdb::Error err) {
		    numTxCompleted++;
		    if (err.code() == error_code_success) {
			    cont();
		    } else {
			    std::string msg = fmt::format("Transaction failed with error: {} ({})", err.code(), err.what());
			    if (failOnError) {
				    error(msg);
				    failed = true;
			    } else {
				    info(msg);
				    cont();
			    }
		    }
		    scheduledTaskDone();
	    },
	    tenant,
	    transactional,
	    maxTxTimeoutMs > 0);
}

void WorkloadBase::info(const std::string& msg) {
	fmt::print(stderr, "[{}] {}\n", workloadId, msg);
}

void WorkloadBase::error(const std::string& msg) {
	fmt::print(stderr, "[{}] ERROR: {}\n", workloadId, msg);
	numErrors++;
	if (numErrors > maxErrors && !failed) {
		fmt::print(stderr, "[{}] ERROR: Stopping workload after {} errors\n", workloadId, numErrors.load());
		failed = true;
	}
}

void WorkloadBase::scheduledTaskDone() {
	if (--tasksScheduled == 0) {
		inProgress = false;
		if (numErrors > 0) {
			error(fmt::format("Workload failed with {} errors", numErrors.load()));
		} else {
			info("Workload successfully completed");
		}
		ASSERT(numTxStarted == numTxCompleted);
		manager->workloadDone(this, numErrors > 0);
	}
}

void WorkloadBase::confirmProgress() {
	info("Progress confirmed");
	manager->confirmProgress(this);
}

void WorkloadManager::add(std::shared_ptr<IWorkload> workload, TTaskFct cont) {
	std::unique_lock<std::mutex> lock(mutex);
	workloads[workload.get()] = WorkloadInfo{ workload, cont, workload->getControlIfc(), false };
	fmt::print(stderr, "Workload {} added\n", workload->getWorkloadId());
}

void WorkloadManager::run() {
	std::vector<std::shared_ptr<IWorkload>> initialWorkloads;
	{
		std::unique_lock<std::mutex> lock(mutex);
		for (auto iter : workloads) {
			initialWorkloads.push_back(iter.second.ref);
		}
	}
	for (auto iter : initialWorkloads) {
		iter->init(this);
	}
	for (auto iter : initialWorkloads) {
		iter->start();
	}
	scheduler->join();
	if (ctrlInputThread.joinable()) {
		ctrlInputThread.join();
	}
	if (outputPipe.is_open()) {
		outputPipe << "DONE" << std::endl;
		outputPipe.close();
	}
	if (failed()) {
		fmt::print(stderr, "{} workloads failed\n", numWorkloadsFailed);
	} else {
		fprintf(stderr, "All workloads successfully completed\n");
	}
}

void WorkloadManager::workloadDone(IWorkload* workload, bool failed) {
	std::unique_lock<std::mutex> lock(mutex);
	auto iter = workloads.find(workload);
	ASSERT(iter != workloads.end());
	lock.unlock();
	iter->second.cont();
	lock.lock();
	workloads.erase(iter);
	if (failed) {
		numWorkloadsFailed++;
	}
	bool done = workloads.empty();
	lock.unlock();
	if (done) {
		if (statsTimer) {
			statsTimer->cancel();
		}
		scheduler->stop();
	}
}

void WorkloadManager::openControlPipes(const std::string& inputPipeName, const std::string& outputPipeName) {
	if (!inputPipeName.empty()) {
		ctrlInputThread = std::thread(&WorkloadManager::readControlInput, this, inputPipeName);
	}
	if (!outputPipeName.empty()) {
		fmt::print(stderr, "Opening pipe {} for writing\n", outputPipeName);
		outputPipe.open(outputPipeName, std::ofstream::out);
	}
}

void WorkloadManager::readControlInput(std::string pipeName) {
	fmt::print(stderr, "Opening pipe {} for reading\n", pipeName);
	// Open in binary mode and read char-by-char to avoid
	// any kind of buffering
	FILE* f = fopen(pipeName.c_str(), "rb");
	setbuf(f, NULL);
	std::string line;
	while (true) {
		int ch = fgetc(f);
		if (ch == EOF) {
			return;
		}
		if (ch != '\n') {
			line += ch;
			continue;
		}
		if (line.empty()) {
			continue;
		}
		fmt::print(stderr, "Received {} command\n", line);
		if (line == "STOP") {
			handleStopCommand();
		} else if (line == "CHECK") {
			handleCheckCommand();
		}
		line.clear();
	}
}

void WorkloadManager::schedulePrintStatistics(int timeIntervalMs) {
	statsTimer = scheduler->scheduleWithDelay(timeIntervalMs, [this, timeIntervalMs]() {
		for (auto workload : getActiveWorkloads()) {
			workload->printStats();
		}
		this->schedulePrintStatistics(timeIntervalMs);
	});
}

std::vector<std::shared_ptr<IWorkload>> WorkloadManager::getActiveWorkloads() {
	std::unique_lock<std::mutex> lock(mutex);
	std::vector<std::shared_ptr<IWorkload>> res;
	for (auto iter : workloads) {
		res.push_back(iter.second.ref);
	}
	return res;
}

void WorkloadManager::handleStopCommand() {
	std::unique_lock<std::mutex> lock(mutex);
	for (auto& iter : workloads) {
		IWorkloadControlIfc* controlIfc = iter.second.controlIfc;
		if (controlIfc) {
			controlIfc->stop();
		}
	}
}

void WorkloadManager::handleCheckCommand() {
	std::unique_lock<std::mutex> lock(mutex);
	// Request to confirm progress from all workloads
	// providing the control interface
	for (auto& iter : workloads) {
		IWorkloadControlIfc* controlIfc = iter.second.controlIfc;
		if (controlIfc) {
			iter.second.progressConfirmed = false;
			controlIfc->checkProgress();
		}
	}
}

void WorkloadManager::confirmProgress(IWorkload* workload) {
	std::unique_lock<std::mutex> lock(mutex);
	// Save the progress confirmation of the workload
	auto iter = workloads.find(workload);
	ASSERT(iter != workloads.end());
	iter->second.progressConfirmed = true;
	// Check if all workloads have confirmed progress
	bool allConfirmed = true;
	for (auto& iter : workloads) {
		if (iter.second.controlIfc && !iter.second.progressConfirmed) {
			allConfirmed = false;
			break;
		}
	}
	lock.unlock();
	if (allConfirmed) {
		// Notify the test controller about the successful progress check
		ASSERT(outputPipe.is_open());
		outputPipe << "CHECK_OK" << std::endl;
	}
}

std::shared_ptr<IWorkload> IWorkloadFactory::create(std::string const& name, const WorkloadConfig& config) {
	auto it = factories().find(name);
	if (it == factories().end())
		return {}; // or throw?
	return it->second->create(config);
}

std::unordered_map<std::string, IWorkloadFactory*>& IWorkloadFactory::factories() {
	static std::unordered_map<std::string, IWorkloadFactory*> theFactories;
	return theFactories;
}

} // namespace FdbApiTester
