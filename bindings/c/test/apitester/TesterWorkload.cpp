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
#include <memory>

namespace FdbApiTester {

void WorkloadBase::init(WorkloadManager* manager) {
	this->manager = manager;
}

void WorkloadBase::schedule(TTaskFct task) {
	tasksScheduled++;
	manager->scheduler->schedule([this, task]() {
		task();
		tasksScheduled--;
		checkIfDone();
	});
}

void WorkloadBase::execTransaction(std::shared_ptr<ITransactionActor> tx, TTaskFct cont) {
	tasksScheduled++;
	manager->txExecutor->execute(tx, [this, cont]() {
		cont();
		tasksScheduled--;
		checkIfDone();
	});
}

void WorkloadBase::checkIfDone() {
	if (tasksScheduled == 0) {
		manager->workloadDone(this);
	}
}

void WorkloadManager::add(std::shared_ptr<IWorkload> workload, TTaskFct cont) {
	std::unique_lock<std::mutex> lock(mutex);
	workloads[workload.get()] = WorkloadInfo{ workload, cont };
}

void WorkloadManager::run() {
	for (auto iter : workloads) {
		iter.first->init(this);
	}
	for (auto iter : workloads) {
		iter.first->start();
	}
	scheduler->join();
}

void WorkloadManager::workloadDone(IWorkload* workload) {
	std::unique_lock<std::mutex> lock(mutex);
	auto iter = workloads.find(workload);
	ASSERT(iter != workloads.end());
	lock.unlock();
	iter->second.cont();
	lock.lock();
	workloads.erase(iter);
	bool done = workloads.empty();
	lock.unlock();
	if (done) {
		scheduler->stop();
	}
}

} // namespace FdbApiTester