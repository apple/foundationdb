/*
 * SysTestWorkload.h
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

#ifndef SYS_TEST_WORKLOAD_H
#define SYS_TEST_WORKLOAD_H

#include "SysTestTransactionExecutor.h"

namespace FDBSystemTester {

class IWorkload {
public:
	virtual ~IWorkload() {}
	virtual void init(ITransactionExecutor* txExecutor, IScheduler* sched, TTaskFct cont) = 0;
	virtual void start() = 0;
};

class WorkloadBase : public IWorkload {
public:
	WorkloadBase() : txExecutor(nullptr), scheduler(nullptr), tasksScheduled(0), txRunning(0) {}
	void init(ITransactionExecutor* txExecutor, IScheduler* sched, TTaskFct cont) override;

protected:
	void schedule(TTaskFct task);
	void execTransaction(ITransactionActor* tx, TTaskFct cont);
	void contIfDone();

private:
	ITransactionExecutor* txExecutor;
	IScheduler* scheduler;
	TTaskFct doneCont;
	std::atomic<int> tasksScheduled;
	std::atomic<int> txRunning;
};

} // namespace FDBSystemTester

#endif