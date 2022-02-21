/*
 * SysTestTransactionExecutor.h
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

#ifndef SYS_TEST_TRANSACTION_EXECUTOR_H
#define SYS_TEST_TRANSACTION_EXECUTOR_H

#include "SysTestOptions.h"
#include "SysTestApiWrapper.h"
#include "SysTestScheduler.h"
#include <string_view>

namespace FDBSystemTester {

class ITransactionContext {
public:
	virtual ~ITransactionContext() {}
	virtual Transaction* tx() = 0;
	virtual void continueAfter(Future& f, TTaskFct cont) = 0;
	virtual void commit() = 0;
	virtual void done() = 0;
	virtual std::string_view dbKey(std::string_view key) = 0;
};

class ITransactionActor {
public:
	virtual ~ITransactionActor() {}
	virtual void init(ITransactionContext* ctx) = 0;
	virtual void start() = 0;
	virtual void reset() = 0;
};

class TransactionActorBase : public ITransactionActor {
public:
	void init(ITransactionContext* ctx) override { context = ctx; }

protected:
	ITransactionContext* ctx() { return context; }
	Transaction* tx() { return ctx()->tx(); }
	std::string_view dbKey(std::string_view key) { return ctx()->dbKey(key); }
	void commit() { ctx()->commit(); }

private:
	ITransactionContext* context = nullptr;
};

struct TransactionExecutorOptions {
	std::string prefix = "";
	bool blockOnFutures = false;
};

class ITransactionExecutor {
public:
	virtual ~ITransactionExecutor() {}
	virtual void init(IScheduler* sched, const char* clusterFile, const TransactionExecutorOptions& options) = 0;
	virtual void execute(ITransactionActor* tx, TTaskFct cont) = 0;
	virtual void release() = 0;
};

ITransactionExecutor* createTransactionExecutor();

} // namespace FDBSystemTester

#endif