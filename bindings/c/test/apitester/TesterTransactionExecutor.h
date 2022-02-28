/*
 * TesterTransactionExecutor.h
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

#ifndef APITESTER_TRANSACTION_EXECUTOR_H
#define APITESTER_TRANSACTION_EXECUTOR_H

#include "TesterOptions.h"
#include "TesterApiWrapper.h"
#include "TesterScheduler.h"
#include <string_view>
#include <memory>

namespace FdbApiTester {

class ITransactionContext {
public:
	virtual ~ITransactionContext() {}
	virtual Transaction* tx() = 0;
	virtual void continueAfter(Future f, TTaskFct cont) = 0;
	virtual void commit() = 0;
	virtual void done() = 0;
	virtual void continueAfterAll(std::shared_ptr<std::vector<Future>> futures, TTaskFct cont);
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
	void commit() { ctx()->commit(); }
	void reset() override {}

private:
	ITransactionContext* context = nullptr;
};

using TTxStartFct = std::function<void(ITransactionContext*)>;

class TransactionFct : public TransactionActorBase {
public:
	TransactionFct(TTxStartFct startFct) : startFct(startFct) {}
	void start() override { startFct(this->ctx()); }

private:
	TTxStartFct startFct;
};

struct TransactionExecutorOptions {
	std::string prefix = "";
	bool blockOnFutures = false;
	int numDatabases = 1;
};

class ITransactionExecutor {
public:
	virtual ~ITransactionExecutor() {}
	virtual void init(IScheduler* sched, const char* clusterFile, const TransactionExecutorOptions& options) = 0;
	virtual void execute(std::shared_ptr<ITransactionActor> tx, TTaskFct cont) = 0;
	virtual void release() = 0;
};

std::unique_ptr<ITransactionExecutor> createTransactionExecutor();

} // namespace FdbApiTester

#endif