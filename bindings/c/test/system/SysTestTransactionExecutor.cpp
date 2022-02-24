/*
 * SysTestTransactionExecutor.cpp
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

#include "SysTestTransactionExecutor.h"
#include <iostream>
#include <cassert>
#include <memory>
#include <random>

namespace FDBSystemTester {

namespace {

void fdb_check(fdb_error_t e) {
	if (e) {
		std::cerr << fdb_get_error(e) << std::endl;
		std::abort();
	}
}

} // namespace

class TransactionContext : public ITransactionContext {
public:
	TransactionContext(FDBTransaction* tx,
	                   std::shared_ptr<ITransactionActor> txActor,
	                   TTaskFct cont,
	                   const TransactionExecutorOptions& options,
	                   IScheduler* scheduler)
	  : options(options), fdbTx(tx), txActor(txActor), contAfterDone(cont), scheduler(scheduler), finalError(0) {}

	Transaction* tx() override { return &fdbTx; }
	void continueAfter(Future f, TTaskFct cont) override { doContinueAfter(f, cont); }
	void commit() override {
		currFuture = fdbTx.commit();
		doContinueAfter(currFuture, [this]() { done(); });
	}
	void done() override {
		TTaskFct cont = contAfterDone;
		delete this;
		cont();
	}
	std::string_view dbKey(std::string_view key) override {
		std::string keyWithPrefix(options.prefix);
		keyWithPrefix.append(key);
		return key;
	}

private:
	void doContinueAfter(Future f, TTaskFct cont) {
		if (options.blockOnFutures) {
			blockingContinueAfter(f, cont);
		} else {
			asyncContinueAfter(f, cont);
		}
	}

	void blockingContinueAfter(Future f, TTaskFct cont) {
		scheduler->schedule([this, f, cont]() mutable {
			fdb_check(fdb_future_block_until_ready(f.fdbFuture()));
			fdb_error_t err = f.getError();
			if (err) {
				currFuture = fdbTx.onError(err);
				fdb_check(fdb_future_block_until_ready(currFuture.fdbFuture()));
				handleOnErrorResult();
			} else {
				cont();
			}
		});
	}

	void asyncContinueAfter(Future f, TTaskFct cont) {
		currCont = cont;
		currFuture = f;
		fdb_check(fdb_future_set_callback(f.fdbFuture(), futureReadyCallback, this));
	}

	static void futureReadyCallback(FDBFuture* f, void* param) {
		TransactionContext* txCtx = (TransactionContext*)param;
		txCtx->onFutureReady(f);
	}

	void onFutureReady(FDBFuture* f) {
		fdb_error_t err = fdb_future_get_error(f);
		if (err) {
			currFuture = tx()->onError(err);
			fdb_check(fdb_future_set_callback(currFuture.fdbFuture(), onErrorReadyCallback, this));
		} else {
			scheduler->schedule(currCont);
			currFuture.reset();
			currCont = TTaskFct();
		}
	}

	static void onErrorReadyCallback(FDBFuture* f, void* param) {
		TransactionContext* txCtx = (TransactionContext*)param;
		txCtx->onErrorReady(f);
	}

	void onErrorReady(FDBFuture* f) {
		scheduler->schedule([this]() { handleOnErrorResult(); });
	}

	void handleOnErrorResult() {
		fdb_error_t err = currFuture.getError();
		currFuture.reset();
		currCont = TTaskFct();
		if (err) {
			finalError = err;
			done();
		} else {
			txActor->reset();
			txActor->start();
		}
	}

	const TransactionExecutorOptions& options;
	Transaction fdbTx;
	std::shared_ptr<ITransactionActor> txActor;
	TTaskFct currCont;
	TTaskFct contAfterDone;
	IScheduler* scheduler;
	fdb_error_t finalError;
	Future currFuture;
};

class TransactionExecutor : public ITransactionExecutor {
public:
	TransactionExecutor() : scheduler(nullptr) {}

	~TransactionExecutor() { release(); }

	void init(IScheduler* scheduler, const char* clusterFile, const TransactionExecutorOptions& options) override {
		this->scheduler = scheduler;
		this->options = options;
		for (int i = 0; i < options.numDatabases; i++) {
			FDBDatabase* db;
			fdb_check(fdb_create_database(clusterFile, &db));
			databases.push_back(db);
		}
		std::random_device dev;
		random.seed(dev());
	}

	void execute(std::shared_ptr<ITransactionActor> txActor, TTaskFct cont) override {
		int idx = std::uniform_int_distribution<>(0, options.numDatabases - 1)(random);
		FDBTransaction* tx;
		fdb_check(fdb_database_create_transaction(databases[idx], &tx));
		TransactionContext* ctx = new TransactionContext(tx, txActor, cont, options, scheduler);
		txActor->init(ctx);
		txActor->start();
	}

	void release() override {
		for (FDBDatabase* db : databases) {
			fdb_database_destroy(db);
		}
	}

private:
	std::vector<FDBDatabase*> databases;
	TransactionExecutorOptions options;
	IScheduler* scheduler;
	std::mt19937 random;
};

std::unique_ptr<ITransactionExecutor> createTransactionExecutor() {
	return std::make_unique<TransactionExecutor>();
}

} // namespace FDBSystemTester