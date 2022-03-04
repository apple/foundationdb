/*
 * TesterTransactionExecutor.cpp
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

#include "TesterTransactionExecutor.h"
#include "TesterUtil.h"
#include "test/apitester/TesterScheduler.h"
#include <memory>
#include <unordered_map>
#include <mutex>
#include <atomic>

namespace FdbApiTester {

void ITransactionContext::continueAfterAll(std::shared_ptr<std::vector<Future>> futures, TTaskFct cont) {
	auto counter = std::make_shared<std::atomic<int>>(futures->size());
	for (auto& f : *futures) {
		continueAfter(f, [counter, cont]() {
			if (--(*counter) == 0) {
				cont();
			}
		});
	}
}

class TransactionContext : public ITransactionContext {
public:
	TransactionContext(FDBTransaction* tx,
	                   std::shared_ptr<ITransactionActor> txActor,
	                   TTaskFct cont,
	                   const TransactionExecutorOptions& options,
	                   IScheduler* scheduler)
	  : options(options), fdbTx(tx), txActor(txActor), contAfterDone(cont), scheduler(scheduler) {}

	Transaction* tx() override { return &fdbTx; }
	void continueAfter(Future f, TTaskFct cont) override { doContinueAfter(f, cont); }
	void commit() override {
		Future f = fdbTx.commit();
		doContinueAfter(f, [this]() { done(); });
	}
	void done() override {
		TTaskFct cont = contAfterDone;
		ASSERT(!onErrorFuture);
		ASSERT(waitMap.empty());
		delete this;
		cont();
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
			std::unique_lock<std::mutex> lock(mutex);
			if (!onErrorFuture) {
				fdb_error_t err = fdb_future_block_until_ready(f.fdbFuture());
				if (err) {
					lock.unlock();
					transactionFailed(err);
					return;
				}
				err = f.getError();
				if (err) {
					if (err != error_code_transaction_cancelled) {
						onErrorFuture = fdbTx.onError(err);
						fdb_error_t err2 = fdb_future_block_until_ready(onErrorFuture.fdbFuture());
						if (err2) {
							lock.unlock();
							transactionFailed(err2);
							return;
						}
						scheduler->schedule([this]() { handleOnErrorResult(); });
					}
				} else {
					scheduler->schedule([cont]() { cont(); });
				}
			}
		});
	}

	void asyncContinueAfter(Future f, TTaskFct cont) {
		std::unique_lock<std::mutex> lock(mutex);
		if (!onErrorFuture) {
			waitMap[f.fdbFuture()] = WaitInfo{ f, cont };
			lock.unlock();
			fdb_error_t err = fdb_future_set_callback(f.fdbFuture(), futureReadyCallback, this);
			if (err) {
				transactionFailed(err);
			}
		}
	}

	static void futureReadyCallback(FDBFuture* f, void* param) {
		TransactionContext* txCtx = (TransactionContext*)param;
		txCtx->onFutureReady(f);
	}

	void onFutureReady(FDBFuture* f) {
		std::unique_lock<std::mutex> lock(mutex);
		auto iter = waitMap.find(f);
		if (iter == waitMap.end()) {
			return;
		}
		fdb_error_t err = fdb_future_get_error(f);
		TTaskFct cont = iter->second.cont;
		waitMap.erase(iter);
		if (err) {
			if (err != error_code_transaction_cancelled) {
				waitMap.clear();
				onErrorFuture = tx()->onError(err);
				lock.unlock();
				fdb_error_t err = fdb_future_set_callback(onErrorFuture.fdbFuture(), onErrorReadyCallback, this);
				if (err) {
					transactionFailed(err);
				}
			}
		} else {
			scheduler->schedule(cont);
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
		std::unique_lock<std::mutex> lock(mutex);
		fdb_error_t err = onErrorFuture.getError();
		onErrorFuture.reset();
		if (err) {
			transactionFailed(err);
		} else {
			lock.unlock();
			txActor->reset();
			txActor->start();
		}
	}

	struct WaitInfo {
		Future future;
		TTaskFct cont;
	};

	void transactionFailed(fdb_error_t err) {
		std::unique_lock<std::mutex> lock(mutex);
		onErrorFuture.reset();
		waitMap.clear();
		lock.unlock();
		txActor->setError(err);
		done();
	}

	const TransactionExecutorOptions& options;
	Transaction fdbTx;
	std::shared_ptr<ITransactionActor> txActor;
	std::mutex mutex;
	std::unordered_map<FDBFuture*, WaitInfo> waitMap;
	Future onErrorFuture;
	TTaskFct contAfterDone;
	IScheduler* scheduler;
};

class TransactionExecutorBase : public ITransactionExecutor {
public:
	TransactionExecutorBase(const TransactionExecutorOptions& options) : options(options), scheduler(nullptr) {}

	void init(IScheduler* scheduler, const char* clusterFile) override {
		this->scheduler = scheduler;
		this->clusterFile = clusterFile;
	}

protected:
	void executeWithDatabase(FDBDatabase* db, std::shared_ptr<ITransactionActor> txActor, TTaskFct cont) {
		FDBTransaction* tx;
		fdb_error_t err = fdb_database_create_transaction(db, &tx);
		if (err != error_code_success) {
			txActor->setError(err);
			cont();
		} else {
			TransactionContext* ctx = new TransactionContext(tx, txActor, cont, options, scheduler);
			txActor->init(ctx);
			txActor->start();
		}
	}

protected:
	TransactionExecutorOptions options;
	std::string clusterFile;
	IScheduler* scheduler;
};

class DBPoolTransactionExecutor : public TransactionExecutorBase {
public:
	DBPoolTransactionExecutor(const TransactionExecutorOptions& options) : TransactionExecutorBase(options) {}

	~DBPoolTransactionExecutor() override { release(); }

	void init(IScheduler* scheduler, const char* clusterFile) override {
		TransactionExecutorBase::init(scheduler, clusterFile);
		for (int i = 0; i < options.numDatabases; i++) {
			FDBDatabase* db;
			fdb_error_t err = fdb_create_database(clusterFile, &db);
			if (err != error_code_success) {
				throw TesterError(fmt::format("Failed create database with the culster file '{}'. Error: {}({})",
				                              clusterFile,
				                              err,
				                              fdb_get_error(err)));
			}
			databases.push_back(db);
		}
	}

	void execute(std::shared_ptr<ITransactionActor> txActor, TTaskFct cont) override {
		int idx = random.randomInt(0, options.numDatabases - 1);
		executeWithDatabase(databases[idx], txActor, cont);
	}

	void release() {
		for (FDBDatabase* db : databases) {
			fdb_database_destroy(db);
		}
	}

private:
	std::vector<FDBDatabase*> databases;
	Random random;
};

class DBPerTransactionExecutor : public TransactionExecutorBase {
public:
	DBPerTransactionExecutor(const TransactionExecutorOptions& options) : TransactionExecutorBase(options) {}

	void execute(std::shared_ptr<ITransactionActor> txActor, TTaskFct cont) override {
		FDBDatabase* db = nullptr;
		fdb_error_t err = fdb_create_database(clusterFile.c_str(), &db);
		if (err != error_code_success) {
			txActor->setError(err);
			cont();
		}
		executeWithDatabase(db, txActor, [cont, db]() {
			fdb_database_destroy(db);
			cont();
		});
	}
};

std::unique_ptr<ITransactionExecutor> createTransactionExecutor(const TransactionExecutorOptions& options) {
	if (options.databasePerTransaction) {
		return std::make_unique<DBPerTransactionExecutor>(options);
	} else {
		return std::make_unique<DBPoolTransactionExecutor>(options);
	}
}

} // namespace FdbApiTester