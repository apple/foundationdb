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
#include "foundationdb/fdb_c_types.h"
#include "test/apitester/TesterScheduler.h"
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <chrono>
#include <thread>
#include <fmt/format.h>

namespace FdbApiTester {

constexpr int LONG_WAIT_TIME_US = 1000000;
constexpr int LARGE_NUMBER_OF_RETRIES = 5;

void TransactionActorBase::complete(fdb_error_t err) {
	error = err;
	context = {};
}

void ITransactionContext::continueAfterAll(std::vector<Future> futures, TTaskFct cont) {
	auto counter = std::make_shared<std::atomic<int>>(futures.size());
	auto errorCode = std::make_shared<std::atomic<fdb_error_t>>(error_code_success);
	auto thisPtr = shared_from_this();
	for (auto& f : futures) {
		continueAfter(
		    f,
		    [thisPtr, f, counter, errorCode, cont]() {
			    if (f.getError() != error_code_success) {
				    (*errorCode) = f.getError();
			    }
			    if (--(*counter) == 0) {
				    if (*errorCode == error_code_success) {
					    // all futures successful -> continue
					    cont();
				    } else {
					    // at least one future failed -> retry the transaction
					    thisPtr->onError(*errorCode);
				    }
			    }
		    },
		    false);
	}
}

/**
 * Transaction context base class, containing reusable functionality
 */
class TransactionContextBase : public ITransactionContext {
public:
	TransactionContextBase(FDBTransaction* tx,
	                       std::shared_ptr<ITransactionActor> txActor,
	                       TTaskFct cont,
	                       IScheduler* scheduler,
	                       int retryLimit,
	                       std::string bgBasePath)
	  : fdbTx(tx), txActor(txActor), contAfterDone(cont), scheduler(scheduler), retryLimit(retryLimit),
	    txState(TxState::IN_PROGRESS), commitCalled(false), bgBasePath(bgBasePath) {}

	// A state machine:
	// IN_PROGRESS -> (ON_ERROR -> IN_PROGRESS)* [-> ON_ERROR] -> DONE
	enum class TxState { IN_PROGRESS, ON_ERROR, DONE };

	Transaction* tx() override { return &fdbTx; }

	// Set a continuation to be executed when a future gets ready
	void continueAfter(Future f, TTaskFct cont, bool retryOnError) override { doContinueAfter(f, cont, retryOnError); }

	// Complete the transaction with a commit
	void commit() override {
		std::unique_lock<std::mutex> lock(mutex);
		if (txState != TxState::IN_PROGRESS) {
			return;
		}
		commitCalled = true;
		lock.unlock();
		Future f = fdbTx.commit();
		auto thisRef = shared_from_this();
		doContinueAfter(
		    f, [thisRef]() { thisRef->done(); }, true);
	}

	// Complete the transaction without a commit (for read transactions)
	void done() override {
		std::unique_lock<std::mutex> lock(mutex);
		if (txState != TxState::IN_PROGRESS) {
			return;
		}
		txState = TxState::DONE;
		lock.unlock();
		if (retriedErrors.size() >= LARGE_NUMBER_OF_RETRIES) {
			fmt::print("Transaction succeeded after {} retries on errors: {}\n",
			           retriedErrors.size(),
			           fmt::join(retriedErrors, ", "));
		}
		// cancel transaction so that any pending operations on it
		// fail gracefully
		fdbTx.cancel();
		txActor->complete(error_code_success);
		cleanUp();
		contAfterDone();
	}

	std::string getBGBasePath() override { return bgBasePath; }

protected:
	virtual void doContinueAfter(Future f, TTaskFct cont, bool retryOnError) = 0;

	// Clean up transaction state after completing the transaction
	// Note that the object may live longer, because it is referenced
	// by not yet triggered callbacks
	virtual void cleanUp() {
		ASSERT(txState == TxState::DONE);
		ASSERT(!onErrorFuture);
		txActor = {};
	}

	// Complete the transaction with an (unretriable) error
	void transactionFailed(fdb_error_t err) {
		ASSERT(err != error_code_success);
		std::unique_lock<std::mutex> lock(mutex);
		if (txState == TxState::DONE) {
			return;
		}
		txState = TxState::DONE;
		lock.unlock();
		txActor->complete(err);
		cleanUp();
		contAfterDone();
	}

	// Handle result of an a transaction onError call
	void handleOnErrorResult() {
		ASSERT(txState == TxState::ON_ERROR);
		fdb_error_t err = onErrorFuture.getError();
		onErrorFuture = {};
		if (err) {
			transactionFailed(err);
		} else {
			std::unique_lock<std::mutex> lock(mutex);
			txState = TxState::IN_PROGRESS;
			commitCalled = false;
			lock.unlock();
			txActor->start();
		}
	}

	// Checks if a transaction can be retried. Fails the transaction if the check fails
	bool canRetry(fdb_error_t lastErr) {
		ASSERT(txState == TxState::ON_ERROR);
		retriedErrors.push_back(lastErr);
		if (retryLimit == 0 || retriedErrors.size() <= retryLimit) {
			if (retriedErrors.size() == LARGE_NUMBER_OF_RETRIES) {
				fmt::print("Transaction already retried {} times, on errors: {}\n",
				           retriedErrors.size(),
				           fmt::join(retriedErrors, ", "));
			}
			return true;
		}
		fmt::print("Transaction retry limit reached. Retried on errors: {}\n", fmt::join(retriedErrors, ", "));
		transactionFailed(lastErr);
		return false;
	}

	// FDB transaction
	Transaction fdbTx;

	// Actor implementing the transaction worklflow
	std::shared_ptr<ITransactionActor> txActor;

	// Mutex protecting access to shared mutable state
	std::mutex mutex;

	// Continuation to be called after completion of the transaction
	TTaskFct contAfterDone;

	// Reference to the scheduler
	IScheduler* scheduler;

	// Retry limit
	int retryLimit;

	// Transaction execution state
	TxState txState;

	// onError future used in ON_ERROR state
	Future onErrorFuture;

	// The error code on which onError was called
	fdb_error_t onErrorArg;

	// The time point of calling onError
	TimePoint onErrorCallTimePoint;

	// Transaction is committed or being committed
	bool commitCalled;

	// A history of errors on which the transaction was retried
	std::vector<fdb_error_t> retriedErrors;

	// blob granule base path
	std::string bgBasePath;
};

/**
 *  Transaction context using blocking waits to implement continuations on futures
 */
class BlockingTransactionContext : public TransactionContextBase {
public:
	BlockingTransactionContext(FDBTransaction* tx,
	                           std::shared_ptr<ITransactionActor> txActor,
	                           TTaskFct cont,
	                           IScheduler* scheduler,
	                           int retryLimit,
	                           std::string bgBasePath)
	  : TransactionContextBase(tx, txActor, cont, scheduler, retryLimit, bgBasePath) {}

protected:
	void doContinueAfter(Future f, TTaskFct cont, bool retryOnError) override {
		auto thisRef = std::static_pointer_cast<BlockingTransactionContext>(shared_from_this());
		scheduler->schedule(
		    [thisRef, f, cont, retryOnError]() mutable { thisRef->blockingContinueAfter(f, cont, retryOnError); });
	}

	void blockingContinueAfter(Future f, TTaskFct cont, bool retryOnError) {
		std::unique_lock<std::mutex> lock(mutex);
		if (txState != TxState::IN_PROGRESS) {
			return;
		}
		lock.unlock();
		auto start = timeNow();
		fdb_error_t err = fdb_future_block_until_ready(f.fdbFuture());
		if (err) {
			transactionFailed(err);
			return;
		}
		err = f.getError();
		auto waitTimeUs = timeElapsedInUs(start);
		if (waitTimeUs > LONG_WAIT_TIME_US) {
			fmt::print("Long waiting time on a future: {:.3f}s, return code {} ({}), commit called: {}\n",
			           microsecToSec(waitTimeUs),
			           err,
			           fdb_get_error(err),
			           commitCalled);
		}
		if (err == error_code_transaction_cancelled) {
			return;
		}
		if (err == error_code_success || !retryOnError) {
			scheduler->schedule([cont]() { cont(); });
			return;
		}

		onError(err);
	}

	virtual void onError(fdb_error_t err) override {
		std::unique_lock<std::mutex> lock(mutex);
		if (txState != TxState::IN_PROGRESS) {
			// Ignore further errors, if the transaction is in the error handing mode or completed
			return;
		}
		txState = TxState::ON_ERROR;
		lock.unlock();

		if (!canRetry(err)) {
			return;
		}

		ASSERT(!onErrorFuture);
		onErrorFuture = fdbTx.onError(err);
		onErrorArg = err;

		auto start = timeNow();
		fdb_error_t err2 = fdb_future_block_until_ready(onErrorFuture.fdbFuture());
		if (err2) {
			transactionFailed(err2);
			return;
		}
		auto waitTimeUs = timeElapsedInUs(start);
		if (waitTimeUs > LONG_WAIT_TIME_US) {
			fdb_error_t err3 = onErrorFuture.getError();
			fmt::print("Long waiting time on onError({}) future: {:.3f}s, return code {} ({})\n",
			           onErrorArg,
			           microsecToSec(waitTimeUs),
			           err3,
			           fdb_get_error(err3));
		}
		auto thisRef = std::static_pointer_cast<BlockingTransactionContext>(shared_from_this());
		scheduler->schedule([thisRef]() { thisRef->handleOnErrorResult(); });
	}
};

/**
 *  Transaction context using callbacks to implement continuations on futures
 */
class AsyncTransactionContext : public TransactionContextBase {
public:
	AsyncTransactionContext(FDBTransaction* tx,
	                        std::shared_ptr<ITransactionActor> txActor,
	                        TTaskFct cont,
	                        IScheduler* scheduler,
	                        int retryLimit,
	                        std::string bgBasePath)
	  : TransactionContextBase(tx, txActor, cont, scheduler, retryLimit, bgBasePath) {}

protected:
	void doContinueAfter(Future f, TTaskFct cont, bool retryOnError) override {
		std::unique_lock<std::mutex> lock(mutex);
		if (txState != TxState::IN_PROGRESS) {
			return;
		}
		callbackMap[f.fdbFuture()] = CallbackInfo{ f, cont, shared_from_this(), retryOnError, timeNow() };
		lock.unlock();
		fdb_error_t err = fdb_future_set_callback(f.fdbFuture(), futureReadyCallback, this);
		if (err) {
			lock.lock();
			callbackMap.erase(f.fdbFuture());
			lock.unlock();
			transactionFailed(err);
		}
	}

	static void futureReadyCallback(FDBFuture* f, void* param) {
		try {
			AsyncTransactionContext* txCtx = (AsyncTransactionContext*)param;
			txCtx->onFutureReady(f);
		} catch (std::runtime_error& err) {
			fmt::print("Unexpected exception in callback {}\n", err.what());
			abort();
		} catch (...) {
			fmt::print("Unknown error in callback\n");
			abort();
		}
	}

	void onFutureReady(FDBFuture* f) {
		auto endTime = timeNow();
		injectRandomSleep();
		// Hold a reference to this to avoid it to be
		// destroyed before releasing the mutex
		auto thisRef = shared_from_this();
		std::unique_lock<std::mutex> lock(mutex);
		auto iter = callbackMap.find(f);
		ASSERT(iter != callbackMap.end());
		CallbackInfo cbInfo = iter->second;
		callbackMap.erase(iter);
		if (txState != TxState::IN_PROGRESS) {
			return;
		}
		lock.unlock();
		fdb_error_t err = fdb_future_get_error(f);
		auto waitTimeUs = timeElapsedInUs(cbInfo.startTime, endTime);
		if (waitTimeUs > LONG_WAIT_TIME_US) {
			fmt::print("Long waiting time on a future: {:.3f}s, return code {} ({})\n",
			           microsecToSec(waitTimeUs),
			           err,
			           fdb_get_error(err));
		}
		if (err == error_code_transaction_cancelled) {
			return;
		}
		if (err == error_code_success || !cbInfo.retryOnError) {
			scheduler->schedule(cbInfo.cont);
			return;
		}
		onError(err);
	}

	virtual void onError(fdb_error_t err) override {
		std::unique_lock<std::mutex> lock(mutex);
		if (txState != TxState::IN_PROGRESS) {
			// Ignore further errors, if the transaction is in the error handing mode or completed
			return;
		}
		txState = TxState::ON_ERROR;
		lock.unlock();

		if (!canRetry(err)) {
			return;
		}

		ASSERT(!onErrorFuture);
		onErrorArg = err;
		onErrorFuture = tx()->onError(err);
		onErrorCallTimePoint = timeNow();
		onErrorThisRef = std::static_pointer_cast<AsyncTransactionContext>(shared_from_this());
		fdb_error_t err2 = fdb_future_set_callback(onErrorFuture.fdbFuture(), onErrorReadyCallback, this);
		if (err2) {
			onErrorFuture = {};
			transactionFailed(err2);
		}
	}

	static void onErrorReadyCallback(FDBFuture* f, void* param) {
		try {
			AsyncTransactionContext* txCtx = (AsyncTransactionContext*)param;
			txCtx->onErrorReady(f);
		} catch (std::runtime_error& err) {
			fmt::print("Unexpected exception in callback {}\n", err.what());
			abort();
		} catch (...) {
			fmt::print("Unknown error in callback\n");
			abort();
		}
	}

	void onErrorReady(FDBFuture* f) {
		auto waitTimeUs = timeElapsedInUs(onErrorCallTimePoint);
		if (waitTimeUs > LONG_WAIT_TIME_US) {
			fdb_error_t err = onErrorFuture.getError();
			fmt::print("Long waiting time on onError({}): {:.3f}s, return code {} ({})\n",
			           onErrorArg,
			           microsecToSec(waitTimeUs),
			           err,
			           fdb_get_error(err));
		}
		injectRandomSleep();
		auto thisRef = onErrorThisRef;
		onErrorThisRef = {};
		scheduler->schedule([thisRef]() { thisRef->handleOnErrorResult(); });
	}

	void cleanUp() override {
		TransactionContextBase::cleanUp();

		// Cancel all pending operations
		// Note that the callbacks of the cancelled futures will still be called
		std::unique_lock<std::mutex> lock(mutex);
		std::vector<Future> futures;
		for (auto& iter : callbackMap) {
			futures.push_back(iter.second.future);
		}
		lock.unlock();
		for (auto& f : futures) {
			f.cancel();
		}
	}

	// Inject a random sleep with a low probability
	void injectRandomSleep() {
		if (Random::get().randomBool(0.01)) {
			std::this_thread::sleep_for(std::chrono::milliseconds(Random::get().randomInt(1, 5)));
		}
	}

	// Object references for a future callback
	struct CallbackInfo {
		Future future;
		TTaskFct cont;
		std::shared_ptr<ITransactionContext> thisRef;
		bool retryOnError;
		TimePoint startTime;
	};

	// Map for keeping track of future waits and holding necessary object references
	std::unordered_map<FDBFuture*, CallbackInfo> callbackMap;

	// Holding reference to this for onError future C callback
	std::shared_ptr<AsyncTransactionContext> onErrorThisRef;
};

/**
 * Transaction executor base class, containing reusable functionality
 */
class TransactionExecutorBase : public ITransactionExecutor {
public:
	TransactionExecutorBase(const TransactionExecutorOptions& options) : options(options), scheduler(nullptr) {}

	void init(IScheduler* scheduler, const char* clusterFile, const std::string& bgBasePath) override {
		this->scheduler = scheduler;
		this->clusterFile = clusterFile;
		this->bgBasePath = bgBasePath;
	}

protected:
	// Execute the transaction on the given database instance
	void executeOnDatabase(FDBDatabase* db, std::shared_ptr<ITransactionActor> txActor, TTaskFct cont) {
		FDBTransaction* tx;
		fdb_error_t err = fdb_database_create_transaction(db, &tx);
		if (err != error_code_success) {
			txActor->complete(err);
			cont();
		} else {
			std::shared_ptr<ITransactionContext> ctx;
			if (options.blockOnFutures) {
				ctx = std::make_shared<BlockingTransactionContext>(
				    tx, txActor, cont, scheduler, options.transactionRetryLimit, bgBasePath);
			} else {
				ctx = std::make_shared<AsyncTransactionContext>(
				    tx, txActor, cont, scheduler, options.transactionRetryLimit, bgBasePath);
			}
			txActor->init(ctx);
			txActor->start();
		}
	}

protected:
	TransactionExecutorOptions options;
	std::string bgBasePath;
	std::string clusterFile;
	IScheduler* scheduler;
};

/**
 * Transaction executor load balancing transactions over a fixed pool of databases
 */
class DBPoolTransactionExecutor : public TransactionExecutorBase {
public:
	DBPoolTransactionExecutor(const TransactionExecutorOptions& options) : TransactionExecutorBase(options) {}

	~DBPoolTransactionExecutor() override { release(); }

	void init(IScheduler* scheduler, const char* clusterFile, const std::string& bgBasePath) override {
		TransactionExecutorBase::init(scheduler, clusterFile, bgBasePath);
		for (int i = 0; i < options.numDatabases; i++) {
			FDBDatabase* db;
			fdb_error_t err = fdb_create_database(clusterFile, &db);
			if (err != error_code_success) {
				throw TesterError(fmt::format("Failed create database with the cluster file '{}'. Error: {}({})",
				                              clusterFile,
				                              err,
				                              fdb_get_error(err)));
			}
			databases.push_back(db);
		}
	}

	void execute(std::shared_ptr<ITransactionActor> txActor, TTaskFct cont) override {
		int idx = Random::get().randomInt(0, options.numDatabases - 1);
		executeOnDatabase(databases[idx], txActor, cont);
	}

	void release() {
		for (FDBDatabase* db : databases) {
			fdb_database_destroy(db);
		}
	}

private:
	std::vector<FDBDatabase*> databases;
};

/**
 * Transaction executor executing each transaction on a separate database
 */
class DBPerTransactionExecutor : public TransactionExecutorBase {
public:
	DBPerTransactionExecutor(const TransactionExecutorOptions& options) : TransactionExecutorBase(options) {}

	void execute(std::shared_ptr<ITransactionActor> txActor, TTaskFct cont) override {
		FDBDatabase* db = nullptr;
		fdb_error_t err = fdb_create_database(clusterFile.c_str(), &db);
		if (err != error_code_success) {
			txActor->complete(err);
			cont();
		}
		executeOnDatabase(db, txActor, [cont, db]() {
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