/*
 * TesterTransactionExecutor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "test/fdb_api.hpp"
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <chrono>
#include <thread>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <filesystem>

namespace FdbApiTester {

constexpr int LONG_WAIT_TIME_US = 2000000;
constexpr int LARGE_NUMBER_OF_RETRIES = 10;

void ITransactionContext::continueAfterAll(std::vector<fdb::Future> futures, TTaskFct cont) {
	auto counter = std::make_shared<std::atomic<int>>(futures.size());
	auto errorCode = std::make_shared<std::atomic<fdb::Error>>(fdb::Error::success());
	auto thisPtr = shared_from_this();
	for (auto& f : futures) {
		continueAfter(
		    f,
		    [thisPtr, f, counter, errorCode, cont]() {
			    if (f.error().code() != error_code_success) {
				    (*errorCode) = f.error();
			    }
			    if (--(*counter) == 0) {
				    if (errorCode->load().code() == error_code_success) {
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
	TransactionContextBase(ITransactionExecutor* executor,
	                       TOpStartFct startFct,
	                       TOpContFct cont,
	                       IScheduler* scheduler,
	                       int retryLimit,
	                       std::string bgBasePath,
	                       std::optional<fdb::BytesRef> tenantName,
	                       bool transactional,
	                       bool restartOnTimeout)
	  : executor(executor), startFct(startFct), contAfterDone(cont), scheduler(scheduler), retryLimit(retryLimit),
	    txState(TxState::IN_PROGRESS), commitCalled(false), bgBasePath(bgBasePath), tenantName(tenantName),
	    transactional(transactional), restartOnTimeout(restartOnTimeout),
	    selfConflictingKey(Random::get().randomByteStringLowerCase(8, 8)) {
		databaseCreateErrorInjected = executor->getOptions().injectDatabaseCreateErrors &&
		                              Random::get().randomBool(executor->getOptions().databaseCreateErrorRatio);
		if (databaseCreateErrorInjected) {
			fdbDb = fdb::Database(executor->getClusterFileForErrorInjection());
		} else {
			fdbDb = executor->selectDatabase();
		}

		if (tenantName) {
			fdbTenant = fdbDb.openTenant(*tenantName);
			fdbDbOps = std::make_shared<fdb::Tenant>(fdbTenant);
		} else {
			fdbDbOps = std::make_shared<fdb::Database>(fdbDb);
		}

		if (transactional) {
			fdbTx = fdbDbOps->createTransaction();
		}
	}

	virtual ~TransactionContextBase() { ASSERT(txState == TxState::DONE); }

	// A state machine:
	// IN_PROGRESS -> (ON_ERROR -> IN_PROGRESS)* [-> ON_ERROR] -> DONE
	enum class TxState { IN_PROGRESS, ON_ERROR, DONE };

	fdb::Database db() override { return fdbDb.atomic_load(); }

	fdb::Tenant tenant() override { return fdbTenant.atomic_load(); }

	std::shared_ptr<fdb::IDatabaseOps> dbOps() override { return std::atomic_load(&fdbDbOps); }

	fdb::Transaction tx() override { return fdbTx.atomic_load(); }

	// Set a continuation to be executed when a future gets ready
	void continueAfter(fdb::Future f, TTaskFct cont, bool retryOnError) override {
		doContinueAfter(f, cont, retryOnError);
	}

	// Complete the transaction with a commit
	void commit() override {
		ASSERT(transactional);
		std::unique_lock<std::mutex> lock(mutex);
		if (txState != TxState::IN_PROGRESS) {
			return;
		}
		commitCalled = true;
		lock.unlock();
		fdb::Future f = fdbTx.commit();
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

		// No need for lock from here on, because only one thread
		// can enter DONE state and handle it

		if (retriedErrors.size() >= LARGE_NUMBER_OF_RETRIES) {
			fmt::print("Transaction succeeded after {} retries on errors: {}\n",
			           retriedErrors.size(),
			           fmt::join(retriedErrorCodes(), ", "));
		}

		if (transactional) {
			// cancel transaction so that any pending operations on it
			// fail gracefully
			fdbTx.cancel();
		}
		cleanUp();
		ASSERT(txState == TxState::DONE);
		contAfterDone(fdb::Error::success());
	}

	void makeSelfConflicting() override {
		ASSERT(transactional);
		if (restartOnTimeout) {
			auto transaction = tx();
			transaction.addReadConflictRange(selfConflictingKey, selfConflictingKey + fdb::Key(1, '\x00'));
			transaction.addWriteConflictRange(selfConflictingKey, selfConflictingKey + fdb::Key(1, '\x00'));
		}
	}

	std::string getBGBasePath() override { return bgBasePath; }

	virtual void onError(fdb::Error err) override {
		std::unique_lock<std::mutex> lock(mutex);
		if (txState != TxState::IN_PROGRESS) {
			// Ignore further errors, if the transaction is in the error handing mode or completed
			return;
		}
		txState = TxState::ON_ERROR;
		lock.unlock();

		// No need to hold the lock from here on, because ON_ERROR state is handled sequentially, and
		// other callbacks are simply ignored while it stays in this state

		if (!canRetry(err)) {
			return;
		}

		ASSERT(!onErrorFuture);

		if ((databaseCreateErrorInjected && canBeInjectedDatabaseCreateError(err.code())) ||
		    (restartOnTimeout && err.code() == error_code_transaction_timed_out)) {
			// Failed to create a database because of failure injection
			// Restart by recreating the transaction in a valid database
			recreateAndRestartTransaction();
		} else if (transactional) {
			onErrorArg = err;
			onErrorFuture = tx().onError(err);
			handleOnErrorFuture();
		} else if (err.retryable()) {
			restartTransaction();
		} else {
			transactionFailed(err);
		}
	}

protected:
	virtual void doContinueAfter(fdb::Future f, TTaskFct cont, bool retryOnError) = 0;

	virtual void handleOnErrorFuture() = 0;

	// Clean up transaction state after completing the transaction
	// Note that the object may live longer, because it is referenced
	// by not yet triggered callbacks
	void cleanUp() {
		ASSERT(txState == TxState::DONE);
		ASSERT(!onErrorFuture);
		cancelPendingFutures();
	}

	virtual void cancelPendingFutures() {}

	bool canBeInjectedDatabaseCreateError(fdb::Error::CodeType errCode) {
		return errCode == error_code_no_cluster_file_found || errCode == error_code_connection_string_invalid;
	}

	// Complete the transaction with an (unretriable) error
	void transactionFailed(fdb::Error err) {
		ASSERT(err);
		std::unique_lock<std::mutex> lock(mutex);
		if (txState == TxState::DONE) {
			return;
		}
		txState = TxState::DONE;
		lock.unlock();

		// No need for lock from here on, because only one thread
		// can enter DONE state and handle it

		cleanUp();
		contAfterDone(err);
	}

	// Handle result of an a transaction onError call
	void handleOnErrorResult() {
		ASSERT(txState == TxState::ON_ERROR);
		fdb::Error err = onErrorFuture.error();
		onErrorFuture = {};
		if (err) {
			if (restartOnTimeout && err.code() == error_code_transaction_timed_out) {
				recreateAndRestartTransaction();
			} else {
				transactionFailed(err);
			}
		} else {
			restartTransaction();
		}
	}

	void restartTransaction() {
		ASSERT(txState == TxState::ON_ERROR);
		cancelPendingFutures();
		std::unique_lock<std::mutex> lock(mutex);
		txState = TxState::IN_PROGRESS;
		commitCalled = false;
		lock.unlock();
		startFct(shared_from_this());
	}

	void recreateAndRestartTransaction() {
		auto thisRef = std::static_pointer_cast<TransactionContextBase>(shared_from_this());
		scheduler->schedule([thisRef]() {
			fdb::Database db = thisRef->executor->selectDatabase();
			thisRef->fdbDb.atomic_store(db);
			if (thisRef->tenantName) {
				fdb::Tenant tenant = db.openTenant(*thisRef->tenantName);
				thisRef->fdbTenant.atomic_store(tenant);
				std::atomic_store(&thisRef->fdbDbOps,
				                  std::dynamic_pointer_cast<fdb::IDatabaseOps>(std::make_shared<fdb::Tenant>(tenant)));
			} else {
				std::atomic_store(&thisRef->fdbDbOps,
				                  std::dynamic_pointer_cast<fdb::IDatabaseOps>(std::make_shared<fdb::Database>(db)));
			}
			if (thisRef->transactional) {
				thisRef->fdbTx.atomic_store(thisRef->fdbDbOps->createTransaction());
			}
			thisRef->restartTransaction();
		});
	}

	// Checks if a transaction can be retried. Fails the transaction if the check fails
	bool canRetry(fdb::Error lastErr) {
		ASSERT(txState == TxState::ON_ERROR);
		retriedErrors.push_back(lastErr);
		if (retryLimit == 0 || retriedErrors.size() <= retryLimit) {
			if (retriedErrors.size() == LARGE_NUMBER_OF_RETRIES) {
				fmt::print("Transaction already retried {} times, on errors: {}\n",
				           retriedErrors.size(),
				           fmt::join(retriedErrorCodes(), ", "));
			}
			return true;
		}
		fmt::print("Transaction retry limit reached. Retried on errors: {}\n", fmt::join(retriedErrorCodes(), ", "));
		transactionFailed(lastErr);
		return false;
	}

	std::vector<fdb::Error::CodeType> retriedErrorCodes() {
		std::vector<fdb::Error::CodeType> retriedErrorCodes;
		for (auto e : retriedErrors) {
			retriedErrorCodes.push_back(e.code());
		}
		return retriedErrorCodes;
	}

	// Pointer to the transaction executor interface
	// Set in constructor, stays immutable
	ITransactionExecutor* const executor;

	// FDB database
	// Provides a thread safe interface by itself (no need for mutex)
	fdb::Database fdbDb;

	// FDB tenant
	// Provides a thread safe interface by itself (no need for mutex)
	fdb::Tenant fdbTenant;

	// FDB IDatabaseOps to hide database/tenant accordingly.
	// Provides a shared pointer to database functions based on if db or tenant.
	std::shared_ptr<fdb::IDatabaseOps> fdbDbOps;

	// FDB transaction
	// Provides a thread safe interface by itself (no need for mutex)
	fdb::Transaction fdbTx;

	// The function implementing the starting point of the transaction
	// Set in constructor and reset on cleanup (no need for mutex)
	TOpStartFct startFct;

	// Mutex protecting access to shared mutable state
	// Only the state that is accessible under IN_PROGRESS state
	// must be protected by mutex
	std::mutex mutex;

	// Continuation to be called after completion of the transaction
	// Set in constructor, stays immutable
	const TOpContFct contAfterDone;

	// Reference to the scheduler
	// Set in constructor, stays immutable
	// Cannot be accessed in DONE state, workloads can be completed and the scheduler deleted
	IScheduler* const scheduler;

	// Retry limit
	// Set in constructor, stays immutable
	const int retryLimit;

	// Transaction execution state
	// Must be accessed under mutex
	TxState txState;

	// onError future
	// used only in ON_ERROR state (no need for mutex)
	fdb::Future onErrorFuture;

	// The error code on which onError was called
	// used only in ON_ERROR state (no need for mutex)
	fdb::Error onErrorArg;

	// The time point of calling onError
	// used only in ON_ERROR state (no need for mutex)
	TimePoint onErrorCallTimePoint;

	// Transaction is committed or being committed
	// Must be accessed under mutex
	bool commitCalled;

	// A history of errors on which the transaction was retried
	// used only in ON_ERROR and DONE states (no need for mutex)
	std::vector<fdb::Error> retriedErrors;

	// blob granule base path
	// Set in constructor, stays immutable
	const std::string bgBasePath;

	// Indicates if the database error was injected
	// Accessed on initialization and in ON_ERROR state only (no need for mutex)
	bool databaseCreateErrorInjected;

	// Restart the transaction automatically on timeout errors
	const bool restartOnTimeout;

	// The tenant that we will run this transaction in
	const std::optional<fdb::BytesRef> tenantName;

	// Specifies whether the operation is transactional
	const bool transactional;

	// A randomly generated key for making transaction self-conflicting
	const fdb::Key selfConflictingKey;
};

/**
 *  Transaction context using blocking waits to implement continuations on futures
 */
class BlockingTransactionContext : public TransactionContextBase {
public:
	BlockingTransactionContext(ITransactionExecutor* executor,
	                           TOpStartFct startFct,
	                           TOpContFct cont,
	                           IScheduler* scheduler,
	                           int retryLimit,
	                           std::string bgBasePath,
	                           std::optional<fdb::BytesRef> tenantName,
	                           bool transactional,
	                           bool restartOnTimeout)
	  : TransactionContextBase(executor,
	                           startFct,
	                           cont,
	                           scheduler,
	                           retryLimit,
	                           bgBasePath,
	                           tenantName,
	                           transactional,
	                           restartOnTimeout) {}

protected:
	void doContinueAfter(fdb::Future f, TTaskFct cont, bool retryOnError) override {
		auto thisRef = std::static_pointer_cast<BlockingTransactionContext>(shared_from_this());
		scheduler->schedule(
		    [thisRef, f, cont, retryOnError]() mutable { thisRef->blockingContinueAfter(f, cont, retryOnError); });
	}

	void blockingContinueAfter(fdb::Future f, TTaskFct cont, bool retryOnError) {
		std::unique_lock<std::mutex> lock(mutex);
		if (txState != TxState::IN_PROGRESS) {
			return;
		}
		lock.unlock();
		auto start = timeNow();
		fdb::Error err = f.blockUntilReady();
		if (err) {
			transactionFailed(err);
			return;
		}
		err = f.error();
		auto waitTimeUs = timeElapsedInUs(start);
		if (waitTimeUs > LONG_WAIT_TIME_US) {
			fmt::print("Long waiting time on a future: {:.3f}s, return code {} ({}), commit called: {}\n",
			           microsecToSec(waitTimeUs),
			           err.code(),
			           err.what(),
			           commitCalled);
		}
		if (err.code() == error_code_transaction_cancelled) {
			return;
		}
		if (err.code() == error_code_success || !retryOnError) {
			scheduler->schedule([cont]() { cont(); });
			return;
		}

		onError(err);
	}

	virtual void handleOnErrorFuture() override {
		ASSERT(txState == TxState::ON_ERROR);

		auto start = timeNow();
		fdb::Error err2 = onErrorFuture.blockUntilReady();
		if (err2) {
			transactionFailed(err2);
			return;
		}
		auto waitTimeUs = timeElapsedInUs(start);
		if (waitTimeUs > LONG_WAIT_TIME_US) {
			fdb::Error err3 = onErrorFuture.error();
			fmt::print("Long waiting time on onError({}) future: {:.3f}s, return code {} ({})\n",
			           onErrorArg.code(),
			           microsecToSec(waitTimeUs),
			           err3.code(),
			           err3.what());
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
	AsyncTransactionContext(ITransactionExecutor* executor,
	                        TOpStartFct startFct,
	                        TOpContFct cont,
	                        IScheduler* scheduler,
	                        int retryLimit,
	                        std::string bgBasePath,
	                        std::optional<fdb::BytesRef> tenantName,
	                        bool transactional,
	                        bool restartOnTimeout)
	  : TransactionContextBase(executor,
	                           startFct,
	                           cont,
	                           scheduler,
	                           retryLimit,
	                           bgBasePath,
	                           tenantName,
	                           transactional,
	                           restartOnTimeout) {}

protected:
	void doContinueAfter(fdb::Future f, TTaskFct cont, bool retryOnError) override {
		std::unique_lock<std::mutex> lock(mutex);
		if (txState != TxState::IN_PROGRESS) {
			return;
		}
		callbackMap[f] = CallbackInfo{ f, cont, shared_from_this(), retryOnError, timeNow(), false };
		lock.unlock();
		try {
			f.then([this](fdb::Future f) { futureReadyCallback(f, this); });
		} catch (std::exception& err) {
			lock.lock();
			callbackMap.erase(f);
			lock.unlock();
			transactionFailed(fdb::Error(error_code_operation_failed));
		}
	}

	static void futureReadyCallback(fdb::Future f, void* param) {
		try {
			AsyncTransactionContext* txCtx = (AsyncTransactionContext*)param;
			txCtx->onFutureReady(f);
		} catch (std::exception& err) {
			fmt::print("Unexpected exception in callback {}\n", err.what());
			abort();
		} catch (...) {
			fmt::print("Unknown error in callback\n");
			abort();
		}
	}

	void onFutureReady(fdb::Future f) {
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
		fdb::Error err = f.error();
		auto waitTimeUs = timeElapsedInUs(cbInfo.startTime, endTime);
		if (waitTimeUs > LONG_WAIT_TIME_US) {
			fmt::print("Long waiting time on a future: {:.3f}s, return code {} ({})\n",
			           microsecToSec(waitTimeUs),
			           err.code(),
			           err.what());
		}
		if (err.code() == error_code_transaction_cancelled || cbInfo.cancelled) {
			return;
		}
		if (err.code() == error_code_success || !cbInfo.retryOnError) {
			scheduler->schedule(cbInfo.cont);
			return;
		}
		// We keep lock until here to prevent transitions from the IN_PROGRESS state
		// which could possibly lead to completion of the workload and destruction
		// of the scheduler
		lock.unlock();
		onError(err);
	}

	virtual void handleOnErrorFuture() override {
		ASSERT(txState == TxState::ON_ERROR);

		onErrorCallTimePoint = timeNow();
		onErrorThisRef = std::static_pointer_cast<AsyncTransactionContext>(shared_from_this());
		try {
			onErrorFuture.then([this](fdb::Future f) { onErrorReadyCallback(f, this); });
		} catch (...) {
			onErrorFuture = {};
			transactionFailed(fdb::Error(error_code_operation_failed));
		}
	}

	static void onErrorReadyCallback(fdb::Future f, void* param) {
		try {
			AsyncTransactionContext* txCtx = (AsyncTransactionContext*)param;
			txCtx->onErrorReady(f);
		} catch (std::exception& err) {
			fmt::print("Unexpected exception in callback {}\n", err.what());
			abort();
		} catch (...) {
			fmt::print("Unknown error in callback\n");
			abort();
		}
	}

	void onErrorReady(fdb::Future f) {
		auto waitTimeUs = timeElapsedInUs(onErrorCallTimePoint);
		if (waitTimeUs > LONG_WAIT_TIME_US) {
			fdb::Error err = onErrorFuture.error();
			fmt::print("Long waiting time on onError({}): {:.3f}s, return code {} ({})\n",
			           onErrorArg.code(),
			           microsecToSec(waitTimeUs),
			           err.code(),
			           err.what());
		}
		injectRandomSleep();
		auto thisRef = onErrorThisRef;
		onErrorThisRef = {};
		scheduler->schedule([thisRef]() { thisRef->handleOnErrorResult(); });
	}

	void cancelPendingFutures() override {
		// Cancel all pending operations
		// Note that the callbacks of the cancelled futures will still be called
		std::unique_lock<std::mutex> lock(mutex);
		std::vector<fdb::Future> futures;
		for (auto& iter : callbackMap) {
			iter.second.cancelled = true;
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
		fdb::Future future;
		TTaskFct cont;
		std::shared_ptr<ITransactionContext> thisRef;
		bool retryOnError;
		TimePoint startTime;
		bool cancelled;
	};

	// Map for keeping track of future waits and holding necessary object references
	// It can be accessed at any time when callbacks are triggered, so it mus always
	// be mutex protected
	std::unordered_map<fdb::Future, CallbackInfo> callbackMap;

	// Holding reference to this for onError future C callback
	// Accessed only in ON_ERROR state (no need for mutex)
	std::shared_ptr<AsyncTransactionContext> onErrorThisRef;
};

/**
 * Transaction executor base class, containing reusable functionality
 */
class TransactionExecutorBase : public ITransactionExecutor {
public:
	TransactionExecutorBase(const TransactionExecutorOptions& options) : options(options), scheduler(nullptr) {}

	~TransactionExecutorBase() {
		if (tamperClusterFileThread.joinable()) {
			tamperClusterFileThread.join();
		}
	}

	void init(IScheduler* scheduler, const char* clusterFile, const std::string& bgBasePath) override {
		this->scheduler = scheduler;
		this->clusterFile = clusterFile;
		this->bgBasePath = bgBasePath;

		ASSERT(!options.tmpDir.empty());
		emptyClusterFile.create(options.tmpDir, "fdbempty.cluster");
		invalidClusterFile.create(options.tmpDir, "fdbinvalid.cluster");
		invalidClusterFile.write(Random().get().randomStringLowerCase<std::string>(1, 100));

		emptyListClusterFile.create(options.tmpDir, "fdbemptylist.cluster");
		emptyListClusterFile.write(fmt::format("{}:{}@",
		                                       Random().get().randomStringLowerCase<std::string>(3, 8),
		                                       Random().get().randomStringLowerCase<std::string>(1, 100)));

		if (options.tamperClusterFile) {
			tamperedClusterFile.create(options.tmpDir, "fdb.cluster");
			originalClusterFile = clusterFile;
			this->clusterFile = tamperedClusterFile.getFileName();

			// begin with a valid cluster file, but with non existing address
			tamperedClusterFile.write(fmt::format("{}:{}@192.168.{}.{}:{}",
			                                      Random().get().randomStringLowerCase<std::string>(3, 8),
			                                      Random().get().randomStringLowerCase<std::string>(1, 100),
			                                      Random().get().randomInt(1, 254),
			                                      Random().get().randomInt(1, 254),
			                                      Random().get().randomInt(2000, 10000)));

			tamperClusterFileThread = std::thread([this]() {
				std::this_thread::sleep_for(std::chrono::seconds(2));
				// now write an invalid connection string
				tamperedClusterFile.write(fmt::format("{}:{}@",
				                                      Random().get().randomStringLowerCase<std::string>(3, 8),
				                                      Random().get().randomStringLowerCase<std::string>(1, 100)));
				std::this_thread::sleep_for(std::chrono::seconds(2));
				// finally use correct cluster file contents
				std::filesystem::copy_file(std::filesystem::path(originalClusterFile),
				                           std::filesystem::path(tamperedClusterFile.getFileName()),
				                           std::filesystem::copy_options::overwrite_existing);
			});
		}
	}

	const TransactionExecutorOptions& getOptions() override { return options; }

	void execute(TOpStartFct startFct,
	             TOpContFct cont,
	             std::optional<fdb::BytesRef> tenantName,
	             bool transactional,
	             bool restartOnTimeout) override {
		try {
			std::shared_ptr<ITransactionContext> ctx;
			if (options.blockOnFutures) {
				ctx = std::make_shared<BlockingTransactionContext>(this,
				                                                   startFct,
				                                                   cont,
				                                                   scheduler,
				                                                   options.transactionRetryLimit,
				                                                   bgBasePath,
				                                                   tenantName,
				                                                   transactional,
				                                                   restartOnTimeout);
			} else {
				ctx = std::make_shared<AsyncTransactionContext>(this,
				                                                startFct,
				                                                cont,
				                                                scheduler,
				                                                options.transactionRetryLimit,
				                                                bgBasePath,
				                                                tenantName,
				                                                transactional,
				                                                restartOnTimeout);
			}
			startFct(ctx);
		} catch (...) {
			cont(fdb::Error(error_code_operation_failed));
		}
	}

	std::string getClusterFileForErrorInjection() override {
		switch (Random::get().randomInt(0, 3)) {
		case 0:
			return fmt::format("{}{}", "not-existing-file", Random::get().randomStringLowerCase<std::string>(0, 2));
		case 1:
			return emptyClusterFile.getFileName();
		case 2:
			return invalidClusterFile.getFileName();
		default: // case 3
			return emptyListClusterFile.getFileName();
		}
	}

protected:
	TransactionExecutorOptions options;
	std::string bgBasePath;
	std::string clusterFile;
	IScheduler* scheduler;
	TmpFile emptyClusterFile;
	TmpFile invalidClusterFile;
	TmpFile emptyListClusterFile;
	TmpFile tamperedClusterFile;
	std::thread tamperClusterFileThread;
	std::string originalClusterFile;
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
			fdb::Database db(this->clusterFile);
			databases.push_back(db);
		}
	}

	fdb::Database selectDatabase() override {
		int idx = Random::get().randomInt(0, options.numDatabases - 1);
		return databases[idx];
	}

private:
	void release() { databases.clear(); }

	std::vector<fdb::Database> databases;
};

/**
 * Transaction executor executing each transaction on a separate database
 */
class DBPerTransactionExecutor : public TransactionExecutorBase {
public:
	DBPerTransactionExecutor(const TransactionExecutorOptions& options) : TransactionExecutorBase(options) {}

	fdb::Database selectDatabase() override { return fdb::Database(clusterFile.c_str()); }
};

std::unique_ptr<ITransactionExecutor> createTransactionExecutor(const TransactionExecutorOptions& options) {
	if (options.databasePerTransaction) {
		return std::make_unique<DBPerTransactionExecutor>(options);
	} else {
		return std::make_unique<DBPoolTransactionExecutor>(options);
	}
}

} // namespace FdbApiTester
