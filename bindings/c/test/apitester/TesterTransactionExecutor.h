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

#include "test/fdb_api.hpp"
#include "TesterOptions.h"
#include "TesterScheduler.h"
#include <string_view>
#include <memory>

namespace FdbApiTester {

/**
 * Interface to be used for implementation of a concrete transaction
 */
class ITransactionContext : public std::enable_shared_from_this<ITransactionContext> {
public:
	virtual ~ITransactionContext() {}

	// Current FDB transaction
	virtual fdb::Transaction tx() = 0;

	// Schedule a continuation to be executed when the future gets ready
	// retryOnError controls whether transaction is retried in case of an error instead
	// of calling the continuation
	virtual void continueAfter(fdb::Future f, TTaskFct cont, bool retryOnError = true) = 0;

	// Complete the transaction with a commit
	virtual void commit() = 0;

	// retry transaction on error
	virtual void onError(fdb::Error err) = 0;

	// Mark the transaction as completed without committing it (for read transactions)
	virtual void done() = 0;

	// Plumbing for blob granule base path
	virtual std::string getBGBasePath() = 0;

	// A continuation to be executed when all of the given futures get ready
	virtual void continueAfterAll(std::vector<fdb::Future> futures, TTaskFct cont);
};

/**
 * Interface of an actor object implementing a concrete transaction
 */
class ITransactionActor {
public:
	virtual ~ITransactionActor() {}

	// Initialize with the given transaction context
	virtual void init(std::shared_ptr<ITransactionContext> ctx) = 0;

	// Start execution of the transaction, also called on retries
	virtual void start() = 0;

	// Transaction completion result (error_code_success in case of success)
	virtual fdb::Error getError() = 0;

	// Notification about the completion of the transaction
	virtual void complete(fdb::Error err) = 0;
};

/**
 * A helper base class for transaction actors
 */
class TransactionActorBase : public ITransactionActor {
public:
	void init(std::shared_ptr<ITransactionContext> ctx) override { context = ctx; }
	fdb::Error getError() override { return error; }
	void complete(fdb::Error err) override;

protected:
	std::shared_ptr<ITransactionContext> ctx() { return context; }

private:
	std::shared_ptr<ITransactionContext> context;
	fdb::Error error = fdb::Error::success();
};

// Type of the lambda functions implementing a transaction
using TTxStartFct = std::function<void(std::shared_ptr<ITransactionContext>)>;

/**
 * A wrapper class for transactions implemented by lambda functions
 */
class TransactionFct : public TransactionActorBase {
public:
	TransactionFct(TTxStartFct startFct) : startFct(startFct) {}
	void start() override { startFct(this->ctx()); }

private:
	TTxStartFct startFct;
};

/**
 * Configuration of transaction execution mode
 */
struct TransactionExecutorOptions {
	// Use blocking waits on futures
	bool blockOnFutures = false;

	// Create each transaction in a separate database instance
	bool databasePerTransaction = false;

	// Enable injection of database create errors
	bool injectDatabaseCreateErrors = false;

	// Test tampering cluster file contents
	bool tamperClusterFile = false;

	// The probability of injected database create errors
	// Used if injectDatabaseCreateErrors = true
	double databaseCreateErrorRatio = 0.1;

	// The size of the database instance pool
	int numDatabases = 1;

	// The number of tenants to create in the cluster. If 0, no tenants are used.
	int numTenants = 0;

	// Maximum number of retries per transaction (0 - unlimited)
	int transactionRetryLimit = 0;

	// Temporary directory
	std::string tmpDir;
};

/**
 * Transaction executor provides an interface for executing transactions
 * It is responsible for instantiating FDB databases and transactions and managing their lifecycle
 * according to the provided options
 */
class ITransactionExecutor {
public:
	virtual ~ITransactionExecutor() {}
	virtual void init(IScheduler* sched, const char* clusterFile, const std::string& bgBasePath) = 0;
	virtual void execute(std::shared_ptr<ITransactionActor> tx,
	                     TTaskFct cont,
	                     std::optional<fdb::BytesRef> tenantName = {}) = 0;
	virtual fdb::Database selectDatabase() = 0;
	virtual std::string getClusterFileForErrorInjection() = 0;
	virtual const TransactionExecutorOptions& getOptions() = 0;
};

// Create a transaction executor for the given options
std::unique_ptr<ITransactionExecutor> createTransactionExecutor(const TransactionExecutorOptions& options);

} // namespace FdbApiTester

#endif
