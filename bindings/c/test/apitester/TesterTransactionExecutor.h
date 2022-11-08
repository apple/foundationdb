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

	// Current FDB database
	virtual fdb::Database db() = 0;

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

	// Make the transaction self-conflicting if a timeout is set
	// It is necessary for avoiding in-flight transactions when retrying write transactions
	// on timeouts
	virtual void makeSelfConflicting() = 0;

	// Plumbing for blob granule base path
	virtual std::string getBGBasePath() = 0;

	// A continuation to be executed when all of the given futures get ready
	virtual void continueAfterAll(std::vector<fdb::Future> futures, TTaskFct cont);
};

// Type of the lambda functions implementing a database operation
using TOpStartFct = std::function<void(std::shared_ptr<ITransactionContext>)>;

// Type of the lambda functions implementing a database operation
using TOpContFct = std::function<void(fdb::Error)>;

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
	virtual void execute(TOpStartFct start,
	                     TOpContFct cont,
	                     std::optional<fdb::BytesRef> tenantName,
	                     bool transactional,
	                     bool restartOnTimeout) = 0;
	virtual fdb::Database selectDatabase() = 0;
	virtual std::string getClusterFileForErrorInjection() = 0;
	virtual const TransactionExecutorOptions& getOptions() = 0;
};

// Create a transaction executor for the given options
std::unique_ptr<ITransactionExecutor> createTransactionExecutor(const TransactionExecutorOptions& options);

} // namespace FdbApiTester

#endif
