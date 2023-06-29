/*
 * disconnected_timeout_tests.cpp
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

// Unit tests that test the timeouts for a disconnected cluster

#define FDB_USE_LATEST_API_VERSION

#include <chrono>
#include <iostream>
#include <string.h>
#include <thread>

#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest.h"

#include "test/fdb_api.hpp"

void fdbCheck(const fdb::Error& err) {
	if (err) {
		std::cerr << err.what() << std::endl;
		std::abort();
	}
}

static fdb::Database db;
static fdb::Database timeoutDb;

fdb::Error waitFuture(fdb::Future& f) {
	fdbCheck(f.blockUntilReady());
	return f.error();
}

void validateTimeoutDuration(double expectedSeconds, std::chrono::time_point<std::chrono::steady_clock> start) {
	std::chrono::duration<double> duration = std::chrono::steady_clock::now() - start;
	double actualSeconds = duration.count();
	CHECK(actualSeconds >= expectedSeconds - 1e-3);
	CHECK(actualSeconds < expectedSeconds * 2);
}

TEST_CASE("500ms_transaction_timeout") {
	auto start = std::chrono::steady_clock::now();

	auto tr = db.createTransaction();

	int64_t timeout = 500;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);

	auto grvFuture = tr.getReadVersion();
	auto err = waitFuture(grvFuture);
	CHECK(err.code() == 1031);

	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("500ms_transaction_timeout_after_op") {
	auto start = std::chrono::steady_clock::now();

	auto tr = db.createTransaction();
	auto grvFuture = tr.getReadVersion();

	int64_t timeout = 500;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);

	auto err = waitFuture(grvFuture);
	CHECK(err.code() == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("500ms_transaction_timeout_before_op_2000ms_after") {
	auto start = std::chrono::steady_clock::now();

	auto tr = db.createTransaction();

	int64_t timeout = 500;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);

	auto grvFuture = tr.getReadVersion();
	timeout = 2000;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);

	auto err = waitFuture(grvFuture);
	CHECK(err.code() == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("2000ms_transaction_timeout_before_op_500ms_after") {
	auto start = std::chrono::steady_clock::now();

	auto tr = db.createTransaction();

	int64_t timeout = 2000;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);

	auto grvFuture = tr.getReadVersion();
	timeout = 500;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);

	auto err = waitFuture(grvFuture);
	CHECK(err.code() == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("500ms_database_timeout") {
	auto start = std::chrono::steady_clock::now();

	int64_t timeout = 500;
	timeoutDb.setOption(FDB_DB_OPTION_TRANSACTION_TIMEOUT, timeout);

	auto tr = timeoutDb.createTransaction();

	auto grvFuture = tr.getReadVersion();
	auto err = waitFuture(grvFuture);

	CHECK(err.code() == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("2000ms_database_timeout_500ms_transaction_timeout") {
	auto start = std::chrono::steady_clock::now();

	int64_t timeout = 2000;
	timeoutDb.setOption(FDB_DB_OPTION_TRANSACTION_TIMEOUT, timeout);

	auto tr = timeoutDb.createTransaction();

	timeout = 500;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);

	auto grvFuture = tr.getReadVersion();
	auto err = waitFuture(grvFuture);

	CHECK(err.code() == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("500ms_database_timeout_2000ms_transaction_timeout_with_reset") {
	auto start = std::chrono::steady_clock::now();

	int64_t dbTimeout = 500;
	timeoutDb.setOption(FDB_DB_OPTION_TRANSACTION_TIMEOUT, dbTimeout);

	auto tr = timeoutDb.createTransaction();

	int64_t trTimeout = 2000;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, trTimeout);

	tr.reset();

	auto grvFuture = tr.getReadVersion();
	auto err = waitFuture(grvFuture);

	CHECK(err.code() == 1031);
	validateTimeoutDuration(dbTimeout / 1000.0, start);
}

TEST_CASE("transaction_reset_cancels_without_timeout") {
	auto tr = db.createTransaction();
	auto grvFuture = tr.getReadVersion();
	tr.reset();

	auto err = waitFuture(grvFuture);
	CHECK(err.code() == 1025);
}

TEST_CASE("transaction_reset_cancels_with_timeout") {
	auto tr = db.createTransaction();

	int64_t timeout = 500;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);

	auto grvFuture = tr.getReadVersion();
	tr.reset();

	auto err = waitFuture(grvFuture);
	CHECK(err.code() == 1025);
}

TEST_CASE("transaction_destruction_cancels_without_timeout") {
	auto tr = db.createTransaction();
	auto grvFuture = tr.getReadVersion();
	// Force the destruction of the original transaction
	tr = fdb::Transaction();
	auto err = waitFuture(grvFuture);
	CHECK(err.code() == 1025);
}

TEST_CASE("transaction_destruction_cancels_with_timeout") {
	auto tr = db.createTransaction();
	int64_t timeout = 500;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);
	auto grvFuture = tr.getReadVersion();
	// Force the destruction of the original transaction
	tr = fdb::Transaction();
	auto err = waitFuture(grvFuture);
	CHECK(err.code() == 1025);
}

TEST_CASE("transaction_set_timeout_and_destroy_repeatedly") {
	for (int i = 0; i < 1000; ++i) {
		auto tr = db.createTransaction();
		int64_t timeout = 500;
		tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);
	}
}

int main(int argc, char** argv) {
	if (argc < 2) {
		std::cout << "Disconnected timeout unit tests for the FoundationDB C API.\n"
		          << "Usage: disconnected_timeout_tests <unavailableClusterFile> [externalClient] [doctest args]"
		          << std::endl;
		return 1;
	}
	fdb::selectApiVersion(FDB_API_VERSION);
	if (argc >= 3) {
		std::string externalClientLibrary = argv[2];
		if (externalClientLibrary.substr(0, 2) != "--") {
			fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_DISABLE_LOCAL_CLIENT);
			fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY, externalClientLibrary);
		}
	}

	doctest::Context context;
	context.applyCommandLine(argc, argv);

	fdb::network::setup();
	std::thread network_thread{ [] { fdbCheck(fdb::network::run()); } };

	db = fdb::Database(argv[1]);
	timeoutDb = fdb::Database(argv[1]);

	int res = context.run();

	if (context.shouldExit()) {
		fdbCheck(fdb::network::stop());
		network_thread.join();
		return res;
	}
	fdbCheck(fdb::network::stop());
	network_thread.join();

	return res;
}
