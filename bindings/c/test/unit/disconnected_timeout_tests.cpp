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

#define FDB_API_VERSION 710
#include <foundationdb/fdb_c.h>

#include <chrono>
#include <iostream>
#include <string.h>
#include <thread>

#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest.h"
#include "fdb_api.hpp"

void fdb_check(fdb_error_t e) {
	if (e) {
		std::cerr << fdb_get_error(e) << std::endl;
		std::abort();
	}
}

FDBDatabase* fdb_open_database(const char* clusterFile) {
	FDBDatabase* db;
	fdb_check(fdb_create_database(clusterFile, &db));
	return db;
}

static FDBDatabase* db = nullptr;
static FDBDatabase* timeoutDb = nullptr;

// Blocks until the given future is ready, returning an error code if there was
// an issue.
fdb_error_t wait_future(fdb::Future& f) {
	fdb_check(f.block_until_ready());
	return f.get_error();
}

void validateTimeoutDuration(double expectedSeconds, std::chrono::time_point<std::chrono::steady_clock> start) {
	std::chrono::duration<double> duration = std::chrono::steady_clock::now() - start;
	double actualSeconds = duration.count();
	CHECK(actualSeconds >= expectedSeconds - 1e-3);
	CHECK(actualSeconds < expectedSeconds * 2);
}

TEST_CASE("500ms_transaction_timeout") {
	auto start = std::chrono::steady_clock::now();

	fdb::Transaction tr(db);

	int64_t timeout = 500;
	fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	fdb::Int64Future grvFuture = tr.get_read_version();
	fdb_error_t err = wait_future(grvFuture);

	CHECK(err == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("500ms_transaction_timeout_after_op") {
	auto start = std::chrono::steady_clock::now();

	fdb::Transaction tr(db);
	fdb::Int64Future grvFuture = tr.get_read_version();

	int64_t timeout = 500;
	fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	fdb_error_t err = wait_future(grvFuture);

	CHECK(err == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("500ms_transaction_timeout_before_op_2000ms_after") {
	auto start = std::chrono::steady_clock::now();

	fdb::Transaction tr(db);

	int64_t timeout = 500;
	fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	fdb::Int64Future grvFuture = tr.get_read_version();

	timeout = 2000;
	fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	fdb_error_t err = wait_future(grvFuture);

	CHECK(err == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("2000ms_transaction_timeout_before_op_500ms_after") {
	auto start = std::chrono::steady_clock::now();

	fdb::Transaction tr(db);

	int64_t timeout = 2000;
	fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	fdb::Int64Future grvFuture = tr.get_read_version();

	timeout = 500;
	fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	fdb_error_t err = wait_future(grvFuture);

	CHECK(err == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("500ms_database_timeout") {
	auto start = std::chrono::steady_clock::now();

	int64_t timeout = 500;
	fdb_check(fdb_database_set_option(
	    timeoutDb, FDB_DB_OPTION_TRANSACTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	fdb::Transaction tr(timeoutDb);

	fdb::Int64Future grvFuture = tr.get_read_version();
	fdb_error_t err = wait_future(grvFuture);

	CHECK(err == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("2000ms_database_timeout_500ms_transaction_timeout") {
	auto start = std::chrono::steady_clock::now();

	int64_t timeout = 2000;
	fdb_check(fdb_database_set_option(
	    timeoutDb, FDB_DB_OPTION_TRANSACTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	fdb::Transaction tr(timeoutDb);

	timeout = 500;
	fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	fdb::Int64Future grvFuture = tr.get_read_version();
	fdb_error_t err = wait_future(grvFuture);

	CHECK(err == 1031);
	validateTimeoutDuration(timeout / 1000.0, start);
}

TEST_CASE("500ms_database_timeout_2000ms_transaction_timeout_with_reset") {
	auto start = std::chrono::steady_clock::now();

	int64_t dbTimeout = 500;
	fdb_check(fdb_database_set_option(
	    timeoutDb, FDB_DB_OPTION_TRANSACTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&dbTimeout), sizeof(dbTimeout)));

	fdb::Transaction tr(timeoutDb);

	int64_t trTimeout = 2000;
	fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&trTimeout), sizeof(trTimeout)));

	tr.reset();

	fdb::Int64Future grvFuture = tr.get_read_version();
	fdb_error_t err = wait_future(grvFuture);

	CHECK(err == 1031);
	validateTimeoutDuration(dbTimeout / 1000.0, start);
}

TEST_CASE("transaction_reset_cancels_without_timeout") {
	fdb::Transaction tr(db);
	fdb::Int64Future grvFuture = tr.get_read_version();
	tr.reset();

	fdb_error_t err = wait_future(grvFuture);
	CHECK(err == 1025);
}

TEST_CASE("transaction_reset_cancels_with_timeout") {
	fdb::Transaction tr(db);

	int64_t timeout = 500;
	fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	fdb::Int64Future grvFuture = tr.get_read_version();
	tr.reset();

	fdb_error_t err = wait_future(grvFuture);
	CHECK(err == 1025);
}

TEST_CASE("transaction_destruction_cancels_without_timeout") {
	FDBTransaction* tr;
	fdb_check(fdb_database_create_transaction(db, &tr));

	FDBFuture* grvFuture = fdb_transaction_get_read_version(tr);
	fdb_transaction_destroy(tr);

	fdb_check(fdb_future_block_until_ready(grvFuture));
	fdb_error_t err = fdb_future_get_error(grvFuture);
	CHECK(err == 1025);

	fdb_future_destroy(grvFuture);
}

TEST_CASE("transaction_destruction_cancels_with_timeout") {
	FDBTransaction* tr;
	fdb_check(fdb_database_create_transaction(db, &tr));

	int64_t timeout = 500;
	fdb_check(fdb_transaction_set_option(
	    tr, FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));

	FDBFuture* grvFuture = fdb_transaction_get_read_version(tr);
	fdb_transaction_destroy(tr);

	fdb_check(fdb_future_block_until_ready(grvFuture));
	fdb_error_t err = fdb_future_get_error(grvFuture);
	CHECK(err == 1025);

	fdb_future_destroy(grvFuture);
}

TEST_CASE("transaction_set_timeout_and_destroy_repeatedly") {
	for (int i = 0; i < 1000; ++i) {
		fdb::Transaction tr(db);
		int64_t timeout = 500;
		fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t*>(&timeout), sizeof(timeout)));
	}
}

int main(int argc, char** argv) {
	if (argc < 2) {
		std::cout << "Disconnected timeout unit tests for the FoundationDB C API.\n"
		          << "Usage: disconnected_timeout_tests <unavailableClusterFile> [externalClient] [doctest args]"
		          << std::endl;
		return 1;
	}
	fdb_check(fdb_select_api_version(710));
	if (argc >= 3) {
		std::string externalClientLibrary = argv[2];
		if (externalClientLibrary.substr(0, 2) != "--") {
			fdb_check(fdb_network_set_option(
			    FDBNetworkOption::FDB_NET_OPTION_DISABLE_LOCAL_CLIENT, reinterpret_cast<const uint8_t*>(""), 0));
			fdb_check(fdb_network_set_option(FDBNetworkOption::FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY,
			                                 reinterpret_cast<const uint8_t*>(externalClientLibrary.c_str()),
			                                 externalClientLibrary.size()));
		}
	}

	doctest::Context context;
	context.applyCommandLine(argc, argv);

	fdb_check(fdb_setup_network());
	std::thread network_thread{ &fdb_run_network };

	db = fdb_open_database(argv[1]);
	timeoutDb = fdb_open_database(argv[1]);

	int res = context.run();
	fdb_database_destroy(db);
	fdb_database_destroy(timeoutDb);

	if (context.shouldExit()) {
		fdb_check(fdb_stop_network());
		network_thread.join();
		return res;
	}
	fdb_check(fdb_stop_network());
	network_thread.join();

	return res;
}
