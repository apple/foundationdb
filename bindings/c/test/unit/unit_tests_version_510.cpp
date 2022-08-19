/*
 * unit_tests_header_510.cpp
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

// Unit tests for the FoundationDB C API, at api header version 510

#include "fdb_c_options.g.h"
#include <thread>

#define FDB_API_VERSION 510
static_assert(FDB_API_VERSION == 510, "Don't change this! This test intentionally tests an old api header version");

#include <foundationdb/fdb_c.h>

#define DOCTEST_CONFIG_IMPLEMENT
#include "doctest.h"

#include "flow/config.h"

void fdb_check(fdb_error_t e) {
	if (e) {
		std::cerr << fdb_get_error(e) << std::endl;
		std::abort();
	}
}

std::string clusterFilePath;
std::string prefix;

FDBDatabase* db;

struct Future {
	FDBFuture* f = nullptr;
	Future() = default;
	explicit Future(FDBFuture* f) : f(f) {}
	~Future() {
		if (f)
			fdb_future_destroy(f);
	}
};

struct Transaction {
	FDBTransaction* tr = nullptr;
	Transaction() = default;
	explicit Transaction(FDBTransaction* tr) : tr(tr) {}
	~Transaction() {
		if (tr)
			fdb_transaction_destroy(tr);
	}
};

// TODO add more tests. The motivation for this test for now is to test the
// assembly code that handles emulating older api versions, but there's no
// reason why this shouldn't also test api version 510 specific behavior.

TEST_CASE("GRV") {
	Transaction tr;
	fdb_check(fdb_database_create_transaction(db, &tr.tr));
	Future grv{ fdb_transaction_get_read_version(tr.tr) };
	fdb_check(fdb_future_block_until_ready(grv.f));
}

int main(int argc, char** argv) {
	if (argc < 3) {
		std::cout << "Unit tests for the FoundationDB C API.\n"
		          << "Usage: " << argv[0] << " /path/to/cluster_file key_prefix [doctest args]" << std::endl;
		return 1;
	}
	fdb_check(fdb_select_api_version(FDB_API_VERSION));

	doctest::Context context;
	context.applyCommandLine(argc, argv);

	fdb_check(fdb_setup_network());
	std::thread network_thread{ &fdb_run_network };

	{
		FDBCluster* cluster;
		Future clusterFuture{ fdb_create_cluster(argv[1]) };
		fdb_check(fdb_future_block_until_ready(clusterFuture.f));
		fdb_check(fdb_future_get_cluster(clusterFuture.f, &cluster));
		Future databaseFuture{ fdb_cluster_create_database(cluster, (const uint8_t*)"DB", 2) };
		fdb_check(fdb_future_block_until_ready(databaseFuture.f));
		fdb_check(fdb_future_get_database(databaseFuture.f, &db));
		fdb_cluster_destroy(cluster);
	}

	clusterFilePath = std::string(argv[1]);
	prefix = argv[2];
	int res = context.run();
	fdb_database_destroy(db);

	if (context.shouldExit()) {
		fdb_check(fdb_stop_network());
		network_thread.join();
		return res;
	}
	fdb_check(fdb_stop_network());
	network_thread.join();

	return res;
}
