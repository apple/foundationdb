/*
 * client_memory_test.cpp
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

#define FDB_API_VERSION 710
#include <foundationdb/fdb_c.h>

#include "unit/fdb_api.hpp"

#include <thread>
#include <iostream>
#include <vector>

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

int main(int argc, char** argv) {
	if (argc != 2) {
		printf("Usage: %s <cluster_file>", argv[0]);
	}
	fdb_check(fdb_select_api_version(710));
	fdb_check(fdb_setup_network());
	std::thread network_thread{ &fdb_run_network };

	fdb_check(
	    fdb_network_set_option(FDBNetworkOption::FDB_NET_OPTION_TRACE_ENABLE, reinterpret_cast<const uint8_t*>(""), 0));
	fdb_check(fdb_network_set_option(
	    FDBNetworkOption::FDB_NET_OPTION_TRACE_FORMAT, reinterpret_cast<const uint8_t*>("json"), 4));

	// Use a bunch of memory from different client threads
	FDBDatabase* db = fdb_open_database(argv[1]);
	auto thread_func = [&]() {
		fdb::Transaction tr(db);
		for (int i = 0; i < 10000; ++i) {
			tr.set(std::to_string(i), std::string(i, '\x00'));
		}
		tr.cancel();
	};
	std::vector<std::thread> threads;
	constexpr auto kThreadCount = 64;
	for (int i = 0; i < kThreadCount; ++i) {
		threads.emplace_back(thread_func);
	}
	for (auto& thread : threads) {
		thread.join();
	}
	fdb_database_destroy(db);
	db = nullptr;

	// Memory usage should go down now if the allocator is returning memory to the OS. It's expected that something is
	// externally monitoring the memory usage of this process during this sleep.
	using namespace std::chrono_literals;
	std::this_thread::sleep_for(10s);

	fdb_check(fdb_stop_network());
	network_thread.join();
}