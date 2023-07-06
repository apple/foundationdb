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

#define FDB_USE_LATEST_API_VERSION
#include <thread>
#include <iostream>
#include <vector>

#include "test/fdb_api.hpp"

void fdbCheck(const fdb::Error& err) {
	if (err) {
		std::cerr << err.what() << std::endl;
		std::abort();
	}
}

int main(int argc, char** argv) {
	if (argc != 2) {
		printf("Usage: %s <cluster_file>", argv[0]);
	}
	fdb::selectApiVersion(FDB_API_VERSION);
	fdb::network::setup();
	std::thread network_thread{ [] { fdbCheck(fdb::network::run()); } };

	fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_TRACE_ENABLE);
	std::string traceFormat("json");
	fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_TRACE_FORMAT, traceFormat);

	// Use a bunch of memory from different client threads
	auto db = fdb::Database(argv[1]);
	auto thread_func = [&]() {
		auto tr = db.createTransaction();
		for (int i = 0; i < 10000; ++i) {
			tr.set(fdb::toBytesRef(std::to_string(i)), fdb::toBytesRef(std::string(i, '\x00')));
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
	// Force the database reference to be destroyed
	db = fdb::Database();

	// Memory usage should go down now if the allocator is returning memory to the OS. It's expected that something is
	// externally monitoring the memory usage of this process during this sleep.
	using namespace std::chrono_literals;
	std::this_thread::sleep_for(10s);

	fdbCheck(fdb::network::stop());
	network_thread.join();
}
