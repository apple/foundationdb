/*
 * trace_partial_file_suffix_test.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>

#include "flow/Platform.h"

#define FDB_API_VERSION 620
#include "foundationdb/fdb_c.h"

#undef NDEBUG
#include <cassert>

void fdb_check(fdb_error_t e) {
	if (e) {
		std::cerr << fdb_get_error(e) << std::endl;
		std::abort();
	}
}

void set_net_opt(FDBNetworkOption option, const std::string& value) {
	fdb_check(fdb_network_set_option(option, reinterpret_cast<const uint8_t*>(value.c_str()), value.size()));
}

bool file_exists(const char* path) {
	FILE* f = fopen(path, "r");
	if (f) {
		fclose(f);
		return true;
	}
	return false;
}

int main(int argc, char** argv) {
	fdb_check(fdb_select_api_version(620));

	// We don't have the file_identifier network option in 6.2, so this test will be less robust.
	std::string file_identifier = "trace.127.0.0.1";
	std::string trace_partial_file_suffix = ".tmp";

	set_net_opt(FDBNetworkOption::FDB_NET_OPTION_TRACE_ENABLE, "");
	set_net_opt(FDBNetworkOption::FDB_NET_OPTION_TRACE_PARTIAL_FILE_SUFFIX, trace_partial_file_suffix);

	fdb_check(fdb_setup_network());
	std::thread network_thread{ &fdb_run_network };

	// Apparently you need to open a database to initialize logging
	FDBDatabase* out;
	fdb_check(fdb_create_database(nullptr, &out));
	fdb_database_destroy(out);

	// Eventually there's a new trace file for this test ending in .tmp
	std::string name;
	for (;;) {
		for (const auto& path : platform::listFiles(".")) {
			if (path.find(file_identifier) != std::string::npos) {
				assert(path.substr(path.size() - trace_partial_file_suffix.size()) == trace_partial_file_suffix);
				name = path;
				break;
			}
		}
		if (!name.empty()) {
			break;
		}
	}

	fdb_check(fdb_stop_network());
	network_thread.join();

	// After shutting down, the suffix is removed for both our new file
	if (!trace_partial_file_suffix.empty()) {
		assert(!file_exists(name.c_str()));
	}

	auto new_name = name.substr(0, name.size() - trace_partial_file_suffix.size());
	assert(file_exists(new_name.c_str()));
	remove(new_name.c_str());
	assert(!file_exists(new_name.c_str()));
}
