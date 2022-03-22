/*
 * setup_tests.cpp
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

// Unit tests for API setup, network initialization functions from the FDB C API.

#define FDB_API_VERSION 710
#include <foundationdb/fdb_c.h>
#include <iostream>
#include <thread>

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

void fdb_check(fdb_error_t e) {
	if (e) {
		std::cerr << fdb_get_error(e) << std::endl;
		std::abort();
	}
}

TEST_CASE("setup") {
	fdb_error_t err;
	// Version passed here must be <= FDB_API_VERSION
	err = fdb_select_api_version(9000);
	CHECK(err);

	// Select current API version
	fdb_check(fdb_select_api_version(710));

	// Error to call again after a successful return
	err = fdb_select_api_version(710);
	CHECK(err);

	CHECK(fdb_get_max_api_version() >= 710);

	fdb_check(fdb_setup_network());
	// Calling a second time should fail
	err = fdb_setup_network();
	CHECK(err);

	struct Context {
		bool called = false;
	};
	Context context;
	fdb_check(fdb_add_network_thread_completion_hook(
	    [](void* param) {
		    auto* context = static_cast<Context*>(param);
		    context->called = true;
	    },
	    &context));

	std::thread network_thread{ &fdb_run_network };

	CHECK(!context.called);
	fdb_check(fdb_stop_network());
	network_thread.join();
	CHECK(context.called);
}
