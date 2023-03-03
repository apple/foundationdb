/*
 * shim_lib_tester.cpp
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

/*
 * A utility for testing shim library usage with various valid and invalid configurations
 */

#include "fmt/core.h"
#include "test/fdb_api.hpp"
#include "SimpleOpt/SimpleOpt.h"
#include <thread>
#include <string_view>
#include "foundationdb/fdb_c_shim.h"

#undef ERROR
#define ERROR(name, number, description) enum { error_code_##name = number };

#include "flow/error_definitions.h"

using namespace std::string_view_literals;

namespace {

enum TesterOptionId {
	OPT_HELP,
	OPT_CONNFILE,
	OPT_LOCAL_CLIENT_LIBRARY,
	OPT_EXTERNAL_CLIENT_LIBRARY,
	OPT_EXTERNAL_CLIENT_DIRECTORY,
	OPT_DISABLE_LOCAL_CLIENT,
	OPT_API_VERSION
};

const int MIN_TESTABLE_API_VERSION = 400;

CSimpleOpt::SOption TesterOptionDefs[] = //
    { { OPT_HELP, "-h", SO_NONE },
	  { OPT_HELP, "--help", SO_NONE },
	  { OPT_CONNFILE, "-C", SO_REQ_SEP },
	  { OPT_CONNFILE, "--cluster-file", SO_REQ_SEP },
	  { OPT_LOCAL_CLIENT_LIBRARY, "--local-client-library", SO_REQ_SEP },
	  { OPT_EXTERNAL_CLIENT_LIBRARY, "--external-client-library", SO_REQ_SEP },
	  { OPT_EXTERNAL_CLIENT_DIRECTORY, "--external-client-dir", SO_REQ_SEP },
	  { OPT_DISABLE_LOCAL_CLIENT, "--disable-local-client", SO_NONE },
	  { OPT_API_VERSION, "--api-version", SO_REQ_SEP },
	  SO_END_OF_OPTIONS };

class TesterOptions {
public:
	// FDB API version, using the latest version by default
	int apiVersion = FDB_API_VERSION;
	std::string clusterFile;
	std::string localClientLibrary;
	std::string externalClientLibrary;
	std::string externalClientDir;
	bool disableLocalClient = false;
};

void printProgramUsage(const char* execName) {
	printf("usage: %s [OPTIONS]\n"
	       "\n",
	       execName);
	printf("  -C, --cluster-file FILE\n"
	       "                 The path of a file containing the connection string for the\n"
	       "                 FoundationDB cluster. The default is `fdb.cluster'\n"
	       "  --local-client-library FILE\n"
	       "                 Path to the local client library.\n"
	       "  --external-client-library FILE\n"
	       "                 Path to the external client library.\n"
	       "  --external-client-dir DIR\n"
	       "                 Directory containing external client libraries.\n"
	       "  --disable-local-client DIR\n"
	       "                 Disable the local client, i.e. use only external client libraries.\n"
	       "  --api-version VERSION\n"
	       "                 Required FDB API version (default %d).\n"
	       "  -h, --help     Display this help and exit.\n",
	       FDB_API_VERSION);
}

bool processIntOption(const std::string& optionName, const std::string& value, int minValue, int maxValue, int& res) {
	char* endptr;
	res = strtol(value.c_str(), &endptr, 10);
	if (*endptr != '\0') {
		fmt::print(stderr, "Invalid value {} for {}", value, optionName);
		return false;
	}
	if (res < minValue || res > maxValue) {
		fmt::print(stderr, "Value for {} must be between {} and {}", optionName, minValue, maxValue);
		return false;
	}
	return true;
}

bool processArg(TesterOptions& options, const CSimpleOpt& args) {
	switch (args.OptionId()) {
	case OPT_CONNFILE:
		options.clusterFile = args.OptionArg();
		break;
	case OPT_LOCAL_CLIENT_LIBRARY:
		options.localClientLibrary = args.OptionArg();
		break;
	case OPT_EXTERNAL_CLIENT_LIBRARY:
		options.externalClientLibrary = args.OptionArg();
		break;
	case OPT_EXTERNAL_CLIENT_DIRECTORY:
		options.externalClientDir = args.OptionArg();
		break;
	case OPT_DISABLE_LOCAL_CLIENT:
		options.disableLocalClient = true;
		break;
	case OPT_API_VERSION:
		if (!processIntOption(
		        args.OptionText(), args.OptionArg(), MIN_TESTABLE_API_VERSION, FDB_API_VERSION, options.apiVersion)) {
			return false;
		}
		break;
	}
	return true;
}

bool parseArgs(TesterOptions& options, int argc, char** argv) {
	// declare our options parser, pass in the arguments from main
	// as well as our array of valid options.
	CSimpleOpt args(argc, argv, TesterOptionDefs);

	// while there are arguments left to process
	while (args.Next()) {
		if (args.LastError() == SO_SUCCESS) {
			if (args.OptionId() == OPT_HELP) {
				printProgramUsage(argv[0]);
				return false;
			}
			if (!processArg(options, args)) {
				return false;
			}
		} else {
			fmt::print(stderr, "ERROR: Invalid argument: {}\n", args.OptionText());
			printProgramUsage(argv[0]);
			return false;
		}
	}
	return true;
}

void fdb_check(fdb::Error e, std::string_view msg, fdb::Error::CodeType expectedError = error_code_success) {
	if (e.code()) {
		fmt::print(stderr, "{}, Error: {}({})\n", msg, e.code(), e.what());
		std::abort();
	}
}

void applyNetworkOptions(TesterOptions& options) {
	if (!options.externalClientLibrary.empty()) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_DISABLE_LOCAL_CLIENT);
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY,
		                        options.externalClientLibrary);
	} else if (!options.externalClientDir.empty()) {
		if (options.disableLocalClient) {
			fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_DISABLE_LOCAL_CLIENT);
		}
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_EXTERNAL_CLIENT_DIRECTORY, options.externalClientDir);
	} else {
		if (options.disableLocalClient) {
			fmt::print(stderr, "Invalid options: Cannot disable local client if no external library is provided");
			exit(1);
		}
	}
}

void testBasicApi(const TesterOptions& options) {
	fdb::Database db(options.clusterFile);
	fdb::Transaction tx = db.createTransaction();
	while (true) {
		try {
			// Set a time out to avoid long delays when testing invalid configurations
			tx.setOption(FDB_TR_OPTION_TIMEOUT, 1000);
			tx.set(fdb::toBytesRef("key1"sv), fdb::toBytesRef("val1"sv));
			fdb_check(tx.commit().blockUntilReady(), "Wait on commit failed");
			break;
		} catch (const fdb::Error& err) {
			if (err.code() == error_code_timed_out) {
				exit(1);
			}
			auto onErrorFuture = tx.onError(err);
			fdb_check(onErrorFuture.blockUntilReady(), "Wait on onError failed");
			fdb_check(onErrorFuture.error(), "onError failed");
		}
	}
}

void test710Api(const TesterOptions& options) {
	fdb::Database db(options.clusterFile);
	try {
		db.openTenant(fdb::toBytesRef("not_existing_tenant"sv));
	} catch (const fdb::Error& err) {
		fdb_check(err, "Tenant not found expected", error_code_tenant_not_found);
	}
}

} // namespace

int main(int argc, char** argv) {
	int retCode = 0;
	try {
		TesterOptions options;
		if (!parseArgs(options, argc, argv)) {
			return 1;
		}

		if (!options.localClientLibrary.empty()) {
			// Must be called before the first FDB API call
			fdb_shim_set_local_client_library_path(options.localClientLibrary.c_str());
		}

		fdb::selectApiVersionCapped(options.apiVersion);
		applyNetworkOptions(options);
		fdb::network::setup();

		std::thread network_thread{ [] { fdb_check(fdb::network::run(), "FDB network thread failed"); } };

		// Try calling some basic functionality that is available
		// in all recent API versions
		testBasicApi(options);

		// Try calling 710-specific API. This enables testing what
		// happens if a library is missing a function
		test710Api(options);

		fdb_check(fdb::network::stop(), "Stop network failed");
		network_thread.join();
	} catch (const std::runtime_error& err) {
		fmt::print(stderr, "runtime error caught: {}\n", err.what());
		retCode = 1;
	}
	return retCode;
}
