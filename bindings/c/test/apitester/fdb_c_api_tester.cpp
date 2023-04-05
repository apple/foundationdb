/*
 * fdb_c_api_tester.cpp
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

#include "TesterOptions.h"
#include "TesterWorkload.h"
#include "TesterScheduler.h"
#include "TesterTransactionExecutor.h"
#include "TesterTestSpec.h"
#include "TesterUtil.h"
#include "SimpleOpt/SimpleOpt.h"
#include "test/fdb_api.hpp"

#include <memory>
#include <stdexcept>
#include <thread>
#include <fmt/format.h>

namespace FdbApiTester {

namespace {

enum TesterOptionId {
	OPT_CONNFILE,
	OPT_HELP,
	OPT_TRACE,
	OPT_TRACE_DIR,
	OPT_LOGGROUP,
	OPT_TRACE_FORMAT,
	OPT_EXTERNAL_CLIENT_LIBRARY,
	OPT_EXTERNAL_CLIENT_DIRECTORY,
	OPT_FUTURE_VERSION_CLIENT_LIBRARY,
	OPT_TMP_DIR,
	OPT_DISABLE_LOCAL_CLIENT,
	OPT_TEST_FILE,
	OPT_INPUT_PIPE,
	OPT_OUTPUT_PIPE,
	OPT_FDB_API_VERSION,
	OPT_TRANSACTION_RETRY_LIMIT,
	OPT_BLOB_GRANULE_LOCAL_FILE_PATH,
	OPT_STATS_INTERVAL,
	OPT_TLS_CERT_FILE,
	OPT_TLS_KEY_FILE,
	OPT_TLS_CA_FILE,
	OPT_RETAIN_CLIENT_LIB_COPIES,
};

CSimpleOpt::SOption TesterOptionDefs[] = //
    { { OPT_CONNFILE, "-C", SO_REQ_SEP },
	  { OPT_CONNFILE, "--cluster-file", SO_REQ_SEP },
	  { OPT_TRACE, "--log", SO_NONE },
	  { OPT_TRACE_DIR, "--log-dir", SO_REQ_SEP },
	  { OPT_LOGGROUP, "--log-group", SO_REQ_SEP },
	  { OPT_HELP, "-h", SO_NONE },
	  { OPT_HELP, "--help", SO_NONE },
	  { OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	  { OPT_EXTERNAL_CLIENT_LIBRARY, "--external-client-library", SO_REQ_SEP },
	  { OPT_EXTERNAL_CLIENT_DIRECTORY, "--external-client-dir", SO_REQ_SEP },
	  { OPT_FUTURE_VERSION_CLIENT_LIBRARY, "--future-version-client-library", SO_REQ_SEP },
	  { OPT_TMP_DIR, "--tmp-dir", SO_REQ_SEP },
	  { OPT_DISABLE_LOCAL_CLIENT, "--disable-local-client", SO_NONE },
	  { OPT_TEST_FILE, "-f", SO_REQ_SEP },
	  { OPT_TEST_FILE, "--test-file", SO_REQ_SEP },
	  { OPT_INPUT_PIPE, "--input-pipe", SO_REQ_SEP },
	  { OPT_OUTPUT_PIPE, "--output-pipe", SO_REQ_SEP },
	  { OPT_FDB_API_VERSION, "--api-version", SO_REQ_SEP },
	  { OPT_TRANSACTION_RETRY_LIMIT, "--transaction-retry-limit", SO_REQ_SEP },
	  { OPT_BLOB_GRANULE_LOCAL_FILE_PATH, "--blob-granule-local-file-path", SO_REQ_SEP },
	  { OPT_STATS_INTERVAL, "--stats-interval", SO_REQ_SEP },
	  { OPT_TLS_CERT_FILE, "--tls-cert-file", SO_REQ_SEP },
	  { OPT_TLS_KEY_FILE, "--tls-key-file", SO_REQ_SEP },
	  { OPT_TLS_CA_FILE, "--tls-ca-file", SO_REQ_SEP },
	  { OPT_RETAIN_CLIENT_LIB_COPIES, "--retain-client-lib-copies", SO_NONE },
	  SO_END_OF_OPTIONS };

void printProgramUsage(const char* execName) {
	printf("usage: %s [OPTIONS]\n"
	       "\n",
	       execName);
	printf("  -C, --cluster-file FILE\n"
	       "                 The path of a file containing the connection string for the\n"
	       "                 FoundationDB cluster. The default is `fdb.cluster'\n"
	       "  --log          Enables trace file logging for the CLI session.\n"
	       "  --log-dir PATH Specifes the output directory for trace files. If\n"
	       "                 unspecified, defaults to the current directory. Has\n"
	       "                 no effect unless --log is specified.\n"
	       "  --log-group LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n"
	       "  --trace-format FORMAT\n"
	       "                 Select the format of the log files. xml (the default) and json\n"
	       "                 are supported. Has no effect unless --log is specified.\n"
	       "  --external-client-library FILE\n"
	       "                 Path to the external client library.\n"
	       "  --external-client-dir DIR\n"
	       "                 Directory containing external client libraries.\n"
	       "  --future-version-client-library FILE\n"
	       "                 Path to a client library to be used with a future protocol version.\n"
	       "  --tmp-dir DIR\n"
	       "                 Directory for temporary files of the client.\n"
	       "  --disable-local-client DIR\n"
	       "                 Disable the local client, i.e. use only external client libraries.\n"
	       "  --input-pipe NAME\n"
	       "                 Name of the input pipe for communication with the test controller.\n"
	       "  --output-pipe NAME\n"
	       "                 Name of the output pipe for communication with the test controller.\n"
	       "  --api-version VERSION\n"
	       "                 Required FDB API version (default %d).\n"
	       "  --transaction-retry-limit NUMBER\n"
	       "				 Maximum number of retries per tranaction (default: 0 - unlimited)\n"
	       "  --blob-granule-local-file-path PATH\n"
	       "				 Path to blob granule files on local filesystem\n"
	       "  -f, --test-file FILE\n"
	       "                 Test file to run.\n"
	       "  --stats-interval MILLISECONDS\n"
	       "                 Time interval in milliseconds for printing workload statistics (default: 0 - disabled).\n"
	       "  --tls-cert-file FILE\n"
	       "                 Path to file containing client's TLS certificate chain\n"
	       "  --tls-key-file FILE\n"
	       "                 Path to file containing client's TLS private key\n"
	       "  --tls-ca-file FILE\n"
	       "                 Path to file containing TLS CA certificate\n"
	       "  --retain-client-lib-copies\n"
	       "                 Retain temporary external client library copies\n"
	       "  -h, --help     Display this help and exit.\n",
	       FDB_API_VERSION);
}

bool validateTraceFormat(std::string_view format) {
	return format == "xml" || format == "json";
}

const int MIN_TESTABLE_API_VERSION = 400;

void processIntOption(const std::string& optionName, const std::string& value, int minValue, int maxValue, int& res) {
	char* endptr;
	res = strtol(value.c_str(), &endptr, 10);
	if (*endptr != '\0') {
		throw TesterError(fmt::format("Invalid value {} for {}", value, optionName));
	}
	if (res < minValue || res > maxValue) {
		throw TesterError(fmt::format("Value for {} must be between {} and {}", optionName, minValue, maxValue));
	}
}

bool processArg(TesterOptions& options, const CSimpleOpt& args) {
	switch (args.OptionId()) {
	case OPT_CONNFILE:
		options.clusterFile = args.OptionArg();
		break;
	case OPT_TRACE:
		options.trace = true;
		break;
	case OPT_TRACE_DIR:
		options.traceDir = args.OptionArg();
		break;
	case OPT_LOGGROUP:
		options.logGroup = args.OptionArg();
		break;
	case OPT_TRACE_FORMAT:
		if (!validateTraceFormat(args.OptionArg())) {
			fmt::print(stderr, "ERROR: Unrecognized trace format `{}'\n", args.OptionArg());
			return false;
		}
		options.traceFormat = args.OptionArg();
		break;
	case OPT_EXTERNAL_CLIENT_LIBRARY:
		options.externalClientLibrary = args.OptionArg();
		break;
	case OPT_EXTERNAL_CLIENT_DIRECTORY:
		options.externalClientDir = args.OptionArg();
		break;
	case OPT_FUTURE_VERSION_CLIENT_LIBRARY:
		options.futureVersionClientLibrary = args.OptionArg();
		break;
	case OPT_TMP_DIR:
		options.tmpDir = args.OptionArg();
		break;
	case OPT_DISABLE_LOCAL_CLIENT:
		options.disableLocalClient = true;
		break;
	case OPT_TEST_FILE:
		options.testFile = args.OptionArg();
		options.testSpec = readTomlTestSpec(options.testFile);
		break;
	case OPT_INPUT_PIPE:
		options.inputPipeName = args.OptionArg();
		break;
	case OPT_OUTPUT_PIPE:
		options.outputPipeName = args.OptionArg();
		break;
	case OPT_FDB_API_VERSION:
		processIntOption(
		    args.OptionText(), args.OptionArg(), MIN_TESTABLE_API_VERSION, FDB_API_VERSION, options.apiVersion);
		break;
	case OPT_TRANSACTION_RETRY_LIMIT:
		processIntOption(args.OptionText(), args.OptionArg(), 0, 1000, options.transactionRetryLimit);
		break;
	case OPT_BLOB_GRANULE_LOCAL_FILE_PATH:
		options.bgBasePath = args.OptionArg();
		break;
	case OPT_STATS_INTERVAL:
		processIntOption(args.OptionText(), args.OptionArg(), 0, 60000, options.statsIntervalMs);
		break;
	case OPT_TLS_CERT_FILE:
		options.tlsCertFile.assign(args.OptionArg());
		break;
	case OPT_TLS_KEY_FILE:
		options.tlsKeyFile.assign(args.OptionArg());
		break;
	case OPT_TLS_CA_FILE:
		options.tlsCaFile.assign(args.OptionArg());
		break;
	case OPT_RETAIN_CLIENT_LIB_COPIES:
		options.retainClientLibCopies = true;
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
	if (!options.tmpDir.empty() && options.apiVersion >= FDB_API_VERSION_CLIENT_TMP_DIR) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_CLIENT_TMP_DIR, options.tmpDir);
	}
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
			throw TesterError("Invalid options: Cannot disable local client if no external library is provided");
		}
	}

	if (!options.futureVersionClientLibrary.empty()) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_FUTURE_VERSION_CLIENT_LIBRARY,
		                        options.futureVersionClientLibrary);
	}

	if (options.testSpec.multiThreaded) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION, options.numFdbThreads);
	}

	if (options.testSpec.fdbCallbacksOnExternalThreads) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_CALLBACKS_ON_EXTERNAL_THREADS);
	}

	if (options.testSpec.buggify) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_CLIENT_BUGGIFY_ENABLE);
	}

	if (options.testSpec.disableClientBypass && options.apiVersion >= FDB_API_VERSION_DISABLE_CLIENT_BYPASS) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_DISABLE_CLIENT_BYPASS);
	}

	if (options.testSpec.runLoopProfiler) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_ENABLE_RUN_LOOP_PROFILING);
	}

	if (options.trace) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_TRACE_ENABLE, options.traceDir);
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_TRACE_FORMAT, options.traceFormat);
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_TRACE_LOG_GROUP, options.logGroup);
	}

	if (!options.tlsCertFile.empty()) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_TLS_CERT_PATH, options.tlsCertFile);
	}

	if (!options.tlsKeyFile.empty()) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_TLS_KEY_PATH, options.tlsKeyFile);
	}

	if (!options.tlsCaFile.empty()) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_TLS_CA_PATH, options.tlsCaFile);
	}

	if (options.retainClientLibCopies) {
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_RETAIN_CLIENT_LIBRARY_COPIES);
	}

	for (auto knob : options.testSpec.knobs) {
		fmt::print(stderr, "Setting knob {}={}\n", knob.first.c_str(), knob.second.c_str());
		fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_KNOB,
		                        fmt::format("{}={}", knob.first.c_str(), knob.second.c_str()));
	}
}

void randomizeOptions(TesterOptions& options) {
	Random& random = Random::get();
	options.numFdbThreads = random.randomInt(options.testSpec.minFdbThreads, options.testSpec.maxFdbThreads);
	options.numClientThreads = random.randomInt(options.testSpec.minClientThreads, options.testSpec.maxClientThreads);
	options.numDatabases = random.randomInt(options.testSpec.minDatabases, options.testSpec.maxDatabases);
	options.numClients = random.randomInt(options.testSpec.minClients, options.testSpec.maxClients);

	// Choose a random number of tenants. If a test is configured to allow 0 tenants, then use 0 tenants half the time.
	if (options.testSpec.maxTenants >= options.testSpec.minTenants &&
	    (options.testSpec.minTenants > 0 || random.randomBool(0.5))) {
		options.numTenants = random.randomInt(options.testSpec.minTenants, options.testSpec.maxTenants);
	}
}

bool runWorkloads(TesterOptions& options) {
	try {
		TransactionExecutorOptions txExecOptions;
		txExecOptions.blockOnFutures = options.testSpec.blockOnFutures;
		txExecOptions.numDatabases = options.numDatabases;
		txExecOptions.databasePerTransaction = options.testSpec.databasePerTransaction;
		// 7.1 and older releases crash on database create errors
		txExecOptions.injectDatabaseCreateErrors = options.testSpec.buggify && options.apiVersion > 710;
		txExecOptions.transactionRetryLimit = options.transactionRetryLimit;
		txExecOptions.tmpDir = options.tmpDir.empty() ? std::string("/tmp") : options.tmpDir;
		txExecOptions.tamperClusterFile = options.testSpec.tamperClusterFile;
		txExecOptions.numTenants = options.numTenants;

		std::vector<std::shared_ptr<IWorkload>> workloads;
		workloads.reserve(options.testSpec.workloads.size() * options.numClients);
		int maxSelfBlockingFutures = 0;
		for (const auto& workloadSpec : options.testSpec.workloads) {
			for (int i = 0; i < options.numClients; i++) {
				WorkloadConfig config;
				config.name = workloadSpec.name;
				config.options = workloadSpec.options;
				config.clientId = i;
				config.numClients = options.numClients;
				config.numTenants = options.numTenants;
				config.apiVersion = options.apiVersion;
				std::shared_ptr<IWorkload> workload = IWorkloadFactory::create(workloadSpec.name, config);
				if (!workload) {
					throw TesterError(fmt::format("Unknown workload '{}'", workloadSpec.name));
				}
				maxSelfBlockingFutures += workload->getMaxSelfBlockingFutures();
				workloads.emplace_back(std::move(workload));
			}
		}

		int numClientThreads = options.numClientThreads;
		if (txExecOptions.blockOnFutures) {
			// If futures block a thread, we must ensure that we have enough threads for self-blocking futures to avoid
			// a deadlock.
			int minClientThreads = maxSelfBlockingFutures + 1;
			if (numClientThreads < minClientThreads) {
				fmt::print(
				    stderr, "WARNING: Adjusting minClientThreads from {} to {}\n", numClientThreads, minClientThreads);
				numClientThreads = minClientThreads;
			}
		}

		std::unique_ptr<IScheduler> scheduler = createScheduler(numClientThreads);
		std::unique_ptr<ITransactionExecutor> txExecutor = createTransactionExecutor(txExecOptions);
		txExecutor->init(scheduler.get(), options.clusterFile.c_str(), options.bgBasePath);

		WorkloadManager workloadMgr(txExecutor.get(), scheduler.get());
		for (const auto& workload : workloads) {
			workloadMgr.add(workload);
		}

		if (!options.inputPipeName.empty() || !options.outputPipeName.empty()) {
			workloadMgr.openControlPipes(options.inputPipeName, options.outputPipeName);
		}

		scheduler->start();
		if (options.statsIntervalMs) {
			workloadMgr.schedulePrintStatistics(options.statsIntervalMs);
		}
		workloadMgr.run();
		return !workloadMgr.failed();
	} catch (const std::exception& err) {
		fmt::print(stderr, "ERROR: {}\n", err.what());
		return false;
	}
}

} // namespace
} // namespace FdbApiTester

using namespace FdbApiTester;

int main(int argc, char** argv) {
	int retCode = 0;
	try {
		TesterOptions options;
		if (!parseArgs(options, argc, argv)) {
			return 1;
		}
		randomizeOptions(options);

		fdb::selectApiVersionCapped(options.apiVersion);
		applyNetworkOptions(options);
		fdb::network::setup();

		std::thread network_thread{ [] { fdb_check(fdb::network::run(), "FDB network thread failed"); } };

		if (!runWorkloads(options)) {
			retCode = 1;
		}

		fprintf(stderr, "Stopping FDB network thread\n");
		fdb_check(fdb::network::stop(), "Failed to stop FDB thread");
		network_thread.join();
		fprintf(stderr, "FDB network thread successfully stopped\n");
	} catch (const std::exception& err) {
		fmt::print(stderr, "ERROR: {}\n", err.what());
		retCode = 1;
	}
	return retCode;
}
