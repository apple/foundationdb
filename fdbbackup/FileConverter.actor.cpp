/*
 * FileConverter.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#include "fdbbackup/FileConverter.h"

#include <algorithm>
#include <iostream>
#include <cinttypes>
#include <cstdio>
#include <vector>

#include "fdbclient/BackupContainer.h"
#include "fdbrpc/simulator.h"
#include "flow/flow.h"

void printConvertUsage() {
	printf("\n");
	printf("  -r, --container Container URL.\n");
	printf("  -b, --begin BEGIN\n"
	       "                  Begin version.\n");
	printf("  -e, --end END   End version.\n");
	printf("  --log           Enables trace file logging for the CLI session.\n"
	       "  --logdir PATH   Specifes the output directory for trace files. If\n"
	       "                  unspecified, defaults to the current directory. Has\n"
	       "                  no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                  Sets the LogGroup field with the specified value for all\n"
	       "                  events in the trace output (defaults to `default').\n");
	printf("  --trace_format FORMAT\n"
	       "                  Select the format of the trace files. xml (the default) and json are supported.\n"
	       "                  Has no effect unless --log is specified.\n");
	printf("  -h, --help      Display this help and exit.\n");
	printf("\n");

	return;
}

std::vector<LogFile> getRelevantLogFiles(const std::vector<LogFile>& files, Version begin, Version end) {
	std::vector<LogFile> results;
	for (const auto& file : files) {
		if (file.beginVersion <= end && file.endVersion >= begin) {
			results.push_back(file);
		}
	}
	return results;
}

void printLogFiles(std::string msg, const std::vector<LogFile>& files) {
	std::cout<< msg << " " << files.size() << " log files\n";
	for (const auto& file : files) {
		std::cout<< file.toString() << "\n";
	}
	std::cout << std::endl;
}

struct ConvertParams {
	std::string container_url;
	Version begin, end;
	std::string log_dir;
	std::string trace_file;

	std::string toString() {
		std::string s;
		s.append("ContainerURL:");
		s.append(container_url);
		s.append(" Begin:");
		s.append(format("%" PRId64, begin));
		s.append(" End:");
		s.append(format("%" PRId64, end));
		return s;
	}
};

ACTOR Future<Void> test_container(ConvertParams params) {
	state Reference<IBackupContainer> container = IBackupContainer::openContainer(params.container_url);
	state BackupFileList listing = wait(container->dumpFileList());
	std::sort(listing.logs.begin(), listing.logs.end());
	printLogFiles("Container has", listing.logs);
	// state BackupDescription desc = wait(container->describeBackup());
	// std::cout << "\n" << desc.toString() << "\n";

	std::vector<LogFile> v1 = getRelevantLogFiles(listing.logs, params.begin, params.end);
	printLogFiles("Range has", v1);

	return Void();
}

int parseCommandLine(ConvertParams* param, CSimpleOpt* args) {
	while (args->Next()) {
		auto lastError = args->LastError();
		switch (lastError) {
			case SO_SUCCESS:
				break;

			default:
				fprintf(stderr, "ERROR: argument given for option `%s'\n", args->OptionText());
				return FDB_EXIT_ERROR;
				break;
		}

		int optId = args->OptionId();
		const char* arg = args->OptionArg();
		switch (optId) {
			case OPT_HELP:
				printConvertUsage();
				return FDB_EXIT_ERROR;

			case OPT_BEGIN_VERSION:
				if (!sscanf(arg, "%" SCNd64, &param->begin)) {
					std::cerr << "ERROR: could not parse begin version " << arg << "\n";
					printConvertUsage();
					return FDB_EXIT_ERROR;
				}
				break;

			case OPT_END_VERSION:
				if (!sscanf(arg, "%" SCNd64, &param->end)) {
					std::cerr << "ERROR: could not parse end version " << arg << "\n";
					printConvertUsage();
					return FDB_EXIT_ERROR;
				}
				break;

			case OPT_CONTAINER:
				param->container_url = args->OptionArg();
				break;

			case OPT_TRACE:
			case OPT_TRACE_DIR:
			case OPT_TRACE_FORMAT:
			case OPT_TRACE_LOG_GROUP:
				// TODO
				break;
		}
	}
	return FDB_EXIT_SUCCESS;
}

int main(int argc, char** argv) {
	try {
		if (argc < 3) {
			printConvertUsage();
			return FDB_EXIT_ERROR;
		}
		CSimpleOpt* args = new CSimpleOpt(argc, argv, gConverterOptions, SO_O_EXACT);
		ConvertParams param;
		int status = parseCommandLine(&param, args);
		std::cout << "Params: " << param.toString() << "\n";
		if (status != FDB_EXIT_SUCCESS) return status;

		platformInit();
		Error::init();

		StringRef url(param.container_url);
		if (url.startsWith(LiteralStringRef("file://"))) {
			// For container starts with "file://", create a simulated network.
			startNewSimulator();
		} else {
			setupNetwork(0, true);
		}

		TraceEvent::setNetworkThread();
		openTraceFile(NetworkAddress(), 10 << 20, 10 << 20, "", "convert");

		auto f = stopAfter(test_container(param));
		return status;
	}	catch (Error& e) {
		fprintf(stderr, "ERROR: %s\n", e.what());
		return FDB_EXIT_ERROR;
	} catch (std::exception& e) {
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		return FDB_EXIT_MAIN_EXCEPTION;
	}
}