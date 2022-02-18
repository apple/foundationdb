/*
 * TesterOptions.h
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

#pragma once

#ifndef SYSTEM_TESTER_TESTER_OPTIONS_H
#define SYSTEM_TESTER_TESTER_OPTIONS_H

#include "flow/SimpleOpt.h"
#include <string>
#include <vector>

#define FDB_API_VERSION 710
#include "bindings/c/foundationdb/fdb_c.h"

namespace FDBSystemTester {

class TesterOptions {
public:
	enum {
		OPT_CONNFILE,
		OPT_HELP,
		OPT_TRACE,
		OPT_TRACE_DIR,
		OPT_LOGGROUP,
		OPT_TRACE_FORMAT,
		OPT_KNOB,
		OPT_API_VERSION,
	};
	static const CSimpleOpt::SOption optionDefs[];

	std::string clusterFile;
	bool trace = false;
	std::string traceDir;
	std::string traceFormat;
	std::string logGroup;
	bool initialStatusCheck = true;
	bool cliHints = true;
	std::vector<std::pair<std::string, std::string>> knobs;
	// api version, using the latest version by default
	int api_version = FDB_API_VERSION;

	bool parseArgs(int argc, char** argv);

private:
	bool processArg(const CSimpleOpt& args);
	static void printProgramUsage(const char* execName);
};

} // namespace FDBSystemTester

#endif