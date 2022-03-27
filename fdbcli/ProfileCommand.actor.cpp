/*
 * ProfileCommand.actor.cpp
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

#include "boost/lexical_cast.hpp"

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Tuple.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> profileCommandActor(Reference<ITransaction> tr, std::vector<StringRef> tokens, bool intrans) {
	state bool result = true;
	if (tokens.size() == 1) {
		printUsage(tokens[0]);
		result = false;
	} else if (tokencmp(tokens[1], "client")) {
		if (tokens.size() == 2) {
			fprintf(stderr, "ERROR: Usage: profile client <get|set>\n");
			return false;
		}
		wait(GlobalConfig::globalConfig().onInitialized());
		if (tokencmp(tokens[2], "get")) {
			if (tokens.size() != 3) {
				fprintf(stderr, "ERROR: Addtional arguments to `get` are not supported.\n");
				return false;
			}
			std::string sampleRateStr = "default";
			std::string sizeLimitStr = "default";
			const double sampleRateDbl = GlobalConfig::globalConfig().get<double>(
			    fdbClientInfoTxnSampleRate, std::numeric_limits<double>::infinity());
			if (!std::isinf(sampleRateDbl)) {
				sampleRateStr = std::to_string(sampleRateDbl);
			}
			const int64_t sizeLimit = GlobalConfig::globalConfig().get<int64_t>(fdbClientInfoTxnSizeLimit, -1);
			if (sizeLimit != -1) {
				sizeLimitStr = boost::lexical_cast<std::string>(sizeLimit);
			}
			printf("Client profiling rate is set to %s and size limit is set to %s.\n",
			       sampleRateStr.c_str(),
			       sizeLimitStr.c_str());
		} else if (tokencmp(tokens[2], "set")) {
			if (tokens.size() != 5) {
				fprintf(stderr, "ERROR: Usage: profile client set <RATE|default> <SIZE|default>\n");
				return false;
			}
			double sampleRate;
			if (tokencmp(tokens[3], "default")) {
				sampleRate = std::numeric_limits<double>::infinity();
			} else {
				char* end;
				sampleRate = std::strtod((const char*)tokens[3].begin(), &end);
				if (!std::isspace(*end)) {
					fprintf(stderr, "ERROR: %s failed to parse.\n", printable(tokens[3]).c_str());
					return false;
				}
			}
			int64_t sizeLimit;
			if (tokencmp(tokens[4], "default")) {
				sizeLimit = -1;
			} else {
				Optional<uint64_t> parsed = parse_with_suffix(tokens[4].toString());
				if (parsed.present()) {
					sizeLimit = parsed.get();
				} else {
					fprintf(stderr, "ERROR: `%s` failed to parse.\n", printable(tokens[4]).c_str());
					return false;
				}
			}

			Tuple rate = Tuple().appendDouble(sampleRate);
			Tuple size = Tuple().append(sizeLimit);
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tr->set(GlobalConfig::prefixedKey(fdbClientInfoTxnSampleRate), rate.pack());
			tr->set(GlobalConfig::prefixedKey(fdbClientInfoTxnSizeLimit), size.pack());
			if (!intrans) {
				wait(safeThreadFutureToFuture(tr->commit()));
			}
		} else {
			fprintf(stderr, "ERROR: Unknown action: %s\n", printable(tokens[2]).c_str());
			result = false;
		}
	} else if (tokencmp(tokens[1], "list")) {
		if (tokens.size() != 2) {
			fprintf(stderr, "ERROR: Usage: profile list\n");
			return false;
		}
		// Hold the reference to the standalone's memory
		state ThreadFuture<RangeResult> kvsFuture =
		    tr->getRange(KeyRangeRef(LiteralStringRef("\xff\xff/worker_interfaces/"),
		                             LiteralStringRef("\xff\xff/worker_interfaces0")),
		                 CLIENT_KNOBS->TOO_MANY);
		RangeResult kvs = wait(safeThreadFutureToFuture(kvsFuture));
		ASSERT(!kvs.more);
		for (const auto& pair : kvs) {
			auto ip_port =
			    (pair.key.endsWith(LiteralStringRef(":tls")) ? pair.key.removeSuffix(LiteralStringRef(":tls"))
			                                                 : pair.key)
			        .removePrefix(LiteralStringRef("\xff\xff/worker_interfaces/"));
			printf("%s\n", printable(ip_port).c_str());
		}
	} else {
		fprintf(stderr, "ERROR: Unknown type: %s\n", printable(tokens[1]).c_str());
		result = false;
	}
	return result;
}

CommandFactory profileFactory("profile",
                              CommandHelp("profile <client|list> <action> <ARGS>",
                                          "namespace for all the profiling-related commands.",
                                          "Different types support different actions.  Run `profile` to get a list of "
                                          "types, and iteratively explore the help.\n"));
} // namespace fdb_cli
