/*
 * fdb_checksum_tool.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "flow/flow.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/Trace.h"
#include "flow/Platform.h"
#include "flow/IThreadPool.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "flow/TLSConfig.actor.h"
#include "flow/xxhash.h"
#include "fdbclient/ChecksumDatabase.actor.h" // This now brings in actorcompiler.h for its own actors

// Include C API here
#include "foundationdb/fdb_c.h"

#include <fmt/format.h>
#include <thread>

#include <iostream>
#include <string>
#include <vector>
#include <stdexcept>
#include <boost/program_options.hpp>

#include "fdbclient/Tuple.h"

#include "fdbclient/ThreadSafeTransaction.h"
#include "flow/ApiVersion.h"
#include "flow/ArgParseUtil.h"
#include "flow/DeterministicRandom.h"

#include "fdbclient/MultiVersionTransaction.h" // For MultiVersionApi access

#define API ((IClientApi*)MultiVersionApi::api) // Define API like in fdbcli

namespace po = boost::program_options;

// Include actorcompiler.h here, before local actor definitions
#include "flow/actorcompiler.h"

// Forward declare the fdbclient actor we will call *inside* the wrapper
namespace fdb {
ACTOR Future<fdb::ChecksumResult> calculateDatabaseChecksum(Database cx, Optional<KeyRange> range);
}

// Helper actor to ensure network is stopped
ACTOR template <class T>
Future<T> stopNetworkAfter_actor(Future<T> what) {
	state T result;
	try {
		T t = wait(what);
		result = t;
	} catch (...) {
		API->stopNetwork(); // Use API to stop network
		throw;
	}
	API->stopNetwork(); // Use API to stop network
	return result;
}

// Helper function to unescape keys (from previous work)
// Very close to  NativeAPI::unprintable. TODO: Unify them.
std::string unescapeKeyString(const std::string& s) {
	std::string result_str; // Renamed to avoid conflict with actor state 'result'
	result_str.reserve(s.length());
	for (size_t i = 0; i < s.length(); ++i) {
		if (s[i] == '\\') {
			if (i + 1 < s.length()) {
				char next = s[i + 1];
				if (next == 'x') {
					if (i + 3 < s.length()) {
						try {
							std::string hex = s.substr(i + 2, 2);
							char val = static_cast<char>(std::stoi(hex, nullptr, 16));
							result_str.push_back(val);
							i += 3;
						} catch (const std::exception& e) {
							// Invalid hex sequence, treat as literal '\\x'
							result_str.push_back(s[i]);
							result_str.push_back(next);
							i += 1;
						}
					} else {
						// Incomplete hex, treat as literal '\\x'
						result_str.push_back(s[i]);
						result_str.push_back(next);
						i += 1;
					}
				} else if (next == '\\') {
					result_str.push_back('\\');
					i += 1;
				} else {
					// Unknown escape, treat as literal backslash
					result_str.push_back(s[i]);
				}
			} else {
				// Trailing backslash
				result_str.push_back(s[i]);
			}
		} else {
			result_str.push_back(s[i]);
		}
	}
	return result_str;
}

// Helper function to format bytes for display
std::string formatBytes(uint64_t bytes) {
	const char* units[] = { "B", "KB", "MB", "GB", "TB" };
	int unit = 0;
	double size = static_cast<double>(bytes);

	while (size >= 1024.0 && unit < 4) {
		size /= 1024.0;
		unit++;
	}

	return fmt::format("{:.2f} {}", size, units[unit]);
}

// Main tool actor - Modified to create Database inside
ACTOR Future<Void> checksumToolActor(Reference<ClusterConnectionFile> ccf,
                                     int apiVersion,
                                     Optional<Standalone<StringRef>> beginKeyOpt,
                                     Optional<Standalone<StringRef>> endKeyOpt) {
	state Database db;
	state Optional<KeyRange> keyRange;
	state double startTime;
	state fdb::ChecksumResult result;

	try {
		db = Database::createDatabase(ccf, apiVersion, IsInternal::False, LocalityData());

		if (endKeyOpt.present()) {
			TraceEvent(SevInfo, "ChecksumToolEndKeyOptDebug")
			    .detail("EndKeyOptPrintable", endKeyOpt.get().printable())
			    .detail("EndKeyOptSize", endKeyOpt.get().size());
		}

		if (beginKeyOpt.present() || endKeyOpt.present()) {
			Key b = beginKeyOpt.present() ? beginKeyOpt.get() : KeyRef();
			Key e = endKeyOpt.present() ? endKeyOpt.get() : "\\xff\\xff"_sr;
			keyRange = KeyRangeRef(b, e);
		}

		printf("Attempting to get read version (timeout 10s)...\n");
		state ReadYourWritesTransaction trTest(db);
		try {
			Version v = wait(trTest.getReadVersion());
			printf("Successfully connected. Read version: %lld\n", v);
		} catch (Error& e) {
			printf("ERROR: Could not connect to database or get read version: %s (%d)\n", e.what(), e.code());
			throw;
		}

		printf("Begin key: %s\n", keyRange.present() ? keyRange.get().begin.printable().c_str() : "");
		printf("End key:   %s\n\n", keyRange.present() ? keyRange.get().end.printable().c_str() : "\\\\xff");

		printf("\nStarting checksum calculation...\n");
		startTime = timer_monotonic();

		state fdb::ChecksumResult checksum_actor_result = wait(fdb::calculateDatabaseChecksum(db, keyRange));
		result = checksum_actor_result;

		double duration = timer_monotonic() - startTime;
		printf("\nChecksum calculation complete.\n");
		printf("-------------------------------------\n");
		printf("FoundationDB Database Checksum Result\n");
		printf("-------------------------------------\n");
		printf("Checksum:         0x%016llx\n", result.checksum);
		printf("Total Keys:       %lld\n", result.totalKeys);
		printf("Total Bytes:      %s (%lld B)\n", formatBytes(result.totalBytes).c_str(), result.totalBytes);
		printf("Time Taken:       %.2f seconds\n", duration);
		printf("-------------------------------------\n");

	} catch (Error& e) {
		throw;
	}

	return Void();
}

// Corrected runChecksumTool to take ccf and create Database inside
ACTOR Future<Void> runChecksumTool(Reference<ClusterConnectionFile> ccf,
                                   int apiVersion,
                                   std::string beginKeyStr,
                                   std::string endKeyStr) {
	// Database creation moved to checksumToolActor

	Optional<Standalone<StringRef>> beginKeyOpt;
	if (!beginKeyStr.empty()) {
		beginKeyOpt = StringRef((const uint8_t*)beginKeyStr.c_str(), beginKeyStr.size());
	}
	Optional<Standalone<StringRef>> endKeyOpt;
	if (!endKeyStr.empty()) {
		endKeyOpt = StringRef((const uint8_t*)endKeyStr.c_str(), endKeyStr.size());
	}

	try {
		// Pass ccf and apiVersion to checksumToolActor
		wait(checksumToolActor(ccf, apiVersion, beginKeyOpt, endKeyOpt));
	} catch (Error& e) {
		// This will now catch any error thrown by checksumToolActor
		TraceEvent(SevError, "RunChecksumToolError").error(e); // Keep tracing
		fprintf(stderr, "Error in runChecksumTool: %s (%d)\n", e.what(), e.code()); // Print for clarity
		throw; // Rethrow to be caught by main
	}
	return Void();
}

int main(int argc, char** argv) {
	platformInit(); // Add platformInit() like in fdbcli
	try {
		// --- Stage 1: Parse command line options ---
		std::string clusterFile;
		std::string beginKeyStr = "";
		std::string endKeyStr = "\xff";
		std::string tlsCertPath, tlsKeyPath, tlsCaPath;
		std::string tlsVerifyPeers;
		int parsedApiVersion = FDB_API_VERSION; // Use a different name to avoid conflict with fdb_c.h FDB_API_VERSION
		bool logTrace = false;
		std::string logDir;
		std::string logGroup;
		std::vector<std::string> knobs;
		bool printVersion = false;

		po::options_description desc("fdb_checksum_tool options");
		desc.add_options()("help,h", "Print help message")(
		    "version,v", po::bool_switch(&printVersion), "Print version information and exit")(
		    "cluster-file,C",
		    po::value<std::string>(&clusterFile)->default_value(""),
		    "FoundationDB cluster file (defaults to system default)")(
		    "api-version", po::value<int>(&parsedApiVersion)->default_value(FDB_API_VERSION), "API version to use")(
		    "begin,b", po::value<std::string>(&beginKeyStr), "Begin key (escaped string)")(
		    "end,e", po::value<std::string>(&endKeyStr), "End key (escaped string, defaults to \\\\xff)")(
		    "log", po::bool_switch(&logTrace), "Enable trace logging")(
		    "log-dir", po::value<std::string>(&logDir), "Directory for trace logs")(
		    "log-group", po::value<std::string>(&logGroup), "Trace log group name")(
		    "knob",
		    po::value<std::vector<std::string>>(&knobs)->composing(),
		    "Set a knob (e.g., knob_name=knob_value)")(
		    "tls_certificate_file", po::value<std::string>(&tlsCertPath)->default_value(""), "TLS certificate file")(
		    "tls_key_file", po::value<std::string>(&tlsKeyPath)->default_value(""), "TLS key file")(
		    "tls_ca_file", po::value<std::string>(&tlsCaPath)->default_value(""), "TLS CA certificate bundle file")(
		    "tls_verify_peers",
		    po::value<std::string>(&tlsVerifyPeers)->default_value(""),
		    "TLS peer verification rules");

		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, desc), vm);
		po::notify(vm);

		if (vm.count("help")) {
			std::cout << desc << std::endl;
			return 0;
		}

		if (printVersion) {
			std::cout << "FDB API Version compiled: " << FDB_API_VERSION << std::endl;
			return 0;
		}

		// Apply network options BEFORE API version selection or network setup
		if (!tlsCertPath.empty()) {
			setNetworkOption(FDBNetworkOptions::TLS_CERT_PATH, StringRef(tlsCertPath));
		}
		if (!tlsKeyPath.empty()) {
			setNetworkOption(FDBNetworkOptions::TLS_KEY_PATH, StringRef(tlsKeyPath));
		}
		if (!tlsCaPath.empty()) {
			setNetworkOption(FDBNetworkOptions::TLS_CA_PATH, StringRef(tlsCaPath));
		}
		if (!tlsVerifyPeers.empty()) {
			setNetworkOption(FDBNetworkOptions::TLS_VERIFY_PEERS, StringRef(tlsVerifyPeers));
		}
		// Apply other network options like trace logging if parsed
		if (logTrace) {
			setNetworkOption(FDBNetworkOptions::TRACE_ENABLE,
			                 logDir.empty() ? Optional<StringRef>() : Optional<StringRef>(StringRef(logDir)));
			if (!logGroup.empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_LOG_GROUP, StringRef(logGroup));
			}
			// traceFormat is already handled by networkOptions defaults or set via TRACE_FORMAT if explicitly set
		}

		API->selectApiVersion(parsedApiVersion);
		API->setupNetwork();
		TraceEvent::setNetworkThread(); // Set the current thread as the network thread

		// Moved knob application to after setupNetwork, similar to fdbcli's opt.setupKnobs()
		// Note: fdbcli does IKnobCollection::setupKnobs and then
		// IKnobCollection::getMutableGlobalKnobCollection().initialize(). Here, we just re-apply via setNetworkOption
		// which should achieve immediate application.
		for (const auto& knob_str : knobs) { // 'knobs' is the std::vector<std::string> from arg parsing
			setNetworkOption(FDBNetworkOptions::KNOB, StringRef(knob_str));
		}

		// --- Stage 4: Get Cluster Connection File ---
		Reference<ClusterConnectionFile> ccf;
		if (clusterFile.empty()) {
			ccf = ClusterConnectionFile::openOrDefault("");
		} else {
			ccf = ClusterConnectionFile::openOrDefault(clusterFile);
		}

		// Database object created inside runChecksumTool actor

		// Prepare arguments for the main actor
		std::string unescapedBeginKey = unescapeKeyString(beginKeyStr);
		std::string unescapedEndKey = unescapeKeyString(endKeyStr);

		Future<Void> checksumFuture = runChecksumTool(ccf, parsedApiVersion, unescapedBeginKey, unescapedEndKey);
		Future<Void> mainFuture = stopNetworkAfter_actor(checksumFuture); // Use renamed helper

		try {
			API->runNetwork(); // Use API interface

			if (mainFuture.isError()) {
				throw mainFuture.getError();
			}

			return 0;
		} catch (Error& e) {
			fprintf(stderr, "ERROR: Network or Actor failed: %s (code: %d)\n", e.what(), e.code());
			return 1;
		}

	} catch (const boost::program_options::error& e) {
		std::cerr << "ERROR parsing command line options: " << e.what() << std::endl;
		return 1;
	} catch (const std::exception& e) {
		std::cerr << "ERROR: " << e.what() << std::endl;
		return 1;
	}
	return 0;
}