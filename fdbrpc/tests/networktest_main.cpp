/*
 * networktest_main.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "NetworkTest.h"
#include "SimpleOpt/SimpleOpt.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/Net2FileSystem.h"
#include "flow/ArgParseUtil.h"
#include "flow/BooleanParam.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.h"
#include "flow/Trace.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <exception>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

FDB_BOOLEAN_PARAM(Randomize);
FDB_BOOLEAN_PARAM(IsSimulated);

namespace {

enum Option {
	OPT_HELP,
	OPT_MODE,
	OPT_TESTSERVERS,
	OPT_PUBLIC_ADDRESS,
	OPT_LISTEN_ADDRESS,
	OPT_P2P_OPTION,
	OPT_KNOB,
	OPT_TRACE_DIR,
};

CSimpleOpt::SOption options[] = { { OPT_HELP, "-h", SO_NONE },
	                              { OPT_HELP, "--help", SO_NONE },
	                              { OPT_MODE, "-m", SO_REQ_SEP },
	                              { OPT_MODE, "--mode", SO_REQ_SEP },
	                              { OPT_TESTSERVERS, "--testservers", SO_REQ_SEP },
	                              { OPT_PUBLIC_ADDRESS, "-p", SO_REQ_SEP },
	                              { OPT_PUBLIC_ADDRESS, "--public-address", SO_REQ_SEP },
	                              { OPT_LISTEN_ADDRESS, "-l", SO_REQ_SEP },
	                              { OPT_LISTEN_ADDRESS, "--listen-address", SO_REQ_SEP },
	                              { OPT_P2P_OPTION, "--test-", SO_REQ_SEP },
	                              { OPT_KNOB, "--knob-", SO_REQ_SEP },
	                              { OPT_TRACE_DIR, "--trace-dir", SO_REQ_SEP },
	                              { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	                              TLS_OPTION_FLAGS,
	                              SO_END_OF_OPTIONS };

struct Options {
	std::string mode;
	std::string testServers;
	std::vector<std::string> publicAddresses;
	std::vector<std::string> listenAddresses;
	P2PNetworkTestOptions p2pOptions;
	bool hasP2POptions = false;
	TLSConfig tlsConfig{ TLSEndpointType::SERVER };
	std::string traceDir = ".";
	bool showHelp = false;
};

void printUsage(const char* program) {
	printf("Usage: %s --mode MODE [OPTIONS]\n"
	       "\n"
	       "Modes:\n"
	       "  server          Serve RPC network-test requests\n"
	       "  client          Send RPC requests to --testservers ADDRESS[,ADDRESS...]\n"
	       "  p2p             Exercise raw connections and traffic\n"
	       "  p2p-oneshot     Exercise connection handshakes without traffic\n"
	       "\n"
	       "Options:\n"
	       "  -p, --public-address ADDR   RPC server public IP:PORT[:tls]\n"
	       "  -l, --listen-address ADDR   RPC server bind address (default: public)\n"
	       "      --testservers ADDRS     RPC server addresses, or nanosleep\n"
	       "      --test_NAME VALUE       P2P parameter (e.g. listenerAddresses,\n"
	       "                             remoteAddresses, targetDuration, connectionsOut)\n"
	       "      --knob_NAME VALUE       Override a Flow knob\n"
	       "      --trace-dir DIR         Trace directory (default: .)\n"
	       "  -h, --help                  Show this help\n"
	       "\n"
	       "Public/listen addresses accept comma-separated lists or repeated options,\n"
	       "with at most two addresses. A listen address may be 'public'.\n"
	       "Option names accept either hyphens or underscores.\n"
	       "\n%s",
	       program,
	       TLS_HELP);
}

void appendAddresses(std::vector<std::string>& addresses, const char* text) {
	std::string remaining(text);
	for (;;) {
		const auto comma = remaining.find(',');
		addresses.push_back(remaining.substr(0, comma));
		if (comma == std::string::npos) {
			return;
		}
		remaining.erase(0, comma + 1);
	}
}

Optional<int> parseNonnegativeInt(std::string_view text, int maximum = std::numeric_limits<int>::max()) {
	int value;
	const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
	if (error != std::errc() || end != text.data() + text.size() || value < 0 || value > maximum) {
		return {};
	}
	return value;
}

Optional<NetworkTestIntRange> parseRange(std::string_view text) {
	const auto colon = text.find(':');
	const auto low = parseNonnegativeInt(text.substr(0, colon), std::numeric_limits<int>::max() - 1);
	const auto high = colon == std::string_view::npos
	                      ? low
	                      : parseNonnegativeInt(text.substr(colon + 1), std::numeric_limits<int>::max() - 1);
	if (!low.present() || !high.present()) {
		return {};
	}
	return NetworkTestIntRange(low.get(), high.get());
}

bool parseP2POption(P2PNetworkTestOptions& options, const std::string& name, const std::string& value) {
	if (name == "listenerAddresses" || name == "remoteAddresses") {
		std::vector<NetworkAddress> addresses;
		if (!value.empty()) {
			addresses = NetworkAddress::parseList(value);
			if (addresses.empty() || !std::all_of(addresses.begin(), addresses.end(), [](const auto& address) {
				    return address.isValid();
			    })) {
				return false;
			}
		}
		(name == "listenerAddresses" ? options.listenerAddresses : options.remoteAddresses) = std::move(addresses);
		return true;
	}
	if (name == "connectionsOut") {
		const auto count = parseNonnegativeInt(value);
		if (!count.present()) {
			return false;
		}
		options.connectionsOut = count.get();
		return true;
	}
	if (name == "targetDuration") {
		try {
			size_t end;
			const auto duration = std::stod(value, &end);
			if (end == value.size() && std::isfinite(duration) && duration >= 0) {
				options.targetDuration = duration;
				return true;
			}
		} catch (const std::exception&) {
		}
		return false;
	}
	NetworkTestIntRange* range = nullptr;
	if (name == "requestBytes") {
		range = &options.requestBytes;
	} else if (name == "replyBytes") {
		range = &options.replyBytes;
	} else if (name == "requests") {
		range = &options.requests;
	} else if (name == "idleMilliseconds") {
		range = &options.idleMilliseconds;
	} else if (name == "waitReadMilliseconds") {
		range = &options.waitReadMilliseconds;
	} else if (name == "waitWriteMilliseconds") {
		range = &options.waitWriteMilliseconds;
	}
	const auto parsed = parseRange(value);
	if (!range || !parsed.present()) {
		return false;
	}
	*range = parsed.get();
	return true;
}

bool parseArgs(int argc, char** argv, Options& result, FlowKnobs& knobs) {
	CSimpleOpt args(argc, argv, options, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
	while (args.Next()) {
		if (args.LastError() != SO_SUCCESS) {
			fprintf(stderr, "ERROR: Invalid or incomplete option '%s'\n", args.OptionText());
			return false;
		}
		switch (args.OptionId()) {
		case OPT_HELP:
			result.showHelp = true;
			return true;
		case OPT_MODE:
			result.mode = args.OptionArg();
			break;
		case OPT_TESTSERVERS:
			result.testServers = args.OptionArg();
			break;
		case OPT_PUBLIC_ADDRESS:
			appendAddresses(result.publicAddresses, args.OptionArg());
			break;
		case OPT_LISTEN_ADDRESS:
			appendAddresses(result.listenAddresses, args.OptionArg());
			break;
		case OPT_P2P_OPTION: {
			auto name = extractPrefixedArgument("--test", args.OptionSyntax());
			if (!name.present() || name.get().empty()) {
				return false;
			}
			if (!parseP2POption(result.p2pOptions, name.get(), args.OptionArg())) {
				fprintf(stderr, "ERROR: Invalid P2P option --test_%s=%s\n", name.get().c_str(), args.OptionArg());
				return false;
			}
			result.hasP2POptions = true;
			break;
		}
		case OPT_KNOB: {
			auto name = extractPrefixedArgument("--knob", args.OptionSyntax());
			if (!name.present() || name.get().empty()) {
				return false;
			}
			const auto value = knobs.parseKnobValue(name.get(), args.OptionArg());
			const bool set = std::visit(
			    [&](const auto& parsed) {
				    if constexpr (std::is_same_v<std::decay_t<decltype(parsed)>, NoKnobFound>) {
					    return false;
				    } else {
					    return knobs.setKnob(name.get(), parsed);
				    }
			    },
			    value);
			if (!set) {
				fprintf(stderr, "ERROR: Unknown Flow knob '%s'\n", name.get().c_str());
				return false;
			}
			break;
		}
		case OPT_TRACE_DIR:
			result.traceDir = args.OptionArg();
			break;
		case TLSConfig::OPT_TLS_PLUGIN:
			break;
		case TLSConfig::OPT_TLS_CERTIFICATES:
			result.tlsConfig.setCertificatePath(args.OptionArg());
			break;
		case TLSConfig::OPT_TLS_KEY:
			result.tlsConfig.setKeyPath(args.OptionArg());
			break;
		case TLSConfig::OPT_TLS_CA_FILE:
			result.tlsConfig.setCAPath(args.OptionArg());
			break;
		case TLSConfig::OPT_TLS_PASSWORD:
			result.tlsConfig.setPassword(args.OptionArg());
			break;
		case TLSConfig::OPT_TLS_VERIFY_PEERS:
			result.tlsConfig.addVerifyPeers(args.OptionArg());
			break;
		case TLSConfig::OPT_TLS_DISABLE_PLAINTEXT_CONNECTION:
			result.tlsConfig.setDisablePlainTextConnection(true);
			break;
		}
	}
	if (args.FileCount() != 0 ||
	    (result.mode != "client" && result.mode != "server" && result.mode != "p2p" && result.mode != "p2p-oneshot")) {
		fprintf(stderr, "ERROR: Expected --mode client, server, p2p, or p2p-oneshot\n");
		return false;
	}
	if (result.mode == "server") {
		if (result.publicAddresses.empty() || result.publicAddresses.size() > 2 ||
		    (!result.listenAddresses.empty() && result.listenAddresses.size() != result.publicAddresses.size())) {
			fprintf(stderr, "ERROR: Server requires one or two public addresses and matching listen addresses\n");
			return false;
		}
		result.listenAddresses.resize(result.publicAddresses.size(), "public");
	} else if (!result.publicAddresses.empty() || !result.listenAddresses.empty()) {
		fprintf(stderr, "ERROR: --public-address and --listen-address require --mode server\n");
		return false;
	}
	if ((result.mode == "client") != !result.testServers.empty()) {
		fprintf(stderr, "ERROR: --testservers is required for client mode and is only valid in client mode\n");
		return false;
	}
	if (result.mode == "p2p" || result.mode == "p2p-oneshot") {
		if (result.p2pOptions.listenerAddresses.empty() &&
		    (result.p2pOptions.remoteAddresses.empty() || result.p2pOptions.connectionsOut == 0)) {
			fprintf(stderr, "ERROR: P2P mode requires a listener or a remote with positive connectionsOut\n");
			return false;
		}
		return true;
	}
	if (result.hasP2POptions) {
		fprintf(stderr, "ERROR: --test_NAME parameters require a P2P mode\n");
		return false;
	}
	return true;
}

Future<Void> stopNetworkAfter(Future<Void> work) {
	try {
		co_await work;
	} catch (Error&) {
		g_network->stop();
		throw;
	}
	g_network->stop();
}

} // namespace

int main(int argc, char** argv) {
	try {
		platformInit();
		Error::init();
		setvbuf(stdout, nullptr, _IOLBF, BUFSIZ);
		setvbuf(stderr, nullptr, _IOLBF, BUFSIZ);
		setThreadLocalDeterministicRandomSeed(platform::getRandomSeed());
		// Network and trace globals retain these knobs through process shutdown.
		auto* knobs = new FlowKnobs(Randomize::False, IsSimulated::False);
		FLOW_KNOBS = knobs;
		Options opts;
		if (!parseArgs(argc, argv, opts, *knobs)) {
			printUsage(argv[0]);
			return 1;
		}
		if (opts.showHelp) {
			printUsage(argv[0]);
			return 0;
		}

		TraceEvent::setNetworkThread();
		g_network = newNet2(opts.tlsConfig, false, true);
		g_network->addStopCallback(Net2FileSystem::stop);
		Net2FileSystem::newFileSystem();
		FlowTransport::createInstance(false, 1, WLTOKEN_NETWORKTEST + 1);
		openTraceFile({}, 10 << 20, 10 << 20, opts.traceDir, "networktest");
		g_network->initTLS();
		g_network->initMetrics();
		FlowTransport::transport().initMetrics();

		std::vector<Future<Void>> work;
		if (opts.mode == "server") {
			for (size_t i = 0; i < opts.publicAddresses.size(); ++i) {
				const auto publicAddress = NetworkAddress::parse(opts.publicAddresses[i]);
				const auto listenAddress = opts.listenAddresses[i] == "public"
				                               ? publicAddress
				                               : NetworkAddress::parse(opts.listenAddresses[i]);
				if (!publicAddress.isValid() || !listenAddress.isValid() ||
				    publicAddress.isTLS() != listenAddress.isTLS()) {
					fprintf(stderr, "ERROR: Public/listen addresses must be valid and use matching TLS settings\n");
					return 1;
				}
				auto listenError = FlowTransport::transport().bind(publicAddress, listenAddress);
				if (listenError.isReady()) {
					listenError.get();
				}
				work.push_back(listenError);
				printf("Listener: %s\n", listenAddress.toString().c_str());
			}
			work.push_back(networkTestServer());
		} else if (opts.mode == "client") {
			work.push_back(networkTestClient(opts.testServers));
		} else {
			work.push_back(networkTestP2P(opts.p2pOptions, opts.mode == "p2p-oneshot"));
		}
		Future<Void> done = stopNetworkAfter(waitForAny(work));
		g_network->run();
		flushTraceFileVoid();
		done.get();
		return 0;
	} catch (Error& e) {
		fprintf(stderr, "ERROR: Network test failed: %s (%d)\n", e.what(), e.code());
	} catch (std::exception& e) {
		fprintf(stderr, "ERROR: Network test failed: %s\n", e.what());
	}
	flushTraceFileVoid();
	return 1;
}
