/*
 * MkCertCli.cpp
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

#include <cstdlib>
#include <fstream>
#include <string>
#include <string_view>
#include <thread>
#include <fmt/format.h>
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/MkCert.h"
#include "flow/network.h"
#include "flow/Platform.h"
#include "flow/ScopeExit.h"
#include "SimpleOpt/SimpleOpt.h"
#include "flow/TLSConfig.actor.h"
#include "flow/Trace.h"

enum EMkCertOpt : int {
	OPT_HELP,
	OPT_SERVER_CHAIN_LEN,
	OPT_CLIENT_CHAIN_LEN,
	OPT_SERVER_CERT_FILE,
	OPT_SERVER_KEY_FILE,
	OPT_SERVER_CA_FILE,
	OPT_CLIENT_CERT_FILE,
	OPT_CLIENT_KEY_FILE,
	OPT_CLIENT_CA_FILE,
	OPT_EXPIRE_SERVER_CERT,
	OPT_EXPIRE_CLIENT_CERT,
	OPT_PRINT_SERVER_CERT,
	OPT_PRINT_CLIENT_CERT,
	OPT_PRINT_ARGUMENTS,
	OPT_ENABLE_TRACE,
	OPT_NO_SHARED_SERVER_CLIENT_CA,
};

CSimpleOpt::SOption gOptions[] = { { OPT_HELP, "--help", SO_NONE },
	                               { OPT_HELP, "-h", SO_NONE },
	                               { OPT_SERVER_CHAIN_LEN, "--server-chain-length", SO_REQ_SEP },
	                               { OPT_SERVER_CHAIN_LEN, "-S", SO_REQ_SEP },
	                               { OPT_CLIENT_CHAIN_LEN, "--client-chain-length", SO_REQ_SEP },
	                               { OPT_CLIENT_CHAIN_LEN, "-C", SO_REQ_SEP },
	                               { OPT_SERVER_CERT_FILE, "--server-cert-file", SO_REQ_SEP },
	                               { OPT_SERVER_KEY_FILE, "--server-key-file", SO_REQ_SEP },
	                               { OPT_SERVER_CA_FILE, "--server-ca-file", SO_REQ_SEP },
	                               { OPT_CLIENT_CERT_FILE, "--client-cert-file", SO_REQ_SEP },
	                               { OPT_CLIENT_KEY_FILE, "--client-key-file", SO_REQ_SEP },
	                               { OPT_CLIENT_CA_FILE, "--client-ca-file", SO_REQ_SEP },
	                               { OPT_EXPIRE_SERVER_CERT, "--expire-server-cert", SO_NONE },
	                               { OPT_EXPIRE_CLIENT_CERT, "--expire-client-cert", SO_NONE },
	                               { OPT_PRINT_SERVER_CERT, "--print-server-cert", SO_NONE },
	                               { OPT_PRINT_CLIENT_CERT, "--print-client-cert", SO_NONE },
	                               { OPT_PRINT_ARGUMENTS, "--print-args", SO_NONE },
	                               { OPT_ENABLE_TRACE, "--trace", SO_NONE },
	                               { OPT_NO_SHARED_SERVER_CLIENT_CA, "--no-shared-server-client-ca", SO_NONE },
	                               SO_END_OF_OPTIONS };

template <size_t Len>
void printOptionUsage(std::string_view option, const char* (&&optionDescLines)[Len]) {
	constexpr std::string_view optionIndent{ "  " };
	constexpr std::string_view descIndent{ "                " };
	fmt::print(stdout, "{}{}\n", optionIndent, option);
	for (auto descLine : optionDescLines)
		fmt::print(stdout, "{}{}\n", descIndent, descLine);
	fmt::print("\n");
}

void printUsage(std::string_view binary) {
	fmt::print(stdout,
	           "mkcert: FDB test certificate chain generator\n\n"
	           "Usage: {} [OPTIONS...]\n\n",
	           binary);
	printOptionUsage("--server-chain-length LENGTH, -S LENGTH (default: 3)",
	                 { "Length of server certificate chain including root CA certificate." });
	printOptionUsage("--client-chain-length LENGTH, -C LENGTH (default: 2)",
	                 { "Length of client certificate chain including root CA certificate.",
	                   "Use zero-length to test to setup untrusted clients." });
	printOptionUsage("--server-cert-file PATH (default: 'server_cert.pem')",
	                 { "Output filename for server certificate chain excluding root CA.",
	                   "Intended for SERVERS to use as 'tls_certificate_file'.",
	                   "Certificates are concatenated in leaf-to-CA order." });
	printOptionUsage("--server-key-file PATH (default: 'server_key.pem')",
	                 { "Output filename for server private key matching its leaf certificate.",
	                   "Intended for SERVERS to use as 'tls_key_file'" });
	printOptionUsage("--server-ca-file PATH (default: 'server_ca.pem')",
	                 { "Output filename for server's root CA certificate.",
	                   "Content same as '--server-cert-file' for '--server-chain-length' == 1.",
	                   "Intended for CLIENTS to use as 'tls_ca_file': i.e. cert issuer to trust." });
	printOptionUsage("--client-cert-file PATH (default: 'client_cert.pem')",
	                 { "Output filename for client certificate chain excluding root CA.",
	                   "Intended for CLIENTS to use as 'tls_certificate_file'.",
	                   "Certificates are concatenated in leaf-to-CA order." });
	printOptionUsage("--client-key-file PATH (default: 'client_key.pem')",
	                 { "Output filename for client private key matching its leaf certificate.",
	                   "Intended for CLIENTS to use as 'tls_key_file'" });
	printOptionUsage("--client-ca-file PATH (default: 'client_ca.pem')",
	                 { "Output filename for client's root CA certificate.",
	                   "Content same as '--client-cert-file' for '--client-chain-length' == 1.",
	                   "Intended for SERVERS to use as 'tls_ca_file': i.e. cert issuer to trust." });
	printOptionUsage("--expire-server-cert (default: no)",
	                 { "Deliberately expire server's leaf certificate for testing." });
	printOptionUsage("--expire-client-cert (default: no)",
	                 { "Deliberately expire client's leaf certificate for testing." });
	printOptionUsage("--print-server-cert (default: no)",
	                 { "Print generated server certificate chain including root in human readable form.",
	                   "Printed certificates are in leaf-to-CA order.",
	                   "If --print-client-cert is also used, server chain precedes client's." });
	printOptionUsage("--print-client-cert (default: no)",
	                 { "Print generated client certificate chain including root in human readable form.",
	                   "Printed certificates are in leaf-to-CA order.",
	                   "If --print-server-cert is also used, server chain precedes client's." });
	printOptionUsage("--print-args (default: no)", { "Print chain generation arguments." });
	printOptionUsage("--no-shared-server-client-ca (default: no)",
	                 { "DISABLE the use of common root CA for client and server certificates." });
}

struct ChainSpec {
	unsigned length;
	std::string certFile;
	std::string keyFile;
	std::string caFile;
	mkcert::ESide side;
	bool expireLeaf;
	void transformPathToAbs() {
		certFile = abspath(certFile);
		keyFile = abspath(keyFile);
		caFile = abspath(caFile);
	}
	void print() {
		fmt::print(stdout, "{}-side:\n", side == mkcert::ESide::Server ? "Server" : "Client");
		fmt::print(stdout, "  Chain length: {}\n", length);
		fmt::print(stdout, "  Certificate file: {}\n", certFile);
		fmt::print(stdout, "  Key file: {}\n", keyFile);
		fmt::print(stdout, "  CA file: {}\n", caFile);
		fmt::print(stdout, "  Expire cert: {}\n", expireLeaf);
	}
	mkcert::CertChainRef makeChain(Arena& arena, Optional<mkcert::CertAndKeyRef> rootCa = {});
};

mkcert::CertChainRef ChainSpec::makeChain(Arena& arena, Optional<mkcert::CertAndKeyRef> rootCa) {
	auto checkStream = [](std::ofstream& fs, std::string_view filename) {
		if (!fs) {
			throw std::runtime_error(fmt::format("cannot open '{}' for writing", filename));
		}
	};
	auto ofsCert = std::ofstream(certFile, std::ofstream::out | std::ofstream::trunc);
	checkStream(ofsCert, certFile);
	auto ofsKey = std::ofstream(keyFile, std::ofstream::out | std::ofstream::trunc);
	checkStream(ofsKey, keyFile);
	auto ofsCa = std::ofstream(caFile, std::ofstream::out | std::ofstream::trunc);
	checkStream(ofsCa, caFile);
	if (!length)
		return {};
	auto specs = mkcert::makeCertChainSpec(arena, length, side);
	if (expireLeaf) {
		specs[0].offsetNotBefore = -60l * 60 * 24 * 365;
		specs[0].offsetNotAfter = -10l;
	}
	auto chain = mkcert::makeCertChain(arena, specs, rootCa);
	auto ca = chain.back().certPem;
	ofsCa.write(reinterpret_cast<char const*>(ca.begin()), ca.size());
	auto chainMinusRoot = chain;
	if (chainMinusRoot.size() > 1)
		chainMinusRoot.pop_back();
	auto cert = mkcert::concatCertChain(arena, chainMinusRoot);
	ofsCert.write(reinterpret_cast<char const*>(cert.begin()), cert.size());
	auto key = chain[0].privateKeyPem;
	ofsKey.write(reinterpret_cast<char const*>(key.begin()), key.size());
	return chain;
}

int main(int argc, char** argv) {
	// default chain specs
	auto serverArgs = ChainSpec{ 3u /*length*/,   "server_cert.pem",     "server_key.pem",
		                         "server_ca.pem", mkcert::ESide::Server, false /* expireLeaf */ };
	auto clientArgs = ChainSpec{ 2u /*length*/,   "client_cert.pem",     "client_key.pem",
		                         "client_ca.pem", mkcert::ESide::Client, false /* expireLeaf */ };
	auto printServerCert = false;
	auto printClientCert = false;
	auto printArguments = false;
	auto enableTrace = false;
	auto noSharedServerClientCa = false;
	auto args = CSimpleOpt(argc, argv, gOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
	while (args.Next()) {
		if (auto err = args.LastError()) {
			switch (err) {
			case SO_ARG_INVALID_DATA:
				fmt::print(stderr, "ERROR: invalid argument to option '{}'\n", args.OptionText());
				return FDB_EXIT_ERROR;
			case SO_ARG_INVALID:
				fmt::print(stderr, "ERROR: argument given to no-argument option '{}'\n", args.OptionText());
				return FDB_EXIT_ERROR;
			case SO_ARG_MISSING:
				fmt::print(stderr, "ERROR: argument missing for option '{}'\n", args.OptionText());
				return FDB_EXIT_ERROR;
			case SO_OPT_INVALID:
				fmt::print(stderr, "ERROR: unknown option '{}'\n", args.OptionText());
				return FDB_EXIT_ERROR;
			default:
				fmt::print(
				    stderr, "ERROR: unknown error {} with option '{}'\n", static_cast<int>(err), args.OptionText());
				return FDB_EXIT_ERROR;
			}
		} else {
			auto const optId = args.OptionId();
			switch (optId) {
			case OPT_HELP:
				printUsage(argv[0]);
				return FDB_EXIT_SUCCESS;
			case OPT_SERVER_CHAIN_LEN:
				try {
					serverArgs.length = std::stoul(args.OptionArg());
					assert(serverArgs.length > 0);
				} catch (std::exception const& ex) {
					fmt::print(stderr, "ERROR: Invalid chain length ({})\n", ex.what());
					return FDB_EXIT_ERROR;
				}
				break;
			case OPT_CLIENT_CHAIN_LEN:
				try {
					clientArgs.length = std::stoul(args.OptionArg());
					assert(clientArgs.length > 0);
				} catch (std::exception const& ex) {
					fmt::print(stderr, "ERROR: Invalid chain length ({})\n", ex.what());
					return FDB_EXIT_ERROR;
				}
				break;
			case OPT_SERVER_CERT_FILE:
				serverArgs.certFile.assign(args.OptionArg());
				break;
			case OPT_SERVER_KEY_FILE:
				serverArgs.keyFile.assign(args.OptionArg());
				break;
			case OPT_SERVER_CA_FILE:
				serverArgs.caFile.assign(args.OptionArg());
				break;
			case OPT_CLIENT_CERT_FILE:
				clientArgs.certFile.assign(args.OptionArg());
				break;
			case OPT_CLIENT_KEY_FILE:
				clientArgs.keyFile.assign(args.OptionArg());
				break;
			case OPT_CLIENT_CA_FILE:
				clientArgs.caFile.assign(args.OptionArg());
				break;
			case OPT_EXPIRE_SERVER_CERT:
				serverArgs.expireLeaf = true;
				break;
			case OPT_EXPIRE_CLIENT_CERT:
				clientArgs.expireLeaf = true;
				break;
			case OPT_PRINT_SERVER_CERT:
				printServerCert = true;
				break;
			case OPT_PRINT_CLIENT_CERT:
				printClientCert = true;
				break;
			case OPT_PRINT_ARGUMENTS:
				printArguments = true;
				break;
			case OPT_ENABLE_TRACE:
				enableTrace = true;
				break;
			case OPT_NO_SHARED_SERVER_CLIENT_CA:
				noSharedServerClientCa = true;
				break;
			default:
				fmt::print(stderr, "ERROR: Unknown option {}\n", args.OptionText());
				return FDB_EXIT_ERROR;
			}
		}
	}
	// Need to involve flow for the TraceEvent.
	try {
		platformInit();
		Error::init();
		g_network = newNet2(TLSConfig());
		if (enableTrace)
			openTraceFile({}, 10 << 20, 10 << 20, ".", "mkcert");
		auto thread = std::thread([]() { g_network->run(); });
		auto cleanUpGuard = ScopeExit([&thread, enableTrace]() {
			g_network->stop();
			thread.join();
			if (enableTrace)
				closeTraceFile();
		});

		serverArgs.transformPathToAbs();
		clientArgs.transformPathToAbs();
		if (printArguments) {
			serverArgs.print();
			clientArgs.print();
		}
		auto arena = Arena();
		auto serverChain = serverArgs.makeChain(arena);
		// IMPORTANT: By default, use same root CA for server and client, such that servers can trust other servers
		auto clientChain = clientArgs.makeChain(
		    arena, noSharedServerClientCa ? Optional<mkcert::CertAndKeyRef>() : serverChain.back());

		if (printServerCert || printClientCert) {
			if (printServerCert) {
				for (auto i = 0; i < serverChain.size(); i++) {
					mkcert::printCert(stdout, serverChain[i].certPem);
				}
			}
			if (printClientCert) {
				for (auto i = 0; i < clientChain.size(); i++) {
					mkcert::printCert(stdout, clientChain[i].certPem);
				}
			}
		} else {
			fmt::print("OK\n");
		}
		return FDB_EXIT_SUCCESS;
	} catch (const Error& e) {
		fmt::print(stderr, "error: {}\n", e.name());
		return FDB_EXIT_MAIN_ERROR;
	} catch (const std::exception& e) {
		fmt::print(stderr, "exception: {}\n", e.what());
		return FDB_EXIT_MAIN_EXCEPTION;
	}
}
