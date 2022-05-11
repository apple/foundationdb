/*
 * MkCertCli.cpp
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

#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>
#include <fmt/format.h>
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/MkCert.h"
#include "flow/network.h"
#include "flow/Platform.h"
#include "flow/ScopeExit.h"
#include "flow/SimpleOpt.h"
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
	                               SO_END_OF_OPTIONS };

template <size_t Len>
void printOptionUsage(std::string_view option, std::string_view(&&optionDescLines)[Len]) {
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
	printOptionUsage("--expire-server-cert", { "Deliberately expire server's leaf certificate for testing." });
	printOptionUsage("--expire-client-cert", { "Deliberately expire client's leaf certificate for testing." });
}

int main(int argc, char** argv) {
	auto serverChainLen = 3;
	auto clientChainLen = 2;
	auto serverCertFile = std::string("server_cert.pem");
	auto serverKeyFile = std::string("server_key.pem");
	auto serverCaFile = std::string("server_ca.pem");
	auto clientCertFile = std::string("client_cert.pem");
	auto clientKeyFile = std::string("client_key.pem");
	auto clientCaFile = std::string("client_ca.pem");
	auto expireServerCert = false;
	auto expireClientCert = false;
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
				fmt::print(stderr, "ERROR: unknown error {} with option '{}'\n", err, args.OptionText());
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
					serverChainLen = std::stoi(args.OptionArg());
				} catch (std::exception const& ex) {
					fmt::print(stderr, "ERROR: Invalid chain length ({})\n", ex.what());
					return FDB_EXIT_ERROR;
				}
				break;
			case OPT_CLIENT_CHAIN_LEN:
				try {
					clientChainLen = std::stoi(args.OptionArg());
				} catch (std::exception const& ex) {
					fmt::print(stderr, "ERROR: Invalid chain length ({})\n", ex.what());
					return FDB_EXIT_ERROR;
				}
				break;
			case OPT_SERVER_CERT_FILE:
				serverCertFile.assign(args.OptionArg());
				break;
			case OPT_SERVER_KEY_FILE:
				serverKeyFile.assign(args.OptionArg());
				break;
			case OPT_SERVER_CA_FILE:
				serverCaFile.assign(args.OptionArg());
				break;
			case OPT_CLIENT_CERT_FILE:
				clientCertFile.assign(args.OptionArg());
				break;
			case OPT_CLIENT_KEY_FILE:
				clientKeyFile.assign(args.OptionArg());
				break;
			case OPT_CLIENT_CA_FILE:
				clientCaFile.assign(args.OptionArg());
				break;
			case OPT_EXPIRE_SERVER_CERT:
				expireServerCert = true;
				break;
			case OPT_EXPIRE_CLIENT_CERT:
				expireClientCert = true;
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
		openTraceFile(NetworkAddress(), 10 << 20, 10 << 20, ".", "mkcert");
		auto thread = std::thread([]() {
			TraceEvent::setNetworkThread();
			g_network->run();
		});
		auto cleanUpGuard = ScopeExit([&thread]() {
			flushTraceFileVoid();
			g_network->stop();
			thread.join();
		});

		serverCertFile = abspath(serverCertFile);
		serverKeyFile = abspath(serverKeyFile);
		serverCaFile = abspath(serverCaFile);
		clientCertFile = abspath(clientCertFile);
		clientKeyFile = abspath(clientKeyFile);
		clientCaFile = abspath(clientCaFile);
		fmt::print("Server certificate chain length: {}\n"
		           "Client certificate chain length: {}\n"
		           "Server certificate file: {}\n"
		           "Server private key file: {}\n"
		           "Server CA file: {}\n"
		           "Client certificate file: {}\n"
		           "Client private key file: {}\n"
		           "Client CA file: {}\n",
		           serverChainLen,
		           clientChainLen,
		           serverCertFile,
		           serverKeyFile,
		           serverCaFile,
		           clientCertFile,
		           clientKeyFile,
		           clientCaFile);

		using FileStream = std::ofstream;
		auto checkStream = [](FileStream& fs, std::string_view filename) {
			if (!fs) {
				throw std::runtime_error(fmt::format("cannot open '{}' for writing", filename));
			}
		};
		auto ofsServerCert = FileStream(serverCertFile, std::ofstream::out | std::ofstream::trunc);
		checkStream(ofsServerCert, serverCertFile);
		auto ofsServerKey = FileStream(serverKeyFile, std::ofstream::out | std::ofstream::trunc);
		checkStream(ofsServerKey, serverKeyFile);
		auto ofsServerCa = FileStream(serverCaFile, std::ofstream::out | std::ofstream::trunc);
		checkStream(ofsServerCa, serverCaFile);
		auto ofsClientCert = FileStream(clientCertFile, std::ofstream::out | std::ofstream::trunc);
		checkStream(ofsClientCert, clientCertFile);
		auto ofsClientKey = FileStream(clientKeyFile, std::ofstream::out | std::ofstream::trunc);
		checkStream(ofsClientKey, clientKeyFile);
		auto ofsClientCa = FileStream(clientCaFile, std::ofstream::out | std::ofstream::trunc);
		checkStream(ofsClientCa, clientCaFile);
		if (serverChainLen) {
			auto arena = Arena();
			auto specs = mkcert::makeCertChainSpec(arena, std::abs(serverChainLen), mkcert::ESide::Server);
			if (expireServerCert) {
				specs[0].offsetNotBefore = -60l * 60 * 24 * 365;
				specs[0].offsetNotAfter = -10l;
			}
			auto serverChain = mkcert::makeCertChain(arena, specs, {} /*generate root CA*/);
			auto serverCa = serverChain.back().certPem;
			ofsServerCa.write(reinterpret_cast<char const*>(serverCa.begin()), serverCa.size());
			if (serverChain.size() > 1)
				serverChain.pop_back();
			auto serverCert = mkcert::concatCertChain(arena, serverChain);
			ofsServerCert.write(reinterpret_cast<char const*>(serverCert.begin()), serverCert.size());
			auto serverKey = serverChain[0].privateKeyPem;
			ofsServerKey.write(reinterpret_cast<char const*>(serverKey.begin()), serverKey.size());
		}
		ofsServerCert.close();
		ofsServerKey.close();
		ofsServerCa.close();
		if (clientChainLen) {
			auto arena = Arena();
			auto specs = mkcert::makeCertChainSpec(arena, std::abs(serverChainLen), mkcert::ESide::Server);
			if (expireClientCert) {
				specs[0].offsetNotBefore = -60l * 60 * 24 * 365;
				specs[0].offsetNotAfter = -10l;
			}
			auto serverChain = mkcert::makeCertChain(arena, specs, {} /*generate root CA*/);
			auto serverCa = serverChain.back().certPem;
			ofsServerCa.write(reinterpret_cast<char const*>(serverCa.begin()), serverCa.size());
			if (serverChain.size() > 1)
				serverChain.pop_back();
			auto serverCert = mkcert::concatCertChain(arena, serverChain);
			ofsServerCert.write(reinterpret_cast<char const*>(serverCert.begin()), serverCert.size());
			auto serverKey = serverChain[0].privateKeyPem;
			ofsServerKey.write(reinterpret_cast<char const*>(serverKey.begin()), serverKey.size());
		}
		ofsClientCert.close();
		ofsClientKey.close();
		ofsClientCa.close();
		fmt::print("OK\n");
		return FDB_EXIT_SUCCESS;
	} catch (const Error& e) {
		fmt::print(stderr, "error: {}\n", e.name());
		return FDB_EXIT_MAIN_ERROR;
	} catch (const std::exception& e) {
		fmt::print(stderr, "exception: {}\n", e.what());
		return FDB_EXIT_MAIN_EXCEPTION;
	}
}
