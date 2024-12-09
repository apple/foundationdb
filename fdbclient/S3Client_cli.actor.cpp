/*
 * S3Client_cli.cpp
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

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include <fcntl.h>
#include <filesystem>

#ifdef _WIN32
#include <io.h>
#endif

#include "fdbclient/BuildFlags.h"
#include "fdbclient/BackupContainerFileSystem.h"
#import "fdbclient/BackupTLSConfig.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/versions.h"
#include "fdbclient/S3Client.actor.h"
#include "flow/Platform.h"
#include "flow/ArgParseUtil.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/TLSConfig.actor.h"
#include "SimpleOpt/SimpleOpt.h"

#include "flow/actorcompiler.h" // has to be last include

// CLI for S3Client.

extern const char* getSourceVersion();

namespace s3client_cli {

enum {
	OPT_BLOB_CREDENTIALS,
	OPT_TRACE,
	OPT_TRACE_DIR,
	OPT_TRACE_FORMAT,
	OPT_TRACE_LOG_GROUP,
	OPT_BUILD_FLAGS,
	OPT_KNOB,
	OPT_HELP
};

CSimpleOpt::SOption Options[] = { { OPT_TRACE, "--log", SO_NONE },
	                              { OPT_TRACE, "--logs", SO_NONE },
	                              { OPT_TRACE, "-l", SO_NONE },
	                              { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	                              { OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	                              { OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	                              { OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	                              TLS_OPTION_FLAGS,
	                              { OPT_BUILD_FLAGS, "--build-flags", SO_NONE },
	                              { OPT_KNOB, "--knob-", SO_REQ_SEP },
	                              { OPT_HELP, "-h", SO_NONE },
	                              { OPT_HELP, "--help", SO_NONE },
	                              SO_END_OF_OPTIONS };

static void printUsage(std::string const& programName) {
	std::cout << "Usage: " << programName
	          << " [OPTIONS] SOURCE TARGET\n"
	             "Copy files to and from S3.\n"
	             "Options:\n"
	             "  --log          Enables trace file logging for the CLI session.\n"
	             "  --logdir PATH  Specifies the output directory for trace files. If\n"
	             "                 unspecified, defaults to the current directory. Has\n"
	             "                 no effect unless --log is specified.\n"
	             "  --loggroup     LOG_GROUP\n"
	             "                 Sets the LogGroup field with the specified value for all\n"
	             "                 events in the trace output (defaults to `default').\n"
	             "  --trace-format FORMAT\n"
	             "                 Select the format of the trace files, xml (the default) or json.\n"
	             "                 Has no effect unless --log is specified.\n"
	             "  --blob-credentials FILE\n"
	             "                 File containing blob credentials in JSON format.\n"
	             "                 The same credential format/file fdbbackup uses.\n" TLS_HELP
	             "  --build-flags  Print build information and exit.\n"
	             "  --knob-KNOBNAME KNOBVALUE\n"
	             "                 Changes a knob value. KNOBNAME should be lowercase.\n"
	             "Arguments:\n"
	             " SOURCE          File, directory, or s3 bucket URL to copy from.\n"
	             "                 If SOURCE is an s3 bucket URL, TARGET must be a directory and vice versa.\n"
	             "                 See 'Backup URLs' in https://apple.github.io/foundationdb/backups.html for\n"
	             "                 the fdb s3 'blobstore://' url format.\n"
	             " TARGET          Where to place the copy.\n"
	             "Examples:\n"
	             " "
	          << programName
	          << " --blob-credentials /path/to/credentials.json /path/to/source /path/to/target\n"
	             " "
	          << programName
	          << " --knob_http_verbose_level=10 --log  "
	             "'blobstore://localhost:8333/x?bucket=backup&region=us&secure_connection=0' dir3\n";
	return;
}

static void printBuildInformation() {
	std::cout << jsonBuildInformation() << "\n";
}

struct Params : public ReferenceCounted<Params> {
	Optional<std::string> proxy;
	bool log_enabled = false;
	std::string log_dir, trace_format, trace_log_group;
	BackupTLSConfig tlsConfig;
	std::vector<std::pair<std::string, std::string>> knobs;
	std::string src;
	std::string tgt;
	int whichIsBlobstoreURL = -1;

	std::string toString() {
		std::string s;
		if (proxy.present()) {
			s.append(", Proxy: ");
			s.append(proxy.get());
		}
		if (log_enabled) {
			if (!log_dir.empty()) {
				s.append(" LogDir:").append(log_dir);
			}
			if (!trace_format.empty()) {
				s.append(" Format:").append(trace_format);
			}
			if (!trace_log_group.empty()) {
				s.append(" LogGroup:").append(trace_log_group);
			}
		}
		for (const auto& [knob, value] : knobs) {
			s.append(", KNOB-").append(knob).append(" = ").append(value);
		}
		s.append(", Source: ").append(src);
		s.append(", Target: ").append(tgt);
		return s;
	}

	void updateKnobs() {
		IKnobCollection::setupKnobs(knobs);

		// Reinitialize knobs in order to update knobs that are dependent on explicitly set knobs
		IKnobCollection::getMutableGlobalKnobCollection().initialize(Randomize::False, IsSimulated::False);
	}
};

static int isBlobStoreURL(const std::string& url) {
	return url.starts_with(BLOBSTORE_PREFIX);
}

static int parseCommandLine(Reference<Params> param, CSimpleOpt* args) {
	while (args->Next()) {
		auto lastError = args->LastError();
		switch (lastError) {
		case SO_SUCCESS:
			break;

		default:
			std::cerr << "ERROR: argument given for option: " << args->OptionText() << "\n";
			return FDB_EXIT_ERROR;
			break;
		}
		int optId = args->OptionId();
		switch (optId) {
		case OPT_HELP:
			return FDB_EXIT_ERROR;

		case OPT_TRACE:
			param->log_enabled = true;
			break;

		case OPT_TRACE_DIR:
			param->log_dir = args->OptionArg();
			break;

		case OPT_TRACE_FORMAT:
			if (!selectTraceFormatter(args->OptionArg())) {
				std::cerr << "ERROR: Unrecognized trace format " << args->OptionArg() << "\n";
				return FDB_EXIT_ERROR;
			}
			param->trace_format = args->OptionArg();
			break;

		case OPT_TRACE_LOG_GROUP:
			param->trace_log_group = args->OptionArg();
			break;

		case OPT_BLOB_CREDENTIALS:
			param->tlsConfig.blobCredentials.push_back(args->OptionArg());
			break;

		case OPT_KNOB: {
			Optional<std::string> knobName = extractPrefixedArgument("--knob", args->OptionSyntax());
			if (!knobName.present()) {
				std::cerr << "ERROR: unable to parse knob option '" << args->OptionSyntax() << "'\n";
				return FDB_EXIT_ERROR;
			}
			param->knobs.emplace_back(knobName.get(), args->OptionArg());
			break;
		}

		case TLSConfig::OPT_TLS_PLUGIN:
			args->OptionArg();
			break;

		case TLSConfig::OPT_TLS_CERTIFICATES:
			param->tlsConfig.tlsCertPath = args->OptionArg();
			break;

		case TLSConfig::OPT_TLS_PASSWORD:
			param->tlsConfig.tlsPassword = args->OptionArg();
			break;

		case TLSConfig::OPT_TLS_CA_FILE:
			param->tlsConfig.tlsCAPath = args->OptionArg();
			break;

		case TLSConfig::OPT_TLS_KEY:
			param->tlsConfig.tlsKeyPath = args->OptionArg();
			break;

		case TLSConfig::OPT_TLS_VERIFY_PEERS:
			param->tlsConfig.tlsVerifyPeers = args->OptionArg();
			break;

		case OPT_BUILD_FLAGS:
			printBuildInformation();
			return FDB_EXIT_ERROR;
			break;
		}
	}
	// Now read the src and tgt arguments.
	if (args->FileCount() != 2) {
		std::cerr << "ERROR: Not enough arguments; need a SOURCE and a TARGET" << std::endl;
		return FDB_EXIT_ERROR;
	}
	param->src = args->Files()[0];
	param->tgt = args->Files()[1];
	param->whichIsBlobstoreURL = isBlobStoreURL(param->src) ? 0 : isBlobStoreURL(param->tgt) ? 1 : -1;
	if (param->whichIsBlobstoreURL < 0) {
		std::cerr << "ERROR: Either SOURCE or TARGET needs to be a blobstore URL "
		          << "(e.g. blobstore://myKey:mySecret@something.domain.com:80/dec_1_2017_0400?bucket=backups)"
		          << std::endl;
		return FDB_EXIT_ERROR;
	}
	return FDB_EXIT_SUCCESS;
}

// Method called by main. Figures which of copy_up or copy_down to call.
ACTOR Future<Void> run(Reference<Params> params) {
	if (params->whichIsBlobstoreURL == 1) {
		if (std::filesystem::is_directory(params->src)) {
			wait(copyUpDirectory(params->src, params->tgt));
		} else {
			wait(copyUpFile(params->src, params->tgt));
		}
	} else {
		if (std::filesystem::is_directory(params->tgt)) {
			wait(copyDownDirectory(params->src, params->tgt));
		} else {
			wait(copyDownFile(params->src, params->tgt));
		}
	}
	return Void();
}
} // namespace s3client_cli

int main(int argc, char** argv) {
	std::string commandLine;
	for (int a = 0; a < argc; a++) {
		if (a) {
			commandLine += ' ';
		}
		commandLine += argv[a];
	}
	int status;
	try {
		// This csimpleopt parser is not the smartest. If you pass --logs instead of --log, it will
		// treat the --logs as an argument though it is a misnamed option. Doesn't seem to be a way
		// around it.
		std::unique_ptr<CSimpleOpt> args(
		    new CSimpleOpt(argc, argv, s3client_cli::Options, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE | SO_O_NOERR));
		auto param = makeReference<s3client_cli::Params>();
		status = s3client_cli::parseCommandLine(param, args.get());
		if (status != FDB_EXIT_SUCCESS) {
			s3client_cli::printUsage(argv[0]);
			return status;
		}
		if (param->log_enabled) {
			if (param->log_dir.empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_ENABLE);
			} else {
				setNetworkOption(FDBNetworkOptions::TRACE_ENABLE, StringRef(param->log_dir));
			}
			if (!param->trace_format.empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_FORMAT, StringRef(param->trace_format));
			} else {
				setNetworkOption(FDBNetworkOptions::TRACE_FORMAT, "json"_sr);
			}
			if (!param->trace_log_group.empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_LOG_GROUP, StringRef(param->trace_log_group));
			}
		}

		if (!param->tlsConfig.setupTLS()) {
			TraceEvent(SevError, "TLSError").log();
			throw tls_error();
		}

		platformInit();
		Error::init();

		setupNetwork(0, UseMetrics::True);

		// Must be called after setupNetwork() to be effective
		param->updateKnobs();

		TraceEvent("ProgramStart")
		    .setMaxEventLength(12000)
		    .detail("SourceVersion", getSourceVersion())
		    .detail("Version", FDB_VT_VERSION)
		    .detail("PackageName", FDB_VT_PACKAGE_NAME)
		    .detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(NULL))
		    .setMaxFieldLength(10000)
		    .detail("CommandLine", commandLine)
		    .setMaxFieldLength(0)
		    .trackLatest("ProgramStart");

		TraceEvent::setNetworkThread();
		std::string path(argv[0]);
		openTraceFile(
		    {}, 10 << 20, 500 << 20, param->log_dir, path.substr(path.find_last_of("/\\") + 1), param->trace_log_group);
		param->tlsConfig.setupBlobCredentials();
		auto f = stopAfter(run(param));
		runNetwork();
	} catch (Error& e) {
		std::cerr << "ERROR: " << e.what() << "\n";
		return FDB_EXIT_ERROR;
	} catch (std::exception& e) {
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		return FDB_EXIT_MAIN_EXCEPTION;
	}
	flushAndExit(status);
}
