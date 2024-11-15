/*
 * S3Cp.actor.cpp
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
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/versions.h"
#include "fdbclient/S3BlobStore.h"
#include "flow/Platform.h"
#include "flow/ArgParseUtil.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/TLSConfig.actor.h"
#include "SimpleOpt/SimpleOpt.h"

#include "flow/actorcompiler.h" // has to be last include

extern const char* getSourceVersion();

// Copy files and directories to and from s3.
// Has a main function so can exercise the actor from the command line. Uses
// the S3BlobStoreEndpoint to interact with s3. The s3url is of the form
// expected by S3BlobStoreEndpoint:
//   blobstore://<access_key>:<secret_key>@<endpoint>/resource?bucket=<bucket>, etc.
// See the section 'Backup URls' in the backup documentation,
// https://apple.github.io/foundationdb/backups.html, for more information.
// TODO: Handle prefix as a parameter on the URL so can strip the first part
// of the resource from the blobstore URL.

namespace s3cp {
const std::string BLOBSTORE_PREFIX = "blobstore://";

// TLS and blob credentials for backups and setup for these credentials.
// Copied from fdbbackup/BackupTLSConfig.* and renamed S3CpTLSConfig.
struct S3CpTLSConfig {
	std::string tlsCertPath, tlsKeyPath, tlsCAPath, tlsPassword, tlsVerifyPeers;
	std::vector<std::string> blobCredentials;

	// Returns if TLS setup is successful
	bool setupTLS();

	// Sets up blob crentials. Add the file specified by FDB_BLOB_CREDENTIALS as well.
	// Note this must be called after g_network is set up.
	void setupBlobCredentials();
};

void S3CpTLSConfig::setupBlobCredentials() {
	// Add blob credentials files from the environment to the list collected from the command line.
	const char* blobCredsFromENV = getenv("FDB_BLOB_CREDENTIALS");
	if (blobCredsFromENV != nullptr) {
		StringRef t((uint8_t*)blobCredsFromENV, strlen(blobCredsFromENV));
		do {
			StringRef file = t.eat(":");
			if (file.size() != 0)
				blobCredentials.push_back(file.toString());
		} while (t.size() != 0);
	}

	// Update the global blob credential files list
	std::vector<std::string>* pFiles = (std::vector<std::string>*)g_network->global(INetwork::enBlobCredentialFiles);
	if (pFiles != nullptr) {
		for (auto& f : blobCredentials) {
			pFiles->push_back(f);
		}
	}
}

bool S3CpTLSConfig::setupTLS() {
	if (tlsCertPath.size()) {
		try {
			setNetworkOption(FDBNetworkOptions::TLS_CERT_PATH, tlsCertPath);
		} catch (Error& e) {
			std::cerr << "ERROR: cannot set TLS certificate path to " << tlsCertPath << " (" << e.what() << ")\n";
			return false;
		}
	}

	if (tlsCAPath.size()) {
		try {
			setNetworkOption(FDBNetworkOptions::TLS_CA_PATH, tlsCAPath);
		} catch (Error& e) {
			std::cerr << "ERROR: cannot set TLS CA path to " << tlsCAPath << " (" << e.what() << ")\n";
			return false;
		}
	}
	if (tlsKeyPath.size()) {
		try {
			if (tlsPassword.size())
				setNetworkOption(FDBNetworkOptions::TLS_PASSWORD, tlsPassword);

			setNetworkOption(FDBNetworkOptions::TLS_KEY_PATH, tlsKeyPath);
		} catch (Error& e) {
			std::cerr << "ERROR: cannot set TLS key path to " << tlsKeyPath << " (" << e.what() << ")\n";
			return false;
		}
	}
	if (tlsVerifyPeers.size()) {
		try {
			setNetworkOption(FDBNetworkOptions::TLS_VERIFY_PEERS, tlsVerifyPeers);
		} catch (Error& e) {
			std::cerr << "ERROR: cannot set TLS peer verification to " << tlsVerifyPeers << " (" << e.what() << ")\n";
			return false;
		}
	}
	return true;
}

// Get the endpoint for the given s3url.
// Populates parameters and resource with parse of s3url.
static Reference<S3BlobStoreEndpoint> getEndpoint(std::string s3url,
                                                  std::string& resource,
                                                  S3BlobStoreEndpoint::ParametersT& parameters) {
	std::string error;
	Reference<S3BlobStoreEndpoint> endpoint =
	    S3BlobStoreEndpoint::fromString(s3url, {}, &resource, &error, &parameters);
	if (resource.empty()) {
		TraceEvent(SevError, "EmptyResource").detail("s3url", s3url);
		throw backup_invalid_url();
	}
	for (auto c : resource) {
		if (!isalnum(c) && c != '_' && c != '-' && c != '.' && c != '/') {
			TraceEvent(SevError, "IllegalCharacter").detail("s3url", s3url);
			throw backup_invalid_url();
		}
	}
	if (error.size()) {
		TraceEvent(SevError, "GetEndpointError").detail("s3url", s3url).detail("error", error);
		throw backup_invalid_url();
	}
	return endpoint;
}

// Copy filepath to bucket at resource in s3.
ACTOR Future<Void> copy_up_file(Reference<S3BlobStoreEndpoint> endpoint,
                                std::string bucket,
                                std::string resource,
                                std::string filepath) {
	// Reading an SST file fully into memory is pretty obnoxious. They are about 16MB on
	// average. Streaming would require changing this s3blobstore interface.
	// Make 32MB the max size for now even though its arbitrary and way to big.
	state std::string content = readFileBytes(filepath, 1024 * 1024 * 32);
	wait(endpoint->writeEntireFile(bucket, resource, content));
	TraceEvent("Upload")
	    .detail("filepath", filepath)
	    .detail("bucket", bucket)
	    .detail("resource", resource)
	    .detail("size", content.size());
	return Void();
}

// Copy the file from the local filesystem up to s3.
ACTOR Future<Void> copy_up_file(std::string filepath, std::string s3url) {
	std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	wait(copy_up_file(endpoint, parameters["bucket"], resource, filepath));
	return Void();
}

// Copy the directory content from the local filesystem up to s3.
ACTOR Future<Void> copy_up_directory(std::string dirpath, std::string s3url) {
	state std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	state Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	state std::string bucket = parameters["bucket"];
	state std::vector<std::string> files;
	platform::findFilesRecursively(dirpath, files);
	TraceEvent("UploadDirStart")
	    .detail("filecount", files.size())
	    .detail("bucket", bucket)
	    .detail("resource", resource);
	for (const auto& file : files) {
		std::string filepath = file;
		std::string s3path = resource + "/" + file.substr(dirpath.size() + 1);
		wait(copy_up_file(endpoint, bucket, s3path, filepath));
	}
	TraceEvent("UploadDirEnd").detail("bucket", bucket).detail("resource", resource);
	return Void();
}

// Copy filepath to bucket at resource in s3.
ACTOR Future<Void> copy_down_file(Reference<S3BlobStoreEndpoint> endpoint,
                                  std::string bucket,
                                  std::string resource,
                                  std::string filepath) {
	std::string content = wait(endpoint->readEntireFile(bucket, resource));
	auto parent = std::filesystem::path(filepath).parent_path();
	if (parent != "" && !std::filesystem::exists(parent)) {
		std::filesystem::create_directories(parent);
	}
	writeFile(filepath, content);
	TraceEvent("Download")
	    .detail("filepath", filepath)
	    .detail("bucket", bucket)
	    .detail("resource", resource)
	    .detail("size", content.size());
	return Void();
}

// Copy the file from s3 down to the local filesystem.
// Overwrites existing file.
ACTOR Future<Void> copy_down_file(std::string s3url, std::string filepath) {
	std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	wait(copy_down_file(endpoint, parameters["bucket"], resource, filepath));
	return Void();
}

// Copy down the directory content from s3 to the local filesystem.
ACTOR Future<Void> copy_down_directory(std::string s3url, std::string dirpath) {
	state std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	state Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	state std::string bucket = parameters["bucket"];
	S3BlobStoreEndpoint::ListResult items = wait(endpoint->listObjects(parameters["bucket"], resource));
	state std::vector<S3BlobStoreEndpoint::ObjectInfo> objects = items.objects;
	TraceEvent("DownloadDirStart")
	    .detail("filecount", objects.size())
	    .detail("bucket", bucket)
	    .detail("resource", resource);
	for (const auto& object : objects) {
		std::string filepath = dirpath + "/" + object.name.substr(resource.size());
		std::string s3path = object.name;
		wait(copy_down_file(endpoint, bucket, s3path, filepath));
	}
	TraceEvent("DownloadDirEnd").detail("bucket", bucket).detail("resource", resource);
	return Void();
}

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
	             "                 the fdb s3 'blobstore://' url format."
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
	s3cp::S3CpTLSConfig tlsConfig;
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
	return url.starts_with(s3cp::BLOBSTORE_PREFIX);
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
			wait(copy_up_directory(params->src, params->tgt));
		} else {
			wait(copy_up_file(params->src, params->tgt));
		}
	} else {
		if (std::filesystem::is_directory(params->tgt)) {
			wait(copy_down_directory(params->src, params->tgt));
		} else {
			wait(copy_down_file(params->src, params->tgt));
		}
	}
	return Void();
}
} // namespace s3cp

int main(int argc, char** argv) {
	std::string commandLine;
	for (int a = 0; a < argc; a++) {
		if (a) {
			commandLine += ' ';
		}
		commandLine += argv[a];
	}

	try {
		// This csimpleopt parser is not the smartest. If you pass --logs instead of --log, it will
		// treat the --logs as an argument though it is a misnamed option. Doesn't seem to be a way
		// around it.
		std::unique_ptr<CSimpleOpt> args(
		    new CSimpleOpt(argc, argv, s3cp::Options, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE | SO_O_NOERR));
		auto param = makeReference<s3cp::Params>();
		int status = s3cp::parseCommandLine(param, args.get());
		if (status != FDB_EXIT_SUCCESS) {
			s3cp::printUsage(argv[0]);
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

		flushTraceFileVoid();
		fflush(stdout);
		closeTraceFile();

		return status;
	} catch (Error& e) {
		std::cerr << "ERROR: " << e.what() << "\n";
		return FDB_EXIT_ERROR;
	} catch (std::exception& e) {
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		return FDB_EXIT_MAIN_EXCEPTION;
	}
}