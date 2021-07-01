/*
 * BackupRestoreCommon.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbbackup/BackupRestoreCommon.h"

extern bool g_crashOnError;

void printHelpTeaser(std::string const& programName) {
	fprintf(stderr, "Try `%s --help' for more information.\n", programName.c_str());
}

void handleArgsError(CSimpleOpt const& args, const char* programName) {
	auto lastError = args.LastError();

	switch (lastError) {
	case SO_SUCCESS:
		return;

	case SO_ARG_INVALID_DATA:
		fprintf(stderr, "ERROR: invalid argument to option `%s'\n", args.OptionText());
		break;

	case SO_ARG_INVALID:
		fprintf(stderr, "ERROR: argument given for option `%s'\n", args.OptionText());
		break;

	case SO_ARG_MISSING:
		fprintf(stderr, "ERROR: missing argument for option `%s'\n", args.OptionText());
		break;

	case SO_OPT_INVALID:
		fprintf(stderr, "ERROR: unknown option `%s'\n", args.OptionText());
		break;

	default:
		fprintf(stderr, "ERROR: argument given for option `%s'\n", args.OptionText());
		break;
	}

	printHelpTeaser(programName);
	throw internal_error(); // TODO: Throw different error?
}

Reference<IBackupContainer> openBackupContainer(const char* name, std::string const& destinationContainer) {
	// Error, if no dest container was specified
	if (destinationContainer.empty()) {
		fprintf(stderr, "ERROR: No backup destination was specified.\n");
		printHelpTeaser(name);
		throw backup_error();
	}

	Reference<IBackupContainer> c;
	try {
		c = IBackupContainer::openContainer(destinationContainer);
	} catch (Error& e) {
		std::string msg = format("ERROR: '%s' on URL '%s'", e.what(), destinationContainer.c_str());
		if (e.code() == error_code_backup_invalid_url && !IBackupContainer::lastOpenError.empty()) {
			msg += format(": %s", IBackupContainer::lastOpenError.c_str());
		}
		fprintf(stderr, "%s\n", msg.c_str());
		printHelpTeaser(name);
		throw;
	}

	return c;
}

void printBackupContainerInfo() {
	printf("                 Backup URL forms:\n\n");
	std::vector<std::string> formats = IBackupContainer::getURLFormats();
	for (const auto& f : formats)
		printf("                     %s\n", f.c_str());
	printf("\n");
}

const char* BlobCredentialInfo =
    "  BLOB CREDENTIALS\n"
    "     Blob account secret keys can optionally be omitted from blobstore:// URLs, in which case they will be\n"
    "     loaded, if possible, from 1 or more blob credentials definition files.\n\n"
    "     These files can be specified with the --blob_credentials argument described above or via the environment "
    "variable\n"
    "     FDB_BLOB_CREDENTIALS, whose value is a colon-separated list of files.  The command line takes priority over\n"
    "     over the environment but all files from both sources are used.\n\n"
    "     At connect time, the specified files are read in order and the first matching account specification "
    "(user@host)\n"
    "     will be used to obtain the secret key.\n\n"
    "     The JSON schema is:\n"
    "        { \"accounts\" : { \"user@host\" : { \"secret\" : \"SECRETKEY\" }, \"user2@host2\" : { \"secret\" : "
    "\"SECRET\" } } }\n";

CSimpleOpt::SOption g_rgOptions[] = { { OPT_VERSION, "-v", SO_NONE },
	                                  { OPT_VERSION, "--version", SO_NONE },
	                                  { OPT_BUILD_FLAGS, "--build_flags", SO_NONE },
	                                  { OPT_HELP, "-?", SO_NONE },
	                                  { OPT_HELP, "-h", SO_NONE },
	                                  { OPT_HELP, "--help", SO_NONE },

	                                  SO_END_OF_OPTIONS };

bool DriverBase::processCommonArg(std::string const& programName, CSimpleOpt& args) {
	auto optId = args.OptionId();
	switch (optId) {
	case OPT_TRACE:
		trace = true;
		return true;
	case OPT_TRACE_DIR:
		trace = true;
		traceDir = args.OptionArg();
		return true;
	case OPT_TRACE_FORMAT:
		if (!validateTraceFormat(args.OptionArg())) {
			fprintf(stderr, "WARNING: Unrecognized trace format `%s'\n", args.OptionArg());
		}
		traceFormat = args.OptionArg();
		return true;
	case OPT_TRACE_LOG_GROUP:
		traceLogGroup = args.OptionArg();
		return true;
	case OPT_KNOB: {
		std::string syn = args.OptionSyntax();
		if (!StringRef(syn).startsWith(LiteralStringRef("--knob_"))) {
			fprintf(stderr, "ERROR: unable to parse knob option '%s'\n", syn.c_str());
			throw invalid_option();
		}
		syn = syn.substr(7);
		knobs.emplace_back(syn, args.OptionArg());
		return true;
	}
	case OPT_MEMLIMIT: {
		auto ti = parse_with_suffix(args.OptionArg(), "MiB");
		if (!ti.present()) {
			fprintf(stderr, "ERROR: Could not parse memory limit from `%s'\n", args.OptionArg());
			printHelpTeaser(programName.c_str());
			throw invalid_option_value();
		}
		memLimit = ti.get();
		return true;
	}
	case OPT_CRASHONERROR: {
		g_crashOnError = true;
		return true;
	}
	case OPT_QUIET:
		quietDisplay = true;
		return true;
	default:
		return false;
	}
}

void DriverBase::initTraceFile() const {
	if (trace) {
		openTraceFile(
		    NetworkAddress(), TRACE_DEFAULT_ROLL_SIZE, TRACE_DEFAULT_MAX_LOGS_SIZE, traceDir, "trace", traceLogGroup);
	}
}
