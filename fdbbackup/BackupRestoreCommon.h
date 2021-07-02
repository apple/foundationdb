/*
 * BackupRestoreCommon.h
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

#pragma once

#include "fdbbackup/BackupTLSConfig.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/versions.h"
#include "flow/SimpleOpt.h"

#include <string>

extern const char* BlobCredentialInfo;

extern const char* getSourceVersion();

enum {
	// Backup options
	OPT_DESTCONTAINER,
	OPT_SNAPSHOTINTERVAL,
	OPT_INITIAL_SNAPSHOT_INTERVAL,
	OPT_ERRORLIMIT,
	OPT_NOSTOPWHENDONE,
	OPT_EXPIRE_BEFORE_VERSION,
	OPT_EXPIRE_BEFORE_DATETIME,
	OPT_EXPIRE_DELETE_BEFORE_DAYS,
	OPT_EXPIRE_RESTORABLE_AFTER_VERSION,
	OPT_EXPIRE_RESTORABLE_AFTER_DATETIME,
	OPT_EXPIRE_MIN_RESTORABLE_DAYS,
	OPT_BASEURL,
	OPT_BLOB_CREDENTIALS,
	OPT_DESCRIBE_DEEP,
	OPT_DESCRIBE_TIMESTAMPS,
	OPT_DUMP_BEGIN,
	OPT_DUMP_END,
	OPT_JSON,
	OPT_DELETE_DATA,
	OPT_MIN_CLEANUP_SECONDS,
	OPT_USE_PARTITIONED_LOG,

	// Backup and Restore options
	OPT_TAGNAME,
	OPT_BACKUPKEYS,
	OPT_WAITFORDONE,
	OPT_BACKUPKEYS_FILTER,
	OPT_INCREMENTALONLY,

	// Backup Modify options
	OPT_MOD_ACTIVE_INTERVAL,
	OPT_MOD_VERIFY_UID,

	// Restore options
	OPT_RESTORECONTAINER,
	OPT_RESTORE_VERSION,
	OPT_RESTORE_TIMESTAMP,
	OPT_PREFIX_ADD,
	OPT_PREFIX_REMOVE,
	OPT_RESTORE_CLUSTERFILE_DEST,
	OPT_RESTORE_CLUSTERFILE_ORIG,
	OPT_RESTORE_BEGIN_VERSION,
	OPT_RESTORE_INCONSISTENT_SNAPSHOT_ONLY,

	// Shared options
	OPT_CLUSTERFILE,
	OPT_QUIET,
	OPT_DRYRUN,
	OPT_FORCE,
	OPT_HELP,
	OPT_DEVHELP,
	OPT_VERSION,
	OPT_BUILD_FLAGS,
	OPT_PARENTPID,
	OPT_CRASHONERROR,
	OPT_NOBUFSTDOUT,
	OPT_BUFSTDOUTERR,
	OPT_TRACE,
	OPT_TRACE_DIR,
	OPT_KNOB,
	OPT_TRACE_LOG_GROUP,
	OPT_MEMLIMIT,
	OPT_LOCALITY,
	OPT_TRACE_FORMAT,

	// DB options
	OPT_SOURCE_CLUSTER,
	OPT_DEST_CLUSTER,
	OPT_CLEANUP,
	OPT_DSTONLY,
};

// Top level binary commands.
extern CSimpleOpt::SOption g_rgOptions[];

void handleArgsError(CSimpleOpt const& args, const char* programName);

void printHelpTeaser(std::string const& programName);

Reference<IBackupContainer> openBackupContainer(const char* name, std::string const& destinationContainer);

void printVersion();
void printBuildInformation();

void printBackupContainerInfo();

class DriverBase {
protected:
	bool trace{ false };
	std::string traceDir;
	std::string traceFormat;
	std::string traceLogGroup;
	std::vector<std::pair<std::string, std::string>> knobs;
	uint64_t memLimit{ 8LL << 30 };
	bool quietDisplay{ false };
	BackupTLSConfig tlsConfig;

	// Returns true iff common arg found
	bool processCommonArg(std::string const& programName, CSimpleOpt& arg);

	void addBlobCredentials(std::string const& blobCreds) { tlsConfig.blobCredentials.push_back(blobCreds); }

	void initTraceFile() const;

public:
	bool traceEnabled() const { return trace; }

	std::string const& getTraceDir() const { return traceDir; }

	std::string const& getTraceFormat() const { return traceFormat; }

	std::string const& getTraceLogGroup() const { return traceLogGroup; }

	uint64_t getMemLimit() const { return memLimit; }

	std::vector<std::pair<std::string, std::string>> const& getKnobOverrides() { return knobs; }

	bool setupTLS() { return tlsConfig.setupTLS(); }

	void setupBlobCredentials() { tlsConfig.setupBlobCredentials(); }
};

template <class T>
class Driver : public DriverBase {
protected:
	void processArgs(CSimpleOpt& args) {
		while (args.Next()) {
			handleArgsError(args, T::getProgramName().c_str());
			const auto& constArgs = args;
			if (!processCommonArg(static_cast<T*>(this)->getProgramName(), args)) {
				static_cast<T*>(this)->processArg(constArgs);
			}
		}

		for (int argLoop = 0; argLoop < args.FileCount(); ++argLoop) {
			fprintf(stderr,
			        "ERROR: %s does not support argument value `%s'\n",
			        T::getProgramName().c_str(),
			        args.File(argLoop));
			printHelpTeaser(T::getProgramName());
			throw invalid_option_value();
		}
	}

	// TODO: Confusing to exit program in this function?
	static void runTopLevelCommand(int optId) {
		switch (optId) {
		case OPT_VERSION:
			printVersion();
			break;
			flushAndExit(FDB_EXIT_SUCCESS);
		case OPT_BUILD_FLAGS:
			printBuildInformation();
			flushAndExit(FDB_EXIT_SUCCESS);
		case OPT_HELP:
			T::printUsage(false);
			flushAndExit(FDB_EXIT_SUCCESS);
		default:
			throw invalid_option();
		}
	}
};

template <class Driver>
int commonMain(int argc, char** argv) {
	platformInit();

	int status = FDB_EXIT_SUCCESS;

	std::string commandLine;
	for (int a = 0; a < argc; a++) {
		if (a) {
			commandLine += ' ';
		}
		commandLine += argv[a];
	}

	try {
#ifdef ALLOC_INSTRUMENTATION
		g_extra_memory = new uint8_t[1000000];
#endif
		registerCrashHandler();

		// Set default of line buffering standard out and error
		setvbuf(stdout, nullptr, _IONBF, 0);
		setvbuf(stderr, nullptr, _IONBF, 0);

		if (argc == 1) {
			Driver::printUsage(false);
			flushAndExit(FDB_EXIT_ERROR);
		}

		Driver driver;
		try {
			driver.parseCommandLineArgs(argc, argv);
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "FailedToParseCommandLineArguments").error(e);
			flushAndExit(FDB_EXIT_ERROR);
		}

		IKnobCollection::setGlobalKnobCollection(IKnobCollection::Type::CLIENT, Randomize::NO, IsSimulated::NO);
		auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
		for (const auto& [knobName, knobValueString] : driver.getKnobOverrides()) {
			try {
				auto knobValue = g_knobs.parseKnobValue(knobName, knobValueString);
				g_knobs.setKnob(knobName, knobValue);
			} catch (Error& e) {
				if (e.code() == error_code_invalid_option_value) {
					fprintf(stderr,
					        "WARNING: Invalid value '%s' for knob option '%s'\n",
					        knobValueString.c_str(),
					        knobName.c_str());
					TraceEvent(SevWarnAlways, "InvalidKnobValue")
					    .detail("Knob", printable(knobName))
					    .detail("Value", printable(knobValueString));
				} else {
					fprintf(stderr, "ERROR: Failed to set knob option '%s': %s\n", knobName.c_str(), e.what());
					TraceEvent(SevError, "FailedToSetKnob")
					    .detail("Knob", printable(knobName))
					    .detail("Value", printable(knobValueString))
					    .error(e);
					throw;
				}
			}
		}

		// Reinitialize knobs in order to update knobs that are dependent on explicitly set knobs
		g_knobs.initialize(Randomize::NO, IsSimulated::NO);

		if (driver.traceEnabled()) {
			if (!driver.getTraceLogGroup().empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_LOG_GROUP, StringRef(driver.getTraceLogGroup()));
			}

			if (driver.getTraceDir().empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_ENABLE);
			} else {
				setNetworkOption(FDBNetworkOptions::TRACE_ENABLE, StringRef(driver.getTraceDir()));
			}
			if (!driver.getTraceFormat().empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_FORMAT, StringRef(driver.getTraceFormat()));
			}

			setNetworkOption(FDBNetworkOptions::ENABLE_SLOW_TASK_PROFILING);
		}
		setNetworkOption(FDBNetworkOptions::DISABLE_CLIENT_STATISTICS_LOGGING);

		// deferred TLS options
		if (!driver.setupTLS()) {
			return 1;
		}

		Error::init();
		std::set_new_handler(&platform::outOfMemory);
		setMemoryQuota(driver.getMemLimit());

		try {
			setupNetwork(0, true);
		} catch (Error& e) {
			fprintf(stderr, "ERROR: %s\n", e.what());
			return FDB_EXIT_ERROR;
		}

		TraceEvent("ProgramStart")
		    .setMaxEventLength(12000)
		    .detail("SourceVersion", getSourceVersion())
		    .detail("Version", FDB_VT_VERSION)
		    .detail("PackageName", FDB_VT_PACKAGE_NAME)
		    .detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(NULL))
		    .setMaxFieldLength(10000)
		    .detail("CommandLine", commandLine)
		    .setMaxFieldLength(0)
		    .detail("MemoryLimit", driver.getMemLimit())
		    .trackLatest("ProgramStart");

		// Ordinarily, this is done when the network is run. However, network thread should be set before TraceEvents
		// are logged. This thread will eventually run the network, so call it now.
		TraceEvent::setNetworkThread();

		// Sets up blob credentials, including one from the environment FDB_BLOB_CREDENTIALS.
		driver.setupBlobCredentials();

		if (!driver.setup()) {
			TraceEvent(SevWarnAlways, "FailedDriverSetup");
			flushAndExit(FDB_EXIT_ERROR);
		}
		Future<Optional<Void>> f = driver.run();

		runNetwork();

		if (f.isValid() && f.isReady() && !f.isError() && !f.get().present()) {
			status = FDB_EXIT_ERROR;
		}

#ifdef ALLOC_INSTRUMENTATION
		{
			cout << "Page Counts: " << FastAllocator<16>::pageCount << " " << FastAllocator<32>::pageCount << " "
			     << FastAllocator<64>::pageCount << " " << FastAllocator<128>::pageCount << " "
			     << FastAllocator<256>::pageCount << " " << FastAllocator<512>::pageCount << " "
			     << FastAllocator<1024>::pageCount << " " << FastAllocator<2048>::pageCount << " "
			     << FastAllocator<4096>::pageCount << " " << FastAllocator<8192>::pageCount << " "
			     << FastAllocator<16384>::pageCount << endl;

			std::vector<std::pair<std::string, const char*>> typeNames;
			for (auto i = allocInstr.begin(); i != allocInstr.end(); ++i) {
				std::string s;

#ifdef __linux__
				char* demangled = abi::__cxa_demangle(i->first, NULL, NULL, NULL);
				if (demangled) {
					s = demangled;
					if (StringRef(s).startsWith(LiteralStringRef("(anonymous namespace)::")))
						s = s.substr(LiteralStringRef("(anonymous namespace)::").size());
					free(demangled);
				} else
					s = i->first;
#else
				s = i->first;
				if (StringRef(s).startsWith(LiteralStringRef("class `anonymous namespace'::")))
					s = s.substr(LiteralStringRef("class `anonymous namespace'::").size());
				else if (StringRef(s).startsWith(LiteralStringRef("class ")))
					s = s.substr(LiteralStringRef("class ").size());
				else if (StringRef(s).startsWith(LiteralStringRef("struct ")))
					s = s.substr(LiteralStringRef("struct ").size());
#endif

				typeNames.emplace_back(s, i->first);
			}
			std::sort(typeNames.begin(), typeNames.end());
			for (int i = 0; i < typeNames.size(); i++) {
				const char* n = typeNames[i].second;
				auto& f = allocInstr[n];
				printf("%+d\t%+d\t%d\t%d\t%s\n",
				       f.allocCount,
				       -f.deallocCount,
				       f.allocCount - f.deallocCount,
				       f.maxAllocated,
				       typeNames[i].first.c_str());
			}

			// We're about to exit and clean up data structures, this will wreak havoc on allocation recording
			memSample_entered = true;
		}
#endif
	} catch (Error& e) {
		TraceEvent(SevError, "MainError").error(e);
		status = FDB_EXIT_MAIN_ERROR;
	} catch (boost::system::system_error& e) {
		if (g_network) {
			TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		} else {
			fprintf(stderr, "ERROR: %s (%d)\n", e.what(), e.code().value());
		}
		status = FDB_EXIT_MAIN_EXCEPTION;
	} catch (std::exception& e) {
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		status = FDB_EXIT_MAIN_EXCEPTION;
	}

	flushAndExit(status);
	ASSERT(false);
	return 0;
}
