/*
 * FileDecoder.actor.cpp
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

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "fdbbackup/BackupTLSConfig.h"
#include "fdbclient/BuildFlags.h"
#include "fdbbackup/FileConverter.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/MutationList.h"
#include "flow/ArgParseUtil.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/serialize.h"

#include "flow/actorcompiler.h" // has to be last include

#define SevDecodeInfo SevVerbose

extern bool g_crashOnError;

namespace file_converter {

void printDecodeUsage() {
	std::cout << "Decoder for FoundationDB backup mutation logs.\n"
	             "Usage: fdbdecode  [OPTIONS]\n"
	             "  -r, --container URL\n"
	             "                 Backup container URL, e.g., file:///some/path/.\n"
	             "  -i, --input    FILE\n"
	             "                 Log file filter, only matched files are decoded.\n"
	             "  --log          Enables trace file logging for the CLI session.\n"
	             "  --logdir PATH  Specifes the output directory for trace files. If\n"
	             "                 unspecified, defaults to the current directory. Has\n"
	             "                 no effect unless --log is specified.\n"
	             "  --loggroup     LOG_GROUP\n"
	             "                 Sets the LogGroup field with the specified value for all\n"
	             "                 events in the trace output (defaults to `default').\n"
	             "  --trace-format FORMAT\n"
	             "                 Select the format of the trace files, xml (the default) or json.\n"
	             "                 Has no effect unless --log is specified.\n"
	             "  --crash        Crash on serious error.\n"
	             "  --blob-credentials FILE\n"
	             "                 File containing blob credentials in JSON format.\n"
	             "                 The same credential format/file fdbbackup uses.\n"
#ifndef TLS_DISABLED
	    TLS_HELP
#endif
	             "  --build-flags  Print build information and exit.\n"
	             "  --list-only    Print file list and exit.\n"
	             "  -k KEY_PREFIX  Use the prefix for filtering mutations\n"
	             "  --hex-prefix   HEX_PREFIX\n"
	             "                 The prefix specified in HEX format, e.g., \"\\\\x05\\\\x01\".\n"
	             "  --begin-version-filter BEGIN_VERSION\n"
	             "                 The version range's begin version (inclusive) for filtering.\n"
	             "  --end-version-filter END_VERSION\n"
	             "                 The version range's end version (exclusive) for filtering.\n"
	             "  --knob-KNOBNAME KNOBVALUE\n"
	             "                 Changes a knob value. KNOBNAME should be lowercase."
	             "\n";
	return;
}

void printBuildInformation() {
	std::cout << jsonBuildInformation() << "\n";
}

struct DecodeParams {
	std::string container_url;
	Optional<std::string> proxy;
	std::string fileFilter; // only files match the filter will be decoded
	bool log_enabled = true;
	std::string log_dir, trace_format, trace_log_group;
	BackupTLSConfig tlsConfig;
	bool list_only = false;
	std::string prefix; // Key prefix for filtering
	Version beginVersionFilter = 0;
	Version endVersionFilter = std::numeric_limits<Version>::max();

	std::vector<std::pair<std::string, std::string>> knobs;

	// Returns if [begin, end) overlap with the filter range
	bool overlap(Version begin, Version end) const {
		// Filter [100, 200),  [50,75) [200, 300)
		return !(begin >= endVersionFilter || end <= beginVersionFilter);
	}

	std::string toString() {
		std::string s;
		s.append("ContainerURL: ");
		s.append(container_url);
		if (proxy.present()) {
			s.append(", Proxy: ");
			s.append(proxy.get());
		}
		s.append(", FileFilter: ");
		s.append(fileFilter);
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
		s.append(", list_only: ").append(list_only ? "true" : "false");
		if (beginVersionFilter != 0) {
			s.append(", beginVersionFilter: ").append(std::to_string(beginVersionFilter));
		}
		if (endVersionFilter < std::numeric_limits<Version>::max()) {
			s.append(", endVersionFilter: ").append(std::to_string(endVersionFilter));
		}
		if (!prefix.empty()) {
			s.append(", KeyPrefix: ").append(printable(KeyRef(prefix)));
		}
		for (const auto& [knob, value] : knobs) {
			s.append(", KNOB-").append(knob).append(" = ").append(value);
		}
		return s;
	}

	void updateKnobs() {
		auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
		for (const auto& [knobName, knobValueString] : knobs) {
			try {
				auto knobValue = g_knobs.parseKnobValue(knobName, knobValueString);
				g_knobs.setKnob(knobName, knobValue);
			} catch (Error& e) {
				if (e.code() == error_code_invalid_option_value) {
					std::cerr << "WARNING: Invalid value '" << knobValueString << "' for knob option '" << knobName
					          << "'\n";
					TraceEvent(SevWarnAlways, "InvalidKnobValue")
					    .detail("Knob", printable(knobName))
					    .detail("Value", printable(knobValueString));
				} else {
					std::cerr << "ERROR: Failed to set knob option '" << knobName << "': " << e.what() << "\n";
					TraceEvent(SevError, "FailedToSetKnob")
					    .errorUnsuppressed(e)
					    .detail("Knob", printable(knobName))
					    .detail("Value", printable(knobValueString));
					throw;
				}
			}
		}

		// Reinitialize knobs in order to update knobs that are dependent on explicitly set knobs
		g_knobs.initialize(Randomize::True, IsSimulated::False);
	}
};

// Decode an ASCII string, e.g., "\x15\x1b\x19\x04\xaf\x0c\x28\x0a",
// into the binary string.
std::string decode_hex_string(std::string line) {
	size_t i = 0;
	std::string ret;

	while (i <= line.length()) {
		switch (line[i]) {
		case '\\':
			if (i + 2 > line.length()) {
				std::cerr << "Invalid hex string at: " << i << "\n";
				return ret;
			}
			switch (line[i + 1]) {
				char ent, save;
			case '"':
			case '\\':
			case ' ':
			case ';':
				line.erase(i, 1);
				break;
			case 'x':
				if (i + 4 > line.length()) {
					std::cerr << "Invalid hex string at: " << i << "\n";
					return ret;
				}
				char* pEnd;
				save = line[i + 4];
				line[i + 4] = 0;
				ent = char(strtoul(line.data() + i + 2, &pEnd, 16));
				if (*pEnd) {
					std::cerr << "Invalid hex string at: " << i << "\n";
					return ret;
				}
				line[i + 4] = save;
				line.replace(i, 4, 1, ent);
				break;
			default:
				std::cerr << "Invalid hex string at: " << i << "\n";
				return ret;
			}
		default:
			i++;
		}
	}

	return line.substr(0, i);
}

int parseDecodeCommandLine(DecodeParams* param, CSimpleOpt* args) {
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

		case OPT_CONTAINER:
			param->container_url = args->OptionArg();
			break;

		case OPT_LIST_ONLY:
			param->list_only = true;
			break;

		case OPT_KEY_PREFIX:
			param->prefix = args->OptionArg();
			break;

		case OPT_HEX_KEY_PREFIX:
			param->prefix = decode_hex_string(args->OptionArg());
			break;

		case OPT_BEGIN_VERSION_FILTER:
			param->beginVersionFilter = std::atoll(args->OptionArg());
			break;

		case OPT_END_VERSION_FILTER:
			param->endVersionFilter = std::atoll(args->OptionArg());
			break;

		case OPT_CRASHONERROR:
			g_crashOnError = true;
			break;

		case OPT_INPUT_FILE:
			param->fileFilter = args->OptionArg();
			break;

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

#ifndef TLS_DISABLED
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
#endif

		case OPT_BUILD_FLAGS:
			printBuildInformation();
			return FDB_EXIT_ERROR;
			break;
		}
	}
	return FDB_EXIT_SUCCESS;
}

void printLogFiles(std::string msg, const std::vector<LogFile>& files) {
	std::cout << msg << " " << files.size() << " log files\n";
	for (const auto& file : files) {
		std::cout << file.toString() << "\n";
	}
	std::cout << std::endl;
}

std::vector<LogFile> getRelevantLogFiles(const std::vector<LogFile>& files, const DecodeParams& params) {
	std::vector<LogFile> filtered;
	for (const auto& file : files) {
		if (file.fileName.find(params.fileFilter) != std::string::npos &&
		    params.overlap(file.beginVersion, file.endVersion + 1)) {
			filtered.push_back(file);
		}
	}
	return filtered;
}

struct VersionedMutations {
	Version version;
	std::vector<MutationRef> mutations;
	std::string serializedMutations; // buffer that contains mutations
};

/*
 * Model a decoding progress for a mutation file. Usage is:
 *
 *    DecodeProgress progress(logfile);
 *    wait(progress->openFile(container));
 *    while (!progress->finished()) {
 *        VersionedMutations m = wait(progress->getNextBatch());
 *        ...
 *    }
 *
 * Internally, the decoding process is done block by block -- each block is
 * decoded into a list of key/value pairs, which are then decoded into batches
 * of mutations. Because a version's mutations can be split into many key/value
 * pairs, the decoding of mutation batch needs to look ahead one more pair. So
 * at any time this object might have two blocks of data in memory.
 */
class DecodeProgress {
	std::vector<Standalone<VectorRef<KeyValueRef>>> blocks;
	std::unordered_map<Version, fileBackup::AccumulatedMutations> mutationBlocksByVersion;

public:
	DecodeProgress() = default;
	DecodeProgress(const LogFile& file) : file(file) {}

	// If there are no more mutations to pull from the file.
	bool finished() const { return done; }

	// Open and loads file into memory
	Future<Void> openFile(Reference<IBackupContainer> container) { return openFileImpl(this, container); }

	// The following are private APIs:

	// PRECONDITION: finished() must return false before calling this function.
	// Returns the next batch of mutations along with the arena backing it.
	// Note the returned batch can be empty when the file has unfinished
	// version batch data that are in the next file.
	VersionedMutations getNextBatch() {
		ASSERT(!finished());

		VersionedMutations vms;
		for (auto& [version, m] : mutationBlocksByVersion) {
			if (m.isComplete()) {
				vms.version = version;
				std::vector<MutationRef> mutations = fileBackup::decodeMutationLogValue(m.serializedMutations);
				TraceEvent("Decode").detail("Version", vms.version).detail("N", mutations.size());
				vms.mutations.insert(vms.mutations.end(), mutations.begin(), mutations.end());
				vms.serializedMutations = m.serializedMutations;
				mutationBlocksByVersion.erase(version);
				return vms;
			}
		}

		// No complete versions
		if (!mutationBlocksByVersion.empty()) {
			TraceEvent(SevWarn, "UnfishedBlocks").detail("NumberOfVersions", mutationBlocksByVersion.size());
		}
		done = true;
		return vms;
	}

	ACTOR static Future<Void> openFileImpl(DecodeProgress* self, Reference<IBackupContainer> container) {
		Reference<IAsyncFile> fd = wait(container->readFile(self->file.fileName));
		self->fd = fd;
		while (!self->eof) {
			wait(readAndDecodeFile(self));
		}
		return Void();
	}

	// Add chunks to mutationBlocksByVersion
	void addBlockKVPairs(VectorRef<KeyValueRef> chunks) {
		for (auto& kv : chunks) {
			auto versionAndChunkNumber = fileBackup::decodeMutationLogKey(kv.key);
			mutationBlocksByVersion[versionAndChunkNumber.first].addChunk(versionAndChunkNumber.second, kv);
		}
	}

	// Reads a file block, decodes it into key/value pairs, and stores these pairs.
	ACTOR static Future<Void> readAndDecodeFile(DecodeProgress* self) {
		try {
			state int64_t len = std::min<int64_t>(self->file.blockSize, self->file.fileSize - self->offset);
			if (len == 0) {
				self->eof = true;
				return Void();
			}

			// Decode a file block into log_key and log_value chunks
			Standalone<VectorRef<KeyValueRef>> chunks =
			    wait(fileBackup::decodeMutationLogFileBlock(self->fd, self->offset, len));
			self->blocks.push_back(chunks);

			TraceEvent("ReadFile")
			    .detail("Name", self->file.fileName)
			    .detail("Len", len)
			    .detail("Offset", self->offset);
			self->addBlockKVPairs(chunks);
			self->offset += len;

			return Void();
		} catch (Error& e) {
			TraceEvent(SevWarn, "CorruptLogFileBlock")
			    .error(e)
			    .detail("Filename", self->file.fileName)
			    .detail("BlockOffset", self->offset)
			    .detail("BlockLen", self->file.blockSize);
			throw;
		}
	}

	LogFile file;
	Reference<IAsyncFile> fd;
	int64_t offset = 0;
	bool eof = false;
	bool done = false;
};

ACTOR Future<Void> process_file(Reference<IBackupContainer> container, LogFile file, UID uid, DecodeParams params) {
	if (file.fileSize == 0) {
		TraceEvent("SkipEmptyFile", uid).detail("Name", file.fileName);
		return Void();
	}

	state DecodeProgress progress(file);
	wait(progress.openFile(container));
	while (!progress.finished()) {
		VersionedMutations vms = progress.getNextBatch();
		if (vms.version < params.beginVersionFilter || vms.version >= params.endVersionFilter) {
			TraceEvent("SkipVersion").detail("Version", vms.version);
			continue;
		}

		int sub = 0;
		for (const auto& m : vms.mutations) {
			sub++; // sub sequence number starts at 1
			bool print = params.prefix.empty(); // no filtering

			if (!print) {
				if (isSingleKeyMutation((MutationRef::Type)m.type)) {
					print = m.param1.startsWith(StringRef(params.prefix));
				} else if (m.type == MutationRef::ClearRange) {
					KeyRange range(KeyRangeRef(m.param1, m.param2));
					KeyRange range2 = prefixRange(StringRef(params.prefix));
					print = range.intersects(range2);
				} else {
					ASSERT(false);
				}
			}
			if (print) {
				TraceEvent(format("Mutation_%llu_%d", vms.version, sub).c_str(), uid)
				    .detail("Version", vms.version)
				    .setMaxFieldLength(10000)
				    .detail("M", m.toString());
				std::cout << vms.version << " " << m.toString() << "\n";
			}
		}
	}
	TraceEvent("ProcessFileDone", uid).detail("File", file.fileName);
	return Void();
}

ACTOR Future<Void> decode_logs(DecodeParams params) {
	state Reference<IBackupContainer> container =
	    IBackupContainer::openContainer(params.container_url, params.proxy, {});
	state UID uid = deterministicRandom()->randomUniqueID();
	state BackupFileList listing = wait(container->dumpFileList());
	// remove partitioned logs
	listing.logs.erase(std::remove_if(listing.logs.begin(),
	                                  listing.logs.end(),
	                                  [](const LogFile& file) {
		                                  std::string prefix("plogs/");
		                                  return file.fileName.substr(0, prefix.size()) == prefix;
	                                  }),
	                   listing.logs.end());
	std::sort(listing.logs.begin(), listing.logs.end());
	TraceEvent("Container", uid).detail("URL", params.container_url).detail("Logs", listing.logs.size());
	TraceEvent("DecodeParam", uid).setMaxFieldLength(100000).detail("Value", params.toString());

	BackupDescription desc = wait(container->describeBackup());
	std::cout << "\n" << desc.toString() << "\n";

	state std::vector<LogFile> logs = getRelevantLogFiles(listing.logs, params);
	printLogFiles("Relevant files are: ", logs);

	if (params.list_only)
		return Void();

	state int idx = 0;
	while (idx < logs.size()) {
		TraceEvent("ProcessFile").detail("Name", logs[idx].fileName).detail("I", idx);
		wait(process_file(container, logs[idx], uid, params));
		idx++;
	}
	TraceEvent("DecodeDone", uid).log();
	return Void();
}

} // namespace file_converter

int main(int argc, char** argv) {
	try {
		CSimpleOpt* args =
		    new CSimpleOpt(argc, argv, file_converter::gConverterOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
		file_converter::DecodeParams param;
		int status = file_converter::parseDecodeCommandLine(&param, args);
		std::cout << "Params: " << param.toString() << "\n";
		if (status != FDB_EXIT_SUCCESS) {
			file_converter::printDecodeUsage();
			return status;
		}

		if (param.log_enabled) {
			if (param.log_dir.empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_ENABLE);
			} else {
				setNetworkOption(FDBNetworkOptions::TRACE_ENABLE, StringRef(param.log_dir));
			}
			if (!param.trace_format.empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_FORMAT, StringRef(param.trace_format));
			} else {
				setNetworkOption(FDBNetworkOptions::TRACE_FORMAT, "json"_sr);
			}
			if (!param.trace_log_group.empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_LOG_GROUP, StringRef(param.trace_log_group));
			}
		}

		if (!param.tlsConfig.setupTLS()) {
			TraceEvent(SevError, "TLSError").log();
			throw tls_error();
		}

		platformInit();
		Error::init();

		StringRef url(param.container_url);
		setupNetwork(0, UseMetrics::True);

		// Must be called after setupNetwork() to be effective
		param.updateKnobs();

		TraceEvent::setNetworkThread();
		openTraceFile(NetworkAddress(), 10 << 20, 500 << 20, param.log_dir, "decode", param.trace_log_group);
		param.tlsConfig.setupBlobCredentials();

		auto f = stopAfter(decode_logs(param));

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
