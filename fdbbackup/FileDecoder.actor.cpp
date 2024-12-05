/*
 * FileDecoder.actor.cpp
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

#ifdef _WIN32
#include <io.h>
#endif

#include "fdbclient/BackupTLSConfig.h"
#include "fdbclient/BuildFlags.h"
#include "fdbbackup/FileConverter.h"
#include "fdbbackup/Decode.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BuildFlags.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/versions.h"
#include "flow/ArgParseUtil.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/serialize.h"

#include "flow/actorcompiler.h" // has to be last include

#define SevDecodeInfo SevVerbose

extern bool g_crashOnError;
extern const char* getSourceVersion();

namespace file_converter {

void printDecodeUsage() {
	std::cout << "Decoder for FoundationDB backup mutation logs.\n"
	             "Usage: fdbdecode  [OPTIONS]\n"
	             "  -r, --container URL\n"
	             "                 Backup container URL, e.g., file:///some/path/.\n"
	             "  -i, --input    FILE\n"
	             "                 Log file filter, only matched files are decoded.\n"
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
	             "  --crash        Crash on serious error.\n"
	             "  --blob-credentials FILE\n"
	             "                 File containing blob credentials in JSON format.\n"
	             "                 The same credential format/file fdbbackup uses.\n" TLS_HELP
	             "  -t, --file-type [log|range|both]\n"
	             "                 Specifies the backup file type to decode.\n"
	             "  --build-flags  Print build information and exit.\n"
	             "  --list-only    Print file list and exit.\n"
	             "  --validate-filters Validate the default RangeMap filtering logic with a slower one.\n"
	             "  -k KEY_PREFIX  Use a single prefix for filtering mutations.\n"
	             "  --filters PREFIX_FILTER_FILE\n"
	             "                 A file containing a list of prefix filters in HEX format separated by \";\",\n"
	             "                 e.g., \"\\x05\\x01;\\x15\\x2b\"\n"
	             "  --hex-prefix   HEX_PREFIX\n"
	             "                 The prefix specified in HEX format, e.g., --hex-prefix \"\\\\x05\\\\x01\".\n"
	             "  --begin-version-filter BEGIN_VERSION\n"
	             "                 The version range's begin version (inclusive) for filtering.\n"
	             "  --end-version-filter END_VERSION\n"
	             "                 The version range's end version (exclusive) for filtering.\n"
	             "  --knob-KNOBNAME KNOBVALUE\n"
	             "                 Changes a knob value. KNOBNAME should be lowercase.\n"
	             "  -s, --save     Save a copy of downloaded files (default: not saving).\n"
	             "\n";
	return;
}

void printBuildInformation() {
	std::cout << jsonBuildInformation() << "\n";
}

struct DecodeParams : public ReferenceCounted<DecodeParams> {
	std::string container_url;
	Optional<std::string> proxy;
	std::string fileFilter; // only files match the filter will be decoded
	bool log_enabled = true;
	std::string log_dir, trace_format, trace_log_group;
	BackupTLSConfig tlsConfig;
	bool list_only = false;
	bool decode_logs = true;
	bool decode_range = true;
	bool save_file_locally = false;
	bool validate_filters = false;
	std::vector<std::string> prefixes; // Key prefixes for filtering
	// more efficient data structure for intersection queries than "prefixes"
	fileBackup::RangeMapFilters filters;
	Version beginVersionFilter = 0;
	Version endVersionFilter = std::numeric_limits<Version>::max();

	std::vector<std::pair<std::string, std::string>> knobs;

	// Returns if [begin, end) overlap with the filter range
	bool overlap(Version begin, Version end) const {
		// Filter [100, 200),  [50,75) [200, 300)
		return !(begin >= endVersionFilter || end <= beginVersionFilter);
	}

	bool overlap(Version version) const { return version >= beginVersionFilter && version < endVersionFilter; }

	void updateRangeMap() { filters.updateFilters(prefixes); }

	bool matchFilters(const MutationRef& m) const {
		bool match = filters.match(m);
		if (!validate_filters) {
			return match;
		}

		// If we choose to validate the filters, go through filters one by one
		for (const auto& prefix : prefixes) {
			if (isSingleKeyMutation((MutationRef::Type)m.type)) {
				if (m.param1.startsWith(StringRef(prefix))) {
					ASSERT(match);
					return true;
				}
			} else if (m.type == MutationRef::ClearRange) {
				KeyRange range(KeyRangeRef(m.param1, m.param2));
				KeyRange range2 = prefixRange(StringRef(prefix));
				if (range.intersects(range2)) {
					ASSERT(match);
					return true;
				}
			} else {
				ASSERT(false);
			}
		}
		ASSERT(!match);
		return false;
	}

	bool matchFilters(const KeyRange& range) const {
		bool match = filters.match(range);
		if (!validate_filters) {
			return match;
		}

		for (const auto& prefix : prefixes) {
			if (range.intersects(prefixRange(StringRef(prefix)))) {
				ASSERT(match);
				return true;
			}
		}
		return false;
	}

	bool matchFilters(KeyValueRef kv) const {
		bool match = filters.match(kv);

		if (!validate_filters) {
			return match;
		}

		for (const auto& prefix : prefixes) {
			if (kv.key.startsWith(StringRef(prefix))) {
				ASSERT(match);
				return true;
			}
		}

		return match;
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
		s.append(", validate_filters: ").append(validate_filters ? "true" : "false");
		if (beginVersionFilter != 0) {
			s.append(", beginVersionFilter: ").append(std::to_string(beginVersionFilter));
		}
		if (endVersionFilter < std::numeric_limits<Version>::max()) {
			s.append(", endVersionFilter: ").append(std::to_string(endVersionFilter));
		}
		if (!prefixes.empty()) {
			s.append(", KeyPrefixes: ").append(printable(describe(prefixes)));
		}
		for (const auto& [knob, value] : knobs) {
			s.append(", KNOB-").append(knob).append(" = ").append(value);
		}
		s.append(", SaveFile: ").append(save_file_locally ? "true" : "false");
		return s;
	}

	void updateKnobs() {
		IKnobCollection::setupKnobs(knobs);

		// Reinitialize knobs in order to update knobs that are dependent on explicitly set knobs
		IKnobCollection::getMutableGlobalKnobCollection().initialize(Randomize::False, IsSimulated::False);
	}
};

// Parses and returns a ";" separated HEX encoded strings. So the ";" in
// the string should be escaped as "\;".
// Sets "err" to true if there is any parsing error.
std::vector<std::string> parsePrefixesLine(const std::string& line, bool& err) {
	std::vector<std::string> results;
	err = false;

	int p = 0;
	while (p < line.size()) {
		int end = line.find_first_of(';', p);
		if (end == line.npos) {
			end = line.size();
		}
		auto prefix = decode_hex_string(line.substr(p, end - p), err);
		if (err) {
			return results;
		}
		results.push_back(prefix);
		p = end + 1;
	}
	return results;
}

std::vector<std::string> parsePrefixFile(const std::string& filename, bool& err) {
	std::string line = readFileBytes(filename, 64 * 1024 * 1024);
	return parsePrefixesLine(line, err);
}

int parseDecodeCommandLine(Reference<DecodeParams> param, CSimpleOpt* args) {
	bool err = false;

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

		case OPT_FILE_TYPE: {
			auto ftype = std::string(args->OptionArg());
			if (ftype == "log") {
				param->decode_range = false;
			} else if (ftype == "range") {
				param->decode_logs = false;
			} else if (ftype != "both" && ftype != "") {
				err = true;
				std::cerr << "ERROR: Unrecognized backup file type option: " << args->OptionArg() << "\n";
				return FDB_EXIT_ERROR;
			}
			break;
		}

		case OPT_LIST_ONLY:
			param->list_only = true;
			break;

		case OPT_VALIDATE_FILTERS:
			param->validate_filters = true;
			break;

		case OPT_KEY_PREFIX:
			param->prefixes.push_back(args->OptionArg());
			break;

		case OPT_FILTERS:
			param->prefixes = parsePrefixFile(args->OptionArg(), err);
			if (err) {
				throw std::runtime_error("ERROR:" + std::string(args->OptionArg()) + "contains invalid prefix(es)");
			}
			break;

		case OPT_HEX_KEY_PREFIX:
			param->prefixes.push_back(decode_hex_string(args->OptionArg(), err));
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

		case OPT_SAVE_FILE:
			param->save_file_locally = true;
			break;

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
	return FDB_EXIT_SUCCESS;
}

template <class BackupFile>
void printLogFiles(std::string msg, const std::vector<BackupFile>& files) {
	std::cout << msg << " " << files.size() << " total\n";
	for (const auto& file : files) {
		std::cout << file.toString() << "\n";
	}
	std::cout << std::endl;
}

std::vector<LogFile> getRelevantLogFiles(const std::vector<LogFile>& files, const Reference<DecodeParams> params) {
	std::vector<LogFile> filtered;
	for (const auto& file : files) {
		if (file.fileName.find(params->fileFilter) != std::string::npos &&
		    params->overlap(file.beginVersion, file.endVersion + 1)) {
			filtered.push_back(file);
		}
	}
	return filtered;
}

std::vector<RangeFile> getRelevantRangeFiles(const std::vector<RangeFile>& files,
                                             const Reference<DecodeParams> params) {
	std::vector<RangeFile> filtered;
	for (const auto& file : files) {
		if (file.fileName.find(params->fileFilter) != std::string::npos && params->overlap(file.version)) {
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
 *    while (1) {
 *        Optional<VersionedMutations> batch = wait(progress->getNextBatch());
 *        if (!batch.present()) break;
 *        ... // process the batch mutations
 *    }
 *
 * Internally, the decoding process is done block by block -- each block is
 * decoded into a list of key/value pairs, which are then decoded into batches
 * of mutations. Because a version's mutations can be split into many key/value
 * pairs, the decoding of mutation needs to look ahead to find all batches that
 * belong to the same version.
 */
class DecodeProgress {
	std::vector<Standalone<VectorRef<KeyValueRef>>> blocks;
	std::unordered_map<Version, fileBackup::AccumulatedMutations> mutationBlocksByVersion;

public:
	DecodeProgress() = default;
	DecodeProgress(const LogFile& file, bool save) : file(file), save(save) {}

	~DecodeProgress() {
		if (lfd != -1) {
			close(lfd);
		}
	}

	// Open and loads file into memory
	Future<Void> openFile(Reference<IBackupContainer> container) { return openFileImpl(this, container); }

	// The following are private APIs:

	// Returns the next batch of mutations along with the arena backing it.
	// Note the returned batch can be empty when the file has unfinished
	// version batch data that are in the next file.
	Optional<VersionedMutations> getNextBatch() {
		for (auto& [version, m] : mutationBlocksByVersion) {
			if (m.isComplete()) {
				VersionedMutations vms;
				vms.version = version;
				vms.serializedMutations = m.serializedMutations;
				vms.mutations = fileBackup::decodeMutationLogValue(vms.serializedMutations);
				TraceEvent("Decode").detail("Version", vms.version).detail("N", vms.mutations.size());
				mutationBlocksByVersion.erase(version);
				return vms;
			}
		}

		// No complete versions
		if (!mutationBlocksByVersion.empty()) {
			TraceEvent(SevWarn, "UnfishedBlocks").detail("NumberOfVersions", mutationBlocksByVersion.size());
		}
		return Optional<VersionedMutations>();
	}

	ACTOR static Future<Void> openFileImpl(DecodeProgress* self, Reference<IBackupContainer> container) {
		Reference<IAsyncFile> fd = wait(container->readFile(self->file.fileName));
		self->fd = fd;
		state Standalone<StringRef> buf = makeString(self->file.fileSize);
		int rLen = wait(self->fd->read(mutateString(buf), self->file.fileSize, 0));
		if (rLen != self->file.fileSize) {
			throw restore_bad_read();
		}

		if (self->save) {
			std::string dir = self->file.fileName;
			std::size_t found = self->file.fileName.find_last_of('/');
			if (found != std::string::npos) {
				std::string path = self->file.fileName.substr(0, found);
				if (!directoryExists(path)) {
					platform::createDirectory(path);
				}
			}
			self->lfd = open(self->file.fileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
			if (self->lfd == -1) {
				TraceEvent(SevError, "OpenLocalFileFailed").detail("File", self->file.fileName);
				throw platform_error();
			}
			int wlen = write(self->lfd, buf.begin(), self->file.fileSize);
			if (wlen != self->file.fileSize) {
				TraceEvent(SevError, "WriteLocalFileFailed")
				    .detail("File", self->file.fileName)
				    .detail("Len", self->file.fileSize);
				throw platform_error();
			}
			TraceEvent("WriteLocalFile").detail("Name", self->file.fileName).detail("Len", self->file.fileSize);
		}

		self->decodeFile(buf);
		return Void();
	}

	// Add chunks to mutationBlocksByVersion
	void addBlockKVPairs(VectorRef<KeyValueRef> chunks) {
		for (auto& kv : chunks) {
			auto versionAndChunkNumber = fileBackup::decodeMutationLogKey(kv.key);
			mutationBlocksByVersion[versionAndChunkNumber.first].addChunk(versionAndChunkNumber.second, kv);
		}
	}

	// Reads a file a file content in the buffer, decodes it into key/value pairs, and stores these pairs.
	void decodeFile(const Standalone<StringRef>& buf) {
		try {
			loop {
				int64_t len = std::min<int64_t>(file.blockSize, file.fileSize - offset);
				if (len == 0) {
					return;
				}

				// Decode a file block into log_key and log_value chunks
				Standalone<VectorRef<KeyValueRef>> chunks =
				    fileBackup::decodeMutationLogFileBlock(buf.substr(offset, len));
				blocks.push_back(chunks);
				addBlockKVPairs(chunks);
				offset += len;
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "CorruptLogFileBlock")
			    .error(e)
			    .detail("Filename", file.fileName)
			    .detail("BlockOffset", offset)
			    .detail("BlockLen", file.blockSize);
			throw;
		}
	}

	LogFile file;
	Reference<IAsyncFile> fd;
	int64_t offset = 0;
	bool eof = false;
	bool save = false;
	int lfd = -1; // local file descriptor
};

class DecodeRangeProgress {
public:
	std::vector<Standalone<VectorRef<KeyValueRef>>> blocks;

	DecodeRangeProgress() = default;
	DecodeRangeProgress(const RangeFile& file, bool save) : file(file), save(save) {}
	~DecodeRangeProgress() {
		if (lfd != -1) {
			close(lfd);
		}
	}

	// Open and loads file into memory
	Future<Void> openFile(Reference<IBackupContainer> container) { return openFileImpl(this, container); }

	ACTOR static Future<Void> openFileImpl(DecodeRangeProgress* self, Reference<IBackupContainer> container) {
		TraceEvent("ReadFile").detail("Name", self->file.fileName).detail("Len", self->file.fileSize);

		Reference<IAsyncFile> fd = wait(container->readFile(self->file.fileName));
		self->fd = fd;
		state Standalone<StringRef> buf = makeString(self->file.fileSize);
		int rLen = wait(self->fd->read(mutateString(buf), self->file.fileSize, 0));
		if (rLen != self->file.fileSize) {
			throw restore_bad_read();
		}

		if (self->save) {
			std::string dir = self->file.fileName;
			std::size_t found = self->file.fileName.find_last_of('/');
			if (found != std::string::npos) {
				std::string path = self->file.fileName.substr(0, found);
				if (!directoryExists(path)) {
					platform::createDirectory(path);
				}
			}

			self->lfd = open(self->file.fileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
			if (self->lfd == -1) {
				TraceEvent(SevError, "OpenLocalFileFailed").detail("File", self->file.fileName);
				throw platform_error();
			}
			int wlen = write(self->lfd, buf.begin(), self->file.fileSize);
			if (wlen != self->file.fileSize) {
				TraceEvent(SevError, "WriteLocalFileFailed")
				    .detail("File", self->file.fileName)
				    .detail("Len", self->file.fileSize);
				throw platform_error();
			}
			TraceEvent("WriteLocalFile").detail("Name", self->file.fileName).detail("Len", self->file.fileSize);
		}

		self->decodeFile(buf);
		return Void();
	}

	// Reads a file content in the buffer, decodes it into key/value pairs, and stores these pairs.
	void decodeFile(const Standalone<StringRef>& buf) {
		try {
			loop {
				// process one block at a time
				int64_t len = std::min<int64_t>(file.blockSize, file.fileSize - offset);
				if (len == 0) {
					return;
				}

				Standalone<VectorRef<KeyValueRef>> chunks = fileBackup::decodeRangeFileBlock(buf.substr(offset, len));
				blocks.push_back(chunks);
				offset += len;
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "CorruptRangeFileBlock")
			    .error(e)
			    .detail("Filename", file.fileName)
			    .detail("BlockOffset", offset)
			    .detail("BlockLen", file.blockSize);
			throw;
		}
	}

	RangeFile file;
	Reference<IAsyncFile> fd;
	int64_t offset = 0;
	bool save = false;
	int lfd = -1; // local file descriptor
};

// convert a StringRef to Hex string
std::string hexStringRef(const StringRef& s) {
	std::string result;
	result.reserve(s.size() * 2);
	for (int i = 0; i < s.size(); i++) {
		result.append(format("%02x", s[i]));
	}
	return result;
}

ACTOR Future<Void> process_range_file(Reference<IBackupContainer> container,
                                      RangeFile file,
                                      UID uid,
                                      Reference<DecodeParams> params) {

	if (file.fileSize == 0) {
		TraceEvent("SkipEmptyFile", uid).detail("Name", file.fileName);
		return Void();
	}

	state DecodeRangeProgress progress(file, params->save_file_locally);
	wait(progress.openFile(container));

	for (auto& block : progress.blocks) {
		for (const auto& kv : block) {
			bool print = params->prefixes.empty(); // no filtering

			if (!print) {
				print = params->matchFilters(kv);
			}

			if (print) {
				TraceEvent(format("KVPair_%llu", file.version).c_str(), uid)
				    .detail("Version", file.version)
				    .setMaxFieldLength(1000)
				    .detail("KV", kv);
				std::cout << file.version << " key: " << hexStringRef(kv.key) << "  value: " << hexStringRef(kv.value)
				          << std::endl;
			}
		}
	}
	TraceEvent("ProcessRangeFileDone", uid).detail("File", file.fileName);

	return Void();
}

ACTOR Future<Void> process_file(Reference<IBackupContainer> container,
                                LogFile file,
                                UID uid,
                                Reference<DecodeParams> params) {
	if (file.fileSize == 0) {
		TraceEvent("SkipEmptyFile", uid).detail("Name", file.fileName);
		return Void();
	}

	state DecodeProgress progress(file, params->save_file_locally);
	wait(progress.openFile(container));
	while (true) {
		auto batch = progress.getNextBatch();
		if (!batch.present())
			break;

		const VersionedMutations& vms = batch.get();
		if (vms.version < params->beginVersionFilter || vms.version >= params->endVersionFilter) {
			TraceEvent("SkipVersion").detail("Version", vms.version);
			continue;
		}

		int sub = 0;
		for (const auto& m : vms.mutations) {
			sub++; // sub sequence number starts at 1
			bool print = params->prefixes.empty(); // no filtering

			if (!print) {
				print = params->matchFilters(m);
			}
			if (print) {
				TraceEvent(format("Mutation_%llu_%d", vms.version, sub).c_str(), uid)
				    .detail("Version", vms.version)
				    .setMaxFieldLength(1000)
				    .detail("M", m.toString());
				std::cout << vms.version << "." << sub << " " << typeString[(int)m.type]
				          << " param1: " << hexStringRef(m.param1) << " param2: " << hexStringRef(m.param2) << "\n";
			}
		}
	}
	TraceEvent("ProcessFileDone", uid).detail("File", file.fileName);
	return Void();
}

// Use the snapshot metadata to quickly identify relevant range files and
// then filter by versions.
ACTOR Future<std::vector<RangeFile>> getRangeFiles(Reference<IBackupContainer> bc, Reference<DecodeParams> params) {
	state std::vector<KeyspaceSnapshotFile> snapshots =
	    wait((dynamic_cast<BackupContainerFileSystem*>(bc.getPtr()))->listKeyspaceSnapshots());
	state std::vector<RangeFile> files;

	state int i = 0;
	for (; i < snapshots.size(); i++) {
		try {
			std::pair<std::vector<RangeFile>, std::map<std::string, KeyRange>> results =
			    wait((dynamic_cast<BackupContainerFileSystem*>(bc.getPtr()))->readKeyspaceSnapshot(snapshots[i]));
			for (const auto& rangeFile : results.first) {
				const auto& keyRange = results.second.at(rangeFile.fileName);
				if (params->matchFilters(keyRange)) {
					files.push_back(rangeFile);
				}
			}
		} catch (Error& e) {
			TraceEvent("ReadKeyspaceSnapshotError").error(e).detail("I", i);
			if (e.code() != error_code_restore_missing_data) {
				throw;
			}
		}
	}
	return getRelevantRangeFiles(files, params);
}

ACTOR Future<Void> decode_logs(Reference<DecodeParams> params) {
	state Reference<IBackupContainer> container =
	    IBackupContainer::openContainer(params->container_url, params->proxy, {});
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
	TraceEvent("Container", uid).detail("URL", params->container_url).detail("Logs", listing.logs.size());
	TraceEvent("DecodeParam", uid).setMaxFieldLength(100000).detail("Value", params->toString());

	BackupDescription desc = wait(container->describeBackup());
	std::cout << "\n" << desc.toString() << "\n";

	state std::vector<LogFile> logFiles;
	state std::vector<RangeFile> rangeFiles;

	if (params->decode_logs) {
		logFiles = getRelevantLogFiles(listing.logs, params);
		printLogFiles("Relevant log files are: ", logFiles);
	}

	if (params->decode_range) {
		// rangeFiles = getRelevantRangeFiles(filteredRangeFiles, params);
		std::vector<RangeFile> files = wait(getRangeFiles(container, params));
		rangeFiles = files;
		printLogFiles("Relevant range files are: ", rangeFiles);
	}

	TraceEvent("TotalFiles", uid).detail("LogFiles", logFiles.size()).detail("RangeFiles", rangeFiles.size());

	if (params->list_only)
		return Void();

	// Decode log files.
	state int idx = 0;
	if (params->decode_logs) {
		while (idx < logFiles.size()) {
			TraceEvent("ProcessFile").detail("Name", logFiles[idx].fileName).detail("I", idx);
			wait(process_file(container, logFiles[idx], uid, params));
			idx++;
		}
		TraceEvent("DecodeLogsDone", uid).log();
	}

	// Decode range files.
	if (params->decode_range) {
		idx = 0;
		while (idx < rangeFiles.size()) {
			TraceEvent("ProcessFile").detail("Name", rangeFiles[idx].fileName).detail("I", idx);
			wait(process_range_file(container, rangeFiles[idx], uid, params));
			idx++;
		}
		TraceEvent("DecodeRangeFileDone", uid).log();
	}

	return Void();
}

} // namespace file_converter

int main(int argc, char** argv) {
	std::string commandLine;
	for (int a = 0; a < argc; a++) {
		if (a)
			commandLine += ' ';
		commandLine += argv[a];
	}

	try {
		std::unique_ptr<CSimpleOpt> args(
		    new CSimpleOpt(argc, argv, file_converter::gConverterOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE));
		auto param = makeReference<file_converter::DecodeParams>();
		int status = file_converter::parseDecodeCommandLine(param, args.get());
		std::cout << "Params: " << param->toString() << "\n";
		param->updateRangeMap();
		if (status != FDB_EXIT_SUCCESS) {
			file_converter::printDecodeUsage();
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

		StringRef url(param->container_url);
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
		openTraceFile({}, 10 << 20, 500 << 20, param->log_dir, "decode", param->trace_log_group);
		param->tlsConfig.setupBlobCredentials();

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
