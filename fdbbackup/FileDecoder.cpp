/*
 * FileDecoder.cpp
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

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <map>
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
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupFileFormat.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
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
	             "  -k KEY_PREFIX  Use a single prefix for filtering mutations and range files.\n"
	             "  --filters PREFIX_FILTER_FILE\n"
	             "                 A file containing a list of prefix filters in HEX format separated by \";\",\n"
	             "                 e.g., \"\\x05\\x01;\\x15\\x2b\"\n"
	             "  --hex-prefix   HEX_PREFIX\n"
	             "                 The prefix specified in HEX format, e.g., --hex-prefix \"\\\\x05\\\\x01\".\n"
	             "                 With none of -k, --filters or --hex-prefix, everything is decoded.\n"
	             "  --begin-version-filter BEGIN_VERSION\n"
	             "                 The version range's begin version (inclusive) for filtering.\n"
	             "  --end-version-filter END_VERSION\n"
	             "                 The version range's end version (exclusive) for filtering.\n"
	             "  --knob-KNOBNAME KNOBVALUE\n"
	             "                 Changes a knob value. KNOBNAME should be lowercase.\n"
	             "  -s, --save     Save a copy of downloaded files (default: not saving).\n"
	             "  --encryption-key-file FILE\n"
	             "                 AES-128-GCM encryption key file for encrypted backups.\n"
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
	Optional<std::string> encryptionKeyFileName;

	// Returns if [begin, end) overlap with the filter range
	bool overlap(Version begin, Version end) const {
		// Filter [100, 200),  [50,75) [200, 300)
		return !(begin >= endVersionFilter || end <= beginVersionFilter);
	}

	bool overlap(Version version) const { return version >= beginVersionFilter && version < endVersionFilter; }

	bool validVersionFilters() { return beginVersionFilter < endVersionFilter; }

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
		if (encryptionKeyFileName.present()) {
			s.append(", EncryptionKeyFile: ").append(encryptionKeyFileName.get());
		}
		return s;
	}

	void updateKnobs() {
		setupClientKnobs(knobs);

		// Reinitialize knobs in order to update knobs that are dependent on explicitly set knobs
		initializeClientKnobs(Randomize::False, IsSimulated::False);
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
		// Newlines separate entries as well as ';', so a file with one prefix per line parses as intended
		// rather than as a single prefix with embedded newline bytes.
		int end = line.find_first_of(";\n", p);
		if (end == line.npos) {
			end = line.size();
		}
		std::string token = line.substr(p, end - p);
		size_t first = token.find_first_not_of(" \t\r\n");
		p = end + 1;
		if (first == std::string::npos) {
			continue; // blank entry, e.g. a trailing newline or ";;"
		}
		token = token.substr(first, token.find_last_not_of(" \t\r\n") - first + 1);
		auto prefix = decode_hex_string(token, err);
		if (err) {
			return results;
		}
		results.push_back(prefix);
	}
	return results;
}

std::vector<std::string> parsePrefixFile(const std::string& filename, bool& err) {
	std::string line = readFileBytes(filename, size_t{ 64 } * 1024 * 1024);
	return parsePrefixesLine(line, err);
}

// Parses a non-negative decimal version, rejecting trailing garbage and overflow that atoll ignores.
bool parseVersion(const char* arg, Version& out) {
	errno = 0;
	char* end = nullptr;
	long long v = std::strtoll(arg, &end, 10);
	if (errno != 0 || end == arg || *end != '\0' || v < 0) {
		return false;
	}
	out = v;
	return true;
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
			} else if (ftype != "both" && !ftype.empty()) {
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
			// An empty prefix would reach strinc("") in prefixRange() and assert.
			if (*args->OptionArg() == '\0') {
				std::cerr << "ERROR: -k requires a non-empty prefix\n";
				return FDB_EXIT_ERROR;
			}
			param->prefixes.push_back(args->OptionArg());
			break;

		case OPT_FILTERS: {
			// Appends, so -k/--hex-prefix given alongside --filters are all honored.
			std::vector<std::string> filePrefixes = parsePrefixFile(args->OptionArg(), err);
			if (err) {
				std::cerr << "ERROR: " << args->OptionArg() << " contains invalid prefix(es)\n";
				return FDB_EXIT_ERROR;
			}
			param->prefixes.insert(param->prefixes.end(), filePrefixes.begin(), filePrefixes.end());
			break;
		}

		case OPT_HEX_KEY_PREFIX: {
			std::string prefix = decode_hex_string(args->OptionArg(), err);
			if (err || prefix.empty()) {
				std::cerr << "ERROR: invalid hex prefix: " << args->OptionArg() << "\n";
				return FDB_EXIT_ERROR;
			}
			param->prefixes.push_back(prefix);
			break;
		}

		case OPT_PROXY:
			param->proxy = args->OptionArg();
			break;

		case OPT_BEGIN_VERSION_FILTER:
			if (!parseVersion(args->OptionArg(), param->beginVersionFilter)) {
				std::cerr << "ERROR: invalid version for --begin-version-filter: " << args->OptionArg() << "\n";
				return FDB_EXIT_ERROR;
			}
			break;

		case OPT_END_VERSION_FILTER:
			if (!parseVersion(args->OptionArg(), param->endVersionFilter)) {
				std::cerr << "ERROR: invalid version for --end-version-filter: " << args->OptionArg() << "\n";
				return FDB_EXIT_ERROR;
			}
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

		case OPT_ENCRYPTION_KEY_FILE:
			param->encryptionKeyFileName = args->OptionArg();
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

		default:
			// gConverterOptions is shared with fdbconvert and carries options this tool does not
			// implement, e.g. -b/--begin and -e/--end. Reject them instead of ignoring them.
			std::cerr << "ERROR: unsupported option: " << args->OptionText() << "\n";
			return FDB_EXIT_ERROR;
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
 *    co_await progress.openFile(container);
 *    while (1) {
 *        Optional<VersionedMutations> batch = progress.getNextBatch();
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
	// Ordered so that mutations are emitted in version order; an unordered_map makes output depend on hash
	// iteration order and differ between builds.
	std::map<Version, fileBackup::AccumulatedMutations> mutationBlocksByVersion;

public:
	DecodeProgress() = default;
	DecodeProgress(const LogFile& file, bool save) : file(file), save(save) {}

	~DecodeProgress() {
		if (lfd != -1) {
			close(lfd);
		}
	}

	// Open and loads file into memory
	Future<Void> openFile(Reference<IBackupContainer> container) {
		fd = co_await container->readFile(file.fileName);
		Standalone<StringRef> buf = makeString(file.fileSize);
		int rLen = co_await fd->read(mutateString(buf), file.fileSize, 0);
		if (rLen != file.fileSize) {
			throw restore_bad_read();
		}

		if (save) {
			std::string dir = file.fileName;
			std::size_t found = file.fileName.find_last_of('/');
			if (found != std::string::npos) {
				std::string path = file.fileName.substr(0, found);
				if (!directoryExists(path)) {
					platform::createDirectory(path);
				}
			}
			lfd = open(file.fileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
			if (lfd == -1) {
				TraceEvent(SevError, "OpenLocalFileFailed").detail("File", file.fileName);
				throw platform_error();
			}
			int wlen = write(lfd, buf.begin(), file.fileSize);
			if (wlen != file.fileSize) {
				TraceEvent(SevError, "WriteLocalFileFailed").detail("File", file.fileName).detail("Len", file.fileSize);
				throw platform_error();
			}
			TraceEvent("WriteLocalFile").detail("Name", file.fileName).detail("Len", file.fileSize);
		}

		decodeFile(buf);
	}

	// The following are private APIs:

	// Returns the next batch of mutations along with the arena backing it.
	// Note the returned batch can be empty when the file has unfinished
	// version batch data that are in the next file.
	Optional<VersionedMutations> getNextBatch() {
		for (auto& [version, m] : mutationBlocksByVersion) {
			Optional<StringRef> completeMutations = m.getCompleteMutations();
			if (completeMutations.present()) {
				VersionedMutations vms;
				vms.version = version;
				vms.serializedMutations = completeMutations.get().toString();
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
			while (true) {
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
	Future<Void> openFile(Reference<IBackupContainer> container) {
		TraceEvent("ReadFile").detail("Name", file.fileName).detail("Len", file.fileSize);

		fd = co_await container->readFile(file.fileName);
		Standalone<StringRef> buf = makeString(file.fileSize);
		int rLen = co_await fd->read(mutateString(buf), file.fileSize, 0);
		if (rLen != file.fileSize) {
			throw restore_bad_read();
		}

		if (save) {
			std::string dir = file.fileName;
			std::size_t found = file.fileName.find_last_of('/');
			if (found != std::string::npos) {
				std::string path = file.fileName.substr(0, found);
				if (!directoryExists(path)) {
					platform::createDirectory(path);
				}
			}

			lfd = open(file.fileName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
			if (lfd == -1) {
				TraceEvent(SevError, "OpenLocalFileFailed").detail("File", file.fileName);
				throw platform_error();
			}
			int wlen = write(lfd, buf.begin(), file.fileSize);
			if (wlen != file.fileSize) {
				TraceEvent(SevError, "WriteLocalFileFailed").detail("File", file.fileName).detail("Len", file.fileSize);
				throw platform_error();
			}
			TraceEvent("WriteLocalFile").detail("Name", file.fileName).detail("Len", file.fileSize);
		}

		decodeFile(buf);
	}

	// Reads a file content in the buffer, decodes it into key/value pairs, and stores these pairs.
	void decodeFile(const Standalone<StringRef>& buf) {
		try {
			while (true) {
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
	result.reserve(static_cast<size_t>(s.size()) * 2);
	for (int i = 0; i < s.size(); i++) {
		result.append(format("%02x", s[i]));
	}
	return result;
}

Future<Void> process_range_file(Reference<IBackupContainer> container,
                                RangeFile file,
                                UID uid,
                                Reference<DecodeParams> params) {

	if (file.fileSize == 0) {
		TraceEvent("SkipEmptyFile", uid).detail("Name", file.fileName);
		co_return;
	}

	DecodeRangeProgress progress(file, params->save_file_locally);
	co_await progress.openFile(container);

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
}

Future<Void> process_file(Reference<IBackupContainer> container,
                          LogFile file,
                          UID uid,
                          Reference<DecodeParams> params) {
	if (file.fileSize == 0) {
		TraceEvent("SkipEmptyFile", uid).detail("Name", file.fileName);
		co_return;
	}

	DecodeProgress progress(file, params->save_file_locally);
	co_await progress.openFile(container);
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
				// getTypeString() bounds-checks; m.type comes unvalidated from the file.
				std::cout << vms.version << "." << sub << " " << getTypeString(m.type)
				          << " param1: " << hexStringRef(m.param1) << " param2: " << hexStringRef(m.param2) << "\n";
			}
		}
	}
	TraceEvent("ProcessFileDone", uid).detail("File", file.fileName);
}

// Use the snapshot metadata to quickly identify relevant range files and
// then filter by versions.
Future<std::vector<RangeFile>> getRangeFiles(Reference<IBackupContainer> bc,
                                             Reference<DecodeParams> params,
                                             Optional<Version> expiredEndVersion) {
	// Only consider snapshots whose version range overlaps the requested filter. Reading a snapshot
	// means downloading and parsing its entire manifest and checking every file it lists against the
	// container, so snapshots that getRelevantRangeFiles() would discard below must not be read at
	// all. A partially expired snapshot outside the filter would otherwise report every expired file
	// it lists as a SevError, burying the result the caller asked for.
	std::vector<KeyspaceSnapshotFile> snapshots =
	    co_await (dynamic_cast<BackupContainerFileSystem*>(bc.getPtr()))
	        ->listKeyspaceSnapshots(params->beginVersionFilter, params->endVersionFilter);
	std::vector<RangeFile> files;

	for (int i = 0; i < snapshots.size(); i++) {
		try {
			std::pair<std::vector<RangeFile>, std::map<std::string, KeyRange>> results =
			    co_await (dynamic_cast<BackupContainerFileSystem*>(bc.getPtr()))->readKeyspaceSnapshot(snapshots[i]);
			for (const auto& rangeFile : results.first) {
				// No prefix filter, or a manifest with no per-file key ranges (encrypted backups), selects
				// every file. An empty RangeMapFilters matches nothing, so it cannot answer this.
				if (params->prefixes.empty() || results.second.empty()) {
					files.push_back(rangeFile);
					continue;
				}
				const auto& keyRange = results.second.at(rangeFile.fileName);
				if (params->matchFilters(keyRange)) {
					files.push_back(rangeFile);
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_restore_missing_data) {
				TraceEvent("ReadKeyspaceSnapshotError").error(e).detail("I", i);
				throw;
			}
			// Files this snapshot lists are gone, so skipping it makes the reported file set incomplete.
			// Expiration deletes range files but keeps a manifest that straddles the expiry boundary, so
			// that case is expected; anything else is unexplained data loss.
			double expiredPct = snapshots[i].expiredPct(expiredEndVersion);
			if (expiredPct > 0) {
				std::cerr << "WARNING: skipping snapshot " << snapshots[i].fileName << ": " << expiredPct
				          << "% of its version range is expired\n";
				TraceEvent(SevWarnAlways, "DecodeSkippedExpiredSnapshot")
				    .detail("File", snapshots[i].fileName)
				    .detail("ExpiredPct", expiredPct);
			} else {
				std::cerr << "ERROR: skipping snapshot " << snapshots[i].fileName
				          << ": it references range files that are absent from the container\n";
				TraceEvent(SevError, "DecodeSnapshotMissingRangeFiles").detail("File", snapshots[i].fileName);
			}
		}
	}
	co_return getRelevantRangeFiles(files, params);
}

Future<Void> decode_logs(Reference<DecodeParams> params) {
	Reference<IBackupContainer> container =
	    IBackupContainer::openContainer(params->container_url, params->proxy, params->encryptionKeyFileName, 0);
	UID uid = deterministicRandom()->randomUniqueID();

	// describeBackup() must run before any file listing: for an encrypted container the listing converts raw
	// file sizes using the block size it establishes, and converting against 0 asserts or yields sizes of 0.
	BackupDescription desc = co_await container->describeBackup();
	container->setEncryptionBlockSize(desc.encryptionBlockSize);

	BackupFileList listing = co_await container->dumpFileList();

	// Partitioned logs use a block format this tool cannot parse; only fdbconvert reads them.
	size_t logsBeforeFilter = listing.logs.size();
	listing.logs.erase(std::remove_if(listing.logs.begin(),
	                                  listing.logs.end(),
	                                  [](const LogFile& file) {
		                                  std::string prefix("plogs/");
		                                  return file.fileName.substr(0, prefix.size()) == prefix;
	                                  }),
	                   listing.logs.end());
	size_t partitionedLogs = logsBeforeFilter - listing.logs.size();
	if (partitionedLogs > 0) {
		std::cerr << "WARNING: skipping " << partitionedLogs
		          << " partitioned log file(s); use fdbconvert to read them\n";
		TraceEvent(SevWarnAlways, "DecodeSkippedPartitionedLogs", uid).detail("Count", partitionedLogs);
	}

	std::sort(listing.logs.begin(), listing.logs.end());
	// A log file whose progress was not saved is rewritten with the same begin version, leaving subsets of
	// other files in the container. Without this the same mutations are emitted more than once.
	listing.logs = fileBackup::filterDuplicateLogFiles(listing.logs);
	TraceEvent("Container", uid).detail("URL", params->container_url).detail("Logs", listing.logs.size());
	TraceEvent("DecodeParam", uid).setMaxFieldLength(100000).detail("Value", params->toString());

	std::cout << "\n" << desc.toString() << "\n";

	std::vector<LogFile> logFiles;
	std::vector<RangeFile> rangeFiles;

	if (params->decode_logs) {
		logFiles = getRelevantLogFiles(listing.logs, params);
		printLogFiles("Relevant log files are: ", logFiles);
		// Mutations between a gap's endpoints are absent from the container, so the decoded stream is
		// incomplete there. Restore refuses such a set outright; report it and continue.
		for (int i = 1; i < logFiles.size(); i++) {
			if (logFiles[i].beginVersion > logFiles[i - 1].endVersion) {
				std::cerr << "WARNING: gap in mutation log between versions " << logFiles[i - 1].endVersion
				          << " and " << logFiles[i].beginVersion << "\n";
				TraceEvent(SevWarnAlways, "DecodeLogGap", uid)
				    .detail("From", logFiles[i - 1].endVersion)
				    .detail("To", logFiles[i].beginVersion);
			}
		}
	}

	if (params->decode_range) {
		// rangeFiles = getRelevantRangeFiles(filteredRangeFiles, params);
		std::vector<RangeFile> files = co_await getRangeFiles(container, params, desc.expiredEndVersion);
		rangeFiles = files;
		printLogFiles("Relevant range files are: ", rangeFiles);
	}

	TraceEvent("TotalFiles", uid).detail("LogFiles", logFiles.size()).detail("RangeFiles", rangeFiles.size());

	if (params->list_only)
		co_return;

	// Decode log files.
	int idx = 0;
	if (params->decode_logs) {
		while (idx < logFiles.size()) {
			TraceEvent("ProcessFile").detail("Name", logFiles[idx].fileName).detail("I", idx);
			co_await process_file(container, logFiles[idx], uid, params);
			idx++;
		}
		TraceEvent("DecodeLogsDone", uid).log();
	}

	// Decode range files.
	if (params->decode_range) {
		idx = 0;
		while (idx < rangeFiles.size()) {
			TraceEvent("ProcessFile").detail("Name", rangeFiles[idx].fileName).detail("I", idx);
			co_await process_range_file(container, rangeFiles[idx], uid, params);
			idx++;
		}
		TraceEvent("DecodeRangeFileDone", uid).log();
	}
}

} // namespace file_converter

#ifndef EXCLUDE_MAIN_FUNCTION
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

		// Check if the beginVersionFilter is greater than the endVersionFilter, otherwise the filtering will be
		// invalid.
		if (!param->validVersionFilters()) {
			std::cerr << "--begin-version-filter " << param->beginVersionFilter
			          << " cannot be equal or greater than --end-version-filter " << param->endVersionFilter << "\n";
			file_converter::printDecodeUsage();
			return FDB_EXIT_ERROR;
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
		    .detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(nullptr))
		    .setMaxFieldLength(10000)
		    .detail("CommandLine", commandLine)
		    .setMaxFieldLength(0)
		    .trackLatest("ProgramStart");

		TraceEvent::setNetworkThread();
		openTraceFile({}, 10 << 20, 500 << 20, param->log_dir, "decode", param->trace_log_group);
		param->tlsConfig.setupBlobCredentials();

		auto f = stopAfter(decode_logs(param));

		runNetwork();

		// stopAfter() reports failure by leaving the Optional unset. Without this the tool exits 0 after any
		// decode error, as fdbbackup's main() already guards against.
		if (f.isValid() && f.isReady() && (f.isError() || !f.get().present())) {
			status = FDB_EXIT_ERROR;
		}

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
#else // EXCLUDE_MAIN_FUNCTION

int main() {
	auto assertValid = [](file_converter::DecodeParams& p, bool expected, const char* label) {
		bool result = p.validVersionFilters();
		if (result != expected) {
			fprintf(stderr, "FAIL [%s]: expected %s\n", label, expected ? "valid" : "invalid");
			return false;
		}
		printf("PASS [%s]\n", label);
		return true;
	};

	bool ok = true;
	file_converter::DecodeParams p;

	ok &= assertValid(p, true, "defaults");

	p.beginVersionFilter = 100;
	p.endVersionFilter = 200;
	ok &= assertValid(p, true, "begin < end");

	p.beginVersionFilter = 200;
	p.endVersionFilter = 200;
	ok &= assertValid(p, false, "begin == end");

	p.beginVersionFilter = 300;
	p.endVersionFilter = 200;
	ok &= assertValid(p, false, "begin > end");

	auto check = [&ok](bool cond, const char* label) {
		if (cond) {
			printf("PASS [%s]\n", label);
		} else {
			fprintf(stderr, "FAIL [%s]\n", label);
			ok = false;
		}
	};

	// parseVersion rejects what atoll() silently accepted.
	Version v = -1;
	check(file_converter::parseVersion("100", v) && v == 100, "parseVersion decimal");
	check(!file_converter::parseVersion("v100", v), "parseVersion rejects leading garbage");
	check(!file_converter::parseVersion("100x", v), "parseVersion rejects trailing garbage");
	check(!file_converter::parseVersion("", v), "parseVersion rejects empty");
	check(!file_converter::parseVersion("-1", v), "parseVersion rejects negative");
	check(!file_converter::parseVersion("99999999999999999999", v), "parseVersion rejects overflow");

	// A prefix file ends in a newline, which must not become part of the last prefix.
	bool err = false;
	std::vector<std::string> prefixes = file_converter::parsePrefixesLine("\\x05\\x01;\\x15\\x2b\n", err);
	check(!err && prefixes.size() == 2, "parsePrefixesLine count");
	check(prefixes.size() == 2 && prefixes[0] == std::string("\x05\x01", 2), "parsePrefixesLine first prefix");
	check(prefixes.size() == 2 && prefixes[1] == std::string("\x15\x2b", 2), "parsePrefixesLine trailing newline");

	// Blank entries are skipped rather than yielding an empty prefix, which would assert in prefixRange().
	prefixes = file_converter::parsePrefixesLine("\\x05;;\\x15\n\n", err);
	check(!err && prefixes.size() == 2, "parsePrefixesLine skips blanks");

	// One prefix per line must not collapse into a single prefix with embedded newline bytes.
	prefixes = file_converter::parsePrefixesLine("\\x05\\x01\n\\x15\\x2b\n", err);
	check(!err && prefixes.size() == 2, "parsePrefixesLine newline separates entries");
	check(prefixes.size() == 2 && prefixes[1] == std::string("\x15\x2b", 2), "parsePrefixesLine multiline prefix");

	// Malformed escapes are reported, not silently dropped.
	decode_hex_string("\\xZZ", err);
	check(err, "decode_hex_string reports bad hex");

	return ok ? 0 : 1;
}
#endif // EXCLUDE_MAIN_FUNCTION
