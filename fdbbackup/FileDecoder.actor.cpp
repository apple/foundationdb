/*
 * FileDecoder.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbbackup/FileConverter.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/MutationList.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/serialize.h"
#include "fdbclient/BuildFlags.h"
#include "flow/actorcompiler.h" // has to be last include

#define SevDecodeInfo SevVerbose

extern bool g_crashOnError;

namespace file_converter {

void printDecodeUsage() {
	std::cout
	    << "Decoder for FoundationDB backup mutation logs.\n"
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
	       "  --trace_format FORMAT\n"
	       "                 Select the format of the trace files, xml (the default) or json.\n"
	       "                 Has no effect unless --log is specified.\n"
	       "  --crash        Crash on serious error.\n"
	       "  --blob_credentials FILE\n"
	       "                 File containing blob credentials in JSON format.\n"
	       "                 The same credential format/file fdbbackup uses.\n"
#ifndef TLS_DISABLED
	    TLS_HELP
#endif
	       "  --build_flags  Print build information and exit.\n"
		   "  --list_only    Print file list and exit.\n"
		   "  -k KEY_PREFIX  Use the prefix for filtering mutations\n"
		   "  --hex_prefix   HEX_PREFIX\n"
		   "                 The prefix specified in HEX format, e.g., \\x05\\x01.\n"
		   "  --begin_version_filter BEGIN_VERSION\n"
		   "                 The version range's begin version (inclusive) for filtering.\n"
		   "  --end_version_filter END_VERSION\n"
		   "                 The version range's end version (exclusive) for filtering.\n"
	       "\n";
	return;
}

void printBuildInformation() {
	std::cout << jsonBuildInformation() << "\n";
}

struct DecodeParams {
	std::string container_url;
	std::string fileFilter; // only files match the filter will be decoded
	bool log_enabled = true;
	std::string log_dir, trace_format, trace_log_group;
	BackupTLSConfig tlsConfig;
	bool list_only = false;
	std::string prefix; // Key prefix for filtering
	Version beginVersionFilter = 0;
	Version endVersionFilter = std::numeric_limits<Version>::max();

	// Returns if [begin, end) overlap with the filter range
	bool overlap(Version begin, Version end) const {
		// Filter [100, 200),  [50,75) [200, 300)
		return !(begin >= endVersionFilter || end <= beginVersionFilter);
	}

	std::string toString() {
		std::string s;
		s.append("ContainerURL: ");
		s.append(container_url);
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
		return s;
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

std::pair<Version, int32_t> decode_key(const StringRef& key) {
	ASSERT(key.size() == sizeof(uint8_t) + sizeof(Version) + sizeof(int32_t));

	uint8_t hash;
	Version version;
	int32_t part;
	BinaryReader rd(key, Unversioned());
	rd >> hash >> version >> part;
	version = bigEndian64(version);
	part = bigEndian32(part);

	int32_t v = version / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
	ASSERT(((uint8_t)hashlittle(&v, sizeof(v), 0)) == hash);

	return std::make_pair(version, part);
}

// Decodes an encoded list of mutations in the format of:
//   [includeVersion:uint64_t][val_length:uint32_t][mutation_1][mutation_2]...[mutation_k],
// where a mutation is encoded as:
//   [type:uint32_t][keyLength:uint32_t][valueLength:uint32_t][key][value]
std::vector<MutationRef> decode_value(const StringRef& value) {
	StringRefReader reader(value, restore_corrupted_data());

	reader.consume<uint64_t>(); // Consume the includeVersion
	uint32_t val_length = reader.consume<uint32_t>();
	if (val_length != value.size() - sizeof(uint64_t) - sizeof(uint32_t)) {
		TraceEvent(SevError, "ValueError")
		    .detail("ValueLen", val_length)
		    .detail("ValueSize", value.size())
		    .detail("Value", printable(value));
	}

	std::vector<MutationRef> mutations;
	while (1) {
		if (reader.eof())
			break;

		// Deserialization of a MutationRef, which was packed by MutationListRef::push_back_deep()
		uint32_t type, p1len, p2len;
		type = reader.consume<uint32_t>();
		p1len = reader.consume<uint32_t>();
		p2len = reader.consume<uint32_t>();

		const uint8_t* key = reader.consume(p1len);
		const uint8_t* val = reader.consume(p2len);

		mutations.emplace_back((MutationRef::Type)type, StringRef(key, p1len), StringRef(val, p2len));
	}
	return mutations;
}


// Decodes a mutation log key, which contains (hash, commitVersion, chunkNumber) and
// returns (commitVersion, chunkNumber)
std::pair<Version, int32_t> decodeLogKey(const StringRef& key) {
	ASSERT(key.size() == sizeof(uint8_t) + sizeof(Version) + sizeof(int32_t));

	uint8_t hash;
	Version version;
	int32_t part;
	BinaryReader rd(key, Unversioned());
	rd >> hash >> version >> part;
	version = bigEndian64(version);
	part = bigEndian32(part);

	int32_t v = version / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
	ASSERT(((uint8_t)hashlittle(&v, sizeof(v), 0)) == hash);

	return std::make_pair(version, part);
}

// Decodes an encoded list of mutations in the format of:
//   [includeVersion:uint64_t][val_length:uint32_t][mutation_1][mutation_2]...[mutation_k],
// where a mutation is encoded as:
//   [type:uint32_t][keyLength:uint32_t][valueLength:uint32_t][param1][param2]
std::vector<MutationRef> decodeLogValue(const StringRef& value) {
	StringRefReader reader(value, restore_corrupted_data());

	Version protocolVersion = reader.consume<uint64_t>();
	if (protocolVersion <= 0x0FDB00A200090001) {
		throw incompatible_protocol_version();
	}

	uint32_t val_length = reader.consume<uint32_t>();
	if (val_length != value.size() - sizeof(uint64_t) - sizeof(uint32_t)) {
		TraceEvent(SevError, "FileRestoreLogValueError")
		    .detail("ValueLen", val_length)
		    .detail("ValueSize", value.size())
		    .detail("Value", printable(value));
	}

	std::vector<MutationRef> mutations;
	while (1) {
		if (reader.eof())
			break;

		// Deserialization of a MutationRef, which was packed by MutationListRef::push_back_deep()
		uint32_t type, p1len, p2len;
		type = reader.consume<uint32_t>();
		p1len = reader.consume<uint32_t>();
		p2len = reader.consume<uint32_t>();

		const uint8_t* key = reader.consume(p1len);
		const uint8_t* val = reader.consume(p2len);

		mutations.emplace_back((MutationRef::Type)type, StringRef(key, p1len), StringRef(val, p2len));
	}
	return mutations;
}

// Accumulates mutation log value chunks, as both a vector of chunks and as a combined chunk,
// in chunk order, and can check the chunk set for completion or intersection with a set
// of ranges.
struct AccumulatedMutations {
	AccumulatedMutations() : lastChunkNumber(-1) {}

	// Add a KV pair for this mutation chunk set
	// It will be accumulated onto serializedMutations if the chunk number is
	// the next expected value.
	void addChunk(int chunkNumber, const KeyValueRef& kv) {
		if (chunkNumber == lastChunkNumber + 1) {
			lastChunkNumber = chunkNumber;
			serializedMutations += kv.value.toString();
		} else {
			lastChunkNumber = -2;
			serializedMutations.clear();
		}
		kvs.push_back(kv);
	}

	// Returns true if both
	//   - 1 or more chunks were added to this set
	//   - The header of the first chunk contains a valid protocol version and a length
	//     that matches the bytes after the header in the combined value in serializedMutations
	bool isComplete() const {
		if (lastChunkNumber >= 0) {
			StringRefReader reader(serializedMutations, restore_corrupted_data());

			Version protocolVersion = reader.consume<uint64_t>();
			if (protocolVersion <= 0x0FDB00A200090001) {
				throw incompatible_protocol_version();
			}

			uint32_t vLen = reader.consume<uint32_t>();
			return vLen == reader.remainder().size();
		}

		return false;
	}

	// Returns true if a complete chunk contains any MutationRefs which intersect with any
	// range in ranges.
	// It is undefined behavior to run this if isComplete() does not return true.
	bool matchesAnyRange(const std::vector<KeyRange>& ranges) const {
		std::vector<MutationRef> mutations = decodeLogValue(serializedMutations);
		for (auto& m : mutations) {
			for (auto& r : ranges) {
				if (m.type == MutationRef::ClearRange) {
					if (r.intersects(KeyRangeRef(m.param1, m.param2))) {
						return true;
					}
				} else {
					if (r.contains(m.param1)) {
						return true;
					}
				}
			}
		}

		return false;
	}

	std::vector<KeyValueRef> kvs;
	std::string serializedMutations;
	int lastChunkNumber;
};



struct VersionedMutations {
	Version version;
	std::vector<MutationRef> mutations;
	Arena arena; // The arena that contains the mutations.
	std::string serializedMutations; // buffer that contains mutations
};

ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeLogFileBlock(Reference<IAsyncFile> file,
                                                                    int64_t offset,
                                                                    int len) {
	state Standalone<StringRef> buf = makeString(len);
	int rLen = wait(file->read(mutateString(buf), len, offset));
	if (rLen != len)
		throw restore_bad_read();

	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	state StringRefReader reader(buf, restore_corrupted_data());

	try {
		// Read header, currently only decoding version BACKUP_AGENT_MLOG_VERSION
		if (reader.consume<int32_t>() != BACKUP_AGENT_MLOG_VERSION)
			throw restore_unsupported_file_version();

		// Read k/v pairs.  Block ends either at end of last value exactly or with 0xFF as first key len byte.
		while (1) {
			// If eof reached or first key len bytes is 0xFF then end of block was reached.
			if (reader.eof() || *reader.rptr == 0xFF)
				break;

			// Read key and value.  If anything throws then there is a problem.
			uint32_t kLen = reader.consumeNetworkUInt32();
			const uint8_t* k = reader.consume(kLen);
			uint32_t vLen = reader.consumeNetworkUInt32();
			const uint8_t* v = reader.consume(vLen);

			results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));
		}

		// Make sure any remaining bytes in the block are 0xFF
		for (auto b : reader.remainder())
			if (b != 0xFF)
				throw restore_corrupted_data_padding();

		return results;

	} catch (Error& e) {
		TraceEvent(SevWarn, "FileRestoreCorruptLogFileBlock")
		    .error(e)
		    .detail("Filename", file->getFilename())
		    .detail("BlockOffset", offset)
		    .detail("BlockLen", len)
		    .detail("ErrorRelativeOffset", reader.rptr - buf.begin())
		    .detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset);
		throw;
	}
}

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
	Standalone<VectorRef<KeyValueRef>> blocks;
	std::unordered_map<Version, AccumulatedMutations> mutationBlocksByVersion;

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
				std::vector<MutationRef> mutations = decodeLogValue(m.serializedMutations);
				TraceEvent("Decode").detail("version", vms.version).detail("N", mutations.size());
				vms.mutations.insert(vms.mutations.end(), mutations.begin(), mutations.end());
				vms.arena = blocks.arena();
				vms.serializedMutations = m.serializedMutations;
				mutationBlocksByVersion.erase(version);
				return vms;
			}
		}

		// No complete versions
		TraceEvent(SevWarn, "UnfishedBlocks").detail("NumberOfVersions", mutationBlocksByVersion.size());
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

	// Add blocks to mutationBlocksByVersion
	void filterLogMutationKVPairs(VectorRef<KeyValueRef> blocks) {
		for (auto& kv : blocks) {
			auto versionAndChunkNumber = decodeLogKey(kv.key);
			mutationBlocksByVersion[versionAndChunkNumber.first].addChunk(versionAndChunkNumber.second, kv);
		}
/*
		std::vector<KeyValueRef> output;

		for (auto& vb : mutationBlocksByVersion) {
			AccumulatedMutations& m = vb.second;

			// If the mutations are incomplete or match one of the ranges, include in results.
			if (!m.isComplete() || m.matchesAnyRange(ranges)) {
				output.insert(output.end(), m.kvs.begin(), m.kvs.end());
			}
		}

		return output;*/
	}

	// Reads a file block, decodes it into key/value pairs, and stores these pairs.
	ACTOR static Future<Void> readAndDecodeFile(DecodeProgress* self) {
		try {
			state int64_t len = std::min<int64_t>(self->file.blockSize, self->file.fileSize - self->offset);
			if (len == 0) {
				self->eof = true;
				return Void();
			}

			// Decode a file block into log_key and log_value pairs
			Standalone<VectorRef<KeyValueRef>> blocks = wait(decodeLogFileBlock(self->fd, self->offset, len));
			// This is memory inefficient, but we don't know if blocks are complete version data
			self->blocks.reserve(self->blocks.arena(), self->blocks.size() + blocks.size());
			for (int i = 0; i < blocks.size(); i++) {
				self->blocks.push_back_deep(self->blocks.arena(), blocks[i]);
			}

			TraceEvent("ReadFile")
			    .detail("Name", self->file.fileName)
			    .detail("Len", len)
			    .detail("Offset", self->offset);
			self->filterLogMutationKVPairs(blocks);
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
	TraceEvent("ProcessFile").detail("Name", file.fileName);
	if (file.fileSize == 0) {
		TraceEvent("SkipEmptyFile").detail("Name", file.fileName);
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
					print = range.contains(StringRef(params.prefix));
				} else {
					ASSERT(false);
				}
			}
			if (print) {
				TraceEvent(format("Mutation_%d_%d", vms.version, sub).c_str(), uid)
					.detail("Version", vms.version)
					.setMaxFieldLength(10000)
					.detail("M", m.toString());
				std::cout << vms.version << " " << m.toString() << "\n";
			}
		}
	}
	TraceEvent("ProcessFileDone").detail("File", file.fileName);
	return Void();
}

ACTOR Future<Void> decode_logs(DecodeParams params) {
	state Reference<IBackupContainer> container = IBackupContainer::openContainer(params.container_url);
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

	if (params.list_only) return Void();

	state int idx = 0;
	while (idx < logs.size()) {
		TraceEvent("ProcessFileI").detail("Name", logs[idx].fileName).detail("I", idx);

		wait(process_file(container, logs[idx], uid, params));

		TraceEvent("ProcessFileIDone").detail("I", idx);
		idx++;
	}
	TraceEvent("DecodeDone", uid);
	return Void();
}

} // namespace file_converter

int main(int argc, char** argv) {
	try {
		CSimpleOpt* args = new CSimpleOpt(argc, argv, file_converter::gConverterOptions, SO_O_EXACT);
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
