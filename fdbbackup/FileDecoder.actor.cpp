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
#include <iostream>
#include <vector>

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbbackup/FileConverter.h"
#include "fdbclient/MutationList.h"
#include "flow/flow.h"
#include "flow/serialize.h"
#include "flow/actorcompiler.h" // has to be last include

#define SevDecodeInfo SevVerbose

extern bool g_crashOnError;

namespace file_converter {

void printDecodeUsage() {
	std::cout << "\n"
	             "  -r, --container   Container URL.\n"
	             "  -i, --input FILE  Log file to be decoded.\n"
	             "  --crash           Crash on serious error.\n"
	             "\n";
	return;
}

struct DecodeParams {
	std::string container_url;
	std::string file;
	bool log_enabled = false;
	std::string log_dir, trace_format, trace_log_group;

	std::string toString() {
		std::string s;
		s.append("ContainerURL: ");
		s.append(container_url);
		s.append(", File: ");
		s.append(file);
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
		return s;
	}
};

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
			printDecodeUsage();
			return FDB_EXIT_ERROR;

		case OPT_CONTAINER:
			param->container_url = args->OptionArg();
			break;

		case OPT_CRASHONERROR:
			g_crashOnError = true;
			break;

		case OPT_INPUT_FILE:
			param->file = args->OptionArg();
			break;

		case OPT_TRACE:
			param->log_enabled = true;
			break;

		case OPT_TRACE_DIR:
			param->log_dir = args->OptionArg();
			break;

		case OPT_TRACE_FORMAT:
			if (!validateTraceFormat(args->OptionArg())) {
				std::cerr << "ERROR: Unrecognized trace format " << args->OptionArg() << "\n";
				return FDB_EXIT_ERROR;
			}
			param->trace_format = args->OptionArg();
			break;

		case OPT_TRACE_LOG_GROUP:
			param->trace_log_group = args->OptionArg();
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
		if (file.fileName.find(params.file) != std::string::npos) {
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
		if (reader.eof()) break;

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

struct VersionedMutations {
	Version version;
	std::vector<MutationRef> mutations;
	Arena arena; // The arena that contains the mutations.
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
struct DecodeProgress {
	DecodeProgress() = default;
	template <class U>
	DecodeProgress(const LogFile& file, U &&values)
	  : file(file), keyValues(std::forward<U>(values)) {}

	// If there are no more mutations to pull from the file.
	// However, we could have unfinished version in the buffer when EOF is true,
	// which means we should look for data in the next file. The caller
	// should call getUnfinishedBuffer() to get these left data.
	bool finished() { return (eof && keyValues.empty()) || (leftover && !keyValues.empty()); }

	std::vector<std::tuple<Arena, Version, int32_t, StringRef>>&& getUnfinishedBuffer() && { return std::move(keyValues); }

	// Returns all mutations of the next version in a batch.
	Future<VersionedMutations> getNextBatch() { return getNextBatchImpl(this); }

	Future<Void> openFile(Reference<IBackupContainer> container) { return openFileImpl(this, container); }

	// The following are private APIs:

	// Returns true if value contains complete data.
	bool isValueComplete(StringRef value) {
		StringRefReader reader(value, restore_corrupted_data());

		reader.consume<uint64_t>(); // Consume the includeVersion
		uint32_t val_length = reader.consume<uint32_t>();
		return val_length == value.size() - sizeof(uint64_t) - sizeof(uint32_t);
	}

	// PRECONDITION: finished() must return false before calling this function.
	// Returns the next batch of mutations along with the arena backing it.
	// Note the returned batch can be empty when the file has unfinished
	// version batch data that are in the next file.
	ACTOR static Future<VersionedMutations> getNextBatchImpl(DecodeProgress* self) {
		ASSERT(!self->finished());

		loop {
			if (self->keyValues.size() <= 1) {
				// Try to decode another block when less than one left
				wait(readAndDecodeFile(self));
			}

			auto& tuple = self->keyValues[0];
			ASSERT(std::get<2>(tuple) == 0); // first part number must be 0.

			// decode next versions, check if they are continuous parts
			int idx = 1; // next kv pair in "keyValues"
			int bufSize = std::get<3>(tuple).size();
			for (int lastPart = 0; idx < self->keyValues.size(); idx++, lastPart++) {
				if (idx == self->keyValues.size()) break;

				auto next_tuple = self->keyValues[idx];
				if (std::get<1>(tuple) != std::get<1>(next_tuple)) {
					break;
				}

				if (lastPart + 1 != std::get<2>(next_tuple)) {
					TraceEvent("DecodeError").detail("Part1", lastPart).detail("Part2", std::get<2>(next_tuple));
					throw restore_corrupted_data();
				}
				bufSize += std::get<3>(next_tuple).size();
			}

			VersionedMutations m;
			m.version = std::get<1>(tuple);
			TraceEvent("Decode").detail("Version", m.version).detail("Idx", idx).detail("Q", self->keyValues.size());
			StringRef value = std::get<3>(tuple);
			if (idx > 1) {
				// Stitch parts into one and then decode one by one
				Standalone<StringRef> buf = self->combineValues(idx, bufSize);
				value = buf;
				m.arena = buf.arena();
			}
			if (self->isValueComplete(value)) {
				m.mutations = decode_value(value);
				if (m.arena.getSize() == 0) {
					m.arena = std::get<0>(tuple);
				}
				self->keyValues.erase(self->keyValues.begin(), self->keyValues.begin() + idx);
				return m;
			} else if (!self->eof) {
				// Read one more block, hopefully the missing part of the value can be found.
				wait(readAndDecodeFile(self));
			} else {
				TraceEvent(SevWarn, "MissingValue").detail("Version", m.version);
				self->leftover = true;
				return m; // Empty mutations
			}
		}
	}

	// Returns a buffer which stitches first "idx" values into one.
	// "len" MUST equal the summation of these values.
	Standalone<StringRef> combineValues(const int idx, const int len) {
		ASSERT(idx <= keyValues.size() && idx > 1);

		Standalone<StringRef> buf = makeString(len);
		int n = 0;
		for (int i = 0; i < idx; i++) {
			const auto& value = std::get<3>(keyValues[i]);
			memcpy(mutateString(buf) + n, value.begin(), value.size());
			n += value.size();
		}

		ASSERT(n == len);
		return buf;
	}

	// Decodes a block into KeyValueRef stored in "keyValues".
	void decode_block(const Standalone<StringRef>& buf, int len) {
		StringRef block(buf.begin(), len);
		StringRefReader reader(block, restore_corrupted_data());

		try {
			// Read header, currently only decoding version BACKUP_AGENT_MLOG_VERSION
			if (reader.consume<int32_t>() != BACKUP_AGENT_MLOG_VERSION) throw restore_unsupported_file_version();

			// Read k/v pairs. Block ends either at end of last value exactly or with 0xFF as first key len byte.
			while (1) {
				// If eof reached or first key len bytes is 0xFF then end of block was reached.
				if (reader.eof() || *reader.rptr == 0xFF) break;

				// Read key and value.  If anything throws then there is a problem.
				uint32_t kLen = reader.consumeNetworkUInt32();
				const uint8_t* k = reader.consume(kLen);
				std::pair<Version, int32_t> version_part = decode_key(StringRef(k, kLen));
				uint32_t vLen = reader.consumeNetworkUInt32();
				const uint8_t* v = reader.consume(vLen);
				TraceEvent(SevDecodeInfo, "Block")
				    .detail("KeySize", kLen)
				    .detail("valueSize", vLen)
				    .detail("Offset", reader.rptr - buf.begin())
				    .detail("Version", version_part.first)
				    .detail("Part", version_part.second);
				keyValues.emplace_back(buf.arena(), version_part.first, version_part.second, StringRef(v, vLen));
			}

			// Make sure any remaining bytes in the block are 0xFF
			for (auto b : reader.remainder()) {
				if (b != 0xFF) throw restore_corrupted_data_padding();
			}

			// The (version, part) in a block can be out of order, i.e., (3, 0)
			// can be followed by (4, 0), and then (3, 1). So we need to sort them
			// first by version, and then by part number.
			std::sort(keyValues.begin(), keyValues.end(),
			          [](const std::tuple<Arena, Version, int32_t, StringRef>& a,
			             const std::tuple<Arena, Version, int32_t, StringRef>& b) {
				          return std::get<1>(a) == std::get<1>(b) ? std::get<2>(a) < std::get<2>(b)
				                                                  : std::get<1>(a) < std::get<1>(b);
			          });
			return;
		} catch (Error& e) {
			TraceEvent(SevWarn, "CorruptBlock").error(e).detail("Offset", reader.rptr - buf.begin());
			throw;
		}
	}

	ACTOR static Future<Void> openFileImpl(DecodeProgress* self, Reference<IBackupContainer> container) {
		Reference<IAsyncFile> fd = wait(container->readFile(self->file.fileName));
		self->fd = fd;
		wait(readAndDecodeFile(self));
		return Void();
	}

	// Reads a file block, decodes it into key/value pairs, and stores these pairs.
	ACTOR static Future<Void> readAndDecodeFile(DecodeProgress* self) {
		try {
			state int64_t len = std::min<int64_t>(self->file.blockSize, self->file.fileSize - self->offset);
			if (len == 0) {
				self->eof = true;
				return Void();
			}

			state Standalone<StringRef> buf = makeString(len);
			state int rLen = wait(self->fd->read(mutateString(buf), len, self->offset));
			TraceEvent("ReadFile")
			    .detail("Name", self->file.fileName)
			    .detail("Len", rLen)
			    .detail("Offset", self->offset);
			if (rLen != len) {
				throw restore_corrupted_data();
			}
			self->decode_block(buf, rLen);
			self->offset += rLen;
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
	bool leftover = false; // Done but has unfinished version batch data left
	// A (version, part_number)'s mutations and memory arena.
	std::vector<std::tuple<Arena, Version, int32_t, StringRef>> keyValues;
};

ACTOR Future<Void> decode_logs(DecodeParams params) {
	state Reference<IBackupContainer> container = IBackupContainer::openContainer(params.container_url);

	state BackupFileList listing = wait(container->dumpFileList());
	// remove partitioned logs
	listing.logs.erase(std::remove_if(listing.logs.begin(), listing.logs.end(),
	                                  [](const LogFile& file) {
		                                  std::string prefix("plogs/");
		                                  return file.fileName.substr(0, prefix.size()) == prefix;
	                                  }),
	                   listing.logs.end());
	std::sort(listing.logs.begin(), listing.logs.end());
	TraceEvent("Container").detail("URL", params.container_url).detail("Logs", listing.logs.size());

	BackupDescription desc = wait(container->describeBackup());
	std::cout << "\n" << desc.toString() << "\n";

	state std::vector<LogFile> logs = getRelevantLogFiles(listing.logs, params);
	printLogFiles("Relevant files are: ", logs);

	state int i = 0;
	// Previous file's unfinished version data
	state std::vector<std::tuple<Arena, Version, int32_t, StringRef>> left;
	for (; i < logs.size(); i++) {
		if (logs[i].fileSize == 0) continue;

		state DecodeProgress progress(logs[i], std::move(left));
		wait(progress.openFile(container));
		while (!progress.finished()) {
			VersionedMutations vms = wait(progress.getNextBatch());
			for (const auto& m : vms.mutations) {
				std::cout << vms.version << " " << m.toString() << "\n";
			}
		}
		left = std::move(progress).getUnfinishedBuffer();
		if (!left.empty()) {
			TraceEvent("UnfinishedFile").detail("File", logs[i].fileName).detail("Q", left.size());
		}
	}
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
			}
			if (!param.trace_log_group.empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_LOG_GROUP, StringRef(param.trace_log_group));
			}
		}

		platformInit();
		Error::init();

		StringRef url(param.container_url);
		setupNetwork(0, true);

		TraceEvent::setNetworkThread();
		openTraceFile(NetworkAddress(), 10 << 20, 10 << 20, param.log_dir, "decode", param.trace_log_group);

		auto f = stopAfter(decode_logs(param));

		runNetwork();
		return status;
	} catch (Error& e) {
		fprintf(stderr, "ERROR: %s\n", e.what());
		return FDB_EXIT_ERROR;
	} catch (std::exception& e) {
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		return FDB_EXIT_MAIN_EXCEPTION;
	}
}
