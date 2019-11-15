/*
 * FileConverter.actor.cpp
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

#include "fdbbackup/FileConverter.h"

#include <algorithm>
#include <iostream>
#include <cinttypes>
#include <cstdio>
#include <vector>

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/MutationList.h"
#include "flow/flow.h"
#include "flow/serialize.h"

namespace file_converter {

void printConvertUsage() {
	printf("\n");
	printf("  -r, --container Container URL.\n");
	printf("  -b, --begin BEGIN\n"
	       "                  Begin version.\n");
	printf("  -e, --end END   End version.\n");
	printf("  --log           Enables trace file logging for the CLI session.\n"
	       "  --logdir PATH   Specifes the output directory for trace files. If\n"
	       "                  unspecified, defaults to the current directory. Has\n"
	       "                  no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                  Sets the LogGroup field with the specified value for all\n"
	       "                  events in the trace output (defaults to `default').\n");
	printf("  --trace_format FORMAT\n"
	       "                  Select the format of the trace files. xml (the default) and json are supported.\n"
	       "                  Has no effect unless --log is specified.\n");
	printf("  -h, --help      Display this help and exit.\n");
	printf("\n");

	return;
}

std::vector<LogFile> getRelevantLogFiles(const std::vector<LogFile>& files, Version begin, Version end) {
	std::vector<LogFile> results;
	for (const auto& file : files) {
		if (file.beginVersion <= end && file.endVersion >= begin) {
			results.push_back(file);
		}
	}
	return results;
}

void printLogFiles(std::string msg, const std::vector<LogFile>& files) {
	std::cout << msg << " " << files.size() << " log files\n";
	for (const auto& file : files) {
		std::cout << file.toString() << "\n";
	}
	std::cout << std::endl;
}

struct ConvertParams {
	std::string container_url;
	Version begin = invalidVersion;
	Version end = invalidVersion;
	bool log_enabled = false;
	std::string log_dir, trace_format, trace_log_group;

	bool isValid() { return begin != invalidVersion && end != invalidVersion && !container_url.empty(); }

	std::string toString() {
		std::string s;
		s.append("ContainerURL:");
		s.append(container_url);
		s.append(" Begin:");
		s.append(format("%" PRId64, begin));
		s.append(" End:");
		s.append(format("%" PRId64, end));
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

struct VersionedData {
	LogMessageVersion version;
	MutationRef mutation;
	Arena arena; // The arena that contains mutation.

	VersionedData() : version(invalidVersion, -1) {}
	VersionedData(LogMessageVersion v, MutationRef m, Arena a) : version(v), mutation(m), arena(a) {}
};

struct MutationFilesReadProgress : public ReferenceCounted<MutationFilesReadProgress> {
	MutationFilesReadProgress(std::vector<LogFile>& logs, Version begin, Version end)
	  : files(logs), beginVersion(begin), endVersion(end) {}

	struct FileProgress : public ReferenceCounted<FileProgress> {
		FileProgress(int idx, Reference<IAsyncFile> f) : index(idx), fd(f), offset(0), eof(false) {}

		bool operator<(const FileProgress& rhs) const {
			if (rhs.mutations.empty()) return true;
			if (mutations.empty()) return false;
			return mutations[0].version < rhs.mutations[0].version;
		}
		bool empty() { return eof && mutations.empty(); }

		const int index; // index in "files" vector
		Reference<IAsyncFile> fd;
		int64_t offset; // offset of the file to be read

		std::vector<VersionedData> mutations; // Buffered mutations read so far
		bool eof; // If EOF is seen so far or endVersion is encountered. If true, the file can't be read further.
	};

	bool hasMutations() {
		for (const auto& fp : fileProgress) {
			if (!fp->empty()) return true;
		}
		return false;
	}

	void dumpProgress(std::string msg) {
		std::cout << msg;
		for (const auto fp : fileProgress) {
			std::cout << fp->fd->getFilename() << " " << fp->mutations.size() << " mutations";
			if (fp->mutations.size() > 0) {
				std::cout << ", range " << fp->mutations[0].version.toString() << " "
				          << fp->mutations.back().version.toString() << "\n";
			}
		}
	}

	// Sorts files according to their first mutation version and removes files without mutations.
	void sortAndRemoveEmpty() {
		std::sort(fileProgress.begin(), fileProgress.end(),
		          [](const Reference<FileProgress>& a, const Reference<FileProgress>& b) { return (*a) < (*b); });
		while (!fileProgress.empty() && fileProgress.back()->empty()) {
			fileProgress.pop_back();
		}
	}

	// Requires hasMutations() return true before calling this function.
	// The caller must hold on the the arena associated with the mutation.
	Future<VersionedData> getNextMutation() { return getMutationImpl(this); }

	ACTOR static Future<VersionedData> getMutationImpl(MutationFilesReadProgress* self) {
		ASSERT(!self->fileProgress.empty() && !self->fileProgress[0]->mutations.empty());

		state Reference<FileProgress> fp = self->fileProgress[0];
		state VersionedData data = fp->mutations[0];
		fp->mutations.erase(fp->mutations.begin());
		if (fp->mutations.empty()) {
			// decode one more block
			wait(decodeToVersion(fp, /*version=*/0));
		}

		if (fp->empty()) {
			self->fileProgress.erase(self->fileProgress.begin());
		} else {
			// Keep fileProgress sorted
			for (int i = 1; i < self->fileProgress.size(); i++) {
				if (*self->fileProgress[i - 1] <= *self->fileProgress[i]) break;
				std::swap(self->fileProgress[i - 1], self->fileProgress[i]);
			}
		}
		return data;
	}

	Future<Void> openLogFiles(Reference<IBackupContainer> container) { return openLogFilesImpl(this, container); }

	// Opens log files in the progress and starts decoding until the beginVersion is seen.
	ACTOR static Future<Void> openLogFilesImpl(MutationFilesReadProgress* progress,
	                                           Reference<IBackupContainer> container) {
		state std::vector<Future<Reference<IAsyncFile>>> asyncFiles;
		for (const auto& file : progress->files) {
			asyncFiles.push_back(container->readFile(file.fileName));
		}
		wait(waitForAll(asyncFiles)); // open all files

		state std::vector<Reference<IAsyncFile>> results;
		for (const auto& file : asyncFiles) {
			results.push_back(file.get());
		}

		// Attempt decode the first few blocks of log files until beginVersion is consumed
		std::vector<Future<Void>> fileDecodes;
		for (int idx = 0; idx < results.size(); idx++) {
			Reference<FileProgress> fp(new FileProgress(idx, results[idx]));
			progress->fileProgress.push_back(fp);
			fileDecodes.push_back(decodeToVersion(fp, progress->beginVersion));
		}

		wait(waitForAll(fileDecodes));

		progress->sortAndRemoveEmpty();

		return Void();
	}

	// Decode the file until EOF or an mutation >= version
	ACTOR static Future<Void> decodeToVersion(Reference<FileProgress> fileProgress, Version version) {
		if (fileProgress->empty()) return Void();

		if (!fileProgress->mutations.empty() && fileProgress->mutations.back().version.version >= version)
			return Void();

		state int len = (1 << 20);
		try {
			while (1) {
				// Read block by block until we see the version
				state Standalone<StringRef> buf = makeString(len);
				state int rLen = wait(fileProgress->fd->read(mutateString(buf), len, fileProgress->offset));
				TraceEvent("ReadFile")
				    .detail("Name", fileProgress->fd->getFilename())
				    .detail("Len", rLen)
				    .detail("Offset", fileProgress->offset);
				if (rLen == 0) {
					fileProgress->eof = true;
					return Void();
				}
				StringRef block(buf.begin(), rLen);
				state StringRefReader reader(block, restore_corrupted_data());
				state bool found = false;

				int count = 0, inserted = 0;
				while (1) {
					// If eof reached or first key len bytes is 0xFF then end of block was reached.
					if (reader.eof() || *reader.rptr == 0xFF) break;

					// Deserialize messages written in saveMutationsToFile().
					Version msgVersion = reader.consume<Version>();
					uint32_t sub = reader.consume<uint32_t>();
					int msgSize = reader.consume<int>();
					const uint8_t* message = reader.consume(msgSize);

					BinaryReader rd(message, msgSize, AssumeVersion(currentProtocolVersion));
					MutationRef m;
					rd >> m;
					count++;
					if (msgVersion >= version) {
						fileProgress->mutations.emplace_back(LogMessageVersion(msgVersion, sub), m, buf.arena());
						found = true;
						inserted++;
						// std::cout << msgVersion << ":" << sub << " m = " << m.toString() << "\n";
					}
				}
				std::cout << "Decoded " << count << " mutations in " << fileProgress->fd->getFilename() << ", inserted "
				          << inserted << ", total " << fileProgress->mutations.size() << "\n";
				if (found) break;
			}

			fileProgress->offset += rLen;
			return Void();
		} catch (Error& e) {
			TraceEvent(SevWarn, "FileConvertCorruptLogFileBlock")
			    .error(e)
			    .detail("Filename", fileProgress->fd->getFilename())
			    .detail("BlockOffset", fileProgress->offset)
			    .detail("BlockLen", len)
			    .detail("ErrorRelativeOffset", reader.rptr - buf.begin())
			    .detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + fileProgress->offset);
			throw;
		}
	}

	std::vector<LogFile> files;
	const Version beginVersion, endVersion;
	std::vector<Reference<FileProgress>> fileProgress;
};

// Writes a log file in the old backup format, described in backup-dataFormat.md.
// This is similar to the LogFileWriter in FileBackupAgent.actor.cpp.
class LogFileWriter {
	LogFileWriter(Reference<IBackupFile> f, int bsize) : file(f), blockSize(bsize) {}

	// Returns the block key, i.e., `Param1`, in the back file. The format is
	// `hash_value|commitVersion|part`.
	static Standalone<StringRef> getBlockKey(Version commitVersion, int part) {
		const int32_t version = commitVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;

		BinaryWriter wr(Unversioned());
		wr << (uint8_t)hashlittle(&version, sizeof(version), 0);
		wr << bigEndian64(commitVersion);
		wr << bigEndian32(part);
		return wr.toValue();
	}

	// Return a block of contiguous padding bytes, growing if needed.
	static Value makePadding(int size) {
		static Value pad;
		if (pad.size() < size) {
			pad = makeString(size);
			memset(mutateString(pad), '\xff', pad.size());
		}

		return pad.substr(0, size);
	}

	// Start a new block if needed, then write the key and value
	ACTOR static Future<Void> writeKV_impl(LogFileWriter* self, Key k, Value v) {
		// If key and value do not fit in this block, end it and start a new one
		int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
		if (self->file->size() + toWrite > self->blockEnd) {
			// Write padding if needed
			int bytesLeft = self->blockEnd - self->file->size();
			if (bytesLeft > 0) {
				state Value paddingFFs = makePadding(bytesLeft);
				wait(self->file->append(paddingFFs.begin(), bytesLeft));
			}

			// Set new blockEnd
			self->blockEnd += self->blockSize;

			// write Header
			wait(self->file->append((uint8_t*)&self->fileVersion, sizeof(self->fileVersion)));
		}

		wait(self->file->appendStringRefWithLen(k));
		wait(self->file->appendStringRefWithLen(v));

		// At this point we should be in whatever the current block is or the block size is too small
		if (self->file->size() > self->blockEnd) throw backup_bad_block_size();

		return Void();
	}

	Future<Void> writeKV(Key k, Value v) { return writeKV_impl(this, k, v); }

	// Adds a new mutation to an interal buffer and writes out when encountering
	// a new commitVersion or exceeding the block size.
	ACTOR static Future<Void> addMutation(LogFileWriter* self, Version commitVersion, MutationListRef mutations) {
		state Standalone<StringRef> value = BinaryWriter::toValue(mutations, IncludeVersion());

		state int part = 0;
		for (; part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE < value.size(); part++) {
			StringRef partBuf = value.substr(
			    part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE,
			    std::min(value.size() - part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE, CLIENT_KNOBS->MUTATION_BLOCK_SIZE));
			Standalone<StringRef> key = getBlockKey(commitVersion, part);
			wait(writeKV_impl(self, key, partBuf));
		}
		return Void();
	}

private:
	Reference<IBackupFile> file;
	int blockSize;
	int64_t blockEnd = 0;
	const uint32_t fileVersion = 2001;
};

ACTOR Future<Void> test_container(ConvertParams params) {
	state Reference<IBackupContainer> container = IBackupContainer::openContainer(params.container_url);
	state BackupFileList listing = wait(container->dumpFileList());
	std::sort(listing.logs.begin(), listing.logs.end());
	TraceEvent("Container").detail("URL", params.container_url).detail("Logs", listing.logs.size());
	state BackupDescription desc = wait(container->describeBackup());
	std::cout << "\n" << desc.toString() << "\n";

	std::vector<LogFile> logs = getRelevantLogFiles(listing.logs, params.begin, params.end);
	printLogFiles("Range has", logs);

	state Reference<MutationFilesReadProgress> progress(new MutationFilesReadProgress(logs, params.begin, params.end));

	wait(progress->openLogFiles(container));

	while (progress->hasMutations()) {
		VersionedData data = wait(progress->getNextMutation());
		// emit a mutation and write to a batch;
		std::cout << data.version.toString() << " m = " << data.mutation.toString() << "\n";
	}

	return Void();
}

int parseCommandLine(ConvertParams* param, CSimpleOpt* args) {
	while (args->Next()) {
		auto lastError = args->LastError();
		switch (lastError) {
		case SO_SUCCESS:
			break;

		default:
			fprintf(stderr, "ERROR: argument given for option `%s'\n", args->OptionText());
			return FDB_EXIT_ERROR;
			break;
		}

		int optId = args->OptionId();
		const char* arg = args->OptionArg();
		switch (optId) {
		case OPT_HELP:
			printConvertUsage();
			return FDB_EXIT_ERROR;

		case OPT_BEGIN_VERSION:
			if (!sscanf(arg, "%" SCNd64, &param->begin)) {
				std::cerr << "ERROR: could not parse begin version " << arg << "\n";
				printConvertUsage();
				return FDB_EXIT_ERROR;
			}
			break;

		case OPT_END_VERSION:
			if (!sscanf(arg, "%" SCNd64, &param->end)) {
				std::cerr << "ERROR: could not parse end version " << arg << "\n";
				printConvertUsage();
				return FDB_EXIT_ERROR;
			}
			break;

		case OPT_CONTAINER:
			param->container_url = args->OptionArg();
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

}  // namespace file_converter

int main(int argc, char** argv) {
	try {
		CSimpleOpt* args = new CSimpleOpt(argc, argv, file_converter::gConverterOptions, SO_O_EXACT);
		file_converter::ConvertParams param;
		int status = file_converter::parseCommandLine(&param, args);
		std::cout << "Params: " << param.toString() << "\n";
		if (status != FDB_EXIT_SUCCESS || !param.isValid()) {
			file_converter::printConvertUsage();
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
		openTraceFile(NetworkAddress(), 10 << 20, 10 << 20, param.log_dir, "convert", param.trace_log_group);

		auto f = stopAfter(test_container(param));

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