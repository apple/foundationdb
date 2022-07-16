/*
 * FileConverter.actor.cpp
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
#include "fdbclient/BuildFlags.h"
#include "flow/actorcompiler.h" // has to be last include

namespace file_converter {

void printConvertUsage() {
	std::cout << "\n"
	          << "  -r, --container Container URL.\n"
	          << "  -b, --begin BEGIN\n"
	          << "                  Begin version.\n"
	          << "  -e, --end END   End version.\n"
	          << "  --log           Enables trace file logging for the CLI session.\n"
	          << "  --logdir PATH   Specifes the output directory for trace files. If\n"
	          << "                  unspecified, defaults to the current directory. Has\n"
	          << "                  no effect unless --log is specified.\n"
	          << "  --loggroup LOG_GROUP\n"
	          << "                  Sets the LogGroup field with the specified value for all\n"
	          << "                  events in the trace output (defaults to `default').\n"
	          << "  --trace-format FORMAT\n"
	          << "                  Select the format of the trace files. xml (the default) and json are supported.\n"
	          << "                  Has no effect unless --log is specified.\n"
	          << "  --build-flags   Print build information and exit.\n"
	          << "  -h, --help      Display this help and exit.\n"
	          << "\n";

	return;
}

void printBuildInformation() {
	printf("%s", jsonBuildInformation().c_str());
}

void printLogFiles(std::string msg, const std::vector<LogFile>& files) {
	std::cout << msg << " " << files.size() << " log files\n";
	for (const auto& file : files) {
		std::cout << file.toString() << "\n";
	}
	std::cout << std::endl;
}

std::vector<LogFile> getRelevantLogFiles(const std::vector<LogFile>& files, Version begin, Version end) {
	std::vector<LogFile> filtered;
	for (const auto& file : files) {
		if (file.beginVersion <= end && file.endVersion >= begin && file.tagId >= 0 && file.fileSize > 0) {
			filtered.push_back(file);
		}
	}
	std::sort(filtered.begin(), filtered.end());

	// Remove duplicates. This is because backup workers may store the log for
	// old epochs successfully, but do not update the progress before another
	// recovery happened. As a result, next epoch will retry and creates
	// duplicated log files.
	std::vector<LogFile> sorted;
	int i = 0;
	for (int j = 1; j < filtered.size(); j++) {
		if (!filtered[i].isSubset(filtered[j])) {
			sorted.push_back(filtered[i]);
		}
		i = j;
	}
	if (i < filtered.size()) {
		sorted.push_back(filtered[i]);
	}

	return sorted;
}

struct ConvertParams {
	std::string container_url;
	Optional<std::string> proxy;
	Version begin = invalidVersion;
	Version end = invalidVersion;
	bool log_enabled = false;
	std::string log_dir, trace_format, trace_log_group;

	bool isValid() { return begin != invalidVersion && end != invalidVersion && !container_url.empty(); }

	std::string toString() {
		std::string s;
		s.append("ContainerURL:");
		s.append(container_url);
		if (proxy.present()) {
			s.append(" Proxy:");
			s.append(proxy.get());
		}
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
	StringRef message; // Serialized mutation.
	Arena arena; // The arena that contains mutation.

	VersionedData() : version(invalidVersion, -1) {}
	VersionedData(LogMessageVersion v, StringRef m, Arena a) : version(v), message(m), arena(a) {}
};

struct MutationFilesReadProgress : public ReferenceCounted<MutationFilesReadProgress> {
	MutationFilesReadProgress(std::vector<LogFile>& logs, Version begin, Version end)
	  : files(logs), beginVersion(begin), endVersion(end) {}

	struct FileProgress : public ReferenceCounted<FileProgress> {
		FileProgress(Reference<IAsyncFile> f, int index) : fd(f), idx(index), offset(0), eof(false) {}

		bool operator<(const FileProgress& rhs) const {
			if (rhs.mutations.empty())
				return true;
			if (mutations.empty())
				return false;
			return mutations[0].version < rhs.mutations[0].version;
		}
		bool operator<=(const FileProgress& rhs) const {
			if (rhs.mutations.empty())
				return true;
			if (mutations.empty())
				return false;
			return mutations[0].version <= rhs.mutations[0].version;
		}
		bool empty() { return eof && mutations.empty(); }

		// Decodes the block into mutations and save them if >= minVersion and < maxVersion.
		// Returns true if new mutations has been saved.
		bool decodeBlock(const Standalone<StringRef>& buf, int len, Version minVersion, Version maxVersion) {
			StringRef block(buf.begin(), len);
			StringRefReader reader(block, restore_corrupted_data());
			int count = 0, inserted = 0;
			Version msgVersion = invalidVersion;

			try {
				// Read block header
				if (reader.consume<int32_t>() != PARTITIONED_MLOG_VERSION)
					throw restore_unsupported_file_version();

				while (1) {
					// If eof reached or first key len bytes is 0xFF then end of block was reached.
					if (reader.eof() || *reader.rptr == 0xFF)
						break;

					// Deserialize messages written in saveMutationsToFile().
					msgVersion = bigEndian64(reader.consume<Version>());
					uint32_t sub = bigEndian32(reader.consume<uint32_t>());
					int msgSize = bigEndian32(reader.consume<int>());
					const uint8_t* message = reader.consume(msgSize);

					ArenaReader rd(
					    buf.arena(), StringRef(message, msgSize), AssumeVersion(g_network->protocolVersion()));
					MutationRef m;
					rd >> m;
					count++;
					if (msgVersion >= maxVersion) {
						TraceEvent("FileDecodeEnd")
						    .detail("MaxV", maxVersion)
						    .detail("Version", msgVersion)
						    .detail("File", fd->getFilename());
						eof = true;
						break; // skip
					}
					if (msgVersion >= minVersion) {
						mutations.emplace_back(
						    LogMessageVersion(msgVersion, sub), StringRef(message, msgSize), buf.arena());
						inserted++;
					}
				}
				offset += len;

				TraceEvent("Decoded")
				    .detail("Name", fd->getFilename())
				    .detail("Count", count)
				    .detail("Insert", inserted)
				    .detail("BlockOffset", reader.rptr - buf.begin())
				    .detail("Total", mutations.size())
				    .detail("EOF", eof)
				    .detail("Version", msgVersion)
				    .detail("NewOffset", offset);
				return inserted > 0;
			} catch (Error& e) {
				TraceEvent(SevWarn, "CorruptLogFileBlock")
				    .error(e)
				    .detail("Filename", fd->getFilename())
				    .detail("BlockOffset", offset)
				    .detail("BlockLen", len)
				    .detail("ErrorRelativeOffset", reader.rptr - buf.begin())
				    .detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset);
				throw;
			}
		}

		Reference<IAsyncFile> fd;
		int idx; // index in the MutationFilesReadProgress::files vector
		int64_t offset; // offset of the file to be read
		bool eof; // If EOF is seen so far or endVersion is encountered. If true, the file can't be read further.
		std::vector<VersionedData> mutations; // Buffered mutations read so far
	};

	bool hasMutations() {
		for (const auto& fp : fileProgress) {
			if (!fp->empty())
				return true;
		}
		return false;
	}

	void dumpProgress(std::string msg) {
		std::cout << msg << "\n  ";
		for (const auto& fp : fileProgress) {
			std::cout << fp->fd->getFilename() << " " << fp->mutations.size() << " mutations";
			if (fp->mutations.size() > 0) {
				std::cout << ", range " << fp->mutations[0].version.toString() << " "
				          << fp->mutations.back().version.toString() << "\n";
			} else {
				std::cout << "\n\n";
			}
		}
	}

	// Sorts files according to their first mutation version and removes files without mutations.
	void sortAndRemoveEmpty() {
		std::sort(fileProgress.begin(),
		          fileProgress.end(),
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
			wait(decodeToVersion(fp, /*version=*/0, self->endVersion, self->getLogFile(fp->idx)));
		}

		if (fp->empty()) {
			self->fileProgress.erase(self->fileProgress.begin());
		} else {
			// Keep fileProgress sorted
			for (int i = 1; i < self->fileProgress.size(); i++) {
				if (*self->fileProgress[i - 1] <= *self->fileProgress[i]) {
					break;
				}
				std::swap(self->fileProgress[i - 1], self->fileProgress[i]);
			}
		}
		return data;
	}

	LogFile& getLogFile(int index) { return files[index]; }

	Future<Void> openLogFiles(Reference<IBackupContainer> container) { return openLogFilesImpl(this, container); }

	// Opens log files in the progress and starts decoding until the beginVersion is seen.
	ACTOR static Future<Void> openLogFilesImpl(MutationFilesReadProgress* progress,
	                                           Reference<IBackupContainer> container) {
		state std::vector<Future<Reference<IAsyncFile>>> asyncFiles;
		for (const auto& file : progress->files) {
			asyncFiles.push_back(container->readFile(file.fileName));
		}
		wait(waitForAll(asyncFiles)); // open all files

		// Attempt decode the first few blocks of log files until beginVersion is consumed
		std::vector<Future<Void>> fileDecodes;
		for (int i = 0; i < asyncFiles.size(); i++) {
			auto fp = makeReference<FileProgress>(asyncFiles[i].get(), i);
			progress->fileProgress.push_back(fp);
			fileDecodes.push_back(
			    decodeToVersion(fp, progress->beginVersion, progress->endVersion, progress->getLogFile(i)));
		}

		wait(waitForAll(fileDecodes));

		progress->sortAndRemoveEmpty();

		return Void();
	}

	// Decodes the file until EOF or an mutation >= minVersion and saves these mutations.
	// Skip mutations >= maxVersion.
	ACTOR static Future<Void> decodeToVersion(Reference<FileProgress> fp,
	                                          Version minVersion,
	                                          Version maxVersion,
	                                          LogFile file) {
		if (fp->empty())
			return Void();

		if (!fp->mutations.empty() && fp->mutations.back().version.version >= minVersion)
			return Void();

		state int64_t len;
		try {
			// Read block by block until we see the minVersion
			loop {
				len = std::min<int64_t>(file.blockSize, file.fileSize - fp->offset);
				if (len == 0) {
					fp->eof = true;
					return Void();
				}

				state Standalone<StringRef> buf = makeString(len);
				int rLen = wait(fp->fd->read(mutateString(buf), len, fp->offset));
				if (len != rLen)
					throw restore_bad_read();

				TraceEvent("ReadFile")
				    .detail("Name", fp->fd->getFilename())
				    .detail("Length", rLen)
				    .detail("Offset", fp->offset);
				if (fp->decodeBlock(buf, rLen, minVersion, maxVersion))
					break;
			}
			return Void();
		} catch (Error& e) {
			TraceEvent(SevWarn, "CorruptedLogFileBlock")
			    .error(e)
			    .detail("Filename", fp->fd->getFilename())
			    .detail("BlockOffset", fp->offset)
			    .detail("BlockLen", len);
			throw;
		}
	}

	std::vector<LogFile> files;
	const Version beginVersion, endVersion;
	std::vector<Reference<FileProgress>> fileProgress;
};

// Writes a log file in the old backup format, described in backup-dataFormat.md.
// This is similar to the LogFileWriter in FileBackupAgent.actor.cpp.
struct LogFileWriter {
	LogFileWriter() : blockSize(-1) {}
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

	// Start a new block if needed, then write the key and value
	ACTOR static Future<Void> writeKV_impl(LogFileWriter* self, Key k, Value v) {
		// If key and value do not fit in this block, end it and start a new one
		int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
		if (self->file->size() + toWrite > self->blockEnd) {
			// Write padding if needed
			int bytesLeft = self->blockEnd - self->file->size();
			if (bytesLeft > 0) {
				state Value paddingFFs = fileBackup::makePadding(bytesLeft);
				wait(self->file->append(paddingFFs.begin(), bytesLeft));
			}

			// Set new blockEnd
			self->blockEnd += self->blockSize;

			// write Header
			wait(self->file->append((uint8_t*)&BACKUP_AGENT_MLOG_VERSION, sizeof(BACKUP_AGENT_MLOG_VERSION)));
		}

		wait(self->file->appendStringRefWithLen(k));
		wait(self->file->appendStringRefWithLen(v));

		// At this point we should be in whatever the current block is or the block size is too small
		if (self->file->size() > self->blockEnd)
			throw backup_bad_block_size();

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
};

ACTOR Future<Void> convert(ConvertParams params) {
	state Reference<IBackupContainer> container =
	    IBackupContainer::openContainer(params.container_url, params.proxy, {});
	state BackupFileList listing = wait(container->dumpFileList());
	std::sort(listing.logs.begin(), listing.logs.end());
	TraceEvent("Container").detail("URL", params.container_url).detail("Logs", listing.logs.size());
	state BackupDescription desc = wait(container->describeBackup());
	std::cout << "\n" << desc.toString() << "\n";

	// std::cout << "Using Protocol Version: 0x" << std::hex << g_network->protocolVersion().version() << std::dec <<
	// "\n";

	std::vector<LogFile> logs = getRelevantLogFiles(listing.logs, params.begin, params.end);
	printLogFiles("Range has", logs);

	state Reference<MutationFilesReadProgress> progress(new MutationFilesReadProgress(logs, params.begin, params.end));

	wait(progress->openLogFiles(container));

	state int blockSize = CLIENT_KNOBS->BACKUP_LOGFILE_BLOCK_SIZE;
	state Reference<IBackupFile> outFile = wait(container->writeLogFile(params.begin, params.end, blockSize));
	state LogFileWriter logFile(outFile, blockSize);
	std::cout << "Output file: " << outFile->getFileName() << "\n";

	state MutationList list;
	state Arena arena;
	state Version version = invalidVersion;
	while (progress->hasMutations()) {
		state VersionedData data = wait(progress->getNextMutation());

		// emit a mutation batch to file when encounter a new version
		if (list.totalSize() > 0 && version != data.version.version) {
			wait(LogFileWriter::addMutation(&logFile, version, list));
			list = MutationList();
			arena = Arena();
		}

		ArenaReader rd(data.arena, data.message, AssumeVersion(g_network->protocolVersion()));
		MutationRef m;
		rd >> m;
		std::cout << data.version.toString() << " m = " << m.toString() << "\n";
		list.push_back_deep(arena, m);
		version = data.version.version;
	}
	if (list.totalSize() > 0) {
		wait(LogFileWriter::addMutation(&logFile, version, list));
	}

	wait(outFile->finish());

	return Void();
}

int parseCommandLine(ConvertParams* param, CSimpleOpt* args) {
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
		case OPT_BUILD_FLAGS:
			printBuildInformation();
			return FDB_EXIT_ERROR;
			break;
		}
	}
	return FDB_EXIT_SUCCESS;
}

} // namespace file_converter

int main(int argc, char** argv) {
	try {
		CSimpleOpt* args =
		    new CSimpleOpt(argc, argv, file_converter::gConverterOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
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
		setupNetwork(0, UseMetrics::True);

		TraceEvent::setNetworkThread();
		openTraceFile(NetworkAddress(), 10 << 20, 10 << 20, param.log_dir, "convert", param.trace_log_group);

		auto f = stopAfter(convert(param));

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
