/*
 * BackupContainerLocalDirectory.actor.cpp
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

#include "fdbclient/BackupContainerLocalDirectory.h"
#include "fdbrpc/AsyncFileReadAhead.actor.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/Platform.actor.h"
#include "flow/Platform.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

class BackupFile : public IBackupFile, ReferenceCounted<BackupFile> {
public:
	BackupFile(const std::string& fileName, Reference<IAsyncFile> file, const std::string& finalFullPath)
	  : IBackupFile(fileName), m_file(file), m_writeOffset(0), m_finalFullPath(finalFullPath),
	    m_blockSize(CLIENT_KNOBS->BACKUP_LOCAL_FILE_WRITE_BLOCK) {
		if (BUGGIFY) {
			m_blockSize = deterministicRandom()->randomInt(100, 20000);
		}
		m_buffer.reserve(m_buffer.arena(), m_blockSize);
	}

	Future<Void> append(const void* data, int len) override {
		m_buffer.append(m_buffer.arena(), (const uint8_t*)data, len);

		if (m_buffer.size() >= m_blockSize) {
			return flush(m_blockSize);
		}

		return Void();
	}

	Future<Void> flush(int size) {
		// Avoid empty write
		if (size == 0) {
			return Void();
		}

		ASSERT(size <= m_buffer.size());

		// Keep a reference to the old buffer
		Standalone<VectorRef<uint8_t>> old = m_buffer;
		// Make a new buffer, initialized with the excess bytes over the block size from the old buffer
		m_buffer = Standalone<VectorRef<uint8_t>>(old.slice(size, old.size()));

		// Write the old buffer to the underlying file and update the write offset
		Future<Void> r = holdWhile(old, m_file->write(old.begin(), size, m_writeOffset));
		m_writeOffset += size;

		return r;
	}

	ACTOR static Future<Void> finish_impl(Reference<BackupFile> f) {
		wait(f->flush(f->m_buffer.size()));
		wait(f->m_file->truncate(f->size())); // Some IAsyncFile implementations extend in whole block sizes.
		wait(f->m_file->sync());
		std::string name = f->m_file->getFilename();
		f->m_file.clear();
		wait(IAsyncFileSystem::filesystem()->renameFile(name, f->m_finalFullPath));
		return Void();
	}

	int64_t size() const override { return m_buffer.size() + m_writeOffset; }

	Future<Void> finish() override { return finish_impl(Reference<BackupFile>::addRef(this)); }

	void addref() override { return ReferenceCounted<BackupFile>::addref(); }
	void delref() override { return ReferenceCounted<BackupFile>::delref(); }

private:
	Reference<IAsyncFile> m_file;
	Standalone<VectorRef<uint8_t>> m_buffer;
	int64_t m_writeOffset;
	std::string m_finalFullPath;
	int m_blockSize;
};

ACTOR static Future<BackupContainerFileSystem::FilesAndSizesT> listFiles_impl(std::string path, std::string m_path) {
	state std::vector<std::string> files;
	wait(platform::findFilesRecursivelyAsync(joinPath(m_path, path), &files));

	BackupContainerFileSystem::FilesAndSizesT results;

	// Remove .lnk files from results, they are a side effect of a backup that was *read* during simulation.  See
	// openFile() above for more info on why they are created.
	if (g_network->isSimulated())
		files.erase(
		    std::remove_if(files.begin(),
		                   files.end(),
		                   [](std::string const& f) { return StringRef(f).endsWith(LiteralStringRef(".lnk")); }),
		    files.end());

	for (const auto& f : files) {
		// Hide .part or .temp files.
		StringRef s(f);
		if (!s.endsWith(LiteralStringRef(".part")) && !s.endsWith(LiteralStringRef(".temp")))
			results.push_back({ f.substr(m_path.size() + 1), ::fileSize(f) });
	}

	return results;
}

} // namespace

void BackupContainerLocalDirectory::addref() {
	return ReferenceCounted<BackupContainerLocalDirectory>::addref();
}
void BackupContainerLocalDirectory::delref() {
	return ReferenceCounted<BackupContainerLocalDirectory>::delref();
}

std::string BackupContainerLocalDirectory::getURLFormat() {
	return "file://</path/to/base/dir/>";
}

BackupContainerLocalDirectory::BackupContainerLocalDirectory(const std::string& url,
                                                             const Optional<std::string>& encryptionKeyFileName) {
	setEncryptionKey(encryptionKeyFileName);

	std::string path;
	if (url.find("file://") != 0) {
		TraceEvent(SevWarn, "BackupContainerLocalDirectory")
		    .detail("Description", "Invalid URL for BackupContainerLocalDirectory")
		    .detail("URL", url);
	}

	path = url.substr(7);
	// Remove trailing slashes on path
	path.erase(path.find_last_not_of("\\/") + 1);

	std::string absolutePath = abspath(path);

	if (!g_network->isSimulated() && path != absolutePath) {
		TraceEvent(SevWarn, "BackupContainerLocalDirectory")
		    .detail("Description", "Backup path must be absolute (e.g. file:///some/path)")
		    .detail("URL", url)
		    .detail("Path", path)
		    .detail("AbsolutePath", absolutePath);
		// throw io_error();
		IBackupContainer::lastOpenError =
		    format("Backup path '%s' must be the absolute path '%s'", path.c_str(), absolutePath.c_str());
		throw backup_invalid_url();
	}

	// Finalized path written to will be will be <path>/backup-<uid>
	m_path = path;
}

Future<std::vector<std::string>> BackupContainerLocalDirectory::listURLs(const std::string& url) {
	std::string path;
	if (url.find("file://") != 0) {
		TraceEvent(SevWarn, "BackupContainerLocalDirectory")
		    .detail("Description", "Invalid URL for BackupContainerLocalDirectory")
		    .detail("URL", url);
	}

	path = url.substr(7);
	// Remove trailing slashes on path
	path.erase(path.find_last_not_of("\\/") + 1);

	if (!g_network->isSimulated() && path != abspath(path)) {
		TraceEvent(SevWarn, "BackupContainerLocalDirectory")
		    .detail("Description", "Backup path must be absolute (e.g. file:///some/path)")
		    .detail("URL", url)
		    .detail("Path", path);
		throw io_error();
	}
	std::vector<std::string> dirs = platform::listDirectories(path);
	std::vector<std::string> results;

	for (const auto& r : dirs) {
		if (r == "." || r == "..")
			continue;
		results.push_back(std::string("file://") + joinPath(path, r));
	}

	return results;
}

Future<Void> BackupContainerLocalDirectory::create() {
	if (usesEncryption()) {
		return encryptionSetupComplete();
	}
	// No directory should be created here because create() can be called by any process working with the container URL,
	// such as fdbbackup. Since "local directory" containers are by definition local to the machine they are
	// accessed from, the container's creation (in this case the creation of a directory) must be ensured prior to
	// every file creation, which is done in openFile(). Creating the directory here will result in unnecessary
	// directories being created on machines that run fdbbackup but not agents.
	return Void();
}

Future<bool> BackupContainerLocalDirectory::exists() {
	return directoryExists(m_path);
}

Future<Reference<IAsyncFile>> BackupContainerLocalDirectory::readFile(const std::string& path) {
	int flags = IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED;
	if (usesEncryption()) {
		flags |= IAsyncFile::OPEN_ENCRYPTED;
	}
	// Simulation does not properly handle opening the same file from multiple machines using a shared filesystem,
	// so create a symbolic link to make each file opening appear to be unique.  This could also work in production
	// but only if the source directory is writeable which shouldn't be required for a restore.
	std::string fullPath = joinPath(m_path, path);
#ifndef _WIN32
	if (g_network->isSimulated()) {
		if (!fileExists(fullPath)) {
			throw file_not_found();
		}

		if (g_simulator.getCurrentProcess()->uid == UID()) {
			TraceEvent(SevError, "BackupContainerReadFileOnUnsetProcessID").log();
		}
		std::string uniquePath = fullPath + "." + g_simulator.getCurrentProcess()->uid.toString() + ".lnk";
		unlink(uniquePath.c_str());
		ASSERT(symlink(basename(path).c_str(), uniquePath.c_str()) == 0);
		fullPath = uniquePath;
	}
// Opening cached mode forces read/write mode at a lower level, overriding the readonly request.  So cached mode
// can't be used because backup files are read-only.  Cached mode can only help during restore task retries handled
// by the same process that failed the first task execution anyway, which is a very rare case.
#endif
	Future<Reference<IAsyncFile>> f = IAsyncFileSystem::filesystem()->open(fullPath, flags, 0644);

	if (g_network->isSimulated()) {
		int blockSize = 0;
		// Extract block size from the filename, if present
		size_t lastComma = path.find_last_of(',');
		if (lastComma != path.npos) {
			blockSize = atoi(path.substr(lastComma + 1).c_str());
		}
		if (blockSize <= 0) {
			blockSize = deterministicRandom()->randomInt(1e4, 1e6);
		}
		if (deterministicRandom()->random01() < .01) {
			blockSize /= deterministicRandom()->randomInt(1, 3);
		}
		ASSERT(blockSize > 0);

		return map(f, [=](Reference<IAsyncFile> fr) {
			int readAhead = deterministicRandom()->randomInt(0, 3);
			int reads = deterministicRandom()->randomInt(1, 3);
			int cacheSize = deterministicRandom()->randomInt(0, 3);
			return Reference<IAsyncFile>(new AsyncFileReadAheadCache(fr, blockSize, readAhead, reads, cacheSize));
		});
	}

	return f;
}

Future<Reference<IBackupFile>> BackupContainerLocalDirectory::writeFile(const std::string& path) {
	int flags = IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_CREATE |
	            IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE;
	if (usesEncryption()) {
		flags |= IAsyncFile::OPEN_ENCRYPTED;
	}
	std::string fullPath = joinPath(m_path, path);
	platform::createDirectory(parentDirectory(fullPath));
	std::string temp = fullPath + "." + deterministicRandom()->randomUniqueID().toString() + ".temp";
	Future<Reference<IAsyncFile>> f = IAsyncFileSystem::filesystem()->open(temp, flags, 0644);
	return map(f, [=](Reference<IAsyncFile> f) { return Reference<IBackupFile>(new BackupFile(path, f, fullPath)); });
}

Future<Void> BackupContainerLocalDirectory::deleteFile(const std::string& path) {
	::deleteFile(joinPath(m_path, path));
	return Void();
}

Future<BackupContainerFileSystem::FilesAndSizesT> BackupContainerLocalDirectory::listFiles(
    const std::string& path,
    std::function<bool(std::string const&)>) {
	return listFiles_impl(path, m_path);
}

Future<Void> BackupContainerLocalDirectory::deleteContainer(int* pNumDeleted) {
	// In order to avoid deleting some random directory due to user error, first describe the backup
	// and make sure it has something in it.
	return map(describeBackup(false, invalidVersion), [=](BackupDescription const& desc) {
		// If the backup has no snapshots and no logs then it's probably not a valid backup
		if (desc.snapshots.size() == 0 && !desc.minLogBegin.present())
			throw backup_invalid_url();

		int count = platform::eraseDirectoryRecursive(m_path);
		if (pNumDeleted != nullptr)
			*pNumDeleted = count;

		return Void();
	});
}
