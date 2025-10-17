/*
 * BackupContainerS3BlobStore.actor.cpp
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

#include "fdbclient/AsyncFileS3BlobStore.actor.h"
#include "fdbclient/BackupContainerS3BlobStore.h"
#include "fdbrpc/AsyncFileEncrypted.h"
#include "fdbrpc/AsyncFileReadAhead.actor.h"
#include "fdbrpc/HTTP.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class BackupContainerS3BlobStoreImpl {
public:
	// Backup files to under a single folder prefix with subfolders for each named backup
	static const std::string DATAFOLDER;

	// Indexfolder contains keys for which user-named backups exist.  Backup names can contain an arbitrary
	// number of slashes so the backup names are kept in a separate folder tree from their actual data.
	static const std::string INDEXFOLDER;

	ACTOR static Future<std::vector<std::string>> listURLs(Reference<S3BlobStoreEndpoint> bstore, std::string bucket) {
		state std::string basePath = INDEXFOLDER + '/';
		S3BlobStoreEndpoint::ListResult contents = wait(bstore->listObjects(bucket, basePath));
		std::vector<std::string> results;
		for (const auto& f : contents.objects) {
			// URL decode the object name since S3 XML responses contain URL-encoded names
			std::string decodedName = HTTP::urlDecode(f.name);
			results.push_back(
			    bstore->getResourceURL(decodedName.substr(basePath.size()), format("bucket=%s", bucket.c_str())));
		}
		return results;
	}

	class BackupFile : public IBackupFile, ReferenceCounted<BackupFile> {
	public:
		BackupFile(std::string fileName, Reference<IAsyncFile> file)
		  : IBackupFile(fileName), m_file(file), m_offset(0) {}

		Future<Void> append(const void* data, int len) override {
			Future<Void> r = m_file->write(data, len, m_offset);
			m_offset += len;
			return r;
		}

		Future<Void> finish() override {
			Reference<BackupFile> self = Reference<BackupFile>::addRef(this);
			return map(m_file->sync(), [=](Void _) {
				self->m_file.clear();
				return Void();
			});
		}

		int64_t size() const override { return m_offset; }

		void addref() final { return ReferenceCounted<BackupFile>::addref(); }
		void delref() final { return ReferenceCounted<BackupFile>::delref(); }

	private:
		Reference<IAsyncFile> m_file;
		int64_t m_offset;
	};

	ACTOR static Future<BackupContainerFileSystem::FilesAndSizesT> listFiles(
	    Reference<BackupContainerS3BlobStore> bc,
	    std::string path,
	    std::function<bool(std::string const&)> pathFilter) {
		// pathFilter expects container based paths, so create a wrapper which converts a raw path
		// to a container path by removing the known backup name prefix.
		state int prefixTrim = bc->dataPath("").size();
		std::function<bool(std::string const&)> rawPathFilter = [=](const std::string& folderPath) {
			ASSERT(folderPath.size() >= prefixTrim);
			return pathFilter(folderPath.substr(prefixTrim));
		};

		// Use flat listing for backup files to ensure all files are found regardless of directory structure
		state S3BlobStoreEndpoint::ListResult result =
		    wait(bc->m_bstore->listObjects(bc->m_bucket, bc->dataPath(path), Optional<char>(), 0, rawPathFilter));
		BackupContainerFileSystem::FilesAndSizesT files;
		for (const auto& o : result.objects) {
			ASSERT(o.name.size() >= prefixTrim);
			// URL decode the object name since S3 XML responses contain URL-encoded names
			std::string decodedName = HTTP::urlDecode(o.name);
			files.push_back({ decodedName.substr(prefixTrim), o.size });
		}
		return files;
	}

	ACTOR static Future<Void> create(Reference<BackupContainerS3BlobStore> bc) {
		wait(bc->m_bstore->createBucket(bc->m_bucket));

		// Check/create the index entry
		bool exists = wait(bc->m_bstore->objectExists(bc->m_bucket, bc->indexEntry()));
		if (!exists) {
			wait(bc->m_bstore->writeEntireFile(bc->m_bucket, bc->indexEntry(), ""));
		}

		if (bc->usesEncryption()) {
			wait(bc->encryptionSetupComplete());
		}

		return Void();
	}

	ACTOR static Future<Void> deleteContainer(Reference<BackupContainerS3BlobStore> bc, int* pNumDeleted) {
		bool e = wait(bc->exists());
		if (!e) {
			TraceEvent(SevWarnAlways, "BackupContainerDoesNotExist").detail("URL", bc->getURL());
			throw backup_does_not_exist();
		}

		// First delete everything under the data prefix in the bucket
		wait(bc->m_bstore->deleteRecursively(bc->m_bucket, bc->dataPath(""), pNumDeleted));

		// Now that all files are deleted, delete the index entry
		wait(bc->m_bstore->deleteObject(bc->m_bucket, bc->indexEntry()));

		return Void();
	}
};

const std::string BackupContainerS3BlobStoreImpl::DATAFOLDER = "data";
const std::string BackupContainerS3BlobStoreImpl::INDEXFOLDER = "backups";

std::string BackupContainerS3BlobStore::dataPath(const std::string& path) {
	// if backup, include the backup data prefix.
	// if m_name ends in a trailing slash, don't add another
	std::string dataPath = "";
	if (isBackup) {
		dataPath = BackupContainerS3BlobStoreImpl::DATAFOLDER + "/";
	}
	if (!m_name.empty() && m_name.back() == '/') {
		dataPath += m_name + path;
	} else {
		dataPath += m_name + "/" + path;
	}
	return dataPath;
}

// Get the path of the backups's index entry
std::string BackupContainerS3BlobStore::indexEntry() {
	ASSERT(isBackup);
	return BackupContainerS3BlobStoreImpl::INDEXFOLDER + "/" + m_name;
}

BackupContainerS3BlobStore::BackupContainerS3BlobStore(Reference<S3BlobStoreEndpoint> bstore,
                                                       const std::string& name,
                                                       const S3BlobStoreEndpoint::ParametersT& params,
                                                       const Optional<std::string>& encryptionKeyFileName,
                                                       bool isBackup)
  : m_bstore(bstore), m_name(name), m_bucket("FDB_BACKUPS_V2"), isBackup(isBackup) {
	setEncryptionKey(encryptionKeyFileName);
	// Currently only one parameter is supported, "bucket"
	for (const auto& [name, value] : params) {
		if (name == "bucket") {
			m_bucket = value;
			continue;
		}
		TraceEvent(SevWarn, "BackupContainerS3BlobStoreInvalidParameter").detail("Name", name).detail("Value", value);
		IBackupContainer::lastOpenError = format("Unknown URL parameter: '%s'", name.c_str());
		throw backup_invalid_url();
	}
}

void BackupContainerS3BlobStore::addref() {
	return ReferenceCounted<BackupContainerS3BlobStore>::addref();
}
void BackupContainerS3BlobStore::delref() {
	return ReferenceCounted<BackupContainerS3BlobStore>::delref();
}

std::string BackupContainerS3BlobStore::getURLFormat() {
	return S3BlobStoreEndpoint::getURLFormat(true) + " (Note: The 'bucket' parameter is required.)";
}

Future<Reference<IAsyncFile>> BackupContainerS3BlobStore::readFile(const std::string& path) {
	Reference<IAsyncFile> f = makeReference<AsyncFileS3BlobStoreRead>(m_bstore, m_bucket, dataPath(path));

	if (usesEncryption() && !StringRef(path).startsWith("properties/"_sr)) {
		f = makeReference<AsyncFileEncrypted>(f, AsyncFileEncrypted::Mode::READ_ONLY);
	}
	if (m_bstore->knobs.enable_read_cache) {
		f = makeReference<AsyncFileReadAheadCache>(f,
		                                           m_bstore->knobs.read_block_size,
		                                           m_bstore->knobs.read_ahead_blocks,
		                                           m_bstore->knobs.concurrent_reads_per_file,
		                                           m_bstore->knobs.read_cache_blocks_per_file);
	}
	return f;
}

Future<std::vector<std::string>> BackupContainerS3BlobStore::listURLs(Reference<S3BlobStoreEndpoint> bstore,
                                                                      const std::string& bucket) {
	return BackupContainerS3BlobStoreImpl::listURLs(bstore, bucket);
}

Future<Reference<IBackupFile>> BackupContainerS3BlobStore::writeFile(const std::string& path) {
	Reference<IAsyncFile> f = makeReference<AsyncFileS3BlobStoreWrite>(m_bstore, m_bucket, dataPath(path));
	if (usesEncryption() && !StringRef(path).startsWith("properties/"_sr)) {
		f = makeReference<AsyncFileEncrypted>(f, AsyncFileEncrypted::Mode::APPEND_ONLY);
	}
	return Future<Reference<IBackupFile>>(makeReference<BackupContainerS3BlobStoreImpl::BackupFile>(path, f));
}

Future<Void> BackupContainerS3BlobStore::writeEntireFile(const std::string& path, const std::string& fileContents) {
	return m_bstore->writeEntireFile(m_bucket, dataPath(path), fileContents);
}

Future<Void> BackupContainerS3BlobStore::deleteFile(const std::string& path) {
	return m_bstore->deleteObject(m_bucket, dataPath(path));
}

Future<BackupContainerFileSystem::FilesAndSizesT> BackupContainerS3BlobStore::listFiles(
    const std::string& path,
    std::function<bool(std::string const&)> pathFilter) {
	return BackupContainerS3BlobStoreImpl::listFiles(
	    Reference<BackupContainerS3BlobStore>::addRef(this), path, pathFilter);
}

Future<Void> BackupContainerS3BlobStore::create() {
	return BackupContainerS3BlobStoreImpl::create(Reference<BackupContainerS3BlobStore>::addRef(this));
}

Future<bool> BackupContainerS3BlobStore::exists() {
	return m_bstore->objectExists(m_bucket, indexEntry());
}

Future<Void> BackupContainerS3BlobStore::deleteContainer(int* pNumDeleted) {
	return BackupContainerS3BlobStoreImpl::deleteContainer(Reference<BackupContainerS3BlobStore>::addRef(this),
	                                                       pNumDeleted);
}

std::string BackupContainerS3BlobStore::getBucket() const {
	return m_bucket;
}
