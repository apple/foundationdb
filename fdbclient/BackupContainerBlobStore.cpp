/*
 * BackupContainerBlobStore.cpp
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

#include "fdbclient/AsyncFileBlobStore.h"
#include "BackupContainerBlobStore.h"
#include "fdbclient/IBlobStore.h"
#include "fdbrpc/AsyncFileEncrypted.h"
#include "fdbrpc/AsyncFileReadAhead.h"
#include "fdbrpc/HTTP.h"

class BackupContainerBlobStoreImpl {
public:
	// Backup files to under a single folder prefix with subfolders for each named backup
	static const std::string DATAFOLDER;

	// Indexfolder contains keys for which user-named backups exist.  Backup names can contain an arbitrary
	// number of slashes so the backup names are kept in a separate folder tree from their actual data.
	static const std::string INDEXFOLDER;

	static Future<std::vector<std::string>> listURLs(Reference<IBlobStoreEndpoint> bstore, std::string bucket) {
		std::string basePath = INDEXFOLDER + '/';
		IBlobStoreEndpoint::ListResult contents = co_await bstore->listObjects(bucket, basePath);
		std::vector<std::string> results;
		for (const auto& f : contents.objects) {
			// URL decode the object name since S3 XML responses contain URL-encoded names
			std::string decodedName = HTTP::urlDecode(f.name);
			results.push_back(
			    bstore->getResourceURL(decodedName.substr(basePath.size()), format("bucket=%s", bucket.c_str())));
		}
		co_return results;
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

	static Future<BackupContainerFileSystem::FilesAndSizesT> listFiles(
	    Reference<BackupContainerBlobStore> bc,
	    std::string path,
	    std::function<bool(std::string const&)> pathFilter) {
		// pathFilter expects container based paths, so create a wrapper which converts a raw path
		// to a container path by removing the known backup name prefix.
		int prefixTrim = bc->dataPath("").size();
		std::function<bool(std::string const&)> rawPathFilter = [=](const std::string& folderPath) {
			ASSERT(folderPath.size() >= prefixTrim);
			return pathFilter(folderPath.substr(prefixTrim));
		};

		// Use flat listing for backup files to ensure all files are found regardless of directory structure
		IBlobStoreEndpoint::ListResult result =
		    co_await bc->m_bstore->listObjects(bc->m_bucket, bc->dataPath(path), Optional<char>(), 0, rawPathFilter);
		BackupContainerFileSystem::FilesAndSizesT files;
		for (const auto& o : result.objects) {
			ASSERT(o.name.size() >= prefixTrim);
			// URL decode the object name since S3 XML responses contain URL-encoded names
			std::string decodedName = HTTP::urlDecode(o.name);
			files.push_back({ decodedName.substr(prefixTrim), o.size });
		}
		co_return files;
	}

	static Future<Void> create(Reference<BackupContainerBlobStore> bc) {
		co_await bc->m_bstore->createBucket(bc->m_bucket);

		// Check/create the index entry
		bool exists = co_await bc->m_bstore->objectExists(bc->m_bucket, bc->indexEntry());
		if (!exists) {
			co_await bc->m_bstore->writeEntireFile(bc->m_bucket, bc->indexEntry(), "");
		}

		if (bc->usesEncryption()) {
			co_await bc->encryptionSetupComplete();
		}
	}

	static Future<Void> deleteContainer(Reference<BackupContainerBlobStore> bc, int* pNumDeleted) {
		bool e = co_await bc->exists();
		if (!e) {
			TraceEvent(SevWarnAlways, "BackupContainerDoesNotExist").detail("URL", bc->getURL());
			throw backup_does_not_exist();
		}

		// First delete everything under the data prefix in the bucket
		co_await bc->m_bstore->deleteRecursively(bc->m_bucket, bc->dataPath(""), pNumDeleted);

		// Now that all files are deleted, delete the index entry
		co_await bc->m_bstore->deleteObject(bc->m_bucket, bc->indexEntry());
	}
};

const std::string BackupContainerBlobStoreImpl::DATAFOLDER = "data";
const std::string BackupContainerBlobStoreImpl::INDEXFOLDER = "backups";

std::string BackupContainerBlobStore::dataPath(const std::string& path) {
	// if backup, include the backup data prefix.
	// if m_name ends in a trailing slash, don't add another
	std::string dataPath = "";
	if (isBackup) {
		dataPath = BackupContainerBlobStoreImpl::DATAFOLDER + "/";
	}
	if (!m_name.empty() && m_name.back() == '/') {
		dataPath += m_name + path;
	} else {
		dataPath += m_name + "/" + path;
	}
	return dataPath;
}

// Get the path of the backups's index entry
std::string BackupContainerBlobStore::indexEntry() {
	ASSERT(isBackup);
	return BackupContainerBlobStoreImpl::INDEXFOLDER + "/" + m_name;
}

BackupContainerBlobStore::BackupContainerBlobStore(Reference<IBlobStoreEndpoint> bstore,
                                                   const std::string& name,
                                                   const IBlobStoreEndpoint::ParametersT& params,
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
		TraceEvent(SevWarn, "BackupContainerBlobStoreInvalidParameter").detail("Name", name).detail("Value", value);
		IBackupContainer::lastOpenError = format("Unknown URL parameter: '%s'", name.c_str());
		throw backup_invalid_url();
	}
}

void BackupContainerBlobStore::addref() {
	return ReferenceCounted<BackupContainerBlobStore>::addref();
}
void BackupContainerBlobStore::delref() {
	return ReferenceCounted<BackupContainerBlobStore>::delref();
}

std::string BackupContainerBlobStore::getURLFormat(bool withResource) {
	return IBlobStoreEndpoint::getURLFormat(withResource) + " (Note: The 'bucket' parameter is required.)";
}

void BackupContainerBlobStore::validateBackupUrl(const std::string& resource) {
	if (resource.empty())
		throw backup_invalid_url();

	for (auto c : resource)
		if (!isalnum(static_cast<unsigned char>(c)) && c != '_' && c != '-' && c != '.' && c != '/')
			throw backup_invalid_url();
}

Future<Reference<IAsyncFile>> BackupContainerBlobStore::readFile(const std::string& path) {
	Reference<IAsyncFile> f = makeReference<AsyncFileBlobStoreRead>(m_bstore, m_bucket, dataPath(path));

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

Future<std::vector<std::string>> BackupContainerBlobStore::listURLs(Reference<IBlobStoreEndpoint> bstore,
                                                                    const std::string& bucket) {
	return BackupContainerBlobStoreImpl::listURLs(bstore, bucket);
}

Future<Reference<IBackupFile>> BackupContainerBlobStore::writeFile(const std::string& path) {
	Reference<IAsyncFile> f = makeReference<AsyncFileBlobStoreWrite>(m_bstore, m_bucket, dataPath(path));
	if (usesEncryption() && !StringRef(path).startsWith("properties/"_sr)) {
		f = makeReference<AsyncFileEncrypted>(f, AsyncFileEncrypted::Mode::APPEND_ONLY);
	}
	return Future<Reference<IBackupFile>>(makeReference<BackupContainerBlobStoreImpl::BackupFile>(path, f));
}

Future<Void> BackupContainerBlobStore::writeEntireFile(const std::string& path, const std::string& fileContents) {
	return m_bstore->writeEntireFile(m_bucket, dataPath(path), fileContents);
}

Future<Void> BackupContainerBlobStore::deleteFile(const std::string& path) {
	return m_bstore->deleteObject(m_bucket, dataPath(path));
}

Future<BackupContainerFileSystem::FilesAndSizesT> BackupContainerBlobStore::listFiles(
    const std::string& path,
    std::function<bool(std::string const&)> pathFilter) {
	return BackupContainerBlobStoreImpl::listFiles(Reference<BackupContainerBlobStore>::addRef(this), path, pathFilter);
}

Future<Void> BackupContainerBlobStore::create() {
	return BackupContainerBlobStoreImpl::create(Reference<BackupContainerBlobStore>::addRef(this));
}

Future<bool> BackupContainerBlobStore::exists() {
	return m_bstore->objectExists(m_bucket, indexEntry());
}

Future<Void> BackupContainerBlobStore::deleteContainer(int* pNumDeleted) {
	return BackupContainerBlobStoreImpl::deleteContainer(Reference<BackupContainerBlobStore>::addRef(this),
	                                                     pNumDeleted);
}

std::string BackupContainerBlobStore::getBucket() const {
	return m_bucket;
}
