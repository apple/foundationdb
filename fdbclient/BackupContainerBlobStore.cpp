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

#include "AsyncFileBlobStore.h"
#include "BackupContainerBlobStore.h"
#include "fdbclient/IBlobStore.h"
#include "fdbrpc/AsyncFileEncrypted.h"
#include "fdbrpc/AsyncFileReadAhead.h"
#include "fdbrpc/HTTP.h"
#include "flow/Platform.h"
#include "flow/UnitTest.h"

#include <boost/algorithm/string/case_conv.hpp>

class BackupContainerBlobStoreImpl {
public:
	// Backup files to under a single folder prefix with subfolders for each named backup
	static const std::string DATAFOLDER;

	// Indexfolder contains keys for which user-named backups exist.  Backup names can contain an arbitrary
	// number of slashes so the backup names are kept in a separate folder tree from their actual data.
	static const std::string INDEXFOLDER;

	static Future<std::vector<std::string>> listURLs(Reference<IBlobStoreEndpoint> bstore,
	                                                 std::string bucket,
	                                                 std::string prefix) {
		std::string basePath = INDEXFOLDER + '/';
		std::string params = format("bucket=%s", bucket.c_str());
		if (!prefix.empty()) {
			basePath = prefix + "/" + basePath;
			params += format("&prefix=%s", prefix.c_str());
		}
		IBlobStoreEndpoint::ListResult contents = co_await bstore->listObjects(bucket, basePath);
		std::vector<std::string> results;
		for (const auto& f : contents.objects) {
			// URL decode the object name since S3 XML responses contain URL-encoded names
			std::string decodedName = HTTP::urlDecode(f.name);
			results.push_back(bstore->getResourceURL(decodedName.substr(basePath.size()), params));
		}
		co_return results;
	}

	class BackupFile : public IBackupFile, ReferenceCounted<BackupFile> {
	public:
		BackupFile(std::string fileName, Reference<IAsyncFile> file)
		  : IBackupFile(fileName), m_file(file), m_offset(0) {}

		Future<Void> appendImpl(const void* data, size_t len) override {
			Future<Void> r = m_file->write(data, static_cast<int>(len), m_offset);
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

std::string BackupContainerBlobStore::dataPath(const std::string& path) const {
	// if a key prefix was given, the entire layout lives under it.
	// if backup, include the backup data prefix.
	// if m_name ends in a trailing slash, don't add another
	std::string dataPath = "";
	if (!m_prefix.empty()) {
		dataPath = m_prefix + "/";
	}
	if (isBackup) {
		dataPath += BackupContainerBlobStoreImpl::DATAFOLDER + "/";
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
	std::string path = BackupContainerBlobStoreImpl::INDEXFOLDER + "/" + m_name;
	if (!m_prefix.empty()) {
		path = m_prefix + "/" + path;
	}
	return path;
}

BackupContainerBlobStore::BackupContainerBlobStore(Reference<IBlobStoreEndpoint> bstore,
                                                   const std::string& name,
                                                   const IBlobStoreEndpoint::ParametersT& params,
                                                   const Optional<std::string>& encryptionKeyFileName,
                                                   int encryptionBlockSize,
                                                   bool isBackup)
  : m_bstore(bstore), m_name(name), m_bucket("FDB_BACKUPS_V2"), isBackup(isBackup) {
	setEncryptionKey(encryptionKeyFileName);
	this->encryptionBlockSize = encryptionBlockSize;
	// Two parameters are supported, "bucket" and "prefix"
	for (const auto& [name, value] : params) {
		if (name == "bucket") {
			m_bucket = value;
			continue;
		}
		if (name == "prefix") {
			m_prefix = normalizePrefix(value, &IBackupContainer::lastOpenError);
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
	return IBlobStoreEndpoint::getURLFormat(withResource) +
	       " (Note: The 'bucket' parameter is required.  The optional 'prefix' parameter places the backup's "
	       "data and index folder trees under the given object key prefix instead of the bucket root.)";
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

	if (m_bstore->knobs.enable_read_cache) {
		f = makeReference<AsyncFileReadAheadCache>(f,
		                                           m_bstore->knobs.read_block_size,
		                                           m_bstore->knobs.read_ahead_blocks,
		                                           m_bstore->knobs.concurrent_reads_per_file,
		                                           m_bstore->knobs.read_cache_blocks_per_file);
	}
	if (usesEncryption() && !StringRef(path).startsWith("properties/"_sr)) {
		// Agents can open an existing container without calling create(), so key loading may still be in flight.
		return encryptedFile(f, AsyncFileEncrypted::Mode::READ_ONLY);
	}
	return f;
}

Future<std::vector<std::string>> BackupContainerBlobStore::listURLs(Reference<IBlobStoreEndpoint> bstore,
                                                                    const std::string& bucket,
                                                                    const std::string& prefix) {
	return BackupContainerBlobStoreImpl::listURLs(bstore, bucket, prefix);
}

Future<Reference<IBackupFile>> BackupContainerBlobStore::writeFile(const std::string& path) {
	Reference<IAsyncFile> rawFile = makeReference<AsyncFileBlobStoreWrite>(m_bstore, m_bucket, dataPath(path));
	Future<Reference<IAsyncFile>> f = rawFile;
	if (usesEncryption() && !StringRef(path).startsWith("properties/"_sr)) {
		f = encryptedFile(rawFile, AsyncFileEncrypted::Mode::APPEND_ONLY);
	}
	return map(f, [path](Reference<IAsyncFile> file) -> Reference<IBackupFile> {
		return makeReference<BackupContainerBlobStoreImpl::BackupFile>(path, file);
	});
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

std::string BackupContainerBlobStore::getPrefix() const {
	return m_prefix;
}

std::string BackupContainerBlobStore::getLocation() const {
	// Spell the service as a port, so a URL without one matches the same URL with the default port.
	std::string service = m_bstore->service.empty() ? m_bstore->defaultService() : m_bstore->service;
	if (service == "https") {
		service = "443";
	} else if (service == "http") {
		service = "80";
	}
	// Bucket names cannot contain '/', so the data path that follows keeps the location unambiguous.
	return format("blobstore://%s:%s/%s/%s",
	              boost::algorithm::to_lower_copy(m_bstore->host).c_str(),
	              service.c_str(),
	              m_bucket.c_str(),
	              dataPath("").c_str());
}

TEST_CASE("/backup/containers/blobstore/encryptionSetup") {
	std::string keyFile = joinPath(params.getDataDir(), "encryption-key");
	co_await BackupContainerFileSystem::createTestEncryptionKeyFile(keyFile);
	std::string resource;
	IBlobStoreEndpoint::ParametersT backupParams;
	auto endpoint = IBlobStoreEndpoint::fromString(
	    "blobstore://localhost:9999/encryption-setup?bucket=test", {}, &resource, nullptr, &backupParams);
	auto container = makeReference<BackupContainerBlobStore>(endpoint, resource, backupParams, keyFile, 4096, true);

	// An agent reopening a container does not call create(). No blob requests are needed to open these handles.
	auto read = container->readFile("range");
	auto write = container->writeFile("range");
	if (!container->encryptionSetupComplete().isReady()) {
		ASSERT(!read.isReady());
		ASSERT(!write.isReady());
	}
	co_await (success(read) && success(write));
	ASSERT(container->encryptionSetupComplete().isReady());
	ASSERT(!container->encryptionSetupComplete().isError());
}

TEST_CASE("/backup/containers/blobstore/prefix") {
	// Normalization: leading/trailing slashes are stripped, empty selects the default layout.
	ASSERT(BackupContainerBlobStore::normalizePrefix("").empty());
	ASSERT(BackupContainerBlobStore::normalizePrefix("/").empty());
	ASSERT(BackupContainerBlobStore::normalizePrefix("///").empty());
	ASSERT(BackupContainerBlobStore::normalizePrefix("a") == "a");
	ASSERT(BackupContainerBlobStore::normalizePrefix("/a/b/") == "a/b");
	ASSERT(BackupContainerBlobStore::normalizePrefix("tenant/data") == "tenant/data");
	ASSERT(BackupContainerBlobStore::normalizePrefix("my_instance/backups") == "my_instance/backups");
	ASSERT(BackupContainerBlobStore::normalizePrefix("v1/backups/backup-001.fdb") == "v1/backups/backup-001.fdb");

	// Invalid values: reserved first segments, empty, "." or ".." segments and disallowed characters.
	for (auto bad : { "data",
	                  "/data/x/",
	                  "backups",
	                  "/backups/x/",
	                  "a//b",
	                  ".",
	                  "..",
	                  "a/../b",
	                  "a/./b",
	                  "a b",
	                  "a?b",
	                  "a%2Fb" }) {
		try {
			BackupContainerBlobStore::normalizePrefix(bad);
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_backup_invalid_url);
		}
	}

	// URL parameter parsing: "prefix" is accepted and normalized, unknown parameters still throw.
	std::string resource;
	std::string error;
	IBlobStoreEndpoint::ParametersT backupParams;
	Reference<IBlobStoreEndpoint> bstore = IBlobStoreEndpoint::fromString(
	    "blobstore://ak:sk@localhost:9999/some/container?bucket=bkt&prefix=old&prefix=/p1/p2/",
	    {},
	    &resource,
	    &error,
	    &backupParams);
	ASSERT(bstore.isValid());
	ASSERT(resource == "some/container");
	auto c = makeReference<BackupContainerBlobStore>(bstore, resource, backupParams, Optional<std::string>(), 0, true);
	ASSERT(c->getBucket() == "bkt");
	ASSERT(c->getPrefix() == "p1/p2");

	// HTML entity decoding belongs to the component which reads a URL from HTML, not the URL parser.
	std::string htmlResource;
	IBlobStoreEndpoint::ParametersT htmlParams;
	Reference<IBlobStoreEndpoint> htmlStore = IBlobStoreEndpoint::fromString(
	    "blobstore://localhost:9999/some/container?bucket=bkt&amp;prefix=p", {}, &htmlResource, &error, &htmlParams);
	ASSERT(htmlStore.isValid());
	try {
		makeReference<BackupContainerBlobStore>(htmlStore, htmlResource, htmlParams, Optional<std::string>(), 0, true);
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_backup_invalid_url);
	}

	IBlobStoreEndpoint::ParametersT badParams = backupParams;
	badParams["prefix"] = "has space";
	try {
		makeReference<BackupContainerBlobStore>(bstore, resource, badParams, Optional<std::string>(), 0, true);
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_backup_invalid_url);
	}

	return Void();
}

TEST_CASE("/backup/containers/location") {
	// Non-AWS hosts need an explicit region to parse.
	auto location = [](const std::string& url) {
		return IBackupContainer::openContainer(url, {}, {}, 0)->getLocation();
	};
	const std::string base = location("blobstore://ak:sk@host.example.com:8080/name?bucket=bkt&prefix=p1/p2&region=r1");

	// Credentials, parameter order, other parameters, host case and slashes around the prefix are ignored.
	for (auto same :
	     { "blobstore://other:creds@host.example.com:8080/name?bucket=bkt&prefix=p1/p2&region=r1",
	       "blobstore://host.example.com:8080/name?region=r1&prefix=p1/p2&bucket=bkt",
	       "blobstore://ak:sk@HOST.example.com:8080/name?bucket=bkt&prefix=/p1/p2/&sc=0&region=r2",
	       "blobstore://ak:sk@host.example.com:8080/name?bucket=bkt&prefix=p1/p2&region=r1&header=x-a:b&cu=1",
	       // A trailing slash on the name does not change the data path.
	       "blobstore://ak:sk@host.example.com:8080/name/?bucket=bkt&prefix=p1/p2&region=r1" }) {
		ASSERT_EQ(location(same), base);
	}
	ASSERT_EQ(location("blobstore://h:80/n?region=r1"), location("blobstore://h:80/n?bucket=FDB_BACKUPS_V2&region=r1"));
	// A missing port is the default port for the connection type.
	ASSERT_EQ(location("blobstore://h/n?region=r1"), location("blobstore://h:443/n?region=r1"));
	ASSERT_EQ(location("blobstore://h/n?region=r1"), location("blobstore://h:https/n?region=r1"));
	ASSERT_EQ(location("blobstore://h/n?region=r1&sc=0"), location("blobstore://h:80/n?region=r1&sc=0"));
	ASSERT_NE(location("blobstore://h/n?region=r1"), location("blobstore://h:80/n?region=r1"));
	ASSERT_NE(location("blobstore://h/n?region=r1&sc=0"), location("blobstore://h:443/n?region=r1&sc=0"));
	// Different prefix and name splits can address the same data objects.
	ASSERT_EQ(location("blobstore://h:80/b?prefix=a/data&region=r1"),
	          location("blobstore://h:80/data/b?prefix=a&region=r1"));

	for (auto different : { "blobstore://ak:sk@other.example.com:8080/name?bucket=bkt&prefix=p1/p2&region=r1",
	                        "blobstore://ak:sk@host.example.com:8081/name?bucket=bkt&prefix=p1/p2&region=r1",
	                        "blobstore://ak:sk@host.example.com:8080/name?bucket=bkt2&prefix=p1/p2&region=r1",
	                        "blobstore://ak:sk@host.example.com:8080/name?bucket=bkt&region=r1",
	                        "blobstore://ak:sk@host.example.com:8080/name?bucket=bkt&prefix=p1&region=r1",
	                        "blobstore://ak:sk@host.example.com:8080/name?bucket=bkt&prefix=p1/p3&region=r1",
	                        "blobstore://ak:sk@host.example.com:8080/name2?bucket=bkt&prefix=p1/p2&region=r1",
	                        "blobstore://ak:sk@host.example.com:8080/name/sub?bucket=bkt&prefix=p1/p2&region=r1",
	                        "blobstore://ak:sk@host.example.com:8080//name?bucket=bkt&prefix=p1/p2&region=r1",
	                        "file:///p1/p2/data/name" }) {
		ASSERT_NE(location(different), base);
	}

	ASSERT_EQ(location("file:///backups/a"), location("file:///backups/a/"));
	ASSERT_NE(location("file:///backups/a"), location("file:///backups/b"));
	return Void();
}
