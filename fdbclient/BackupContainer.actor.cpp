/*
 * BackupContainer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "BackupContainer.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/Hash3.h"
#include "fdbrpc/AsyncFileBlobStore.actor.h"
#include "fdbrpc/AsyncFileReadAhead.actor.h"
#include "fdbclient/Status.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <algorithm>

const std::string BackupContainerBlobStore::META_BUCKET = "FDB_BACKUPS";

class BackupContainerLocalDirectory : public IBackupContainer, ReferenceCounted<BackupContainerLocalDirectory> {
public:
	void addref() { return ReferenceCounted<BackupContainerLocalDirectory>::addref(); }
	void delref() { return ReferenceCounted<BackupContainerLocalDirectory>::delref(); }

	static std::string getURLFormat() { return "file://</path/to/base/dir/>"; }

    BackupContainerLocalDirectory(std::string url) {
		std::string path;
		if(url.find("file://") != 0) {
			TraceEvent(SevWarn, "BackupContainerLocalDirectory").detail("Description", "Invalid URL for BackupContainerLocalDirectory").detail("URL", url);
		}

		path = url.substr(7);
		// Remove trailing slashes on path
		path.erase(path.find_last_not_of("\\/") + 1);

		if(!g_network->isSimulated() && path != abspath(path)) {
			TraceEvent(SevWarn, "BackupContainerLocalDirectory").detail("Description", "Backup path must be absolute (e.g. file:///some/path)").detail("URL", url).detail("Path", path);
			throw io_error();
		}

		// Finalized path written to will be will be <path>/backup-<uid>
		m_path = path;
	}

	Future<Void> create() {
		// Nothing should be done here because create() can be called by any process working with the container URL, such as fdbbackup.
		// Since "local directory" containers are by definition local to the machine they are accessed from,
		// the container's creation (in this case the creation of a directory) must be ensured prior to every file creation,
		// which is done in openFile().
		// Creating the directory here will result in unnecessary directories being created on machines that run fdbbackup but not agents.
		return Void();
	}

	virtual Future<Reference<IAsyncFile>> openFile(std::string name, EMode mode) {
		int flags = IAsyncFile::OPEN_NO_AIO;
		if(mode == WRITEONLY) {
			// Since each backup agent could be writing to a different, non-shared directory, every open for write
			// must make sure the directory has been created because it may not have been created when the
			// backup task was created (by the command line tool, on some single machine).
			platform::createDirectory(m_path);
			flags |= IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE;
		}
		else if(mode == READONLY) {
			// Simulation does not properly handle opening the same file from multiple machines using a shared filesystem,
			// so create a symbolic link to make each file opening appear to be unique.  This could also work in production
			// but only if the source directory is writeable which shouldn't be required for a restore.
			#ifndef _WIN32
			if(g_network->isSimulated()) {
				std::string uniqueName = name + "." + g_random->randomUniqueID().toString() + ".lnk";
				unlink((m_path + "/" + uniqueName).c_str());
				ASSERT(symlink(name.c_str(), (m_path + "/" + uniqueName).c_str()) == 0);
				name = uniqueName;
			}
			// Opening cached mode forces read/write mode at a lower level, overriding the readonly request.  So cached mode
			// can't be used because backup files are read-only.  Cached mode can only help during restore task retries handled
			// by the same process that failed the first task execution anyway, which is a very rare case.
			flags |= IAsyncFile::OPEN_UNCACHED;
			#endif
			flags |= IAsyncFile::OPEN_READONLY;
		}
		else
			throw io_error();

		return IAsyncFileSystem::filesystem()->open(m_path + "/" + name, flags, 0644);
	}

	virtual Future<bool> fileExists(std::string name) {
		return ::fileExists(m_path + "/" + name);
	}

	virtual Future<std::vector<std::string>> listFiles() {
		std::vector<std::string> files = platform::listFiles(m_path, "");
		// Remove .lnk files, they are a side effect of a backup that was *read* during simulation.  See openFile() above for more info on why they are created.
		files.erase(std::remove_if(files.begin(), files.end(), [](std::string const &f) { return StringRef(f).endsWith(LiteralStringRef(".lnk")); }), files.end());
		return files;
	}

	virtual Future<Void> renameFile(std::string from, std::string to) {
		::renameFile(m_path + "/" + from, m_path + "/" + to);
		return Void();
	}

private:
	std::string m_path;
};

// Read (or Create, if desired) the metadata entry for this backup container in a fixed-name metadata bucket.
// Returns the number of buckets in this backup container, for now.  If more important properties
// are added later then it should return something different like a JSON object or a struct.
// If the record doesn't exist yet, then one will be created using bucketCount if bucketCount is 0 or more.
// Otherwise, directory_does_not_exist() will be thrown.
ACTOR Future<int> createOrGetMetaRecord(Reference<BlobStoreEndpoint> bstore, std::string bucketBase, int bucketCount = -1) {
	bool exists = wait(bstore->objectExists(BackupContainerBlobStore::META_BUCKET, bucketBase));
	if(exists) {
		std::string data = wait(bstore->readEntireFile(BackupContainerBlobStore::META_BUCKET, bucketBase));
		json_spirit::mValue json;
		try {
			json_spirit::read_string(data, json);
			JSONDoc j(json);
			bucketCount = -1;
			j.tryGet("bucketCount", bucketCount);
		} catch(Error &e) {
			throw http_bad_response();
		}

		if(bucketCount < 0) {
			TraceEvent(SevWarn, "BlobBackupContainerInvalid").detail("bucketCount", bucketCount).detail("URL", bstore->getResourceURL(bucketBase));
			throw io_error();
		}
		return bucketCount;
	}

	if(bucketCount >= 0) {
		json_spirit::mObject o;
		o["bucketCount"] = bucketCount;
		std::string json = json_spirit::write_string(json_spirit::mValue(o));
		Void _ = wait(bstore->writeEntireFile(BackupContainerBlobStore::META_BUCKET, bucketBase, json));
		return bucketCount;
	}

	throw directory_does_not_exist();
}

ACTOR Future<Reference<IAsyncFile>> openFile_impl(Reference<BackupContainerBlobStore> bc, std::string name, IBackupContainer::EMode mode) {
	state std::string bucket = wait(bc->getBucketForFile(name));

	if(mode == BackupContainerBlobStore::WRITEONLY)
		return Reference<IAsyncFile>(new AsyncFileBlobStoreWrite(bc->m_bstore, bucket, name));
	else if(mode == BackupContainerBlobStore::READONLY) {
		// This isn't great logic but if "temp" is not in the filename then treat the file as a symlink and redirect to the name in its content
		if(name.find("temp") == name.npos) {
			std::string realName = wait(bc->m_bstore->readEntireFile(bucket, name));
			name = realName;
			if(realName.empty())
				throw io_error();
			std::string realBucket = wait(bc->getBucketForFile(name));
			bucket = realBucket;
		}

		Reference<IAsyncFile> f(new AsyncFileBlobStoreRead(bc->m_bstore, bucket, name));
		return Reference<IAsyncFile>(new AsyncFileReadAheadCache(f,
					bc->m_bstore->knobs.read_block_size,
					bc->m_bstore->knobs.read_ahead_blocks,
					bc->m_bstore->knobs.concurrent_reads_per_file,
					bc->m_bstore->knobs.read_cache_blocks_per_file
				));
	}
	else
		throw io_error();
}

Future<Reference<IAsyncFile>> BackupContainerBlobStore::openFile(std::string name, EMode mode) {
	return openFile_impl(Reference<BackupContainerBlobStore>::addRef(this), name, mode);
}

ACTOR Future<bool> fileExists_impl(Reference<BackupContainerBlobStore> bc, std::string name) {
	std::string bucket = wait(bc->getBucketForFile(name));
	bool e = wait(bc->m_bstore->objectExists(bucket, name));
	return e;
}

Future<bool> BackupContainerBlobStore::fileExists(std::string name) {
	return fileExists_impl(Reference<BackupContainerBlobStore>::addRef(this), name);
}

// Since blob store cannot rename, this just writes a file called <to> that contains <from>. This unfornately
// means that there has to be some more knowledge sharing between backup container and its users than is ideal.
// Specfically, any file written to a backup container should be opened as a temp file and then renamed
// to a valid backup file type upon closure.  The blob store backup container must know the naming conventions
// of valid final backup file filenames so it can redirect openings of such to the temp file that it was
// created from.
ACTOR Future<Void> renameFile_impl(BackupContainerBlobStore *b, std::string from, std::string to) {
	try {
		// Checking for the existing of the source file isn't really necesary and depending on the blob store's
		// causal consistency guarauntee it's possible that a file that just finished uploading is found not
		// to exist yet.
		//bool exists = wait(b->fileExists(from));
		//if(!exists)
			//throw file_not_found();
		std::string bucket = wait(b->getBucketForFile(to));
		Void _ = wait(b->m_bstore->writeEntireFile(bucket, to, from));
	} catch(Error &e) {
		TraceEvent(SevWarn, "BlobStoreRenameFailure").detail("From", from).detail("To", to).error(e);
		throw;
	}

	return Void();
}

Future<Void> BackupContainerBlobStore::renameFile(std::string from, std::string to) {
	return renameFile_impl(this, from, to);
}

ACTOR Future<Void> listFilesStream_impl(Reference<BackupContainerBlobStore> bc, PromiseStream<BlobStoreEndpoint::ObjectInfo> results) {
	state std::vector<std::string> buckets = wait(bc->getBucketList());
	state std::vector<Future<Void>> bucketFutures;

	// Start listing all of the buckets to the stream
	for(auto &b : buckets)
		bucketFutures.push_back(bc->m_bstore->getBucketContentsStream(b, results));

	Void _ = wait(waitForAll(bucketFutures));

	return Void();
}

Future<Void> BackupContainerBlobStore::listFilesStream(PromiseStream<BlobStoreEndpoint::ObjectInfo> results) {
	return listFilesStream_impl(Reference<BackupContainerBlobStore>::addRef(this), results);
}

ACTOR Future<std::vector<std::string>> listFiles_impl(Reference<BackupContainerBlobStore> bc) {
	state PromiseStream<BlobStoreEndpoint::ObjectInfo> resultsStream;
	state std::vector<std::string> results;
	state Future<Void> done = bc->listFilesStream(resultsStream);

	loop {
		choose {
			when(Void _ = wait(done)) {
				break;
			}
			when(BlobStoreEndpoint::ObjectInfo info = waitNext(resultsStream.getFuture())) {
				results.push_back(std::move(info.name));
			}
		}
	}

	return results;
}

Future<std::vector<std::string>> BackupContainerBlobStore::listFiles() {
	return listFiles_impl(Reference<BackupContainerBlobStore>::addRef(this));
}

ACTOR Future<Void> create_impl(Reference<BackupContainerBlobStore> bc) {
	// If bucketCount is known then container is already created.
	if(bc->m_bucketCount.isValid()) {
		try {
			int _ = wait(bc->m_bucketCount);
			return Void();
		} catch(Error &e) {}
	}

	// If future isn't valid or an error was thrown, then attempt to read/create the meta record
	bc->m_bucketCount = createOrGetMetaRecord(bc->m_bstore, bc->m_bucketPrefix, bc->m_bstore->knobs.buckets_to_span);
	// If we get a bucket count then the container now exists.
	int _ = wait(bc->m_bucketCount);
	return Void();
}

Future<Void> BackupContainerBlobStore::create() {
	return create_impl(Reference<BackupContainerBlobStore>::addRef(this));
}

Future<int> BackupContainerBlobStore::getBucketCount() {
	if(!m_bucketCount.isValid() || m_bucketCount.isError())
		m_bucketCount = createOrGetMetaRecord(m_bstore, m_bucketPrefix);
	return m_bucketCount;
}

ACTOR Future<std::string> getBucketForFile_impl(BackupContainerBlobStore *bc, std::string fname) {
	int count = wait(bc->getBucketCount());
	return bc->getBucketString(hashlittle(fname.data(), fname.size(), 0) % count);
}

Future<std::string> BackupContainerBlobStore::getBucketForFile(std::string const &name) { return getBucketForFile_impl(this, name); }

ACTOR Future<std::vector<std::string>> getBucketList_impl(BackupContainerBlobStore *bc) {
	int count = wait(bc->getBucketCount());
	std::vector<std::string> buckets;
	for(int i = 0; i < count; ++i)
		buckets.push_back(bc->getBucketString(i));
	return buckets;
}

Future<std::vector<std::string>> BackupContainerBlobStore::getBucketList() { return getBucketList_impl(this); }

ACTOR Future<std::vector<std::string>> BackupContainerBlobStore::listBackupContainers(Reference<BlobStoreEndpoint> bs) {
	state std::vector<std::string> results;
	BlobStoreEndpoint::BucketContentsT contents = wait(bs->getBucketContents(BackupContainerBlobStore::META_BUCKET));
	for(auto &o : contents)
		results.push_back(std::move(o.name));
	return results;
}

std::vector<std::string> IBackupContainer::getURLFormats() {
	std::vector<std::string> formats;
	formats.push_back(BackupContainerLocalDirectory::getURLFormat());
	formats.push_back(BackupContainerBlobStore::getURLFormat());
	return formats;
}

ACTOR Future<Void> deleteContainer_impl(Reference<BackupContainerBlobStore> bc, int *pNumDeleted) {
	state std::vector<std::string> buckets = wait(bc->getBucketList());
	state std::vector<Future<Void>> bucketFutures;

	// Start deleting all the buckets
	for(auto &b : buckets)
		bucketFutures.push_back(bc->m_bstore->deleteBucket(b, pNumDeleted));

	// Wait for all object deletes to be finished
	Void _ = wait(waitForAll(bucketFutures));

	// Delete metadata entry last, if deleting all the other stuff worked.
	Void _ = wait(bc->m_bstore->deleteObject(BackupContainerBlobStore::META_BUCKET, bc->m_bucketPrefix));
	return Void();
}

Future<Void> BackupContainerBlobStore::deleteContainer(int *pNumDeleted) {
	return deleteContainer_impl(Reference<BackupContainerBlobStore>::addRef(this), pNumDeleted);
}

Future<std::string> BackupContainerBlobStore::containerInfo() {
	return std::string();
}

// Get an IBackupContainer based on a container URL string
Reference<IBackupContainer> IBackupContainer::openContainer(std::string url, std::string *errors)
{
	static std::map<std::string, Reference<IBackupContainer>> m_cache;

	Reference<IBackupContainer> &r = m_cache[url];
	if(r)
		return r;

	try {
		StringRef u(url);
		if(u.startsWith(LiteralStringRef("file://")))
			r = Reference<IBackupContainer>(new BackupContainerLocalDirectory(url));
		else if(u.startsWith(LiteralStringRef("blobstore://"))) {
			std::string resource;
			Reference<BlobStoreEndpoint> bstore = BlobStoreEndpoint::fromString(url, &resource, errors);
			if(resource.empty())
				throw file_not_found();
			for(auto c : resource)
				if(!isalnum(c) && c != '_' && c != '-' && c != '.')
					throw invalid_option_value();
			r = Reference<IBackupContainer>(new BackupContainerBlobStore(bstore, resource));
		}
		else
			throw file_not_found();

		return r;
	} catch(Error &e) {
		TraceEvent(SevWarn, "BackupContainer").detail("Description", "Invalid container specification.  See help.").detail("URL", url);
		throw;
	}
}


TEST_CASE("backup/containers/localdir") {
	state Reference<IBackupContainer> c = IBackupContainer::openContainer(format("file://simfdb/backups/%llx", timer_int()));
	state Reference<IAsyncFile> f;

	Reference<IAsyncFile> fh = wait(c->openFile("test", IBackupContainer::WRITEONLY));
	f = fh;
	Void _ = wait(f->write((const uint8_t *)"asdf", 4, 0));
	Void _ = wait(f->truncate(4));
	Void _ = wait(f->sync());

	Reference<IAsyncFile> fh = wait(c->openFile("test2", IBackupContainer::WRITEONLY));
	f = fh;
	Void _ = wait(f->write((const uint8_t *)"1234", 4, 0));
	Void _ = wait(f->truncate(4));
	Void _ = wait(f->sync());

	Reference<IAsyncFile> fh = wait(c->openFile("test", IBackupContainer::READONLY));
	f = fh;
	int64_t size = wait(f->size());
	ASSERT(size == 4);
	state std::string buf;
	buf.resize(4);
	int rs = wait(f->read((void *)buf.data(), 4, 0));
	ASSERT(rs == 4);
	ASSERT(buf == "asdf");

	Reference<IAsyncFile> fh = wait(c->openFile("test2", IBackupContainer::READONLY));
	f = fh;
	int64_t size = wait(f->size());
	ASSERT(size == 4);
	int rs = wait(f->read((void *)buf.data(), 4, 0));
	ASSERT(rs == 4);
	ASSERT(buf == "1234");

	std::vector<std::string> files = wait(c->listFiles());
	ASSERT(files.size() == 2);

	Void _ = wait(c->renameFile("test", "testrenamed"));

	std::vector<std::string> files = wait(c->listFiles());
	ASSERT(files.size() == 2);

	return Void();
};

