/*
 * BackupContainer.h
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

#pragma once

#include "flow/flow.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbrpc/BlobStore.h"
#include <vector>

// Class representing a container for backup files, such as a mounted directory or a remote filesystem.
class IBackupContainer {
public:
	virtual void addref() = 0;
	virtual void delref() = 0;

	enum EMode { READONLY, WRITEONLY };

	static std::vector<std::string> getURLFormats();

	IBackupContainer() {}
	virtual ~IBackupContainer() {}

	// Create the container (if necessary)
	virtual Future<Void> create() = 0;

	// Open a named file in the container for reading (restore mode) or writing (backup mode)
	virtual Future<Reference<IAsyncFile>> openFile(std::string name, EMode mode) = 0;

	// Returns whether or not a file exists in the container
	virtual Future<bool> fileExists(std::string name) = 0;

	// Get a list of backup files in the container
	virtual Future<std::vector<std::string>> listFiles() = 0;

	// Rename a file
	virtual Future<Void> renameFile(std::string from, std::string to) = 0;

	// Get an IBackupContainer based on a container spec string
	static Reference<IBackupContainer> openContainer(std::string url, std::string *error = nullptr);
};

class BackupContainerBlobStore : public IBackupContainer, ReferenceCounted<BackupContainerBlobStore> {
public:
	void addref() { return ReferenceCounted<BackupContainerBlobStore>::addref(); }
	void delref() { return ReferenceCounted<BackupContainerBlobStore>::delref(); }
	static const std::string META_BUCKET;

	static std::string getURLFormat() { return BlobStoreEndpoint::getURLFormat(true); }
	static Future<std::vector<std::string>> listBackupContainers(Reference<BlobStoreEndpoint> const &bs);

    BackupContainerBlobStore(Reference<BlobStoreEndpoint> bstore, std::string name)
	  : m_bstore(bstore), m_bucketPrefix(name) {}

	virtual ~BackupContainerBlobStore() { m_bucketCount.cancel(); }

	// IBackupContainer methods
	Future<Void> create();
	Future<Reference<IAsyncFile>> openFile(std::string name, EMode mode);
	Future<bool> fileExists(std::string name);
	Future<Void> renameFile(std::string from, std::string to);
	Future<std::vector<std::string>> listFiles();
	Future<Void> listFilesStream(PromiseStream<BlobStoreEndpoint::ObjectInfo> results);

	Future<Void> deleteContainer(int *pNumDeleted = NULL);
	Future<std::string> containerInfo();
	Future<int> getBucketCount();
	std::string getBucketString(int num) { return format("%s_%d", m_bucketPrefix.c_str(), num); }
	Future<std::string> getBucketForFile(std::string const &name);
	Future<std::vector<std::string>> getBucketList();

	Reference<BlobStoreEndpoint> m_bstore;
	std::string m_bucketPrefix;
	Future<int> m_bucketCount;
};

