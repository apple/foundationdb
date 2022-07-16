/*
 * BackupContainerS3BlobStore.h
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

#ifndef FDBCLIENT_BACKUP_CONTAINER_S3_BLOBSTORE_H
#define FDBCLIENT_BACKUP_CONTAINER_S3_BLOBSTORE_H
#pragma once

#include "fdbclient/AsyncFileS3BlobStore.actor.h"
#include "fdbclient/BackupContainerFileSystem.h"

class BackupContainerS3BlobStore final : public BackupContainerFileSystem,
                                         ReferenceCounted<BackupContainerS3BlobStore> {
	Reference<S3BlobStoreEndpoint> m_bstore;
	std::string m_name;

	// All backup data goes into a single bucket
	std::string m_bucket;

	std::string dataPath(const std::string& path);

	// Get the path of the backups's index entry
	std::string indexEntry();

	friend class BackupContainerS3BlobStoreImpl;

public:
	BackupContainerS3BlobStore(Reference<S3BlobStoreEndpoint> bstore,
	                           const std::string& name,
	                           const S3BlobStoreEndpoint::ParametersT& params,
	                           const Optional<std::string>& encryptionKeyFileName);

	void addref() override;
	void delref() override;

	static std::string getURLFormat();

	Future<Reference<IAsyncFile>> readFile(const std::string& path) final;

	static Future<std::vector<std::string>> listURLs(Reference<S3BlobStoreEndpoint> bstore, const std::string& bucket);

	Future<Reference<IBackupFile>> writeFile(const std::string& path) final;

	Future<Void> deleteFile(const std::string& path) final;

	Future<FilesAndSizesT> listFiles(const std::string& path, std::function<bool(std::string const&)> pathFilter) final;

	Future<Void> create() final;

	// The container exists if the index entry in the blob bucket exists
	Future<bool> exists() final;

	Future<Void> deleteContainer(int* pNumDeleted) final;

	std::string getBucket() const;
};

#endif
