/*
 * BackupContainerAzureBlobStore.h
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

#if (!defined FDBCLIENT_BACKUP_CONTAINER_AZURE_BLOBSTORE_H) && (defined BUILD_AZURE_BACKUP)
#define FDBCLIENT_BACKUP_CONTAINER_AZURE_BLOBSTORE_H
#pragma once

#include "fdbclient/AsyncTaskThread.h"
#include "fdbclient/BackupContainerFileSystem.h"

#include "storage_credential.h"
#include "storage_account.h"
#include "blob/blob_client.h"

class BackupContainerAzureBlobStore final : public BackupContainerFileSystem,
                                            ReferenceCounted<BackupContainerAzureBlobStore> {
	using AzureClient = azure::storage_lite::blob_client;

	std::shared_ptr<AzureClient> client;
	std::string containerName;
	AsyncTaskThread asyncTaskThread;

	Future<bool> blobExists(const std::string& fileName);

	friend class BackupContainerAzureBlobStoreImpl;

public:
	BackupContainerAzureBlobStore(const std::string& endpoint,
	                              const std::string& accountName,
	                              const std::string& containerName,
	                              const Optional<std::string>& encryptionKeyFileName);

	void addref() override;
	void delref() override;

	Future<Void> create() override;

	Future<bool> exists() override;

	Future<Reference<IAsyncFile>> readFile(const std::string& fileName) override;

	Future<Reference<IBackupFile>> writeFile(const std::string& fileName) override;

	Future<FilesAndSizesT> listFiles(const std::string& path = "",
	                                 std::function<bool(std::string const&)> folderPathFilter = nullptr) override;

	Future<Void> deleteFile(const std::string& fileName) override;

	Future<Void> deleteContainer(int* pNumDeleted) override;

	static Future<std::vector<std::string>> listURLs(const std::string& baseURL);

	static std::string getURLFormat();
};

#endif
