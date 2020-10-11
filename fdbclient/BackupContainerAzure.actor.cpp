/*
 * BackupContainerAzure.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include <cstdlib>

#include "storage_credential.h"
#include "storage_account.h"
#include "blob/blob_client.h"

#include "fdbclient/BackupContainer.h"
#include "flow/actorcompiler.h" // has to be last include

class BackupContainerAzureBlobStore final : public IBackupContainer, ReferenceCounted<BackupContainerAzureBlobStore> {

	azure::storage_lite::blob_client client;

	using Self = BackupContainerAzureBlobStore;

	ACTOR static Future<Reference<IBackupFile>> writeLogFile_impl(Self* self, Version beginVersion, Version endVersion,
	                                                              int blockSize) {
		wait(delay(0.0));
		return Reference<IBackupFile>(new BackupFile("test"));
	}

	ACTOR static Future<Reference<IBackupFile>> writeRangeFile_impl(Self* self, Version snapshotBeginVersion,
	                                                                int snapshotFileCount, Version fileVersion,
	                                                                int blockSize) {
		wait(delay(0.0));
		return Reference<IBackupFile>(new BackupFile("test"));
	}

	ACTOR static Future<Reference<IBackupFile>> writeTaggedLogFile_impl(Self* self, Version beginVersion,
	                                                                    Version endVersion, int blockSize,
	                                                                    uint16_t tagId, int totalTags) {
		wait(delay(0.0));
		return Reference<IBackupFile>(new BackupFile("test"));
	}

public:
	void addref() override { return ReferenceCounted<BackupContainerAzureBlobStore>::addref(); }
	void delref() override { return ReferenceCounted<BackupContainerAzureBlobStore>::delref(); }

	Future<Void> create() override {
		std::string account_name = std::getenv("AZURE_TESTACCOUNT"); //"devstoreaccount1";
		std::string account_key = std::getenv("AZURE_TESTKEY");
		bool use_https = true;
		// std::string blob_endpoint = "https://127.0.0.1:10000/devstoreaccount1/";
		int connection_count = 2;

		auto credential = std::make_shared<azure::storage_lite::shared_key_credential>(account_name, account_key);
		auto storage_account =
		    std::make_shared<azure::storage_lite::storage_account>(account_name, credential, use_https);

		client = azure::storage_lite::blob_client(storage_account, connection_count);
		return Void();
	}

	Future<bool> exists() override { return true; }

	class BackupFile : public IBackupFile, ReferenceCounted<BackupFile> {
	public:
		BackupFile(std::string fileName) : IBackupFile(fileName) {}
		Future<Void> append(const void* data, int len) override { return Void(); }
		Future<Void> finish() override { return Void(); }
		void addref() override { ReferenceCounted<BackupFile>::addref(); }
		void delref() override { ReferenceCounted<BackupFile>::delref(); }
	};

	Future<Reference<IBackupFile>> writeLogFile(Version beginVersion, Version endVersion, int blockSize) override {
		return writeLogFile_impl(this, beginVersion, endVersion, blockSize);
	}
	Future<Reference<IBackupFile>> writeRangeFile(Version snapshotBeginVersion, int snapshotFileCount,
	                                              Version fileVersion, int blockSize) override {
		return writeRangeFile_impl(this, snapshotBeginVersion, snapshotFileCount, fileVersion, blockSize);
	}

	Future<Reference<IBackupFile>> writeTaggedLogFile(Version beginVersion, Version endVersion, int blockSize,
	                                                  uint16_t tagId, int totalTags) override {
		return writeTaggedLogFile_impl(this, beginVersion, endVersion, blockSize, tagId, totalTags);
	}

	Future<Void> writeKeyspaceSnapshotFile(const std::vector<std::string>& fileNames,
	                                       const std::vector<std::pair<Key, Key>>& beginEndKeys,
	                                       int64_t totalBytes) override {
		return delay(0.0);
	}
	Future<Reference<IAsyncFile>> readFile(std::string name) override { return Reference<IAsyncFile>(); }
	Future<KeyRange> getSnapshotFileKeyRange(const RangeFile& file) override { return KeyRange(normalKeys); }

	Future<Void> expireData(Version expireEndVersion, bool force = false, ExpireProgress* progress = nullptr,
	                        Version restorableBeginVersion = std::numeric_limits<Version>::max()) override {
		return Void();
	}

	Future<Void> deleteContainer(int* pNumDeleted = nullptr) override { return Void(); }

	Future<BackupDescription> describeBackup(bool deepScan = false,
	                                         Version logStartVersionOverride = invalidVersion) override {
		return {};
	}

	Future<BackupFileList> dumpFileList(Version begin = 0, Version end = std::numeric_limits<Version>::max()) override {
		return {};
	}

	Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion) { return {}; }
};
