/*
 * BackupContainerAzureBlobStore.actor.cpp
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

#include "fdbclient/BackupContainerAzureBlobStore.h"
#include "fdbrpc/AsyncFileEncrypted.h"
#include <future>

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

std::string const notFoundErrorCode = "404";

void printAzureError(std::string const& operationName, azure::storage_lite::storage_error const& err) {
	printf("(%s) : Error from Azure SDK : %s (%s) : %s",
	       operationName.c_str(),
	       err.code_name.c_str(),
	       err.code.c_str(),
	       err.message.c_str());
}

template <class T>
T waitAzureFuture(std::future<azure::storage_lite::storage_outcome<T>>&& f, std::string const& operationName) {
	auto outcome = f.get();
	if (outcome.success()) {
		return outcome.response();
	} else {
		printAzureError(operationName, outcome.error());
		throw backup_error();
	}
}

} // namespace

class BackupContainerAzureBlobStoreImpl {
public:
	using AzureClient = azure::storage_lite::blob_client;

	class ReadFile final : public IAsyncFile, ReferenceCounted<ReadFile> {
		AsyncTaskThread* asyncTaskThread;
		std::string containerName;
		std::string blobName;
		std::shared_ptr<AzureClient> client;

	public:
		ReadFile(AsyncTaskThread& asyncTaskThread,
		         const std::string& containerName,
		         const std::string& blobName,
		         std::shared_ptr<AzureClient> const& client)
		  : asyncTaskThread(&asyncTaskThread), containerName(containerName), blobName(blobName), client(client) {}

		void addref() override { ReferenceCounted<ReadFile>::addref(); }
		void delref() override { ReferenceCounted<ReadFile>::delref(); }
		Future<int> read(void* data, int length, int64_t offset) override {
			TraceEvent(SevDebug, "BCAzureBlobStoreRead")
			    .detail("Length", length)
			    .detail("Offset", offset)
			    .detail("ContainerName", containerName)
			    .detail("BlobName", blobName);
			return asyncTaskThread->execAsync([client = this->client,
			                                   containerName = this->containerName,
			                                   blobName = this->blobName,
			                                   data,
			                                   length,
			                                   offset] {
				std::ostringstream oss(std::ios::out | std::ios::binary);
				waitAzureFuture(client->download_blob_to_stream(containerName, blobName, offset, length, oss),
				                "download_blob_to_stream");
				auto str = std::move(oss).str();
				memcpy(data, str.c_str(), str.size());
				return static_cast<int>(str.size());
			});
		}
		Future<Void> zeroRange(int64_t offset, int64_t length) override { throw file_not_writable(); }
		Future<Void> write(void const* data, int length, int64_t offset) override { throw file_not_writable(); }
		Future<Void> truncate(int64_t size) override { throw file_not_writable(); }
		Future<Void> sync() override { throw file_not_writable(); }
		Future<int64_t> size() const override {
			TraceEvent(SevDebug, "BCAzureBlobStoreReadFileSize")
			    .detail("ContainerName", containerName)
			    .detail("BlobName", blobName);
			return asyncTaskThread->execAsync(
			    [client = this->client, containerName = this->containerName, blobName = this->blobName] {
				    auto resp =
				        waitAzureFuture(client->get_blob_properties(containerName, blobName), "get_blob_properties");
				    return static_cast<int64_t>(resp.size);
			    });
		}
		std::string getFilename() const override { return blobName; }
		int64_t debugFD() const override { return 0; }
	};

	class WriteFile final : public IAsyncFile, ReferenceCounted<WriteFile> {
		AsyncTaskThread* asyncTaskThread;
		std::shared_ptr<AzureClient> client;
		std::string containerName;
		std::string blobName;
		int64_t m_cursor{ 0 };
		// Ideally this buffer should not be a string, but
		// the Azure SDK only supports/tests uploading to append
		// blobs from a stringstream.
		std::string buffer;

		static constexpr size_t bufferLimit = 1 << 20;

	public:
		WriteFile(AsyncTaskThread& asyncTaskThread,
		          const std::string& containerName,
		          const std::string& blobName,
		          std::shared_ptr<AzureClient> const& client)
		  : asyncTaskThread(&asyncTaskThread), containerName(containerName), blobName(blobName), client(client) {}

		void addref() override { ReferenceCounted<WriteFile>::addref(); }
		void delref() override { ReferenceCounted<WriteFile>::delref(); }
		Future<int> read(void* data, int length, int64_t offset) override { throw file_not_readable(); }
		Future<Void> write(void const* data, int length, int64_t offset) override {
			if (offset != m_cursor) {
				throw non_sequential_op();
			}
			m_cursor += length;
			auto p = static_cast<char const*>(data);
			buffer.append(p, length);
			if (buffer.size() > bufferLimit) {
				return sync();
			} else {
				return Void();
			}
		}
		Future<Void> truncate(int64_t size) override {
			if (size != m_cursor) {
				throw non_sequential_op();
			}
			return Void();
		}
		Future<Void> sync() override {
			TraceEvent(SevDebug, "BCAzureBlobStoreSync")
			    .detail("Length", buffer.size())
			    .detail("ContainerName", containerName)
			    .detail("BlobName", blobName);
			auto movedBuffer = std::move(buffer);
			buffer = {};
			if (!movedBuffer.empty()) {
				return asyncTaskThread->execAsync([client = this->client,
				                                   containerName = this->containerName,
				                                   blobName = this->blobName,
				                                   buffer = std::move(movedBuffer)] {
					std::istringstream iss(std::move(buffer));
					waitAzureFuture(client->append_block_from_stream(containerName, blobName, iss),
					                "append_block_from_stream");
					return Void();
				});
			}
			return Void();
		}
		Future<int64_t> size() const override {
			TraceEvent(SevDebug, "BCAzureBlobStoreSize")
			    .detail("ContainerName", containerName)
			    .detail("BlobName", blobName);
			return asyncTaskThread->execAsync(
			    [client = this->client, containerName = this->containerName, blobName = this->blobName] {
				    auto resp =
				        waitAzureFuture(client->get_blob_properties(containerName, blobName), "get_blob_properties");
				    return static_cast<int64_t>(resp.size);
			    });
		}
		std::string getFilename() const override { return blobName; }
		int64_t debugFD() const override { return -1; }
	};

	class BackupFile final : public IBackupFile, ReferenceCounted<BackupFile> {
		Reference<IAsyncFile> m_file;
		int64_t m_offset;

	public:
		BackupFile(const std::string& fileName, Reference<IAsyncFile> file)
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
		void addref() override { ReferenceCounted<BackupFile>::addref(); }
		void delref() override { ReferenceCounted<BackupFile>::delref(); }
	};

	static bool isDirectory(const std::string& blobName) { return blobName.size() && blobName.back() == '/'; }

	// Hack to get around the fact that macros don't work inside actor functions
	static Reference<IAsyncFile> encryptFile(Reference<IAsyncFile> const& f, AsyncFileEncrypted::Mode mode) {
		Reference<IAsyncFile> result = f;
#if ENCRYPTION_ENABLED
		result = makeReference<AsyncFileEncrypted>(result, mode);
#endif
		return result;
	}

	ACTOR static Future<Reference<IAsyncFile>> readFile(BackupContainerAzureBlobStore* self, std::string fileName) {
		bool exists = wait(self->blobExists(fileName));
		if (!exists) {
			throw file_not_found();
		}
		Reference<IAsyncFile> f =
		    makeReference<ReadFile>(self->asyncTaskThread, self->containerName, fileName, self->client);
		if (self->usesEncryption()) {
			f = encryptFile(f, AsyncFileEncrypted::Mode::READ_ONLY);
		}
		return f;
	}

	ACTOR static Future<Reference<IBackupFile>> writeFile(BackupContainerAzureBlobStore* self, std::string fileName) {
		TraceEvent(SevDebug, "BCAzureBlobStoreCreateWriteFile")
		    .detail("ContainerName", self->containerName)
		    .detail("FileName", fileName);
		wait(self->asyncTaskThread.execAsync(
		    [client = self->client, containerName = self->containerName, fileName = fileName] {
			    waitAzureFuture(client->create_append_blob(containerName, fileName), "create_append_blob");
			    return Void();
		    }));
		Reference<IAsyncFile> f =
		    makeReference<WriteFile>(self->asyncTaskThread, self->containerName, fileName, self->client);
		if (self->usesEncryption()) {
			f = encryptFile(f, AsyncFileEncrypted::Mode::APPEND_ONLY);
		}
		return makeReference<BackupFile>(fileName, f);
	}

	static void listFiles(std::shared_ptr<AzureClient> const& client,
	                      const std::string& containerName,
	                      const std::string& path,
	                      std::function<bool(std::string const&)> folderPathFilter,
	                      BackupContainerFileSystem::FilesAndSizesT& result) {
		auto resp = waitAzureFuture(client->list_blobs_segmented(containerName, "/", "", path), "list_blobs_segmented");
		for (const auto& blob : resp.blobs) {
			if (isDirectory(blob.name) && (!folderPathFilter || folderPathFilter(blob.name))) {
				listFiles(client, containerName, blob.name, folderPathFilter, result);
			} else {
				result.emplace_back(blob.name, blob.content_length);
			}
		}
	}

	ACTOR static Future<Void> deleteContainer(BackupContainerAzureBlobStore* self, int* pNumDeleted) {
		state int filesToDelete = 0;
		if (pNumDeleted) {
			BackupContainerFileSystem::FilesAndSizesT files = wait(self->listFiles());
			filesToDelete = files.size();
		}
		TraceEvent(SevDebug, "BCAzureBlobStoreDeleteContainer")
		    .detail("FilesToDelete", filesToDelete)
		    .detail("ContainerName", self->containerName)
		    .detail("TrackNumDeleted", pNumDeleted != nullptr);
		wait(self->asyncTaskThread.execAsync([containerName = self->containerName, client = self->client] {
			waitAzureFuture(client->delete_container(containerName), "delete_container");
			return Void();
		}));
		if (pNumDeleted) {
			*pNumDeleted += filesToDelete;
		}
		return Void();
	}
};

Future<bool> BackupContainerAzureBlobStore::blobExists(const std::string& fileName) {
	TraceEvent(SevDebug, "BCAzureBlobStoreCheckExists")
	    .detail("FileName", fileName)
	    .detail("ContainerName", containerName);
	return asyncTaskThread.execAsync([client = this->client, containerName = this->containerName, fileName = fileName] {
		auto outcome = client->get_blob_properties(containerName, fileName).get();
		if (outcome.success()) {
			return true;
		} else {
			auto const& err = outcome.error();
			if (err.code == notFoundErrorCode) {
				return false;
			} else {
				printAzureError("get_blob_properties", err);
				throw backup_error();
			}
		}
	});
}

BackupContainerAzureBlobStore::BackupContainerAzureBlobStore(const std::string& endpoint,
                                                             const std::string& accountName,
                                                             const std::string& containerName,
                                                             const Optional<std::string>& encryptionKeyFileName)
  : containerName(containerName) {
	setEncryptionKey(encryptionKeyFileName);
	const char* _accountKey = std::getenv("AZURE_KEY");
	if (!_accountKey) {
		TraceEvent(SevError, "EnvironmentVariableNotFound").detail("EnvVariable", "AZURE_KEY");
		// TODO: More descriptive error?
		throw backup_error();
	}
	std::string accountKey = _accountKey;
	auto credential = std::make_shared<azure::storage_lite::shared_key_credential>(accountName, accountKey);
	auto storageAccount = std::make_shared<azure::storage_lite::storage_account>(
	    accountName, credential, true, format("https://%s", endpoint.c_str()));
	client = std::make_unique<AzureClient>(storageAccount, 1);
}

void BackupContainerAzureBlobStore::addref() {
	return ReferenceCounted<BackupContainerAzureBlobStore>::addref();
}
void BackupContainerAzureBlobStore::delref() {
	return ReferenceCounted<BackupContainerAzureBlobStore>::delref();
}

Future<Void> BackupContainerAzureBlobStore::create() {
	TraceEvent(SevDebug, "BCAzureBlobStoreCreateContainer").detail("ContainerName", containerName);
	Future<Void> createContainerFuture =
	    asyncTaskThread.execAsync([containerName = this->containerName, client = this->client] {
		    waitAzureFuture(client->create_container(containerName), "create_container");
		    return Void();
	    });
	Future<Void> encryptionSetupFuture = usesEncryption() ? encryptionSetupComplete() : Void();
	return createContainerFuture && encryptionSetupFuture;
}
Future<bool> BackupContainerAzureBlobStore::exists() {
	TraceEvent(SevDebug, "BCAzureBlobStoreCheckContainerExists").detail("ContainerName", containerName);
	return asyncTaskThread.execAsync([containerName = this->containerName, client = this->client] {
		auto outcome = client->get_container_properties(containerName).get();
		if (outcome.success()) {
			return true;
		} else {
			auto const& err = outcome.error();
			if (err.code == notFoundErrorCode) {
				return false;
			} else {
				printAzureError("got_container_properties", err);
				throw backup_error();
			}
		}
	});
}

Future<Reference<IAsyncFile>> BackupContainerAzureBlobStore::readFile(const std::string& fileName) {
	return BackupContainerAzureBlobStoreImpl::readFile(this, fileName);
}

Future<Reference<IBackupFile>> BackupContainerAzureBlobStore::writeFile(const std::string& fileName) {
	return BackupContainerAzureBlobStoreImpl::writeFile(this, fileName);
}

Future<BackupContainerFileSystem::FilesAndSizesT> BackupContainerAzureBlobStore::listFiles(
    const std::string& path,
    std::function<bool(std::string const&)> folderPathFilter) {
	TraceEvent(SevDebug, "BCAzureBlobStoreListFiles").detail("ContainerName", containerName).detail("Path", path);
	return asyncTaskThread.execAsync(
	    [client = this->client, containerName = this->containerName, path = path, folderPathFilter = folderPathFilter] {
		    FilesAndSizesT result;
		    BackupContainerAzureBlobStoreImpl::listFiles(client, containerName, path, folderPathFilter, result);
		    return result;
	    });
}

Future<Void> BackupContainerAzureBlobStore::deleteFile(const std::string& fileName) {
	TraceEvent(SevDebug, "BCAzureBlobStoreDeleteFile")
	    .detail("ContainerName", containerName)
	    .detail("FileName", fileName);
	return asyncTaskThread.execAsync([containerName = this->containerName, fileName = fileName, client = client]() {
		client->delete_blob(containerName, fileName).wait();
		return Void();
	});
}

Future<Void> BackupContainerAzureBlobStore::deleteContainer(int* pNumDeleted) {
	return BackupContainerAzureBlobStoreImpl::deleteContainer(this, pNumDeleted);
}

Future<std::vector<std::string>> BackupContainerAzureBlobStore::listURLs(const std::string& baseURL) {
	// TODO: Implement this
	return std::vector<std::string>{};
}

std::string BackupContainerAzureBlobStore::getURLFormat() {
	return "azure://<accountname>@<endpoint>/<container>/";
}
