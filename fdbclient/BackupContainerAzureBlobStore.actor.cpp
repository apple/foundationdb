/*
 * BackupContainerAzureBlobStore.actor.cpp
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

#include "fdbclient/BackupContainerAzureBlobStore.h"

#include "flow/actorcompiler.h" // This must be the last #include.

class BackupContainerAzureBlobStoreImpl {
public:
	using AzureClient = azure::storage_lite::blob_client;

	class ReadFile final : public IAsyncFile, ReferenceCounted<ReadFile> {
		AsyncTaskThread& asyncTaskThread;
		std::string containerName;
		std::string blobName;
		AzureClient* client;

	public:
		ReadFile(AsyncTaskThread& asyncTaskThread, const std::string& containerName, const std::string& blobName,
		         AzureClient* client)
		  : asyncTaskThread(asyncTaskThread), containerName(containerName), blobName(blobName), client(client) {}

		void addref() override { ReferenceCounted<ReadFile>::addref(); }
		void delref() override { ReferenceCounted<ReadFile>::delref(); }
		Future<int> read(void* data, int length, int64_t offset) {
			return asyncTaskThread.execAsync([client = this->client, containerName = this->containerName,
			                                  blobName = this->blobName, data, length, offset] {
				std::ostringstream oss(std::ios::out | std::ios::binary);
				client->download_blob_to_stream(containerName, blobName, offset, length, oss);
				auto str = oss.str();
				memcpy(data, str.c_str(), str.size());
				return static_cast<int>(str.size());
			});
		}
		Future<Void> zeroRange(int64_t offset, int64_t length) override { throw file_not_writable(); }
		Future<Void> write(void const* data, int length, int64_t offset) override { throw file_not_writable(); }
		Future<Void> truncate(int64_t size) override { throw file_not_writable(); }
		Future<Void> sync() override { throw file_not_writable(); }
		Future<int64_t> size() const override {
			return asyncTaskThread.execAsync([client = this->client, containerName = this->containerName,
			                                  blobName = this->blobName] {
				return static_cast<int64_t>(client->get_blob_properties(containerName, blobName).get().response().size);
			});
		}
		std::string getFilename() const override { return blobName; }
		int64_t debugFD() const override { return 0; }
	};

	class WriteFile final : public IAsyncFile, ReferenceCounted<WriteFile> {
		AsyncTaskThread& asyncTaskThread;
		AzureClient* client;
		std::string containerName;
		std::string blobName;
		int64_t m_cursor{ 0 };
		// Ideally this buffer should not be a string, but
		// the Azure SDK only supports/tests uploading to append
		// blobs from a stringstream.
		std::string buffer;

		static constexpr size_t bufferLimit = 1 << 20;

	public:
		WriteFile(AsyncTaskThread& asyncTaskThread, const std::string& containerName, const std::string& blobName,
		          AzureClient* client)
		  : asyncTaskThread(asyncTaskThread), containerName(containerName), blobName(blobName), client(client) {}

		void addref() override { ReferenceCounted<WriteFile>::addref(); }
		void delref() override { ReferenceCounted<WriteFile>::delref(); }
		Future<int> read(void* data, int length, int64_t offset) override { throw file_not_readable(); }
		Future<Void> write(void const* data, int length, int64_t offset) override {
			if (offset != m_cursor) {
				throw non_sequential_op();
			}
			m_cursor += length;
			auto p = static_cast<char const*>(data);
			buffer.insert(buffer.cend(), p, p + length);
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
			auto movedBuffer = std::move(buffer);
			buffer.clear();
			return asyncTaskThread.execAsync([client = this->client, containerName = this->containerName,
			                                  blobName = this->blobName, buffer = std::move(movedBuffer)] {
				std::istringstream iss(std::move(buffer));
				auto resp = client->append_block_from_stream(containerName, blobName, iss).get();
				return Void();
			});
		}
		Future<int64_t> size() const override {
			return asyncTaskThread.execAsync(
			    [client = this->client, containerName = this->containerName, blobName = this->blobName] {
				    auto resp = client->get_blob_properties(containerName, blobName).get().response();
				    ASSERT(resp.valid()); // TODO: Should instead throw here
				    return static_cast<int64_t>(resp.size);
			    });
		}
		std::string getFilename() const override { return blobName; }
		int64_t debugFD() const override { return -1; }
	};

	class BackupFile final : public IBackupFile, ReferenceCounted<BackupFile> {
		Reference<IAsyncFile> m_file;

	public:
		BackupFile(const std::string& fileName, Reference<IAsyncFile> file) : IBackupFile(fileName), m_file(file) {}
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
		void addref() override { ReferenceCounted<BackupFile>::addref(); }
		void delref() override { ReferenceCounted<BackupFile>::delref(); }
	};

	static bool isDirectory(const std::string& blobName) { return blobName.size() && blobName.back() == '/'; }

	ACTOR static Future<Reference<IAsyncFile>> readFile(BackupContainerAzureBlobStore* self, std::string fileName) {
		bool exists = wait(self->blobExists(fileName));
		if (!exists) {
			throw file_not_found();
		}
		return Reference<IAsyncFile>(
		    new ReadFile(self->asyncTaskThread, self->containerName, fileName, self->client.get()));
	}

	ACTOR static Future<Reference<IBackupFile>> writeFile(BackupContainerAzureBlobStore* self, std::string fileName) {
		wait(self->asyncTaskThread.execAsync(
		    [client = self->client.get(), containerName = self->containerName, fileName = fileName] {
			    auto outcome = client->create_append_blob(containerName, fileName).get();
			    return Void();
		    }));
		return Reference<IBackupFile>(
		    new BackupFile(fileName, Reference<IAsyncFile>(new WriteFile(self->asyncTaskThread, self->containerName,
		                                                                 fileName, self->client.get()))));
	}

	static void listFiles(AzureClient* client, const std::string& containerName, const std::string& path,
	                      std::function<bool(std::string const&)> folderPathFilter,
	                      BackupContainerFileSystem::FilesAndSizesT& result) {
		auto resp = client->list_blobs_segmented(containerName, "/", "", path).get().response();
		for (const auto& blob : resp.blobs) {
			if (isDirectory(blob.name) && folderPathFilter(blob.name)) {
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
		wait(self->asyncTaskThread.execAsync([containerName = self->containerName, client = self->client.get()] {
			client->delete_container(containerName).wait();
			return Void();
		}));
		if (pNumDeleted) {
			*pNumDeleted += filesToDelete;
		}
		return Void();
	}
};

Future<bool> BackupContainerAzureBlobStore::blobExists(const std::string& fileName) {
	return asyncTaskThread.execAsync(
	    [client = this->client.get(), containerName = this->containerName, fileName = fileName] {
		    auto resp = client->get_blob_properties(containerName, fileName).get().response();
		    return resp.valid();
	    });
}

BackupContainerAzureBlobStore::BackupContainerAzureBlobStore(const NetworkAddress& address,
                                                             const std::string& accountName,
                                                             const std::string& containerName)
  : containerName(containerName) {
	std::string accountKey = std::getenv("AZURE_KEY");

	auto credential = std::make_shared<azure::storage_lite::shared_key_credential>(accountName, accountKey);
	auto storageAccount = std::make_shared<azure::storage_lite::storage_account>(
	    accountName, credential, false, format("http://%s/%s", address.toString().c_str(), accountName.c_str()));

	client = std::make_unique<AzureClient>(storageAccount, 1);
}

void BackupContainerAzureBlobStore::addref() {
	return ReferenceCounted<BackupContainerAzureBlobStore>::addref();
}
void BackupContainerAzureBlobStore::delref() {
	return ReferenceCounted<BackupContainerAzureBlobStore>::delref();
}

Future<Void> BackupContainerAzureBlobStore::create() {
	return asyncTaskThread.execAsync([containerName = this->containerName, client = this->client.get()] {
		client->create_container(containerName).wait();
		return Void();
	});
}
Future<bool> BackupContainerAzureBlobStore::exists() {
	return asyncTaskThread.execAsync([containerName = this->containerName, client = this->client.get()] {
		auto resp = client->get_container_properties(containerName).get().response();
		return resp.valid();
	});
}

Future<Reference<IAsyncFile>> BackupContainerAzureBlobStore::readFile(const std::string& fileName) {
	return BackupContainerAzureBlobStoreImpl::readFile(this, fileName);
}

Future<Reference<IBackupFile>> BackupContainerAzureBlobStore::writeFile(const std::string& fileName) {
	return BackupContainerAzureBlobStoreImpl::writeFile(this, fileName);
}

Future<BackupContainerFileSystem::FilesAndSizesT> BackupContainerAzureBlobStore::listFiles(
    const std::string& path, std::function<bool(std::string const&)> folderPathFilter) {
	return asyncTaskThread.execAsync([client = this->client.get(), containerName = this->containerName, path = path,
	                                  folderPathFilter = folderPathFilter] {
		FilesAndSizesT result;
		BackupContainerAzureBlobStoreImpl::listFiles(client, containerName, path, folderPathFilter, result);
		return result;
	});
}

Future<Void> BackupContainerAzureBlobStore::deleteFile(const std::string& fileName) {
	return asyncTaskThread.execAsync(
	    [containerName = this->containerName, fileName = fileName, client = client.get()]() {
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
	return "azure://<ip>:<port>/<accountname>/<container>/<path_to_file>";
}
