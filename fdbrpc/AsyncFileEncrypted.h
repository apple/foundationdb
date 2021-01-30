/*
 * AsyncFileEncrypted.h
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

#ifndef __FDBRPC_ASYNC_FILE_ENCRYPTED_H__
#define __FDBRPC_ASYNC_FILE_ENCRYPTED_H__

#include "fdbrpc/IAsyncFile.h"
#include "flow/FastRef.h"
#include "flow/flow.h"
#include "flow/IRandom.h"
#include "flow/StreamCipher.h"

#include <array>

/*
 * Append-only file encrypted using AES-128-GCM.
 * */
class AsyncFileEncrypted : public IAsyncFile, public ReferenceCounted<AsyncFileEncrypted> {
	Reference<IAsyncFile> file;
	StreamCipher::IV firstBlockIV;
	StreamCipher::IV getIV(uint16_t block) const;
	bool canWrite;
	Future<Void> writeLastBlockToFile();
	friend class AsyncFileEncryptedImpl;
	static Optional<StreamCipher::Key> key;
	static StreamCipher::Key getKey();

	template <class K, class V>
	class RandomCache {
		size_t maxSize;
		std::vector<K> vec;
		std::unordered_map<K, V> hashMap;

		size_t evict() {
			ASSERT(vec.size() == maxSize);
			auto index = deterministicRandom()->randomInt(0, maxSize);
			hashMap.erase(vec[index]);
			return index;
		}

	public:
		RandomCache(size_t maxSize) : maxSize(maxSize) { vec.reserve(maxSize); }

		void insert(const K& key, const V& value) {
			auto [it, found] = hashMap.insert({ key, value });
			if (found) {
				return;
			} else if (vec.size() < maxSize) {
				vec.push_back(key);
			} else {
				auto index = evict();
				vec[index] = key;
			}
		}

		Optional<V> get(const K& key) const {
			auto it = hashMap.find(key);
			if (it == hashMap.end()) {
				return {};
			} else {
				return it->second;
			}
		}
	};

	// Reading:
	RandomCache<uint16_t, Standalone<StringRef>> readBuffers;

	// Writing (append only):
	std::unique_ptr<EncryptionStreamCipher> encryptor;
	uint16_t currentBlock{ 0 };
	int offsetInBlock{ 0 };
	std::vector<unsigned char> writeBuffer;

public:
	AsyncFileEncrypted(Reference<IAsyncFile>, bool canWrite);
	void addref() override;
	void delref() override;
	Future<int> read(void* data, int length, int64_t offset) override;
	Future<Void> write(void const* data, int length, int64_t offset) override;
	Future<Void> zeroRange(int64_t offset, int64_t length) override;
	Future<Void> truncate(int64_t size) override;
	Future<Void> sync() override;
	Future<Void> flush() override;
	Future<int64_t> size() const override;
	std::string getFilename() const override;
	Future<Void> readZeroCopy(void** data, int* length, int64_t offset) override;
	void releaseZeroCopy(void* data, int length, int64_t offset) override;
	int64_t debugFD() const override;
	static Future<Void> initializeKey(const Reference<IAsyncFile>& keyFile, int64_t offset = 0);
};

#endif
