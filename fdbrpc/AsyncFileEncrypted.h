/*
 * AsyncFileEncrypted.h
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

#pragma once

#include "fdbrpc/IAsyncFile.h"
#include "flow/FastRef.h"
#include "flow/flow.h"
#include "flow/IRandom.h"
#include "flow/StreamCipher.h"

#if ENCRYPTION_ENABLED

#include <array>

/*
 * Append-only file encrypted using AES-128-GCM.
 * */
class AsyncFileEncrypted : public IAsyncFile, public ReferenceCounted<AsyncFileEncrypted> {
public:
	enum class Mode { APPEND_ONLY, READ_ONLY };

private:
	Reference<IAsyncFile> file;
	StreamCipher::IV firstBlockIV;
	StreamCipher::IV getIV(uint32_t block) const;
	Mode mode;
	Future<Void> writeLastBlockToFile();
	friend class AsyncFileEncryptedImpl;

	// Reading:
	class RandomCache {
		size_t maxSize;
		std::vector<uint32_t> vec;
		std::unordered_map<uint32_t, Standalone<StringRef>> hashMap;
		size_t evict();

	public:
		RandomCache(size_t maxSize);
		void insert(uint32_t block, const Standalone<StringRef>& value);
		Optional<Standalone<StringRef>> get(uint32_t block) const;
	} readBuffers;

	// Writing (append only):
	std::unique_ptr<EncryptionStreamCipher> encryptor;
	uint32_t currentBlock{ 0 };
	int offsetInBlock{ 0 };
	std::vector<unsigned char> writeBuffer;
	Future<Void> initialize();

public:
	AsyncFileEncrypted(Reference<IAsyncFile>, Mode);
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
};

#endif // ENCRYPTION_ENABLED
