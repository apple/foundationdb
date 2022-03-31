/*
 * AsyncFile.cpp
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

#include "fdbserver/workloads/workloads.actor.h"
#include "flow/ActorCollection.h"
#include "fdbserver/workloads/AsyncFile.actor.h"
#include "flow/actorcompiler.h"

// class RandomByteGenerator

RandomByteGenerator::RandomByteGenerator() {
	BUF_SIZE = 16 * (1 << 20);
	b1 = new char[BUF_SIZE];
	for (int i = 0; i < BUF_SIZE / sizeof(uint32_t); i++)
		((uint32_t*)b1)[i] = deterministicRandom()->randomUInt32();
}

RandomByteGenerator::~RandomByteGenerator() {
	delete b1;
}

// only works if buf and bytes are 8-byte aligned
void RandomByteGenerator::writeRandomBytesToBuffer(void* buf, int bytes) {
	ASSERT(bytes < BUF_SIZE - 1);
	int o1, o2;
	o1 = deterministicRandom()->randomInt(0, BUF_SIZE - bytes) / 8;
	do {
		o2 = deterministicRandom()->randomInt(0, BUF_SIZE - bytes) / 8;
	} while (o1 == o2);

	int64_t* out64 = (int64_t*)buf;
	int64_t* in64 = (int64_t*)b1;
	int n = bytes / 8;
	for (int b = 0; b < n; b++)
		out64[b] = in64[o1 + b] ^ in64[o2 + b];

	// for (int b=0;b<bytes;b++){
	//((char*)buf)[b] = b1[o1+b] ^ b1[o2+b];
	//}
}

//// Asynch File Workload

const int AsyncFileWorkload::_PAGE_SIZE = 4096;

AsyncFileWorkload::AsyncFileWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), fileHandle(nullptr) {
	// Only run on one client
	enabled = clientId == 0;
	testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
	unbufferedIO = getOption(options, LiteralStringRef("unbufferedIO"), false);
	uncachedIO = getOption(options, LiteralStringRef("uncachedIO"), false);
	fillRandom = getOption(options, LiteralStringRef("fillRandom"), false);
	path = getOption(options, LiteralStringRef("fileName"), LiteralStringRef("")).toString();
}

Reference<AsyncFileBuffer> AsyncFileWorkload::allocateBuffer(size_t size) {
	return makeReference<AsyncFileBuffer>(size, unbufferedIO);
}

Future<bool> AsyncFileWorkload::check(Database const& cx) {
	return true;
}

// Allocates a buffer of a given size.  If necessary, the buffer will be aligned to 4K
AsyncFileBuffer::AsyncFileBuffer(size_t size, bool aligned) {
	if (aligned) {
#ifdef WIN32
		buffer = (unsigned char*)_aligned_malloc(size, AsyncFileWorkload::_PAGE_SIZE);
#else
		if (posix_memalign((void**)&buffer, AsyncFileWorkload::_PAGE_SIZE, size) != 0)
			buffer = nullptr;
#endif
	} else
		buffer = (unsigned char*)malloc(size);

	if (buffer == nullptr) {
		TraceEvent(SevError, "TestFailure").detail("Reason", "Insufficient memory");
		ASSERT(false);
	}

	memset(buffer, 0, size);
	this->aligned = aligned;
}

// Special logic needed here to work with _aligned_malloc on windows
AsyncFileBuffer::~AsyncFileBuffer() {
#ifdef WIN32
	if (aligned) {
		_aligned_free(buffer);
		return;
	}
#endif

	free(buffer);
}

AsyncFileHandle::AsyncFileHandle(Reference<IAsyncFile> file, std::string path, bool temporary) {
	this->file = file;
	this->path = path;
	this->temporary = temporary;
}

AsyncFileHandle::~AsyncFileHandle() {
	if (temporary)
		deleteFile(path);
}
