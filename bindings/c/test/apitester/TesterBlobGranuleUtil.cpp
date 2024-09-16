/*
 * TesterBlobGranuleUtil.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "TesterBlobGranuleUtil.h"
#include "TesterUtil.h"
#include <fstream>

namespace FdbApiTester {

// FIXME: avoid duplicating this between files!
static int64_t granule_start_load(const char* filename,
                                  int filenameLength,
                                  int64_t offset,
                                  int64_t length,
                                  int64_t fullFileLength,
                                  void* context) {

	TesterGranuleContext* ctx = (TesterGranuleContext*)context;
	int64_t loadId = ctx->nextId++;

	uint8_t* buffer = new uint8_t[length];
	std::ifstream fin(ctx->basePath + std::string(filename, filenameLength), std::ios::in | std::ios::binary);
	if (fin.fail()) {
		delete[] buffer;
		buffer = nullptr;
	} else {
		fin.seekg(offset);
		fin.read((char*)buffer, length);
	}

	ctx->loadsInProgress.insert({ loadId, buffer });

	return loadId;
}

static uint8_t* granule_get_load(int64_t loadId, void* context) {
	TesterGranuleContext* ctx = (TesterGranuleContext*)context;
	return ctx->loadsInProgress.at(loadId);
}

static void granule_free_load(int64_t loadId, void* context) {
	TesterGranuleContext* ctx = (TesterGranuleContext*)context;
	auto it = ctx->loadsInProgress.find(loadId);
	uint8_t* dataToFree = it->second;
	delete[] dataToFree;

	ctx->loadsInProgress.erase(it);
}

fdb::native::FDBReadBlobGranuleContext createGranuleContext(const TesterGranuleContext* testerContext) {
	fdb::native::FDBReadBlobGranuleContext granuleContext;

	granuleContext.userContext = (void*)testerContext;
	granuleContext.debugNoMaterialize = false;
	granuleContext.granuleParallelism = 1 + Random::get().randomInt(0, 3);
	granuleContext.start_load_f = &granule_start_load;
	granuleContext.get_load_f = &granule_get_load;
	granuleContext.free_load_f = &granule_free_load;

	return granuleContext;
}

} // namespace FdbApiTester