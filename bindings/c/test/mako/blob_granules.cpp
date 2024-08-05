/*
 * blob_granules.cpp
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

#include "blob_granules.hpp"
#include "limit.hpp"
#include "logger.hpp"
#include <cstdio>
#include <fdb_api.hpp>

extern thread_local mako::Logger logr;

// FIXME: use the same implementation as the api tester! this implementation was from back when mako was written in C
// and is inferior.

namespace mako::blob_granules::local_file {

int64_t startLoad(const char* filename,
                  int filenameLength,
                  int64_t offset,
                  int64_t length,
                  int64_t fullFileLength,
                  void* userContext) {
	FILE* fp;
	char full_fname[PATH_MAX]{
		0,
	};
	int loadId;
	uint8_t* data;
	size_t readSize;

	auto context = static_cast<UserContext*>(userContext);

	loadId = context->nextId;
	if (context->dataById[loadId] != 0) {
		logr.error("too many granule file loads at once: {}", MAX_BG_IDS);
		return -1;
	}
	context->nextId = (context->nextId + 1) % MAX_BG_IDS;

	int ret = snprintf(full_fname, PATH_MAX, "%s%s", context->bgFilePath, filename);
	if (ret < 0 || ret >= PATH_MAX) {
		logr.error("BG filename too long: {}{}", context->bgFilePath, filename);
		return -1;
	}

	fp = fopen(full_fname, "r");
	if (!fp) {
		logr.error("BG could not open file: {}", full_fname);
		return -1;
	}

	// don't seek if offset == 0
	if (offset && fseek(fp, offset, SEEK_SET)) {
		// if fseek was non-zero, it failed
		logr.error("BG could not seek to %{} in file {}", offset, full_fname);
		fclose(fp);
		return -1;
	}

	data = new uint8_t[length];
	readSize = fread(data, sizeof(uint8_t), length, fp);
	fclose(fp);

	if (readSize != length) {
		logr.error("BG could not read {} bytes from file: {}", length, full_fname);
		return -1;
	}

	context->dataById[loadId] = data;
	return loadId;
}

uint8_t* getLoad(int64_t loadId, void* userContext) {
	auto context = static_cast<UserContext*>(userContext);
	if (context->dataById[loadId] == 0) {
		logr.error("BG loadId invalid for get_load: {}", loadId);
		return 0;
	}
	return context->dataById[loadId];
}

void freeLoad(int64_t loadId, void* userContext) {
	auto context = static_cast<UserContext*>(userContext);
	if (context->dataById[loadId] == 0) {
		logr.error("BG loadId invalid for free_load: {}", loadId);
	}
	delete[] context->dataById[loadId];
	context->dataById[loadId] = 0;
}

fdb::native::FDBReadBlobGranuleContext createApiContext(UserContext& ctx, bool materialize_files) {
	auto ret = fdb::native::FDBReadBlobGranuleContext{};
	ret.userContext = &ctx;
	ret.start_load_f = &startLoad;
	ret.get_load_f = &getLoad;
	ret.free_load_f = &freeLoad;
	ret.debugNoMaterialize = !materialize_files;
	ret.granuleParallelism = 2; // TODO make knob or setting for changing this?
	return ret;
}

} // namespace mako::blob_granules::local_file
