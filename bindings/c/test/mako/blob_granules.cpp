#include "blob_granules.hpp"
#include "logger.hpp"
#include <cstdio>
#include <fdb.hpp>

extern thread_local mako::Logger logr;

namespace mako::blob_granules::local_file {

int64_t startLoad(const char* filename,
                  int filenameLength,
                  int64_t offset,
                  int64_t length,
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
	return ret;
}

} // namespace mako::blob_granules::local_file
