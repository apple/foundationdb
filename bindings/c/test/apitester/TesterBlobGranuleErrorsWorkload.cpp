/*
 * TesterBlobGranuleErrorsWorkload.cpp
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
#include "TesterApiWorkload.h"
#include "TesterUtil.h"
#include <memory>
#include <fmt/format.h>

namespace FdbApiTester {

// FIXME: avoid duplicating this between files!
class TesterErrorGranuleContext {
public:
	std::unordered_map<int64_t, uint8_t*> loadsInProgress;
	int64_t nextId = 0;
	std::string basePath;

	~TesterErrorGranuleContext() {
		// if there was an error or not all loads finished, delete data
		for (auto& it : loadsInProgress) {
			uint8_t* dataToFree = it.second;
			delete[] dataToFree;
		}
	}
};

static int64_t granule_start_load(const char* filename,
                                  int filenameLength,
                                  int64_t offset,
                                  int64_t length,
                                  int64_t fullFileLength,
                                  void* context) {

	TesterErrorGranuleContext* ctx = (TesterErrorGranuleContext*)context;
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
	TesterErrorGranuleContext* ctx = (TesterErrorGranuleContext*)context;
	return ctx->loadsInProgress.at(loadId);
}

static void granule_free_load(int64_t loadId, void* context) {
	TesterErrorGranuleContext* ctx = (TesterErrorGranuleContext*)context;
	auto it = ctx->loadsInProgress.find(loadId);
	uint8_t* dataToFree = it->second;
	delete[] dataToFree;

	ctx->loadsInProgress.erase(it);
}

class BlobGranuleErrorsWorkload : public ApiWorkload {
public:
	BlobGranuleErrorsWorkload(const WorkloadConfig& config) : ApiWorkload(config) {}

private:
	enum OpType {
		OP_READ_NO_MATERIALIZE,
		OP_READ_FILE_LOAD_ERROR,
		OP_READ_TOO_OLD,
		OP_CANCEL_RANGES,
		OP_LAST = OP_CANCEL_RANGES
	};

	void doErrorOp(TTaskFct cont,
	               std::string basePathAddition,
	               bool doMaterialize,
	               int64_t readVersion,
	               fdb::native::fdb_error_t expectedError) {
		fdb::Key begin = randomKeyName();
		fdb::Key end = begin;
		// [K - K) empty range will succeed read because there is trivially nothing to do, so don't do it
		while (end == begin) {
			end = randomKeyName();
		}
		if (begin > end) {
			std::swap(begin, end);
		}

		execTransaction(
		    [this, begin, end, basePathAddition, doMaterialize, readVersion, expectedError](auto ctx) {
			    ctx->tx().setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);

			    TesterErrorGranuleContext testerContext;
			    testerContext.basePath = ctx->getBGBasePath() + basePathAddition;

			    fdb::native::FDBReadBlobGranuleContext granuleContext;
			    granuleContext.userContext = &testerContext;
			    granuleContext.debugNoMaterialize = !doMaterialize;
			    granuleContext.granuleParallelism = 1 + Random::get().randomInt(0, 3);
			    granuleContext.start_load_f = &granule_start_load;
			    granuleContext.get_load_f = &granule_get_load;
			    granuleContext.free_load_f = &granule_free_load;

			    fdb::Result res =
			        ctx->tx().readBlobGranules(begin, end, 0 /* beginVersion */, readVersion, granuleContext);
			    auto out = fdb::Result::KeyValueRefArray{};
			    fdb::Error err = res.getKeyValueArrayNothrow(out);

			    if (err.code() == error_code_success) {
				    error(fmt::format("Operation succeeded in error test!"));
			    }
			    ASSERT(err.code() != error_code_success);
			    if (err.code() != expectedError) {
				    info(fmt::format("incorrect error. Expected {}, Got {}", err.code(), expectedError));
				    ctx->onError(err);
			    } else {
				    ctx->done();
			    }
		    },
		    [this, cont]() { schedule(cont); },
		    false);
	}

	void randomOpReadNoMaterialize(TTaskFct cont) {
		info("DoErrorOp NoMaterialize");
		// ensure setting noMaterialize flag produces blob_granule_not_materialized
		doErrorOp(cont, "", false, -2 /*latest read version */, error_code_blob_granule_not_materialized);
	}

	void randomOpReadFileLoadError(TTaskFct cont) {
		info("DoErrorOp FileError");
		// point to a file path that doesn't exist by adding an extra suffix
		doErrorOp(cont, "extrapath/", true, -2 /*latest read version */, error_code_blob_granule_file_load_error);
	}

	void randomOpReadTooOld(TTaskFct cont) {
		info("DoErrorOp TooOld");
		// read at a version (1) that should predate granule data
		doErrorOp(cont, "", true, 1, error_code_blob_granule_transaction_too_old);
	}

	void randomCancelGetRangesOp(TTaskFct cont) {
		info("DoErrorOp CancelGetRanges");
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execTransaction(
		    [begin, end](auto ctx) {
			    fdb::Future f = ctx->tx().getBlobGranuleRanges(begin, end, 1000).eraseType();
			    ctx->done();
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomOperation(TTaskFct cont) override {
		OpType txType = (OpType)Random::get().randomInt(0, OP_LAST);
		switch (txType) {
		case OP_READ_NO_MATERIALIZE:
			randomOpReadNoMaterialize(cont);
			break;
		case OP_READ_FILE_LOAD_ERROR:
			randomOpReadFileLoadError(cont);
			break;
		case OP_READ_TOO_OLD:
			randomOpReadTooOld(cont);
			break;
		case OP_CANCEL_RANGES:
			randomCancelGetRangesOp(cont);
			break;
		}
	}
};

WorkloadFactory<BlobGranuleErrorsWorkload> BlobGranuleErrorsWorkloadFactory("BlobGranuleErrors");

} // namespace FdbApiTester
