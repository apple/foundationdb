/*
 * TesterBlobGranuleCorrectnessWorkload.cpp
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

class TesterGranuleContext {
public:
	std::unordered_map<int64_t, uint8_t*> loadsInProgress;
	int64_t nextId = 0;
	std::string basePath;

	~TesterGranuleContext() {
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

	TesterGranuleContext* ctx = (TesterGranuleContext*)context;
	int64_t loadId = ctx->nextId++;

	uint8_t* buffer = new uint8_t[length];
	std::ifstream fin(ctx->basePath + std::string(filename, filenameLength), std::ios::in | std::ios::binary);
	fin.seekg(offset);
	fin.read((char*)buffer, length);

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

class ApiBlobGranuleCorrectnessWorkload : public ApiWorkload {
public:
	ApiBlobGranuleCorrectnessWorkload(const WorkloadConfig& config) : ApiWorkload(config) {
		// sometimes don't do range clears
		if (Random::get().randomInt(0, 1) == 0) {
			excludedOpTypes.push_back(OP_CLEAR_RANGE);
		}
	}

private:
	enum OpType { OP_INSERT, OP_CLEAR, OP_CLEAR_RANGE, OP_READ, OP_GET_RANGES, OP_LAST = OP_GET_RANGES };
	std::vector<OpType> excludedOpTypes;

	// Allow reads at the start to get blob_granule_transaction_too_old if BG data isn't initialized yet
	// FIXME: should still guarantee a read succeeds eventually somehow
	bool seenReadSuccess = false;

	void randomReadOp(TTaskFct cont) {
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		auto results = std::make_shared<std::vector<fdb::KeyValue>>();
		auto tooOld = std::make_shared<bool>(false);
		if (begin > end) {
			std::swap(begin, end);
		}
		execTransaction(
		    [this, begin, end, results, tooOld](auto ctx) {
			    ctx->tx().setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
			    TesterGranuleContext testerContext;
			    testerContext.basePath = ctx->getBGBasePath();

			    fdb::native::FDBReadBlobGranuleContext granuleContext;
			    granuleContext.userContext = &testerContext;
			    granuleContext.debugNoMaterialize = false;
			    granuleContext.granuleParallelism = 1;
			    granuleContext.start_load_f = &granule_start_load;
			    granuleContext.get_load_f = &granule_get_load;
			    granuleContext.free_load_f = &granule_free_load;

			    fdb::Result res = ctx->tx().readBlobGranules(
			        begin, end, 0 /* beginVersion */, -2 /* latest read version */, granuleContext);
			    auto out = fdb::Result::KeyValueRefArray{};
			    fdb::Error err = res.getKeyValueArrayNothrow(out);
			    if (err.code() == error_code_blob_granule_transaction_too_old) {
				    info("BlobGranuleCorrectness::randomReadOp bg too old\n");
				    ASSERT(!seenReadSuccess);
				    *tooOld = true;
				    ctx->done();
			    } else if (err.code() != error_code_success) {
				    ctx->onError(err);
			    } else {
				    auto& [out_kv, out_count, out_more] = out;
				    ASSERT(!out_more);
				    if (!seenReadSuccess) {
					    info("BlobGranuleCorrectness::randomReadOp first success\n");
				    }
				    seenReadSuccess = true;
				    ctx->done();
			    }
		    },
		    [this, begin, end, results, tooOld, cont]() {
			    if (!*tooOld) {
				    std::vector<fdb::KeyValue> expected = store.getRange(begin, end, store.size(), false);
				    if (results->size() != expected.size()) {
					    error(fmt::format("randomReadOp result size mismatch. expected: {} actual: {}",
					                      expected.size(),
					                      results->size()));
				    }
				    ASSERT(results->size() == expected.size());

				    for (int i = 0; i < results->size(); i++) {
					    if ((*results)[i].key != expected[i].key) {
						    error(fmt::format("randomReadOp key mismatch at {}/{}. expected: {} actual: {}",
						                      i,
						                      results->size(),
						                      fdb::toCharsRef(expected[i].key),
						                      fdb::toCharsRef((*results)[i].key)));
					    }
					    ASSERT((*results)[i].key == expected[i].key);

					    if ((*results)[i].value != expected[i].value) {
						    error(fmt::format(
						        "randomReadOp value mismatch at {}/{}. key: {} expected: {:.80} actual: {:.80}",
						        i,
						        results->size(),
						        fdb::toCharsRef(expected[i].key),
						        fdb::toCharsRef(expected[i].value),
						        fdb::toCharsRef((*results)[i].value)));
					    }
					    ASSERT((*results)[i].value == expected[i].value);
				    }
			    }
			    schedule(cont);
		    });
	}

	void randomGetRangesOp(TTaskFct cont) {
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		auto results = std::make_shared<std::vector<fdb::KeyRange>>();
		if (begin > end) {
			std::swap(begin, end);
		}
		execTransaction(
		    [begin, end, results](auto ctx) {
			    fdb::Future f = ctx->tx().getBlobGranuleRanges(begin, end).eraseType();
			    ctx->continueAfter(
			        f,
			        [ctx, f, results]() {
				        *results = copyKeyRangeArray(f.get<fdb::future_var::KeyRangeRefArray>());
				        ctx->done();
			        },
			        true);
		    },
		    [this, begin, end, results, cont]() {
			    if (seenReadSuccess) {
				    ASSERT(results->size() > 0);
				    ASSERT(results->front().beginKey <= begin);
				    ASSERT(results->back().endKey >= end);
			    }

			    for (int i = 0; i < results->size(); i++) {
				    // no empty or inverted ranges
				    ASSERT((*results)[i].beginKey < (*results)[i].endKey);
			    }

			    for (int i = 1; i < results->size(); i++) {
				    // ranges contain entire requested key range
				    ASSERT((*results)[i].beginKey == (*results)[i - 1].endKey);
			    }

			    schedule(cont);
		    });
	}

	void randomOperation(TTaskFct cont) {
		OpType txType = (store.size() == 0) ? OP_INSERT : (OpType)Random::get().randomInt(0, OP_LAST);
		while (std::count(excludedOpTypes.begin(), excludedOpTypes.end(), txType)) {
			txType = (OpType)Random::get().randomInt(0, OP_LAST);
		}
		switch (txType) {
		case OP_INSERT:
			randomInsertOp(cont);
			break;
		case OP_CLEAR:
			randomClearOp(cont);
			break;
		case OP_CLEAR_RANGE:
			randomClearRangeOp(cont);
			break;
		case OP_READ:
			randomReadOp(cont);
			break;
		case OP_GET_RANGES:
			randomGetRangesOp(cont);
			break;
		}
	}
};

WorkloadFactory<ApiBlobGranuleCorrectnessWorkload> ApiBlobGranuleCorrectnessWorkloadFactory(
    "ApiBlobGranuleCorrectness");

} // namespace FdbApiTester
