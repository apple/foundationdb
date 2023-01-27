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
#include "TesterBlobGranuleUtil.h"
#include "TesterUtil.h"
#include <unordered_set>
#include <memory>
#include <fmt/format.h>

namespace FdbApiTester {

#define BG_API_DEBUG_VERBOSE false

class ApiBlobGranuleCorrectnessWorkload : public ApiWorkload {
public:
	ApiBlobGranuleCorrectnessWorkload(const WorkloadConfig& config) : ApiWorkload(config) {
		// sometimes don't do range clears
		if (Random::get().randomInt(0, 1) == 0) {
			excludedOpTypes.push_back(OP_CLEAR_RANGE);
		}
	}

private:
	// FIXME: add tenant support for DB operations
	enum OpType {
		OP_INSERT,
		OP_CLEAR,
		OP_CLEAR_RANGE,
		OP_READ,
		OP_GET_GRANULES,
		OP_SUMMARIZE,
		OP_GET_BLOB_RANGES,
		OP_VERIFY,
		OP_LAST = OP_VERIFY
	};
	std::vector<OpType> excludedOpTypes;

	void setup(TTaskFct cont) override { setupBlobGranules(cont); }

	// Allow reads at the start to get blob_granule_transaction_too_old if BG data isn't initialized yet
	// FIXME: should still guarantee a read succeeds eventually somehow
	std::unordered_set<std::optional<int>> tenantsWithReadSuccess;

	inline void setReadSuccess(std::optional<int> tenantId) { tenantsWithReadSuccess.insert(tenantId); }

	inline bool seenReadSuccess(std::optional<int> tenantId) { return tenantsWithReadSuccess.count(tenantId); }

	void debugOp(std::string opName, fdb::KeyRange keyRange, std::optional<int> tenantId, std::string message) {
		if (BG_API_DEBUG_VERBOSE) {
			info(fmt::format("{0}: [{1} - {2}) {3}: {4}",
			                 opName,
			                 fdb::toCharsRef(keyRange.beginKey),
			                 fdb::toCharsRef(keyRange.endKey),
			                 debugTenantStr(tenantId),
			                 message));
		}
	}

	void randomReadOp(TTaskFct cont, std::optional<int> tenantId) {
		fdb::KeyRange keyRange = randomNonEmptyKeyRange();

		auto results = std::make_shared<std::vector<fdb::KeyValue>>();
		auto tooOld = std::make_shared<bool>(false);

		debugOp("Read", keyRange, tenantId, "starting");

		execTransaction(
		    [this, keyRange, tenantId, results, tooOld](auto ctx) {
			    ctx->tx().setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
			    TesterGranuleContext testerContext(ctx->getBGBasePath());
			    fdb::native::FDBReadBlobGranuleContext granuleContext = createGranuleContext(&testerContext);

			    fdb::Result res = ctx->tx().readBlobGranules(keyRange.beginKey,
			                                                 keyRange.endKey,
			                                                 0 /* beginVersion */,
			                                                 -2 /* latest read version */,
			                                                 granuleContext);
			    auto out = fdb::Result::KeyValueRefArray{};
			    fdb::Error err = res.getKeyValueArrayNothrow(out);
			    if (err.code() == error_code_blob_granule_transaction_too_old) {
				    bool previousSuccess = seenReadSuccess(tenantId);
				    if (previousSuccess) {
					    error("Read bg too old after read success!\n");
				    } else {
					    info("Read bg too old\n");
				    }
				    ASSERT(!previousSuccess);
				    *tooOld = true;
				    ctx->done();
			    } else if (err.code() != error_code_success) {
				    ctx->onError(err);
			    } else {
				    auto resCopy = copyKeyValueArray(out);
				    auto& [resVector, out_more] = resCopy;
				    ASSERT(!out_more);
				    results.get()->assign(resVector.begin(), resVector.end());
				    bool previousSuccess = seenReadSuccess(tenantId);
				    if (!previousSuccess) {
					    info(fmt::format("Read {0}: first success\n", debugTenantStr(tenantId)));
					    setReadSuccess(tenantId);
				    } else {
					    debugOp("Read", keyRange, tenantId, "complete");
				    }
				    ctx->done();
			    }
		    },
		    [this, keyRange, results, tooOld, cont, tenantId]() {
			    if (!*tooOld) {
				    std::vector<fdb::KeyValue> expected =
				        stores[tenantId].getRange(keyRange.beginKey, keyRange.endKey, stores[tenantId].size(), false);
				    if (results->size() != expected.size()) {
					    error(fmt::format("randomReadOp result size mismatch. expected: {0} actual: {1}",
					                      expected.size(),
					                      results->size()));
				    }
				    ASSERT(results->size() == expected.size());

				    for (int i = 0; i < results->size(); i++) {
					    if ((*results)[i].key != expected[i].key) {
						    error(fmt::format("randomReadOp key mismatch at {0}/{1}. expected: {2} actual: {3}",
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
		    },
		    getTenant(tenantId));
	}

	void randomGetGranulesOp(TTaskFct cont, std::optional<int> tenantId) {
		fdb::KeyRange keyRange = randomNonEmptyKeyRange();
		auto results = std::make_shared<std::vector<fdb::KeyRange>>();

		debugOp("GetGranules", keyRange, tenantId, "starting");

		execTransaction(
		    [keyRange, results](auto ctx) {
			    fdb::Future f = ctx->tx().getBlobGranuleRanges(keyRange.beginKey, keyRange.endKey, 1000).eraseType();
			    ctx->continueAfter(
			        f,
			        [ctx, f, results]() {
				        *results = copyKeyRangeArray(f.get<fdb::future_var::KeyRangeRefArray>());
				        ctx->done();
			        },
			        true);
		    },
		    [this, keyRange, tenantId, results, cont]() {
			    debugOp("GetGranules", keyRange, tenantId, fmt::format("complete with {0} granules", results->size()));
			    this->validateRanges(results, keyRange, seenReadSuccess(tenantId));
			    schedule(cont);
		    },
		    getTenant(tenantId));
	}

	void randomSummarizeOp(TTaskFct cont, std::optional<int> tenantId) {
		if (!seenReadSuccess(tenantId)) {
			// tester can't handle this throwing bg_txn_too_old, so just don't call it unless we have already seen a
			// read success
			schedule(cont);
			return;
		}
		fdb::KeyRange keyRange = randomNonEmptyKeyRange();
		auto results = std::make_shared<std::vector<fdb::GranuleSummary>>();

		debugOp("Summarize", keyRange, tenantId, "starting");

		execTransaction(
		    [keyRange, results](auto ctx) {
			    fdb::Future f =
			        ctx->tx()
			            .summarizeBlobGranules(keyRange.beginKey, keyRange.endKey, -2 /*latest version*/, 1000)
			            .eraseType();
			    ctx->continueAfter(
			        f,
			        [ctx, f, results]() {
				        *results = copyGranuleSummaryArray(f.get<fdb::future_var::GranuleSummaryRefArray>());
				        ctx->done();
			        },
			        true);
		    },
		    [this, keyRange, tenantId, results, cont]() {
			    debugOp("Summarize", keyRange, tenantId, fmt::format("complete with {0} granules", results->size()));

			    // use validateRanges to share validation
			    auto ranges = std::make_shared<std::vector<fdb::KeyRange>>();

			    for (int i = 0; i < results->size(); i++) {
				    // TODO: could do validation of subsequent calls and ensure snapshot version never decreases
				    ASSERT((*results)[i].keyRange.beginKey < (*results)[i].keyRange.endKey);
				    ASSERT((*results)[i].snapshotVersion <= (*results)[i].deltaVersion);
				    ASSERT((*results)[i].snapshotSize > 0);
				    ASSERT((*results)[i].deltaSize >= 0);

				    ranges->push_back((*results)[i].keyRange);
			    }

			    this->validateRanges(ranges, keyRange, true);

			    schedule(cont);
		    },
		    getTenant(tenantId));
	}

	void validateRanges(std::shared_ptr<std::vector<fdb::KeyRange>> results,
	                    fdb::KeyRange keyRange,
	                    bool shouldBeRanges) {
		if (shouldBeRanges) {
			if (results->size() == 0) {
				error(fmt::format("ValidateRanges: [{0} - {1}): No ranges returned!",
				                  fdb::toCharsRef(keyRange.beginKey),
				                  fdb::toCharsRef(keyRange.endKey)));
			}
			ASSERT(results->size() > 0);
			if (results->front().beginKey > keyRange.beginKey || results->back().endKey < keyRange.endKey) {
				error(fmt::format("ValidateRanges: [{0} - {1}): Incomplete range(s) returned [{2} - {3})!",
				                  fdb::toCharsRef(keyRange.beginKey),
				                  fdb::toCharsRef(keyRange.endKey),
				                  fdb::toCharsRef(results->front().beginKey),
				                  fdb::toCharsRef(results->back().endKey)));
			}
			ASSERT(results->front().beginKey <= keyRange.beginKey);
			ASSERT(results->back().endKey >= keyRange.endKey);
		}
		for (int i = 0; i < results->size(); i++) {
			// no empty or inverted ranges
			if ((*results)[i].beginKey >= (*results)[i].endKey) {
				error(fmt::format("ValidateRanges: [{0} - {1}): Empty/inverted range [{2} - {3})",
				                  fdb::toCharsRef(keyRange.beginKey),
				                  fdb::toCharsRef(keyRange.endKey),
				                  fdb::toCharsRef((*results)[i].beginKey),
				                  fdb::toCharsRef((*results)[i].endKey)));
			}
			ASSERT((*results)[i].beginKey < (*results)[i].endKey);
		}

		for (int i = 1; i < results->size(); i++) {
			// ranges contain entire requested key range
			if ((*results)[i].beginKey != (*results)[i].endKey) {
				error(fmt::format("ValidateRanges: [{0} - {1}): Non-covereed range [{2} - {3})",
				                  fdb::toCharsRef(keyRange.beginKey),
				                  fdb::toCharsRef(keyRange.endKey),
				                  fdb::toCharsRef((*results)[i - 1].endKey),
				                  fdb::toCharsRef((*results)[i].endKey)));
			}
			ASSERT((*results)[i].beginKey == (*results)[i - 1].endKey);
		}
	}

	// TODO: tenant support
	void randomGetBlobRangesOp(TTaskFct cont, std::optional<int> tenantId) {
		fdb::KeyRange keyRange = randomNonEmptyKeyRange();

		auto results = std::make_shared<std::vector<fdb::KeyRange>>();

		debugOp("GetBlobRanges", keyRange, tenantId, "starting");

		execOperation(
		    [keyRange, results](auto ctx) {
			    fdb::Future f =
			        ctx->dbOps()->listBlobbifiedRanges(keyRange.beginKey, keyRange.endKey, 1000).eraseType();
			    ctx->continueAfter(f, [ctx, f, results]() {
				    *results = copyKeyRangeArray(f.get<fdb::future_var::KeyRangeRefArray>());
				    ctx->done();
			    });
		    },
		    [this, keyRange, tenantId, results, cont]() {
			    debugOp("GetBlobRanges", keyRange, tenantId, fmt::format("complete with {0} ranges", results->size()));
			    this->validateRanges(results, keyRange, seenReadSuccess(tenantId));
			    schedule(cont);
		    },
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	// TODO: tenant support
	void randomVerifyOp(TTaskFct cont, std::optional<int> tenantId) {
		fdb::KeyRange keyRange = randomNonEmptyKeyRange();

		debugOp("Verify", keyRange, tenantId, "starting");

		auto verifyVersion = std::make_shared<int64_t>(-1);
		execOperation(
		    [keyRange, verifyVersion](auto ctx) {
			    fdb::Future f = ctx->dbOps()
			                        ->verifyBlobRange(keyRange.beginKey, keyRange.endKey, -2 /* latest version*/)
			                        .eraseType();
			    ctx->continueAfter(f, [ctx, verifyVersion, f]() {
				    *verifyVersion = f.get<fdb::future_var::Int64>();
				    ctx->done();
			    });
		    },
		    [this, keyRange, tenantId, verifyVersion, cont]() {
			    debugOp("Verify", keyRange, tenantId, fmt::format("Complete @ {0}", *verifyVersion));
			    bool previousSuccess = seenReadSuccess(tenantId);
			    if (*verifyVersion == -1) {
				    ASSERT(!previousSuccess);
			    } else if (!previousSuccess) {
				    info(fmt::format("Verify {0}: first success\n", debugTenantStr(tenantId)));
				    setReadSuccess(tenantId);
			    }
			    schedule(cont);
		    },
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	void randomOperation(TTaskFct cont) override {
		std::optional<int> tenantId = randomTenant();

		OpType txType = (stores[tenantId].size() == 0) ? OP_INSERT : (OpType)Random::get().randomInt(0, OP_LAST);
		while (std::count(excludedOpTypes.begin(), excludedOpTypes.end(), txType)) {
			txType = (OpType)Random::get().randomInt(0, OP_LAST);
		}

		switch (txType) {
		case OP_INSERT:
			randomInsertOp(cont, tenantId);
			break;
		case OP_CLEAR:
			randomClearOp(cont, tenantId);
			break;
		case OP_CLEAR_RANGE:
			randomClearRangeOp(cont, tenantId);
			break;
		case OP_READ:
			randomReadOp(cont, tenantId);
			break;
		case OP_GET_GRANULES:
			randomGetGranulesOp(cont, tenantId);
			break;
		case OP_SUMMARIZE:
			randomSummarizeOp(cont, tenantId);
			break;
		case OP_GET_BLOB_RANGES:
			randomGetBlobRangesOp(cont, tenantId);
			break;
		case OP_VERIFY:
			randomVerifyOp(cont, tenantId);
			break;
		}
	}
};

WorkloadFactory<ApiBlobGranuleCorrectnessWorkload> ApiBlobGranuleCorrectnessWorkloadFactory(
    "ApiBlobGranuleCorrectness");

} // namespace FdbApiTester
