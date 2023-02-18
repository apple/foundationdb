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
#include <set>
#include "fdb_api.hpp"
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
	// FIXME: use other new blob granule apis!
	enum OpType {
		OP_INSERT,
		OP_CLEAR,
		OP_CLEAR_RANGE,
		OP_READ,
		OP_GET_GRANULES,
		OP_SUMMARIZE,
		OP_GET_BLOB_RANGES,
		OP_VERIFY,
		OP_READ_DESC,
		OP_LAST = OP_READ_DESC
	};
	std::vector<OpType> excludedOpTypes;

	void setup(TTaskFct cont) override { setupBlobGranules(cont); }

	std::set<fdb::ByteString> validatedFiles;

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
			    ASSERT(err.code() != error_code_blob_granule_transaction_too_old);
			    if (err.code() != error_code_success) {
				    ctx->onError(err);
			    } else {
				    auto resCopy = copyKeyValueArray(out);
				    auto& [resVector, out_more] = resCopy;
				    ASSERT(!out_more);
				    results.get()->assign(resVector.begin(), resVector.end());
				    debugOp("Read", keyRange, tenantId, "complete");
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
			    this->validateRanges(results, keyRange);
			    schedule(cont);
		    },
		    getTenant(tenantId));
	}

	void randomSummarizeOp(TTaskFct cont, std::optional<int> tenantId) {
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

			    this->validateRanges(ranges, keyRange);

			    schedule(cont);
		    },
		    getTenant(tenantId));
	}

	void validateRanges(std::shared_ptr<std::vector<fdb::KeyRange>> results, fdb::KeyRange keyRange) {
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
			if ((*results)[i].beginKey != (*results)[i - 1].endKey) {
				error(fmt::format("ValidateRanges: [{0} - {1}): Non-covered range [{2} - {3})",
				                  fdb::toCharsRef(keyRange.beginKey),
				                  fdb::toCharsRef(keyRange.endKey),
				                  fdb::toCharsRef((*results)[i - 1].endKey),
				                  fdb::toCharsRef((*results)[i].endKey)));
			}
			ASSERT((*results)[i].beginKey == (*results)[i - 1].endKey);
		}
	}

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
			    this->validateRanges(results, keyRange);
			    schedule(cont);
		    },
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

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
			    schedule(cont);
		    },
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	void validateSnapshotData(std::shared_ptr<ITransactionContext> ctx,
	                          fdb::native::FDBReadBlobGranuleContext& bgCtx,
	                          fdb::GranuleFilePointer snapshotFile,
	                          fdb::KeyRange keyRange,
	                          fdb::native::FDBBGTenantPrefix const* tenantPrefix,
	                          int64_t& prevFileVersion) {
		ASSERT(snapshotFile.fileVersion > prevFileVersion);
		prevFileVersion = snapshotFile.fileVersion;
		if (validatedFiles.contains(snapshotFile.filename)) {
			return;
		}
		validatedFiles.insert(snapshotFile.filename);

		int64_t snapshotLoadId = bgCtx.start_load_f((const char*)(snapshotFile.filename.data()),
		                                            snapshotFile.filename.size(),
		                                            snapshotFile.offset,
		                                            snapshotFile.length,
		                                            snapshotFile.fullFileLength,
		                                            bgCtx.userContext);
		fdb::BytesRef snapshotData(bgCtx.get_load_f(snapshotLoadId, bgCtx.userContext), snapshotFile.length);
		fdb::Result snapshotRes = ctx->tx().parseSnapshotFile(snapshotData, tenantPrefix, &snapshotFile.encryptionCtx);
		auto out = fdb::Result::KeyValueRefArray{};
		fdb::Error err = snapshotRes.getKeyValueArrayNothrow(out);
		ASSERT(err.code() == error_code_success);
		auto res = copyKeyValueArray(out);
		bgCtx.free_load_f(snapshotLoadId, bgCtx.userContext);
		ASSERT(res.second == false);

		for (int i = 0; i < res.first.size(); i++) {
			ASSERT(res.first[i].key >= keyRange.beginKey);
			ASSERT(res.first[i].key < keyRange.endKey);
			if (i > 0) {
				ASSERT(res.first[i - 1].key < res.first[i].key);
			}
			// TODO add snapshot rows to map
		}
	}

	void validateDeltaData(std::shared_ptr<ITransactionContext> ctx,
	                       fdb::native::FDBReadBlobGranuleContext& bgCtx,
	                       fdb::GranuleFilePointer deltaFile,
	                       fdb::KeyRange keyRange,
	                       fdb::native::FDBBGTenantPrefix const* tenantPrefix,
	                       int64_t& lastDFMaxVersion,
	                       int64_t& prevFileVersion) {
		ASSERT(deltaFile.fileVersion > prevFileVersion);
		prevFileVersion = deltaFile.fileVersion;
		if (validatedFiles.contains(deltaFile.filename)) {
			return;
		}

		validatedFiles.insert(deltaFile.filename);
		int64_t deltaLoadId = bgCtx.start_load_f((const char*)(deltaFile.filename.data()),
		                                         deltaFile.filename.size(),
		                                         deltaFile.offset,
		                                         deltaFile.length,
		                                         deltaFile.fullFileLength,
		                                         bgCtx.userContext);

		fdb::BytesRef deltaData(bgCtx.get_load_f(deltaLoadId, bgCtx.userContext), deltaFile.length);

		fdb::Result deltaRes = ctx->tx().parseDeltaFile(deltaData, tenantPrefix, &deltaFile.encryptionCtx);
		auto out = fdb::Result::GranuleMutationRefArray{};
		fdb::Error err = deltaRes.getGranuleMutationArrayNothrow(out);
		ASSERT(err.code() == error_code_success);
		auto res = copyGranuleMutationArray(out);
		bgCtx.free_load_f(deltaLoadId, bgCtx.userContext);

		int64_t thisDFMaxVersion = 0;
		for (int j = 0; j < res.size(); j++) {
			fdb::GranuleMutation& m = res[j];
			ASSERT(m.version > 0);
			ASSERT(m.version > lastDFMaxVersion);
			// mutations in delta files aren't necessarily in version order, so just validate ordering w.r.t
			// previous file(s)
			thisDFMaxVersion = std::max(thisDFMaxVersion, m.version);

			ASSERT(m.type == 0 || m.type == 1);
			ASSERT(keyRange.beginKey <= m.param1);
			ASSERT(m.param1 < keyRange.endKey);
			if (m.type == 1) {
				ASSERT(keyRange.beginKey <= m.param2);
				ASSERT(m.param2 <= keyRange.endKey);
			}
		}
		lastDFMaxVersion = std::max(lastDFMaxVersion, thisDFMaxVersion);

		// can be higher due to empty versions but must not be lower
		ASSERT(lastDFMaxVersion <= prevFileVersion);

		// TODO have delta mutations update map
	}

	void validateBGDescriptionData(std::shared_ptr<ITransactionContext> ctx,
	                               fdb::native::FDBReadBlobGranuleContext& bgCtx,
	                               fdb::GranuleDescription desc,
	                               fdb::KeyRange keyRange,
	                               std::optional<int> tenantId,
	                               int64_t readVersion) {
		ASSERT(desc.keyRange.beginKey < desc.keyRange.endKey);
		ASSERT(tenantId.has_value() == desc.tenantPrefix.present);
		// beginVersion of zero means snapshot present
		int64_t prevFileVersion = 0;

		// validate snapshot file
		ASSERT(desc.snapshotFile.has_value());
		if (BG_API_DEBUG_VERBOSE) {
			info(fmt::format("Loading snapshot file {0}\n", fdb::toCharsRef(desc.snapshotFile->filename)));
		}
		validateSnapshotData(ctx, bgCtx, *desc.snapshotFile, desc.keyRange, &desc.tenantPrefix, prevFileVersion);

		// validate delta files
		int64_t lastDFMaxVersion = 0;
		for (int i = 0; i < desc.deltaFiles.size(); i++) {
			validateDeltaData(
			    ctx, bgCtx, desc.deltaFiles[i], desc.keyRange, &desc.tenantPrefix, lastDFMaxVersion, prevFileVersion);
		}

		// validate memory mutations
		if (desc.memoryMutations.size()) {
			ASSERT(desc.memoryMutations.front().version > lastDFMaxVersion);
			ASSERT(desc.memoryMutations.front().version > prevFileVersion);
		}
		int64_t lastVersion = prevFileVersion;
		for (int i = 0; i < desc.memoryMutations.size(); i++) {
			fdb::GranuleMutation& m = desc.memoryMutations[i];
			ASSERT(m.type == 0 || m.type == 1);
			ASSERT(m.version > 0);
			ASSERT(m.version >= lastVersion);
			ASSERT(m.version <= readVersion);
			lastVersion = m.version;

			ASSERT(m.type == 0 || m.type == 1);
			ASSERT(desc.keyRange.beginKey <= m.param1);
			ASSERT(m.param1 < desc.keyRange.endKey);
			if (m.type == 1) {
				ASSERT(desc.keyRange.beginKey <= m.param2);
				ASSERT(m.param2 <= desc.keyRange.endKey);
			}

			// TODO have delta mutations update map
		}

		// TODO: validate map against data store
	}

	void validateBlobGranuleDescriptions(std::shared_ptr<ITransactionContext> ctx,
	                                     std::vector<fdb::GranuleDescription> results,
	                                     fdb::KeyRange keyRange,
	                                     std::optional<int> tenantId,
	                                     int64_t readVersion) {
		ASSERT(!results.empty());
		if (tenantId) {
			// all should have the same tenant prefix
			for (int i = 0; i < results.size(); i++) {
				ASSERT(results[i].tenantPrefix.present);
			}
			fdb::ByteString tenantPrefix =
			    fdb::ByteString(results[0].tenantPrefix.prefix.key, results[0].tenantPrefix.prefix.key_length);
			for (int i = 1; i < results.size(); i++) {
				ASSERT(fdb::ByteString(results[0].tenantPrefix.prefix.key, results[0].tenantPrefix.prefix.key_length) ==
				       tenantPrefix);
			}
		}
		ASSERT(results.front().keyRange.beginKey <= keyRange.beginKey);
		ASSERT(keyRange.endKey <= results.back().keyRange.endKey);
		for (int i = 0; i < results.size() - 1; i++) {
			ASSERT(results[i].keyRange.endKey == results[i + 1].keyRange.beginKey);
		}

		TesterGranuleContext testerContext(ctx->getBGBasePath());
		fdb::native::FDBReadBlobGranuleContext bgCtx = createGranuleContext(&testerContext);
		for (int i = 0; i < results.size(); i++) {
			validateBGDescriptionData(ctx, bgCtx, results[i], keyRange, tenantId, readVersion);
		}
	}

	void randomReadDescription(TTaskFct cont, std::optional<int> tenantId) {
		fdb::KeyRange keyRange = randomNonEmptyKeyRange();
		auto results = std::make_shared<std::vector<fdb::GranuleDescription>>();
		auto readVersionOut = std::make_shared<int64_t>();

		debugOp("ReadDesc", keyRange, tenantId, "starting");

		execTransaction(
		    [this, keyRange, tenantId, results, readVersionOut](auto ctx) {
			    ctx->tx().setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);

			    int64_t* rvo = (int64_t*)readVersionOut.get();
			    fdb::Future f =
			        ctx->tx().readBlobGranulesDescription(keyRange.beginKey, keyRange.endKey, 0, -2, rvo).eraseType();
			    ctx->continueAfter(
			        f,
			        [this, ctx, keyRange, tenantId, results, readVersionOut, f]() {
				        *results = copyGranuleDescriptionArray(f.get<fdb::future_var::GranuleDescriptionRefArray>());
				        this->validateBlobGranuleDescriptions(ctx, *results, keyRange, tenantId, *readVersionOut);
				        ctx->done();
			        },
			        true);
		    },
		    [this, keyRange, tenantId, results, readVersionOut, cont]() {
			    debugOp("ReadDesc",
			            keyRange,
			            tenantId,
			            fmt::format("complete @ {0} with {1} granules", *readVersionOut, results->size()));
			    schedule(cont);
		    },
		    getTenant(tenantId));
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
		case OP_READ_DESC:
			randomReadDescription(cont, tenantId);
			break;
		}
	}
};

WorkloadFactory<ApiBlobGranuleCorrectnessWorkload> ApiBlobGranuleCorrectnessWorkloadFactory(
    "ApiBlobGranuleCorrectness");

} // namespace FdbApiTester
