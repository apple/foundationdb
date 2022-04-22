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

namespace FdbApiTester {

class BlobGranuleErrorsWorkload : public ApiWorkload {
public:
	BlobGranuleErrorsWorkload(const WorkloadConfig& config) : ApiWorkload(config) {}

private:
	enum OpType {
		OP_READ_NO_MATERIALIZE,
		OP_READ_FILE_LOAD_ERROR,
		OP_READ_TOO_OLD,
		OP_GRANULE_READ_CALLBACK_BLOCKED,
		OP_CANCEL_RANGES,
		OP_LAST = OP_CANCEL_RANGES
	};

	void doErrorOp(TTaskFct cont,
	               std::string basePathAddition,
	               bool doMaterialize,
	               int64_t readVersion,
	               fdb_error_t expectedError) {
		std::string begin = randomKeyName();
		std::string end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}

		execTransaction(
		    [begin, end, basePathAddition, doMaterialize, readVersion, expectedError](auto ctx) {
			    ctx->tx()->setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
			    KeyValuesResult res = ctx->tx()->readBlobGranules(
			        begin, end, ctx->getBGBasePath() + basePathAddition, doMaterialize, readVersion);
			    bool more;
			    res.getKeyValues(&more);

			    ASSERT(res.getError() != error_code_success);
			    if (res.getError() != expectedError) {
				    ctx->onError(res.getError());
			    } else {
				    ctx->done();
			    }
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomOpReadNoMaterialize(TTaskFct cont) {
		// ensure setting noMaterialize flag produces blob_granule_not_materialized
		doErrorOp(cont, "", false, -2, error_code_blob_granule_not_materialized);
	}

	void randomOpReadFileLoadError(TTaskFct cont) {
		// point to a file path that doesn't exist by adding an extra suffix
		doErrorOp(cont, "extrapath/", true, -2, error_code_blob_granule_file_load_error);
	}

	void randomOpReadTooOld(TTaskFct cont) {
		// read at a version (1) that predates granule data
		doErrorOp(cont, "extrapath/", true, 1, error_code_blob_granule_transaction_too_old);
	}

	// Do a get, and in its callback do a readBlobGranules. Because readBlobGranules is synchronous and does a
	// blockOnMainThread, it should get the error_code_blocked_from_network_thread error to avoid deadlocks.
	void randomReadCallbackBlocked(TTaskFct cont) {
		std::string begin = randomKeyName();
		std::string end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execTransaction(
		    [begin, end](auto ctx) {
			    ctx->tx()->setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
			    // this key doesn't matter, just that readBlobGranules is in the callback
			    auto f = ctx->tx()->get(begin, false);
			    ctx->continueAfter(
			        f,
			        [ctx, f, begin, end]() {
				        KeyValuesResult res = ctx->tx()->readBlobGranules(begin, end, ctx->getBGBasePath());
				        bool more;
				        res.getKeyValues(&more);
				        ASSERT(res.getError() != error_code_success);
				        if (res.getError() != error_code_blocked_from_network_thread) {
					        ctx->onError(res.getError());
				        } else {
					        ctx->done();
				        }
			        },
			        true);
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomCancelGetRangesOp(TTaskFct cont) {
		std::string begin = randomKeyName();
		std::string end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execTransaction(
		    [begin, end](auto ctx) {
			    KeyRangesFuture f = ctx->tx()->getBlobGranuleRanges(begin, end);
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
		case OP_GRANULE_READ_CALLBACK_BLOCKED:
			// FIXME: this does not successfully trigger blocked_from_network_thread
			// randomReadCallbackBlocked(cont);
			break;
		case OP_CANCEL_RANGES:
			randomCancelGetRangesOp(cont);
			break;
		}
	}
};

WorkloadFactory<BlobGranuleErrorsWorkload> BlobGranuleErrorsWorkloadFactory("BlobGranuleErrors");

} // namespace FdbApiTester