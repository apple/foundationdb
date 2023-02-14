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
#include "TesterBlobGranuleUtil.h"
#include "TesterUtil.h"
#include <memory>
#include <fmt/format.h>

namespace FdbApiTester {

class BlobGranuleErrorsWorkload : public ApiWorkload {
public:
	BlobGranuleErrorsWorkload(const WorkloadConfig& config) : ApiWorkload(config) {}

private:
	enum OpType {
		OP_READ_NO_MATERIALIZE,
		OP_READ_FILE_LOAD_ERROR,
		OP_READ_TOO_OLD,
		OP_PURGE_UNALIGNED,
		OP_BLOBBIFY_UNALIGNED,
		OP_UNBLOBBIFY_UNALIGNED,
		OP_CANCEL_GET_GRANULES,
		OP_CANCEL_GET_RANGES,
		OP_CANCEL_VERIFY,
		OP_CANCEL_SUMMARIZE,
		OP_CANCEL_BLOBBIFY,
		OP_CANCEL_UNBLOBBIFY,
		OP_CANCEL_PURGE,
		OP_LAST = OP_CANCEL_PURGE
	};

	void setup(TTaskFct cont) override { setupBlobGranules(cont); }

	// could add summarize too old and verify too old as ops if desired but those are lower value

	// Allow reads at the start to get blob_granule_transaction_too_old if BG data isn't initialized yet
	// FIXME: should still guarantee a read succeeds eventually somehow
	bool seenReadSuccess = false;

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

			    TesterGranuleContext testerContext(ctx->getBGBasePath() + basePathAddition);
			    fdb::native::FDBReadBlobGranuleContext granuleContext = createGranuleContext(&testerContext);
			    granuleContext.debugNoMaterialize = !doMaterialize;

			    fdb::Result res =
			        ctx->tx().readBlobGranules(begin, end, 0 /* beginVersion */, readVersion, granuleContext);
			    auto out = fdb::Result::KeyValueRefArray{};
			    fdb::Error err = res.getKeyValueArrayNothrow(out);

			    if (err.code() == error_code_success) {
				    error(fmt::format("Operation succeeded in error test!"));
			    }
			    ASSERT(err.code() != error_code_success);
			    if (err.code() != expectedError) {
				    info(fmt::format("incorrect error. Expected {}, Got {}", expectedError, err.code()));
				    if (err.code() == error_code_blob_granule_transaction_too_old) {
					    ASSERT(!seenReadSuccess);
					    ctx->done();
				    } else {
					    ctx->onError(err);
				    }
			    } else {
				    if (err.code() != error_code_blob_granule_transaction_too_old) {
					    seenReadSuccess = true;
				    }
				    ctx->done();
			    }
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomOpReadNoMaterialize(TTaskFct cont) {
		// ensure setting noMaterialize flag produces blob_granule_not_materialized
		doErrorOp(cont, "", false, -2 /*latest read version */, error_code_blob_granule_not_materialized);
	}

	void randomOpReadFileLoadError(TTaskFct cont) {
		// point to a file path that doesn't exist by adding an extra suffix
		doErrorOp(cont, "extrapath/", true, -2 /*latest read version */, error_code_blob_granule_file_load_error);
	}

	void randomOpReadTooOld(TTaskFct cont) {
		// read at a version (1) that should predate granule data
		doErrorOp(cont, "", true, 1, error_code_blob_granule_transaction_too_old);
	}

	void randomPurgeUnalignedOp(TTaskFct cont) {
		// blobbify/unblobbify need to be aligned to blob range boundaries, so this should always fail
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execOperation(
		    [this, begin, end](auto ctx) {
			    fdb::Future f = ctx->db().purgeBlobGranules(begin, end, -2, false).eraseType();
			    ctx->continueAfter(
			        f,
			        [this, ctx, f]() {
				        info(fmt::format("unaligned purge got {}", f.error().code()));
				        ASSERT(f.error().code() == error_code_unsupported_operation);
				        ctx->done();
			        },
			        true);
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomBlobbifyUnalignedOp(bool blobbify, TTaskFct cont) {
		// blobbify/unblobbify need to be aligned to blob range boundaries, so this should always return false
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		auto success = std::make_shared<bool>(false);
		execOperation(
		    [begin, end, blobbify, success](auto ctx) {
			    fdb::Future f = blobbify ? ctx->db().blobbifyRange(begin, end).eraseType()
			                             : ctx->db().unblobbifyRange(begin, end).eraseType();
			    ctx->continueAfter(
			        f,
			        [ctx, f, success]() {
				        *success = f.get<fdb::future_var::Bool>();
				        ctx->done();
			        },
			        true);
		    },
		    [this, cont, success]() {
			    ASSERT(!(*success));
			    schedule(cont);
		    });
	}

	void randomCancelGetGranulesOp(TTaskFct cont) {
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

	void randomCancelGetRangesOp(TTaskFct cont) {
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execOperation(
		    [begin, end](auto ctx) {
			    fdb::Future f = ctx->db().listBlobbifiedRanges(begin, end, 1000).eraseType();
			    ctx->done();
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomCancelVerifyOp(TTaskFct cont) {
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execOperation(
		    [begin, end](auto ctx) {
			    fdb::Future f = ctx->db().verifyBlobRange(begin, end, -2 /* latest version*/).eraseType();
			    ctx->done();
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomCancelSummarizeOp(TTaskFct cont) {
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execTransaction(
		    [begin, end](auto ctx) {
			    fdb::Future f = ctx->tx().summarizeBlobGranules(begin, end, -2, 1000).eraseType();
			    ctx->done();
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomCancelBlobbifyOp(TTaskFct cont) {
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execOperation(
		    [begin, end](auto ctx) {
			    fdb::Future f = ctx->db().blobbifyRange(begin, end).eraseType();
			    ctx->done();
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomCancelUnblobbifyOp(TTaskFct cont) {
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execOperation(
		    [begin, end](auto ctx) {
			    fdb::Future f = ctx->db().unblobbifyRange(begin, end).eraseType();
			    ctx->done();
		    },
		    [this, cont]() { schedule(cont); });
	}

	void randomCancelPurgeOp(TTaskFct cont) {
		fdb::Key begin = randomKeyName();
		fdb::Key end = randomKeyName();
		if (begin > end) {
			std::swap(begin, end);
		}
		execOperation(
		    [begin, end](auto ctx) {
			    fdb::Future f = ctx->db().purgeBlobGranules(begin, end, -2, false).eraseType();
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
		case OP_PURGE_UNALIGNED:
			// gets the correct error but it doesn't propagate properly in the test
			// randomPurgeUnalignedOp(cont);
			break;
		case OP_BLOBBIFY_UNALIGNED:
			randomBlobbifyUnalignedOp(true, cont);
			break;
		case OP_UNBLOBBIFY_UNALIGNED:
			randomBlobbifyUnalignedOp(false, cont);
			break;
		case OP_CANCEL_GET_GRANULES:
			randomCancelGetGranulesOp(cont);
			break;
		case OP_CANCEL_GET_RANGES:
			randomCancelGetRangesOp(cont);
			break;
		case OP_CANCEL_VERIFY:
			randomCancelVerifyOp(cont);
			break;
		case OP_CANCEL_SUMMARIZE:
			randomCancelSummarizeOp(cont);
			break;
		case OP_CANCEL_BLOBBIFY:
			randomCancelBlobbifyOp(cont);
			break;
		case OP_CANCEL_UNBLOBBIFY:
			randomCancelUnblobbifyOp(cont);
			break;
		case OP_CANCEL_PURGE:
			randomCancelPurgeOp(cont);
			break;
		}
	}
};

WorkloadFactory<BlobGranuleErrorsWorkload> BlobGranuleErrorsWorkloadFactory("BlobGranuleErrors");

} // namespace FdbApiTester
