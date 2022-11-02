/*
 * future.hpp
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

#ifndef MAKO_FUTURE_HPP
#define MAKO_FUTURE_HPP

#include <fdb_api.hpp>
#include <cassert>
#include <string_view>
#include "logger.hpp"
#include "macro.hpp"
#include "stats.hpp"

extern thread_local mako::Logger logr;

namespace mako {

enum class FutureRC { OK, RETRY, ABORT };

force_inline void updateErrorStats(ThreadStatistics& stats, const fdb::Error& err, int op) {
	if (err) {
		if (err.is(1020 /*not_commited*/)) {
			stats.incrConflictCount();
		} else {
			stats.incrErrorCount(op);
		}
	}
}

template <class FutureType>
force_inline FutureRC handleForOnError(fdb::Transaction& tx,
                                       FutureType& f,
                                       std::string_view step,
                                       ThreadStatistics* stats = nullptr,
                                       int op = 0) {
	if (auto err = f.error()) {
		assert(!(err.retryable()));
		logr.error("Unretryable error '{}' found at on_error(), step: {}", err.what(), step);
		tx.reset();
		if (stats) {
			updateErrorStats(*stats, err, op);
		}
		return FutureRC::ABORT;
	} else {
		return FutureRC::RETRY;
	}
}

template <class FutureType>
force_inline FutureRC waitAndHandleForOnError(fdb::Transaction& tx,
                                              FutureType& f,
                                              std::string_view step,
                                              ThreadStatistics* stats = nullptr,
                                              int op = 0,
                                              bool on_error_future_updates = false) {
	assert(f);
	if (!waitFuture(f, step, stats, op)) {
		return FutureRC::ABORT;
	}

	return handleForOnError(tx, f, step, on_error_future_updates ? stats : nullptr, op);
}

// wait on any non-immediate tx-related step to complete. Follow up with on_error().
template <class FutureType>
force_inline FutureRC waitAndHandleError(fdb::Transaction& tx,
                                         FutureType& f,
                                         std::string_view step,
                                         ThreadStatistics* stats = nullptr,
                                         int op = 0) {
	assert(f);
	if (!waitFuture(f, step, stats, op)) {
		return FutureRC::ABORT;
	}
	auto err = f.error();
	if (!err)
		return FutureRC::OK;

	if (stats) {
		updateErrorStats(*stats, err, op);
	}
	if (err.retryable()) {
		logr.warn("step {} returned '{}'", step, err.what());
	} else {
		logr.error("step {} returned '{}'", step, err.what());
	}
	// implicit backoff
	auto follow_up = tx.onError(err);
	return waitAndHandleForOnError(tx, follow_up, step, stats, op, false);
}

template <class FutureType>
force_inline bool waitFuture(FutureType& f, std::string_view step, ThreadStatistics* stats = nullptr, int op = 0) {
	assert(f);
	auto err = f.blockUntilReady();
	if (err) {
		const auto retry = err.retryable();
		logr.error("{} error '{}' found while waiting for future during step: {}",
		           (retry ? "Retryable" : "Unretryable"),
		           err.what(),
		           step);
		if (stats) {
			updateErrorStats(*stats, err, op);
		}
		return false;
	} else {
		return true;
	}
}

} // namespace mako

#endif /*MAKO_FUTURE_HPP*/
