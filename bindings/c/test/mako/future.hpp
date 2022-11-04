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

extern thread_local mako::Logger logr;

namespace mako {

enum class FutureRC { OK, RETRY, ABORT };

template <class FutureType>
force_inline bool waitFuture(FutureType& f, std::string_view step) {
	assert(f);
	auto err = f.blockUntilReady();
	if (err) {
		assert(!err.retryable());
		logr.error("'{}' found at blockUntilReady during step '{}'", err.what(), step);
		return false;
	} else {
		return true;
	}
}

template <class FutureType>
force_inline FutureRC
handleForOnError(fdb::Transaction& tx, FutureType& f, std::string_view step, bool timeouts_expected = false) {
	if (auto err = f.error()) {
		assert(!(err.retryable()));
		logr.printWithLogLevel(err.is(1031 /*timeout*/) && timeouts_expected ? VERBOSE_WARN : VERBOSE_DEFAULT,
		                       "ERROR",
		                       "Unretryable error '{}' found at on_error(), step: {}",
		                       err.what(),
		                       step);
		tx.reset();
		return FutureRC::ABORT;
	} else {
		return FutureRC::RETRY;
	}
}

template <class FutureType>
force_inline FutureRC
waitAndHandleForOnError(fdb::Transaction& tx, FutureType& f, std::string_view step, bool timeouts_expected = false) {
	assert(f);
	if (!waitFuture(f, step)) {
		return FutureRC::ABORT;
	}
	return handleForOnError(tx, f, step, timeouts_expected);
}

// wait on any non-immediate tx-related step to complete. Follow up with on_error().
template <class FutureType>
force_inline FutureRC
waitAndHandleError(fdb::Transaction& tx, FutureType& f, std::string_view step, bool timeouts_expected = false) {
	assert(f);
	if (!waitFuture(f, step)) {
		return FutureRC::ABORT;
	}
	auto err = f.error();
	if (!err) {
		return FutureRC::OK;
	}
	logr.printWithLogLevel(((err.is(1031 /*timeout*/) && timeouts_expected) || err.retryable()) ? VERBOSE_WARN
	                                                                                            : VERBOSE_DEFAULT,
	                       "ERROR",
	                       "step {} returned '{}'",
	                       step,
	                       err.what());

	// implicit backoff
	auto follow_up = tx.onError(err);
	return waitAndHandleForOnError(tx, follow_up, step, timeouts_expected);
}
} // namespace mako

#endif /*MAKO_FUTURE_HPP*/
