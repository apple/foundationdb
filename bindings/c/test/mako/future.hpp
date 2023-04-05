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
#include <type_traits>
#include "logger.hpp"
#include "macro.hpp"

extern thread_local mako::Logger logr;

namespace mako {

enum class FutureRC { OK, RETRY, ABORT };

struct LogContext {
	static constexpr const bool do_log = true;
	LogContext(std::string_view step) noexcept : step(step), transaction_timeout_expected(false) {}
	LogContext(std::string_view step, bool transaction_timeout_expected) noexcept
	  : step(step), transaction_timeout_expected(transaction_timeout_expected) {}
	std::string_view step;
	bool transaction_timeout_expected;
};

struct NoLog {
	static constexpr const bool do_log = false;
};

template <class FutureType, class LogInfo>
force_inline bool waitFuture(FutureType& f, LogInfo log_info) {
	assert(f);
	auto err = f.blockUntilReady();
	if (err) {
		assert(!err.retryable());
		if constexpr (LogInfo::do_log) {
			logr.error("'{}' found at blockUntilReady during step '{}'", err.what(), log_info.step);
		}
		return false;
	} else {
		return true;
	}
}

namespace detail {

template <class FutureType, class LogInfo>
force_inline FutureRC handleForOnError(fdb::Transaction& tx, FutureType& f, LogInfo log_info) {
	if (auto err = f.error()) {
		assert(!(err.retryable()));
		if constexpr (LogInfo::do_log) {
			logr.printWithLogLevel(err.is(1031 /*timeout*/) && log_info.transaction_timeout_expected ? VERBOSE_WARN
			                                                                                         : VERBOSE_NONE,
			                       "ERROR",
			                       "Unretryable error '{}' found at on_error(), step: {}",
			                       err.what(),
			                       log_info.step);
		}
		tx.reset();
		return FutureRC::ABORT;
	} else {
		return FutureRC::RETRY;
	}
}

} // namespace detail

template <class FutureType>
force_inline FutureRC
handleForOnError(fdb::Transaction& tx, FutureType& f, std::string_view step, bool transaction_timeout_expected) {
	return detail::handleForOnError(tx, f, LogContext(step, transaction_timeout_expected));
}

template <class FutureType>
force_inline FutureRC handleForOnError(fdb::Transaction& tx, FutureType& f, std::string_view step) {
	return detail::handleForOnError(tx, f, LogContext(step));
}

template <class FutureType>
force_inline FutureRC handleForOnError(fdb::Transaction& tx, FutureType& f) {
	return detail::handleForOnError(tx, f, NoLog{});
}

namespace detail {

template <class FutureType, class LogInfo>
force_inline FutureRC waitAndHandleForOnError(fdb::Transaction& tx, FutureType& f, LogInfo log_info) {
	assert(f);
	if (!waitFuture(f, log_info)) {
		return FutureRC::ABORT;
	}
	return detail::handleForOnError(tx, f, log_info);
}

// wait on any non-immediate tx-related step to complete. Follow up with on_error().
template <class FutureType, class LogInfo>
force_inline FutureRC waitAndHandleError(fdb::Transaction& tx, FutureType& f, LogInfo log_info) {
	assert(f);
	if (!waitFuture(f, log_info)) {
		return FutureRC::ABORT;
	}
	auto err = f.error();
	if (!err) {
		return FutureRC::OK;
	}
	if constexpr (LogInfo::do_log) {
		logr.printWithLogLevel(((err.is(1031 /*timeout*/) && log_info.transaction_timeout_expected) || err.retryable())
		                           ? VERBOSE_WARN
		                           : VERBOSE_NONE,
		                       "ERROR",
		                       "step {} returned '{}'",
		                       log_info.step,
		                       err.what());
	}

	// implicit backoff
	auto follow_up = tx.onError(err);
	return waitAndHandleForOnError(tx, follow_up, log_info);
}

} // namespace detail

template <class FutureType>
force_inline FutureRC
waitAndHandleForOnError(fdb::Transaction& tx, FutureType& f, std::string_view step, bool transaction_timeout_expected) {
	return detail::waitAndHandleForOnError(tx, f, LogContext(step, transaction_timeout_expected));
}

template <class FutureType>
force_inline FutureRC
waitAndHandleError(fdb::Transaction& tx, FutureType& f, std::string_view step, bool transaction_timeout_expected) {
	return detail::waitAndHandleError(tx, f, LogContext(step, transaction_timeout_expected));
}

template <class FutureType>
force_inline FutureRC waitAndHandleError(fdb::Transaction& tx, FutureType& f, std::string_view step) {
	return detail::waitAndHandleError(tx, f, LogContext(step));
}

template <class FutureType>
force_inline FutureRC waitAndHandleError(fdb::Transaction& tx, FutureType& f) {
	return detail::waitAndHandleError(tx, f, NoLog{});
}

} // namespace mako

#endif /*MAKO_FUTURE_HPP*/
