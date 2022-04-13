#ifndef MAKO_FUTURE_HPP
#define MAKO_FUTURE_HPP

#include <fdb_api.hpp>
#include <cassert>
#include <string_view>
#include "logger.hpp"

extern thread_local mako::Logger logr;

namespace mako {

enum class FutureRC { OK, RETRY, CONFLICT, ABORT };

template <class FutureType>
FutureRC handleForOnError(fdb::Transaction tx, FutureType f, std::string_view step) {
	if (auto err = f.error()) {
		if (err.is(1020 /*not_committed*/)) {
			return FutureRC::CONFLICT;
		} else if (err.retryable()) {
			logr.warn("Retryable error '{}' found at on_error(), step: {}", err.what(), step);
			return FutureRC::RETRY;
		} else {
			logr.error("Unretryable error '{}' found at on_error(), step: {}", err.what(), step);
			tx.reset();
			return FutureRC::ABORT;
		}
	} else {
		return FutureRC::RETRY;
	}
}

template <class FutureType>
FutureRC waitAndHandleForOnError(fdb::Transaction tx, FutureType f, std::string_view step) {
	assert(f);
	if (auto err = f.blockUntilReady()) {
		logr.error("'{}' found while waiting for on_error() future, step: {}", err.what(), step);
		return FutureRC::ABORT;
	}
	return handleForOnError(tx, f, step);
}

// wait on any non-immediate tx-related step to complete. Follow up with on_error().
template <class FutureType>
FutureRC waitAndHandleError(fdb::Transaction tx, FutureType f, std::string_view step) {
	assert(f);
	auto err = fdb::Error{};
	if ((err = f.blockUntilReady())) {
		const auto retry = err.retryable();
		logr.error("{} error '{}' found during step: {}", (retry ? "Retryable" : "Unretryable"), err.what(), step);
		return retry ? FutureRC::RETRY : FutureRC::ABORT;
	}
	err = f.error();
	if (!err)
		return FutureRC::OK;
	if (err.retryable()) {
		logr.warn("step {} returned '{}'", step, err.what());
	} else {
		logr.error("step {} returned '{}'", step, err.what());
	}
	// implicit backoff
	auto follow_up = tx.onError(err);
	return waitAndHandleForOnError(tx, f, step);
}

} // namespace mako

#endif /*MAKO_FUTURE_HPP*/
