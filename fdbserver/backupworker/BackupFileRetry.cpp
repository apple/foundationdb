/*
 * BackupFileRetry.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "BackupFileRetry.h"
#include "fdbserver/core/Knobs.h"
#include "flow/UnitTest.h"

namespace {
bool retryableBackupFileError(Error error) {
	switch (error.code()) {
	case error_code_io_error:
	case error_code_io_timeout:
	case error_code_platform_error:
	case error_code_timed_out:
	case error_code_connection_failed:
	case error_code_lookup_failed:
	case error_code_http_request_failed:
	case error_code_http_bad_response:
		return true;
	default:
		return false;
	}
}
} // namespace

BackupFileRetry::BackupFileRetry(UID workerId)
  : BackupFileRetry(workerId,
                    SERVER_KNOBS->BACKUP_WORKER_UPLOAD_RETRY_LIMIT,
                    SERVER_KNOBS->BACKUP_WORKER_UPLOAD_RETRY_DELAY,
                    SERVER_KNOBS->BACKUP_WORKER_UPLOAD_RETRY_MAX_DELAY) {}

BackupFileRetry::BackupFileRetry(UID workerId, int retryLimit, double initialDelay, double maxDelay)
  : workerId(workerId), retryLimit(std::max(0, retryLimit)), nextDelay(std::max(0.0, std::min(initialDelay, maxDelay))),
    maxDelay(std::max(0.0, maxDelay)), startedAt(now()) {}

Future<Void> BackupFileRetry::onError(Error error) {
	if (!retryableBackupFileError(error) || retries >= retryLimit) {
		return error;
	}
	++retries;
	const double waitSeconds = nextDelay * (0.5 + 0.5 * deterministicRandom()->random01());
	nextDelay = std::min(maxDelay, nextDelay * 2);
	TraceEvent(SevWarn, "BackupWorkerFileRetry", workerId)
	    .errorUnsuppressed(error)
	    .detail("Retry", retries)
	    .detail("RetryLimit", retryLimit)
	    .detail("Delay", waitSeconds)
	    .detail("Elapsed", now() - startedAt);
	return delay(waitSeconds);
}

TEST_CASE("/BackupWorker/FileRetry/Exhaustion") {
	BackupFileRetry retry(UID(), 3, 0, 0);
	const Error error = io_error().asInjectedFault();
	for (int i = 0; i < 3; ++i) {
		co_await retry.onError(error);
	}
	Future<Void> exhausted = retry.onError(error);
	ASSERT(exhausted.isError());
	ASSERT_EQ(exhausted.getError().code(), error_code_io_error);
	ASSERT(exhausted.getError().isInjectedFault());
	BackupFileRetry disabled(UID(), 0, 0, 0);
	ASSERT(disabled.onError(platform_error()).isError());
}

TEST_CASE("/BackupWorker/FileRetry/RetryableErrors") {
	for (Error error : { io_error(),
	                     io_timeout(),
	                     platform_error(),
	                     timed_out(),
	                     connection_failed(),
	                     lookup_failed(),
	                     http_request_failed(),
	                     http_bad_response() }) {
		BackupFileRetry retry(UID(), 1, 0, 0);
		co_await retry.onError(error);
		Future<Void> exhausted = retry.onError(error);
		ASSERT(exhausted.isError());
		ASSERT_EQ(exhausted.getError().code(), error.code());
	}
}

TEST_CASE("/BackupWorker/FileRetry/TerminalErrors") {
	for (Error error : { actor_cancelled(),
	                     worker_removed(),
	                     broken_promise(),
	                     checksum_failed(),
	                     http_auth_failed(),
	                     backup_invalid_url(),
	                     file_not_writable(),
	                     Error::fromCode(error_code_internal_error) }) {
		BackupFileRetry retry(UID(), 3, 0, 0);
		Future<Void> result = retry.onError(error);
		ASSERT(result.isError());
		ASSERT_EQ(result.getError().code(), error.code());
	}
	return Void();
}

TEST_CASE("/BackupWorker/FileRetry/CancelBackoff") {
	// Native timers need not resolve on cancellation; test cancellation of the awaiting upload actor.
	auto waitForBackoff = [](BackupFileRetry retry) -> Future<Void> { co_await retry.onError(io_error()); };
	Future<Void> backoff = waitForBackoff(BackupFileRetry(UID(), 3, 10, 10));
	ASSERT(!backoff.isReady());
	backoff.cancel();
	ASSERT(backoff.isError());
	ASSERT_EQ(backoff.getError().code(), error_code_actor_cancelled);
	return Void();
}

void forceLinkBackupFileRetryTests() {}
