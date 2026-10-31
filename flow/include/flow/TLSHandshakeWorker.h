/*
 * TLSHandshakeWorker.h
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

#ifndef FLOW_TLSHANDSHAKEWORKER_H
#define FLOW_TLSHANDSHAKEWORKER_H
#pragma once

// Declared on every platform so callers can hold pointers without platform checks; defined only where it is supported.
class TLSHandshakeWorker;

#ifndef _WIN32

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>

#include "flow/IThreadPool.h"
#include "flow/network.h"

using TLSHandshakeSocket = boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>;

// One TLS handshake owned by a TLSHandshakeWorker. The worker is the only thread that touches the socket until `done`
// is fulfilled, and never touches it (or `fd`) after; meanwhile the network thread may only cancel the job with
// TLSHandshakeWorker::cancelJob(). The socket must stay non-blocking.
struct TLSHandshakeJob {
	TLSHandshakeJob(TLSHandshakeSocket& socket,
	                TLSHandshakeSocket::handshake_type type,
	                NetworkAddress peerAddr,
	                double timeoutSeconds);
	TLSHandshakeSocket& socket; // The connection's TLS stream.
	const TLSHandshakeSocket::handshake_type type; // Client or server side.
	const NetworkAddress peerAddr; // For trace events.
	const int fd; // The socket's descriptor, watched with poll().
	const std::chrono::steady_clock::time_point deadline; // The job fails with timed_out at this time.
	boost::system::error_code error; // Why the handshake failed;
	std::atomic<bool> cancelled{ false }; // Whether job is cancelled.
	TLSHandshakeWorker* worker = nullptr; // The worker associated with the job.
	ThreadReturnPromise<Void> done; // Sent once the handshake finishes, fails, times out or is cancelled.
};

// Drives many TLS handshakes on one thread. Each handshake runs OpenSSL's state machine on a non-blocking socket and is
// resumed when poll() reports the socket readable, so a slow or silent peer costs no thread time while it waits, and
// every handshake ends by its deadline.
class TLSHandshakeWorker {
public:
	TLSHandshakeWorker();
	~TLSHandshakeWorker();

	TLSHandshakeWorker(const TLSHandshakeWorker&) = delete;
	TLSHandshakeWorker& operator=(const TLSHandshakeWorker&) = delete;

	// Number of jobs submitted and not yet finished.
	int activeJobCount() const { return activeJobs.load(std::memory_order_relaxed); }

	// Hands `job` to the worker thread, which drives its handshake until `done` is fulfilled. Safe to call from any
	// thread.
	void submitJob(std::shared_ptr<TLSHandshakeJob> job);

	// Asks the worker to cancel `job`, which must have been submitted to it. Safe to call from the network thread at
	// any time, including after the handshake finished.
	void cancelJob(TLSHandshakeJob& job);

private:
	// Other threads write to [1] to wake the worker thread out of poll(), which watches [0].
	int wakePipe[2] = { -1, -1 };
	std::atomic<int> activeJobs{ 0 }; // See activeJobCount().
	std::atomic<bool> stopping{ false }; // Set by the destructor to end run().
	std::mutex mutex; // Guards `pending`.
	std::vector<std::shared_ptr<TLSHandshakeJob>> pending; // Jobs submitted but not yet taken by the worker thread.
	std::thread thread; // Runs run().

	// Interrupts the worker thread's poll() so it notices new jobs, cancellations and `stopping`.
	void wakeWorker();

	// The worker thread's loop: takes pending jobs, polls their sockets, and advances, times out or cancels each one.
	void run();

	// Advances the handshake as far as the data received so far allows. Returns true once the job is completed.
	bool advanceHandshake(TLSHandshakeJob& job);

	// Stores the result (logging it if `ec` is an error), fulfills `done`, and decrements `activeJobs`.
	void completeJob(TLSHandshakeJob& job, const boost::system::error_code& ec);
};

#endif // _WIN32

#endif // FLOW_TLSHANDSHAKEWORKER_H
