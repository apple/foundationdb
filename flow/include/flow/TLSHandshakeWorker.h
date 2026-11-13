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

// Declared on every platform so callers can hold pointers without platform checks; defined only where it is supported.
class TLSHandshakeWorker;

#ifndef _WIN32

#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>

#include "flow/IThreadPool.h"
#include "flow/Platform.h"
#include "flow/network.h"

using TLSHandshakeSocket = boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>;

// One TLS handshake owned by a TLSHandshakeWorker. The worker is the only thread that touches the socket until `done`
// is fulfilled, and never touches it (or `fd`) after; meanwhile the network thread may only cancel the job with
// TLSHandshakeWorker::cancelJob(). The socket must stay non-blocking.
struct TLSHandshakeJob {
	TLSHandshakeJob(TLSHandshakeSocket& socket, TLSHandshakeSocket::handshake_type type, NetworkAddress peerAddr);
	TLSHandshakeSocket& socket; // The connection's TLS stream.
	const TLSHandshakeSocket::handshake_type type; // Client or server side.
	const NetworkAddress peerAddr; // For trace events.
	const int fd; // The socket's descriptor, watched by the worker's poller.
	std::chrono::steady_clock::time_point deadline; // Set by submitJob(); the job fails with timed_out at this time.
	boost::system::error_code error; // Why the handshake failed.
	std::atomic<bool> cancelled{ false }; // Set by TLSHandshakeWorker::cancelJob().
	TLSHandshakeWorker* worker = nullptr; // The worker the job was submitted to.
	std::list<std::shared_ptr<TLSHandshakeJob>>::iterator position; // In the worker's job list; worker thread only.
	ThreadReturnPromise<Void> done; // Sent once the handshake finishes, fails, times out or is cancelled.
};

// Drives many TLS handshakes on one thread. Each handshake runs OpenSSL's state machine on a non-blocking socket and is
// resumed when the socket becomes readable (epoll or kqueue), so a slow or silent peer costs no thread time while it
// waits, and every handshake ends by its deadline.
class TLSHandshakeWorker {
public:
	// Every job submitted to this worker fails with timed_out `handshakeTimeoutSeconds` after it is submitted.
	explicit TLSHandshakeWorker(double handshakeTimeoutSeconds);
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
	const std::chrono::steady_clock::duration timeout; // Of every job; see the constructor.
	platform::ReadinessPoller poller; // Watches the sockets of the jobs in progress.
	// Only a load-balancing estimate, read to pick the least loaded worker: nothing else is published through it, so
	// relaxed ordering suffices. Jobs themselves reach the worker thread under `mutex`.
	std::atomic<int> activeJobs{ 0 };
	std::atomic<bool> cancelRequested{ false }; // Set by cancelJob() until the worker thread checks its jobs.
	std::atomic<bool> stopping{ false }; // Set by the destructor to end run().
	std::mutex mutex; // Guards `submitted`.
	std::vector<std::shared_ptr<TLSHandshakeJob>> submitted; // Jobs not yet taken by the worker thread.
	THREAD_HANDLE thread; // Runs run(). Declared last, so that everything above exists before it starts.

	THREAD_FUNC threadMain(void* worker);

	// The worker thread's loop: takes submitted jobs, waits for their sockets, and advances, cancels or times out each.
	void run();

	// Advances the handshake as far as the data received so far allows. Returns true once it is over, with its result.
	bool advanceHandshake(TLSHandshakeJob& job, boost::system::error_code& result);

	// Stores the result (logging it if `ec` is an error), decrements `activeJobs`, and fulfills `done`.
	void completeJob(TLSHandshakeJob& job, const boost::system::error_code& ec);
};

#endif // _WIN32

#endif // FLOW_TLSHANDSHAKEWORKER_H
