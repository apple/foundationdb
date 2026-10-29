/*
 * TLSHandshakeWorker.cpp
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

#include "flow/TLSHandshakeWorker.h"

#ifndef _WIN32

#include <algorithm>
#include <limits>

#include <sys/socket.h>

#include "flow/Error.h"
#include "flow/Trace.h"

namespace {

// Boost's synchronous handshake loses the unsent part of a write that would block, and reports it only as
// would_block. Flag flights large enough to come close to a new socket's send buffer, so that failure is visible.
void warnIfLargeFlight(const TLSHandshakeJob& job) {
	constexpr int largeFlightBytes = 8 * 1024; // Typical flights are 3-6 KB.
	int queued = platform::socketSendQueueBytes(job.fd);
	if (queued < largeFlightBytes) {
		return;
	}
	int sendBuffer = 0;
	socklen_t len = sizeof(sendBuffer);
	::getsockopt(job.fd, SOL_SOCKET, SO_SNDBUF, &sendBuffer, &len);
	TraceEvent(SevWarnAlways, "TLSHandshakeLargeFlight")
	    .suppressFor(1.0)
	    .detail("PeerAddr", job.peerAddr)
	    .detail("Type", job.type == TLSHandshakeSocket::client ? "Client" : "Server")
	    .detail("SendQueueBytes", queued)
	    .detail("SendBuffer", sendBuffer);
}

} // namespace

TLSHandshakeJob::TLSHandshakeJob(TLSHandshakeSocket& socket,
                                 TLSHandshakeSocket::handshake_type type,
                                 NetworkAddress peerAddr)
  : socket(socket), type(type), peerAddr(peerAddr), fd(socket.lowest_layer().native_handle()) {}

TLSHandshakeWorker::TLSHandshakeWorker(double handshakeTimeoutSeconds)
  : timeout(std::chrono::duration_cast<std::chrono::steady_clock::duration>(
        std::chrono::duration<double>(handshakeTimeoutSeconds))),
    thread(startThread(threadMain, this, 0, "fdb-tls-hshake")) {}

TLSHandshakeWorker::~TLSHandshakeWorker() {
	// Set the flag before waking, so the thread sees it when its wait returns. run() completes every remaining job
	// before it exits, and the poller is destroyed only after the join.
	stopping.store(true);
	poller.wake();
	waitThread(thread);
}

void TLSHandshakeWorker::submitJob(std::shared_ptr<TLSHandshakeJob> job) {
	job->worker = this;
	// Counted before the worker sees the job, so that back-to-back submissions spread across workers.
	activeJobs.fetch_add(1, std::memory_order_relaxed);
	// The mutex hands the job, and everything written to it, over to the worker thread; setting the deadline under it
	// keeps deadlines in list order. Only a submission to an empty list needs to wake the worker: it takes the whole
	// list at once, and checks it after every wait.
	bool wasEmpty;
	{
		std::lock_guard<std::mutex> lock(mutex);
		job->deadline = std::chrono::steady_clock::now() + timeout;
		wasEmpty = submitted.empty();
		submitted.push_back(std::move(job));
	}
	if (wasEmpty) {
		poller.wake();
	}
}

void TLSHandshakeWorker::cancelJob(TLSHandshakeJob& job) {
	job.cancelled.store(true);
	cancelRequested.store(true);
	poller.wake();
}

THREAD_FUNC_RETURN TLSHandshakeWorker::threadMain(void* worker) {
	static_cast<TLSHandshakeWorker*>(worker)->run();
	THREAD_RETURN;
}

void TLSHandshakeWorker::run() {
	// The jobs this worker is driving, in submission order, which is also deadline order: every job has the same
	// timeout. Exactly these have their sockets watched by `poller`, and the list keeps each alive until it is
	// completed.
	std::list<std::shared_ptr<TLSHandshakeJob>> jobs;
	// Stops watching the socket before completing: once `done` is fulfilled the owner may close it, and its descriptor
	// number could then be reused by another connection. Erasing may destroy the job, so it comes last.
	auto finish = [&](TLSHandshakeJob& job, const boost::system::error_code& ec) {
		poller.unwatch(job.fd);
		completeJob(job, ec);
		jobs.erase(job.position);
	};

	std::vector<std::shared_ptr<TLSHandshakeJob>> incoming;
	std::vector<void*> ready;
	while (!stopping.load()) {
		// Take newly submitted jobs; the swap holds the lock only for an instant. The first advance sends a client's
		// ClientHello, or reads whatever a server's peer has already sent; watching starts after it, and reports at
		// once anything that arrived in between.
		{
			std::lock_guard<std::mutex> lock(mutex);
			incoming.swap(submitted);
		}
		for (auto& job : incoming) {
			boost::system::error_code ec;
			if (advanceHandshake(*job, ec)) {
				completeJob(*job, ec);
			} else {
				int err = poller.watch(job->fd, job.get());
				if (err == 0) {
					jobs.push_back(std::move(job));
					jobs.back()->position = std::prev(jobs.end());
				} else {
					completeJob(*job, boost::system::error_code(err, boost::system::system_category()));
				}
			}
		}
		incoming.clear();

		// Wait for a peer's next message, a new job, a cancellation or `stopping`, but no longer than the nearest
		// deadline. Rounding up keeps the wait from ending just before a deadline and spinning.
		int timeoutMs = -1;
		if (!jobs.empty()) {
			auto remaining =
			    std::chrono::ceil<std::chrono::milliseconds>(jobs.front()->deadline - std::chrono::steady_clock::now())
			        .count();
			timeoutMs = static_cast<int>(std::clamp<int64_t>(remaining, 0, std::numeric_limits<int>::max()));
		}
		ready.clear();
		poller.wait(timeoutMs, ready);

		// Every tag is a job still in `jobs`: each socket is reported at most once per wait, and is watched until its
		// job leaves the list.
		for (void* tag : ready) {
			auto& job = *static_cast<TLSHandshakeJob*>(tag);
			boost::system::error_code ec;
			if (advanceHandshake(job, ec)) {
				finish(job, ec);
			}
		}

		if (cancelRequested.exchange(false)) {
			for (auto it = jobs.begin(); it != jobs.end();) {
				auto& job = **it++;
				if (job.cancelled.load()) {
					finish(job, boost::asio::error::operation_aborted);
				}
			}
		}

		auto now = std::chrono::steady_clock::now();
		while (!jobs.empty() && jobs.front()->deadline <= now) {
			finish(*jobs.front(), boost::asio::error::timed_out);
		}
	}

	// Stopping: fail every unfinished job, so that nothing waits on its `done` forever.
	while (!jobs.empty()) {
		finish(*jobs.front(), boost::asio::error::operation_aborted);
	}
	std::lock_guard<std::mutex> lock(mutex);
	for (auto& job : submitted) {
		completeJob(*job, boost::asio::error::operation_aborted);
	}
}

bool TLSHandshakeWorker::advanceHandshake(TLSHandshakeJob& job, boost::system::error_code& result) {
	if (job.cancelled.load()) {
		result = boost::asio::error::operation_aborted;
		return true;
	}
	// Runs OpenSSL as far as the data received so far allows. On the non-blocking socket it reads until the socket is
	// empty, as the poller's edge-triggered readiness requires, then returns would_block; OpenSSL keeps its progress in
	// the connection's SSL object for the next call. An exception (e.g. from the certificate verification callback)
	// must not escape the worker thread.
	try {
		// NOLINTNEXTLINE(bugprone-unused-return-value): the result is also stored in `result`.
		job.socket.handshake(job.type, result);
	} catch (...) {
		result = boost::asio::error::fault;
	}
	warnIfLargeFlight(job);
	// A would-block on write would drop the unsent part of a handshake message inside Boost; handshake flights are
	// far smaller than a new socket's send buffer, and a stalled handshake still ends at its deadline.
	return result != boost::asio::error::would_block && result != boost::asio::error::try_again;
}

void TLSHandshakeWorker::completeJob(TLSHandshakeJob& job, const boost::system::error_code& ec) {
	// Same trace events as Net2's other handshake paths, so failures look alike wherever the handshake ran. The caller
	// only gets connection_failed; `error` keeps the cause.
	if (ec) {
		boost::system::error_code endpointError;
		auto endpoint = job.socket.lowest_layer().remote_endpoint(endpointError);
		TraceEvent(SevWarn,
		           job.type == TLSHandshakeSocket::client ? "N2_ConnectHandshakeError"_audit
		                                                  : "N2_AcceptHandshakeError"_audit)
		    .detail("PeerAddr", job.peerAddr)
		    .detail("PeerAddress", job.peerAddr)
		    .detail("PeerEndPoint", endpointError ? std::string("0.0.0.0") : endpoint.address().to_string())
		    .detail("ErrorCode", ec.value())
		    .detail("ErrorMsg", ec.message())
		    .detail("BackgroundThread", true);
		job.error = ec;
	}
	// Decremented first, so the count already excludes the job once anyone sees it finish.
	activeJobs.fetch_sub(1, std::memory_order_relaxed);
	if (ec) {
		job.done.sendError(connection_failed());
	} else {
		job.done.send(Void());
	}
}

#endif // _WIN32
