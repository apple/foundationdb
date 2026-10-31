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

#include <fcntl.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/sockios.h>
#include <pthread.h>
#endif

#include "flow/Error.h"
#include "flow/Platform.h"
#include "flow/Trace.h"

namespace {

// Boost's synchronous handshake loses the unsent part of a write that would block, and reports it only as
// would_block. Flag flights large enough to come close to a new socket's send buffer, so that failure is visible.
void warnIfLargeFlight(const TLSHandshakeJob& job) {
#ifdef __linux__
	constexpr int largeFlightBytes = 8 * 1024; // Typical flights are 3-6 KB.
	int unacked = 0;
	if (::ioctl(job.fd, SIOCOUTQ, &unacked) != 0 || unacked < largeFlightBytes) {
		return;
	}
	int sendBuffer = 0;
	socklen_t len = sizeof(sendBuffer);
	::getsockopt(job.fd, SOL_SOCKET, SO_SNDBUF, &sendBuffer, &len);
	TraceEvent(SevWarnAlways, "TLSHandshakeLargeFlight")
	    .suppressFor(1.0)
	    .detail("PeerAddr", job.peerAddr)
	    .detail("Type", job.type == TLSHandshakeSocket::client ? "Client" : "Server")
	    .detail("UnackedBytes", unacked)
	    .detail("SendBuffer", sendBuffer);
#endif
}

} // namespace

TLSHandshakeJob::TLSHandshakeJob(TLSHandshakeSocket& socket,
                                 TLSHandshakeSocket::handshake_type type,
                                 NetworkAddress peerAddr,
                                 double timeoutSeconds)
  : socket(socket), type(type), peerAddr(peerAddr), fd(socket.lowest_layer().native_handle()),
    deadline(std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                                                    std::chrono::duration<double>(timeoutSeconds))) {}

TLSHandshakeWorker::TLSHandshakeWorker() {
	if (::pipe(wakePipe) != 0) {
		throw platform_error();
	}
	// Non-blocking at both ends: wakeWorker() must never block its caller on a full pipe, and run() drains the read
	// end until it is empty.
	for (int fd : wakePipe) {
		::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
		platform::setCloseOnExec(fd);
	}
	thread = std::thread([this] { run(); });
}

TLSHandshakeWorker::~TLSHandshakeWorker() {
	// Set the flag before waking, so the thread sees it when poll() returns. run() completes every remaining job before
	// it exits, and the pipe is closed only after join(), when nothing can use it any more.
	stopping.store(true);
	wakeWorker();
	thread.join();
	for (int fd : wakePipe) {
		::close(fd);
	}
}

void TLSHandshakeWorker::submitJob(std::shared_ptr<TLSHandshakeJob> job) {
	job->worker = this;
	// Counted before the worker sees the job, so that back-to-back submissions spread across workers.
	activeJobs.fetch_add(1, std::memory_order_relaxed);
	// The mutex hands the job, and everything written to it, over to the worker thread. Waking after unlocking keeps
	// the woken thread from blocking on the lock.
	{
		std::lock_guard<std::mutex> lock(mutex);
		pending.push_back(std::move(job));
	}
	wakeWorker();
}

void TLSHandshakeWorker::cancelJob(TLSHandshakeJob& job) {
	job.cancelled.store(true);
	wakeWorker();
}

void TLSHandshakeWorker::wakeWorker() {
	char byte = 0;
	// A full pipe already guarantees a wakeup.
	[[maybe_unused]] auto rc = ::write(wakePipe[1], &byte, 1);
}

void TLSHandshakeWorker::run() {
#ifdef __linux__
	pthread_setname_np(pthread_self(), "fdb-tls-hshake");
#endif
	// Jobs this worker is driving. Only this thread touches them, so they need no lock.
	std::vector<std::shared_ptr<TLSHandshakeJob>> jobs;
	std::vector<pollfd> fds;
	while (!stopping.load()) {
		// Take newly submitted jobs; the swap holds the lock only for an instant. The first advance sends a client's
		// ClientHello, or reads whatever a server's peer has already sent.
		std::vector<std::shared_ptr<TLSHandshakeJob>> incoming;
		{
			std::lock_guard<std::mutex> lock(mutex);
			incoming.swap(pending);
		}
		for (auto& job : incoming) {
			if (!advanceHandshake(*job)) {
				jobs.push_back(std::move(job));
			}
		}

		// Wait for the peer's next message on any job's socket (hang-ups and errors are reported too), but no longer
		// than the nearest deadline. Rounding up keeps poll() from returning just before a deadline and spinning. With
		// no jobs, it waits until woken.
		fds.clear();
		auto now = std::chrono::steady_clock::now();
		int timeoutMs = -1;
		for (auto& job : jobs) {
			fds.push_back(pollfd{ job->fd, POLLIN, 0 });
			auto remaining = std::chrono::ceil<std::chrono::milliseconds>(job->deadline - now).count();
			int ms = static_cast<int>(std::clamp<int64_t>(remaining, 0, std::numeric_limits<int>::max()));
			timeoutMs = timeoutMs < 0 ? ms : std::min(timeoutMs, ms);
		}
		// Last, so that fds[i] is jobs[i]'s socket.
		fds.push_back(pollfd{ wakePipe[0], POLLIN, 0 });
		::poll(fds.data(), fds.size(), timeoutMs);

		// Drain every wake byte, or the pipe would keep poll() returning at once. No wakeup is lost: writers change
		// state before writing, and that state is checked after this point.
		if (fds.back().revents) {
			char buf[64];
			while (::read(wakePipe[0], buf, sizeof(buf)) > 0) {
			}
		}

		// Check each job.
		now = std::chrono::steady_clock::now();
		size_t kept = 0;
		for (size_t i = 0; i < jobs.size(); ++i) {
			auto& job = jobs[i];
			bool finished;
			if (job->cancelled.load()) {
				completeJob(*job, boost::asio::error::operation_aborted);
				finished = true;
			} else if (fds[i].revents) {
				finished = advanceHandshake(*job);
			} else if (now >= job->deadline) {
				completeJob(*job, boost::asio::error::timed_out);
				finished = true;
			} else {
				finished = false;
			}
			if (!finished) {
				jobs[kept++] = std::move(job);
			}
		}
		jobs.resize(kept);
	}

	// Stopping: fail every unfinished job, so that nothing waits on its `done` forever.
	std::lock_guard<std::mutex> lock(mutex);
	for (auto& job : jobs) {
		completeJob(*job, boost::asio::error::operation_aborted);
	}
	for (auto& job : pending) {
		completeJob(*job, boost::asio::error::operation_aborted);
	}
}

bool TLSHandshakeWorker::advanceHandshake(TLSHandshakeJob& job) {
	if (job.cancelled.load()) {
		completeJob(job, boost::asio::error::operation_aborted);
		return true;
	}
	// Runs OpenSSL as far as the data received so far allows; on the non-blocking socket it returns would_block when it
	// needs more from the peer, and OpenSSL keeps its progress in the connection's SSL object for the next call. An
	// exception (e.g. from the certificate verification callback) must not escape the worker thread.
	boost::system::error_code ec;
	try {
		// NOLINTNEXTLINE(bugprone-unused-return-value): the result is also stored in ec.
		job.socket.handshake(job.type, ec);
	} catch (...) {
		ec = boost::asio::error::fault;
	}
	warnIfLargeFlight(job);
	// A would-block on write would drop the unsent part of a handshake message inside Boost; handshake flights are
	// far smaller than a new socket's send buffer, and a stalled handshake still ends at its deadline.
	if (ec == boost::asio::error::would_block || ec == boost::asio::error::try_again) {
		return false;
	}
	completeJob(job, ec);
	return true;
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
		job.done.sendError(connection_failed());
	} else {
		job.done.send(Void());
	}
	activeJobs.fetch_sub(1, std::memory_order_relaxed);
}

#endif // _WIN32
