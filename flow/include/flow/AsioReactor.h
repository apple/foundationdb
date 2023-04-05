/*
 * AsioReactor.h
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

#ifndef FLOW_ASIOREACTOR_H
#define FLOW_ASIOREACTOR_H
#pragma once

#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>

#include "flow/flow.h"

namespace N2 { // No indent, it's the whole file

class Net2;
class Peer;
class Connection;

class ASIOReactor {
public:
	explicit ASIOReactor(Net2*);

	void sleep(double timeout);
	void react();

	void wake();

	boost::asio::io_service ios;
	boost::asio::io_service::work
	    do_not_stop; // Reactor needs to keep running when there is nothing to do until stopped explicitly

private:
	Net2* network;
	boost::asio::deadline_timer firstTimer;

	static void nullWaitHandler(const boost::system::error_code&) {}
	static void nullCompletionHandler() {}

#ifdef __linux__
	class EventFD : public IEventFD {
		int fd;
		boost::asio::posix::stream_descriptor sd;
		int64_t fdVal;

		static void handle_read(Promise<int64_t> p,
		                        int64_t* pVal,
		                        const boost::system::error_code& ec,
		                        std::size_t bytes_transferred) {
			if (ec)
				return; // Presumably, the EventFD was destroyed?
			ASSERT(bytes_transferred == sizeof(*pVal));
			p.send(*pVal);
		}

	public:
		EventFD(ASIOReactor* reactor) : sd(reactor->ios, open()) {}
		~EventFD() override {
			sd.close(); // Also closes the fd, I assume...
		}
		int getFD() override { return fd; }
		Future<int64_t> read() override {
			Promise<int64_t> p;
			sd.async_read_some(boost::asio::mutable_buffers_1(&fdVal, sizeof(fdVal)),
			                   boost::bind(&EventFD::handle_read,
			                               p,
			                               &fdVal,
			                               boost::asio::placeholders::error,
			                               boost::asio::placeholders::bytes_transferred));
			return p.getFuture();
		}

		int open() {
			fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
			if (fd < 0) {
				TraceEvent(SevError, "EventfdError").GetLastError();
				throw platform_error();
			}
			return fd;
		}
	};

public:
	static IEventFD* getEventFD() { return static_cast<IEventFD*>((void*)g_network->global(INetwork::enEventFD)); }
	static EventFD* newEventFD(ASIOReactor& reactor) { return new EventFD(&reactor); }
#endif
};

} // namespace N2

#endif
