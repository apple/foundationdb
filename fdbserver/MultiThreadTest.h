/*
 * MultiThreadTest.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_MULTITHREADTEST_H
#define FDBSERVER_MULTITHREADTEST_H
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <thread>
#include <typeinfo>
#include <utility>
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "flow/FileIdentifier.h"
#include "flow/multithread.h"

// copied from tutorial.actor.cpp
struct GetRequest {
	constexpr static FileIdentifier file_identifier = 6983506;
	std::string key;
	ReplyPromise<std::string> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, reply);
	}
};

struct SetRequest {
	constexpr static FileIdentifier file_identifier = 7554186;
	std::string key;
	std::string value;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, value, reply);
	}
};

struct ClearRequest {
	constexpr static FileIdentifier file_identifier = 8500026;
	std::string from;
	std::string to;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, from, to, reply);
	}
};

enum class request_type { GET, SET, CLEAR };

class workerThread {
public:
	workerThread() {}
	workerThread(
	    const ThreadFutureStream<std::pair<request_type, std::shared_ptr<void>>>& req,
	    const ThreadPromiseStream<std::tuple<request_type, std::shared_ptr<void>, std::shared_ptr<void>>>& reply)
	  : req(req), reply(reply) {}

	void operator()() {
		fprintf(stderr, "Worker thread started!\n");
		while (true) {
			std::pair<request_type, std::shared_ptr<void>> pair;
			while (req >> pair) {
				// std::ostringstream ss;
				// ss << std::this_thread::get_id();
				// std::string idstr = ss.str();
				// fprintf(stderr, "Worker thread %s get the request\n", idstr.c_str());
				std::shared_ptr<void> replyPtr;
				auto& [type, reqPtr] = pair;
				switch (type) {
				case request_type::GET: {
					auto p = static_cast<GetRequest*>(reqPtr.get());
					auto iter = store.find(p->key);
					std::string val;
					if (iter != store.end()) {
						val = iter->second;
					}
					replyPtr = std::make_shared<std::string>(std::move(val));
					break;
				}
				case request_type::SET: {
					auto p = static_cast<SetRequest*>(reqPtr.get());
					store[p->key] = p->value;
					replyPtr = std::make_shared<Void>();
					break;
				}
				case request_type::CLEAR: {
					auto p = static_cast<ClearRequest*>(reqPtr.get());
					auto from = store.lower_bound(p->from);
					auto to = store.lower_bound(p->to);
					while (from != store.end() && from != to) {
						auto next = from;
						++next;
						store.erase(from);
						from = next;
					}
					replyPtr = std::make_shared<Void>();
					break;
				}
				default:
					ASSERT(false);
				}
				while (!(reply << std::make_tuple(type, reqPtr, replyPtr))) {
					// false means failed, need to resend
				}
				// fprintf(stderr, "Worker thread %s send the reply back\n", idstr.c_str());
			}
		}
	}

private:
	ThreadFutureStream<std::pair<request_type, std::shared_ptr<void>>> req;
	ThreadPromiseStream<std::tuple<request_type, std::shared_ptr<void>, std::shared_ptr<void>>> reply;
	std::map<std::string, std::string> store;
};

struct MultiThreadedFDBInterface {
	RequestStream<struct SetRequest> set;
	RequestStream<struct GetRequest> get;
	RequestStream<struct ClearRequest> clear;
	MultiThreadedFDBInterface() { initialize(); }
	MultiThreadedFDBInterface(NetworkAddress remote);
	MultiThreadedFDBInterface(INetwork* local);

	void initialize() {
		req = ThreadPromiseStream<std::pair<request_type, std::shared_ptr<void>>>(10000);
		ThreadPromiseStream<std::tuple<request_type, std::shared_ptr<void>, std::shared_ptr<void>>> replyPro(10000);
		worker = workerThread(req.getFutureStream(), replyPro);
		reply = replyPro.getFutureStream();
	}

    // worker thread to handle all request and give back replies to the network thread
	workerThread worker;
	// request queue where network thread will push a pointer to the coming request with its type to the queue
    // worker thread then read from the queue and get the requests
    // worker thread do its work and get the reply ready
	ThreadPromiseStream<std::pair<request_type, std::shared_ptr<void>>> req;
    // reply queue where worker thread sends all replies with its request and the type
    // network thread will periodically read from the queue and send the reply back to clients
	ThreadFutureStream<std::tuple<request_type, std::shared_ptr<void>, std::shared_ptr<void>>> reply;
};

Future<Void> multiThreadedFDBTestServer();

Future<Void> multiThreadedFDBTestClient(std::string const& testServer);

#endif
