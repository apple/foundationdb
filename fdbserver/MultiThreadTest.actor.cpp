/*
 * MultiThreadTest.actor.cpp
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

#include "fdbserver/MultiThreadTest.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/actorcompiler.h" // This must be the last #include.

UID WLTOKEN_MULTITHREAD_SET(-1, 5);
UID WLTOKEN_MULTITHREAD_GET(-1, 6);
UID WLTOKEN_MULTITHREAD_CLEAR(-1, 7);

template <typename Type>
bool operator<<(ThreadPromiseStream<Type>& stream, const Type& in) {
	return stream.queue->push(in);
}

template <typename Type>
bool operator>>(const Type& in, ThreadPromiseStream<Type>& stream) {
	return stream.queue->push(in);
}

template <typename Type>
bool operator>>(ThreadFutureStream<Type>& stream, Type& out) {
	return stream.queue->pop(out);
}

template <typename Type>
bool operator<<(Type& out, ThreadFutureStream<Type>& stream) {
	return stream.queue->pop(out);
}

MultiThreadedFDBInterface::MultiThreadedFDBInterface(NetworkAddress remote)
  : set(Endpoint({ remote }, WLTOKEN_MULTITHREAD_SET)), get((Endpoint({ remote }, WLTOKEN_MULTITHREAD_GET))),
    clear((Endpoint({ remote }, WLTOKEN_MULTITHREAD_CLEAR))) {
	initialize();
}

MultiThreadedFDBInterface::MultiThreadedFDBInterface(INetwork* local) {
	initialize();
	set.makeWellKnownEndpoint(WLTOKEN_MULTITHREAD_SET, TaskPriority::DefaultEndpoint);
	get.makeWellKnownEndpoint(WLTOKEN_MULTITHREAD_GET, TaskPriority::DefaultEndpoint);
	clear.makeWellKnownEndpoint(WLTOKEN_MULTITHREAD_CLEAR, TaskPriority::DefaultEndpoint);
}

ACTOR Future<Void> multiThreadedFDBTestServer() {
	state MultiThreadedFDBInterface interf(g_network);
	state std::thread worker(interf.worker);
	state Future<Void> logging = delay(1.0);
	state Future<Void> replyInterval = delay(0.01);
	state double lastTime = now();
	state int sent = 0;
	loop {
		choose {
			when(wait(logging)) {
				auto spd = sent / (now() - lastTime);
				fprintf(stderr, "responses per second: %f\n", spd);
				lastTime = now();
				sent = 0;
				logging = delay(1.0);
			}
			when(GetRequest req = waitNext(interf.get.getFuture())) {
				// fprintf(stderr, "Get request\n");
				std::shared_ptr<void> reqPtr = std::make_shared<GetRequest>(std::move(req));
				std::pair<request_type, std::shared_ptr<void>> pair = std::make_pair(request_type::GET, reqPtr);
				while (!(interf.req << pair)) {
					// resend until successful
				};
			}
			when(SetRequest req = waitNext(interf.set.getFuture())) {
				// fprintf(stderr, "Set request\n");
				std::shared_ptr<void> reqPtr = std::make_shared<SetRequest>(std::move(req));
				std::pair<request_type, std::shared_ptr<void>> pair = std::make_pair(request_type::SET, reqPtr);
				while (!(interf.req << pair)) {
					// resend until successful
				};
			}
			when(ClearRequest req = waitNext(interf.clear.getFuture())) {
				// fprintf(stderr, "Clear request\n");
				std::shared_ptr<void> reqPtr = std::make_shared<ClearRequest>(std::move(req));
				std::pair<request_type, std::shared_ptr<void>> pair = std::make_pair(request_type::CLEAR, reqPtr);
				while (!(interf.req << pair)) {
					// resend until successful
				};
			}
			when(wait(replyInterval)) {
				std::tuple<request_type, std::shared_ptr<void>, std::shared_ptr<void>> tup;
				while (interf.reply >> tup) {
					auto& [type, reqPtr, replyPtr] = tup;
					// fprintf(stderr, "Network thread send reply back\n");
					switch (type) {
					case request_type::GET:
						static_cast<GetRequest*>(reqPtr.get())->reply.send(*static_cast<std::string*>(replyPtr.get()));
						break;
					case request_type::SET:
						static_cast<SetRequest*>(reqPtr.get())->reply.send(*static_cast<Void*>(replyPtr.get()));
						break;
					case request_type::CLEAR:
						static_cast<ClearRequest*>(reqPtr.get())->reply.send(*static_cast<Void*>(replyPtr.get()));
						break;
					default:
						ASSERT(false);
					}
					++sent;
				}
				replyInterval = delay(0.01);
			}
		}
	}
}

ACTOR Future<Void> multithreadtestClient(MultiThreadedFDBInterface interf) {
	state int rangeSize = 100; // 2 << 12;
	state int op;
	loop {
		op = deterministicRandom()->randomInt(0, 3);
		if (op == 0) {
			state SetRequest setRequest;
			setRequest.key = std::to_string(deterministicRandom()->randomInt(0, rangeSize));
			setRequest.value = std::to_string(deterministicRandom()->coinflip());
			wait(retryBrokenPromise(interf.set, setRequest));
			// fprintf(stderr, "Set %s=%s\n", setRequest.key.c_str(), setRequest.value.c_str());
		} else if (op == 1) {
			try {
				state GetRequest getRequest;
				getRequest.key = std::to_string(deterministicRandom()->randomInt(0, rangeSize));
				std::string val = wait(retryBrokenPromise(interf.get, getRequest));
				// if (val.size())
				//     fprintf(stderr, "%s : %s\n", getRequest.key.c_str(), val.c_str());
				// else
				//     fprintf(stderr, "Key %s not found \n", getRequest.key.c_str());
			} catch (Error& e) {
				if (e.code() != error_code_io_error) {
					throw e;
				}
			}
		} else {
			state int from = deterministicRandom()->randomInt(0, rangeSize);
			ClearRequest clearRequest;
			clearRequest.from = std::to_string(from);
			clearRequest.to = std::to_string(from + 1);
			wait(retryBrokenPromise(interf.clear, clearRequest));
			// fprintf(stderr, "Clear key from %d\n", from);
		}
		wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
	}
}

ACTOR Future<Void> multiThreadedFDBTestClient(std::string testServer) {

	state NetworkAddress server = NetworkAddress::parse(testServer);
	state MultiThreadedFDBInterface interf(server);
	state std::vector<Future<Void>> clients;

	for (int i = 0; i < 100; i++) {
		clients.push_back(multithreadtestClient(interf));
	}

	wait(waitForAll(clients));
	return Void();
}
