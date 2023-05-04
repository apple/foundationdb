/*
 * HTTPServer.actor.cpp
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

#include "fdbrpc/HTTP.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Void> callbackHandler(Reference<IConnection> conn,
                                   Future<Void> readRequestDone,
                                   Reference<HTTP::IRequestHandler> requestHandler,
                                   Reference<HTTP::IncomingRequest> req,
                                   FlowMutex* mutex) {
	state Reference<HTTP::OutgoingResponse> response = makeReference<HTTP::OutgoingResponse>();
	state UnsentPacketQueue content;
	response->data.content = &content;
	response->data.contentLen = 0;

	try {
		wait(readRequestDone);
		wait(requestHandler->handleRequest(req, response));
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}
		// FIXME: other errors?
		if (e.code() == error_code_http_request_failed || e.code() == error_code_http_bad_response ||
		    e.code() == error_code_connection_failed) {
			TraceEvent(SevWarn, "HTTPServerConnHandlerInternalError").errorUnsuppressed(e);
			// reset to empty error response
			response->reset();
			response->code = 500;
		} else {
			TraceEvent(SevWarn, "HTTPServerConnHandlerUnexpectedError").errorUnsuppressed(e);
			throw e;
		}
	}
	// take out response mutex to ensure no parallel writers to response connection
	// FIXME: is this necessary? I think it is
	state FlowMutex::Lock lock = wait(mutex->take());
	try {
		wait(response->write(conn));
	} catch (Error& e) {
		lock.release();
		if (e.code() == error_code_connection_failed) {
			// connection back to client failed, end. They will retry if they still need the response.
			TraceEvent("HTTPServerConnHandlerResponseError").errorUnsuppressed(e);
			return Void();
		}
		TraceEvent("HTTPServerConnHandlerResponseUnexpectedError").errorUnsuppressed(e);
		throw e;
	}
	lock.release();
	return Void();
}

ACTOR Future<Void> connectionHandler(Reference<HTTP::SimServerContext> server,
                                     Reference<IConnection> conn,
                                     Reference<HTTP::IRequestHandler> requestHandler) {
	try {
		// TODO do we actually have multiple requests on a connection? how does this work
		state FlowMutex responseMutex;
		state Future<Void> readPrevRequest = Future<Void>(Void());
		wait(conn->acceptHandshake());
		loop {
			wait(readPrevRequest);
			wait(delay(0));
			wait(conn->onReadable());
			state Reference<HTTP::IncomingRequest> req = makeReference<HTTP::IncomingRequest>();
			readPrevRequest = req->read(conn, false);
			server->actors.add(callbackHandler(conn, readPrevRequest, requestHandler, req, &responseMutex));
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			TraceEvent("HTTPConnectionError", server->dbgid)
			    .errorUnsuppressed(e)
			    .suppressFor(1.0)
			    .detail("ConnID", conn->getDebugID())
			    .detail("FromAddress", conn->getPeerAddress());
		}
		conn->close();
	}
	return Void();
}

ACTOR Future<Void> listenActor(Reference<HTTP::SimServerContext> server,
                               Reference<HTTP::IRequestHandler> requestHandler,
                               NetworkAddress addr,
                               Reference<IListener> listener) {
	TraceEvent(SevDebug, "HTTPServerListenStart", server->dbgid).detail("ListenAddress", addr.toString());

	wait(requestHandler->init());

	TraceEvent(SevDebug, "HTTPServerListenInitialized", server->dbgid).detail("ListenAddress", addr.toString());

	try {
		loop {
			Reference<IConnection> conn = wait(listener->accept());
			if (!server->running) {
				TraceEvent("HTTPServerExitedAfterAccept", server->dbgid);
				break;
			}
			if (conn) {
				server->actors.add(connectionHandler(server, conn, requestHandler));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "HTTPListenError", server->dbgid).error(e);
		throw;
	}

	return Void();
}

NetworkAddress HTTP::SimServerContext::newAddress() {
	// allocate new addr, assert we have enough addr space
	ASSERT(listenAddresses.size() < 1000);
	return NetworkAddress(
	    g_simulator->getCurrentProcess()->address.ip, nextPort++, true /* isPublic*/, false /*isTLS*/);
}

void HTTP::SimServerContext::registerNewServer(NetworkAddress addr, Reference<HTTP::IRequestHandler> requestHandler) {
	listenAddresses.push_back(addr);
	listeners.push_back(INetworkConnections::net()->listen(addr));
	actors.add(listenActor(Reference<HTTP::SimServerContext>::addRef(this), requestHandler, addr, listeners.back()));
}

// unit test stuff

ACTOR Future<Void> helloWorldServerCallback(Reference<HTTP::IncomingRequest> req,
                                            Reference<HTTP::OutgoingResponse> response) {
	wait(delay(0));
	ASSERT_EQ(req->verb, HTTP::HTTP_VERB_POST);
	ASSERT_EQ(req->resource, "/hello-world");
	ASSERT_EQ(req->data.headers.size(), 2);
	ASSERT(req->data.headers.count("Hello"));

	ASSERT_EQ(req->data.headers["Hello"], "World");
	ASSERT(req->data.headers.count("Content-Length"));
	ASSERT_EQ(req->data.headers["Content-Length"], std::to_string(req->data.content.size()));
	ASSERT_EQ(req->data.contentLen, req->data.content.size());
	ASSERT_EQ(req->data.content, "Hello World Request!");

	response->code = 200;
	response->data.headers["Hello"] = "World";

	std::string hello = "Hello World Response!";
	PacketWriter pw(response->data.content->getWriteBuffer(hello.size()), nullptr, Unversioned());
	pw.serializeBytes(hello);
	response->data.contentLen = hello.size();

	return Void();
}

struct HelloWorldRequestHandler : HTTP::IRequestHandler, ReferenceCounted<HelloWorldRequestHandler> {
	Future<Void> handleRequest(Reference<HTTP::IncomingRequest> req,
	                           Reference<HTTP::OutgoingResponse> response) override {
		return helloWorldServerCallback(req, response);
	}
	Reference<HTTP::IRequestHandler> clone() override { return makeReference<HelloWorldRequestHandler>(); }

	void addref() override { ReferenceCounted<HelloWorldRequestHandler>::addref(); }
	void delref() override { ReferenceCounted<HelloWorldRequestHandler>::delref(); }
};

ACTOR Future<Void> helloErrorServerCallback(Reference<HTTP::IncomingRequest> req,
                                            Reference<HTTP::OutgoingResponse> response) {
	wait(delay(0));
	if (deterministicRandom()->coinflip()) {
		throw http_bad_response();
	} else {
		throw http_request_failed();
	}
}

struct HelloErrorRequestHandler : HTTP::IRequestHandler, ReferenceCounted<HelloErrorRequestHandler> {
	Future<Void> handleRequest(Reference<HTTP::IncomingRequest> req,
	                           Reference<HTTP::OutgoingResponse> response) override {
		return helloErrorServerCallback(req, response);
	}
	Reference<HTTP::IRequestHandler> clone() override { return makeReference<HelloErrorRequestHandler>(); }

	void addref() override { ReferenceCounted<HelloErrorRequestHandler>::addref(); }
	void delref() override { ReferenceCounted<HelloErrorRequestHandler>::delref(); }
};

typedef std::function<Future<Reference<HTTP::IncomingResponse>>(Reference<IConnection> conn)> DoRequestFunction;

// handles retrying on timeout and reinitializing connection like other users of HTTP (S3BlobStore, RestClient)
ACTOR Future<Reference<HTTP::IncomingResponse>> doRequestTest(std::string hostname,
                                                              std::string service,
                                                              DoRequestFunction reqFunction) {
	state Reference<IConnection> conn;
	loop {
		if (!conn) {
			wait(store(conn, INetworkConnections::net()->connect(hostname, service, false)));
			ASSERT(conn.isValid());
			wait(conn->connectHandshake());
		}

		try {
			Future<Reference<HTTP::IncomingResponse>> f = reqFunction(conn);
			Reference<HTTP::IncomingResponse> response = wait(f);
			conn->close();
			return response;
		} catch (Error& e) {
			conn->close();
			if (e.code() != error_code_timed_out && e.code() != error_code_connection_failed) {
				throw e;
			}
			// request got stuck, close conn and try again
			conn.clear();
			wait(delay(0.1));
		}
	}
}

ACTOR Future<Reference<HTTP::IncomingResponse>> doHelloWorldReq(Reference<IConnection> conn) {
	state UnsentPacketQueue content;
	state Reference<HTTP::OutgoingRequest> req = makeReference<HTTP::OutgoingRequest>();

	state Reference<IRateControl> sendReceiveRate = makeReference<Unlimited>();
	state int64_t bytes_sent = 0;

	req->verb = HTTP::HTTP_VERB_POST;
	req->resource = "/hello-world";
	req->data.headers["Hello"] = "World";

	std::string hello = "Hello World Request!";

	req->data.content = &content;
	req->data.contentLen = hello.size();

	PacketWriter pw(req->data.content->getWriteBuffer(hello.size()), nullptr, Unversioned());
	pw.serializeBytes(hello);

	Reference<HTTP::IncomingResponse> response =
	    wait(timeoutError(HTTP::doRequest(conn, req, sendReceiveRate, &bytes_sent, sendReceiveRate), 30.0));

	ASSERT_EQ(response->code, 200);
	ASSERT_EQ(response->data.headers.size(), 2);
	ASSERT(response->data.headers.count("Hello"));
	ASSERT_EQ(response->data.headers["Hello"], "World");
	ASSERT(response->data.headers.count("Content-Length"));
	ASSERT_EQ(response->data.headers["Content-Length"], std::to_string(response->data.content.size()));
	ASSERT_EQ(response->data.contentLen, response->data.content.size());
	ASSERT_EQ(response->data.content, "Hello World Response!");

	return response;
}

ACTOR Future<Reference<HTTP::IncomingResponse>> doHelloWorldErrorReq(Reference<IConnection> conn) {
	state UnsentPacketQueue content;
	state Reference<HTTP::OutgoingRequest> req = makeReference<HTTP::OutgoingRequest>();

	state Reference<IRateControl> sendReceiveRate = makeReference<Unlimited>();
	state int64_t bytes_sent = 0;

	req->verb = HTTP::HTTP_VERB_GET;
	req->resource = "/hello-error";

	req->data.content = &content;
	req->data.contentLen = 0;

	Reference<HTTP::IncomingResponse> response =
	    wait(timeoutError(HTTP::doRequest(conn, req, sendReceiveRate, &bytes_sent, sendReceiveRate), 30.0));

	ASSERT(response->code == 500);

	return response;
}

// can't run as regular unit test right now because it needs special setup
TEST_CASE("!/HTTP/Server/HelloWorld") {
	ASSERT(g_network->isSimulated());
	fmt::print("Registering sim server\n");
	wait(g_simulator->registerSimHTTPServer("helloworld", "80", 1, makeReference<HelloWorldRequestHandler>()));
	fmt::print("Registered sim server\n");

	wait(success(doRequestTest("helloworld", "80", doHelloWorldReq)));

	fmt::print("Done\n");
	return Void();
}

TEST_CASE("!/HTTP/Server/HelloError") {
	ASSERT(g_network->isSimulated());
	fmt::print("Registering sim server\n");
	wait(g_simulator->registerSimHTTPServer("helloerror", "80", 1, makeReference<HelloErrorRequestHandler>()));
	fmt::print("Registered sim server\n");

	wait(success(doRequestTest("helloerror", "80", doHelloWorldErrorReq)));

	fmt::print("Done\n");
	return Void();
}