/*
 * HTTPServer.cpp
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

#include "fdbrpc/HTTP.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"

struct SharedFlowMutex : ReferenceCounted<SharedFlowMutex> {
	FlowMutex mutex;
};

Future<Void> callbackHandler(Reference<IConnection> conn,
                             Future<Void> readRequestDone,
                             Reference<HTTP::IRequestHandler> requestHandler,
                             Reference<HTTP::IncomingRequest> req,
                             Reference<SharedFlowMutex> mutexHolder) {
	auto response = makeReference<HTTP::OutgoingResponse>();
	UnsentPacketQueue content;
	response->data.content = &content;
	response->data.contentLen = 0;

	try {
		co_await readRequestDone;
		co_await requestHandler->handleRequest(req, response);
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}
		// FIXME: other errors?
		if (e.code() == error_code_connection_failed) {
			TraceEvent(SevWarn, "HTTPServerConnHandlerFailed").error(e);
			// return, client will retry
			co_return;
		}
		if (e.code() == error_code_http_request_failed || e.code() == error_code_http_bad_response) {
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
	FlowMutex::Lock lock = co_await mutexHolder->mutex.take();
	try {
		co_await response->write(conn);
	} catch (Error& e) {
		lock.release();
		if (e.code() == error_code_connection_failed) {
			// connection back to client failed, end. They will retry if they still need the response.
			TraceEvent("HTTPServerConnHandlerResponseError").errorUnsuppressed(e);
			co_return;
		}
		TraceEvent("HTTPServerConnHandlerResponseUnexpectedError").errorUnsuppressed(e);
		throw e;
	}
	lock.release();
}

Future<Void> connectionHandler(Reference<HTTP::SimServerContext> server,
                               Reference<IConnection> conn,
                               Reference<HTTP::IRequestHandler> requestHandler) {
	try {
		// TODO do we actually have multiple requests on a connection? how does this work
		Reference<SharedFlowMutex> responseMutex = makeReference<SharedFlowMutex>();
		Future<Void> readPrevRequest = Future<Void>(Void());
		co_await conn->acceptHandshake();
		while (true) {
			co_await readPrevRequest;
			co_await delay(0);
			co_await conn->onReadable();
			auto req = makeReference<HTTP::IncomingRequest>();
			readPrevRequest = req->read(conn, false);
			server->actors.add(callbackHandler(conn, readPrevRequest, requestHandler, req, responseMutex));
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
}

Future<Void> listenActor(Reference<HTTP::SimServerContext> server,
                         Reference<HTTP::IRequestHandler> requestHandler,
                         NetworkAddress addr,
                         Reference<IListener> listener) {
	TraceEvent(SevDebug, "HTTPServerListenStart", server->dbgid).detail("ListenAddress", addr.toString());

	co_await requestHandler->init();

	TraceEvent(SevDebug, "HTTPServerListenInitialized", server->dbgid).detail("ListenAddress", addr.toString());

	try {
		while (true) {
			Reference<IConnection> conn = co_await listener->accept();
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
}

void HTTP::SimServerContext::registerNewServer(NetworkAddress addr, Reference<HTTP::IRequestHandler> requestHandler) {
	listenAddresses.push_back(addr);
	listeners.push_back(INetworkConnections::net()->listen(addr));
	actors.add(listenActor(Reference<HTTP::SimServerContext>::addRef(this), requestHandler, addr, listeners.back()));
}

void HTTP::SimRegisteredHandlerContext::updateDNS() {
	// if addresses is empty, that violates the assumption that there is at least one address when doing resolution.
	// Only update dns if we have at least one address
	if (!addresses.empty()) {
		INetworkConnections::net()->addMockTCPEndpoint(hostname, service, addresses);
	}
}

void HTTP::SimRegisteredHandlerContext::addAddress(NetworkAddress addr) {
	addresses.push_back(addr);
	fmt::print("HTTP: adding address {0} for {1}:{2}\n", addr.toString(), hostname, service);
	updateDNS();
}

void HTTP::SimRegisteredHandlerContext::removeIp(IPAddress ip) {
	fmt::print("HTTP: removing ip {0} for {1}:{2}\n", ip.toString(), hostname, service);
	for (int i = 0; i < addresses.size(); i++) {
		if (addresses[i].ip == ip) {
			swapAndPop(&addresses, i);
			i--;
		}
	}
	updateDNS();
}

struct AlwaysFailRequestHandler final : HTTP::IRequestHandler, ReferenceCounted<AlwaysFailRequestHandler> {
	Future<Void> handleRequest(Reference<HTTP::IncomingRequest> req,
	                           Reference<HTTP::OutgoingResponse> response) override {
		ASSERT(false);
		return Void();
	}
	Reference<HTTP::IRequestHandler> clone() override { return makeReference<AlwaysFailRequestHandler>(); }

	void addref() override { ReferenceCounted<AlwaysFailRequestHandler>::addref(); }
	void delref() override { ReferenceCounted<AlwaysFailRequestHandler>::delref(); }
};

Future<Void> HTTP::registerAlwaysFailHTTPHandler() {
	return g_simulator->registerSimHTTPServer("alwaysfail_donotuse", "80", makeReference<AlwaysFailRequestHandler>());
}

// unit test stuff

Future<Void> helloWorldServerCallback(Reference<HTTP::IncomingRequest> req,
                                      Reference<HTTP::OutgoingResponse> response) {
	co_await delay(0);
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
	response->data.headers["Content-MD5"] = HTTP::computeMD5Sum(hello);

	PacketWriter pw(response->data.content->getWriteBuffer(hello.size()), nullptr, Unversioned());
	pw.serializeBytes(hello);
	response->data.contentLen = hello.size();
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

Future<Void> helloErrorServerCallback(Reference<HTTP::IncomingRequest> req,
                                      Reference<HTTP::OutgoingResponse> response) {
	co_await delay(0);
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

Future<Void> helloBadMD5ServerCallback(Reference<HTTP::IncomingRequest> req,
                                       Reference<HTTP::OutgoingResponse> response) {
	co_await delay(0);
	ASSERT_EQ(req->verb, HTTP::HTTP_VERB_GET);
	ASSERT_EQ(req->resource, "/hello-world");
	ASSERT_EQ(req->data.headers.size(), 1);
	ASSERT(req->data.headers.count("Content-Length"));
	ASSERT_EQ(req->data.headers["Content-Length"], std::to_string(req->data.content.size()));
	ASSERT_EQ(req->data.contentLen, req->data.content.size());
	ASSERT_EQ(req->data.content, "Hello Bad MD5 Request!");

	response->code = 200;
	response->data.headers["Hello"] = "World";

	std::string hello = "Hello World Response!";
	response->data.headers["Content-MD5"] = HTTP::computeMD5Sum(hello);

	// change content to not match md5 sum!
	hello = "Hello Bad MD5 Response";

	PacketWriter pw(response->data.content->getWriteBuffer(hello.size()), nullptr, Unversioned());
	pw.serializeBytes(hello);
	response->data.contentLen = hello.size();
}

struct HelloBadMD5RequestHandler : HTTP::IRequestHandler, ReferenceCounted<HelloBadMD5RequestHandler> {
	Future<Void> handleRequest(Reference<HTTP::IncomingRequest> req,
	                           Reference<HTTP::OutgoingResponse> response) override {
		return helloBadMD5ServerCallback(req, response);
	}
	Reference<HTTP::IRequestHandler> clone() override { return makeReference<HelloBadMD5RequestHandler>(); }

	void addref() override { ReferenceCounted<HelloBadMD5RequestHandler>::addref(); }
	void delref() override { ReferenceCounted<HelloBadMD5RequestHandler>::delref(); }
};

using DoRequestFunction = std::function<Future<Reference<HTTP::IncomingResponse>>(Reference<IConnection> conn)>;

// handles retrying on timeout and reinitializing connection like other users of HTTP (S3BlobStore, RestClient)
Future<Reference<HTTP::IncomingResponse>> doRequestTest(std::string hostname,
                                                        std::string service,
                                                        DoRequestFunction reqFunction) {
	Reference<IConnection> conn;
	while (true) {
		Error err;
		try {
			if (!conn) {
				conn = co_await INetworkConnections::net()->connect(hostname, service, false);
				ASSERT(conn.isValid());
				co_await conn->connectHandshake();
			}

			Reference<HTTP::IncomingResponse> response = co_await reqFunction(conn);
			conn->close();
			co_return response;
		} catch (Error& e) {
			err = e;
		}

		if (conn) {
			conn->close();
		}
		if (err.code() != error_code_timed_out && err.code() != error_code_connection_failed &&
		    err.code() != error_code_lookup_failed) {
			throw err;
		}
		// Request timed out or the connection could not be established, so clear conn and try again.
		conn.clear();
		co_await delay(0.1);
	}
}

Future<Reference<HTTP::IncomingResponse>> doHelloWorldReq(Reference<IConnection> conn) {
	UnsentPacketQueue content;
	auto req = makeReference<HTTP::OutgoingRequest>();

	Reference<IRateControl> sendReceiveRate = makeReference<Unlimited>();
	int64_t bytes_sent = 0;

	req->verb = HTTP::HTTP_VERB_POST;
	req->resource = "/hello-world";
	req->data.headers["Hello"] = "World";

	std::string hello = "Hello World Request!";

	req->data.content = &content;
	req->data.contentLen = hello.size();

	PacketWriter pw(req->data.content->getWriteBuffer(hello.size()), nullptr, Unversioned());
	pw.serializeBytes(hello);

	Reference<HTTP::IncomingResponse> response =
	    co_await timeoutError(HTTP::doRequest(conn, req, sendReceiveRate, &bytes_sent, sendReceiveRate), 30.0);

	std::string expectedContent = "Hello World Response!";

	ASSERT_EQ(response->code, 200);
	ASSERT_EQ(response->data.headers.size(), 3);
	ASSERT(response->data.headers.count("Hello"));
	ASSERT_EQ(response->data.headers["Hello"], "World");
	ASSERT(response->data.headers.count("Content-Length"));
	ASSERT_EQ(response->data.headers["Content-Length"], std::to_string(response->data.content.size()));
	ASSERT(response->data.headers.count("Content-MD5"));
	ASSERT_EQ(response->data.headers["Content-MD5"], HTTP::computeMD5Sum(expectedContent));
	ASSERT_EQ(response->data.contentLen, response->data.content.size());
	ASSERT_EQ(response->data.content, expectedContent);

	co_return response;
}

Future<Reference<HTTP::IncomingResponse>> doHelloWorldErrorReq(Reference<IConnection> conn) {
	UnsentPacketQueue content;
	auto req = makeReference<HTTP::OutgoingRequest>();

	Reference<IRateControl> sendReceiveRate = makeReference<Unlimited>();
	int64_t bytes_sent = 0;

	req->verb = HTTP::HTTP_VERB_GET;
	req->resource = "/hello-error";

	req->data.content = &content;
	req->data.contentLen = 0;

	Reference<HTTP::IncomingResponse> response =
	    co_await timeoutError(HTTP::doRequest(conn, req, sendReceiveRate, &bytes_sent, sendReceiveRate), 30.0);

	ASSERT(response->code == 500);

	co_return response;
}

Future<Reference<HTTP::IncomingResponse>> doHelloBadMD5Req(Reference<IConnection> conn) {
	UnsentPacketQueue content;
	auto req = makeReference<HTTP::OutgoingRequest>();

	Reference<IRateControl> sendReceiveRate = makeReference<Unlimited>();
	int64_t bytes_sent = 0;

	req->verb = HTTP::HTTP_VERB_GET;
	req->resource = "/hello-world";

	std::string hello = "Hello Bad MD5 Request!";

	req->data.content = &content;
	req->data.contentLen = hello.size();

	PacketWriter pw(req->data.content->getWriteBuffer(hello.size()), nullptr, Unversioned());
	pw.serializeBytes(hello);

	Reference<HTTP::IncomingResponse> response =
	    co_await timeoutError(HTTP::doRequest(conn, req, sendReceiveRate, &bytes_sent, sendReceiveRate), 30.0);

	// should have gotten error
	ASSERT(false);

	co_return response;
}

// can't run as regular unit test right now because it needs special setup
TEST_CASE("/HTTP/Server/HelloWorld") {
	ASSERT(g_network->isSimulated());
	fmt::print("Registering sim server\n");
	std::string hostname = "helloworld-" + deterministicRandom()->randomUniqueID().toString();
	co_await g_simulator->registerSimHTTPServer(hostname, "80", makeReference<HelloWorldRequestHandler>());
	fmt::print("Registered sim server\n");

	co_await doRequestTest(hostname, "80", doHelloWorldReq);

	fmt::print("Done Hello\n");
}

TEST_CASE("/HTTP/Server/HelloError") {
	ASSERT(g_network->isSimulated());
	fmt::print("Registering sim server\n");
	std::string hostname = "helloerror-" + deterministicRandom()->randomUniqueID().toString();
	co_await g_simulator->registerSimHTTPServer(hostname, "80", makeReference<HelloErrorRequestHandler>());
	fmt::print("Registered sim server\n");

	co_await doRequestTest(hostname, "80", doHelloWorldErrorReq);

	fmt::print("Done Error\n");
}

TEST_CASE("/HTTP/Server/HelloBadMD5") {
	ASSERT(g_network->isSimulated());
	fmt::print("Registering sim server\n");
	std::string hostname = "hellobadmd5-" + deterministicRandom()->randomUniqueID().toString();
	co_await g_simulator->registerSimHTTPServer(hostname, "80", makeReference<HelloBadMD5RequestHandler>());
	fmt::print("Registered sim server\n");

	// TODO refactor this into ASSERT_ERROR()?
	try {
		co_await doRequestTest(hostname, "80", doHelloBadMD5Req);
		ASSERT(false);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_http_bad_response);
	}

	fmt::print("Done Bad MD5\n");
}
