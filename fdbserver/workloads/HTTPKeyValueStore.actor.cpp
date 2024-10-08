/*
 * HTTPKeyValueStore.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include "flow/serialize.h"
#include "fdbrpc/HTTP.h"
#include "fdbserver/workloads/workloads.actor.h"
#include <cstring>
#include <limits>

#include "flow/actorcompiler.h" // This must be the last #include.

/*
 * Implements a basic put/get key-value store over HTTP to test the http client and simulated server code.
 */

#define DEBUG_HTTPKV false

static const int maxClients = 100;

struct SimHTTPKVStore : NonCopyable, ReferenceCounted<SimHTTPKVStore> {
	std::unordered_map<std::string, std::string> data;

	int64_t lastClientSeqnos[maxClients];

	SimHTTPKVStore() {
		for (int i = 0; i < maxClients; i++) {
			lastClientSeqnos[i] = 0;
		}
	}

	bool checkClientSeqno(int clientId, int64_t seqNo) {
		ASSERT(clientId < maxClients);
		if (seqNo < lastClientSeqnos[clientId]) {
			if (DEBUG_HTTPKV) {
				fmt::print("Client {0} SeqNo {1} < {2}\n", clientId, seqNo, lastClientSeqnos[clientId]);
			}
			// out of order retransmit, ignore
			return false;
		}
		lastClientSeqnos[clientId] = seqNo;
		return true;
	}
};

void httpKVProcessPut(Reference<SimHTTPKVStore> kvStore,
                      std::string& key,
                      Reference<HTTP::IncomingRequest> req,
                      Reference<HTTP::OutgoingResponse> response) {
	// content is value to put in kv store
	kvStore->data[key] = req->data.content;

	if (DEBUG_HTTPKV) {
		fmt::print("KV:put {0} = {1}\n", key, req->data.content);
	}

	response->code = 200;
	response->data.contentLen = 0;
}

// content is empty for request, content for response is the value for the key
void httpKVProcessGet(Reference<SimHTTPKVStore> kvStore,
                      std::string& key,
                      Reference<HTTP::IncomingRequest> req,
                      Reference<HTTP::OutgoingResponse> response) {

	auto it = kvStore->data.find(key);
	ASSERT(it != kvStore->data.end());

	if (DEBUG_HTTPKV) {
		fmt::print("KV:get {0} = {1}\n", key, it->second);
	}

	response->code = 200;

	response->data.headers["Content-MD5"] = HTTP::computeMD5Sum(it->second);

	PacketWriter pw(response->data.content->getWriteBuffer(it->second.size()), nullptr, Unversioned());
	pw.serializeBytes(it->second);
	response->data.contentLen = it->second.size();
}

// key header always exists for both put and get
ACTOR Future<Void> httpKVRequestCallback(Reference<SimHTTPKVStore> kvStore,
                                         Reference<HTTP::IncomingRequest> req,
                                         Reference<HTTP::OutgoingResponse> response) {
	wait(delay(0));

	ASSERT(req->verb == HTTP::HTTP_VERB_PUT || req->verb == HTTP::HTTP_VERB_GET);
	ASSERT_EQ(req->resource, "/kv");

	// content-length and RequestID from http are already filled in
	// ASSERT_EQ(req->data.headers.size(), 5);
	ASSERT_EQ(req->data.headers.size(), 5);
	ASSERT(req->data.headers.contains("Key"));
	ASSERT(req->data.headers.contains("ClientID"));
	ASSERT(req->data.headers.contains("UID"));
	ASSERT(req->data.headers.contains("SeqNo"));

	int clientId = atoi(req->data.headers["ClientID"].c_str());
	int seqNo = atoi(req->data.headers["SeqNo"].c_str());

	ASSERT(req->data.headers.contains("Content-Length"));
	ASSERT_EQ(req->data.headers["Content-Length"], std::to_string(req->data.content.size()));
	ASSERT_EQ(req->data.contentLen, req->data.content.size());

	if (!kvStore->checkClientSeqno(clientId, seqNo)) {
		CODE_PROBE(true, "kv store ignoring out of order retransmit");
		throw http_request_failed();
	}

	std::string key = req->data.headers["ClientID"] + ":" + req->data.headers["Key"];

	// echo headers back to the client for validation
	response->data.headers["ClientID"] = req->data.headers["ClientID"];
	response->data.headers["Key"] = req->data.headers["Key"];
	response->data.headers["UID"] = req->data.headers["UID"];
	// FIXME: need to echo http request id if present too

	if (req->verb == HTTP::HTTP_VERB_PUT) {
		httpKVProcessPut(kvStore, key, req, response);
	} else {
		httpKVProcessGet(kvStore, key, req, response);
	}

	return Void();
}

static Reference<SimHTTPKVStore> globalKVStore = Reference<SimHTTPKVStore>();

struct KeyValueRequestHandler : HTTP::IRequestHandler, ReferenceCounted<KeyValueRequestHandler> {

	// global kv store for all request handler instances during simulation
	Reference<SimHTTPKVStore> myKVStore;

	KeyValueRequestHandler() {}

	Future<Void> handleRequest(Reference<HTTP::IncomingRequest> req,
	                           Reference<HTTP::OutgoingResponse> response) override {
		return httpKVRequestCallback(myKVStore, req, response);
	}

	Future<Void> init() override {
		if (!globalKVStore) {
			globalKVStore = makeReference<SimHTTPKVStore>();
		}
		myKVStore = globalKVStore;
		return Void();
	}

	Reference<HTTP::IRequestHandler> clone() override { return makeReference<KeyValueRequestHandler>(); }

	void addref() override { ReferenceCounted<KeyValueRequestHandler>::addref(); }
	void delref() override { ReferenceCounted<KeyValueRequestHandler>::delref(); }
};

struct HTTPKeyValueStoreWorkload : TestWorkload {
	static constexpr auto NAME = "HTTPKeyValueStore";
	double testDuration;
	int nodeCount;
	int opsPerSecond;
	Future<Void> client;
	int64_t nextSeqNo = 1;
	bool manualResolve;

	// handle race where test phase killed put in progress
	Optional<std::pair<std::string, std::string>> activePut;

	// client's view of the correct key-value state of the data
	std::unordered_map<std::string, std::string> myData;

	// client's connection it reuses between request attempts
	Reference<IConnection> conn;

	std::string hostname = "httpkvstore";
	std::string service = "80";

	PerfIntCounter getCount, putCount, connectCount, failedConnectCount;

	HTTPKeyValueStoreWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), putCount("PutCount"), getCount("GetCount"), connectCount("ConnectCount"),
	    failedConnectCount("FailedConnectCount") {
		testDuration = getOption(options, "testDuration"_sr, 30.0);
		nodeCount = getOption(options, "nodeCount"_sr, 100);
		opsPerSecond = getOption(options, "nodeCount"_sr, 100);
		manualResolve = getOption(options, "manualResolve"_sr, sharedRandomNumber % 2);
		// it's important that we select this on a test-by-test basis and not a connection-by-connection basis as if one
		// approach works but another doesn't, it would be hidden
		sharedRandomNumber /= 2;
	}

	std::string getKey(int i) {
		// FIXME: larger/more random keys?
		return std::to_string(i);
	}

	std::string randomValue() {
		// FIXME: larger/more random values?
		return deterministicRandom()->randomUniqueID().toString();
	}

	// handles retrying on timeout and reinitializing connection like other users of HTTP (S3BlobStore, RestClient)
	ACTOR Future<Reference<HTTP::IncomingResponse>> doKVRequest(HTTPKeyValueStoreWorkload* self,
	                                                            std::string key,
	                                                            Optional<std::string> value) {
		state UnsentPacketQueue content;
		state int seqNo = self->nextSeqNo;
		++self->nextSeqNo;
		loop {
			try {
				while (!self->conn) {
					// sometimes do resolve and connect directly, other times simulate what rest kms connector does and
					// resolve endpoints themself and then connect to one directly
					if (self->manualResolve) {
						state std::vector<NetworkAddress> addrs =
						    wait(INetworkConnections::net()->resolveTCPEndpoint(self->hostname, self->service));
						ASSERT(!addrs.empty());
						int idx = deterministicRandom()->randomInt(0, addrs.size());
						wait(store(self->conn,
						           timeoutError(INetworkConnections::net()->connect(
						                            addrs[idx].ip.toString(), std::to_string(addrs[idx].port), false),
						                        FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT)));

					} else {
						wait(store(
						    self->conn,
						    timeoutError(INetworkConnections::net()->connect(self->hostname, self->service, false),
						                 FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT)));
					}
					if (self->conn.isValid()) {
						wait(self->conn->connectHandshake());
						++self->connectCount;
					} else {
						wait(delay(0.1));
						++self->failedConnectCount;
					}
				}

				content.discardAll();
				state Reference<HTTP::OutgoingRequest> req = makeReference<HTTP::OutgoingRequest>();
				state UID requestID = deterministicRandom()->randomUniqueID();
				req->data.content = &content;
				req->data.contentLen = 0;
				req->resource = "/kv";
				req->data.headers["Key"] = key;
				req->data.headers["ClientID"] = std::to_string(self->clientId);
				req->data.headers["UID"] = requestID.toString();
				req->data.headers["SeqNo"] = std::to_string(seqNo);

				if (value.present()) {
					// put key-value pair
					req->verb = HTTP::HTTP_VERB_PUT;
					PacketWriter pw(req->data.content->getWriteBuffer(value.get().size()), nullptr, Unversioned());
					pw.serializeBytes(value.get());
					req->data.contentLen = value.get().size();
				} else {
					// get key-value pair
					req->verb = HTTP::HTTP_VERB_GET;
				}

				state Reference<IRateControl> sendReceiveRate = makeReference<Unlimited>();
				state int64_t bytes_sent = 0;
				Reference<HTTP::IncomingResponse> response = wait(
				    timeoutError(HTTP::doRequest(self->conn, req, sendReceiveRate, &bytes_sent, sendReceiveRate), 5.0));

				// sometimes randomly close connection anyway
				if (BUGGIFY_WITH_PROB(0.1)) {
					ASSERT(self->conn.isValid());
					self->conn->close();
					self->conn.clear();
				}

				ASSERT_EQ(response->code, 200);
				ASSERT(response->data.headers.contains("ClientID"));
				ASSERT_EQ(response->data.headers["ClientID"], std::to_string(self->clientId));
				ASSERT(response->data.headers.contains("Key"));
				ASSERT_EQ(response->data.headers["Key"], key);
				ASSERT(response->data.headers.contains("UID"));
				ASSERT_EQ(response->data.headers["UID"], requestID.toString());

				return response;
			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw e;
				}
				if (DEBUG_HTTPKV) {
					fmt::print("REQ: ERROR: {0}\n", e.name());
				}
				if (self->conn) {
					self->conn->close();
					self->conn.clear();
				}
				if (e.code() != error_code_timed_out && e.code() != error_code_connection_failed &&
				    e.code() != error_code_lookup_failed) {
					throw e;
				}

				// request got timed out or connection could not be established, close conn and try again

				wait(delay(0.1));
			}
		}
	}

	ACTOR Future<Void> put(HTTPKeyValueStoreWorkload* self, std::string key, std::string value) {
		// TODO do http request
		std::pair<std::string, std::string> active = { key, value };
		self->activePut = active;

		if (DEBUG_HTTPKV) {
			fmt::print("CL:put {0}:{1} = {2}\n", self->clientId, key, value);
		}

		Reference<HTTP::IncomingResponse> response = wait(self->doKVRequest(self, key, value));

		if (DEBUG_HTTPKV) {
			fmt::print("CL:put {0}:{1} = {2} DONE\n", self->clientId, key, value);
		}

		// upon success, put in map
		self->activePut.reset();
		self->myData[key] = value;
		++self->putCount;

		return Void();
	}

	ACTOR Future<Void> get(HTTPKeyValueStoreWorkload* self, std::string key, bool checkActive) {

		if (DEBUG_HTTPKV) {
			fmt::print("CL:get {0}:{1}\n", self->clientId, key);
		}

		Reference<HTTP::IncomingResponse> response = wait(self->doKVRequest(self, key, {}));

		if (DEBUG_HTTPKV) {
			fmt::print("CL:get {0}:{1} = {2} DONE\n", self->clientId, key, response->data.content);
		}

		if (!checkActive || !self->activePut.present()) {
			ASSERT_EQ(response->data.content, self->myData[key]);
		} else {
			bool contentCorrect = response->data.content == self->myData[key];
			bool inFlightCorrect =
			    self->activePut.get().first == key && self->activePut.get().second == response->data.content;
			ASSERT(contentCorrect || inFlightCorrect);
		}
		++self->getCount;

		return Void();
	}

	Future<Void> setup(Database const& cx) override { return _setup(this); }

	ACTOR Future<Void> _setup(HTTPKeyValueStoreWorkload* self) {
		ASSERT(g_network->isSimulated());
		if (self->clientId == 0) {
			TraceEvent("SimHTTPKeyValueStoreRegistering");
			if (DEBUG_HTTPKV) {
				fmt::print("Registering sim http kv server\n");
			}
			wait(g_simulator->registerSimHTTPServer(
			    self->hostname, self->service, makeReference<KeyValueRequestHandler>()));
			if (DEBUG_HTTPKV) {
				fmt::print("Registered sim http kv server\n");
			}
			TraceEvent("SimHTTPKeyValueStoreRegistered");
		}

		TraceEvent("SimHTTPKeyValueStoreLoading");

		state int i;
		for (i = 0; i < self->nodeCount; i++) {
			wait(self->put(self, self->getKey(i), self->randomValue()));
		}

		TraceEvent("SimHTTPKeyValueStoreLoaded");

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		client = httpKeyValueClient(this);
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		// cancel workload client first
		client = Future<Void>(Void());
		return _check(this);
	}

	ACTOR Future<bool> _check(HTTPKeyValueStoreWorkload* self) {
		// reset conn since it could have been cancelled during part of initialization
		if (self->conn) {
			self->conn->close();
			self->conn.clear();
		}

		TraceEvent("SimHTTPKeyValueStoreWorkloadChecking");
		state int i;
		for (i = 0; i < self->nodeCount; i++) {
			wait(self->get(self, self->getKey(i), true));
		}

		TraceEvent("SimHTTPKeyValueStoreWorkloadChecked");
		// tear down connection after test
		if (self->conn) {
			self->conn->close();
			self->conn.clear();
		}

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(getCount.getMetric());
		m.push_back(putCount.getMetric());
		m.push_back(connectCount.getMetric());
		m.push_back(failedConnectCount.getMetric());
	}

	ACTOR Future<Void> httpKeyValueClient(HTTPKeyValueStoreWorkload* self) {
		TraceEvent("SimHTTPKeyValueStoreWorkloadStarting");
		state double last = now();
		loop {
			state Future<Void> waitNextOp = poisson(&last, 1.0 / self->opsPerSecond);

			int key = deterministicRandom()->randomInt(0, self->nodeCount);

			if (deterministicRandom()->coinflip()) {
				wait(self->put(self, self->getKey(key), self->randomValue()));
			} else {
				wait(self->get(self, self->getKey(key), false));
			}

			wait(waitNextOp);
		}
	}
};

WorkloadFactory<HTTPKeyValueStoreWorkload> HTTPKeyValueStoreWorkloadFactory;