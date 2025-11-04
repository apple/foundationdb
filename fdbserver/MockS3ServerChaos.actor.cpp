/*
 * MockS3ServerChaos.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

// Design: design/mocks3server_chaos_design.md

#include "fdbserver/MockS3ServerChaos.h"
#include "fdbserver/MockS3Server.h"
#include "flow/ChaosMetrics.h"
#include "fdbrpc/simulator.h"
#include "flow/Trace.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h"

// Clear the chaos server registry (for testing/debugging only)
// NOTE: In production simulation tests, the registry should NOT be cleared between tests,
// as it must stay in sync with the simulator's persistent httpHandlers map to prevent
// duplicate registration attempts that would trigger assertions.
void clearMockS3ChaosRegistry() {
	if (g_network && g_simulator) {
		size_t previousSize = g_simulator->registeredMockS3ChaosServers.size();
		g_simulator->registeredMockS3ChaosServers.clear();
		TraceEvent("MockS3ChaosRegistryCleared").detail("PreviousSize", previousSize);
	}
}

// Helper function to classify S3 operations
S3Operation classifyS3Operation(const std::string& method, const std::string& resource) {
	if (method == "GET" || method == "HEAD") {
		return S3Operation::READ;
	} else if (method == "PUT") {
		if (resource.find("uploads") != std::string::npos) {
			return S3Operation::MULTIPART;
		}
		return S3Operation::WRITE;
	} else if (method == "DELETE") {
		return S3Operation::DELETE;
	} else if (method == "POST") {
		if (resource.find("uploads") != std::string::npos) {
			return S3Operation::MULTIPART;
		}
		return S3Operation::WRITE;
	} else {
		return S3Operation::READ; // Default fallback
	}
}

// Get operation-specific multiplier for chaos rates
double getOperationMultiplier(S3Operation op) {
	auto injector = S3FaultInjector::injector();
	switch (op) {
	case S3Operation::READ:
		return injector->getReadMultiplier();
	case S3Operation::WRITE:
		return injector->getWriteMultiplier();
	case S3Operation::DELETE:
		return injector->getDeleteMultiplier();
	case S3Operation::LIST:
		return injector->getListMultiplier();
	case S3Operation::MULTIPART:
		return injector->getWriteMultiplier(); // Use write multiplier for multipart
	default:
		return 1.0;
	}
}

// Generate S3-compatible error XML
std::string generateS3ErrorXML(const std::string& code, const std::string& message, const std::string& resource) {
	return format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
	              "<Error>\n"
	              "  <Code>%s</Code>\n"
	              "  <Message>%s</Message>\n"
	              "  <Resource>%s</Resource>\n"
	              "  <RequestId>%s</RequestId>\n"
	              "</Error>",
	              code.c_str(),
	              message.c_str(),
	              resource.c_str(),
	              deterministicRandom()->randomUniqueID().toString().c_str());
}

// Phase 1: Inject delay if configured
ACTOR Future<Void> maybeInjectDelay(S3Operation op) {
	auto injector = S3FaultInjector::injector();
	double delayRate = injector->getDelayRate() * getOperationMultiplier(op);

	if (delayRate > 0.0 && deterministicRandom()->random01() < delayRate) {
		double maxDelay = injector->getMaxDelay();
		double delayTime = deterministicRandom()->random01() * maxDelay;

		TraceEvent("MockS3ChaosDelay")
		    .detail("Operation", (int)op)
		    .detail("Delay", delayTime)
		    .detail("MaxDelay", maxDelay);

		wait(delay(delayTime));
	}

	return Void();
}

// Phase 2: Inject errors if configured
ACTOR Future<bool> maybeInjectError(Reference<HTTP::IncomingRequest> req,
                                    Reference<HTTP::OutgoingResponse> response,
                                    S3Operation op) {
	auto injector = S3FaultInjector::injector();
	double errorRate = injector->getErrorRate() * getOperationMultiplier(op);

	if (errorRate <= 0.0 || deterministicRandom()->random01() >= errorRate) {
		return false;
	}

	// Check for throttling (429) first
	double throttleRate = injector->getThrottleRate() * getOperationMultiplier(op);
	if (throttleRate > 0.0 && deterministicRandom()->random01() < throttleRate) {
		response->code = 429;
		response->data.headers["Content-Type"] = "application/xml";
		response->data.headers["Retry-After"] = "1";

		std::string errorXML = generateS3ErrorXML("Throttling", "Request was throttled. Please retry.", req->resource);
		response->data.content = new UnsentPacketQueue();
		response->data.contentLen = errorXML.size();
		PacketBuffer* buffer = response->data.content->getWriteBuffer(errorXML.size());
		PacketWriter writer(buffer, nullptr, Unversioned());
		writer.serializeBytes(errorXML);
		writer.finish();

		TraceEvent("MockS3ChaosThrottle").detail("Operation", (int)op).detail("Resource", req->resource);

		// Update metrics
		auto metrics = g_network->global(INetwork::enChaosMetrics);
		if (metrics) {
			static_cast<ChaosMetrics*>(metrics)->s3Throttles++;
		}

		return true;
	}

	// Check for general errors (500, 502, 503, 401, 406)
	if (deterministicRandom()->random01() < errorRate) {
		// Select error type based on weighted distribution
		double errorType = deterministicRandom()->random01();

		std::string errorXML;
		std::string errorType_str;
		int errorCode;

		if (errorType < 0.4) {
			// 40% - 503 Service Unavailable
			errorCode = 503;
			errorType_str = "ServiceUnavailable";
			errorXML = generateS3ErrorXML(
			    "ServiceUnavailable", "The service is temporarily unavailable. Please retry.", req->resource);
		} else if (errorType < 0.7) {
			// 30% - 500 Internal Server Error
			errorCode = 500;
			errorType_str = "InternalError";
			errorXML =
			    generateS3ErrorXML("InternalError", "We encountered an internal error. Please retry.", req->resource);
		} else if (errorType < 0.85) {
			// 15% - 502 Bad Gateway
			errorCode = 502;
			errorType_str = "BadGateway";
			errorXML = generateS3ErrorXML("BadGateway", "Bad gateway error occurred.", req->resource);
		} else if (errorType < 0.92) {
			// 7% - 401 Unauthorized
			errorCode = 401;
			errorType_str = "InvalidToken";
			errorXML = generateS3ErrorXML("InvalidToken", "The provided token is invalid.", req->resource);
		} else {
			// 8% - 406 Not Acceptable / Token expired
			errorCode = 406;
			errorType_str = "ExpiredToken";
			errorXML = generateS3ErrorXML("ExpiredToken", "The provided token has expired.", req->resource);
		}

		response->code = errorCode;
		response->data.headers["Content-Type"] = "application/xml";
		response->data.content = new UnsentPacketQueue();
		response->data.contentLen = errorXML.size();
		PacketBuffer* buffer = response->data.content->getWriteBuffer(errorXML.size());
		PacketWriter writer(buffer, nullptr, Unversioned());
		writer.serializeBytes(errorXML);
		writer.finish();

		TraceEvent("MockS3ChaosError")
		    .detail("Operation", (int)op)
		    .detail("ErrorType", errorType_str)
		    .detail("ErrorCode", errorCode)
		    .detail("Resource", req->resource);

		// Update metrics
		auto metrics = g_network->global(INetwork::enChaosMetrics);
		if (metrics) {
			static_cast<ChaosMetrics*>(metrics)->s3Errors++;
		}

		return true;
	}

	return false;
}

// Phase 4: Corrupt response data if configured
ACTOR Future<Void> maybeCorruptResponse(Reference<HTTP::OutgoingResponse> response, S3Operation op) {
	auto injector = S3FaultInjector::injector();
	double corruptionRate = injector->getCorruptionRate() * getOperationMultiplier(op);

	if (corruptionRate <= 0.0 || deterministicRandom()->random01() >= corruptionRate) {
		return Void();
	}

	// Only corrupt successful responses
	if (response->code >= 200 && response->code < 300) {
		// Invalidate ETag to simulate data corruption
		if (response->data.headers.find("ETag") != response->data.headers.end()) {
			response->data.headers["ETag"] = "\"corrupted-" + deterministicRandom()->randomUniqueID().toString() + "\"";
		}

		TraceEvent("MockS3ChaosCorruption").detail("Operation", (int)op).detail("Resource", "response_data");

		// Update metrics
		auto metrics = g_network->global(INetwork::enChaosMetrics);
		if (metrics) {
			static_cast<ChaosMetrics*>(metrics)->s3Corruptions++;
		}
	}

	return Void();
}

// Core chaos server implementation
class MockS3ChaosServerImpl {
public:
	ACTOR static Future<Void> handleRequest(Reference<HTTP::IncomingRequest> req,
	                                        Reference<HTTP::OutgoingResponse> response) {
		// Classify the operation type (must be state since used after wait())
		state S3Operation op = classifyS3Operation(req->verb, req->resource);

		TraceEvent("MockS3ChaosRequest")
		    .detail("Method", req->verb)
		    .detail("Resource", req->resource)
		    .detail("Operation", (int)op)
		    .detail("ContentLength", req->data.contentLen);

		// Phase 1: Inject delay if configured
		wait(maybeInjectDelay(op));

		// Phase 2: Check if we should inject an error
		state bool errorInjected = wait(maybeInjectError(req, response, op));
		if (errorInjected) {
			return Void();
		}

		// Phase 3: Delegate to base MockS3Server for normal processing
		// Use the exposed processMockS3Request function to access the shared server instance
		wait(processMockS3Request(req, response));

		// Phase 4: Possibly corrupt the response data
		wait(maybeCorruptResponse(response, op));

		return Void();
	}
};

// Public interface implementation
Future<Void> MockS3ChaosRequestHandler::handleRequest(Reference<HTTP::IncomingRequest> req,
                                                      Reference<HTTP::OutgoingResponse> response) {
	return MockS3ChaosServerImpl::handleRequest(req, response);
}

Reference<HTTP::IRequestHandler> MockS3ChaosRequestHandler::clone() {
	return makeReference<MockS3ChaosRequestHandler>();
}

// Safe server registration that prevents conflicts using truly simulator-global registry
ACTOR Future<Void> registerMockS3ChaosServer(std::string ip, std::string port) {
	state std::string serverKey = ip + ":" + port;
	ASSERT(g_simulator);

	TraceEvent("MockS3ChaosServerRegistration")
	    .detail("Phase", "Start")
	    .detail("IP", ip)
	    .detail("Port", port)
	    .detail("ServerKey", serverKey)
	    .detail("IsSimulated", g_network->isSimulated())
	    .detail("AlreadyRegistered", g_simulator->registeredMockS3ChaosServers.count(serverKey) > 0);

	// Check if server is already registered using truly simulator-global registry
	if (g_simulator->registeredMockS3ChaosServers.count(serverKey)) {
		TraceEvent(SevWarn, "MockS3ChaosServerAlreadyRegistered").detail("Address", serverKey);
		return Void();
	}

	try {
		// Enable persistence BEFORE registering the server to prevent race conditions
		// where requests arrive before persistence is configured
		if (!isMockS3PersistenceEnabled()) {
			std::string persistenceDir = "simfdb/mocks3";
			enableMockS3Persistence(persistenceDir);
			TraceEvent("MockS3ChaosServerPersistenceEnabled")
			    .detail("Address", serverKey)
			    .detail("PersistenceDir", persistenceDir);

			// Load any previously persisted state (for crash recovery in simulation)
			wait(loadMockS3PersistedStateFuture());
		}

		wait(g_simulator->registerSimHTTPServer(ip, port, makeReference<MockS3ChaosRequestHandler>()));
		g_simulator->registeredMockS3ChaosServers.insert(serverKey);

		// Enable persistence automatically for all MockS3 instances (including chaos)
		wait(initializeMockS3Persistence(serverKey));

		TraceEvent("MockS3ChaosServerRegistered")
		    .detail("Address", serverKey)
		    .detail("Success", true)
		    .detail("TotalRegistered", g_simulator->registeredMockS3ChaosServers.size());

	} catch (Error& e) {
		TraceEvent(SevError, "MockS3ChaosServerRegistrationFailed")
		    .error(e)
		    .detail("Address", serverKey)
		    .detail("ErrorCode", e.code())
		    .detail("ErrorName", e.name());
		throw;
	}

	return Void();
}

// Public Interface Implementation
ACTOR Future<Void> startMockS3ServerChaos(NetworkAddress listenAddress) {
	ASSERT(g_network->isSimulated());

	TraceEvent("MockS3ChaosServerStart")
	    .detail("Address", listenAddress.toString())
	    .detail("IP", listenAddress.ip.toString())
	    .detail("Port", listenAddress.port);

	// Register the chaos-enabled HTTP server
	wait(registerMockS3ChaosServer(listenAddress.ip.toString(), std::to_string(listenAddress.port)));

	TraceEvent("MockS3ChaosServerStarted").detail("Address", listenAddress.toString()).detail("Ready", true);

	return Void();
}

#include "flow/unactorcompiler.h"