/*
 * MockS3ServerChaos.h
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

#pragma once

#include "flow/flow.h"
#include "flow/network.h"
#include "fdbrpc/HTTP.h"
#include <atomic>

// MockS3ServerChaos - Comprehensive S3 chaos injection for FoundationDB simulation testing
//
// DESIGN:
//   See design/mocks3server_chaos_design.md for full design documentation
//   https://github.com/apple/foundationdb/tree/main/design/mocks3server_chaos_design.md
//
// OVERVIEW:
//   Chaos-enabled wrapper around MockS3Server that injects realistic S3 failures
//   to test client resilience and error handling. Follows AsyncFileChaos pattern.
//
// PHILOSOPHY:
//   "MockS3 should be more intolerant/strict than real S3" - surface bugs early
//   by providing comprehensive fault injection in a controlled, deterministic environment.
//
// CONFIGURATION:
//   Chaos is controlled by S3FaultInjector rates (0.0-1.0) via g_network->global(enS3FaultInjector).
//   No master boolean switch - fine-grained control per fault type.
//
//   S3FaultInjector::injector()->setErrorRate(0.1);        // 10% HTTP errors
//   S3FaultInjector::injector()->setThrottleRate(0.05);    // 5% throttling
//   S3FaultInjector::injector()->setDelayRate(0.1);        // 10% delays
//   S3FaultInjector::injector()->setCorruptionRate(0.01);  // 1% corruption
//   S3FaultInjector::injector()->setMaxDelay(5.0);         // Up to 5s delays
//
// FAULT TYPES:
//   - HTTP Errors: 429 (throttling), 500/502/503 (server errors), 401/406 (auth)
//   - Token Issues: InvalidToken, ExpiredToken, TokenRefreshRequired
//   - Network: Connection drops, timeouts, retry-after headers
//   - Data Corruption: ETag invalidation, response truncation
//   - Operation Targeting: Different rates for GET/PUT/DELETE/multipart/list
//
// USAGE:
//   // Start chaos server
//   wait(startMockS3ServerChaos(NetworkAddress::parse("127.0.0.1:8080")));
//
//   // Use with S3BlobStoreEndpoint
//   auto blobStore = S3BlobStoreEndpoint::fromString("blobstore://key:secret@127.0.0.1:8080", ...);
//
// INTEGRATION:
//   - Integrates with S3BlobStoreEndpoint for production client testing
//   - Works with existing workloads by replacing MockS3Server with MockS3ServerChaos
//   - Supports all S3 operations: PUT, GET, HEAD, DELETE, multipart, list
//   - Deterministic randomness based on simulation seed for reproducibility
//
// METRICS:
//   ChaosMetrics tracks injected faults: s3Errors, s3Throttles, s3Delays, s3Corruptions
//
// EXAMPLES:
//   See tests/slow/S3ClientWorkloadWithChaos.toml for comprehensive test configurations
//   See fdbserver/workloads/S3ClientWorkload.actor.cpp for usage example

// S3 Operation Types for targeted chaos
enum class S3Operation {
	READ, // GET, HEAD
	WRITE, // PUT (single and multipart)
	DELETE, // DELETE
	LIST, // List objects
	MULTIPART // Multipart operations (initiate, upload, complete, abort)
};

// HTTP request handler with chaos injection for Mock S3 Server
class MockS3ChaosRequestHandler : public HTTP::IRequestHandler, public ReferenceCounted<MockS3ChaosRequestHandler> {
public:
	MockS3ChaosRequestHandler() : destructing(false) {}

	// Prevent virtual function calls during destruction
	~MockS3ChaosRequestHandler() { destructing = true; }

	Future<Void> handleRequest(Reference<HTTP::IncomingRequest> req,
	                           Reference<HTTP::OutgoingResponse> response) override;
	Reference<HTTP::IRequestHandler> clone() override;

	void addref() override { ReferenceCounted<MockS3ChaosRequestHandler>::addref(); }
	void delref() override { ReferenceCounted<MockS3ChaosRequestHandler>::delref(); }

private:
	std::atomic<bool> destructing;
};

// Public interface
Future<Void> startMockS3ServerChaos(const NetworkAddress& listenAddress);