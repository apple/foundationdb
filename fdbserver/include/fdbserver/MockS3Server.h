/*
 * MockS3Server.h
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

// Mock S3 Server for deterministic testing of S3 operations
// Supports:
// - Basic GET/PUT/DELETE/HEAD object operations
// - Multipart uploads (initiate, upload parts, complete, abort)
// - Object tagging (put/get tags)
// - In-memory storage with deterministic behavior
// - S3-compatible XML responses

// HTTP request handler for Mock S3 Server
class MockS3RequestHandler : public HTTP::IRequestHandler, public ReferenceCounted<MockS3RequestHandler> {
public:
	MockS3RequestHandler() : destructing(false) {}

	// Prevent virtual function calls during destruction
	~MockS3RequestHandler() { destructing = true; }

	Future<Void> handleRequest(Reference<HTTP::IncomingRequest> req,
	                           Reference<HTTP::OutgoingResponse> response) override;
	Reference<HTTP::IRequestHandler> clone() override;

	void addref() override {
		if (!destructing) {
			ReferenceCounted<MockS3RequestHandler>::addref();
		}
	}
	void delref() override {
		if (!destructing) {
			ReferenceCounted<MockS3RequestHandler>::delref();
		}
	}

private:
	std::atomic<bool> destructing;
};

// Start a mock S3 server listening on the specified address (simulation mode)
Future<Void> startMockS3Server(const NetworkAddress& listenAddress);

// Start a mock S3 server in real HTTP mode for ctests
Future<Void> startMockS3ServerReal(const NetworkAddress& listenAddress);

// Clear all MockS3 global storage - called at the start of each simulation test
// to prevent data accumulation across multiple tests
void clearMockS3Storage();
