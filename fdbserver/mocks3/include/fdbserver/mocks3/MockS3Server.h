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

	void addref() override { ReferenceCounted<MockS3RequestHandler>::addref(); }
	void delref() override { ReferenceCounted<MockS3RequestHandler>::delref(); }

private:
	std::atomic<bool> destructing;
};

// Start a mock S3 server listening on the specified address (simulation mode)
Future<Void> startMockS3Server(const NetworkAddress& listenAddress);

// Start a mock S3 server in real HTTP mode for ctests
// persistenceDir: Optional directory for storing persistence files (default: "simfdb/mocks3")
Future<Void> startMockS3ServerReal(const NetworkAddress& listenAddress, const std::string& persistenceDir = "");

// Clear all MockS3 global storage - called at the start of each simulation test
// to prevent data accumulation across multiple tests
void clearMockS3Storage();

// Register MockS3 server in simulation - this is the preferred way to register MockS3
// as it automatically enables persistence and prevents duplicate registrations
Future<Void> registerMockS3Server(std::string ip, std::string port);

// Enable persistence for MockS3 storage - stores all objects and multipart uploads
// to disk for analysis and crash recovery
// persistenceDir: Directory where data will be stored (e.g., "simfdb/mocks3")
// Creates directory structure: <persistenceDir>/objects/<bucket>/ and <persistenceDir>/multipart/
void enableMockS3Persistence(const std::string& persistenceDir);

// Check if MockS3 persistence is currently enabled
bool isMockS3PersistenceEnabled();

// Load any previously persisted MockS3 state from disk
Future<Void> loadMockS3PersistedStateFuture();

// Initialize MockS3 persistence for a specific server (simulation)
// This combines enabling persistence and loading any previously persisted state
// serverKey: Identifier for the server (e.g., "127.0.0.1:8080")
Future<Void> initializeMockS3Persistence(std::string const& serverKey);

// Process a Mock S3 request directly (for wrapping/chaos injection)
// This is the low-level request processor used by MockS3RequestHandler
Future<Void> processMockS3Request(Reference<HTTP::IncomingRequest> req, Reference<HTTP::OutgoingResponse> response);
