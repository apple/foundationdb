/*
 * NetworkTest.h
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

#ifndef FDBRPC_TESTS_NETWORKTEST_H
#define FDBRPC_TESTS_NETWORKTEST_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "flow/FileIdentifier.h"
#include "flow/UnitTest.h"

constexpr int WLTOKEN_NETWORKTEST = WLTOKEN_FIRST_AVAILABLE;

struct NetworkTestInterface {
	RequestStream<struct NetworkTestRequest> test;
	NetworkTestInterface() = default;
	explicit NetworkTestInterface(NetworkAddress remote);
	explicit NetworkTestInterface(INetwork* local);
};

struct NetworkTestReply {
	constexpr static FileIdentifier file_identifier = 14465374;
	Standalone<StringRef> value;
	NetworkTestReply() = default;
	explicit NetworkTestReply(Standalone<StringRef> value) : value(value) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

struct NetworkTestRequest {
	constexpr static FileIdentifier file_identifier = 4146513;
	Standalone<StringRef> key;
	uint32_t replySize;
	ReplyPromise<struct NetworkTestReply> reply;
	NetworkTestRequest() = default;
	NetworkTestRequest(Standalone<StringRef> key, uint32_t replySize) : key(key), replySize(replySize) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, replySize, reply);
	}
};

Future<Void> networkTestServer();

Future<Void> networkTestClient(std::string const& testServers);

Future<Void> networkTestP2P(const UnitTestParameters& params, bool oneshot);

#endif
