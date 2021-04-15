/*
 * ProcessInterface.h
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

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

constexpr UID WLTOKEN_PROCESS(-1, 11);

struct ProcessInterface {
	constexpr static FileIdentifier file_identifier = 985636;
	RequestStream<struct GetProcessInterfaceRequest> getInterface;
	RequestStream<struct EchoRequest> echo;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, echo);
	}
};

struct GetProcessInterfaceRequest {
	constexpr static FileIdentifier file_identifier = 7632546;
	ReplyPromise<ProcessInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

// TODO: Used for demonstration purposes, remove in later PR
struct EchoRequest {
	constexpr static FileIdentifier file_identifier = 10624019;
	std::string message;
	ReplyPromise<std::string> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, message, reply);
	}
};
