/*
 * ClientWorkerInterface.h
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

#ifndef FDBCLIENT_CLIENTWORKERINTERFACE_H
#define FDBCLIENT_CLIENTWORKERINTERFACE_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/Status.h"
#include "fdbclient/CommitProxyInterface.h"

// Streams from WorkerInterface that are safe and useful to call from a client.
// A ClientWorkerInterface is embedded as the first element of a WorkerInterface.
struct ClientWorkerInterface {
	constexpr static FileIdentifier file_identifier = 12418152;

	RequestStream<struct RebootRequest> reboot;
	RequestStream<struct ProfilerRequest> profiler;
	RequestStream<struct SetFailureInjection> setFailureInjection;

	bool operator==(ClientWorkerInterface const& r) const { return id() == r.id(); }
	bool operator!=(ClientWorkerInterface const& r) const { return id() != r.id(); }
	UID id() const { return reboot.getEndpoint().token; }
	NetworkAddress address() const { return reboot.getEndpoint().getPrimaryAddress(); }

	void initEndpoints() { reboot.getEndpoint(TaskPriority::ReadSocket); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reboot, profiler, setFailureInjection);
	}
};

struct RebootRequest {
	constexpr static FileIdentifier file_identifier = 11913957;
	bool deleteData;
	bool checkData;
	uint32_t waitForDuration; // seconds

	explicit RebootRequest(bool deleteData = false, bool checkData = false, uint32_t waitForDuration = 0)
	  : deleteData(deleteData), checkData(checkData), waitForDuration(waitForDuration) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, deleteData, checkData, waitForDuration);
	}
};

struct ProfilerRequest {
	constexpr static FileIdentifier file_identifier = 15437862;
	ReplyPromise<Void> reply;

	enum class Type : std::int8_t {
		GPROF = 1,
		FLOW = 2,
		GPROF_HEAP = 3,
	};

	enum class Action : std::int8_t { DISABLE = 0, ENABLE = 1, RUN = 2 };

	Type type;
	Action action;
	int duration;
	Standalone<StringRef> outputFile;

	ProfilerRequest() = default;
	explicit ProfilerRequest(Type t, Action a, int d) : type(t), action(a), duration(d) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply, type, action, duration, outputFile);
	}
};

struct SetFailureInjection {
	constexpr static FileIdentifier file_identifier = 15439864;
	ReplyPromise<Void> reply;
	struct DiskFailureCommand {
		// how often should the disk be stalled (0 meaning once, 10 meaning every 10 secs)
		double stallInterval;
		// Period of time disk stalls will be injected for
		double stallPeriod;
		// Period of time the disk will be slowed down for
		double throttlePeriod;

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, stallInterval, stallPeriod, throttlePeriod);
		}
	};

	struct FlipBitsCommand {
		// percent of bits to flip in the given file
		double percentBitFlips;

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, percentBitFlips);
		}
	};

	Optional<DiskFailureCommand> diskFailure;
	Optional<FlipBitsCommand> flipBits;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply, diskFailure, flipBits);
	}
};
#endif
