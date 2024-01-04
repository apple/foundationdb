/*
 * ConsistencyCheck.h
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

#ifndef FDBCLIENT_CONSISTENCYCHECK_H
#define FDBCLIENT_CONSISTENCYCHECK_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

enum class ConsistencyCheckPhase : uint8_t {
	Invalid = 0,
	Complete = 1,
};

enum class ConsistencyCheckAssignment : uint8_t {
	Invalid = 0,
	Assigned = 1,
};

struct ConsistencyCheckState {
	constexpr static FileIdentifier file_identifier = 13804349;

	ConsistencyCheckState() : consistencyCheckerId(0), phase(0), assignment(0) {}

	ConsistencyCheckState(uint64_t consistencyCheckerId)
	  : consistencyCheckerId(consistencyCheckerId), phase(0), assignment(0) {}

	ConsistencyCheckState(ConsistencyCheckPhase phase)
	  : consistencyCheckerId(0), phase(static_cast<uint8_t>(phase)), assignment(0) {}

	ConsistencyCheckState(ConsistencyCheckAssignment assignment)
	  : consistencyCheckerId(0), phase(0), assignment(static_cast<uint8_t>(assignment)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, consistencyCheckerId, phase, assignment);
	}

	inline ConsistencyCheckPhase getPhase() const { return static_cast<ConsistencyCheckPhase>(this->phase); }
	inline ConsistencyCheckAssignment getAssignment() const {
		return static_cast<ConsistencyCheckAssignment>(this->assignment);
	}

	uint64_t consistencyCheckerId;
	uint8_t phase;
	uint8_t assignment;
};

#endif
