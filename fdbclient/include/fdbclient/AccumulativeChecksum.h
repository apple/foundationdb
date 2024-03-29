/*
 * AccumulativeChecksum.h
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

#ifndef FDBCLIENT_ACCUMULATIVECHECKSUM_H
#define FDBCLIENT_ACCUMULATIVECHECKSUM_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

struct AccumulativeChecksumState {
	constexpr static FileIdentifier file_identifier = 13804380;

	AccumulativeChecksumState() : acs(0), version(invalidVersion), epoch(0), acsIndex(0) {}
	AccumulativeChecksumState(uint16_t acsIndex, uint32_t acs, Version version, LogEpoch epoch)
	  : acs(acs), acsIndex(acsIndex), version(version), epoch(epoch) {}

	std::string toString() const {
		return "AccumulativeChecksumState: [ACS Index]: " + std::to_string(acsIndex) +
		       ", [Acs]: " + std::to_string(acs) + ", [Version]: " + std::to_string(version) +
		       ", [Epoch]: " + std::to_string(epoch);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, acsIndex, acs, version, epoch);
	}

	uint32_t acs;
	uint16_t acsIndex;
	Version version;
	LogEpoch epoch;
};

#endif
