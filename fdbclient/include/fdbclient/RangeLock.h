/*
 * RangeLock.h
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

#ifndef FDBCLIENT_RANGELOCK_H
#define FDBCLIENT_RANGELOCK_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

enum class RangeLockType : uint8_t {
	Invalid = 0,
	RejectAll = 1,
};

struct RangeLockState {
	constexpr static FileIdentifier file_identifier = 1384409;

public:
	RangeLockState() = default;

	RangeLockState(RangeLockType type) : lockerType(type) {}

	bool isValid() const { return lockerType != RangeLockType::Invalid; }

	std::string toString() const {
		return "RangeLockState: [lockerType]: " + std::to_string(static_cast<uint8_t>(lockerType));
	}

	bool operator==(RangeLockState const& r) const { return lockerType == r.lockerType; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lockerType);
	}

private:
	RangeLockType lockerType;
};

#endif
