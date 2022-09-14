/*
 * network.h
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

#ifndef FLOW_SWIFT_H
#define FLOW_SWIFT_H

#include "swift/ABI/Task.h"
#include "flow/ProtocolVersion.h"
#pragma once

#include <array>
#include <variant>
#include <atomic>

// ==== ----------------------------------------------------------------------------------------------------------------

#define SWIFT_CXX_REF_IMMORTAL                                                                                         \
	__attribute__((swift_attr("import_as_ref"))) __attribute__((swift_attr("retain:immortal")))                        \
	__attribute__((swift_attr("release:immortal")))
#define SWIFT_SENDABLE __attribute__((swift_attr("@Sendable")))

// ==== ----------------------------------------------------------------------------------------------------------------

/// A count in nanoseconds.
using JobDelay = unsigned long long;

class ExecutorRef {
	void* Identity;
	uintptr_t Implementation;

	constexpr ExecutorRef(void* identity, uintptr_t implementation)
	  : Identity(identity), Implementation(implementation) {}

public:
	constexpr static ExecutorRef generic() { return ExecutorRef(nullptr, 0); }
};

#endif