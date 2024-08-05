/*
 * network.h
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

#ifndef FLOW_SWIFT_H
#define FLOW_SWIFT_H

#include "swift_support.h"
#include "swift/ABI/Task.h"
#include "flow/ProtocolVersion.h"
#pragma once

#include <array>
#include <variant>
#include <atomic>

// ==== ----------------------------------------------------------------------------------------------------------------

inline pthread_t _tid() {
	return pthread_self();
}

// ==== ----------------------------------------------------------------------------------------------------------------

double flow_gNetwork_now();

enum class TaskPriority;
Future<class Void> flow_gNetwork_delay(double seconds, TaskPriority taskID);

// ==== ----------------------------------------------------------------------------------------------------------------

/// A count in nanoseconds.
using JobDelay = unsigned long long;

class ExecutorRef {
public:
	void* Identity;
	uintptr_t Implementation;

	constexpr ExecutorRef(void* identity, uintptr_t implementation)
	  : Identity(identity), Implementation(implementation) {}

	constexpr static ExecutorRef generic() { return ExecutorRef(nullptr, 0); }
};

#endif