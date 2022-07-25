/*
 * FaultInjection.h
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

#ifndef FLOW_FAULTINJECTION_H
#define FLOW_FAULTINJECTION_H
#pragma once

#if 1
#define INJECT_FAULT(error_type, context)                                                                              \
	do {                                                                                                               \
		if (should_inject_fault && should_inject_fault(context, __FILE__, __LINE__, error_code_##error_type))          \
			throw error_type().asInjectedFault();                                                                      \
	} while (0)

#define SHOULD_INJECT_FAULT(context) (should_inject_fault && should_inject_fault(context, __FILE__, __LINE__, 0))

extern bool (*should_inject_fault)(const char* context, const char* file, int line, int error_code);
extern bool faultInjectionActivated;
extern void enableFaultInjection(bool enabled); // Enable fault injection called from fdbserver actor main function
#else
#define INJECT_FAULT(error_type, context)
#endif

#endif