/*
 * ThreadPrimitives.cpp
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

#include "flow/ThreadPrimitives.h"
#include "flow/Trace.h"
#include <stdint.h>
#include <iostream>
#include <errno.h>
#include <string.h>
#include <stdio.h>

#ifdef _WIN32
#include <windows.h>
#undef min
#undef max
#endif

extern std::string format(const char* form, ...);

Event::Event() = default;

Event::~Event() = default;

void Event::set() {
	latch.count_down();
}

void Event::block() {
	latch.wait();
}

// Mutex::impl is allocated from the heap so that its size doesn't need to be in the header.  Is there a better way?
Mutex::Mutex() {
	impl = new CRITICAL_SECTION;
	InitializeCriticalSection((CRITICAL_SECTION*)impl);
}
Mutex::~Mutex() {
	DeleteCriticalSection((CRITICAL_SECTION*)impl);
	delete (CRITICAL_SECTION*)impl;
}
void Mutex::enter() {
	EnterCriticalSection((CRITICAL_SECTION*)impl);
}
void Mutex::leave() {
	LeaveCriticalSection((CRITICAL_SECTION*)impl);
}
