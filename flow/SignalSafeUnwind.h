/*
 * SignalSafeUnwind.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_SIGNAL_SAFE_UNWIND
#define FLOW_SIGNAL_SAFE_UNWIND
#pragma once

#include "Platform.h"


// backtrace() and exception unwinding in glibc both call dl_iterate_phdr(),
// which takes the loader lock and so is not async signal safe.  Profiling or slow task
// profiling can deadlock when they interrupt the unwinding of an exception.

// This library overrides the implementation of dl_iterate_phdr() so that it
// can be async signal safe in this context, at the cost of other restrictions

// Call this function after all dynamic libraries are loaded
// (no further calls to dlopen() or dlclose() are permitted).
// After calling it, dl_iterate_phdr() will be async-signal-safe.
// At this time, it is a no-op on all platforms except Linux
void initSignalSafeUnwind();

// This can be used by tests to measure the number of calls to dl_iterate_phdr intercepted
extern int64_t dl_iterate_phdr_calls;

#endif