/*
 * IDisk.h
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

#ifndef FDBRPC_IDISK_H
#define FDBRPC_IDISK_H
#pragma once

#include "flow/flow.h"
#include "flow/FastRef.h"
#include "flow/Knobs.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct IDisk {
	virtual ~IDisk() = default;
	virtual void addref() = 0;
	virtual void delref() = 0;
	virtual Future<Void> waitUntilDiskReady(int64_t size, bool sync) = 0;
	virtual Future<int> read(int h, void* data, int length) = 0;
	virtual Future<int> write(int h, StringRef data) = 0;
};

Reference<IDisk> createSimSSD();
Reference<IDisk> createNeverDisk();

#endif
