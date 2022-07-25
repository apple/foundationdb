/*
 * TraceFileIO.h
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

#pragma once

#include "flow/flow.h"

// Checks that a given block of data is the same as what has been written by a call to debugFileSet
extern void debugFileCheck(std::string context, std::string file, const void* data, int64_t offset, int length);

// Updates the in-memory copy of tracked data at a given offset
extern void debugFileSet(std::string context, std::string file, const void* data, int64_t offset, int length);

// Updates the in-memory copy of tracked data to account for truncates (this simply invalidates any data after truncate
// point)
extern void debugFileTruncate(std::string context, std::string file, int64_t offset);
