/*
 * EncryptionUtil.cpp
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

// This file implements the functions defined in EncryptionUtil.h

#include "fdbserver/EncryptionUtil.h"
#include "fdbserver/Knobs.h"

#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

bool isEncryptionEnabled(EncryptOperationType operation_type, bool clusterEncryptionEnabled) {
    // TraceEvent("Nim::here").detail("encrypt", clusterEncryptionEnabled).detail("op_type", operation_type).backtrace();
	if (clusterEncryptionEnabled) {
		return false;
	}

	if (operation_type == TLOG_ENCRYPTION) {
		return SERVER_KNOBS->ENABLE_TLOG_ENCRYPTION;
	} else if (operation_type == BLOB_GRANULE_ENCRYPTION) {
		return SERVER_KNOBS->ENABLE_BLOB_GRANULE_ENCRYPTION;
	} else {
		// TODO (Nim): Add once storage server encryption knob is created
		return false;
	}
}
