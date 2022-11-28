/*
 * EncryptionOpUtils.h
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

#ifndef FDBSERVER_ENCRYPTION_OPS_UTIL_H
#define FDBSERVER_ENCRYPTION_OPS_UTIL_H
#pragma once

#include "fdbserver/Knobs.h"
#include "fdbclient/CommitProxyInterface.h"

typedef enum { TLOG_ENCRYPTION = 0, STORAGE_SERVER_ENCRYPTION = 1, BLOB_GRANULE_ENCRYPTION = 2 } EncryptOperationType;

inline bool isEncryptionOpSupported(EncryptOperationType operation_type) {
	// We would check against dbInfo.isEncryptionEnabled instead, but the dbInfo may not be available before
	// ClusterController broadcast the dbInfo to workers. Before the broadcast encryption may appear to be disabled
	// when it should be enabled. Moving the encryption switch to DB config could fix the issue.
	if (!SERVER_KNOBS->ENABLE_ENCRYPTION) {
		return false;
	}

	if (operation_type == TLOG_ENCRYPTION) {
		return SERVER_KNOBS->ENABLE_TLOG_ENCRYPTION;
	} else if (operation_type == STORAGE_SERVER_ENCRYPTION) {
		return SERVER_KNOBS->ENABLE_STORAGE_SERVER_ENCRYPTION;
	} else if (operation_type == BLOB_GRANULE_ENCRYPTION) {
		bool supported = SERVER_KNOBS->ENABLE_BLOB_GRANULE_ENCRYPTION && SERVER_KNOBS->BG_METADATA_SOURCE == "tenant";
		ASSERT((supported && SERVER_KNOBS->ENABLE_ENCRYPTION) || !supported);
		return supported;
	} else {
		return false;
	}
}

inline bool isEncryptionOpSupported(EncryptOperationType operation_type, EncryptionAtRestMode encryptMode) {
	if (operation_type == BLOB_GRANULE_ENCRYPTION) {
		return encryptMode.isEncryptionEnabled() && SERVER_KNOBS->BG_METADATA_SOURCE == "tenant";
	}
	return encryptMode.isEncryptionEnabled();
}

#endif // FDBSERVER_ENCRYPTION_OPS_UTIL_H
