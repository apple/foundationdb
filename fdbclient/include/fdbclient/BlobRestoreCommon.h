
/*
 * BlobRestoreCommon.h
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

#ifndef FDBCLIENT_BLOBRESTORECOMMON_H
#define FDBCLIENT_BLOBRESTORECOMMON_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/KeyBackedTypes.actor.h"

struct BlobGranuleBackupConfig : public KeyBackedClass {
	BlobGranuleBackupConfig(KeyRef prefix = SystemKey("\xff\x02/bgbackup/"_sr)) : KeyBackedClass(prefix) {}

	KeyBackedProperty<bool> enabled() { return { subspace.pack(__FUNCTION__sr), trigger, TupleCodec<bool>() }; }
	KeyBackedProperty<std::string> manifestUrl() { return subspace.pack(__FUNCTION__sr); }
	KeyBackedProperty<std::string> mutationLogsUrl() { return subspace.pack(__FUNCTION__sr); }

	KeyBackedProperty<int64_t> lastFlushTs() { return subspace.pack(__FUNCTION__sr); }
	KeyBackedProperty<Version> lastFlushVersion() { return subspace.pack(__FUNCTION__sr); }
};

// Defines blob restore state
enum BlobRestorePhase {
	UNINIT = 0,
	INIT = 1,
	STARTING_MIGRATOR = 2,
	LOADING_MANIFEST = 3,
	LOADED_MANIFEST = 4,
	COPYING_DATA = 5,
	COPIED_DATA = 6,
	APPLYING_MLOGS = 7,
	DONE = 8,
	ERROR = 9,
	MAX = 10
};

struct BlobGranuleRestoreConfig : public KeyBackedClass {
	BlobGranuleRestoreConfig(KeyRef prefix = SystemKey("\xff\x02/bgrestore/"_sr)) : KeyBackedClass(prefix) {}

	KeyBackedProperty<UID> uid() { return subspace.pack(__FUNCTION__sr); }
	KeyBackedProperty<std::string> manifestUrl() { return subspace.pack(__FUNCTION__sr); }
	KeyBackedProperty<std::string> mutationLogsUrl() { return subspace.pack(__FUNCTION__sr); }
	KeyBackedProperty<Version> targetVersion() { return subspace.pack(__FUNCTION__sr); }

	KeyBackedProperty<BlobRestorePhase> phase() {
		return { subspace.pack(__FUNCTION__sr), trigger, TupleCodec<BlobRestorePhase>() };
	}
	KeyBackedProperty<int> progress() { return subspace.pack(__FUNCTION__sr); }
	KeyBackedProperty<std::string> error() { return subspace.pack(__FUNCTION__sr); }
	KeyBackedMap<BlobRestorePhase, int64_t> phaseStartTs() { return subspace.pack(__FUNCTION__sr); };
	KeyBackedProperty<UID> lock() { return subspace.pack(__FUNCTION__sr); }
	// Begin version to apply mutation logs
	KeyBackedProperty<Version> beginVersion() { return subspace.pack(__FUNCTION__sr); }
};

#endif
