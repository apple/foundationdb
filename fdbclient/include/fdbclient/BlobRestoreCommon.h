
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

	KeyBackedProperty<bool> enabled() { return subspace.pack(__FUNCTION__sr); }
	KeyBackedProperty<std::string> manifestUrl() { return subspace.pack(__FUNCTION__sr); }
	KeyBackedProperty<std::string> mutationLogsUrl() { return subspace.pack(__FUNCTION__sr); }
};

#endif