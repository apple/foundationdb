/*
 * RocksDBCommon.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_ROCKSDBCOMMON_H
#define FDBSERVER_ROCKSDBCOMMON_H
#pragma once

#ifdef WITH_ROCKSDB

#include "fdbclient/FDBTypes.h"

#include <rocksdb/listener.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>

#include <string>

namespace RocksDBCommon {

// Convert FoundationDB StringRef to RocksDB Slice
rocksdb::Slice toSlice(StringRef s);

// Convert RocksDB Slice to FoundationDB StringRef
StringRef toStringRef(rocksdb::Slice s);

// Returns string representation of RocksDB background error reason.
// Error reason code:
// https://github.com/facebook/rocksdb/blob/12d798ac06bcce36be703b057d5f5f4dab3b270c/include/rocksdb/listener.h#L125
std::string getErrorReason(rocksdb::BackgroundErrorReason reason);

// Get WAL recovery mode from SERVER_KNOBS->ROCKSDB_WAL_RECOVERY_MODE
rocksdb::WALRecoveryMode getWalRecoveryMode();

// Get WAL recovery mode from a specific knob value (for sharded variant)
rocksdb::WALRecoveryMode getWalRecoveryModeFromKnob(int knobValue);

// Get compaction priority from a knob value
rocksdb::CompactionPri getCompactionPriorityFromKnob(int knobValue);

// Get index type from a knob value
rocksdb::BlockBasedTableOptions::IndexType getIndexTypeFromKnob(int knobValue);

} // namespace RocksDBCommon

#endif // WITH_ROCKSDB
#endif // FDBSERVER_ROCKSDBCOMMON_H
