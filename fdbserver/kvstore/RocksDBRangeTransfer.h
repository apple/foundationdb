/*
 * RocksDBRangeTransfer.h
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

// RocksDB helpers for shards moving into and out of a storage server.

#ifndef FDBSERVER_ROCKSDBRANGETRANSFER_H
#define FDBSERVER_ROCKSDBRANGETRANSFER_H
#pragma once

#ifdef WITH_ROCKSDB

#include "fdbclient/FDBTypes.h"

#include <rocksdb/db.h>

namespace RocksDBRangeTransfer {

// Drops SST files lying entirely inside [range.begin, range.end) and traces the approximate bytes freed. Level-0 files
// and files partly outside the range are kept.
rocksdb::Status deleteFilesInRange(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, KeyRangeRef range, UID id);

} // namespace RocksDBRangeTransfer

#endif // WITH_ROCKSDB
#endif // FDBSERVER_ROCKSDBRANGETRANSFER_H
