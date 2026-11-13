/*
 * RocksDBRangeTransfer.cpp
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

#include "RocksDBRangeTransfer.h"

#ifdef WITH_ROCKSDB

#include "RocksDBCommon.h"
#include "flow/Platform.h"
#include "flow/Trace.h"

#include <rocksdb/convenience.h>

namespace RocksDBRangeTransfer {

rocksdb::Status deleteFilesInRange(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, KeyRangeRef range, UID id) {
	rocksdb::Slice begin = RocksDBCommon::toSlice(range.begin);
	rocksdb::Slice end = RocksDBCommon::toSlice(range.end);
	uint64_t sstBytesBefore = 0;
	uint64_t sstBytesAfter = 0;
	db->GetIntProperty(cf, rocksdb::DB::Properties::kTotalSstFilesSize, &sstBytesBefore);
	double start = timer_monotonic();
	// FDB ranges exclude their end key, which may belong to a range this server still owns.
	rocksdb::Status s = rocksdb::DeleteFilesInRange(db, cf, &begin, &end, /*include_end=*/false);
	if (s.ok()) {
		db->GetIntProperty(cf, rocksdb::DB::Properties::kTotalSstFilesSize, &sstBytesAfter);
		TraceEvent("RocksDBDeleteFilesInRange", id)
		    .detail("Range", range)
		    .detail("Duration", timer_monotonic() - start)
		    .detail("SstBytesBefore", sstBytesBefore)
		    .detail("SstBytesAfter", sstBytesAfter);
	}
	return s;
}

} // namespace RocksDBRangeTransfer

#endif // WITH_ROCKSDB
