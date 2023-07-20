/*
 * BlobGranuleCommon.cpp
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

#include "fdbclient/BlobGranuleCommon.h"

BlobGranuleSummaryRef summarizeGranuleChunk(Arena& ar, const BlobGranuleChunkRef& chunk) {
	BlobGranuleSummaryRef summary;
	ASSERT(chunk.snapshotFile.present());
	ASSERT(chunk.snapshotVersion != invalidVersion);
	ASSERT(chunk.includedVersion >= chunk.snapshotVersion);
	ASSERT(chunk.newDeltas.empty());

	if (chunk.tenantPrefix.present()) {
		summary.keyRange = KeyRangeRef(ar, chunk.keyRange.removePrefix(chunk.tenantPrefix.get()));
	} else {
		summary.keyRange = KeyRangeRef(ar, chunk.keyRange);
	}

	summary.snapshotVersion = chunk.snapshotVersion;
	summary.snapshotSize = chunk.snapshotFile.get().length;
	summary.deltaVersion = chunk.includedVersion;
	summary.deltaSize = 0;
	for (auto& it : chunk.deltaFiles) {
		summary.deltaSize += it.length;
	}

	return summary;
}