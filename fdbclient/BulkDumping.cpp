/*
 * BulkDumping.cpp
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

#include "fdbclient/BulkDumping.h"
#include "flow/UnitTest.h"

BulkDumpState createBulkDumpJob(const KeyRange& range,
                                const std::string& jobRoot,
                                const BulkLoadType& type,
                                const BulkLoadTransportMethod& transportMethod) {
	return BulkDumpState(range, type, transportMethod, jobRoot, Optional<UID>());
}

TEST_CASE("/bulkdump/generateErrorMetadata/dropsTaskFields") {
	const KeyRange jobRange = KeyRangeRef("a"_sr, "z"_sr);
	BulkDumpState job = createBulkDumpJob(jobRange, "root", BulkLoadType::SST, BulkLoadTransportMethod::CP);
	BulkDumpState task = job.generateRangeTask(KeyRangeRef("b"_sr, "c"_sr));
	const BulkLoadFileSet files("root", "part", "manifest.txt", "data.sst", "", BulkLoadChecksum());
	const BulkLoadManifest manifest(files,
	                                "b"_sr,
	                                "c"_sr,
	                                100,
	                                10,
	                                1,
	                                BulkLoadByteSampleSetting(0, "hashlittle2", 250, 100, 0.5),
	                                BulkLoadType::SST,
	                                BulkLoadTransportMethod::CP);
	const BulkDumpState complete = task.generateBulkDumpMetadataToPersist(manifest);
	ASSERT(complete.getTaskId().present());

	const BulkDumpState error = complete.generateErrorMetadata();
	ASSERT(error.getPhase() == BulkDumpPhase::Error);
	ASSERT(error.getJobId() == job.getJobId());
	ASSERT(error.getJobRange() == jobRange);
	ASSERT(!error.getTaskId().present());
	ASSERT(error.getRange().empty());
	// The job-level configuration survives, so the entry still describes the job.
	ASSERT_EQ(error.getManifest().toString(), job.getManifest().toString());
	return Void();
}
