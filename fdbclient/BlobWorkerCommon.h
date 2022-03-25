/*
 * BlobWorkerCommon.h
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

#ifndef FDBCLIENT_BLOBWORKERCOMMON_H
#define FDBCLIENT_BLOBWORKERCOMMON_H

#include "fdbrpc/Stats.h"

struct BlobWorkerStats {
	CounterCollection cc;
	Counter s3PutReqs, s3GetReqs, s3DeleteReqs;
	Counter deltaFilesWritten, snapshotFilesWritten;
	Counter deltaBytesWritten, snapshotBytesWritten;
	Counter bytesReadFromFDBForInitialSnapshot;
	Counter bytesReadFromS3ForCompaction;
	Counter rangeAssignmentRequests, readRequests;
	Counter wrongShardServer;
	Counter changeFeedInputBytes;
	Counter readReqTotalFilesReturned;
	Counter readReqDeltaBytesReturned;
	Counter commitVersionChecks;
	Counter granuleUpdateErrors;
	Counter granuleRequestTimeouts;
	Counter readRequestsWithBegin;
	Counter readRequestsCollapsed;

	int numRangesAssigned;
	int mutationBytesBuffered;
	int activeReadRequests;

	Future<Void> logger;

	// Current stats maintained for a given blob worker process
	explicit BlobWorkerStats(UID id, double interval)
	  : cc("BlobWorkerStats", id.toString()),

	    s3PutReqs("S3PutReqs", cc), s3GetReqs("S3GetReqs", cc), s3DeleteReqs("S3DeleteReqs", cc),
	    deltaFilesWritten("DeltaFilesWritten", cc), snapshotFilesWritten("SnapshotFilesWritten", cc),
	    deltaBytesWritten("DeltaBytesWritten", cc), snapshotBytesWritten("SnapshotBytesWritten", cc),
	    bytesReadFromFDBForInitialSnapshot("BytesReadFromFDBForInitialSnapshot", cc),
	    bytesReadFromS3ForCompaction("BytesReadFromS3ForCompaction", cc),
	    rangeAssignmentRequests("RangeAssignmentRequests", cc), readRequests("ReadRequests", cc),
	    wrongShardServer("WrongShardServer", cc), changeFeedInputBytes("RangeFeedInputBytes", cc),
	    readReqTotalFilesReturned("ReadReqTotalFilesReturned", cc),
	    readReqDeltaBytesReturned("ReadReqDeltaBytesReturned", cc), commitVersionChecks("CommitVersionChecks", cc),
	    granuleUpdateErrors("GranuleUpdateErrors", cc), granuleRequestTimeouts("GranuleRequestTimeouts", cc),
	    readRequestsWithBegin("ReadRequestsWithBegin", cc), readRequestsCollapsed("ReadRequestsCollapsed", cc),
	    numRangesAssigned(0), mutationBytesBuffered(0), activeReadRequests(0) {
		specialCounter(cc, "NumRangesAssigned", [this]() { return this->numRangesAssigned; });
		specialCounter(cc, "MutationBytesBuffered", [this]() { return this->mutationBytesBuffered; });
		specialCounter(cc, "ActiveReadRequests", [this]() { return this->activeReadRequests; });

		logger = traceCounters("BlobWorkerMetrics", id, interval, &cc, "BlobWorkerMetrics");
	}
};

#endif