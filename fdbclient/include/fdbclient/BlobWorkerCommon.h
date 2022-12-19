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
	Counter rangeAssignmentRequests, readRequests, summaryReads;
	Counter wrongShardServer;
	Counter changeFeedInputBytes;
	Counter readReqTotalFilesReturned;
	Counter readReqDeltaBytesReturned;
	Counter commitVersionChecks;
	Counter granuleUpdateErrors;
	Counter granuleRequestTimeouts;
	Counter readRequestsWithBegin;
	Counter readRequestsCollapsed;
	Counter flushGranuleReqs;
	Counter compressionBytesRaw;
	Counter compressionBytesFinal;
	Counter fullRejections;
	Counter forceFlushCleanups;
	Counter readDrivenCompactions;

	int numRangesAssigned;
	int mutationBytesBuffered;
	int activeReadRequests;
	int granulesPendingSplitCheck;
	Version minimumCFVersion;
	Version cfVersionLag;
	int notAtLatestChangeFeeds;
	int64_t lastResidentMemory;
	int64_t estimatedMaxResidentMemory;

	LatencySample snapshotBlobWriteLatencySample;
	LatencySample deltaBlobWriteLatencySample;
	LatencySample reSnapshotLatencySample;
	LatencySample readLatencySample;

	Reference<FlowLock> initialSnapshotLock;
	Reference<FlowLock> resnapshotLock;
	Reference<FlowLock> deltaWritesLock;

	Future<Void> logger;

	// Current stats maintained for a given blob worker process
	explicit BlobWorkerStats(UID id,
	                         double interval,
	                         Reference<FlowLock> initialSnapshotLock,
	                         Reference<FlowLock> resnapshotLock,
	                         Reference<FlowLock> deltaWritesLock,
	                         double sampleLoggingInterval,
	                         double fileOpLatencySketchAccuracy,
	                         double requestLatencySketchAccuracy)
	  : cc("BlobWorkerStats", id.toString()),

	    s3PutReqs("S3PutReqs", cc), s3GetReqs("S3GetReqs", cc), s3DeleteReqs("S3DeleteReqs", cc),
	    deltaFilesWritten("DeltaFilesWritten", cc), snapshotFilesWritten("SnapshotFilesWritten", cc),
	    deltaBytesWritten("DeltaBytesWritten", cc), snapshotBytesWritten("SnapshotBytesWritten", cc),
	    bytesReadFromFDBForInitialSnapshot("BytesReadFromFDBForInitialSnapshot", cc),
	    bytesReadFromS3ForCompaction("BytesReadFromS3ForCompaction", cc),
	    rangeAssignmentRequests("RangeAssignmentRequests", cc), readRequests("ReadRequests", cc),
	    summaryReads("SummaryReads", cc), wrongShardServer("WrongShardServer", cc),
	    changeFeedInputBytes("ChangeFeedInputBytes", cc), readReqTotalFilesReturned("ReadReqTotalFilesReturned", cc),
	    readReqDeltaBytesReturned("ReadReqDeltaBytesReturned", cc), commitVersionChecks("CommitVersionChecks", cc),
	    granuleUpdateErrors("GranuleUpdateErrors", cc), granuleRequestTimeouts("GranuleRequestTimeouts", cc),
	    readRequestsWithBegin("ReadRequestsWithBegin", cc), readRequestsCollapsed("ReadRequestsCollapsed", cc),
	    flushGranuleReqs("FlushGranuleReqs", cc), compressionBytesRaw("CompressionBytesRaw", cc),
	    compressionBytesFinal("CompressionBytesFinal", cc), fullRejections("FullRejections", cc),
	    forceFlushCleanups("ForceFlushCleanups", cc), readDrivenCompactions("ReadDrivenCompactions", cc),
	    numRangesAssigned(0), mutationBytesBuffered(0), activeReadRequests(0), granulesPendingSplitCheck(0),
	    minimumCFVersion(0), cfVersionLag(0), notAtLatestChangeFeeds(0), lastResidentMemory(0),
	    snapshotBlobWriteLatencySample("SnapshotBlobWriteMetrics",
	                                   id,
	                                   sampleLoggingInterval,
	                                   fileOpLatencySketchAccuracy),
	    deltaBlobWriteLatencySample("DeltaBlobWriteMetrics", id, sampleLoggingInterval, fileOpLatencySketchAccuracy),
	    reSnapshotLatencySample("GranuleResnapshotMetrics", id, sampleLoggingInterval, fileOpLatencySketchAccuracy),
	    readLatencySample("GranuleReadLatencyMetrics", id, sampleLoggingInterval, requestLatencySketchAccuracy),
	    estimatedMaxResidentMemory(0), initialSnapshotLock(initialSnapshotLock), resnapshotLock(resnapshotLock),
	    deltaWritesLock(deltaWritesLock) {
		specialCounter(cc, "NumRangesAssigned", [this]() { return this->numRangesAssigned; });
		specialCounter(cc, "MutationBytesBuffered", [this]() { return this->mutationBytesBuffered; });
		specialCounter(cc, "ActiveReadRequests", [this]() { return this->activeReadRequests; });
		specialCounter(cc, "GranulesPendingSplitCheck", [this]() { return this->granulesPendingSplitCheck; });
		specialCounter(cc, "MinimumChangeFeedVersion", [this]() { return this->minimumCFVersion; });
		specialCounter(cc, "CFVersionLag", [this]() { return this->cfVersionLag; });
		specialCounter(cc, "NotAtLatestChangeFeeds", [this]() { return this->notAtLatestChangeFeeds; });
		specialCounter(cc, "LastResidentMemory", [this]() { return this->lastResidentMemory; });
		specialCounter(cc, "EstimatedMaxResidentMemory", [this]() { return this->estimatedMaxResidentMemory; });
		specialCounter(cc, "InitialSnapshotsActive", [this]() { return this->initialSnapshotLock->activePermits(); });
		specialCounter(cc, "InitialSnapshotsWaiting", [this]() { return this->initialSnapshotLock->waiters(); });
		specialCounter(cc, "ReSnapshotsActive", [this]() { return this->resnapshotLock->activePermits(); });
		specialCounter(cc, "ReSnapshotsWaiting", [this]() { return this->resnapshotLock->waiters(); });
		specialCounter(cc, "DeltaFileWritesActive", [this]() { return this->deltaWritesLock->activePermits(); });
		specialCounter(cc, "DeltaFileWritesWaiting", [this]() { return this->deltaWritesLock->waiters(); });

		logger = cc.traceCounters("BlobWorkerMetrics", id, interval, "BlobWorkerMetrics");
	}
};

#endif
