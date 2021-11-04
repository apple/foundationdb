/*
 * BlobPruner.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/BlobWorkerInterface.h"
#include "flow/actorcompiler.h" // has to be last include

#define BP_DEBUG true

ACTOR Future<Void> deleteFiles(Version lastVersion) {
	// TODO: implement
	return Void();
}

ACTOR Future<Void> blobPruner(PromiseStream<PruneFilesRequest>& pruneStream) {
	Version lastDeleteTime = -1;
	try {
		loop choose {
			when(PruneFilesRequest req = waitNext(pruneStream.getFuture())) {
				if (lastDeleteTime < req.pruneVersion) {
					deleteFiles(req.pruneVersion);
				}
			}
		}
	} catch (Error& err) {
		TraceEvent("BlobPrunerDied");
	}
	return Void();
}
