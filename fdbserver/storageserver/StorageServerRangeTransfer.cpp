/*
 * StorageServerRangeTransfer.cpp
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

#include "StorageServerRangeTransfer.h"

#include "flow/Coroutines.h"
#include "flow/Trace.h"

Future<Void> deleteFilesInRemovedRange(IKeyValueStore* kvStore,
                                       KeyRange range,
                                       Future<Void> clearDurable,
                                       std::function<bool()> stillUnassigned,
                                       UID ssId) {
	// Older reads may still be in progress, wait for the clear to complete before deleting files.
	co_await clearDurable;
	// DD may have moved the range back to this server while we waited. If so, it may now hold newly
	// fetched data, so don't delete its files.
	if (!stillUnassigned()) {
		co_return;
	}
	try {
		co_await kvStore->deleteFilesInRange(range);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		// This is only an optimization: the range's clear already removed the data, and compaction reclaims the space.
		TraceEvent(SevWarnAlways, "DeleteFilesInRemovedRangeFailed", ssId).error(e).detail("Range", range);
	}
}
