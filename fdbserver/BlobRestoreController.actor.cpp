/*
 * BlobRestoreController.actor.cpp
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

#include "fdbserver/BlobGranuleServerCommon.actor.h"

#include "flow/actorcompiler.h" // has to be last include

//
// This module offers routines to manage blob restore state for given range.
//

// Return true if the given key range is restoring. It returns true even if part of the key range is restoring
ACTOR Future<bool> BlobRestoreController::isRestoring(Reference<BlobRestoreController> self) {
	BlobRestoreRangeState rangeState = wait(BlobRestoreController::getRangeState(self));
	return !rangeState.first.empty() && rangeState.second.phase != BlobRestorePhase::DONE;
}

// Check the given key range and return subrange that is restoring. Returns empty range if no restoring
// for any portion of the given range. Note that it's possible that only partial of the range is restoring.
ACTOR Future<BlobRestoreRangeState> BlobRestoreController::getRangeState(Reference<BlobRestoreController> self) {
	state Transaction tr(self->db_);
	loop {
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			state GetRangeLimits limits(SERVER_KNOBS->BLOB_MANIFEST_RW_ROWS);
			limits.minRows = 0;
			state KeySelectorRef begin = firstGreaterOrEqual(blobRestoreCommandKeys.begin);
			state KeySelectorRef end = firstGreaterOrEqual(blobRestoreCommandKeys.end);
			loop {
				RangeResult ranges = wait(tr.getRange(begin, end, limits, Snapshot::True));
				for (auto& r : ranges) {
					KeyRange keyRange = decodeBlobRestoreCommandKeyFor(r.key);
					if (self->range_.intersects(keyRange)) {
						Standalone<BlobRestoreState> restoreState = decodeBlobRestoreState(r.value);
						KeyRangeRef intersected(std::max(self->range_.begin, keyRange.begin),
						                        std::min(self->range_.end, keyRange.end));
						return std::make_pair(intersected, restoreState);
					}
				}
				if (!ranges.more) {
					break;
				}
				if (ranges.readThrough.present()) {
					begin = firstGreaterOrEqual(ranges.readThrough.get());
				} else {
					begin = firstGreaterThan(ranges.end()[-1].key);
				}
			}
			return std::make_pair(KeyRangeRef(), BlobRestoreState(BlobRestorePhase::DONE));
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> BlobRestoreController::updateState(Reference<BlobRestoreController> self,
                                                      Standalone<BlobRestoreState> newState,
                                                      Optional<BlobRestorePhase> expectedPhase) {
	state Transaction tr(self->db_);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			state Key key = blobRestoreCommandKeyFor(self->range_);

			Optional<Value> oldValue = wait(tr.get(key));
			if (oldValue.present()) {
				Standalone<BlobRestoreState> oldState = decodeBlobRestoreState(oldValue.get());
				// check if current phase is expected
				if (expectedPhase.present() && oldState.phase != expectedPhase.get()) {
					TraceEvent("BlobRestoreUnexpectedPhase")
					    .detail("Expected", expectedPhase.get())
					    .detail("Current", oldState.phase);
					throw restore_error();
				}
				newState.phaseStartTs = VectorRef<int64_t>(newState.arena(), oldState.phaseStartTs);
				if (newState.phase != oldState.phase) {
					newState.phaseStartTs.resize(newState.arena(), BlobRestorePhase::MAX);
					newState.phaseStartTs[newState.phase] = now();
				}
			}

			Value value = blobRestoreCommandValueFor(newState);
			tr.set(key, value);
			wait(tr.commit());
			TraceEvent("BlobRestoreUpdatePhase").detail("Phase", newState.phase);
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> BlobRestoreController::updateState(Reference<BlobRestoreController> self,
                                                      BlobRestorePhase newPhase,
                                                      Optional<BlobRestorePhase> expectedPhase) {
	Standalone<BlobRestoreState> newState;
	newState.phase = newPhase;
	wait(BlobRestoreController::updateState(self, newState, expectedPhase));
	return Void();
}

ACTOR Future<Void> BlobRestoreController::updateError(Reference<BlobRestoreController> self,
                                                      Standalone<StringRef> errorMessage) {
	Standalone<BlobRestoreState> newState;
	newState.error = StringRef(newState.arena(), errorMessage);
	newState.phase = BlobRestorePhase::ERROR;
	wait(BlobRestoreController::updateState(self, newState, {}));
	return Void();
}

ACTOR Future<Optional<BlobRestoreState>> BlobRestoreController::getState(Reference<BlobRestoreController> self) {
	std::pair<KeyRange, BlobRestoreState> rangeStatus = wait(BlobRestoreController::getRangeState(self));
	Optional<BlobRestoreState> result;
	if (!rangeStatus.first.empty()) {
		result = rangeStatus.second;
	}
	return result;
}

// Get restore argument
ACTOR Future<Optional<BlobRestoreArg>> BlobRestoreController::getArgument(Reference<BlobRestoreController> self) {
	state Transaction tr(self->db_);
	state Optional<BlobRestoreArg> result;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				state GetRangeLimits limits(SERVER_KNOBS->BLOB_MANIFEST_RW_ROWS);
				limits.minRows = 0;
				state KeySelectorRef begin = firstGreaterOrEqual(blobRestoreArgKeys.begin);
				state KeySelectorRef end = firstGreaterOrEqual(blobRestoreArgKeys.end);
				loop {
					RangeResult ranges = wait(tr.getRange(begin, end, limits, Snapshot::True));
					for (auto& r : ranges) {
						KeyRange keyRange = decodeBlobRestoreArgKeyFor(r.key);
						if (self->range_.intersects(keyRange)) {
							Standalone<BlobRestoreArg> arg = decodeBlobRestoreArg(r.value);
							result = arg;
							return result;
						}
					}
					if (!ranges.more) {
						break;
					}
					if (ranges.readThrough.present()) {
						begin = firstGreaterOrEqual(ranges.readThrough.get());
					} else {
						begin = firstGreaterThan(ranges.back().key);
					}
				}
				return result;
			} catch (Error& e) {
				wait(tr.onError(e));
			}

		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Get restore target version. Return defaultVersion if no restore argument available for the range
ACTOR Future<Version> BlobRestoreController::getTargetVersion(Reference<BlobRestoreController> self,
                                                              Version defaultVersion) {
	Optional<BlobRestoreArg> arg = wait(BlobRestoreController::getArgument(self));
	if (arg.present()) {
		if (arg.get().version.present()) {
			return arg.get().version.get();
		}
	}
	return defaultVersion;
}
