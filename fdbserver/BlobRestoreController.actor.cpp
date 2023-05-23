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
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobRestoreCommon.h"
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"

#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // has to be last include

//
// This module offers routines to manage blob restore state for given range.
//

// Return true if the given key range is restoring. It returns true even if part of the key range is restoring
ACTOR Future<bool> BlobRestoreController::isRestoring(Reference<BlobRestoreController> self) {
	BlobRestorePhase phase = wait(BlobRestoreController::currentPhase(self));
	if (!self->range_.intersects(normalKeys)) {
		return false;
	}
	if (phase < BlobRestorePhase::INIT || phase == BlobRestorePhase::DONE) {
		return false;
	}
	return true;
}

// Get restore target version. Return defaultVersion if no restore argument available for the range
ACTOR Future<Version> BlobRestoreController::getTargetVersion(Reference<BlobRestoreController> self,
                                                              Version defaultVersion) {

	BlobGranuleRestoreConfig config;
	Version version = wait(config.targetVersion().getD(SystemDBWriteLockedNow(self->db_.getReference())));
	return version;
}

// Get current restore phase
ACTOR Future<BlobRestorePhase> BlobRestoreController::currentPhase(Reference<BlobRestoreController> self) {
	BlobGranuleRestoreConfig config;
	auto db = SystemDBWriteLockedNow(self->db_.getReference());
	BlobRestorePhase phase = wait(config.phase().getD(db, Snapshot::False, BlobRestorePhase::UNINIT));
	return phase;
}

// Set current restore phase
ACTOR Future<Void> BlobRestoreController::setPhase(Reference<BlobRestoreController> self,
                                                   BlobRestorePhase newPhase,
                                                   Optional<UID> owerId) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->db_));
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state BlobGranuleRestoreConfig config;
			state BlobRestorePhase phase = wait(config.phase().getD(tr, Snapshot::True, BlobRestorePhase::MAX));
			state UID currentOwnerId = wait(config.lock().getD(tr));
			// check if current phase is expected
			if (owerId.present() && currentOwnerId != owerId.get()) {
				CODE_PROBE(true, "Blob migrator replaced in setPhase");
				TraceEvent("BlobMigratorReplaced").detail("Expected", owerId).detail("Current", currentOwnerId);
				throw blob_migrator_replaced();
			}
			if (phase > newPhase) {
				CODE_PROBE(true, "Blob migrator unexpected phase");
				TraceEvent("BlobMigratorUnexpectedPhase").detail("Phase", newPhase).detail("Current", phase);
				throw restore_error();
			}

			// update phase start timestamp
			if (phase != newPhase) {
				config.phaseStartTs().set(tr, newPhase, now());
				config.phase().set(tr, newPhase);
			}

			wait(tr->commit());
			TraceEvent("BlobRestoreSetPhase").detail("Phase", newPhase);
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Wait on restore phase change
ACTOR Future<Void> BlobRestoreController::onPhaseChange(Reference<BlobRestoreController> self,
                                                        BlobRestorePhase expectedPhase) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->db_));
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			BlobRestorePhase phase =
			    wait(BlobGranuleRestoreConfig().phase().getD(tr, Snapshot::False, BlobRestorePhase::UNINIT));
			if (expectedPhase == phase) {
				return Void();
			}

			state Future<Void> watch = BlobGranuleRestoreConfig().trigger.watch(tr);
			wait(tr->commit());
			wait(watch);
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Update restore progress
ACTOR Future<Void> BlobRestoreController::setProgress(Reference<BlobRestoreController> self,
                                                      int progress,
                                                      UID ownerId) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->db_));
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			UID currentOwner = wait(BlobGranuleRestoreConfig().lock().getD(tr));
			if (currentOwner != ownerId) {
				CODE_PROBE(true, "Blob migrator replaced in setProgress");
				TraceEvent("BlobMigratorReplaced").detail("ActiveId", currentOwner).detail("CallerId", ownerId);
				throw blob_migrator_replaced();
			}
			BlobGranuleRestoreConfig().progress().set(tr, progress);
			wait(tr->commit());
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Set lock owner for current restore. Owner will be checked for further updates of restore config
ACTOR Future<Void> BlobRestoreController::setLockOwner(Reference<BlobRestoreController> self, UID ownerId) {
	wait(runRYWTransaction(self->db_, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		BlobGranuleRestoreConfig().lock().set(tr, ownerId);
		return Void();
	}));
	return Void();
}

// Fail the restore with an error message
ACTOR Future<Void> BlobRestoreController::setError(Reference<BlobRestoreController> self, std::string errorMessage) {
	wait(runRYWTransaction(self->db_, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		BlobGranuleRestoreConfig config;
		config.phase().set(tr, BlobRestorePhase::ERROR);
		config.phaseStartTs().set(tr, BlobRestorePhase::ERROR, now());
		config.error().set(tr, errorMessage);
		return Void();
	}));
	return Void();
}
