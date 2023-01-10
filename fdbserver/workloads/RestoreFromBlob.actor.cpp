/*
 * RestoreFromBlob.actor.cpp
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/BlobStoreWorkload.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RestoreFromBlobWorkload : TestWorkload {
	double restoreAfter;
	Key backupTag;
	Standalone<StringRef> backupURL;
	WaitForComplete waitForComplete{ false };

	static constexpr auto NAME = "RestoreFromBlob";

	RestoreFromBlobWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		restoreAfter = getOption(options, "restoreAfter"_sr, 10.0);
		backupTag = getOption(options, "backupTag"_sr, BackupAgentBase::getDefaultTag());
		auto backupURLString = getOption(options, "backupURL"_sr, "http://0.0.0.0:10000"_sr).toString();
		auto accessKeyEnvVar = getOption(options, "accessKeyVar"_sr, "BLOB_ACCESS_KEY"_sr).toString();
		auto secretKeyEnvVar = getOption(options, "secretKeyVar"_sr, "BLOB_SECRET_KEY"_sr).toString();
		bool provideKeys = getOption(options, "provideKeys"_sr, false);
		waitForComplete.set(getOption(options, "waitForComplete"_sr, true));
		if (provideKeys) {
			updateBackupURL(backupURLString, accessKeyEnvVar, "<access_key>", secretKeyEnvVar, "<secret_key>");
		}
		backupURL = backupURLString;
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	ACTOR static Future<Void> _start(Database cx, RestoreFromBlobWorkload* self) {
		state FileBackupAgent backupAgent;
		state DatabaseConfiguration config = wait(getDatabaseConfiguration(cx));

		wait(delay(self->restoreAfter));
		if (config.encryptionAtRestMode.isEncryptionEnabled()) {
			// restore system keys followed by user keys
			wait(success(backupAgent.restore(
			    cx, {}, self->backupTag, self->backupURL, {}, getSystemBackupRanges(), self->waitForComplete)));
			Standalone<VectorRef<KeyRangeRef>> restoreRanges;
			restoreRanges.push_back_deep(restoreRanges.arena(), normalKeys);
			wait(success(backupAgent.restore(
			    cx, {}, self->backupTag, self->backupURL, {}, restoreRanges, self->waitForComplete)));
		} else {
			Standalone<VectorRef<KeyRangeRef>> restoreRanges;
			addDefaultBackupRanges(restoreRanges);
			wait(success(backupAgent.restore(
			    cx, {}, self->backupTag, self->backupURL, {}, restoreRanges, self->waitForComplete)));
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx, this); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RestoreFromBlobWorkload> RestoreFromBlobWorkloadFactory;
