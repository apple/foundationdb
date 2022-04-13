/*
 * BackupToBlob.actor.cpp
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

#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/workloads/BlobStoreWorkload.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct BackupToBlobWorkload : TestWorkload {
	double backupAfter;
	Key backupTag;
	Standalone<StringRef> backupURL;
	int initSnapshotInterval = 0;
	int snapshotInterval = 100000;

	static constexpr const char* DESCRIPTION = "BackupToBlob";

	BackupToBlobWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupAfter = getOption(options, LiteralStringRef("backupAfter"), 10.0);
		backupTag = getOption(options, LiteralStringRef("backupTag"), BackupAgentBase::getDefaultTag());
		auto backupURLString =
		    getOption(options, LiteralStringRef("backupURL"), LiteralStringRef("http://0.0.0.0:10000")).toString();
		auto accessKeyEnvVar =
		    getOption(options, LiteralStringRef("accessKeyVar"), LiteralStringRef("BLOB_ACCESS_KEY")).toString();
		auto secretKeyEnvVar =
		    getOption(options, LiteralStringRef("secretKeyVar"), LiteralStringRef("BLOB_SECRET_KEY")).toString();
		bool provideKeys = getOption(options, LiteralStringRef("provideKeys"), false);
		if (provideKeys) {
			updateBackupURL(backupURLString, accessKeyEnvVar, "<access_key>", secretKeyEnvVar, "<secret_key>");
		}
		backupURL = backupURLString;
	}

	std::string description() const override { return DESCRIPTION; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	ACTOR static Future<Void> _start(Database cx, BackupToBlobWorkload* self) {
		state FileBackupAgent backupAgent;
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;
		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);

		wait(delay(self->backupAfter));
		wait(backupAgent.submitBackup(cx,
		                              self->backupURL,
		                              {},
		                              self->initSnapshotInterval,
		                              self->snapshotInterval,
		                              self->backupTag.toString(),
		                              backupRanges));
		EBackupState backupStatus = wait(backupAgent.waitBackup(cx, self->backupTag.toString(), StopWhenDone::True));
		TraceEvent("BackupToBlob_BackupStatus").detail("Status", BackupAgentBase::getStateText(backupStatus));
		return Void();
	}

	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx, this); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<BackupToBlobWorkload> BackupToBlobWorkloadFactory(BackupToBlobWorkload::DESCRIPTION);
