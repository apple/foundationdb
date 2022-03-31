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

#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/workloads/BlobStoreWorkload.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RestoreFromBlobWorkload : TestWorkload {
	double restoreAfter;
	Key backupTag;
	Standalone<StringRef> backupURL;
	WaitForComplete waitForComplete{ false };

	static constexpr const char* DESCRIPTION = "RestoreFromBlob";

	RestoreFromBlobWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		restoreAfter = getOption(options, LiteralStringRef("restoreAfter"), 10.0);
		backupTag = getOption(options, LiteralStringRef("backupTag"), BackupAgentBase::getDefaultTag());
		auto backupURLString =
		    getOption(options, LiteralStringRef("backupURL"), LiteralStringRef("http://0.0.0.0:10000")).toString();
		auto accessKeyEnvVar =
		    getOption(options, LiteralStringRef("accessKeyVar"), LiteralStringRef("BLOB_ACCESS_KEY")).toString();
		auto secretKeyEnvVar =
		    getOption(options, LiteralStringRef("secretKeyVar"), LiteralStringRef("BLOB_SECRET_KEY")).toString();
		bool provideKeys = getOption(options, LiteralStringRef("provideKeys"), false);
		waitForComplete.set(getOption(options, LiteralStringRef("waitForComplete"), true));
		if (provideKeys) {
			updateBackupURL(backupURLString, accessKeyEnvVar, "<access_key>", secretKeyEnvVar, "<secret_key>");
		}
		backupURL = backupURLString;
	}

	std::string description() const override { return DESCRIPTION; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	ACTOR static Future<Void> _start(Database cx, RestoreFromBlobWorkload* self) {
		state FileBackupAgent backupAgent;
		state Standalone<VectorRef<KeyRangeRef>> restoreRanges;
		restoreRanges.push_back_deep(restoreRanges.arena(), normalKeys);

		wait(delay(self->restoreAfter));
		Version v = wait(
		    backupAgent.restore(cx, {}, self->backupTag, self->backupURL, {}, restoreRanges, self->waitForComplete));
		return Void();
	}

	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx, this); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RestoreFromBlobWorkload> RestoreFromBlobWorkloadFactory(RestoreFromBlobWorkload::DESCRIPTION);
