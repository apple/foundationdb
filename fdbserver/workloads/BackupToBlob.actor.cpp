/*
 * BackupToBlob.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/BlobStoreWorkload.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/MockS3Server.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct BackupToBlobWorkload : TestWorkload {
	double backupAfter;
	Key backupTag;
	Standalone<StringRef> backupURL;
	int initSnapshotInterval = 0;
	int snapshotInterval = 100000;

	static constexpr auto NAME = "BackupToBlob";

	BackupToBlobWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupAfter = getOption(options, "backupAfter"_sr, 10.0);
		backupTag = getOption(options, "backupTag"_sr, BackupAgentBase::getDefaultTag());
		auto backupURLString = getOption(options, "backupURL"_sr, "http://0.0.0.0:10000"_sr).toString();
		auto accessKeyEnvVar = getOption(options, "accessKeyVar"_sr, "BLOB_ACCESS_KEY"_sr).toString();
		auto secretKeyEnvVar = getOption(options, "secretKeyVar"_sr, "BLOB_SECRET_KEY"_sr).toString();
		bool provideKeys = getOption(options, "provideKeys"_sr, false);
		if (provideKeys) {
			updateBackupURL(backupURLString, accessKeyEnvVar, "<access_key>", secretKeyEnvVar, "<secret_key>");
		}
		backupURL = backupURLString;
	}

	ACTOR static Future<Void> _setup(Database cx, BackupToBlobWorkload* self) {
		if (self->clientId == 0) {
			// Check if we're using a local mock server URL pattern
			bool useMockS3 = self->backupURL.toString().find("127.0.0.1") != std::string::npos ||
			                 self->backupURL.toString().find("localhost") != std::string::npos;

			if (useMockS3 && g_network->isSimulated()) {
				TraceEvent("BackupToBlob").detail("Phase", "Registering MockS3Server").detail("URL", self->backupURL);

				// Register MockS3Server with IP address - simulation environment doesn't support hostname resolution.
				wait(g_simulator->registerSimHTTPServer("127.0.0.1", "8080", makeReference<MockS3RequestHandler>()));

				TraceEvent("BackupToBlob")
				    .detail("Phase", "MockS3Server Registered")
				    .detail("Address", "127.0.0.1:8080");
			}
		}
		return Void();
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	ACTOR static Future<Void> _start(Database cx, BackupToBlobWorkload* self) {
		state FileBackupAgent backupAgent;
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;

		addDefaultBackupRanges(backupRanges);

		wait(delay(self->backupAfter));
		wait(backupAgent.submitBackup(cx,
		                              self->backupURL,
		                              {},
		                              self->initSnapshotInterval,
		                              self->snapshotInterval,
		                              self->backupTag.toString(),
		                              backupRanges,
		                              true));
		EBackupState backupStatus = wait(backupAgent.waitBackup(cx, self->backupTag.toString(), StopWhenDone::True));
		TraceEvent("BackupToBlob_BackupStatus").detail("Status", BackupAgentBase::getStateText(backupStatus));
		return Void();
	}

	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx, this); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<BackupToBlobWorkload> BackupToBlobWorkloadFactory;
