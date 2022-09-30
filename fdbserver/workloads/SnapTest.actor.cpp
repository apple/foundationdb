/*
 * SnapTest.actor.cpp
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

#include <boost/lexical_cast.hpp>
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/ContinuousSample.h"
#include "fdbclient/SimpleIni.h"
#include "fdbserver/Status.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"

void getVersionAndnumTags(TraceEventFields md, Version& version, int& numTags) {
	version = -1;
	numTags = -1;

	version = boost::lexical_cast<int64_t>(md.getValue("Version"));
	numTags = boost::lexical_cast<int>(md.getValue("NumTags"));
}

void getTagAndDurableVersion(TraceEventFields md, Version version, Tag& tag, Version& durableVersion) {
	durableVersion = -1;

	// verify version:
	boost::lexical_cast<int64_t>(md.getValue("Version"));
	std::string tagString = md.getValue("Tag");
	int colon = tagString.find_first_of(':');
	std::string localityString = tagString.substr(0, colon);
	std::string idString = tagString.substr(colon + 1);
	tag.locality = boost::lexical_cast<int>(localityString);
	tag.id = boost::lexical_cast<int>(idString);

	durableVersion = boost::lexical_cast<int64_t>(md.getValue("DurableVersion"));
}

void getMinAndMaxTLogVersions(TraceEventFields md,
                              Version version,
                              Tag tag,
                              Version& minTLogVersion,
                              Version& maxTLogVersion) {
	Tag verifyTag;
	minTLogVersion = maxTLogVersion = -1;

	// verify version:
	boost::lexical_cast<int64_t>(md.getValue("Version"));
	std::string tagString = md.getValue("Tag");
	int colon = tagString.find_first_of(':');
	std::string localityString = tagString.substr(0, colon);
	std::string idString = tagString.substr(colon + 1);
	verifyTag.locality = boost::lexical_cast<int>(localityString);
	verifyTag.id = boost::lexical_cast<int>(idString);
	if (tag != verifyTag) {
		return;
	}
	minTLogVersion = boost::lexical_cast<int64_t>(md.getValue("PoppedTagVersion"));
	maxTLogVersion = boost::lexical_cast<int64_t>(md.getValue("QueueCommittedVersion"));
}

void filterEmptyMessages(std::vector<Future<TraceEventFields>>& messages) {
	messages.erase(std::remove_if(messages.begin(),
	                              messages.end(),
	                              [](Future<TraceEventFields> const& msgFuture) {
		                              return !msgFuture.isReady() || msgFuture.get().size() == 0;
	                              }),
	               messages.end());
	return;
}

void printMessages(std::vector<Future<TraceEventFields>>& messages) {
	for (int i = 0; i < messages.size(); i++) {
		TraceEvent("SnapTestMessages").detail("I", i).detail("Value", messages[i].get().toString());
	}
	return;
}

struct SnapTestWorkload : TestWorkload {
public: // variables
	int numSnaps; // num of snapshots to be taken
	              // FIXME: currently validation works on numSnap = 1
	double maxSnapDelay; // max delay before which a snapshot will be taken
	int testID; // test id
	UID snapUID; // UID used for snap name
	std::string restartInfoLocation; // file location to store the snap restore info
	int maxRetryCntToRetrieveMessage; // number of retires to do trackLatest
	bool skipCheck; // disable check if the exec fails
	int retryLimit; // -1 if no limit

public: // ctor & dtor
	SnapTestWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), numSnaps(0), maxSnapDelay(0.0), testID(0), snapUID() {
		TraceEvent("SnapTestWorkloadConstructor").log();
		std::string workloadName = "SnapTest";
		maxRetryCntToRetrieveMessage = 10;

		numSnaps = getOption(options, "numSnaps"_sr, 0);
		maxSnapDelay = getOption(options, "maxSnapDelay"_sr, 25.0);
		testID = getOption(options, "testID"_sr, 0);
		restartInfoLocation = getOption(options, "restartInfoLocation"_sr, "simfdb/restartInfo.ini"_sr).toString();
		skipCheck = false;
		retryLimit = getOption(options, "retryLimit"_sr, 5);
		g_simulator->allowLogSetKills = false;
	}

public: // workload functions
	std::string description() const override { return "SnapTest"; }
	Future<Void> setup(Database const& cx) override {
		TraceEvent("SnapTestWorkloadSetup").log();
		return Void();
	}
	Future<Void> start(Database const& cx) override {
		TraceEvent("SnapTestWorkloadStart").log();
		if (clientId == 0) {
			return _start(cx, this);
		}
		return Void();
	}

	ACTOR Future<bool> _check(Database cx, SnapTestWorkload* self) {
		if (self->skipCheck) {
			TraceEvent(SevWarnAlways, "SnapCheckIgnored").log();
			return true;
		}
		state Transaction tr(cx);
		// read the key SnapFailedTLog.$UID
		loop {
			try {
				Standalone<StringRef> keyStr =
				    "\xff/SnapTestFailStatus/"_sr.withSuffix(StringRef(self->snapUID.toString()));
				TraceEvent("TestKeyStr").detail("Value", keyStr);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Optional<Value> val = wait(tr.get(keyStr));
				if (val.present()) {
					break;
				}
				// wait for the key to be written out by TLogs
				wait(delay(0.1));
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return true;
	}

	Future<bool> check(Database const& cx) override {
		TraceEvent("SnapTestWorkloadCheck").detail("ClientID", clientId);
		if (clientId != 0) {
			return true;
		}
		if (this->testID != 5 && this->testID != 6) {
			return true;
		}
		return _check(cx, this);
	}

	void getMetrics(std::vector<PerfMetric>& m) override { TraceEvent("SnapTestWorkloadGetMetrics"); }

	ACTOR Future<Void> _create_keys(Database cx, std::string prefix, bool even = true) {
		state Transaction tr(cx);
		state std::vector<int64_t> keys;

		keys.reserve(1000);
		for (int i = 0; i < 1000; i++) {
			keys.push_back(deterministicRandom()->randomInt64(0, INT64_MAX - 2));
		}

		tr.reset();
		loop {
			try {
				for (auto id : keys) {
					if (even) {
						if (id % 2 != 0) {
							id++;
						}
					} else {
						if (id % 2 == 0) {
							id++;
						}
					}
					std::string Key1 = prefix + std::to_string(id);
					Key key1Ref(Key1);
					std::string Val1 = std::to_string(id);
					Value val1Ref(Val1);
					tr.set(key1Ref, val1Ref, AddConflictRange::False);
				}
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> _start(Database cx, SnapTestWorkload* self) {
		state Transaction tr(cx);
		state bool snapFailed = false;

		if (self->testID == 0) {
			// create even keys before the snapshot
			wait(self->_create_keys(cx, "snapKey"));
		} else if (self->testID == 1) {
			// create a snapshot
			state double toDelay = fmod(deterministicRandom()->randomUInt32(), self->maxSnapDelay);
			TraceEvent("ToDelay").detail("Value", toDelay);
			ASSERT(toDelay < self->maxSnapDelay);
			wait(delay(toDelay));

			state int retry = 0;
			loop {
				self->snapUID = deterministicRandom()->randomUniqueID();
				try {
					StringRef snapCmdRef = "/bin/snap_create.sh"_sr;
					Future<Void> status = snapCreate(cx, snapCmdRef, self->snapUID);
					wait(status);
					break;
				} catch (Error& e) {
					if (e.code() == error_code_snap_log_anti_quorum_unsupported) {
						snapFailed = true;
						break;
					}
					TraceEvent("SnapCreateError").error(e);
					++retry;
					// snap v2 can fail for many reasons, so retry for 5 times and then fail it
					if (self->retryLimit != -1 && retry > self->retryLimit) {
						snapFailed = true;
						break;
					}
					wait(delay(5.0));
				}
			}
			CSimpleIni ini;
			ini.SetUnicode();
			ini.LoadFile(self->restartInfoLocation.c_str());
			std::string uidStr = self->snapUID.toString();
			ini.SetValue("RESTORE", "RestoreSnapUID", uidStr.c_str());
			ini.SetValue("RESTORE", "BackupFailed", format("%d", snapFailed).c_str());
			ini.SaveFile(self->restartInfoLocation.c_str());
			// write the snapUID to a file
			TraceEvent("SnapshotCreateStatus").detail("Status", !snapFailed ? "Success" : "Failure");
		} else if (self->testID == 2) {
			// create odd keys after the snapshot
			wait(self->_create_keys(cx, "snapKey", false /*even*/));
		} else if (self->testID == 3) {
			CSimpleIni ini;
			ini.SetUnicode();
			ini.LoadFile(self->restartInfoLocation.c_str());
			bool backupFailed = atoi(ini.GetValue("RESTORE", "BackupFailed"));
			if (backupFailed) {
				// since backup failed, skip the restore checking
				TraceEvent(SevWarnAlways, "BackupFailedSkippingRestoreCheck").log();
				return Void();
			}
			state KeySelector begin = firstGreaterOrEqual(normalKeys.begin);
			state KeySelector end = firstGreaterOrEqual(normalKeys.end);
			state int cnt = 0;
			// read the entire normalKeys range and look at keys prefixed
			// with snapKeys 1) validate that all key ids are even ie -
			// created before snap 2) values are same as the key id 3) # of
			// keys adds up to the total keys created before snap
			tr.reset();
			loop {
				try {
					RangeResult kvRange = wait(tr.getRange(begin, end, 1000));
					if (!kvRange.more && kvRange.size() == 0) {
						TraceEvent("SnapTestNoMoreEntries").log();
						break;
					}

					for (int i = 0; i < kvRange.size(); i++) {
						if (kvRange[i].key.startsWith("snapKey"_sr)) {
							std::string tmp1 = kvRange[i].key.substr(7).toString();
							int64_t id = strtol(tmp1.c_str(), nullptr, 0);
							if (id % 2 != 0) {
								throw operation_failed();
							}
							++cnt;
							std::string tmp2 = kvRange[i].value.toString();
							int64_t value = strtol(tmp2.c_str(), nullptr, 0);
							if (id != value) {
								throw operation_failed();
							}
						}
					}
					begin = firstGreaterThan(kvRange.end()[-1].key);
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			if (cnt != 1000) {
				TraceEvent(SevError, "SnapTestVerifyCntValue").detail("Value", cnt);
				throw operation_failed();
			}
		} else if (self->testID == 4) {
			// create a snapshot with a non whitelisted binary path and operation
			// should fail
			state bool testedFailure = false;
			snapFailed = false;
			loop {
				self->snapUID = deterministicRandom()->randomUniqueID();
				try {
					StringRef snapCmdRef = "/bin/snap_create1.sh"_sr;
					Future<Void> status = snapCreate(cx, snapCmdRef, self->snapUID);
					wait(status);
					break;
				} catch (Error& e) {
					if (e.code() == error_code_snap_not_fully_recovered_unsupported ||
					    e.code() == error_code_snap_log_anti_quorum_unsupported) {
						snapFailed = true;
						break;
					}
					if (e.code() == error_code_snap_path_not_whitelisted) {
						testedFailure = true;
						break;
					}
				}
			}
			ASSERT(testedFailure || snapFailed);
		}
		wait(delay(0.0));
		return Void();
	}
};

WorkloadFactory<SnapTestWorkload> SnapTestWorkloadFactory("SnapTest");
