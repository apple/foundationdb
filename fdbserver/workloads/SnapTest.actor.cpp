#include <boost/lexical_cast.hpp>
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/ContinuousSample.h"
#include "fdbmonitor/SimpleIni.h"
#include "fdbserver/ClusterRecruitmentInterface.h"
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
	Version verifyVersion;
	durableVersion = -1;

	verifyVersion = boost::lexical_cast<int64_t>(md.getValue("Version"));
	std::string tagString = md.getValue("Tag");
	int colon = tagString.find_first_of(':');
	std::string localityString = tagString.substr(0, colon);
	std::string idString = tagString.substr(colon + 1);
	tag.locality = boost::lexical_cast<int>(localityString);
	tag.id = boost::lexical_cast<int>(idString);

	durableVersion = boost::lexical_cast<int64_t>(md.getValue("DurableVersion"));
}

void getMinAndMaxTLogVersions(TraceEventFields md, Version version, Tag tag, Version& minTLogVersion,
                              Version& maxTLogVersion) {
	Version verifyVersion;
	Tag verifyTag;
	minTLogVersion = maxTLogVersion = -1;

	verifyVersion = boost::lexical_cast<int64_t>(md.getValue("Version"));
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
	messages.erase(std::remove_if(messages.begin(), messages.end(),
								  [](Future<TraceEventFields>const & msgFuture)
								  {
									  return !msgFuture.isReady() || msgFuture.get().size() == 0;
								  }
					   ), messages.end());
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
	int snapVersion; // snapVersion to invoke

public: // ctor & dtor
	SnapTestWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), numSnaps(0), maxSnapDelay(0.0), testID(0), snapUID() {
		TraceEvent("SnapTestWorkload Constructor");
		std::string workloadName = "SnapTest";
		maxRetryCntToRetrieveMessage = 10;

		numSnaps = getOption(options, LiteralStringRef("numSnaps"), 0);
		maxSnapDelay = getOption(options, LiteralStringRef("maxSnapDelay"), 25.0);
		testID = getOption(options, LiteralStringRef("testID"), 0);
		restartInfoLocation =
		    getOption(options, LiteralStringRef("restartInfoLocation"), LiteralStringRef("simfdb/restartInfo.ini"))
		        .toString();
		skipCheck = false;
		snapVersion = getOption(options, LiteralStringRef("version"), 1);
	}

public: // workload functions
	std::string description() override { return "SnapTest"; }
	Future<Void> setup(Database const& cx) override {
		TraceEvent("SnapTestWorkloadSetup");
		return Void();
	}
	Future<Void> start(Database const& cx) override {
		TraceEvent("SnapTestWorkloadStart");
		if (clientId == 0) {
			return _start(cx, this);
		}
		return Void();
	}

	ACTOR Future<bool> _check(Database cx, SnapTestWorkload* self) {
		if (self->skipCheck) {
			TraceEvent(SevWarnAlways, "SnapCheckIgnored");
			return true;
		}
		state Transaction tr(cx);
		// read the key SnapFailedTLog.$UID
		loop {
			try {
				Standalone<StringRef> keyStr = snapTestFailStatus.withSuffix(StringRef(self->snapUID.toString()));
				TraceEvent("TestKeyStr").detail("Value", keyStr);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Optional<Value> val = wait(tr.get(keyStr));
				if (val.present()) {
					break;
				}
				// wait for the key to be written out by TLogs
				wait(delay(0.1));
			} catch (Error &e) {
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

	void getMetrics(vector<PerfMetric>& m) override { TraceEvent("SnapTestWorkloadGetMetrics"); }

	ACTOR Future<Void> snapExecHelper(SnapTestWorkload* self, Database cx, StringRef keyRef, StringRef valueRef) {
		state Transaction tr(cx);
		state int retry = 0;
		loop {
			try {
				tr.execute(keyRef, valueRef);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				++retry;
				if (e.code() == error_code_txn_exec_log_anti_quorum) {
					self->skipCheck = true;
					break;

				}
				if (e.code() == error_code_cluster_not_fully_recovered) {
					TraceEvent(SevWarnAlways, "ClusterNotFullyRecovered")
						.error(e);
					if (retry > 10) {
						self->skipCheck = true;
						break;
					}
				}
			}
		}
		return Void();
	}

	ACTOR Future<Void> _create_keys(Database cx, std::string prefix, bool even = true) {
		state Transaction tr(cx);
		state vector<int64_t> keys;

		for (int i = 0; i < 1000; i++) {
			keys.push_back(deterministicRandom()->randomInt64(0, INT64_MAX - 2));
		}

		state int retry = 0;
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
					tr.set(key1Ref, val1Ref, false);
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
			state bool snapFailed = false;
			loop {
				self->snapUID = deterministicRandom()->randomUniqueID();
				try {
					StringRef snapCmdRef = LiteralStringRef("/bin/snap_create.sh");
					Future<Void> status = snapCreate(cx, snapCmdRef, self->snapUID, self->snapVersion);
					wait(status);
					break;
				} catch (Error& e) {
					if (e.code() == error_code_txn_exec_log_anti_quorum) {
						snapFailed = true;
						break;
					}
					if (e.code() == error_code_cluster_not_fully_recovered) {
						++retry;
						if (retry > 10) {
							snapFailed = true;
							break;
						}
					}
					if (self->snapVersion == 2) {
						// snap v2 can fail for many reasons, so retry for 5 times and then fail it
						if (retry > 5) {
							snapFailed = true;
							break;
						}
					}
				}
			}
			CSimpleIni ini;
			ini.SetUnicode();
			ini.LoadFile(self->restartInfoLocation.c_str());
			std::string uidStr = self->snapUID.toString();
			ini.SetValue("RESTORE", "RestoreSnapUID", uidStr.c_str());
			ini.SetValue("RESTORE", "BackupFailed", format("%d", snapFailed).c_str());
			ini.SetValue("RESTORE", "BackupVersion", format("%d", self->snapVersion).c_str());
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
				TraceEvent(SevWarnAlways, "BackupFailedSkippingRestoreCheck");
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
					Standalone<RangeResultRef> kvRange = wait(tr.getRange(begin, end, 1000));
					if (!kvRange.more && kvRange.size() == 0) {
						TraceEvent("SnapTestNoMoreEntries");
						break;
					}

					for (int i = 0; i < kvRange.size(); i++) {
						if (kvRange[i].key.startsWith(LiteralStringRef("snapKey"))) {
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
			// description: if disable of a TLog pop was not followed by a
			// corresponding enable, then TLog will automatically enable the
			// popping of TLogs. this test case validates that we auto
			// enable the popping of TLogs
			state Standalone<StringRef> payLoadRef = LiteralStringRef("empty-binary:uid=a36b2ca0e8dab0452ac3e12b6b926f4b");
			wait(self->snapExecHelper(self, cx, execDisableTLogPop, payLoadRef));
		} else if (self->testID == 5) {
			// snapshot create without disabling pop of the TLog
			StringRef uidStr = LiteralStringRef("d78b08d47f341158e9a54d4baaf4a4dd");
			self->snapUID = UID::fromString(uidStr.toString());
			state Standalone<StringRef> snapPayload = LiteralStringRef("/bin/"
														"snap_create.sh:uid=").withSuffix(uidStr);
			wait(self->snapExecHelper(self, cx, execSnap, snapPayload));
		} else if (self->testID == 6) {
			// disable popping of TLog and snapshot create with mis-matching
			payLoadRef = LiteralStringRef("empty-binary:uid=f49d27ddf7a28b6549d930743e0ebdbe");
			wait(self->snapExecHelper(self, cx, execDisableTLogPop, payLoadRef));
			if (self->skipCheck) {
				return Void();
			}

			StringRef uidStr = LiteralStringRef("ba61e9612a561d60bd83ad83e1b63568");
			self->snapUID = UID::fromString(uidStr.toString());
			snapPayload = LiteralStringRef("/bin/snap_create.sh:uid=").withSuffix(uidStr);
			wait(self->snapExecHelper(self, cx, execSnap, snapPayload));
		} else if (self->testID == 7) {
			// create a snapshot with a non whitelisted binary path and operation
			// should fail
			state bool testedFailure = false;
			snapFailed = false;
			loop {
				self->snapUID = deterministicRandom()->randomUniqueID();
				try {
					StringRef snapCmdRef = LiteralStringRef("/bin/snap_create1.sh");
					Future<Void> status = snapCreate(cx, snapCmdRef, self->snapUID, self->snapVersion);
					wait(status);
					break;
				} catch (Error& e) {
					if (e.code() == error_code_cluster_not_fully_recovered ||
						e.code() == error_code_txn_exec_log_anti_quorum) {
						snapFailed = true;
						break;
					}
					if (e.code() == error_code_transaction_not_permitted) {
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
