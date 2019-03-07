#include "fdbserver/Status.h"
#include "flow/actorcompiler.h"
#include "fdbrpc/ContinuousSample.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "workloads.actor.h"
#include "BulkSetup.actor.h"
#include "fdbserver/ClusterRecruitmentInterface.h"
#include "fdbclient/ReadYourWrites.h"

#include <boost/lexical_cast.hpp>

#undef FLOW_ACOMPILER_STATE
#define FLOW_ACOMPILER_STATE 1

void getVersionAndnumTags(TraceEventFields md, Version& version, int& numTags) {
	version = -1;
	numTags = -1;

	// FIXME: sramamoorthy, WONTWORK
	// std::string versionStr = extractAttribute(msg.toString(), "Version");
	// version = strtol(versionStr.c_str(), nullptr, 0);
	// TraceEvent("version").detail("", version);
	sscanf(md.getValue("Version").c_str(), "%lld", &version);
	sscanf(md.getValue("numTags").c_str(), "%d:%d", &numTags);

	// std::string numTagsStr = extractAttribute(msg.toString(), "numTags");
	// numTags = strtol(numTagsStr.c_str(), nullptr, 0);
	// TraceEvent("numTags").detail("", numTags);
	// FIXME: sramamoorthy, WONTWORK
}

void getTagAndDurableVersion(TraceEventFields md, Version version, Tag& tag, Version& durableVersion) {
	Version verifyVersion;
	// FIXME: sramamoorthy, WONTWORK
	// tag = -1;
	durableVersion = -1;

	int tagLocality;
	int tagId;
	sscanf(md.getValue("version").c_str(), "%lld", &verifyVersion);
	sscanf(md.getValue("tag").c_str(), "%d:%d", &tagLocality, &tagId);
	tag.locality = tagLocality;
	tag.id = tagId;
	sscanf(md.getValue("durableVersion").c_str(), "%lld", &durableVersion);

	// FIXME: sramamoorthy, WONTWORK
	// std::string versionStr = extractAttribute(msg.toString(), "version");
	// verifyVersion = strtol(versionStr.c_str(), nullptr, 0);

	// TraceEvent("version compare").detail("version", version).detail("verifyVersion", verifyVersion);
	// if (version != verifyVersion) {
	//	return;
	// }

	// std::string tagStr = extractAttribute(msg.toString(), "tag");
	// tag = strtol(tagStr.c_str(), nullptr, 0);
	// TraceEvent("tagscan").detail("tag", tag);
	// if (tag == -1) {
	// 	return;
	// }

	// versionStr = extractAttribute(msg.toString(), "durableVersion");
	// durableVersion = strtol(versionStr.c_str(), nullptr, 0);

	// TraceEvent("durableVersion").detail("durablVersion", durableVersion);
	// FIXME: sramamoorthy, WONTWORK
}

void getMinAndMaxTLogVersions(TraceEventFields md, Version version, Tag tag, Version& minTLogVersion,
                              Version& maxTLogVersion) {
	Version verifyVersion;
	Tag verifyTag;
	minTLogVersion = maxTLogVersion = -1;

	sscanf(md.getValue("Version").c_str(), "%lld", &verifyVersion);
	int tagLocality;
	int tagId;
	sscanf(md.getValue("tag").c_str(), "%d:%d", &tagLocality, &tagId);
	verifyTag.locality = tagLocality;
	verifyTag.id = tagId;
	if (tag != verifyTag) {
		return;
	}
	sscanf(md.getValue("poppedTagVersion").c_str(), "%lld", &minTLogVersion);
	sscanf(md.getValue("queueCommittedVersion").c_str(), "%lld", &maxTLogVersion);

	// FIXME: sramamoorthy, WONTWORK
	// std::string versionStr = extractAttribute(msg.toString(), "Version");
	// verifyVersion = strtol(versionStr.c_str(), nullptr, 0);

	// if (version != verifyVersion) {
	//	return;
	// }

	// std::string tagStr = extractAttribute(msg.toString(), "Tag");
	// verifyTag = strtol(tagStr.c_str(), nullptr, 0);

	// if (tag != verifyTag) {
	//	return;
	// }

	// versionStr = extractAttribute(msg.toString(), "poppedTagVersion");
	// minTLogVersion = strtol(versionStr.c_str(), nullptr, 0);
	// versionStr = extractAttribute(msg.toString(), "queueCommittedVersion");
	// maxTLogVersion = strtol(versionStr.c_str(), nullptr, 0);
	// FIXME: sramamoorthy, WONTWORK
}

void filterEmptyMessages(std::vector<Future<TraceEventFields>>& messages) {
	// FIXME, sramamoorthy, FDB6 related
	// std::string emptyStr;
	// auto it = messages.begin();
	// while (it != messages.end()) {
	//	if (it->get() == emptyStr) {
	//		it = messages.erase(it);
	//	} else {
	//		++it;
	//	}
	// }
	return;
}

struct SnapTestWorkload : TestWorkload {
public: // variables
	int numSnaps; // num of snapshots to be taken
	              // FIXME: currently validation works on numSnap = 1
	double maxSnapDelay; // max delay before which a snapshot will be taken
	bool snapCheck; // check for the successful snap create
	int testID; // test id
	UID snapUID; // UID used for snap name

public: // ctor & dtor
	SnapTestWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), numSnaps(0), maxSnapDelay(0.0), snapCheck(false), testID(0), snapUID() {
		TraceEvent("SnapTestWorkload Constructor");
		std::string workloadName = "SnapTest";

		numSnaps = getOption(options, LiteralStringRef("numSnaps"), 0);
		maxSnapDelay = getOption(options, LiteralStringRef("maxSnapDelay"), 25.0);
		snapCheck = getOption(options, LiteralStringRef("snapCheck"), false);
		testID = getOption(options, LiteralStringRef("testID"), 0);
	}

public: // workload functions
	std::string description() override { return "SnapTest"; }
	Future<Void> setup(Database const& cx) override {
		TraceEvent("SnapTestWorkload setup");
		return Void();
	}
	Future<Void> start(Database const& cx) override {
		TraceEvent("SnapTestWorkload start");
		if (clientId == 0) {
			return _start(cx, this);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		// FIXME: sramamoorthy, FDB6 porting fallout
		if (true) return true;
		if (!this->snapCheck || clientId != 0) {
			TraceEvent("returning true here");
			return true;
		}
		switch (this->testID) {
		case 0:
		case 1:
		case 2:
		case 3: {
			TraceEvent("SnapTestWorkload check");
			Future<std::vector<WorkerInterface>> proxyIfaces;
			return (verifyExecTraceVersion(cx, this));
			break;
		}
		case 4: {
			std::string token = "disableTLogPopTimedOut";
			return verifyTLogTrackLatest(cx, this, token);
			break;
		}
		case 5: {
			std::string token = "tLogPopDisableEnableUidMismatch";
			return verifyTLogTrackLatest(cx, this, token);
			break;
		}
		case 6: {
			std::string token = "snapFailIgnorePopNotSet";
			return verifyTLogTrackLatest(cx, this, token);
			break;
		}
		case 7: {
			std::string token = "snapFailedDisableTLogUidMismatch";
			return verifyTLogTrackLatest(cx, this, token);
			break;
		}
		default: { break; }
		}
		return false;
	}

	void getMetrics(vector<PerfMetric>& m) override { TraceEvent("SnapTestWorkload getMetrics"); }

	ACTOR Future<Void> _create_keys(Database cx, std::string prefix, bool even = true) {
		state Transaction tr(cx);

		state int retry = 0;
		loop {
			tr.reset();
			try {
				for (int i = 0; i < 1000; i++) {
					int64_t id = g_random->randomInt64(0, INT64_MAX - 2);
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
			state double toDelay = fmod(g_random->randomUInt32(), self->maxSnapDelay);
			TraceEvent("ToDelay").detail("Value", toDelay);
			ASSERT(toDelay < self->maxSnapDelay);
			wait(delay(toDelay));

			state int retry = 0;
			loop {
				self->snapUID = g_random->randomUniqueID();
				try {
					StringRef snapCmdRef = LiteralStringRef("/bin/snap_create.sh");
					Future<Void> status = snapCreate(cx, snapCmdRef, self->snapUID);
					wait(status);
					break;
				} catch (Error& e) {
					++retry;
					TraceEvent(retry > 3 ? SevWarn : SevInfo, "snapCreate command failed").detail("error", e.what());
					if (retry > 3) {
						throw operation_failed();
					}
				}
			}
			TraceEvent("Snapshot create succeeded");
		} else if (self->testID == 2) {
			// create odd keys after the snapshot
			wait(self->_create_keys(cx, "snapKey", false /*even*/));
		} else if (self->testID == 3) {
			state KeySelector begin = firstGreaterOrEqual(normalKeys.begin);
			state KeySelector end = firstGreaterOrEqual(normalKeys.end);
			state int cnt = 0;
			// read the entire normalKeys range and look at keys prefixed
			// with snapKeys 1) validate that all key ids are even ie -
			// created before snap 2) values are same as the key id 3) # of
			// keys adds up to the total keys created before snap
			loop {
				tr.reset();
				try {
					Standalone<RangeResultRef> kvRange = wait(tr.getRange(begin, end, CLIENT_KNOBS->TOO_MANY));
					if (!kvRange.more && kvRange.size() == 0) {
						TraceEvent("No more entires");
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
				throw operation_failed();
			}
		} else if (self->testID == 4) {
			// description: if disable of a TLog pop was not followed by a
			// corresponding enable, then TLog will automatically enable the
			// popping of TLogs. this test case validates that we auto
			// enable the popping of TLogs
			loop {
				// disable pop of the TLog
				tr.reset();
				try {
					StringRef payLoadRef = LiteralStringRef("empty-binary:uid=test");
					tr.execute(execDisableTLogPop, payLoadRef);
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			// wait for 40 seconds and verify that the enabled pop happened
			// automatically
			wait(delay(40.0));
		} else if (self->testID == 5) {
			// description: disable TLog pop and enable TLog pop with
			// different UIDs should mis-match and print an error
			loop {
				// disable pop of the TLog
				tr.reset();
				try {
					StringRef payLoadRef = LiteralStringRef("empty-binary:uid=tmatch");
					tr.execute(execDisableTLogPop, payLoadRef);
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			loop {
				// enable pop of the TLog
				tr.reset();
				try {
					StringRef payLoadRef = LiteralStringRef("empty-binary:uid=didnotmatch");
					tr.execute(execEnableTLogPop, payLoadRef);
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		} else if (self->testID == 6) {
			// snapshot create without disabling pop of the TLog
			loop {
				try {
					tr.reset();
					StringRef snapPayload = LiteralStringRef("/bin/"
					                                         "snap_create.sh:uid=d78b08d47f341158e9a54d4baaf4a4dd");
					tr.execute(execSnap, snapPayload);
					wait(tr.commit());
					break;
				} catch (Error& e) {
					TraceEvent("snapCreate").detail("snapCreateErrorSnapTLogStorage", e.what());
					throw;
				}
			}
		} else if (self->testID == 7) {
			// disable popping of TLog and snapshot create with mis-matching
			loop {
				// disable pop of the TLog
				tr.reset();
				try {
					StringRef payLoadRef = LiteralStringRef("empty-binary:uid=tmatch");
					tr.execute(execDisableTLogPop, payLoadRef);
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			loop {
				// snap create with different UID
				try {
					tr.reset();
					StringRef snapPayload = LiteralStringRef("empty-binary:uid=ba61e9612a561d60bd83ad83e1b63568");
					tr.execute(execSnap, snapPayload);
					wait(tr.commit());
					break;
				} catch (Error& e) {
					TraceEvent("snapCreate").detail("snapCreateErrorSnapTLogStorage", e.what());
					throw;
				}
			}
		}
		TraceEvent("returning from start");
		wait(delay(0.0));
		return Void();
	}

	ACTOR Future<bool> verifyTLogTrackLatest(Database cx, SnapTestWorkload* self, std::string event) {
		TraceEvent("verifyTLogTrackLatest");
		state StringRef eventTokenRef(event);
		// FIXME: sramamoorthy, FDB6 related
		// state vector<WorkerInterface> tLogWorkers = wait(self->getWorkersWithRole(cx,
		// LocalityData::ClusterRole::TLog)); state vector<std::pair<WorkerInterface, ProcessClass>> tLogWorkers =
		// wait(self->dbInfo->get().clusterInterface.getWorkers());
		state vector<std::pair<WorkerInterface, ProcessClass>> tLogWorkers = wait(getWorkers(self->dbInfo));
		state std::vector<Future<TraceEventFields>> tLogMessages;

		state int i = 0;
		for (; i < tLogWorkers.size(); i++) {
			tLogMessages.push_back(
			    timeoutError(tLogWorkers[i].first.eventLogRequest.getReply(EventLogRequest(eventTokenRef)), 1.0));

			state int retryCnt = 0;
			state bool retry = false;
			loop {
				retry = false;
				try {
					TraceEvent("waiting for tlog messages");
					wait(waitForAll(tLogMessages));
					break;
				} catch (Error& e) {
					TraceEvent("verifyTLogTrackLatest")
					    .detail("token", eventTokenRef.toString())
					    .detail("Reason", "Failed to get tLogMessages")
					    .detail("code", e.what());
					if (e.code() != error_code_timed_out) {
						return false;
					} else {
						retry = true;
						++retryCnt;
					}
				}
				if (retryCnt >= 4) {
					TraceEvent("Unable to retrieve TLog messages");
					return false;
				}
			}
			filterEmptyMessages(tLogMessages);
			if (tLogMessages.size() != 1) {
				TraceEvent("verifyTLogTrackLatest message not found").detail("token", eventTokenRef.toString());
				return false;
			}
			tLogMessages.clear();
		}
		return true;
	}

	ACTOR Future<bool> verifyExecTraceVersion(Database cx, SnapTestWorkload* self) {
		TraceEvent("verifyExecTraceVersion1");

		// FIXME: sramamoorthy, FDB6
		// state std::vector<NetworkAddress> coordAddrs = self->getCoordinatorAddresses();
		state std::vector<NetworkAddress> coordAddrs = wait(getCoordinators(cx));
		TraceEvent("verifyExecTraceVersion2");
		state vector<std::pair<WorkerInterface, ProcessClass>> proxyWorkers = wait(getWorkers(self->dbInfo));
		TraceEvent("verifyExecTraceVersion3");
		state vector<std::pair<WorkerInterface, ProcessClass>> storageWorkers = wait(getWorkers(self->dbInfo));
		TraceEvent("verifyExecTraceVersion4");
		state vector<std::pair<WorkerInterface, ProcessClass>> tLogWorkers = wait(getWorkers(self->dbInfo));
		TraceEvent("verifyExecTraceVersion5");
		state vector<std::pair<WorkerInterface, ProcessClass>> workers = wait(getWorkers(self->dbInfo));
		TraceEvent("verifyExecTraceVersion6");

		state std::vector<Future<TraceEventFields>> proxyMessages;
		state std::vector<Future<TraceEventFields>> tLogMessages;
		state std::vector<Future<TraceEventFields>> storageMessages;
		state std::vector<Future<TraceEventFields>> coordMessages;
		state int numDurableVersionChecks = 0;
		state std::map<Tag, bool> visitedStorageTags;

		state int retryCnt = 0;
		loop {
			proxyMessages.clear();
			storageMessages.clear();
			coordMessages.clear();

			state bool retry = false;

			for (int i = 0; i < workers.size(); i++) {
				std::string eventToken = "ExecTrace/Coordinators/" + self->snapUID.toString();
				StringRef eventTokenRef(eventToken);
				coordMessages.push_back(
				    timeoutError(workers[i].first.eventLogRequest.getReply(EventLogRequest(eventTokenRef)), 1.0));
			}

			for (int i = 0; i < workers.size(); i++) {
				std::string eventToken = "ExecTrace/Proxy/" + self->snapUID.toString();
				StringRef eventTokenRef(eventToken);
				proxyMessages.push_back(
				    timeoutError(workers[i].first.eventLogRequest.getReply(EventLogRequest(eventTokenRef)), 1.0));
			}

			for (int i = 0; i < storageWorkers.size(); i++) {
				std::string eventToken = "ExecTrace/storage/" + self->snapUID.toString();
				StringRef eventTokenRef(eventToken);
				storageMessages.push_back(timeoutError(
				    storageWorkers[i].first.eventLogRequest.getReply(EventLogRequest(eventTokenRef)), 1.0));
			}

			TraceEvent("WAITING for proxy1");
			try {
				wait(waitForAll(proxyMessages));
				// wait(waitForAll(storageMessages));
				// wait(waitForAll(coordMessages));
			} catch (Error& e) {
				TraceEvent("verifyExecTraceVersionFailure")
				    .detail("Reason", "Failed to get proxy or storage messages")
				    .detail("code", e.what());
				if (e.code() != error_code_timed_out) {
					return false;
				} else {
					retry = true;
					++retryCnt;
				}
			}
			TraceEvent("WAITING for proxy2");
			if (retry == false) {
				break;
			}
			TraceEvent("WAITING for proxy3");

			if (retry && retryCnt >= 4) {
				TraceEvent("Unable to retrieve proxy/storage/coord messages "
				           "after retries");
				std::terminate();
				return false;
			}
		}

		// filter out empty messages
		filterEmptyMessages(proxyMessages);
		filterEmptyMessages(storageMessages);
		filterEmptyMessages(coordMessages);

		if (proxyMessages.size() != 1) {
			// if no message from proxy or more than one fail the check
			TraceEvent("No ExecTrace message from Proxy");
			std::terminate();
			return false;
		}

		TraceEvent("CoordinatorSnapStatus")
		    .detail("coordMessage size", coordMessages.size())
		    .detail("coordAddrssize", coordAddrs.size());
		if (coordMessages.size() < (coordAddrs.size() + 1) / 2) {
			TraceEvent("No ExecTrace message from Quorum of coordinators");
			std::terminate();
			return false;
		}

		state int i = 0;
		state int numTags = -1;

		for (; i < proxyMessages.size(); i++) {
			state Version execVersion = -1;
			state std::string emptyStr;

			TraceEvent("Printing Relevant ProxyMessage").detail("msg", proxyMessages[i].get().toString());
			// FIXME: sramamoorthy, how to compare with empty string
			if (proxyMessages[i].get().toString() != emptyStr) {
				getVersionAndnumTags(proxyMessages[i].get(), execVersion, numTags);
				ASSERT(numTags > 0);
			}
			state int j = 0;
			for (; (execVersion != -1) && j < storageMessages.size(); j++) {
				// for each message that has this verison, get the tag and
				// the durable version
				// FIXME: sramamoorthy, for now allow default values
				state Tag tag;
				state Tag invalidTag;
				// FIXME: sramamoorthy, for now allow default values
				state Version durableVersion = -1;
				TraceEvent("Printing Relevant StorageMessage").detail("msg", storageMessages[j].get().toString());
				// FIXME: sramamoorthy, how to compare with empty string
				ASSERT(storageMessages[j].get().toString() != emptyStr);
				getTagAndDurableVersion(storageMessages[j].get(), execVersion, tag, durableVersion);
				TraceEvent("Searching for tlog messages").detail("tag", tag.toString());

				retryCnt = 0;
				loop {
					retry = false;
					tLogMessages.clear();

					// for (int m = 0; (tag != -1) && m < tLogWorkers.size(); m++) {
					for (int m = 0; (tag != invalidTag) && m < tLogWorkers.size(); m++) {
						visitedStorageTags[tag] = true;
						std::string eventToken = "ExecTrace/TLog/" + tag.toString() + "/" + self->snapUID.toString();
						StringRef eventTokenRef(eventToken);
						tLogMessages.push_back(timeoutError(
						    tLogWorkers[m].first.eventLogRequest.getReply(EventLogRequest(eventTokenRef)), 1.0));
					}

					try {
						TraceEvent("waiting for tlog messages");
						if (tag != invalidTag) {
							wait(waitForAll(tLogMessages));
						}
					} catch (Error& e) {
						TraceEvent("verifyExecTraceVersionFailure")
						    .detail("Reason", "Failed to get tLogMessages")
						    .detail("code", e.what());
						if (e.code() != error_code_timed_out) {
							return false;
						} else {
							retry = true;
							++retryCnt;
						}
					}
					if (retry == false) {
						break;
					}
					if (retry && retryCnt > 4) {
						TraceEvent("Unable to retrieve tLog messages after "
						           "retries");
						std::terminate();
						return false;
					}
				}

				filterEmptyMessages(tLogMessages);

				state int k = 0;
				numDurableVersionChecks = 0;
				for (; (tag != invalidTag) && k < tLogMessages.size(); k++) {
					// for each of the message that has this version and tag
					// verify that the minVersioninTlog < durableVersion <
					// maxVersioninTlog
					Version minTLogVersion = -1;
					Version maxTLogVersion = -1;

					TraceEvent("tLogMessage").detail("msg", tLogMessages[k].get().toString());

					// FIXME, sramamoorthy, handle empty string
					ASSERT(tLogMessages[k].get().toString() != emptyStr);
					getMinAndMaxTLogVersions(tLogMessages[k].get(), execVersion, tag, minTLogVersion, maxTLogVersion);
					if (minTLogVersion != -1 && maxTLogVersion != -1) {
						if ((durableVersion > minTLogVersion) && (durableVersion < maxTLogVersion)) {
							++numDurableVersionChecks;
							TraceEvent("Successs!!!");
						}
					}
				}
				// if we did not find even one tlog for a given tag fail the
				// check
				if (numDurableVersionChecks < 1) {
					TraceEvent("No TLog found for a tag");
					std::terminate();
				}

				TraceEvent("next iteration");
				tLogMessages.clear();
			}
		}

		// validates that we encountered unique tags of value numTags
		if (numTags != visitedStorageTags.size()) {
			TraceEvent("Storage messages were not found");
			std::terminate();
			return false;
		}
		TraceEvent("Check Succeeded for verifyExecTraceVersion");
		return true;
	}
};

WorkloadFactory<SnapTestWorkload> SnapTestWorkloadFactory("SnapTest");
