/*
 * Coordination.actor.cpp
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

#include <cstdint>

#include "fdbserver/coordinator/CoordinationServer.h"
#include "fdbserver/core/IKeyValueStore.h"
#include "fdbserver/core/Knobs.h"
#include "OnDemandStore.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "flow/ActorCollection.h"
#include "flow/ProtocolVersion.h"
#include "flow/UnitTest.h"
#include "flow/IndexedSet.h"
#include "flow/genericactors.actor.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/network.h"

#include "flow/CoroUtils.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// This module implements coordinationServer() plus the interfaces in CoordinationInterface.h

namespace {

const std::string fileCoordinatorPrefix = "coordination-";

} // namespace

class LivenessChecker {
	double threshold;
	AsyncVar<double> lastTime;
	static Future<Void> checkStuck(LivenessChecker const* self) {
		while (true) {
			auto res = co_await race(delayUntil(self->lastTime.get() + self->threshold), self->lastTime.onChange());
			if (res.index() == 0) {
				co_return;
			}
		}
	}

public:
	explicit LivenessChecker(double threshold) : threshold(threshold), lastTime(now()) {}

	void confirmLiveness() { lastTime.set(now()); }

	Future<Void> checkStuck() const { return checkStuck(this); }
};

struct GenerationRegVal {
	UniqueGeneration readGen, writeGen;
	Optional<Value> val;

	// To change this serialization, ProtocolVersion::GenerationRegVal must be updated, and downgrades need to be
	// considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, readGen, writeGen, val);
	}
};

class LocalGenerationReg {
public:
	LocalGenerationReg(GenerationRegInterface interf, OnDemandStore* pstore)
	  : readReqs(interf.read.getFuture()), writeReqs(interf.write.getFuture()), pStore(pstore),
	    storeLock(new FlowLock(1)) {}

	Future<Void> run() {
		return serveReadReqs(readReqs, pStore, storeLock) || serveWriteReqs(writeReqs, pStore, storeLock);
	}

private:
	static Future<Void> serveReadReqs(FutureStream<GenerationRegReadRequest> readReqs,
	                                  OnDemandStore* pstore,
	                                  Reference<FlowLock> storeLock) {
		OnDemandStore& store = *pstore;
		while (true) {
			GenerationRegReadRequest req = co_await readReqs;
			TraceEvent("GenerationRegReadRequest")
			    .detail("From", req.reply.getEndpoint().getPrimaryAddress())
			    .detail("K", req.key);
			// SOMEDAY: concurrent access to different keys?
			co_await storeLock->take();
			FlowLock::Releaser storeLockReleaser(*storeLock);
			Optional<Value> rawV = co_await store->readValue(req.key);
			GenerationRegVal v = rawV.present()
			                         ? BinaryReader::fromStringRef<GenerationRegVal>(rawV.get(), IncludeVersion())
			                         : GenerationRegVal();
			TraceEvent("GenerationRegReadReply")
			    .detail("RVSize", rawV.present() ? rawV.get().size() : -1)
			    .detail("VWG", v.writeGen.generation);
			if (v.readGen < req.gen) {
				v.readGen = req.gen;
				store->set(KeyValueRef(
				    req.key, BinaryWriter::toValue(v, IncludeVersion(ProtocolVersion::withGenerationRegVal()))));
				co_await store->commit();
			}
			req.reply.send(GenerationRegReadReply(v.val, v.writeGen, v.readGen));
		}
	}

	static Future<Void> serveWriteReqs(FutureStream<GenerationRegWriteRequest> writeReqs,
	                                   OnDemandStore* pstore,
	                                   Reference<FlowLock> storeLock) {
		OnDemandStore& store = *pstore;
		while (true) {
			GenerationRegWriteRequest wrq = co_await writeReqs;
			// SOMEDAY: concurrent access to different keys?
			co_await storeLock->take();
			FlowLock::Releaser storeLockReleaser(*storeLock);
			Optional<Value> rawV = co_await store->readValue(wrq.kv.key);
			GenerationRegVal v = rawV.present()
			                         ? BinaryReader::fromStringRef<GenerationRegVal>(rawV.get(), IncludeVersion())
			                         : GenerationRegVal();
			if (v.readGen <= wrq.gen && v.writeGen < wrq.gen) {
				v.writeGen = wrq.gen;
				v.val = wrq.kv.value;
				store->set(KeyValueRef(
				    wrq.kv.key, BinaryWriter::toValue(v, IncludeVersion(ProtocolVersion::withGenerationRegVal()))));
				co_await store->commit();
				TraceEvent("GenerationRegWrote")
				    .detail("From", wrq.reply.getEndpoint().getPrimaryAddress())
				    .detail("Key", wrq.kv.key)
				    .detail("ReqGen", wrq.gen.generation)
				    .detail("Returning", v.writeGen.generation);
				wrq.reply.send(v.writeGen);
			} else {
				TraceEvent("GenerationRegWriteFail")
				    .detail("From", wrq.reply.getEndpoint().getPrimaryAddress())
				    .detail("Key", wrq.kv.key)
				    .detail("ReqGen", wrq.gen.generation)
				    .detail("ReadGen", v.readGen.generation)
				    .detail("WriteGen", v.writeGen.generation);
				wrq.reply.send(std::max(v.readGen, v.writeGen));
			}
		}
	}

	FutureStream<GenerationRegReadRequest> readReqs;
	FutureStream<GenerationRegWriteRequest> writeReqs;
	OnDemandStore* pStore;
	Reference<FlowLock> storeLock;
};

TEST_CASE("/fdbserver/Coordination/localGenerationReg/simple") {
	state GenerationRegInterface reg;
	state OnDemandStore store(params.getDataDir(), deterministicRandom()->randomUniqueID(), fileCoordinatorPrefix);
	LocalGenerationReg generationReg(reg, &store);
	state Future<Void> actor = generationReg.run();
	state Key the_key(deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(0, 10)));

	state UniqueGeneration firstGen(0, deterministicRandom()->randomUniqueID());

	{
		GenerationRegReadReply r = wait(reg.read.getReply(GenerationRegReadRequest(the_key, firstGen)));
		//   If there was no prior write(_,_,0) or a data loss fault,
		//     returns (Optional(),0,gen2)
		ASSERT(!r.value.present());
		ASSERT(r.gen == UniqueGeneration());
		ASSERT(r.rgen == firstGen);
	}

	{
		UniqueGeneration g =
		    wait(reg.write.getReply(GenerationRegWriteRequest(KeyValueRef(the_key, "Value1"_sr), firstGen)));
		//   (gen1==gen is considered a "successful" write)
		ASSERT(g == firstGen);
	}

	{
		GenerationRegReadReply r = wait(reg.read.getReply(GenerationRegReadRequest(the_key, UniqueGeneration())));
		// read(key,gen2) returns (value,gen,rgen).
		//     There was some earlier or concurrent write(key,value,gen).
		ASSERT(r.value == "Value1"_sr);
		ASSERT(r.gen == firstGen);
		//     There was some earlier or concurrent read(key,rgen).
		ASSERT(r.rgen == firstGen);
		//     If there is a write(key,_,gen1)=>gen1 s.t. gen1 < gen2 OR the write completed before this read started,
		//     then gen >= gen1.
		ASSERT(r.gen >= firstGen);
		//     If there is a read(key,gen1) that completed before this read started, then rgen >= gen1
		ASSERT(r.rgen >= firstGen);

		ASSERT(!actor.isReady());
	}
	return Void();
}

Future<Void> openDatabase(ClientData* db,
                          int* clientCount,
                          Reference<AsyncVar<bool>> hasConnectedClients,
                          OpenDatabaseCoordRequest req,
                          Future<Void> checkStuck) {
	ErrorOr<CachedSerialization<ClientDBInfo>> replyContents;
	Future<Void> clientInfoOnChange = db->clientInfo->onChange();

	++(*clientCount);
	hasConnectedClients->set(true);

	if (req.supportedVersions.size() > 0 && !req.internal) {
		db->clientStatusInfoMap[req.reply.getEndpoint().getPrimaryAddress()] =
		    ClientStatusInfo(req.traceLogGroup, req.supportedVersions, req.issues);
	}

	while (!db->clientInfo->get().read().id.isValid() || (db->clientInfo->get().read().id == req.knownClientInfoID &&
	                                                      !db->clientInfo->get().read().forward.present())) {
		auto res = co_await race(
		    checkStuck, yieldedFuture(clientInfoOnChange), delayJittered(SERVER_KNOBS->CLIENT_REGISTER_INTERVAL));
		if (res.index() == 0) {
			// checkStuck fired:
			replyContents = failed_to_progress();
			break;
		} else if (res.index() == 1) {
			// clientInfoOnChange fired:
			clientInfoOnChange = db->clientInfo->onChange();
			replyContents = db->clientInfo->get();
		} else if (res.index() == 2) {
			// delay fired:
			if (db->clientInfo->get().read().id.isValid()) {
				replyContents = db->clientInfo->get();
			}
			// Otherwise, we still break out of the loop and return a default_error_or.
			// The client might be long gone!
			break;
		}
	}

	if (req.supportedVersions.size() > 0 && !req.internal) {
		db->clientStatusInfoMap.erase(req.reply.getEndpoint().getPrimaryAddress());
	}

	if (replyContents.present()) {
		req.reply.send(replyContents.get());
	} else {
		req.reply.sendError(replyContents.getError());
	}

	if (--(*clientCount) == 0) {
		hasConnectedClients->set(false);
	}
}

Future<Void> remoteMonitorLeader(int* clientCount,
                                 Reference<AsyncVar<bool>> hasConnectedClients,
                                 Reference<AsyncVar<Optional<LeaderInfo>>> currentElectedLeader,
                                 ElectionResultRequest req) {
	Future<Void> currentElectedLeaderOnChange = currentElectedLeader->onChange();
	++(*clientCount);
	hasConnectedClients->set(true);

	while (!currentElectedLeader->get().present() || req.knownLeader == currentElectedLeader->get().get().changeID) {
		auto res = co_await race(yieldedFuture(currentElectedLeaderOnChange),
		                         delayJittered(SERVER_KNOBS->CLIENT_REGISTER_INTERVAL));
		if (res.index() == 0) {
			currentElectedLeaderOnChange = currentElectedLeader->onChange();
		} else {
			break;
		}
	}

	req.reply.send(currentElectedLeader->get());

	if (--(*clientCount) == 0) {
		hasConnectedClients->set(false);
	}
}

// This class implements a *single* leader-election register (essentially, it ignores
// the .key member of each request).  It returns any time the leader election is in the
// default state, so that only active registers consume memory.
class LeaderRegister : public ReferenceCounted<LeaderRegister>, NonCopyable {
	LeaderElectionRegInterface interf;
	Key key;
	std::set<LeaderInfo> availableCandidates;
	std::set<LeaderInfo> availableLeaders;
	Optional<LeaderInfo> currentNominee;
	Deque<ReplyPromise<Optional<LeaderInfo>>> notify;
	Future<Void> nextInterval;
	AsyncVar<int> nextIntervalGeneration;
	double candidateDelay;
	int leaderIntervalCount;
	Future<Void> notifyCheck;
	ClientData clientData;
	int clientCount;
	Reference<AsyncVar<bool>> hasConnectedClients;
	ActorCollection actors;
	Future<Void> leaderMon;
	Reference<AsyncVar<Optional<LeaderInfo>>> currentElectedLeader;
	LivenessChecker canConnectToLeader;
	Future<Void> hasConnectedClientsOnChange;

	void setNextInterval(Future<Void> interval) {
		nextInterval = interval;
		nextIntervalGeneration.set(nextIntervalGeneration.get() + 1);
	}

	void ensureNextInterval() {
		if (!nextInterval.isValid()) {
			setNextInterval(delay(0));
		}
	}

	void clearNextInterval() {
		if (nextInterval.isValid()) {
			setNextInterval(Future<Void>());
		}
	}

	void sendNotifications(Optional<LeaderInfo> const& nominee) {
		for (unsigned int i = 0; i < notify.size(); i++) {
			notify[i].send(nominee);
		}
		notify.clear();
	}

	void sendNotifications(LeaderInfo const& info) {
		for (unsigned int i = 0; i < notify.size(); i++) {
			notify[i].send(info);
		}
		notify.clear();
	}

	bool checkNotificationLimit() {
		if (notify.size() > SERVER_KNOBS->MAX_NOTIFICATIONS) {
			TraceEvent(SevWarnAlways, "TooManyNotifications").detail("Amount", notify.size());
			sendNotifications(currentNominee.get());
			return true;
		}
		return false;
	}

	Future<Void> serveOpenDatabaseRequests() {
		while (true) {
			OpenDatabaseCoordRequest req = co_await interf.openDatabase.getFuture();
			if (clientData.clientInfo->get().read().id.isValid() &&
			    clientData.clientInfo->get().read().id != req.knownClientInfoID &&
			    !clientData.clientInfo->get().read().forward.present()) {
				req.reply.send(clientData.clientInfo->get());
			} else {
				if (!leaderMon.isValid()) {
					leaderMon = monitorLeaderAndGetClientInfo(
					    req.clusterKey, req.hostnames, req.coordinators, &clientData, currentElectedLeader);
				}
				actors.add(
				    openDatabase(&clientData, &clientCount, hasConnectedClients, req, canConnectToLeader.checkStuck()));
			}
		}
	}

	Future<Void> serveElectionResultRequests() {
		while (true) {
			ElectionResultRequest req = co_await interf.electionResult.getFuture();
			if (currentElectedLeader->get().present() &&
			    req.knownLeader != currentElectedLeader->get().get().changeID) {
				req.reply.send(currentElectedLeader->get());
			} else {
				if (!leaderMon.isValid()) {
					leaderMon = monitorLeaderAndGetClientInfo(
					    req.key, req.hostnames, req.coordinators, &clientData, currentElectedLeader);
				}
				actors.add(remoteMonitorLeader(&clientCount, hasConnectedClients, currentElectedLeader, req));
			}
		}
	}

	Future<Void> serveGetLeaderRequests() {
		while (true) {
			GetLeaderRequest req = co_await interf.getLeader.getFuture();
			if (currentNominee.present() && currentNominee.get().changeID != req.knownLeader) {
				req.reply.send(currentNominee.get());
			} else {
				notify.push_back(req.reply);
				if (!checkNotificationLimit() && !nextInterval.isValid()) {
					ensureNextInterval();
				}
			}
		}
	}

	Future<Void> serveCandidacyRequests() {
		while (true) {
			CandidacyRequest req = co_await interf.candidacy.getFuture();
			ensureNextInterval();
			availableCandidates.erase(LeaderInfo(req.prevChangeID));
			availableCandidates.insert(req.myInfo);
			if (currentNominee.present() && currentNominee.get().changeID != req.knownLeader) {
				req.reply.send(currentNominee.get());
			} else {
				notify.push_back(req.reply);
				checkNotificationLimit();
			}
		}
	}

	Future<Void> serveLeaderHeartbeatRequests() {
		while (true) {
			LeaderHeartbeatRequest req = co_await interf.leaderHeartbeat.getFuture();
			ensureNextInterval();
			// TODO: use notify to only send a heartbeat once per interval
			availableLeaders.erase(LeaderInfo(req.prevChangeID));
			availableLeaders.insert(req.myInfo);
			bool const isCurrentLeader = currentNominee.present() && currentNominee.get().equalInternalId(req.myInfo);
			if (isCurrentLeader) {
				canConnectToLeader.confirmLiveness();
			}
			req.reply.send(LeaderHeartbeatReply{ isCurrentLeader });
		}
	}

	Future<Void> serveForwardRequests() {
		while (true) {
			ForwardRequest req = co_await interf.forward.getFuture();
			LeaderInfo newInfo;
			newInfo.forward = true;
			newInfo.serializedInfo = req.conn.toString();
			sendNotifications(newInfo);
			ClientDBInfo outInfo;
			outInfo.id = deterministicRandom()->randomUniqueID();
			outInfo.forward = req.conn.toString();
			clientData.clientInfo->set(CachedSerialization<ClientDBInfo>(outInfo));
			req.reply.send(Void());
			if (!hasConnectedClients->get()) {
				co_return;
			}
			clearNextInterval();
		}
	}

	Future<Void> monitorNextInterval() {
		while (true) {
			int generation = nextIntervalGeneration.get();
			auto res =
			    co_await race(nextInterval.isValid() ? nextInterval : Never(), nextIntervalGeneration.onChange());
			if (res.index() == 1 || generation != nextIntervalGeneration.get()) {
				continue;
			}
			if (res.index() != 0) {
				UNREACHABLE();
			}

			if (!availableLeaders.size() && !availableCandidates.size() && !notify.size() &&
			    !currentNominee.present()) {
				// Our state is back to the initial state, so we can safely stop this actor
				TraceEvent("EndingLeaderNomination")
				    .detail("Key", key)
				    .detail("HasConnectedClients", hasConnectedClients->get());
				if (!hasConnectedClients->get()) {
					co_return;
				} else {
					clearNextInterval();
				}
			} else {
				Optional<LeaderInfo> nextNominee;
				if (availableCandidates.size() &&
				    (!availableLeaders.size() ||
				     availableLeaders.begin()->leaderChangeRequired(*availableCandidates.begin()))) {
					nextNominee = *availableCandidates.begin();
				} else if (availableLeaders.size()) {
					nextNominee = *availableLeaders.begin();
				}

				// If the current leader's priority became worse, we still need to notified all clients because now one
				// of them might be better than the leader. In addition, even though FitnessRemote is better than
				// FitnessUnknown, we still need to notified clients so that monitorLeaderRemotely has a chance to
				// switch from passively monitoring the leader to actively attempting to become the leader.
				if (!currentNominee.present() || !nextNominee.present() ||
				    !currentNominee.get().equalInternalId(nextNominee.get()) ||
				    nextNominee.get() > currentNominee.get() ||
				    (currentNominee.get().getPriorityInfo().dcFitness ==
				         ClusterControllerPriorityInfo::FitnessUnknown &&
				     nextNominee.get().getPriorityInfo().dcFitness == ClusterControllerPriorityInfo::FitnessRemote)) {
					TraceEvent("NominatingLeader")
					    .detail("NextNominee", nextNominee.present() ? nextNominee.get().changeID : UID())
					    .detail("CurrentNominee", currentNominee.present() ? currentNominee.get().changeID : UID())
					    .detail("Key", printable(key));
					sendNotifications(nextNominee);
				}

				currentNominee = nextNominee;

				if (availableLeaders.size()) {
					setNextInterval(delay(SERVER_KNOBS->POLLING_FREQUENCY));
					if (leaderIntervalCount++ > 5) {
						candidateDelay = SERVER_KNOBS->CANDIDATE_MIN_DELAY;
					}
				} else {
					setNextInterval(delay(candidateDelay));
					candidateDelay = std::min(SERVER_KNOBS->CANDIDATE_MAX_DELAY,
					                          candidateDelay * SERVER_KNOBS->CANDIDATE_GROWTH_RATE);
					leaderIntervalCount = 0;
				}

				availableLeaders.clear();
				availableCandidates.clear();
			}
		}
	}

	Future<Void> monitorNotifyCheck() {
		while (true) {
			co_await notifyCheck;
			notifyCheck = delay(SERVER_KNOBS->NOTIFICATION_FULL_CLEAR_TIME /
			                    std::max<double>(SERVER_KNOBS->MIN_NOTIFICATIONS, notify.size()));
			if (!notify.empty() && currentNominee.present()) {
				notify.front().send(currentNominee.get());
				notify.pop_front();
			}
		}
	}

	Future<Void> monitorConnectedClients() {
		while (true) {
			co_await hasConnectedClientsOnChange;
			hasConnectedClientsOnChange = hasConnectedClients->onChange();
			if (!hasConnectedClients->get() && !nextInterval.isValid()) {
				TraceEvent("LeaderRegisterUnneeded").detail("Key", key);
				co_return;
			}
		}
	}

	Future<Void> monitorActors() { co_await actors.getResult(); }

public:
	LeaderRegister(LeaderElectionRegInterface interf, Key key)
	  : interf(interf), key(key), nextIntervalGeneration(0), candidateDelay(SERVER_KNOBS->CANDIDATE_MIN_DELAY),
	    leaderIntervalCount(0),
	    notifyCheck(delay(SERVER_KNOBS->NOTIFICATION_FULL_CLEAR_TIME / SERVER_KNOBS->MIN_NOTIFICATIONS)),
	    clientCount(0), hasConnectedClients(makeReference<AsyncVar<bool>>(false)), actors(false),
	    currentElectedLeader(makeReference<AsyncVar<Optional<LeaderInfo>>>()),
	    canConnectToLeader(SERVER_KNOBS->COORDINATOR_LEADER_CONNECTION_TIMEOUT),
	    hasConnectedClientsOnChange(hasConnectedClients->onChange()) {}

	static Future<Void> run(Reference<LeaderRegister> self) {
		co_await race(self->serveOpenDatabaseRequests(),
		              self->serveElectionResultRequests(),
		              self->serveGetLeaderRequests(),
		              self->serveCandidacyRequests(),
		              self->serveLeaderHeartbeatRequests(),
		              self->serveForwardRequests(),
		              self->monitorNextInterval(),
		              self->monitorNotifyCheck(),
		              self->monitorConnectedClients(),
		              self->monitorActors());
	}
};

// Generation register values are stored without prefixing in the coordinated state, but always begin with an
// alphanumeric character (they are always derived from a ClusterConnectionString key). Forwarding values are stored in
// this range:
const KeyRangeRef fwdKeys("\xff"
                          "fwd"_sr,
                          "\xff"
                          "fwe"_sr);

// The time when forwarding was last set is stored in this range:
const KeyRangeRef fwdTimeKeys("\xff"
                              "fwdTime"_sr,
                              "\xff"
                              "fwdTimf"_sr);
struct LeaderRegisterCollection {
	// SOMEDAY: Factor this into a generic tool?  Extend ActorCollection to support removal actions?  What?
	ActorCollection actors;
	Map<Key, LeaderElectionRegInterface> registerInterfaces;
	Map<Key, LeaderInfo> forward;
	OnDemandStore* pStore;
	Map<Key, double> forwardStartTime;

	explicit LeaderRegisterCollection(OnDemandStore* pStore) : actors(false), pStore(pStore) {}

	static Future<Void> init(LeaderRegisterCollection* self) {
		if (!self->pStore->exists())
			co_return;
		OnDemandStore& store = *self->pStore;
		Future<Standalone<RangeResultRef>> forwardingInfoF = store->readRange(fwdKeys);
		Future<Standalone<RangeResultRef>> forwardingTimeF = store->readRange(fwdTimeKeys);
		co_await (success(forwardingInfoF) && success(forwardingTimeF));
		Standalone<RangeResultRef> forwardingInfo = forwardingInfoF.get();
		Standalone<RangeResultRef> forwardingTime = forwardingTimeF.get();
		for (int i = 0; i < forwardingInfo.size(); i++) {
			LeaderInfo forwardInfo;
			forwardInfo.forward = true;
			forwardInfo.serializedInfo = forwardingInfo[i].value;
			self->forward[forwardingInfo[i].key.removePrefix(fwdKeys.begin)] = forwardInfo;
		}
		for (int i = 0; i < forwardingTime.size(); i++) {
			double time = BinaryReader::fromStringRef<double>(forwardingTime[i].value, Unversioned());
			self->forwardStartTime[forwardingTime[i].key.removePrefix(fwdTimeKeys.begin)] = time;
		}
	}

	Future<Void> onError() const { return actors.getResult(); }

	// Check if the this coordinator is no longer the leader, and the new one was stored in the "forward" keyspace.
	// If the "forward" keyspace was set some time ago (as configured by knob), log an error to indicate the client is
	// using a very old cluster file.
	Optional<LeaderInfo> getForward(KeyRef key) {
		auto i = forward.find(key);
		auto t = forwardStartTime.find(key);
		if (i == forward.end())
			return Optional<LeaderInfo>();
		if (t != forwardStartTime.end()) {
			double forwardTime = t->value;
			if (now() - forwardTime > SERVER_KNOBS->FORWARD_REQUEST_TOO_OLD) {
				TraceEvent(SevWarnAlways, "AccessOldForward")
				    .detail("ForwardSetSecondsAgo", now() - forwardTime)
				    .detail("ForwardClusterKey", key);
			}
		}
		return i->value;
	}

	// When the lead coordinator changes, store the new connection ID in the "fwd" keyspace.
	// If a request arrives using an old connection id, resend it to the new coordinator using the stored connection id.
	// Store when this change took place in the fwdTime keyspace.
	static Future<Void> setForward(LeaderRegisterCollection* self,
	                               KeyRef key,
	                               ClusterConnectionString conn,
	                               ForwardRequest req,
	                               UID id) {
		double forwardTime = now();
		LeaderInfo forwardInfo;
		forwardInfo.forward = true;
		forwardInfo.serializedInfo = conn.toString();
		self->forward[key] = forwardInfo;
		self->forwardStartTime[key] = forwardTime;
		OnDemandStore& store = *self->pStore;
		store->set(KeyValueRef(key.withPrefix(fwdKeys.begin), conn.toString()));
		store->set(KeyValueRef(key.withPrefix(fwdTimeKeys.begin), BinaryWriter::toValue(forwardTime, Unversioned())));
		co_await store->commit();
		// Do not process a forwarding request until after it has been made durable in case the coordinator restarts
		self->getInterface(req.key, id).forward.send(req);
	}

	LeaderElectionRegInterface& getInterface(KeyRef key, UID id) {
		auto i = registerInterfaces.find(key);
		if (i == registerInterfaces.end()) {
			Key k = key;
			Future<Void> a =
			    wrap(this, k, LeaderRegister::run(makeReference<LeaderRegister>(registerInterfaces[k], k)), id);
			if (a.isError())
				throw a.getError();
			ASSERT(!a.isReady());
			actors.add(a);
			i = registerInterfaces.find(key);
		}
		ASSERT(i != registerInterfaces.end());
		return i->value;
	}

	static Future<Void> wrap(LeaderRegisterCollection* self, Key key, Future<Void> actor, UID id) {
		Error e;
		try {
			// FIXME: Get worker ID here
			startRole(Role::COORDINATOR, id, UID());
			co_await (actor || traceRole(Role::COORDINATOR, id));
			endRole(Role::COORDINATOR, id, "Coordinator changed");
		} catch (Error& err) {
			endRole(Role::COORDINATOR, id, err.what(), err.code() == error_code_actor_cancelled, err);
			if (err.code() == error_code_actor_cancelled)
				throw;
			e = err;
		}
		self->registerInterfaces.erase(key);
		if (e.code() != invalid_error_code)
			throw e;
	}
};

// extract the prefix descriptor from cluster id
StringRef getClusterDescriptor(Key key) {
	StringRef str = key.contents();
	return str.eat(":");
}

// leaderServer multiplexes multiple leaderRegisters onto a single LeaderElectionRegInterface,
// creating and destroying them on demand.
ACTOR Future<Void> leaderServer(LeaderElectionRegInterface interf,
                                OnDemandStore* pStore,
                                UID id,
                                Reference<IClusterConnectionRecord> ccr) {
	state LeaderRegisterCollection regs(pStore);
	state ActorCollection forwarders(false);

	wait(LeaderRegisterCollection::init(&regs));

	loop choose {
		when(CheckDescriptorMutableRequest req = waitNext(interf.checkDescriptorMutable.getFuture())) {
			// Note the response returns the value of a knob enforced by checking only one coordinator. It is not
			// quorum based.
			CheckDescriptorMutableReply rep(SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT);
			req.reply.send(rep);
		}
		when(OpenDatabaseCoordRequest req = waitNext(interf.openDatabase.getFuture())) {
			Optional<LeaderInfo> forward = regs.getForward(req.clusterKey);
			if (forward.present()) {
				ClientDBInfo info;
				info.id = deterministicRandom()->randomUniqueID();
				info.forward = forward.get().serializedInfo;
				req.reply.send(CachedSerialization<ClientDBInfo>(info));
			} else {
				StringRef clusterName = ccr->getConnectionString().clusterKeyName();
				if (!SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT &&
				    getClusterDescriptor(req.clusterKey).compare(clusterName)) {
					TraceEvent(SevWarn, "CCRMismatch")
					    .detail("RequestType", "OpenDatabaseCoordRequest")
					    .detail("LocalCS", ccr->getConnectionString().toString())
					    .detail("IncomingClusterKey", req.clusterKey)
					    .detail("IncomingCoordinators", describeList(req.coordinators, req.coordinators.size()));
					req.reply.sendError(wrong_connection_file());
				} else {
					regs.getInterface(req.clusterKey, id).openDatabase.send(req);
				}
			}
		}
		when(ElectionResultRequest req = waitNext(interf.electionResult.getFuture())) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if (forward.present()) {
				req.reply.send(forward.get());
			} else {
				StringRef clusterName = ccr->getConnectionString().clusterKeyName();
				if (!SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT && getClusterDescriptor(req.key).compare(clusterName)) {
					TraceEvent(SevWarn, "CCRMismatch")
					    .detail("RequestType", "ElectionResultRequest")
					    .detail("LocalCS", ccr->getConnectionString().toString())
					    .detail("IncomingClusterKey", req.key)
					    .detail("ClusterKey", ccr->getConnectionString().clusterKey())
					    .detail("IncomingCoordinators", describeList(req.coordinators, req.coordinators.size()));
					req.reply.sendError(wrong_connection_file());
				} else {
					regs.getInterface(req.key, id).electionResult.send(req);
				}
			}
		}
		when(GetLeaderRequest req = waitNext(interf.getLeader.getFuture())) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if (forward.present())
				req.reply.send(forward.get());
			else {
				StringRef clusterName = ccr->getConnectionString().clusterKeyName();
				if (!SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT && getClusterDescriptor(req.key).compare(clusterName)) {
					TraceEvent(SevWarn, "CCRMismatch")
					    .detail("RequestType", "GetLeaderRequest")
					    .detail("LocalCS", ccr->getConnectionString().toString())
					    .detail("IncomingClusterKey", req.key)
					    .detail("ClusterKey", ccr->getConnectionString().clusterKey());
					req.reply.sendError(wrong_connection_file());
				} else {
					regs.getInterface(req.key, id).getLeader.send(req);
				}
			}
		}
		when(CandidacyRequest req = waitNext(interf.candidacy.getFuture())) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if (forward.present())
				req.reply.send(forward.get());
			else {
				StringRef clusterName = ccr->getConnectionString().clusterKeyName();
				if (!SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT && getClusterDescriptor(req.key).compare(clusterName)) {
					TraceEvent(SevWarn, "CCRMismatch")
					    .detail("RequestType", "CandidacyRequest")
					    .detail("LocalCS", ccr->getConnectionString().toString())
					    .detail("IncomingClusterKey", req.key);
					req.reply.sendError(wrong_connection_file());
				} else {
					regs.getInterface(req.key, id).candidacy.send(req);
				}
			}
		}
		when(LeaderHeartbeatRequest req = waitNext(interf.leaderHeartbeat.getFuture())) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if (forward.present())
				req.reply.send(LeaderHeartbeatReply{ false });
			else {
				StringRef clusterName = ccr->getConnectionString().clusterKeyName();
				if (!SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT && getClusterDescriptor(req.key).compare(clusterName)) {
					TraceEvent(SevWarn, "CCRMismatch")
					    .detail("RequestType", "LeaderHeartbeatRequest")
					    .detail("LocalCS", ccr->getConnectionString().toString())
					    .detail("IncomingClusterKey", req.key);
					req.reply.sendError(wrong_connection_file());
				} else {
					regs.getInterface(req.key, id).leaderHeartbeat.send(req);
				}
			}
		}
		when(ForwardRequest req = waitNext(interf.forward.getFuture())) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if (forward.present()) {
				req.reply.send(Void());
			} else {
				StringRef clusterName = ccr->getConnectionString().clusterKeyName();
				if (!SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT && getClusterDescriptor(req.key).compare(clusterName)) {
					TraceEvent(SevWarn, "CCRMismatch")
					    .detail("RequestType", "ForwardRequest")
					    .detail("LocalCS", ccr->getConnectionString().toString())
					    .detail("IncomingClusterKey", req.key);
					req.reply.sendError(wrong_connection_file());
				} else {
					forwarders.add(LeaderRegisterCollection::setForward(
					    &regs, req.key, ClusterConnectionString(req.conn.toString()), req, id));
				}
			}
		}
		when(wait(forwarders.getResult())) {
			ASSERT(false);
			throw internal_error();
		}
	}
}

Future<Void> coordinationServer(std::string dataFolder, Reference<IClusterConnectionRecord> ccr) {
	UID myID = deterministicRandom()->randomUniqueID();
	LeaderElectionRegInterface myLeaderInterface(g_network);
	GenerationRegInterface myInterface(g_network);
	OnDemandStore store(dataFolder, myID, fileCoordinatorPrefix);
	TraceEvent("CoordinationServer", myID)
	    .detail("MyInterfaceAddr", myInterface.read.getEndpoint().getPrimaryAddress())
	    .detail("Folder", dataFolder);

	Error err;
	try {
		LocalGenerationReg generationReg(myInterface, &store);
		co_await (generationReg.run() || leaderServer(myLeaderInterface, &store, myID, ccr) || store.getError());
		throw internal_error();
	} catch (Error& e) {
		err = e;
	}
	TraceEvent("CoordinationServerError", myID).errorUnsuppressed(err);

	// Handle a rare issue where the coordinator crashes during creation of
	// its disk queue files. A disk queue consists of two files, created in
	// the following manner:
	//
	//   1. Create two .part files.
	//   2. Rename each .part file to remove its .part suffix.
	//
	// Step 2 can crash in between removing the .part suffix of the first
	// and second file. If this occurs, the disk queue will refuse to open
	// on subsequent attempts because it only sees one of its files,
	// causing a process crash with the file_not_found error. Normally this
	// behavior is fine, but in simulation it can occasionally cause
	// problems. Simulation does not take into account injected errors can
	// cause permanent process death. In most cases this is true. But if
	// this inconsistent disk queue state occurs on a coordinator, the
	// coordinator will enter a reboot loop, continuously failing to open
	// its disk queue and crashing. If the simulation run has a small
	// number of processes and another process is colocated with the
	// coordinator, the run may get stuck because the coordinator crashing
	// brings the other role offline as well. This has been observed in a
	// stuck simulation run where a tlog colocated with a coordinator that
	// ran into this issue meant the tlog replication policy couldn't be
	// achieved.
	//
	// As a short term fix, catch file_not_found and fix inconsistent disk
	// queue state on the coordinator. In the long term, we should either
	// modify simulation to consider injected errors as fatal or allow the
	// coordinator to manually fix disk queue state in real clusters.
	if (g_network->isSimulated() && g_simulator->speedUpSimulation && err.code() == error_code_file_not_found) {
		std::vector<Future<Reference<IAsyncFile>>> fs;
		fs.reserve(2);
		for (int i = 0; i < 2; ++i) {
			std::string file = joinPath(dataFolder, format("%s%d.fdq", fileCoordinatorPrefix.c_str(), i));
			fs.push_back(IAsyncFileSystem::filesystem()->open(file,
			                                                  IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNCACHED |
			                                                      IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_LOCK,
			                                                  0));
		}
		co_await waitForAllReady(fs);

		// Make sure one of the disk queue files is missing. This should be
		// the only cause of the file_not_found error.
		ASSERT(((fs[0].isError() && fs[0].getError().code() == error_code_file_not_found) ||
		        (fs[1].isError() && fs[1].getError().code() == error_code_file_not_found)) &&
		       fs[0].isError() != fs[1].isError());

		TraceEvent(SevWarnAlways, "CoordinatorDiskQueueInconsistentState")
		    .detail("File0Missing", fs[0].isError())
		    .detail("File1Missing", fs[1].isError());
		;

		// Remove the remaining disk queue file to allow the coordinator to
		// create new files on next boot.
		if (fs[0].isError()) {
			ASSERT(fs[0].getError().code() == error_code_file_not_found);
			co_await IAsyncFileSystem::filesystem()->deleteFile(joinPath(dataFolder, fileCoordinatorPrefix + "1.fdq"),
			                                                    true);
		}
		if (fs[1].isError()) {
			ASSERT(fs[1].getError().code() == error_code_file_not_found);
			co_await IAsyncFileSystem::filesystem()->deleteFile(joinPath(dataFolder, fileCoordinatorPrefix + "0.fdq"),
			                                                    true);
		}
	}

	throw err;
}

Future<Void> changeClusterDescription(std::string datafolder, KeyRef newClusterKey, KeyRef oldClusterKey) {
	UID myID = deterministicRandom()->randomUniqueID();
	OnDemandStore store(datafolder, myID, fileCoordinatorPrefix);
	RangeResult res = co_await store->readRange(allKeys);
	// Context, in coordinators' kv-store
	// cluster description and the random id are always appear together as the clusterKey
	// The old cluster key, (call it oldCKey) below can appear in the following scenarios:
	// 1. oldCKey is a key in the store: the value is a binary format of _GenerationRegVal_ which contains a different
	// clusterKey(either movedFrom or moveTo)
	// 2. oldCKey appears in a key for forwarding message:
	// 		2.1: the prefix is _fwdKeys.begin_: the value is the new connection string
	//		2.2: the prefix is _fwdTimeKeys.begin_: the value is the time
	// 3. oldCKey does not appear in any keys but in a value:
	// 		3.1: it's in the value of a forwarding message(see 2.1)
	//		3.2: it's inside the value of _GenerationRegVal_ (see 1), which is a cluster connection string.
	//		it seems that even we do not change it the cluster should still be good, but to be safe we still update it.
	for (auto& [key, value] : res) {
		if (key.startsWith(fwdKeys.begin)) {
			if (key.removePrefix(fwdKeys.begin) == oldClusterKey) {
				store->clear(singleKeyRange(key));
				store->set(KeyValueRef(newClusterKey.withPrefix(fwdKeys.begin), value));
			} else if (value.startsWith(oldClusterKey)) {
				store->set(KeyValueRef(key, value.removePrefix(oldClusterKey).withPrefix(newClusterKey)));
			}
		} else if (key.startsWith(fwdTimeKeys.begin) && key.removePrefix(fwdTimeKeys.begin) == oldClusterKey) {
			store->clear(singleKeyRange(key));
			store->set(KeyValueRef(newClusterKey.withPrefix(fwdTimeKeys.begin), value));
		} else if (key == oldClusterKey) {
			store->clear(singleKeyRange(key));
			store->set(KeyValueRef(newClusterKey, value));
		} else {
			// parse the value part
			auto regVal = BinaryReader::fromStringRef<GenerationRegVal>(value, IncludeVersion());
			if (regVal.val.present()) {
				Optional<Value> newVal = updateCCSInMovableValue(regVal.val.get(), oldClusterKey, newClusterKey);
				if (newVal.present()) {
					regVal.val = newVal.get();
					store->set(KeyValueRef(
					    key, BinaryWriter::toValue(regVal, IncludeVersion(ProtocolVersion::withGenerationRegVal()))));
				}
			}
		}
	}
	co_await store->commit();
}

Future<Void> coordChangeClusterKey(std::string dataFolder, KeyRef newClusterKey, KeyRef oldClusterKey) {
	TraceEvent(SevInfo, "CoordChangeClusterKey")
	    .detail("DataFolder", dataFolder)
	    .detail("NewClusterKey", newClusterKey)
	    .detail("OldClusterKey", oldClusterKey);
	std::string absDataFolder = abspath(dataFolder);
	std::vector<std::string> returnList = platform::listDirectories(absDataFolder);
	std::vector<Future<Void>> futures;
	for (const auto& dirEntry : returnList) {
		if (dirEntry == "." || dirEntry == "..") {
			continue;
		}
		std::string processDir = dataFolder + "/" + dirEntry;
		TraceEvent(SevInfo, "UpdatingCoordDataForProcess").detail("ProcessDataDir", processDir);
		std::vector<std::string> returnFiles = platform::listFiles(processDir, "");
		bool isCoord = false;
		for (const auto& fileEntry : returnFiles) {
			if (fileEntry.rfind(fileCoordinatorPrefix, 0) == 0) {
				isCoord = true;
			}
		}
		if (!isCoord)
			continue;
		futures.push_back(changeClusterDescription(processDir, newClusterKey, oldClusterKey));
	}
	return waitForAll(futures);
}
