/*
 * Coordination.actor.cpp
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

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "fdbclient/ConfigTransactionInterface.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/ConfigNode.h"
#include "fdbclient/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/OnDemandStore.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/Status.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/ProtocolVersion.h"
#include "flow/UnitTest.h"
#include "flow/IndexedSet.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/network.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// This module implements coordinationServer() and the interfaces in CoordinationInterface.h

namespace {

class LivenessChecker {
	double threshold;
	AsyncVar<double> lastTime;
	ACTOR static Future<Void> checkStuck(LivenessChecker const* self) {
		loop {
			choose {
				when(wait(delayUntil(self->lastTime.get() + self->threshold))) {
					return Void();
				}
				when(wait(self->lastTime.onChange())) {}
			}
		}
	}

public:
	explicit LivenessChecker(double threshold) : threshold(threshold), lastTime(now()) {}

	void confirmLiveness() { lastTime.set(now()); }

	Future<Void> checkStuck() const { return checkStuck(this); }
};

} // namespace

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

GenerationRegInterface::GenerationRegInterface(NetworkAddress const& remote)
  : read(Endpoint::wellKnown({ remote }, WLTOKEN_GENERATIONREG_READ)),
    write(Endpoint::wellKnown({ remote }, WLTOKEN_GENERATIONREG_WRITE)) {}

GenerationRegInterface::GenerationRegInterface(INetwork* local) {
	read.makeWellKnownEndpoint(WLTOKEN_GENERATIONREG_READ, TaskPriority::Coordination);
	write.makeWellKnownEndpoint(WLTOKEN_GENERATIONREG_WRITE, TaskPriority::Coordination);
}

LeaderElectionRegInterface::LeaderElectionRegInterface(NetworkAddress const& remote)
  : ClientLeaderRegInterface(remote), candidacy(Endpoint::wellKnown({ remote }, WLTOKEN_LEADERELECTIONREG_CANDIDACY)),
    electionResult(Endpoint::wellKnown({ remote }, WLTOKEN_LEADERELECTIONREG_ELECTIONRESULT)),
    leaderHeartbeat(Endpoint::wellKnown({ remote }, WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT)),
    forward(Endpoint::wellKnown({ remote }, WLTOKEN_LEADERELECTIONREG_FORWARD)) {}

LeaderElectionRegInterface::LeaderElectionRegInterface(INetwork* local) : ClientLeaderRegInterface(local) {
	candidacy.makeWellKnownEndpoint(WLTOKEN_LEADERELECTIONREG_CANDIDACY, TaskPriority::Coordination);
	electionResult.makeWellKnownEndpoint(WLTOKEN_LEADERELECTIONREG_ELECTIONRESULT, TaskPriority::Coordination);
	leaderHeartbeat.makeWellKnownEndpoint(WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT, TaskPriority::Coordination);
	forward.makeWellKnownEndpoint(WLTOKEN_LEADERELECTIONREG_FORWARD, TaskPriority::Coordination);
}

ServerCoordinators::ServerCoordinators(Reference<IClusterConnectionRecord> ccr, ConfigDBType configDBType)
  : ClientCoordinators(ccr) {
	ClusterConnectionString cs = ccr->getConnectionString();
	for (auto h : cs.hostnames) {
		leaderElectionServers.emplace_back(h);
		stateServers.emplace_back(h);
		if (configDBType != ConfigDBType::DISABLED) {
			configServers.emplace_back(h);
		}
	}
	for (auto s : cs.coords) {
		leaderElectionServers.emplace_back(s);
		stateServers.emplace_back(s);
		if (configDBType != ConfigDBType::DISABLED) {
			configServers.emplace_back(s);
		}
	}
}

struct CoordinatorRequestCounter {
	enum class RequestType {
		leaderServer_CheckDescriptorMutableRequest,
		leaderServer_OpenDatabaseCoordRequest,
		leaderServer_ElectionResultRequest,
		leaderServer_GetLeaderRequest,
		leaderServer_CandidacyRequest,
		leaderServer_LeaderHeartbeatRequest,
		leaderServer_ForwardRequest,
		localGenerationReg_GenerationRegReadRequest,
		localGenerationReg_GenerationRegWriteRequest,
	};

	std::string typeToString(RequestType type) const {
		switch (type) {
		case RequestType::leaderServer_CheckDescriptorMutableRequest:
			return "CheckDescriptorMutableRequest";
		case RequestType::leaderServer_OpenDatabaseCoordRequest:
			return "OpenDatabaseCoordRequest";
		case RequestType::leaderServer_ElectionResultRequest:
			return "ElectionResultRequest";
		case RequestType::leaderServer_GetLeaderRequest:
			return "GetLeaderRequest";
		case RequestType::leaderServer_CandidacyRequest:
			return "CandidacyRequest";
		case RequestType::leaderServer_LeaderHeartbeatRequest:
			return "LeaderHeartbeatRequest";
		case RequestType::leaderServer_ForwardRequest:
			return "ForwardRequest";
		case RequestType::localGenerationReg_GenerationRegReadRequest:
			return "GenerationRegReadRequest";
		case RequestType::localGenerationReg_GenerationRegWriteRequest:
			return "GenerationRegWriteRequest";
		default:
			return "Unknown";
		}
	}

	void init() {
		requestCounts.clear();
		constexpr RequestType allTypes[] = {
			RequestType::leaderServer_CheckDescriptorMutableRequest,
			RequestType::leaderServer_OpenDatabaseCoordRequest,
			RequestType::leaderServer_ElectionResultRequest,
			RequestType::leaderServer_GetLeaderRequest,
			RequestType::leaderServer_CandidacyRequest,
			RequestType::leaderServer_LeaderHeartbeatRequest,
			RequestType::leaderServer_ForwardRequest,
			RequestType::localGenerationReg_GenerationRegReadRequest,
			RequestType::localGenerationReg_GenerationRegWriteRequest,
		};
		for (RequestType t : allTypes) {
			requestCounts[t] = 0;
		}
	}

	std::unordered_map<RequestType, uint64_t> requestCounts;

	void addRequest(RequestType type) {
		auto it = requestCounts.find(type);
		if (it == requestCounts.end()) {
			requestCounts[type] = 1;
		} else {
			it->second++;
		}
	}

	void logging() {
		TraceEvent e("CoordinatorRequestCounter");
		for (const auto& [type, count] : requestCounts) {
			e.detail(typeToString(type), count);
		}
	}
};

ACTOR Future<Void> localGenerationReg(GenerationRegInterface interf,
                                      OnDemandStore* pstore,
                                      std::shared_ptr<CoordinatorRequestCounter> requestCounter) {
	state GenerationRegVal v;
	state OnDemandStore& store = *pstore;
	// SOMEDAY: concurrent access to different keys?
	loop choose {
		when(GenerationRegReadRequest _req = waitNext(interf.read.getFuture())) {
			requestCounter->addRequest(
			    CoordinatorRequestCounter::RequestType::localGenerationReg_GenerationRegReadRequest);
			TraceEvent("GenerationRegReadRequest")
			    .detail("From", _req.reply.getEndpoint().getPrimaryAddress())
			    .detail("K", _req.key);
			state GenerationRegReadRequest req = _req;
			Optional<Value> rawV = wait(store->readValue(req.key));
			v = rawV.present() ? BinaryReader::fromStringRef<GenerationRegVal>(rawV.get(), IncludeVersion())
			                   : GenerationRegVal();
			TraceEvent("GenerationRegReadReply")
			    .detail("RVSize", rawV.present() ? rawV.get().size() : -1)
			    .detail("VWG", v.writeGen.generation);
			if (v.readGen < req.gen) {
				v.readGen = req.gen;
				store->set(KeyValueRef(
				    req.key, BinaryWriter::toValue(v, IncludeVersion(ProtocolVersion::withGenerationRegVal()))));
				wait(store->commit());
			}
			req.reply.send(GenerationRegReadReply(v.val, v.writeGen, v.readGen));
		}
		when(GenerationRegWriteRequest _wrq = waitNext(interf.write.getFuture())) {
			requestCounter->addRequest(
			    CoordinatorRequestCounter::RequestType::localGenerationReg_GenerationRegWriteRequest);
			state GenerationRegWriteRequest wrq = _wrq;
			Optional<Value> rawV = wait(store->readValue(wrq.kv.key));
			v = rawV.present() ? BinaryReader::fromStringRef<GenerationRegVal>(rawV.get(), IncludeVersion())
			                   : GenerationRegVal();
			if (v.readGen <= wrq.gen && v.writeGen < wrq.gen) {
				v.writeGen = wrq.gen;
				v.val = wrq.kv.value;
				store->set(KeyValueRef(
				    wrq.kv.key, BinaryWriter::toValue(v, IncludeVersion(ProtocolVersion::withGenerationRegVal()))));
				wait(store->commit());
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
}

TEST_CASE("/fdbserver/Coordination/localGenerationReg/simple") {
	state GenerationRegInterface reg;
	state OnDemandStore store(params.getDataDir(), deterministicRandom()->randomUniqueID(), "coordination-");
	state Future<Void> actor = localGenerationReg(reg, &store, std::make_shared<CoordinatorRequestCounter>());
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

ACTOR Future<Void> openDatabase(ClientData* db,
                                int* clientCount,
                                Reference<AsyncVar<bool>> hasConnectedClients,
                                OpenDatabaseCoordRequest req,
                                Future<Void> checkStuck) {
	state ErrorOr<CachedSerialization<ClientDBInfo>> replyContents;
	state Future<Void> clientInfoOnChange = db->clientInfo->onChange();

	++(*clientCount);
	hasConnectedClients->set(true);

	if (req.supportedVersions.size() > 0 && !req.internal) {
		db->clientStatusInfoMap[req.reply.getEndpoint().getPrimaryAddress()] =
		    ClientStatusInfo(req.traceLogGroup, req.supportedVersions, req.issues);
	}

	while (!db->clientInfo->get().read().id.isValid() || (db->clientInfo->get().read().id == req.knownClientInfoID &&
	                                                      !db->clientInfo->get().read().forward.present())) {
		choose {
			when(wait(checkStuck)) {
				replyContents = failed_to_progress();
				break;
			}
			when(wait(yieldedFuture(clientInfoOnChange))) {
				clientInfoOnChange = db->clientInfo->onChange();
				replyContents = db->clientInfo->get();
			}
			when(wait(delayJittered(SERVER_KNOBS->CLIENT_REGISTER_INTERVAL))) {
				if (db->clientInfo->get().read().id.isValid()) {
					replyContents = db->clientInfo->get();
				}
				// Otherwise, we still break out of the loop and return a default_error_or.
				break;
			} // The client might be long gone!
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

	return Void();
}

ACTOR Future<Void> remoteMonitorLeader(int* clientCount,
                                       Reference<AsyncVar<bool>> hasConnectedClients,
                                       Reference<AsyncVar<Optional<LeaderInfo>>> currentElectedLeader,
                                       ElectionResultRequest req) {
	state Future<Void> currentElectedLeaderOnChange = currentElectedLeader->onChange();
	++(*clientCount);
	hasConnectedClients->set(true);

	while (!currentElectedLeader->get().present() || req.knownLeader == currentElectedLeader->get().get().changeID) {
		choose {
			when(wait(yieldedFuture(currentElectedLeaderOnChange))) {
				currentElectedLeaderOnChange = currentElectedLeader->onChange();
			}
			when(wait(delayJittered(SERVER_KNOBS->CLIENT_REGISTER_INTERVAL))) {
				break;
			}
		}
	}

	req.reply.send(currentElectedLeader->get());

	if (--(*clientCount) == 0) {
		hasConnectedClients->set(false);
	}

	return Void();
}

// This actor implements a *single* leader-election register (essentially, it ignores
// the .key member of each request).  It returns any time the leader election is in the
// default state, so that only active registers consume memory.
ACTOR Future<Void> leaderRegister(LeaderElectionRegInterface interf, Key key) {
	state std::set<LeaderInfo> availableCandidates;
	state std::set<LeaderInfo> availableLeaders;
	state Optional<LeaderInfo> currentNominee;
	state Deque<ReplyPromise<Optional<LeaderInfo>>> notify;
	state Future<Void> nextInterval;
	state double candidateDelay = SERVER_KNOBS->CANDIDATE_MIN_DELAY;
	state int leaderIntervalCount = 0;
	state Future<Void> notifyCheck =
	    delay(SERVER_KNOBS->NOTIFICATION_FULL_CLEAR_TIME / SERVER_KNOBS->MIN_NOTIFICATIONS);
	state ClientData clientData;
	state int clientCount = 0;
	state Reference<AsyncVar<bool>> hasConnectedClients = makeReference<AsyncVar<bool>>(false);
	state ActorCollection actors(false);
	state Future<Void> leaderMon;
	state AsyncVar<Value> leaderInterface;
	state Reference<AsyncVar<Optional<LeaderInfo>>> currentElectedLeader =
	    makeReference<AsyncVar<Optional<LeaderInfo>>>();
	state LivenessChecker canConnectToLeader(SERVER_KNOBS->COORDINATOR_LEADER_CONNECTION_TIMEOUT);
	state Future<Void> hasConnectedClientsOnChange = hasConnectedClients->onChange();

	loop choose {
		when(OpenDatabaseCoordRequest req = waitNext(interf.openDatabase.getFuture())) {
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
		when(ElectionResultRequest req = waitNext(interf.electionResult.getFuture())) {
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
		when(GetLeaderRequest req = waitNext(interf.getLeader.getFuture())) {
			if (currentNominee.present() && currentNominee.get().changeID != req.knownLeader) {
				req.reply.send(currentNominee.get());
			} else {
				notify.push_back(req.reply);
				if (notify.size() > SERVER_KNOBS->MAX_NOTIFICATIONS) {
					TraceEvent(SevWarnAlways, "TooManyNotifications").detail("Amount", notify.size());
					for (uint32_t i = 0; i < notify.size(); i++)
						notify[i].send(currentNominee.get());
					notify.clear();
				} else if (!nextInterval.isValid()) {
					nextInterval = delay(0);
				}
			}
		}
		when(CandidacyRequest req = waitNext(interf.candidacy.getFuture())) {
			if (!nextInterval.isValid()) {
				nextInterval = delay(0);
			}
			availableCandidates.erase(LeaderInfo(req.prevChangeID));
			availableCandidates.insert(req.myInfo);
			if (currentNominee.present() && currentNominee.get().changeID != req.knownLeader) {
				req.reply.send(currentNominee.get());
			} else {
				notify.push_back(req.reply);
				if (notify.size() > SERVER_KNOBS->MAX_NOTIFICATIONS) {
					TraceEvent(SevWarnAlways, "TooManyNotifications").detail("Amount", notify.size());
					for (uint32_t i = 0; i < notify.size(); i++)
						notify[i].send(currentNominee.get());
					notify.clear();
				}
			}
		}
		when(LeaderHeartbeatRequest req = waitNext(interf.leaderHeartbeat.getFuture())) {
			if (!nextInterval.isValid()) {
				nextInterval = delay(0);
			}
			// TODO: use notify to only send a heartbeat once per interval
			availableLeaders.erase(LeaderInfo(req.prevChangeID));
			availableLeaders.insert(req.myInfo);
			bool const isCurrentLeader = currentNominee.present() && currentNominee.get().equalInternalId(req.myInfo);
			if (isCurrentLeader) {
				canConnectToLeader.confirmLiveness();
			}
			req.reply.send(LeaderHeartbeatReply{ isCurrentLeader });
		}
		when(ForwardRequest req = waitNext(interf.forward.getFuture())) {
			LeaderInfo newInfo;
			newInfo.forward = true;
			newInfo.serializedInfo = req.conn.toString();
			for (unsigned int i = 0; i < notify.size(); i++)
				notify[i].send(newInfo);
			notify.clear();
			ClientDBInfo outInfo;
			outInfo.id = deterministicRandom()->randomUniqueID();
			outInfo.forward = req.conn.toString();
			clientData.clientInfo->set(CachedSerialization<ClientDBInfo>(outInfo));
			req.reply.send(Void());
			if (!hasConnectedClients->get()) {
				return Void();
			}
			nextInterval = Future<Void>();
		}
		when(wait(nextInterval.isValid() ? nextInterval : Never())) {
			if (!availableLeaders.size() && !availableCandidates.size() && !notify.size() &&
			    !currentNominee.present()) {
				// Our state is back to the initial state, so we can safely stop this actor
				TraceEvent("EndingLeaderNomination")
				    .detail("Key", key)
				    .detail("HasConnectedClients", hasConnectedClients->get());
				if (!hasConnectedClients->get()) {
					return Void();
				} else {
					nextInterval = Future<Void>();
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
					for (unsigned int i = 0; i < notify.size(); i++)
						notify[i].send(nextNominee);
					notify.clear();
				}

				currentNominee = nextNominee;

				if (availableLeaders.size()) {
					nextInterval = delay(SERVER_KNOBS->POLLING_FREQUENCY);
					if (leaderIntervalCount++ > 5) {
						candidateDelay = SERVER_KNOBS->CANDIDATE_MIN_DELAY;
					}
				} else {
					nextInterval = delay(candidateDelay);
					candidateDelay = std::min(SERVER_KNOBS->CANDIDATE_MAX_DELAY,
					                          candidateDelay * SERVER_KNOBS->CANDIDATE_GROWTH_RATE);
					leaderIntervalCount = 0;
				}

				availableLeaders.clear();
				availableCandidates.clear();
			}
		}
		when(wait(notifyCheck)) {
			notifyCheck = delay(SERVER_KNOBS->NOTIFICATION_FULL_CLEAR_TIME /
			                    std::max<double>(SERVER_KNOBS->MIN_NOTIFICATIONS, notify.size()));
			if (!notify.empty() && currentNominee.present()) {
				notify.front().send(currentNominee.get());
				notify.pop_front();
			}
		}
		when(wait(hasConnectedClientsOnChange)) {
			hasConnectedClientsOnChange = hasConnectedClients->onChange();
			if (!hasConnectedClients->get() && !nextInterval.isValid()) {
				TraceEvent("LeaderRegisterUnneeded").detail("Key", key);
				return Void();
			}
		}
		when(wait(actors.getResult())) {}
	}
}

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

	LeaderRegisterCollection(OnDemandStore* pStore) : actors(false), pStore(pStore) {}

	ACTOR static Future<Void> init(LeaderRegisterCollection* self) {
		if (!self->pStore->exists())
			return Void();
		OnDemandStore& store = *self->pStore;
		state Future<Standalone<RangeResultRef>> forwardingInfoF = store->readRange(fwdKeys);
		state Future<Standalone<RangeResultRef>> forwardingTimeF = store->readRange(fwdTimeKeys);
		wait(success(forwardingInfoF) && success(forwardingTimeF));
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
		return Void();
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
	ACTOR static Future<Void> setForward(LeaderRegisterCollection* self,
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
		wait(store->commit());
		// Do not process a forwarding request until after it has been made durable in case the coordinator restarts
		self->getInterface(req.key, id).forward.send(req);
		return Void();
	}

	LeaderElectionRegInterface& getInterface(KeyRef key, UID id) {
		auto i = registerInterfaces.find(key);
		if (i == registerInterfaces.end()) {
			Key k = key;
			Future<Void> a = wrap(this, k, leaderRegister(registerInterfaces[k], k), id);
			if (a.isError())
				throw a.getError();
			ASSERT(!a.isReady());
			actors.add(a);
			i = registerInterfaces.find(key);
		}
		ASSERT(i != registerInterfaces.end());
		return i->value;
	}

	ACTOR static Future<Void> wrap(LeaderRegisterCollection* self, Key key, Future<Void> actor, UID id) {
		state Error e;
		try {
			// FIXME: Get worker ID here
			startRole(Role::COORDINATOR, id, UID());
			wait(actor || traceRole(Role::COORDINATOR, id));
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
		return Void();
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
                                Reference<IClusterConnectionRecord> ccr,
                                std::shared_ptr<CoordinatorRequestCounter> requestCounter) {

	state LeaderRegisterCollection regs(pStore);
	state ActorCollection forwarders(false);

	wait(LeaderRegisterCollection::init(&regs));

	loop choose {
		when(CheckDescriptorMutableRequest req = waitNext(interf.checkDescriptorMutable.getFuture())) {
			requestCounter->addRequest(
			    CoordinatorRequestCounter::RequestType::leaderServer_CheckDescriptorMutableRequest);
			// Note the response returns the value of a knob enforced by checking only one coordinator. It is not
			// quorum based.
			CheckDescriptorMutableReply rep(SERVER_KNOBS->ENABLE_CROSS_CLUSTER_SUPPORT);
			req.reply.send(rep);
		}
		when(OpenDatabaseCoordRequest req = waitNext(interf.openDatabase.getFuture())) {
			requestCounter->addRequest(CoordinatorRequestCounter::RequestType::leaderServer_OpenDatabaseCoordRequest);
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
			requestCounter->addRequest(CoordinatorRequestCounter::RequestType::leaderServer_ElectionResultRequest);
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
			requestCounter->addRequest(CoordinatorRequestCounter::RequestType::leaderServer_GetLeaderRequest);
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
			requestCounter->addRequest(CoordinatorRequestCounter::RequestType::leaderServer_CandidacyRequest);
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
			requestCounter->addRequest(CoordinatorRequestCounter::RequestType::leaderServer_LeaderHeartbeatRequest);
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
			requestCounter->addRequest(CoordinatorRequestCounter::RequestType::leaderServer_ForwardRequest);
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

ACTOR Future<Void> coordinatorRequestCounterLogger(std::shared_ptr<CoordinatorRequestCounter> requestCounter) {
	loop {
		requestCounter->init();
		wait(delay(30.0));
		requestCounter->logging();
	}
}

ACTOR Future<Void> coordinationServer(std::string dataFolder,
                                      Reference<IClusterConnectionRecord> ccr,
                                      Reference<ConfigNode> configNode,
                                      ConfigBroadcastInterface cbi) {
	state UID myID = deterministicRandom()->randomUniqueID();
	state LeaderElectionRegInterface myLeaderInterface(g_network);
	state GenerationRegInterface myInterface(g_network);
	state OnDemandStore store(dataFolder, myID, "coordination-");
	state ConfigTransactionInterface configTransactionInterface;
	state ConfigFollowerInterface configFollowerInterface;
	state Future<Void> configDatabaseServer = Never();

	state std::shared_ptr<CoordinatorRequestCounter> requestCounter = std::make_shared<CoordinatorRequestCounter>();
	state Future<Void> requestCounterLogger = coordinatorRequestCounterLogger(requestCounter);

	TraceEvent("CoordinationServer", myID)
	    .detail("MyInterfaceAddr", myInterface.read.getEndpoint().getPrimaryAddress())
	    .detail("Folder", dataFolder)
	    .detail("ConfigNodeValid", configNode.isValid());

	if (configNode.isValid()) {
		configTransactionInterface.setupWellKnownEndpoints();
		configFollowerInterface.setupWellKnownEndpoints();
		configDatabaseServer =
		    brokenPromiseToNever(configNode->serve(cbi, configTransactionInterface, configFollowerInterface));
	}

	try {
		wait(localGenerationReg(myInterface, &store, requestCounter) ||
		     leaderServer(myLeaderInterface, &store, myID, ccr, requestCounter) || store.getError() ||
		     configDatabaseServer);
		throw internal_error();
	} catch (Error& e) {
		TraceEvent("CoordinationServerError", myID).errorUnsuppressed(e);
		throw;
	}
}

ACTOR Future<Void> changeClusterDescription(std::string datafolder, KeyRef newClusterKey, KeyRef oldClusterKey) {
	state UID myID = deterministicRandom()->randomUniqueID();
	state OnDemandStore store(datafolder, myID, "coordination-");
	RangeResult res = wait(store->readRange(allKeys));
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
			GenerationRegVal regVal = BinaryReader::fromStringRef<GenerationRegVal>(value, IncludeVersion());
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
	wait(store->commit());
	return Void();
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
			if (fileEntry.rfind("coordination-", 0) == 0) {
				isCoord = true;
			}
		}
		if (!isCoord)
			continue;
		futures.push_back(changeClusterDescription(processDir, newClusterKey, oldClusterKey));
	}
	return waitForAll(futures);
}
