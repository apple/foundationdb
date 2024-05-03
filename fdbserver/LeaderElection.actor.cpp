/*
 * LeaderElection.actor.cpp
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

#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Keep trying to become a leader by submitting itself to all coordinators.
// Monitor the health of all coordinators at the same time.
ACTOR Future<Void> submitCandidacy(Key key,
                                   LeaderElectionRegInterface coord,
                                   LeaderInfo myInfo,
                                   UID prevChangeID,
                                   AsyncTrigger* nomineeChange,
                                   Optional<LeaderInfo>* nominee) {
	loop {
		state Optional<LeaderInfo> li;
		if (coord.hostname.present()) {
			wait(store(
			    li,
			    retryGetReplyFromHostname(
			        CandidacyRequest(key, myInfo, nominee->present() ? nominee->get().changeID : UID(), prevChangeID),
			        coord.hostname.get(),
			        WLTOKEN_LEADERELECTIONREG_CANDIDACY,
			        TaskPriority::CoordinationReply)));
		} else {
			wait(store(
			    li,
			    retryBrokenPromise(
			        coord.candidacy,
			        CandidacyRequest(key, myInfo, nominee->present() ? nominee->get().changeID : UID(), prevChangeID),
			        TaskPriority::CoordinationReply)));
		}

		wait(Future<Void>(Void())); // Make sure we weren't cancelled

		if (li != *nominee) {
			*nominee = li;
			nomineeChange->trigger();

			if (li.present() && li.get().forward)
				wait(Future<Void>(Never()));
		}
	}
}

ACTOR template <class T>
Future<Void> buggifyDelayedAsyncVar(Reference<AsyncVar<T>> in, Reference<AsyncVar<T>> out) {
	try {
		loop {
			wait(delay(SERVER_KNOBS->BUGGIFIED_EVENTUAL_CONSISTENCY * deterministicRandom()->random01()));
			out->set(in->get());
			wait(in->onChange());
		}
	} catch (Error& e) {
		out->set(in->get());
		throw;
	}
}

template <class T>
Future<Void> buggifyDelayedAsyncVar(Reference<AsyncVar<T>>& var) {
	auto in = makeReference<AsyncVar<T>>();
	auto f = buggifyDelayedAsyncVar(in, var);
	var = in;
	return f;
}

ACTOR Future<Void> changeLeaderCoordinators(ServerCoordinators coordinators, Value forwardingInfo) {
	std::vector<Future<Void>> forwardRequests;
	forwardRequests.reserve(coordinators.leaderElectionServers.size());
	for (int i = 0; i < coordinators.leaderElectionServers.size(); i++) {
		if (coordinators.leaderElectionServers[i].hostname.present()) {
			forwardRequests.push_back(retryGetReplyFromHostname(ForwardRequest(coordinators.clusterKey, forwardingInfo),
			                                                    coordinators.leaderElectionServers[i].hostname.get(),
			                                                    WLTOKEN_LEADERELECTIONREG_FORWARD));
		} else {
			forwardRequests.push_back(retryBrokenPromise(coordinators.leaderElectionServers[i].forward,
			                                             ForwardRequest(coordinators.clusterKey, forwardingInfo)));
		}
	}
	int quorum_size = forwardRequests.size() / 2 + 1;
	wait(quorum(forwardRequests, quorum_size));
	return Void();
}

ACTOR Future<Void> tryBecomeLeaderInternal(ServerCoordinators coordinators,
                                           Value proposedSerializedInterface,
                                           Reference<AsyncVar<Value>> outSerializedLeader,
                                           bool hasConnected,
                                           Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo) {
	state AsyncTrigger nomineeChange;
	state std::vector<Optional<LeaderInfo>> nominees;
	state LeaderInfo myInfo;
	state Future<Void> candidacies;
	state bool iAmLeader = false;
	state UID prevChangeID;

	if (asyncPriorityInfo->get().dcFitness == ClusterControllerPriorityInfo::FitnessBad ||
	    asyncPriorityInfo->get().dcFitness == ClusterControllerPriorityInfo::FitnessRemote ||
	    asyncPriorityInfo->get().dcFitness == ClusterControllerPriorityInfo::FitnessNotPreferred ||
	    asyncPriorityInfo->get().isExcluded) {
		wait(delay(SERVER_KNOBS->WAIT_FOR_GOOD_REMOTE_RECRUITMENT_DELAY));
	} else if (asyncPriorityInfo->get().processClassFitness > ProcessClass::UnsetFit) {
		wait(delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
	}

	nominees.resize(coordinators.leaderElectionServers.size());

	myInfo.serializedInfo = proposedSerializedInterface;
	outSerializedLeader->set(Value());

	state Future<Void> buggifyDelay =
	    (SERVER_KNOBS->BUGGIFY_ALL_COORDINATION || BUGGIFY) ? buggifyDelayedAsyncVar(outSerializedLeader) : Void();

	while (!iAmLeader) {
		state Future<Void> badCandidateTimeout;

		myInfo.changeID = deterministicRandom()->randomUniqueID();
		prevChangeID = myInfo.changeID;
		myInfo.updateChangeID(asyncPriorityInfo->get());

		std::vector<Future<Void>> cand;
		cand.reserve(coordinators.leaderElectionServers.size());
		for (int i = 0; i < coordinators.leaderElectionServers.size(); i++) {
			cand.push_back(submitCandidacy(coordinators.clusterKey,
			                               coordinators.leaderElectionServers[i],
			                               myInfo,
			                               prevChangeID,
			                               &nomineeChange,
			                               &nominees[i]));
		}
		candidacies = waitForAll(cand);

		loop {
			state Optional<std::pair<LeaderInfo, bool>> leader = getLeader(nominees);
			if (leader.present() && leader.get().first.forward) {
				// These coordinators are forwarded to another set.  But before we change our own cluster file, we need
				// to make sure that a majority of coordinators know that. SOMEDAY: Wait briefly to see if other
				// coordinators will tell us they already know, to save communication?
				// NOTE: If a majority of coordinators (in the current connection string) have failed then we can
				// end up waiting here indefinitely. Try to make progress in that scenario by proceeding with the
				// connection string that we have received. Not a great solution, but can help in certain scenarios.
				choose {
					when(wait(changeLeaderCoordinators(coordinators, leader.get().first.serializedInfo))) {}
					when(wait(delay(20))) {}
				}

				if (!hasConnected) {
					TraceEvent(SevWarnAlways, "IncorrectClusterFileContentsAtConnection")
					    .detail("ClusterFile", coordinators.ccr->toString())
					    .detail("StoredConnectionString", coordinators.ccr->getConnectionString().toString())
					    .detail("CurrentConnectionString", leader.get().first.serializedInfo.toString());
				}
				wait(coordinators.ccr->setAndPersistConnectionString(
				    ClusterConnectionString(leader.get().first.serializedInfo.toString())));
				TraceEvent("LeaderForwarding")
				    .detail("ConnStr", coordinators.ccr->getConnectionString().toString())
				    .trackLatest("LeaderForwarding");
				throw coordinators_changed();
			}

			if (leader.present() && leader.get().second) {
				hasConnected = true;
				coordinators.ccr->notifyConnected();
			}

			if (leader.present() && leader.get().second && leader.get().first.equalInternalId(myInfo)) {
				TraceEvent("BecomingLeader", myInfo.changeID).log();
				ASSERT(leader.get().first.serializedInfo == proposedSerializedInterface);
				outSerializedLeader->set(leader.get().first.serializedInfo);
				iAmLeader = true;
				break;
			}
			if (leader.present()) {
				TraceEvent("LeaderChanged", myInfo.changeID).detail("ToID", leader.get().first.changeID);
				if (leader.get().first.serializedInfo !=
				    proposedSerializedInterface) // We never set outSerializedLeader to our own interface unless we are
				                                 // ready to become leader!
					outSerializedLeader->set(leader.get().first.serializedInfo);
			}

			// If more than 2*SERVER_KNOBS->POLLING_FREQUENCY elapses while we are nominated by some coordinator but
			// there is no leader, we might be breaking the leader election process for someone with better
			// communications but lower ID, so change IDs.
			if ((!leader.present() || !leader.get().second) && std::count(nominees.begin(), nominees.end(), myInfo)) {
				if (!badCandidateTimeout.isValid())
					badCandidateTimeout = delay(SERVER_KNOBS->POLLING_FREQUENCY * 2, TaskPriority::CoordinationReply);
			} else
				badCandidateTimeout = Future<Void>();

			choose {
				when(wait(nomineeChange.onTrigger())) {}
				when(wait(badCandidateTimeout.isValid() ? badCandidateTimeout : Never())) {
					CODE_PROBE(true, "Bad candidate timeout");
					TraceEvent("LeaderBadCandidateTimeout", myInfo.changeID).log();
					break;
				}
				when(wait(candidacies)) {
					ASSERT(false);
				}
				when(wait(asyncPriorityInfo->onChange())) {
					break;
				}
			}
		}

		candidacies.cancel();
	}

	ASSERT(iAmLeader && outSerializedLeader->get() == proposedSerializedInterface);

	loop {
		prevChangeID = myInfo.changeID;
		myInfo.updateChangeID(asyncPriorityInfo->get());
		if (myInfo.changeID != prevChangeID) {
			TraceEvent("ChangeLeaderChangeID")
			    .detail("PrevChangeID", prevChangeID)
			    .detail("NewChangeID", myInfo.changeID);
		}

		state std::vector<Future<Void>> true_heartbeats;
		state std::vector<Future<Void>> false_heartbeats;
		for (int i = 0; i < coordinators.leaderElectionServers.size(); i++) {
			Future<LeaderHeartbeatReply> hb;
			if (coordinators.leaderElectionServers[i].hostname.present()) {
				hb = retryGetReplyFromHostname(LeaderHeartbeatRequest(coordinators.clusterKey, myInfo, prevChangeID),
				                               coordinators.leaderElectionServers[i].hostname.get(),
				                               WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT,
				                               TaskPriority::CoordinationReply);
			} else {
				hb = retryBrokenPromise(coordinators.leaderElectionServers[i].leaderHeartbeat,
				                        LeaderHeartbeatRequest(coordinators.clusterKey, myInfo, prevChangeID),
				                        TaskPriority::CoordinationReply);
			}
			true_heartbeats.push_back(onEqual(hb, LeaderHeartbeatReply{ true }));
			false_heartbeats.push_back(onEqual(hb, LeaderHeartbeatReply{ false }));
		}

		state Future<Void> rate = delay(SERVER_KNOBS->HEARTBEAT_FREQUENCY, TaskPriority::CoordinationReply) ||
		                          asyncPriorityInfo->onChange(); // SOMEDAY: Move to server side?

		choose {
			when(wait(quorum(true_heartbeats, true_heartbeats.size() / 2 + 1))) {
				//TraceEvent("StillLeader", myInfo.changeID);
			} // We are still leader
			when(wait(quorum(false_heartbeats, false_heartbeats.size() / 2 + 1))) {
				TraceEvent("ReplacedAsLeader", myInfo.changeID).log();
				break;
			} // We are definitely not leader
			when(wait(delay(SERVER_KNOBS->POLLING_FREQUENCY))) {
				for (int i = 0; i < coordinators.leaderElectionServers.size(); ++i) {
					if (true_heartbeats[i].isReady())
						TraceEvent("LeaderTrueHeartbeat", myInfo.changeID)
						    .detail("Coordinator",
						            coordinators.leaderElectionServers[i].candidacy.getEndpoint().getPrimaryAddress());
					else if (false_heartbeats[i].isReady())
						TraceEvent("LeaderFalseHeartbeat", myInfo.changeID)
						    .detail("Coordinator",
						            coordinators.leaderElectionServers[i].candidacy.getEndpoint().getPrimaryAddress());
					else
						TraceEvent("LeaderNoHeartbeat", myInfo.changeID)
						    .detail("Coordinator",
						            coordinators.leaderElectionServers[i].candidacy.getEndpoint().getPrimaryAddress());
				}
				TraceEvent("ReleasingLeadership", myInfo.changeID).log();
				break;
			} // Give up on being leader, because we apparently have poor communications
			when(wait(asyncPriorityInfo->onChange())) {}
		}

		wait(rate);
	}

	if (SERVER_KNOBS->BUGGIFY_ALL_COORDINATION || BUGGIFY)
		wait(delay(SERVER_KNOBS->BUGGIFIED_EVENTUAL_CONSISTENCY * deterministicRandom()->random01()));

	return Void(); // We are no longer leader
}
