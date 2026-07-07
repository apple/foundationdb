/*
 * LeaderElection.cpp
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

#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/core/CoordinationInterface.h"
#include "fdbserver/core/Knobs.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/CoroUtils.h"

// Keep trying to become a leader by submitting itself to all coordinators.
// Monitor the health of all coordinators at the same time.
Future<Void> submitCandidacy(Key key,
                             LeaderElectionRegInterface coord,
                             LeaderInfo myInfo,
                             UID prevChangeID,
                             AsyncTrigger* nomineeChange,
                             Optional<LeaderInfo>* nominee) {
	while (true) {
		Optional<LeaderInfo> li;
		if (coord.hostname.present()) {
			li = co_await retryGetReplyFromHostname(
			    CandidacyRequest(key, myInfo, nominee->present() ? nominee->get().changeID : UID(), prevChangeID),
			    coord.hostname.get(),
			    WLTOKEN_LEADERELECTIONREG_CANDIDACY,
			    TaskPriority::CoordinationReply);
		} else {
			li = co_await retryBrokenPromise(
			    coord.candidacy,
			    CandidacyRequest(key, myInfo, nominee->present() ? nominee->get().changeID : UID(), prevChangeID),
			    TaskPriority::CoordinationReply);
		}

		co_await Future<Void>(Void()); // Make sure we weren't cancelled

		if (li != *nominee) {
			*nominee = li;
			nomineeChange->trigger();

			if (li.present() && li.get().forward)
				co_await Future<Void>(Never());
		}
	}
}

template <class T>
Future<Void> buggifyDelayedAsyncVar(Reference<AsyncVar<T>> in, Reference<AsyncVar<T>> out) {
	try {
		while (true) {
			co_await delay(SERVER_KNOBS->BUGGIFIED_EVENTUAL_CONSISTENCY * deterministicRandom()->random01());
			out->set(in->get());
			co_await in->onChange();
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

namespace {

Future<Void> changeLeaderCoordinatorsImpl(ServerCoordinators coordinators, Value forwardingInfo) {
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
	co_await quorum(forwardRequests, quorum_size);
}

} // namespace

Future<Void> changeLeaderCoordinators(ServerCoordinators const& coordinators, Value const& forwardingInfo) {
	return changeLeaderCoordinatorsImpl(coordinators, forwardingInfo);
}

Future<Void> tryBecomeLeaderInternal(ServerCoordinators coordinators,
                                     Value proposedSerializedInterface,
                                     Reference<AsyncVar<Value>> outSerializedLeader,
                                     bool hasConnected,
                                     Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo) {
	AsyncTrigger nomineeChange;
	std::vector<Optional<LeaderInfo>> nominees;
	LeaderInfo myInfo;
	Future<Void> candidacies;
	bool iAmLeader{ false };
	UID prevChangeID;

	if (asyncPriorityInfo->get().dcFitness == ClusterControllerPriorityInfo::FitnessBad ||
	    asyncPriorityInfo->get().dcFitness == ClusterControllerPriorityInfo::FitnessRemote ||
	    asyncPriorityInfo->get().dcFitness == ClusterControllerPriorityInfo::FitnessNotPreferred ||
	    asyncPriorityInfo->get().isExcluded) {
		co_await delay(SERVER_KNOBS->WAIT_FOR_GOOD_REMOTE_RECRUITMENT_DELAY);
	} else if (asyncPriorityInfo->get().processClassFitness > ProcessClass::UnsetFit) {
		co_await delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY);
	}

	nominees.resize(coordinators.leaderElectionServers.size());

	myInfo.serializedInfo = proposedSerializedInterface;
	outSerializedLeader->set(Value());

	[[maybe_unused]] Future<Void> buggifyDelay =
	    (SERVER_KNOBS->BUGGIFY_ALL_COORDINATION || buggify()) ? buggifyDelayedAsyncVar(outSerializedLeader) : Void();

	while (!iAmLeader) {
		Future<Void> badCandidateTimeout;

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

		while (true) {
			Optional<std::pair<LeaderInfo, bool>> leader = getLeader(nominees);
			if (leader.present() && leader.get().first.forward) {
				// These coordinators are forwarded to another set.  But before we change our own cluster file, we need
				// to make sure that a majority of coordinators know that. SOMEDAY: Wait briefly to see if other
				// coordinators will tell us they already know, to save communication?
				// NOTE: If a majority of coordinators (in the current connection string) have failed then we can
				// end up waiting here indefinitely. Try to make progress in that scenario by proceeding with the
				// connection string that we have received. Not a great solution, but can help in certain scenarios.
				co_await race(changeLeaderCoordinators(coordinators, leader.get().first.serializedInfo), delay(20));

				if (!hasConnected) {
					TraceEvent(SevWarnAlways, "IncorrectClusterFileContentsAtConnection")
					    .detail("ClusterFile", coordinators.ccr->toString())
					    .detail("StoredConnectionString", coordinators.ccr->getConnectionString().toString())
					    .detail("CurrentConnectionString", leader.get().first.serializedInfo.toString());
				}
				co_await coordinators.ccr->setAndPersistConnectionString(
				    ClusterConnectionString(leader.get().first.serializedInfo.toString()));
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
			if ((!leader.present() || !leader.get().second) &&
			    std::find(nominees.begin(), nominees.end(), myInfo) != nominees.end()) {
				if (!badCandidateTimeout.isValid())
					badCandidateTimeout = delay(SERVER_KNOBS->POLLING_FREQUENCY * 2, TaskPriority::CoordinationReply);
			} else {
				badCandidateTimeout = Future<Void>();
			}

			{
				auto res = co_await race(nomineeChange.onTrigger(),
				                         badCandidateTimeout.isValid() ? badCandidateTimeout : Never(),
				                         candidacies,
				                         asyncPriorityInfo->onChange());
				if (res.index() == 1) {
					CODE_PROBE(true, "Bad candidate timeout");
					TraceEvent("LeaderBadCandidateTimeout", myInfo.changeID).log();
					break;
				} else if (res.index() == 2) {
					ASSERT(false);
				} else if (res.index() == 3) {
					break;
				} else if (res.index() != 0) {
					UNREACHABLE();
				}
			}
		}

		candidacies.cancel();
	}

	ASSERT(iAmLeader && outSerializedLeader->get() == proposedSerializedInterface);

	while (true) {
		prevChangeID = myInfo.changeID;
		myInfo.updateChangeID(asyncPriorityInfo->get());
		if (myInfo.changeID != prevChangeID) {
			TraceEvent("ChangeLeaderChangeID")
			    .detail("PrevChangeID", prevChangeID)
			    .detail("NewChangeID", myInfo.changeID);
		}

		std::vector<Future<Void>> true_heartbeats;
		std::vector<Future<Void>> false_heartbeats;
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

		Future<Void> rate = delay(SERVER_KNOBS->HEARTBEAT_FREQUENCY, TaskPriority::CoordinationReply) ||
		                    asyncPriorityInfo->onChange(); // SOMEDAY: Move to server side?

		{
			Future<Void> trueHeartbeatQuorum = quorum(true_heartbeats, true_heartbeats.size() / 2 + 1);
			Future<Void> falseHeartbeatQuorum = quorum(false_heartbeats, false_heartbeats.size() / 2 + 1);
			Future<Void> pollingTimeout = delay(SERVER_KNOBS->POLLING_FREQUENCY);
			Future<Void> priorityInfoChanged = asyncPriorityInfo->onChange();
			auto res = co_await race(trueHeartbeatQuorum, falseHeartbeatQuorum, pollingTimeout, priorityInfoChanged);
			if (res.index() == 0) {
				// TraceEvent("StillLeader", myInfo.changeID);
			} else if (res.index() == 1) {
				TraceEvent("ReplacedAsLeader", myInfo.changeID).log();
				break;
			} else if (res.index() == 2) {
				for (int i = 0; i < coordinators.leaderElectionServers.size(); ++i) {
					if (true_heartbeats[i].isReady()) {
						TraceEvent("LeaderTrueHeartbeat", myInfo.changeID)
						    .detail("Coordinator",
						            coordinators.leaderElectionServers[i].candidacy.getEndpoint().getPrimaryAddress());
					} else if (false_heartbeats[i].isReady()) {
						TraceEvent("LeaderFalseHeartbeat", myInfo.changeID)
						    .detail("Coordinator",
						            coordinators.leaderElectionServers[i].candidacy.getEndpoint().getPrimaryAddress());
					} else {
						TraceEvent("LeaderNoHeartbeat", myInfo.changeID)
						    .detail("Coordinator",
						            coordinators.leaderElectionServers[i].candidacy.getEndpoint().getPrimaryAddress());
					}
				}
				TraceEvent("ReleasingLeadership", myInfo.changeID).log();
				break;
			} else if (res.index() != 3) {
				UNREACHABLE();
			}
		}

		co_await rate;
	}

	if (SERVER_KNOBS->BUGGIFY_ALL_COORDINATION || buggify())
		co_await delay(SERVER_KNOBS->BUGGIFIED_EVENTUAL_CONSISTENCY * deterministicRandom()->random01());
}
