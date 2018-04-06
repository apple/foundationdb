/*
 * LeaderElection.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/actorcompiler.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Locality.h"
#include "ClusterRecruitmentInterface.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbclient/MonitorLeader.h"

extern Optional<LeaderInfo> getLeader( vector<Optional<LeaderInfo>> nominees );

ACTOR Future<Void> submitCandidacy( Key key, LeaderElectionRegInterface coord, LeaderInfo myInfo, UID prevChangeID, Reference<AsyncVar<vector<Optional<LeaderInfo>>>> nominees, int index ) {
	loop {
		auto const& nom = nominees->get()[index];
		Optional<LeaderInfo> li = wait( retryBrokenPromise( coord.candidacy, CandidacyRequest( key, myInfo, nom.present() ? nom.get().changeID : UID(), prevChangeID ), TaskCoordinationReply ) );

		if (li != nominees->get()[index]) {
			vector<Optional<LeaderInfo>> v = nominees->get();
			v[index] = li;
			nominees->set(v);

			if( li.present() && li.get().forward )
				Void _ = wait( Future<Void>(Never()) );

			Void _ = wait( Future<Void>(Void()) ); // Make sure we weren't cancelled
		}
	}
}

ACTOR template <class T> Future<Void> buggifyDelayedAsyncVar( Reference<AsyncVar<T>> in, Reference<AsyncVar<T>> out ) {
	try {
		loop {
			Void _ = wait( delay( SERVER_KNOBS->BUGGIFIED_EVENTUAL_CONSISTENCY * g_random->random01() ) );
			out->set( in->get() );
			Void _ = wait( in->onChange() );
		}
	} catch (Error& e) {
		out->set( in->get() );
		throw;
	}
}

template <class T>
Future<Void> buggifyDelayedAsyncVar( Reference<AsyncVar<T>> &var ) {
	Reference<AsyncVar<T>> in( new AsyncVar<T> );
	auto f = buggifyDelayedAsyncVar(in, var);
	var = in;
	return f;
}

ACTOR Future<Void> changeLeaderCoordinators( ServerCoordinators coordinators, Value forwardingInfo ) {
	std::vector<Future<Void>> forwardRequests;
	for( int i = 0; i < coordinators.leaderElectionServers.size(); i++ )
		forwardRequests.push_back( retryBrokenPromise( coordinators.leaderElectionServers[i].forward, ForwardRequest( coordinators.clusterKey, forwardingInfo ) ) );
	int quorum_size = forwardRequests.size()/2 + 1;
	Void _ = wait( quorum( forwardRequests, quorum_size ) );
	return Void();
}

ACTOR Future<Void> tryBecomeLeaderInternal( ServerCoordinators coordinators, Value proposedSerializedInterface, Reference<AsyncVar<Value>> outSerializedLeader, bool hasConnected, Reference<AsyncVar<ProcessClass>> asyncProcessClass, Reference<AsyncVar<bool>> asyncIsExcluded ) {
	state Reference<AsyncVar<vector<Optional<LeaderInfo>>>> nominees( new AsyncVar<vector<Optional<LeaderInfo>>>() );
	state LeaderInfo myInfo;
	state Future<Void> candidacies;
	state bool iAmLeader = false;
	state UID prevChangeID;

	if( asyncProcessClass->get().machineClassFitness(ProcessClass::ClusterController) > ProcessClass::UnsetFit || asyncIsExcluded->get() ) {
		Void _ = wait( delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY) );
	}

	nominees->set( vector<Optional<LeaderInfo>>( coordinators.clientLeaderServers.size() ) );

	myInfo.serializedInfo = proposedSerializedInterface;
	outSerializedLeader->set( Value() );

	state Future<Void> buggifyDelay = (SERVER_KNOBS->BUGGIFY_ALL_COORDINATION || BUGGIFY) ? buggifyDelayedAsyncVar( outSerializedLeader ) : Void();

	while (!iAmLeader) {
		state Future<Void> badCandidateTimeout;

		myInfo.changeID = g_random->randomUniqueID();
		prevChangeID = myInfo.changeID;
		myInfo.updateChangeID( asyncProcessClass->get().machineClassFitness(ProcessClass::ClusterController), asyncIsExcluded->get() );

		vector<Future<Void>> cand;
		for(int i=0; i<coordinators.leaderElectionServers.size(); i++)
			cand.push_back( submitCandidacy( coordinators.clusterKey, coordinators.leaderElectionServers[i], myInfo, prevChangeID, nominees, i ) );
		candidacies = waitForAll(cand);

		loop {
			state Optional<LeaderInfo> leader = getLeader( nominees->get() );
			if( leader.present() && leader.get().forward ) {
				// These coordinators are forwarded to another set.  But before we change our own cluster file, we need to make
				// sure that a majority of coordinators know that.
				// SOMEDAY: Wait briefly to see if other coordinators will tell us they already know, to save communication?
				Void _ = wait( changeLeaderCoordinators( coordinators, leader.get().serializedInfo ) );

				if(!hasConnected) {
					TraceEvent(SevWarnAlways, "IncorrectClusterFileContentsAtConnection").detail("Filename", coordinators.ccf->getFilename())
						.detail("ConnectionStringFromFile", coordinators.ccf->getConnectionString().toString())
						.detail("CurrentConnectionString", leader.get().serializedInfo.toString());
				}
				coordinators.ccf->setConnectionString( ClusterConnectionString( leader.get().serializedInfo.toString() ) );
				TraceEvent("LeaderForwarding").detail("ConnStr", coordinators.ccf->getConnectionString().toString());
				throw coordinators_changed();
			}

			if (leader.present()) {
				hasConnected = true;
				coordinators.ccf->notifyConnected();
			}

			if (leader.present() && leader.get().changeID == myInfo.changeID) {
				TraceEvent("BecomingLeader", myInfo.changeID);
				ASSERT( leader.get().serializedInfo == proposedSerializedInterface );
				outSerializedLeader->set( leader.get().serializedInfo );
				iAmLeader = true;
				break;
			}
			if (leader.present()) {
				TraceEvent("LeaderChanged", myInfo.changeID).detail("ToID", leader.get().changeID);
				if (leader.get().serializedInfo != proposedSerializedInterface) // We never set outSerializedLeader to our own interface unless we are ready to become leader!
					outSerializedLeader->set( leader.get().serializedInfo );
			}

			// If more than 2*SERVER_KNOBS->POLLING_FREQUENCY elapses while we are nominated by some coordinator but there is no leader,
			// we might be breaking the leader election process for someone with better communications but lower ID, so change IDs.
			if (!leader.present() && std::count( nominees->get().begin(), nominees->get().end(), myInfo )) {
				if (!badCandidateTimeout.isValid())
					badCandidateTimeout = delay( SERVER_KNOBS->POLLING_FREQUENCY*2, TaskCoordinationReply );
			} else
				badCandidateTimeout = Future<Void>();

			choose {
				when (Void _ = wait( nominees->onChange() )) {}
				when (Void _ = wait( badCandidateTimeout.isValid() ? badCandidateTimeout : Never() )) {
					TEST(true); // Bad candidate timeout
					TraceEvent("LeaderBadCandidateTimeout", myInfo.changeID);
					break;
				}
				when (Void _ = wait(candidacies)) { ASSERT(false); }
				when (Void _ = wait( asyncProcessClass->onChange() || asyncIsExcluded->onChange() )) {
					break;
				}
			}
		}

		candidacies.cancel();
	}

	ASSERT( iAmLeader && outSerializedLeader->get() == proposedSerializedInterface );

	loop {
		prevChangeID = myInfo.changeID;
		myInfo.updateChangeID( asyncProcessClass->get().machineClassFitness(ProcessClass::ClusterController), asyncIsExcluded->get() );
		if (myInfo.changeID != prevChangeID) {
			TraceEvent("ChangeLeaderChangeID").detail("PrevChangeID", prevChangeID).detail("NewChangeID", myInfo.changeID);
		}

		state vector<Future<Void>> true_heartbeats;
		state vector<Future<Void>> false_heartbeats;
		for(int i=0; i<coordinators.leaderElectionServers.size(); i++) {
			Future<bool> hb = retryBrokenPromise( coordinators.leaderElectionServers[i].leaderHeartbeat, LeaderHeartbeatRequest( coordinators.clusterKey, myInfo, prevChangeID ), TaskCoordinationReply );
			true_heartbeats.push_back( onEqual(hb, true) );
			false_heartbeats.push_back( onEqual(hb, false) );
		}

		state Future<Void> rate = delay( SERVER_KNOBS->HEARTBEAT_FREQUENCY, TaskCoordinationReply ); // SOMEDAY: Move to server side?

		choose {
			when ( Void _ = wait( quorum( true_heartbeats, true_heartbeats.size()/2+1 ) ) ) {
				//TraceEvent("StillLeader", myInfo.changeID);
			} // We are still leader
			when ( Void _ = wait( quorum( false_heartbeats, false_heartbeats.size()/2+1 ) ) ) {
				TraceEvent("ReplacedAsLeader", myInfo.changeID);
				break; } // We are definitely not leader
			when ( Void _ = wait( delay(SERVER_KNOBS->POLLING_FREQUENCY) ) ) {
				for(int i = 0; i < coordinators.leaderElectionServers.size(); ++i) {
					if(true_heartbeats[i].isReady())
						TraceEvent("LeaderTrueHeartbeat", myInfo.changeID).detail("Coordinator", coordinators.leaderElectionServers[i].candidacy.getEndpoint().address);
					else if(false_heartbeats[i].isReady())
						TraceEvent("LeaderFalseHeartbeat", myInfo.changeID).detail("Coordinator", coordinators.leaderElectionServers[i].candidacy.getEndpoint().address);
					else
						TraceEvent("LeaderNoHeartbeat", myInfo.changeID).detail("Coordinator", coordinators.leaderElectionServers[i].candidacy.getEndpoint().address);
				}
				TraceEvent("ReleasingLeadership", myInfo.changeID);
				break; } // Give up on being leader, because we apparently have poor communications
		}

		Void _ = wait( rate );
	}

	if (SERVER_KNOBS->BUGGIFY_ALL_COORDINATION || BUGGIFY) Void _ = wait( delay( SERVER_KNOBS->BUGGIFIED_EVENTUAL_CONSISTENCY * g_random->random01() ) );

	return Void(); // We are no longer leader
}