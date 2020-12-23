/*
 * Coordination.actor.cpp
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

#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/Status.h"
#include "flow/ActorCollection.h"
#include "flow/ProtocolVersion.h"
#include "flow/UnitTest.h"
#include "flow/IndexedSet.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/actorcompiler.h"  // This must be the last #include.
#include "flow/network.h"
#include <cstdint>

// This module implements coordinationServer() and the interfaces in CoordinationInterface.h

struct GenerationRegVal {
	UniqueGeneration readGen, writeGen;
	Optional<Value> val;

	//To change this serialization, ProtocolVersion::GenerationRegVal must be updated, and downgrades need to be considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, readGen, writeGen, val);
	}
};

GenerationRegInterface::GenerationRegInterface( NetworkAddress remote )
	: read( Endpoint({remote}, WLTOKEN_GENERATIONREG_READ) ),
	  write( Endpoint({remote}, WLTOKEN_GENERATIONREG_WRITE) )
{
}

GenerationRegInterface::GenerationRegInterface( INetwork* local )
{
	read.makeWellKnownEndpoint( WLTOKEN_GENERATIONREG_READ, TaskPriority::Coordination );
	write.makeWellKnownEndpoint( WLTOKEN_GENERATIONREG_WRITE, TaskPriority::Coordination );
}

LeaderElectionRegInterface::LeaderElectionRegInterface(NetworkAddress remote)
	: ClientLeaderRegInterface(remote),
	  candidacy( Endpoint({remote}, WLTOKEN_LEADERELECTIONREG_CANDIDACY) ),
    electionResult( Endpoint({remote}, WLTOKEN_LEADERELECTIONREG_ELECTIONRESULT) ),
	  leaderHeartbeat( Endpoint({remote}, WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT) ),
	  forward( Endpoint({remote}, WLTOKEN_LEADERELECTIONREG_FORWARD) )
{
}

LeaderElectionRegInterface::LeaderElectionRegInterface(INetwork* local) 
	: ClientLeaderRegInterface(local)
{
	candidacy.makeWellKnownEndpoint( WLTOKEN_LEADERELECTIONREG_CANDIDACY, TaskPriority::Coordination );
	electionResult.makeWellKnownEndpoint( WLTOKEN_LEADERELECTIONREG_ELECTIONRESULT, TaskPriority::Coordination );
	leaderHeartbeat.makeWellKnownEndpoint( WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT, TaskPriority::Coordination );
	forward.makeWellKnownEndpoint( WLTOKEN_LEADERELECTIONREG_FORWARD, TaskPriority::Coordination );
}

ServerCoordinators::ServerCoordinators( Reference<ClusterConnectionFile> cf )
	: ClientCoordinators(cf)
{
	ClusterConnectionString cs = ccf->getConnectionString();
	for(auto s = cs.coordinators().begin(); s != cs.coordinators().end(); ++s) {
		leaderElectionServers.push_back( LeaderElectionRegInterface( *s ) );
		stateServers.push_back( GenerationRegInterface( *s ) );
	}
}

// The coordination server wants to create its key value store only if it is actually used
struct OnDemandStore {
public:
	OnDemandStore( std::string folder, UID myID ) : folder(folder), store(nullptr), myID(myID) {}
	~OnDemandStore() { if (store) store->close(); }

	IKeyValueStore* get() {
		if (!store) open();
		return store;
	}

	bool exists() {
		if (store)
			return true;
		return fileExists( joinPath(folder, "coordination-0.fdq") ) || fileExists( joinPath(folder, "coordination-1.fdq") ) || fileExists( joinPath(folder, "coordination.fdb") );
	}

	IKeyValueStore* operator->() { return get(); }

	Future<Void> getError() { return onErr(err.getFuture()); }

private:
	std::string folder;
	UID myID;
	IKeyValueStore* store;
	Promise<Future<Void>> err;

	ACTOR static Future<Void> onErr( Future<Future<Void>> e ) {
		Future<Void> f = wait(e);
		wait(f);
		return Void();
	}

	void open() {
		platform::createDirectory( folder );
		store = keyValueStoreMemory( joinPath(folder, "coordination-"), myID, 500e6 );
		err.send( store->getError() );
	}
};

ACTOR Future<Void> localGenerationReg( GenerationRegInterface interf, OnDemandStore* pstore ) {
	state GenerationRegVal v;
	state OnDemandStore& store = *pstore;
	// SOMEDAY: concurrent access to different keys?
	loop choose {
		when ( GenerationRegReadRequest _req = waitNext( interf.read.getFuture() ) ) {
			TraceEvent("GenerationRegReadRequest").detail("From", _req.reply.getEndpoint().getPrimaryAddress()).detail("K", _req.key);
			state GenerationRegReadRequest req = _req;
			Optional<Value> rawV = wait( store->readValue( req.key ) );
			v = rawV.present() ? BinaryReader::fromStringRef<GenerationRegVal>( rawV.get(), IncludeVersion() ) : GenerationRegVal();
			TraceEvent("GenerationRegReadReply").detail("RVSize", rawV.present() ? rawV.get().size() : -1).detail("VWG", v.writeGen.generation);
			if (v.readGen < req.gen) {
				v.readGen = req.gen;
				store->set( KeyValueRef( req.key, BinaryWriter::toValue(v, IncludeVersion(ProtocolVersion::withGenerationRegVal())) ) );
				wait(store->commit());
			}
			req.reply.send( GenerationRegReadReply( v.val, v.writeGen, v.readGen ) );
		}
		when ( GenerationRegWriteRequest _wrq = waitNext( interf.write.getFuture() ) ) {
			state GenerationRegWriteRequest wrq = _wrq;
			Optional<Value> rawV = wait( store->readValue( wrq.kv.key ) );
			v = rawV.present() ? BinaryReader::fromStringRef<GenerationRegVal>( rawV.get(), IncludeVersion() ) : GenerationRegVal();
			if (v.readGen <= wrq.gen && v.writeGen < wrq.gen) {
				v.writeGen = wrq.gen;
				v.val = wrq.kv.value;
				store->set( KeyValueRef( wrq.kv.key, BinaryWriter::toValue(v, IncludeVersion(ProtocolVersion::withGenerationRegVal())) ) );
				wait(store->commit());
				TraceEvent("GenerationRegWrote").detail("From", wrq.reply.getEndpoint().getPrimaryAddress()).detail("Key", wrq.kv.key)
					.detail("ReqGen", wrq.gen.generation).detail("Returning", v.writeGen.generation);
				wrq.reply.send( v.writeGen );
			} else {
				TraceEvent("GenerationRegWriteFail").detail("From", wrq.reply.getEndpoint().getPrimaryAddress()).detail("Key", wrq.kv.key)
					.detail("ReqGen", wrq.gen.generation).detail("ReadGen", v.readGen.generation).detail("WriteGen", v.writeGen.generation);
				wrq.reply.send( std::max( v.readGen, v.writeGen ) );
			}
		}
	}
}

TEST_CASE("/fdbserver/Coordination/localGenerationReg/simple") {
	state GenerationRegInterface reg;
	state OnDemandStore store("simfdb/unittests/", //< FIXME
		deterministicRandom()->randomUniqueID());
	state Future<Void> actor = localGenerationReg(reg, &store);
	state Key the_key(deterministicRandom()->randomAlphaNumeric( deterministicRandom()->randomInt(0, 10)));

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
		UniqueGeneration g = wait(reg.write.getReply(GenerationRegWriteRequest(KeyValueRef(the_key, LiteralStringRef("Value1")), firstGen)));
		//   (gen1==gen is considered a "successful" write)
		ASSERT(g == firstGen);
	}

	{
		GenerationRegReadReply r = wait(reg.read.getReply(GenerationRegReadRequest(the_key, UniqueGeneration())));
		// read(key,gen2) returns (value,gen,rgen).
		//     There was some earlier or concurrent write(key,value,gen).
		ASSERT(r.value == LiteralStringRef("Value1"));
		ASSERT(r.gen == firstGen);
		//     There was some earlier or concurrent read(key,rgen).
		ASSERT(r.rgen == firstGen);
		//     If there is a write(key,_,gen1)=>gen1 s.t. gen1 < gen2 OR the write completed before this read started, then gen >= gen1.
		ASSERT(r.gen >= firstGen);
		//     If there is a read(key,gen1) that completed before this read started, then rgen >= gen1
		ASSERT(r.rgen >= firstGen);

		ASSERT(!actor.isReady());
	}
	return Void();
}

ACTOR Future<Void> openDatabase(ClientData* db, int* clientCount, Reference<AsyncVar<bool>> hasConnectedClients, OpenDatabaseCoordRequest req) {
	++(*clientCount);
	hasConnectedClients->set(true);
	
	if(req.supportedVersions.size() > 0) {
		db->clientStatusInfoMap[req.reply.getEndpoint().getPrimaryAddress()] = ClientStatusInfo(req.traceLogGroup, req.supportedVersions, req.issues);
	}

	while (db->clientInfo->get().read().id == req.knownClientInfoID && !db->clientInfo->get().read().forward.present()) {
		choose {
			when (wait( yieldedFuture(db->clientInfo->onChange()) )) {}
			when (wait( delayJittered( SERVER_KNOBS->CLIENT_REGISTER_INTERVAL ) )) { break; }  // The client might be long gone!
		}
	}

	if(req.supportedVersions.size() > 0) {
		db->clientStatusInfoMap.erase(req.reply.getEndpoint().getPrimaryAddress());
	}

	req.reply.send( db->clientInfo->get() );

	if(--(*clientCount) == 0) {
		hasConnectedClients->set(false);
	}

	return Void();
}

ACTOR Future<Void> remoteMonitorLeader( int* clientCount, Reference<AsyncVar<bool>> hasConnectedClients, Reference<AsyncVar<Optional<LeaderInfo>>> currentElectedLeader, ElectionResultRequest req ) {
	++(*clientCount);
	hasConnectedClients->set(true);

	while (!currentElectedLeader->get().present() || req.knownLeader == currentElectedLeader->get().get().changeID) {
		choose {
			when (wait( yieldedFuture(currentElectedLeader->onChange()) ) ) {}
			when (wait( delayJittered( SERVER_KNOBS->CLIENT_REGISTER_INTERVAL ) )) { break; }
		}
	}

	req.reply.send( currentElectedLeader->get() );

	if(--(*clientCount) == 0) {
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
	state Future<Void> notifyCheck = delay(SERVER_KNOBS->NOTIFICATION_FULL_CLEAR_TIME / SERVER_KNOBS->MIN_NOTIFICATIONS);
	state ClientData clientData;
	state int clientCount = 0;
	state Reference<AsyncVar<bool>> hasConnectedClients = makeReference<AsyncVar<bool>>(false);
	state ActorCollection actors(false);
	state Future<Void> leaderMon;
	state AsyncVar<Value> leaderInterface;
	state Reference<AsyncVar<Optional<LeaderInfo>>> currentElectedLeader =
	    makeReference<AsyncVar<Optional<LeaderInfo>>>();

	loop choose {
		when ( OpenDatabaseCoordRequest req = waitNext( interf.openDatabase.getFuture() ) ) {
			if (clientData.clientInfo->get().read().id != req.knownClientInfoID && !clientData.clientInfo->get().read().forward.present()) {
				req.reply.send(clientData.clientInfo->get());
			} else {
				if(!leaderMon.isValid()) {
					leaderMon = monitorLeaderForProxies(req.clusterKey, req.coordinators, &clientData, currentElectedLeader);
				}
				actors.add(openDatabase(&clientData, &clientCount, hasConnectedClients, req));
			}
		}
		when ( ElectionResultRequest req = waitNext( interf.electionResult.getFuture() ) ) {
			if (currentElectedLeader->get().present() && req.knownLeader != currentElectedLeader->get().get().changeID) {
				req.reply.send(currentElectedLeader->get());
			} else {
				if(!leaderMon.isValid()) {
					leaderMon = monitorLeaderForProxies(req.key, req.coordinators, &clientData, currentElectedLeader);
				}
				actors.add(remoteMonitorLeader(&clientCount, hasConnectedClients, currentElectedLeader, req));
			}
		}
		when ( GetLeaderRequest req = waitNext( interf.getLeader.getFuture() ) ) {
			if (currentNominee.present() && currentNominee.get().changeID != req.knownLeader) {
				req.reply.send( currentNominee.get() );
			} else {
				notify.push_back( req.reply );
				if(notify.size() > SERVER_KNOBS->MAX_NOTIFICATIONS) {
					TraceEvent(SevWarnAlways, "TooManyNotifications").detail("Amount", notify.size());
					for (uint32_t i=0; i<notify.size(); i++)
						notify[i].send( currentNominee.get() );
					notify.clear();
				} else if(!nextInterval.isValid()) {
					nextInterval = delay(0);
				}
			}
		}
		when ( CandidacyRequest req = waitNext( interf.candidacy.getFuture() ) ) {
			if(!nextInterval.isValid()) {
				nextInterval = delay(0);
			}
			availableCandidates.erase( LeaderInfo(req.prevChangeID) );
			availableCandidates.insert( req.myInfo );
			if (currentNominee.present() && currentNominee.get().changeID != req.knownLeader) {
				req.reply.send( currentNominee.get() );
			} else {
				notify.push_back( req.reply );
				if(notify.size() > SERVER_KNOBS->MAX_NOTIFICATIONS) {
					TraceEvent(SevWarnAlways, "TooManyNotifications").detail("Amount", notify.size());
					for (uint32_t i=0; i<notify.size(); i++)
						notify[i].send( currentNominee.get() );
					notify.clear();
				}
			}
		}
		when (LeaderHeartbeatRequest req = waitNext( interf.leaderHeartbeat.getFuture() ) ) {
			if(!nextInterval.isValid()) {
				nextInterval = delay(0);
			}
			//TODO: use notify to only send a heartbeat once per interval
			availableLeaders.erase( LeaderInfo(req.prevChangeID) );
			availableLeaders.insert( req.myInfo );
			req.reply.send(
			    LeaderHeartbeatReply{ currentNominee.present() && currentNominee.get().equalInternalId(req.myInfo) });
		}
		when (ForwardRequest req = waitNext( interf.forward.getFuture() ) ) {
			LeaderInfo newInfo;
			newInfo.forward = true;
			newInfo.serializedInfo = req.conn.toString();
			for(unsigned int i=0; i<notify.size(); i++)
				notify[i].send( newInfo );
			notify.clear();
			ClientDBInfo outInfo;
			outInfo.id = deterministicRandom()->randomUniqueID();
			outInfo.forward = req.conn.toString();
			clientData.clientInfo->set(CachedSerialization<ClientDBInfo>(outInfo));
			req.reply.send( Void() );
			if(!hasConnectedClients->get()) {
				return Void();
			}
			nextInterval = Future<Void>();
		}
		when ( wait(nextInterval.isValid() ? nextInterval : Never()) ) {
			if (!availableLeaders.size() && !availableCandidates.size() && !notify.size() &&
				!currentNominee.present())
			{
				// Our state is back to the initial state, so we can safely stop this actor
				TraceEvent("EndingLeaderNomination").detail("Key", key).detail("HasConnectedClients", hasConnectedClients->get());
				if(!hasConnectedClients->get()) {
					return Void();
				} else {
					nextInterval = Future<Void>();
				}
			} else {
				Optional<LeaderInfo> nextNominee;
				if( availableCandidates.size() && (!availableLeaders.size() || availableLeaders.begin()->leaderChangeRequired(*availableCandidates.begin())) ) {
					nextNominee = *availableCandidates.begin();
				} else if( availableLeaders.size() ) {
					nextNominee = *availableLeaders.begin();
				}

				if( !currentNominee.present() || !nextNominee.present() || !currentNominee.get().equalInternalId(nextNominee.get()) || nextNominee.get() > currentNominee.get() ) {
					TraceEvent("NominatingLeader").detail("NextNominee", nextNominee.present() ? nextNominee.get().changeID : UID())
					.detail("CurrentNominee", currentNominee.present() ? currentNominee.get().changeID : UID()).detail("Key", printable(key));
					for(unsigned int i=0; i<notify.size(); i++)
						notify[i].send( nextNominee );
					notify.clear();
				}

				currentNominee = nextNominee;

				if( availableLeaders.size() ) {
					nextInterval = delay( SERVER_KNOBS->POLLING_FREQUENCY );
					if(leaderIntervalCount++ > 5) {
						candidateDelay = SERVER_KNOBS->CANDIDATE_MIN_DELAY;
					}
				} else {
					nextInterval = delay( candidateDelay );
					candidateDelay = std::min(SERVER_KNOBS->CANDIDATE_MAX_DELAY, candidateDelay * SERVER_KNOBS->CANDIDATE_GROWTH_RATE);
					leaderIntervalCount = 0;
				}

				availableLeaders.clear();
				availableCandidates.clear();
			}
		}
		when( wait(notifyCheck) ) {
			notifyCheck = delay( SERVER_KNOBS->NOTIFICATION_FULL_CLEAR_TIME / std::max<double>(SERVER_KNOBS->MIN_NOTIFICATIONS, notify.size()) );
			if(!notify.empty() && currentNominee.present()) {
				notify.front().send( currentNominee.get() );
				notify.pop_front();
			}
		}
		when( wait(hasConnectedClients->onChange()) ) {
			if(!hasConnectedClients->get() && !nextInterval.isValid()) {
				TraceEvent("LeaderRegisterUnneeded").detail("Key", key);
				return Void();
			}
		}
		when( wait(actors.getResult()) ) {}
	}
}

// Generation register values are stored without prefixing in the coordinated state, but always begin with an alphanumeric character
// (they are always derived from a ClusterConnectionString key).
// Forwarding values are stored in this range:
const KeyRangeRef fwdKeys( LiteralStringRef( "\xff" "fwd" ), LiteralStringRef( "\xff" "fwe" ) );

struct LeaderRegisterCollection {
	// SOMEDAY: Factor this into a generic tool?  Extend ActorCollection to support removal actions?  What?
	ActorCollection actors;
	Map<Key, LeaderElectionRegInterface> registerInterfaces;
	Map<Key, LeaderInfo> forward;
	OnDemandStore *pStore;

	LeaderRegisterCollection( OnDemandStore *pStore ) : actors( false ), pStore( pStore ) {}

	ACTOR static Future<Void> init( LeaderRegisterCollection *self ) {
		if( !self->pStore->exists() )
			return Void();
		OnDemandStore &store = *self->pStore;
		Standalone<RangeResultRef> forwardingInfo = wait( store->readRange( fwdKeys ) );
		for( int i = 0; i < forwardingInfo.size(); i++ ) {
			LeaderInfo forwardInfo;
			forwardInfo.forward = true;
			forwardInfo.serializedInfo = forwardingInfo[i].value;
			self->forward[ forwardingInfo[i].key.removePrefix( fwdKeys.begin ) ] = forwardInfo;
		}
		return Void();
	}

	Future<Void> onError() { return actors.getResult(); }

	Optional<LeaderInfo> getForward(KeyRef key) {
		auto i = forward.find( key );
		if (i == forward.end())
			return Optional<LeaderInfo>();
		return i->value;
	}

	ACTOR static Future<Void> setForward(LeaderRegisterCollection *self, KeyRef key, ClusterConnectionString conn) {
		LeaderInfo forwardInfo;
		forwardInfo.forward = true;
		forwardInfo.serializedInfo = conn.toString();
		self->forward[ key ] = forwardInfo;
		OnDemandStore &store = *self->pStore;
		store->set( KeyValueRef( key.withPrefix( fwdKeys.begin ), conn.toString() ) );
		wait(store->commit());
		return Void();
	}

	LeaderElectionRegInterface& getInterface(KeyRef key, UID id) {
		auto i = registerInterfaces.find( key );
		if (i == registerInterfaces.end()) {
			Key k = key;
			Future<Void> a = wrap(this, k, leaderRegister(registerInterfaces[k], k), id);
			if (a.isError()) throw a.getError();
			ASSERT( !a.isReady() );
			actors.add( a );
			i  = registerInterfaces.find( key );
		}
		ASSERT( i != registerInterfaces.end() );
		return i->value;
	}

	ACTOR static Future<Void> wrap( LeaderRegisterCollection* self, Key key, Future<Void> actor, UID id ) {
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
		if (e.code() != invalid_error_code) throw e;
		return Void();
	}

};

// leaderServer multiplexes multiple leaderRegisters onto a single LeaderElectionRegInterface,
// creating and destroying them on demand.
ACTOR Future<Void> leaderServer(LeaderElectionRegInterface interf, OnDemandStore *pStore, UID id) {
	state LeaderRegisterCollection regs( pStore );
	state ActorCollection forwarders(false);

	wait( LeaderRegisterCollection::init( &regs ) ); 

	loop choose {
		when ( OpenDatabaseCoordRequest req = waitNext( interf.openDatabase.getFuture() ) ) {
			Optional<LeaderInfo> forward = regs.getForward(req.clusterKey);
			if( forward.present() ) {
				ClientDBInfo info;
				info.id = deterministicRandom()->randomUniqueID();
				info.forward = forward.get().serializedInfo;
				req.reply.send( CachedSerialization<ClientDBInfo>(info) );
			} else {
				regs.getInterface(req.clusterKey, id).openDatabase.send( req );
			}
		}
		when ( ElectionResultRequest req = waitNext( interf.electionResult.getFuture() ) ) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if( forward.present() ) {
				req.reply.send( forward.get() );
			} else {
				regs.getInterface(req.key, id).electionResult.send( req );
			}
		}
		when ( GetLeaderRequest req = waitNext( interf.getLeader.getFuture() ) ) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if( forward.present() )
				req.reply.send( forward.get() );
			else
				regs.getInterface(req.key, id).getLeader.send( req );
		}
		when ( CandidacyRequest req = waitNext( interf.candidacy.getFuture() ) ) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if( forward.present() )
				req.reply.send( forward.get() );
			else
				regs.getInterface(req.key, id).candidacy.send(req);
		}
		when ( LeaderHeartbeatRequest req = waitNext( interf.leaderHeartbeat.getFuture() ) ) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if( forward.present() )
				req.reply.send(LeaderHeartbeatReply{ false });
			else
				regs.getInterface(req.key, id).leaderHeartbeat.send(req);
		}
		when ( ForwardRequest req = waitNext( interf.forward.getFuture() ) ) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if( forward.present() )
				req.reply.send( Void() );
			else {
				forwarders.add( LeaderRegisterCollection::setForward( &regs, req.key, ClusterConnectionString(req.conn.toString()) ) );
				regs.getInterface(req.key, id).forward.send(req);
			}
		}
		when( wait( forwarders.getResult() ) ) { ASSERT(false); throw internal_error(); }
	}
}

ACTOR Future<Void> coordinationServer(std::string dataFolder) {
	state UID myID = deterministicRandom()->randomUniqueID();
	state LeaderElectionRegInterface myLeaderInterface( g_network );
	state GenerationRegInterface myInterface( g_network );
	state OnDemandStore store( dataFolder, myID );

	TraceEvent("CoordinationServer", myID).detail("MyInterfaceAddr", myInterface.read.getEndpoint().getPrimaryAddress()).detail("Folder", dataFolder);

	try {
		wait( localGenerationReg(myInterface, &store) || leaderServer(myLeaderInterface, &store, myID) || store.getError() );
		throw internal_error();
	} catch (Error& e) {
		TraceEvent("CoordinationServerError", myID).error(e, true);
		throw;
	}
}
