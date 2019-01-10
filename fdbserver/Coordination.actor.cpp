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
#include "flow/ActorCollection.h"
#include "fdbserver/Knobs.h"
#include "flow/UnitTest.h"
#include "flow/IndexedSet.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

// This module implements coordinationServer() and the interfaces in CoordinationInterface.h

struct GenerationRegVal {
	UniqueGeneration readGen, writeGen;
	Optional<Value> val;
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, readGen, writeGen, val);
	}
};

// UID WLTOKEN_CLIENTLEADERREG_GETLEADER( -1, 2 ); // from fdbclient/MonitorLeader.actor.cpp
UID WLTOKEN_LEADERELECTIONREG_CANDIDACY( -1, 3 );
UID WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT( -1, 4 );
UID WLTOKEN_LEADERELECTIONREG_FORWARD( -1, 5 );
UID WLTOKEN_GENERATIONREG_READ( -1, 6 );
UID WLTOKEN_GENERATIONREG_WRITE( -1, 7 );

GenerationRegInterface::GenerationRegInterface( NetworkAddress remote )
	: read( Endpoint(remote, WLTOKEN_GENERATIONREG_READ) ),
		write( Endpoint(remote, WLTOKEN_GENERATIONREG_WRITE) )
{
}

GenerationRegInterface::GenerationRegInterface( INetwork* local )
{
	read.makeWellKnownEndpoint( WLTOKEN_GENERATIONREG_READ, TaskCoordination );
	write.makeWellKnownEndpoint( WLTOKEN_GENERATIONREG_WRITE, TaskCoordination );
}

LeaderElectionRegInterface::LeaderElectionRegInterface(NetworkAddress remote)
	: ClientLeaderRegInterface(remote),
	  candidacy( Endpoint(remote, WLTOKEN_LEADERELECTIONREG_CANDIDACY) ),
	  leaderHeartbeat( Endpoint(remote, WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT) ),
	  forward( Endpoint(remote, WLTOKEN_LEADERELECTIONREG_FORWARD) )
{
}

LeaderElectionRegInterface::LeaderElectionRegInterface(INetwork* local) 
	: ClientLeaderRegInterface(local)
{
	candidacy.makeWellKnownEndpoint( WLTOKEN_LEADERELECTIONREG_CANDIDACY, TaskCoordination );
	leaderHeartbeat.makeWellKnownEndpoint( WLTOKEN_LEADERELECTIONREG_LEADERHEARTBEAT, TaskCoordination );
	forward.makeWellKnownEndpoint( WLTOKEN_LEADERELECTIONREG_FORWARD, TaskCoordination );
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
	OnDemandStore( std::string folder, UID myID ) : folder(folder), store(NULL), myID(myID) {}
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
			TraceEvent("GenerationRegReadRequest").detail("From", _req.reply.getEndpoint().address).detail("K", printable(_req.key));
			state GenerationRegReadRequest req = _req;
			Optional<Value> rawV = wait( store->readValue( req.key ) );
			v = rawV.present() ? BinaryReader::fromStringRef<GenerationRegVal>( rawV.get(), IncludeVersion() ) : GenerationRegVal();
			TraceEvent("GenerationRegReadReply").detail("RVSize", rawV.present() ? rawV.get().size() : -1).detail("VWG", v.writeGen.generation);
			if (v.readGen < req.gen) {
				v.readGen = req.gen;
				store->set( KeyValueRef( req.key, BinaryWriter::toValue(v, IncludeVersion()) ) );
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
				store->set( KeyValueRef( wrq.kv.key, BinaryWriter::toValue(v, IncludeVersion()) ) );
				wait(store->commit());
				TraceEvent("GenerationRegWrote").detail("From", wrq.reply.getEndpoint().address).detail("Key", printable(wrq.kv.key))
					.detail("ReqGen", wrq.gen.generation).detail("Returning", v.writeGen.generation);
				wrq.reply.send( v.writeGen );
			} else {
				TraceEvent("GenerationRegWriteFail").detail("From", wrq.reply.getEndpoint().address).detail("Key", printable(wrq.kv.key))
					.detail("ReqGen", wrq.gen.generation).detail("ReadGen", v.readGen.generation).detail("WriteGen", v.writeGen.generation);
				wrq.reply.send( std::max( v.readGen, v.writeGen ) );
			}
		}
	}
};

TEST_CASE("/fdbserver/Coordination/localGenerationReg/simple") {
	state GenerationRegInterface reg;
	state OnDemandStore store("simfdb/unittests/", //< FIXME
		g_random->randomUniqueID());
	state Future<Void> actor = localGenerationReg(reg, &store);
	state Key the_key = g_random->randomAlphaNumeric( g_random->randomInt(0, 10) );

	state UniqueGeneration firstGen(0, g_random->randomUniqueID());

	GenerationRegReadReply r = wait(reg.read.getReply(GenerationRegReadRequest(the_key, firstGen)));
	//   If there was no prior write(_,_,0) or a data loss fault, 
	//     returns (Optional(),0,gen2)
	ASSERT(!r.value.present());
	ASSERT(r.gen == UniqueGeneration());
	ASSERT(r.rgen == firstGen); 

	UniqueGeneration g = wait(reg.write.getReply(GenerationRegWriteRequest(KeyValueRef(the_key, LiteralStringRef("Value1")), firstGen)));
	//   (gen1==gen is considered a "successful" write)
	ASSERT(g == firstGen);

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
	state Future<Void> nextInterval = delay( 0 );
	state double candidateDelay = SERVER_KNOBS->CANDIDATE_MIN_DELAY;
	state int leaderIntervalCount = 0;
	state Future<Void> notifyCheck = delay(SERVER_KNOBS->NOTIFICATION_FULL_CLEAR_TIME / SERVER_KNOBS->MIN_NOTIFICATIONS);

	loop choose {
		when ( GetLeaderRequest req = waitNext( interf.getLeader.getFuture() ) ) {
			if (currentNominee.present() && currentNominee.get().changeID != req.knownLeader) {
				req.reply.send( currentNominee.get() );
			} else {
				notify.push_back( req.reply );
				if(notify.size() > SERVER_KNOBS->MAX_NOTIFICATIONS) {
					TraceEvent(SevWarnAlways, "TooManyNotifications").detail("Amount", notify.size());
					for(int i=0; i<notify.size(); i++)
						notify[i].send( currentNominee.get() );
					notify.clear();
				}
			}
		}
		when ( CandidacyRequest req = waitNext( interf.candidacy.getFuture() ) ) {
			//TraceEvent("CandidacyRequest").detail("Nominee", req.myInfo.changeID );
			availableCandidates.erase( LeaderInfo(req.prevChangeID) );
			availableCandidates.insert( req.myInfo );
			if (currentNominee.present() && currentNominee.get().changeID != req.knownLeader) {
				req.reply.send( currentNominee.get() );
			} else {
				notify.push_back( req.reply );
				if(notify.size() > SERVER_KNOBS->MAX_NOTIFICATIONS) {
					TraceEvent(SevWarnAlways, "TooManyNotifications").detail("Amount", notify.size());
					for(int i=0; i<notify.size(); i++)
						notify[i].send( currentNominee.get() );
					notify.clear();
				}
			}
		}
		when (LeaderHeartbeatRequest req = waitNext( interf.leaderHeartbeat.getFuture() ) ) {
			//TODO: use notify to only send a heartbeat once per interval
			availableLeaders.erase( LeaderInfo(req.prevChangeID) );
			availableLeaders.insert( req.myInfo );
			req.reply.send( currentNominee.present() && currentNominee.get().equalInternalId(req.myInfo) );
		}
		when (ForwardRequest req = waitNext( interf.forward.getFuture() ) ) {
			LeaderInfo newInfo;
			newInfo.forward = true;
			newInfo.serializedInfo = req.conn.toString();
			for(int i=0; i<notify.size(); i++)
				notify[i].send( newInfo );
			notify.clear();
			req.reply.send( Void() );
			return Void();
		}
		when ( wait(nextInterval) ) {
			if (!availableLeaders.size() && !availableCandidates.size() && !notify.size() &&
				!currentNominee.present())
			{
				// Our state is back to the initial state, so we can safely stop this actor
				TraceEvent("EndingLeaderNomination").detail("Key", printable(key));
				return Void();
			} else {
				Optional<LeaderInfo> nextNominee;
				if (availableLeaders.size() && availableCandidates.size()) {
					nextNominee = ( *availableLeaders.begin() < *availableCandidates.begin() ) ? *availableLeaders.begin() : *availableCandidates.begin();
				} else if (availableLeaders.size()) {
					nextNominee = *availableLeaders.begin();
				} else if (availableCandidates.size()) {
					nextNominee = *availableCandidates.begin();
				} else {
					nextNominee = Optional<LeaderInfo>();
				}

				bool foundCurrentNominee = false;
				if(currentNominee.present()) {
					for(auto& it : availableLeaders) {
						if(currentNominee.get().equalInternalId(it)) {
							foundCurrentNominee = true;
							break;
						}
					}
				}

				if ( !nextNominee.present() || !foundCurrentNominee || currentNominee.get().leaderChangeRequired(nextNominee.get()) ) {
					TraceEvent("NominatingLeader").detail("Nominee", nextNominee.present() ? nextNominee.get().changeID : UID())
						.detail("Changed", nextNominee != currentNominee).detail("Key", printable(key));
					for(int i=0; i<notify.size(); i++)
						notify[i].send( nextNominee );
					notify.clear();
					currentNominee = nextNominee;
				} else if (currentNominee.get().equalInternalId(nextNominee.get())) {
					// leader becomes better
					currentNominee = nextNominee;
				}

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
		Standalone<VectorRef<KeyValueRef>> forwardingInfo = wait( store->readRange( fwdKeys ) );
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

	LeaderElectionRegInterface& getInterface(KeyRef key) {
		auto i = registerInterfaces.find( key );
		if (i == registerInterfaces.end()) {
			Key k = key;
			Future<Void> a = wrap(this, k, leaderRegister(registerInterfaces[k], k) );
			if (a.isError()) throw a.getError();
			ASSERT( !a.isReady() );
			actors.add( a );
			i  = registerInterfaces.find( key );
		}
		ASSERT( i != registerInterfaces.end() );
		return i->value;
	}

	ACTOR static Future<Void> wrap( LeaderRegisterCollection* self, Key key, Future<Void> actor ) {
		state Error e;
		try { 
			wait(actor); 
		} catch (Error& err) {
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
ACTOR Future<Void> leaderServer(LeaderElectionRegInterface interf, OnDemandStore *pStore) {
	state LeaderRegisterCollection regs( pStore );
	state ActorCollection forwarders(false);

	wait( LeaderRegisterCollection::init( &regs ) ); 

	loop choose {
		when ( GetLeaderRequest req = waitNext( interf.getLeader.getFuture() ) ) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if( forward.present() )
				req.reply.send( forward.get() );
			else
				regs.getInterface(req.key).getLeader.send( req );
		}
		when ( CandidacyRequest req = waitNext( interf.candidacy.getFuture() ) ) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if( forward.present() )
				req.reply.send( forward.get() );
			else
				regs.getInterface(req.key).candidacy.send(req);
		}
		when ( LeaderHeartbeatRequest req = waitNext( interf.leaderHeartbeat.getFuture() ) ) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if( forward.present() )
				req.reply.send( false );
			else
				regs.getInterface(req.key).leaderHeartbeat.send(req);
		}
		when ( ForwardRequest req = waitNext( interf.forward.getFuture() ) ) {
			Optional<LeaderInfo> forward = regs.getForward(req.key);
			if( forward.present() )
				req.reply.send( Void() );
			else {
				forwarders.add( LeaderRegisterCollection::setForward( &regs, req.key, ClusterConnectionString(req.conn.toString()) ) );
				regs.getInterface(req.key).forward.send(req);
			}
		}
		when( wait( forwarders.getResult() ) ) { ASSERT(false); throw internal_error(); }
	}
}

ACTOR Future<Void> coordinationServer(std::string dataFolder) {
	state UID myID = g_random->randomUniqueID();
	state LeaderElectionRegInterface myLeaderInterface( g_network );
	state GenerationRegInterface myInterface( g_network );
	state OnDemandStore store( dataFolder, myID );

	TraceEvent("CoordinationServer", myID).detail("MyInterfaceAddr", myInterface.read.getEndpoint().address).detail("Folder", dataFolder);

	try {
		wait( localGenerationReg(myInterface, &store) || leaderServer(myLeaderInterface, &store) || store.getError() );
		throw internal_error();
	} catch (Error& e) {
		TraceEvent("CoordinationServerError", myID).error(e, true);
		throw;
	}
}
