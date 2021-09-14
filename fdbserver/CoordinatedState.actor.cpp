/*
 * CoordinatedState.actor.cpp
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

#include "fdbserver/CoordinatedState.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/Knobs.h"
#include "flow/ActorCollection.h"
#include "fdbserver/LeaderElection.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<GenerationRegReadReply> waitAndSendRead(RequestStream<GenerationRegReadRequest> to,
                                                     GenerationRegReadRequest req) {
	if (SERVER_KNOBS->BUGGIFY_ALL_COORDINATION || BUGGIFY)
		wait(delay(SERVER_KNOBS->BUGGIFIED_EVENTUAL_CONSISTENCY * deterministicRandom()->random01()));
	state GenerationRegReadReply reply = wait(retryBrokenPromise(to, req));
	if (SERVER_KNOBS->BUGGIFY_ALL_COORDINATION || BUGGIFY)
		wait(delay(SERVER_KNOBS->BUGGIFIED_EVENTUAL_CONSISTENCY * deterministicRandom()->random01()));
	return reply;
}

ACTOR Future<UniqueGeneration> waitAndSendWrite(RequestStream<GenerationRegWriteRequest> to,
                                                GenerationRegWriteRequest req) {
	if (SERVER_KNOBS->BUGGIFY_ALL_COORDINATION || BUGGIFY)
		wait(delay(SERVER_KNOBS->BUGGIFIED_EVENTUAL_CONSISTENCY * deterministicRandom()->random01()));
	state UniqueGeneration reply = wait(retryBrokenPromise(to, req));
	if (SERVER_KNOBS->BUGGIFY_ALL_COORDINATION || BUGGIFY)
		wait(delay(SERVER_KNOBS->BUGGIFIED_EVENTUAL_CONSISTENCY * deterministicRandom()->random01()));
	return reply;
}

ACTOR Future<GenerationRegReadReply> emptyToNever(Future<GenerationRegReadReply> f) {
	state GenerationRegReadReply r = wait(f);
	if (r.gen.generation == 0)
		wait(Future<Void>(Never()));
	return r;
}

ACTOR Future<GenerationRegReadReply> nonemptyToNever(Future<GenerationRegReadReply> f) {
	state GenerationRegReadReply r = wait(f);
	if (r.gen.generation != 0)
		wait(Future<Void>(Never()));
	return r;
}

class CoordinatedState : NonCopyable {
	ServerCoordinators coordinators;
	int stage;
	UniqueGeneration gen;
	uint64_t conflictGen;
	bool doomed;
	ActorCollection ac; // Errors are not reported
	bool initial;

	bool isDoomed(GenerationRegReadReply const& rep) const {
		return rep.gen >
		       gen; // setExclusive is doomed, because there was a write at least started at a higher generation, which
		            // means a read completed at that higher generation
		            // || rep.rgen > gen // setExclusive isn't absolutely doomed, but it may/probably will fail
	}

	ACTOR static Future<Value> read(CoordinatedState* self) {
		ASSERT(self->stage == 0);

		{
			self->stage = 1;
			GenerationRegReadReply rep = wait(self->replicatedRead(
			    self, GenerationRegReadRequest(self->coordinators.clusterKey, UniqueGeneration())));
			self->conflictGen = std::max(self->conflictGen, std::max(rep.gen.generation, rep.rgen.generation)) + 1;
			self->gen = UniqueGeneration(self->conflictGen, deterministicRandom()->randomUniqueID());
		}

		{
			self->stage = 2;
			GenerationRegReadReply rep =
			    wait(self->replicatedRead(self, GenerationRegReadRequest(self->coordinators.clusterKey, self->gen)));
			self->stage = 3;
			self->conflictGen = std::max(self->conflictGen, std::max(rep.gen.generation, rep.rgen.generation));
			if (self->isDoomed(rep))
				self->doomed = true;
			self->initial = rep.gen.generation == 0;

			self->stage = 4;
			return rep.value.present() ? rep.value.get() : Value();
		}
	}
	ACTOR static Future<Void> onConflict(CoordinatedState* self) {
		ASSERT(self->stage == 4);
		if (self->doomed)
			return Void();
		loop {
			wait(delay(SERVER_KNOBS->COORDINATED_STATE_ONCONFLICT_POLL_INTERVAL));
			GenerationRegReadReply rep = wait(self->replicatedRead(
			    self, GenerationRegReadRequest(self->coordinators.clusterKey, UniqueGeneration())));
			if (self->stage > 4)
				break;
			self->conflictGen = std::max(self->conflictGen, std::max(rep.gen.generation, rep.rgen.generation));
			if (self->isDoomed(rep))
				return Void();
		}
		wait(Future<Void>(Never()));
		return Void();
	}
	ACTOR static Future<Void> setExclusive(CoordinatedState* self, Value v) {
		ASSERT(self->stage == 4);
		self->stage = 5;

		UniqueGeneration wgen = wait(self->replicatedWrite(
		    self, GenerationRegWriteRequest(KeyValueRef(self->coordinators.clusterKey, v), self->gen)));
		self->stage = 6;

		TraceEvent("CoordinatedStateSet")
		    .detail("Gen", self->gen.generation)
		    .detail("Wgen", wgen.generation)
		    .detail("Genu", self->gen.uid)
		    .detail("Wgenu", wgen.uid)
		    .detail("Cgen", self->conflictGen);

		if (wgen == self->gen)
			return Void();
		else {
			self->conflictGen = std::max(self->conflictGen, wgen.generation);
			throw coordinated_state_conflict();
		}
	}

	ACTOR static Future<GenerationRegReadReply> replicatedRead(CoordinatedState* self, GenerationRegReadRequest req) {
		state std::vector<GenerationRegInterface>& replicas = self->coordinators.stateServers;
		state vector<Future<GenerationRegReadReply>> rep_empty_reply;
		state vector<Future<GenerationRegReadReply>> rep_reply;
		for (int i = 0; i < replicas.size(); i++) {
			Future<GenerationRegReadReply> reply =
			    waitAndSendRead(replicas[i].read, GenerationRegReadRequest(req.key, req.gen));
			rep_empty_reply.push_back(nonemptyToNever(reply));
			rep_reply.push_back(emptyToNever(reply));
			self->ac.add(success(reply));
		}

		state Future<Void> majorityEmpty =
		    quorum(rep_empty_reply,
		           (replicas.size() + 1) / 2); // enough empty to ensure we cannot achieve a majority non-empty
		wait(quorum(rep_reply, replicas.size() / 2 + 1) || majorityEmpty);

		if (majorityEmpty.isReady()) {
			int best = -1;
			for (int i = 0; i < rep_empty_reply.size(); i++)
				if (rep_empty_reply[i].isReady() && !rep_empty_reply[i].isError()) {
					if (best < 0 || rep_empty_reply[i].get().rgen > rep_empty_reply[best].get().rgen)
						best = i;
				}
			ASSERT(best >= 0);
			auto result = rep_empty_reply[best].get();
			return result;
		} else {
			int best = -1;
			for (int i = 0; i < rep_reply.size(); i++)
				if (rep_reply[i].isReady() && !rep_reply[i].isError()) {
					if (best < 0 || rep_reply[i].get().gen > rep_reply[best].get().gen ||
					    (rep_reply[i].get().gen == rep_reply[best].get().gen &&
					     rep_reply[i].get().rgen > rep_reply[best].get().rgen))
						best = i;
				}
			ASSERT(best >= 0);
			auto result = rep_reply[best].get();
			return result;
		}
	}

	ACTOR static Future<UniqueGeneration> replicatedWrite(CoordinatedState* self, GenerationRegWriteRequest req) {
		state std::vector<GenerationRegInterface>& replicas = self->coordinators.stateServers;
		state vector<Future<UniqueGeneration>> wrep_reply;
		for (int i = 0; i < replicas.size(); i++) {
			Future<UniqueGeneration> reply =
			    waitAndSendWrite(replicas[i].write, GenerationRegWriteRequest(req.kv, req.gen));
			wrep_reply.push_back(reply);
			self->ac.add(success(reply));
		}

		wait(quorum(wrep_reply, self->initial ? replicas.size() : replicas.size() / 2 + 1));

		UniqueGeneration maxGen;
		for (int i = 0; i < wrep_reply.size(); i++)
			if (wrep_reply[i].isReady())
				maxGen = std::max(maxGen, wrep_reply[i].get());
		return maxGen;
	}

public:
	// Callers must ensure that any outstanding operations have been cancelled before destructing *this!
	CoordinatedState(ServerCoordinators const& c)
	  : coordinators(c), stage(0), conflictGen(0), doomed(false), ac(false), initial(false) {}

	// May only be called once.
	// Returns the most recent state if there are no concurrent calls to setExclusive
	// Otherwise might return the state passed to a concurrent call, even if that call ultimately fails.
	// Don't count on the result of this read being part of the serialized history until a subsequent setExclusive has
	// succeeded!
	Future<Value> read() { return read(this); }

	Future<Void> onConflict() { return onConflict(this); }

	// May only be called once, and only after read returns.
	// Eventually returns Void if a call to setExclusive would fail.
	// May or may not return or throw an error after setExclusive is called.
	// (Generally?) doesn't return unless there is some concurrent call to read or setExclusive.

	// read() must have been called and returned first, and this may only be called once.
	// Attempts to change the state value, provided that the value returned by read is still the
	//   most recent.
	// If it returns Void, the state was successfully changed and the state returned by read was
	//   the most recent before the new state.
	// If it throws coordinated_state_conflict, the state may or may not have been changed, and the value
	//   returned from read may or may not ever have been a valid state.  Probably there was a
	//   call to read() or setExclusive() concurrently with this pair.
	Future<Void> setExclusive(Value v) { return setExclusive(this, v); }

	uint64_t getConflict() const { return conflictGen; }
};

struct MovableValue {
	enum MoveState { MaybeTo = 1, Active = 2, MovingFrom = 3 };

	Value value;
	int32_t mode;
	Optional<Value> other; // a cluster connection string

	MovableValue() : mode(Active) {}
	MovableValue(Value const& v, int mode, Optional<Value> other = Optional<Value>())
	  : value(v), mode(mode), other(other) {}

	// To change this serialization, ProtocolVersion::MovableCoordinatedStateV2 must be updated, and downgrades need to
	// be considered
	template <class Ar>
	void serialize(Ar& ar) {
		ASSERT(ar.protocolVersion().hasMovableCoordinatedState());
		serializer(ar, value, mode, other);
	}
};

struct MovableCoordinatedStateImpl {
	ServerCoordinators coordinators;
	CoordinatedState cs;
	Optional<Value> lastValue, // The value passed to setExclusive()
	    lastCSValue; // The value passed to cs.setExclusive()

	MovableCoordinatedStateImpl(ServerCoordinators const& c) : coordinators(c), cs(c) {}

	ACTOR static Future<Value> read(MovableCoordinatedStateImpl* self) {
		state MovableValue moveState;
		Value rawValue = wait(self->cs.read());
		if (rawValue.size()) {
			BinaryReader r(rawValue, IncludeVersion());
			if (!r.protocolVersion().hasMovableCoordinatedState()) {
				// Old coordinated state, not a MovableValue
				moveState.value = rawValue;
			} else
				r >> moveState;
		}
		// SOMEDAY: If moveState.mode == MovingFrom, read (without locking) old state and assert that it corresponds
		// with our state and is ReallyTo(coordinators)
		if (moveState.mode == MovableValue::MaybeTo) {
			TEST(true);
			ASSERT(moveState.other.present());
			wait(self->moveTo(
			    self, &self->cs, ClusterConnectionString(moveState.other.get().toString()), moveState.value));
		}
		return moveState.value;
	}

	Future<Void> onConflict() { return cs.onConflict(); }

	Future<Void> setExclusive(Value v) {
		lastValue = v;
		lastCSValue = BinaryWriter::toValue(MovableValue(v, MovableValue::Active),
		                                    IncludeVersion(ProtocolVersion::withMovableCoordinatedStateV2()));
		return cs.setExclusive(lastCSValue.get());
	}

	ACTOR static Future<Void> move(MovableCoordinatedStateImpl* self, ClusterConnectionString nc) {
		// Call only after setExclusive returns.  Attempts to move the coordinated state
		// permanently to the new ServerCoordinators, which must be uninitialized.  Returns when the process has
		// reached the point where a leader elected by the new coordinators should be doing the rest of the work
		// (and therefore the caller should die).
		state CoordinatedState cs(self->coordinators);
		state CoordinatedState nccs(
		    ServerCoordinators(Reference<ClusterConnectionFile>(new ClusterConnectionFile(nc))));
		state Future<Void> creationTimeout = delay(30);
		ASSERT(self->lastValue.present() && self->lastCSValue.present());
		TraceEvent("StartMove").detail("ConnectionString", nc.toString());
		choose {
			when(wait(creationTimeout)) { throw new_coordinators_timed_out(); }
			when(Value ncInitialValue = wait(nccs.read())) {
				ASSERT(!ncInitialValue.size()); // The new coordinators must be uninitialized!
			}
		}
		TraceEvent("FinishedRead").detail("ConnectionString", nc.toString());

		choose {
			when(wait(creationTimeout)) { throw new_coordinators_timed_out(); }
			when(wait(nccs.setExclusive(
			    BinaryWriter::toValue(MovableValue(self->lastValue.get(),
			                                       MovableValue::MovingFrom,
			                                       self->coordinators.ccf->getConnectionString().toString()),
			                          IncludeVersion(ProtocolVersion::withMovableCoordinatedStateV2()))))) {}
		}

		if (BUGGIFY)
			wait(delay(5));

		Value oldQuorumState = wait(cs.read());
		if (oldQuorumState != self->lastCSValue.get()) {
			TEST(true); // Quorum change aborted by concurrent write to old coordination state
			TraceEvent("QuorumChangeAbortedByConcurrency");
			throw coordinated_state_conflict();
		}

		wait(self->moveTo(self, &cs, nc, self->lastValue.get()));

		throw coordinators_changed();
	}

	ACTOR static Future<Void> moveTo(MovableCoordinatedStateImpl* self,
	                                 CoordinatedState* coordinatedState,
	                                 ClusterConnectionString nc,
	                                 Value value) {
		wait(coordinatedState->setExclusive(
		    BinaryWriter::toValue(MovableValue(value, MovableValue::MaybeTo, nc.toString()),
		                          IncludeVersion(ProtocolVersion::withMovableCoordinatedStateV2()))));

		if (BUGGIFY)
			wait(delay(5));

		// SOMEDAY: If we are worried about someone magically getting the new cluster ID and interfering, do a second
		// cs.setExclusive( encode( ReallyTo, ... ) )
		TraceEvent("ChangingQuorum").detail("ConnectionString", nc.toString());
		wait(changeLeaderCoordinators(self->coordinators, StringRef(nc.toString())));
		TraceEvent("ChangedQuorum").detail("ConnectionString", nc.toString());
		throw coordinators_changed();
	}
};

MovableCoordinatedState& MovableCoordinatedState::operator=(MovableCoordinatedState&& av) = default;
MovableCoordinatedState::MovableCoordinatedState(class ServerCoordinators const& coord)
  : impl(new MovableCoordinatedStateImpl(coord)) {}
MovableCoordinatedState::~MovableCoordinatedState() = default;
Future<Value> MovableCoordinatedState::read() {
	return MovableCoordinatedStateImpl::read(impl.get());
}
Future<Void> MovableCoordinatedState::onConflict() {
	return impl->onConflict();
}
Future<Void> MovableCoordinatedState::setExclusive(Value v) {
	return impl->setExclusive(v);
}
Future<Void> MovableCoordinatedState::move(ClusterConnectionString const& nc) {
	return MovableCoordinatedStateImpl::move(impl.get(), nc);
}
