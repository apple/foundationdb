/*
 * Orderer.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_ORDERER_ACTOR_G_H)
	#define FDBSERVER_ORDERER_ACTOR_G_H
	#include "Orderer.actor.g.h"
#elif !defined(FDBSERVER_ORDERER_ACTOR_H)
	#define FDBSERVER_ORDERER_ACTOR_H

#include "fdbclient/Notified.h"
#include "flow/actorcompiler.h"

template <class Seq>
class Orderer {
public:
	explicit Orderer( Seq s ) : ready(s), started(false) {}
	void reset( Seq s ) {
		ready = NotifiedVersion(s);
		started = false;
	}
	Future<bool> order( Seq s, int taskID = TaskDefaultYield ) {
		if ( ready.get() < s )
			return waitAndOrder( this, s, taskID );
		else
			return dedup(s);
	}
	void complete( Seq s ) {
		ASSERT( s == ready.get() && started );
		started = false;
		ready.set(s+1);
	}
	Seq getNextSequence() { return ready.get(); }  // Returns the next sequence number which has *not* been returned from order()
	Future<Void> whenNextSequenceAtLeast( Seq v ) {
		return ready.whenAtLeast(v);
	}
private:
	ACTOR static Future<bool> waitAndOrder( Orderer<Seq>* self, Seq s, int taskID ) {
		Void _ = wait( self->ready.whenAtLeast(s) );
		Void _ = wait( yield( taskID ) || self->shutdown.getFuture() );
		return self->dedup(s);
	}
	bool dedup( Seq s ) {
		if (s != ready.get() || started)
			return false;
		started = true;
		return true;
	}

	bool started;
	NotifiedVersion ready;   // FIXME: Notified<Seq>
	Promise<Void> shutdown;  // Never set, only broken on destruction
};

#endif
