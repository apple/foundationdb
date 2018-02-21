/*
 * genericactors.actor.cpp
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

#include "actorcompiler.h"	// Gets genericactors.actor.g.h indirectly

ACTOR Future<bool> allTrue( std::vector<Future<bool>> all ) {
	state int i=0;
	while (i != all.size()) {
		bool r = wait( all[i] );
		if (!r) return false;
		i++;
	}
	return true;
}

ACTOR Future<Void> cancelOnly( std::vector<Future<Void>> futures ) {
	// We don't do anything with futures except hold them, we never return, but if we are cancelled we (naturally) drop the futures
	Void _ = wait( Never() );
	return Void();
}

ACTOR Future<Void> timeoutWarningCollector( FutureStream<Void> input, double logDelay, const char* context, UID id ) {
	state uint64_t counter = 0;
	state Future<Void> end = delay( logDelay );
	loop choose {
		when ( Void _ = waitNext( input ) ) {
			counter++;
		}
		when ( Void _ = wait( end ) ) {
			if( counter )
				TraceEvent(SevWarn, context, id).detail("LateProcessCount", counter).detail("LoggingDelay", logDelay);
			end = delay( logDelay );
			counter = 0;
		}
	}
}

ACTOR Future<bool> quorumEqualsTrue( std::vector<Future<bool>> futures, int required ) {
	state std::vector< Future<Void> > true_futures;
	state std::vector< Future<Void> > false_futures;
	for(int i=0; i<futures.size(); i++) {
		true_futures.push_back( onEqual( futures[i], true ) );
		false_futures.push_back( onEqual( futures[i], false ) );
	}

	choose {
		when( Void _ = wait( quorum( true_futures, required ) ) ) {
			return true;
		}
		when( Void _ = wait( quorum( false_futures, futures.size() - required + 1 ) ) ) {
			return false;
		}
	}
}
