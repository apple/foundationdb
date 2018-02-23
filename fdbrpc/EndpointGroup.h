/*
 * EndpointGroup.h
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

#ifndef FLOW_ENDPOINT_GROUP_H
#define FLOW_ENDPOINT_GROUP_H
#pragma once

#include "flow.h"

// EndpointGroup makes it easier to implement backward compatibility for interface serialization
// It also provides a central place to implement more compact serialization for a group of related endpoints in the future.

/* Typical usage:

template <class Ar>
void serialize(Ar& ar) {
  auto endpoints = endpointGroup(ar);
  endpoints.require( ar.protocolVersion() <= currentProtocolVersion );
  endpoints & apple & banana;
  endpoints.require( ar.protocolVersion() >= 0xabc );  // Following endpoints added in this version
  endpoints & cherry;
  endpoints.require( ar.protocolVersion() >= 0xdef );  // .. and then some more were added
  endpoints & date;
}

*/


template <class Ar>
struct EndpointGroup : NonCopyable {
	Ar& ar;
	bool enabled;

	explicit EndpointGroup( Ar& ar ) : ar(ar), enabled(true) {
		ASSERT( ar.protocolVersion() != 0 );
	}
	EndpointGroup( EndpointGroup&& g ) : ar(g.ar), enabled(g.enabled) {}

	EndpointGroup& require( bool condition ) {
		enabled = enabled && condition;
		return *this;
	}

	template <class T>
	EndpointGroup& operator & (PromiseStream<T>& stream) {
		if (enabled)
			ar & stream;
		else if (Ar::isDeserializing)
			stream.sendError( incompatible_protocol_version() );
		return *this;
	}
};

template <class Ar>
EndpointGroup<Ar> endpointGroup( Ar& ar ) { return EndpointGroup<Ar>(ar); }

#endif