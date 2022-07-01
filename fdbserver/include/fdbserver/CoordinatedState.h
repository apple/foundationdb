/*
 * CoordinatedState.h
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

#ifndef FDBSERVER_COORDINATED_STATE_H
#define FDBSERVER_COORDINATED_STATE_H
#pragma once

#include "fdbclient/FDBTypes.h"

class CoordinatedState : NonCopyable {
public:
	// Callers must ensure that any outstanding operations have been cancelled before destructing *this!
	CoordinatedState(class ServerCoordinators const&);
	~CoordinatedState();

	Future<Value> read();
	// May only be called once.
	// Returns the most recent state if there are no concurrent calls to setExclusive
	// Otherwise might return the state passed to a concurrent call, even if that call ultimately fails.
	// Don't count on the result of this read being part of the serialized history until a subsequent setExclusive has
	// succeeded!

	Future<Void> onConflict();
	// May only be called once, and only after read returns.
	// Eventually returns Void if a call to setExclusive would fail.
	// May or may not return or throw an error after setExclusive is called.
	// (Generally?) doesn't return unless there is some concurrent call to read or setExclusive.

	Future<Void> setExclusive(Value);
	// read() must have been called and returned first, and this may only be called once.
	// Attempts to change the state value, provided that the value returned by read is still the
	//   most recent.
	// If it returns Void, the state was successfully changed and the state returned by read was
	//   the most recent before the new state.
	// If it throws coordinated_state_conflict, the state may or may not have been changed, and the value
	//   returned from read may or may not ever have been a valid state.  Probably there was a
	//   call to read() or setExclusive() concurrently with this pair.

	uint64_t getConflict();

private:
	std::unique_ptr<struct CoordinatedStateImpl> impl;
};

class MovableCoordinatedState : NonCopyable {
public:
	MovableCoordinatedState(class ServerCoordinators const&);
	MovableCoordinatedState& operator=(MovableCoordinatedState&& av);
	~MovableCoordinatedState();

	Future<Value> read();

	Future<Void> onConflict();

	Future<Void> setExclusive(Value v);

	Future<Void> move(class ClusterConnectionString const& nc);
	// Call only after setExclusive returns.  Attempts to move the coordinated state
	// permanently to the new ServerCoordinators, which must be uninitialized.  Returns when the process has
	// reached the point where a leader elected by the new coordinators should be doing the rest of the work
	// (and therefore the caller should die).

private:
	std::unique_ptr<struct MovableCoordinatedStateImpl> impl;
};

#endif
