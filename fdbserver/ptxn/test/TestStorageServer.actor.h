/*
 * TestStorageServer.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_PTXN_TEST_TESTSTORAGESERVER_ACTOR_G_H)
#define FDBSERVER_PTXN_TEST_TESTSTORAGESERVER_ACTOR_G_H
#include "fdbserver/ptxn/test/TestStorageServer.actor.g.h"
#elif !defined(FDBSERVER_PTXN_TEST_TESTSTORAGESERVER_ACTOR_H)
#define FDBSERVER_PTXN_TEST_TESTSTORAGESERVER_ACTOR_H

#pragma once

#include "flow/UnitTest.h"

namespace ptxn::test {

struct TestStorageServerPullOptions {
	static const int DEFAULT_TLOGS;
	static const int DEFAULT_STORAGE_TEAMS;
	static const int DEFAULT_INITIAL_VERSION;
	static const int DEFAULT_NUM_VERSIONS;
	static const int DEFAULT_NUM_MUTATIONS_PER_VERSION;

	const int numTLogs;
	const int numStorageTeams;
	const int initialVersion;
	const int numVersions;
	const int numMutationsPerVersion;

	explicit TestStorageServerPullOptions(const UnitTestParameters&);
};

} // namespace ptxn::test

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_PTXN_TEST_TESTSTORAGESERVER_ACTOR_H