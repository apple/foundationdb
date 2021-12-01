/*
 * TestTLogPeek.h
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

#ifndef FDBSERVER_PTXN_TEST_TESTTLOGPEEK_H
#define FDBSERVER_PTXN_TEST_TESTTLOGPEEK_H

#pragma once

#include "fdbserver/ptxn/test/Driver.h"
#include "flow/UnitTest.h"

namespace ptxn::test {

struct TestTLogPeekOptions {
	static const int DEFAULT_NUM_VERSIONS = 100;
	static const int DEFAULT_NUM_MUTATIONS_PER_VERSION = 100;
	static const int DEFAULT_NUM_TEAMS = 3;
	static const int DEFAULT_INITIAL_VERSION = 1000;
	static const int DEFAULT_PEEK_TIMES = 1000;

	// The number of versions for peek
	int numVersions;
	// The number of mutations per version
	int numMutationsPerVersion;
	// The number of teams in the TLog. Mutations are randomly distributed into teams.
	int numStorageTeams;
	// The initial version
	int initialVersion;
	// Number of peek times
	int peekTimes;

	explicit TestTLogPeekOptions(const UnitTestParameters&);
};

struct TestTLogPeekMergeCursorOptions {
	static const int DEFAULT_INITIAL_VERSION = 1000;
	static const int DEFAULT_NUM_VERSIONS = 10;
	static const int DEFAULT_NUM_MUTATIONS_PER_VERSION = 100;
	static const int DEFAULT_NUM_TLOGS = 5;

	// The number of TLogGroup/StorageTeam/TLog
	// One TLogGroup has one StorageTeam and one TLog server
	int numTLogs;
	// Number mutations per version
	int numMutationsPerVersion;
	Version initialVersion;
	int numVersions;

	explicit TestTLogPeekMergeCursorOptions(const UnitTestParameters&);
};

} // namespace ptxn::test

#endif // FDBSERVER_PTEXN_TEST_TESTTLOGPEEK_H