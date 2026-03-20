/*
 * tester.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/Locality.h"
#include "fdbserver/core/TesterInterface.h"
#include "flow/UnitTest.h"

Future<Void> testerServerCore(TesterInterface const& interf,
                              Reference<IClusterConnectionRecord> const& ccr,
                              Reference<AsyncVar<struct ServerDBInfo> const> const& serverDBInfo,
                              LocalityData const& locality,
                              Optional<std::string> const& expectedWorkLoad = Optional<std::string>());

enum test_location_t { TEST_HERE, TEST_ON_SERVERS, TEST_ON_TESTERS };
enum test_type_t {
	TEST_TYPE_FROM_FILE,
	TEST_TYPE_CONSISTENCY_CHECK,
	TEST_TYPE_UNIT_TESTS,
	TEST_TYPE_CONSISTENCY_CHECK_URGENT
};

Future<Void> runTests(Reference<IClusterConnectionRecord> const& connRecord,
                      test_type_t const& whatToRun,
                      test_location_t const& whereToRun,
                      int const& minTestersExpected,
                      std::string const& fileName = std::string(),
                      StringRef const& startingConfiguration = StringRef(),
                      LocalityData const& locality = LocalityData(),
                      UnitTestParameters const& testOptions = UnitTestParameters(),
                      bool const& restartingTest = false);

Future<Void> customShardConfigWorkload(Database const& cx);
