/*
 * TestHarness.h
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

#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/tester/WorkloadUtils.h"

enum class AuditType : uint8_t;
struct ClusterControllerFullInterface;
struct ClusterInterface;
struct ServerDBInfo;

struct TestSet {
	KnobKeyValuePairs overrideKnobs;
	std::vector<TestSpec> testSpecs;
};

struct TesterConsistencyScanState {
	bool enabled = false;
	bool enableAfter = false;
	bool waitForComplete = false;
};

Future<Void> testDatabaseLiveness(Database cx, double databasePingDelay, std::string context, double startDelay = 0.0);
void printSimulatedTopology();

Future<Void> clearData(Database cx);
Future<Void> dumpDatabase(Database const& cx, std::string const& outputFilename, KeyRange const& range);
std::vector<PerfMetric> aggregateMetrics(std::vector<std::vector<PerfMetric>> metrics);
Future<Void> checkConsistencyScanAfterTest(Database cx, TesterConsistencyScanState* csState);

Future<Void> checkConsistency(Database cx,
                              std::vector<TesterInterface> testers,
                              bool doQuiescentCheck,
                              bool doTSSCheck,
                              double maxDDRunTime,
                              double softTimeLimit,
                              double databasePingDelay,
                              Reference<AsyncVar<ServerDBInfo>> dbInfo);
Future<Void> auditStorageCorrectness(Reference<AsyncVar<ServerDBInfo>> dbInfo, AuditType auditType);
Future<Void> checkConsistencyUrgentSim(Database cx, std::vector<TesterInterface> testers);
Future<Void> runConsistencyCheckerUrgentHolder(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc,
                                               Database cx,
                                               Optional<std::vector<TesterInterface>> testers,
                                               int minTestersExpected,
                                               bool repeatRun);

std::vector<TestSpec> readTests(std::ifstream& ifs);
TestSet readTOMLTests(std::string fileName);
