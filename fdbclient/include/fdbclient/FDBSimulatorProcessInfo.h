/*
 * FDBSimulatorProcessInfo.h
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

#ifndef FDBCLIENT_FDBSIMULATORPROCESSINFO_H
#define FDBCLIENT_FDBSIMULATORPROCESSINFO_H
#pragma once

#include "fdbclient/ProcessClass.h"
#include "fdbrpc/SimulatorProcessInfo.h"

struct FDBSimulatorProcessMetadata final : simulator::ProcessInfoMetadata {
	ProcessClass startingClass;

	explicit FDBSimulatorProcessMetadata(ProcessClass startingClass);
};

Reference<simulator::ProcessInfoMetadata> makeFDBSimulatorProcessMetadata(ProcessClass startingClass);

ProcessClass const& getSimulatorProcessClass(simulator::ProcessInfo const& process);

ProcessClass const& getSimulatorProcessClass(simulator::ProcessInfo const* process);

// Return true if the class type is suitable for stateful roles, such as tLog and StorageServer.
bool isAvailableSimulatorProcessClass(simulator::ProcessInfo const& process);

bool isAvailableSimulatorProcessClass(simulator::ProcessInfo const* process);

#endif // FDBCLIENT_FDBSIMULATORPROCESSINFO_H
