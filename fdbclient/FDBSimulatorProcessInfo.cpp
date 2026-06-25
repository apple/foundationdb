/*
 * FDBSimulatorProcessInfo.cpp
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

#include "fdbclient/FDBSimulatorProcessInfo.h"

FDBSimulatorProcessMetadata::FDBSimulatorProcessMetadata(ProcessClass startingClass)
  : simulator::ProcessInfoMetadata(startingClass.toString(), startingClass == ProcessClass::TesterClass),
    startingClass(startingClass) {}

Reference<simulator::ProcessInfoMetadata> makeFDBSimulatorProcessMetadata(ProcessClass startingClass) {
	return makeReference<FDBSimulatorProcessMetadata>(startingClass);
}

ProcessClass const& getSimulatorProcessClass(simulator::ProcessInfo const& process) {
	auto* metadata = dynamic_cast<FDBSimulatorProcessMetadata*>(process.metadata.getPtr());
	ASSERT(metadata != nullptr);
	return metadata->startingClass;
}

ProcessClass const& getSimulatorProcessClass(simulator::ProcessInfo const* process) {
	ASSERT(process != nullptr);
	return getSimulatorProcessClass(*process);
}

bool isAvailableSimulatorProcessClass(simulator::ProcessInfo const& process) {
	switch (getSimulatorProcessClass(process)._class) {
	case ProcessClass::UnsetClass:
	case ProcessClass::StorageClass:
	case ProcessClass::TransactionClass:
	case ProcessClass::LogClass:
		return true;

	case ProcessClass::ResolutionClass:
	case ProcessClass::CommitProxyClass:
	case ProcessClass::GrvProxyClass:
	case ProcessClass::MasterClass:
	case ProcessClass::TesterClass:
	case ProcessClass::StatelessClass:
	case ProcessClass::LogRouterClass:
	case ProcessClass::ClusterControllerClass:
	case ProcessClass::DataDistributorClass:
	case ProcessClass::RatekeeperClass:
	case ProcessClass::ConsistencyScanClass:
	case ProcessClass::BlobManagerClass:
	case ProcessClass::BackupClass:
	case ProcessClass::EncryptKeyProxyClass:
	default:
		return false;
	}
}

bool isAvailableSimulatorProcessClass(simulator::ProcessInfo const* process) {
	ASSERT(process != nullptr);
	return isAvailableSimulatorProcessClass(*process);
}
