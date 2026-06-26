/*
 * ProcessClass.h
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

#ifndef FDBCLIENT_PROCESSCLASS_H
#define FDBCLIENT_PROCESSCLASS_H
#pragma once

#include "flow/flow.h"

struct ProcessClass {
	constexpr static FileIdentifier file_identifier = 6697257;
	// This enum is stored in restartInfo.ini for upgrade tests, so be very careful about changing the existing items!
	enum ClassType {
		UnsetClass,
		StorageClass,
		TransactionClass,
		ResolutionClass,
		TesterClass,
		CommitProxyClass,
		MasterClass,
		StatelessClass,
		LogClass,
		ClusterControllerClass,
		LogRouterClass,
		FastRestoreClass, // deprecated
		DataDistributorClass,
		CoordinatorClass,
		RatekeeperClass,
		RemovedClassPlaceholder1, // removing the name of removed functionality, but don't renumber subsequent entries.
		BackupClass,
		GrvProxyClass,
		BlobManagerClass,
		BlobWorkerClass,
		EncryptKeyProxyClass,
		ConsistencyScanClass,
		BlobMigratorClass,
		SimHTTPServerClass,
		InvalidClass = -1
	};

	// class is serialized by enum value, so it's important not to change the
	// enum value of a class. New classes should only be added to the end.
	static_assert(ProcessClass::UnsetClass == 0);
	static_assert(ProcessClass::StorageClass == 1);
	static_assert(ProcessClass::TransactionClass == 2);
	static_assert(ProcessClass::ResolutionClass == 3);
	static_assert(ProcessClass::TesterClass == 4);
	static_assert(ProcessClass::CommitProxyClass == 5);
	static_assert(ProcessClass::MasterClass == 6);
	static_assert(ProcessClass::StatelessClass == 7);
	static_assert(ProcessClass::LogClass == 8);
	static_assert(ProcessClass::ClusterControllerClass == 9);
	static_assert(ProcessClass::LogRouterClass == 10);
	static_assert(ProcessClass::FastRestoreClass == 11);
	static_assert(ProcessClass::DataDistributorClass == 12);
	static_assert(ProcessClass::CoordinatorClass == 13);
	static_assert(ProcessClass::RatekeeperClass == 14);
	// intentional gap at 15
	static_assert(ProcessClass::BackupClass == 16);
	static_assert(ProcessClass::GrvProxyClass == 17);
	static_assert(ProcessClass::BlobManagerClass == 18);
	static_assert(ProcessClass::BlobWorkerClass == 19);
	static_assert(ProcessClass::EncryptKeyProxyClass == 20);
	static_assert(ProcessClass::ConsistencyScanClass == 21);
	static_assert(ProcessClass::BlobMigratorClass == 22);
	static_assert(ProcessClass::SimHTTPServerClass == 23);
	static_assert(ProcessClass::InvalidClass == -1);

	enum ClassSource { CommandLineSource, AutoSource, DBSource, InvalidSource = -1 };
	int16_t _class;
	int16_t _source;

	// source is serialized by enum value, so it's important not to change the
	// enum value of a source. New sources should only be added to the end.
	static_assert(ProcessClass::CommandLineSource == 0);
	static_assert(ProcessClass::AutoSource == 1);
	static_assert(ProcessClass::DBSource == 2);

public:
	ProcessClass() : _class(UnsetClass), _source(CommandLineSource) {}
	ProcessClass(ClassType type, ClassSource source) : _class(type), _source(source) {}
	explicit ProcessClass(std::string s, ClassSource source);

	ProcessClass(std::string classStr, std::string sourceStr);

	ClassType classType() const { return (ClassType)_class; }
	ClassSource classSource() const { return (ClassSource)_source; }

	bool operator==(const ClassType& rhs) const { return _class == rhs; }
	bool operator!=(const ClassType& rhs) const { return _class != rhs; }

	bool operator==(const ProcessClass& rhs) const { return _class == rhs._class && _source == rhs._source; }
	bool operator!=(const ProcessClass& rhs) const { return _class != rhs._class || _source != rhs._source; }

	std::string toString() const;

	std::string sourceString() const;

	// Client-side configuration validation uses this to count workers that can host storage.
	bool canBecomeStorageServer() const;

	// To change this serialization, ProtocolVersion::ProcessClassValue must be updated, and downgrades need to be
	// considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, _class, _source);
	}
};

#endif // FDBCLIENT_PROCESSCLASS_H
