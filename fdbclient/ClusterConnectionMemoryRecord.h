/*
 * ClusterConnectionMemoryRecord.h
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

#pragma once
#ifndef FDBCLIENT_CLUSTERCONNECTIONMEMORYRECORD_H
#define FDBCLIENT_CLUSTERCONNECTIONMEMORYRECORD_H

#include "fdbclient/CoordinationInterface.h"

class ClusterConnectionMemoryRecord : public IClusterConnectionRecord,
                                      ReferenceCounted<ClusterConnectionMemoryRecord>,
                                      NonCopyable {
public:
	ClusterConnectionMemoryRecord()
	  : IClusterConnectionRecord(false), id(deterministicRandom()->randomUniqueID()), valid(false) {}

	explicit ClusterConnectionMemoryRecord(ClusterConnectionString const& cs)
	  : IClusterConnectionRecord(false), id(deterministicRandom()->randomUniqueID()), cs(cs), valid(true) {}

	ClusterConnectionString const& getConnectionString() const override;
	Future<Void> setConnectionString(ClusterConnectionString const&) override;
	Future<ClusterConnectionString> getStoredConnectionString() override;

	Future<bool> upToDate(ClusterConnectionString& fileConnectionString) override;

	Standalone<StringRef> getLocation() const override;
	Reference<IClusterConnectionRecord> makeIntermediateRecord(
	    ClusterConnectionString const& connectionString) const override;

	bool isValid() const override;
	std::string toString() const override;

	void addref() override { ReferenceCounted<ClusterConnectionMemoryRecord>::addref(); }
	void delref() override { ReferenceCounted<ClusterConnectionMemoryRecord>::delref(); }

protected:
	Future<bool> persist() override;

private:
	UID id;
	ClusterConnectionString cs;
	bool valid;
};

#endif