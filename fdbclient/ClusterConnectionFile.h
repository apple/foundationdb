/*
 * ClusterConnectionFile.h
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

#pragma once
#ifndef FDBCLIENT_CLUSTERCONNECTIONFILE_H
#define FDBCLIENT_CLUSTERCONNECTIONFILE_H

#include "fdbclient/CoordinationInterface.h"
#include "flow/actorcompiler.h" // has to be last include

// An implementation of IClusterConnectionRecord backed by a file.
class ClusterConnectionFile : public IClusterConnectionRecord, ReferenceCounted<ClusterConnectionFile>, NonCopyable {
public:
	// Loads and parses the file at 'filename', throwing errors if the file cannot be read or the format is invalid.
	explicit ClusterConnectionFile(std::string const& filename);

	// Creates a cluster file with a given connection string and saves it to the specified file.
	explicit ClusterConnectionFile(std::string const& filename, ClusterConnectionString const& contents);

	// Sets the connections string held by this object and persists it.
	Future<Void> setAndPersistConnectionString(ClusterConnectionString const&) override;

	// Get the connection string stored in the file.
	Future<ClusterConnectionString> getStoredConnectionString() override;

	// Checks whether the connection string in the file matches the connection string stored in memory. The cluster
	// string stored in the file is returned via the reference parameter connectionString.
	Future<bool> upToDate(ClusterConnectionString& fileConnectionString) override;

	// Returns the specified path of the cluster file.
	std::string getLocation() const override;

	// Creates a copy of this object with a modified connection string but that isn't persisted.
	Reference<IClusterConnectionRecord> makeIntermediateRecord(
	    ClusterConnectionString const& connectionString) const override;

	// Returns a string representation of this cluster connection record. This will include the type of record and the
	// filename of the cluster file.
	std::string toString() const override;

	void addref() override { ReferenceCounted<ClusterConnectionFile>::addref(); }
	void delref() override { ReferenceCounted<ClusterConnectionFile>::delref(); }

	// returns <resolved name, was default file>
	static std::pair<std::string, bool> lookupClusterFileName(std::string const& filename);

	// get a human readable error message describing the error returned from the constructor
	static std::string getErrorString(std::pair<std::string, bool> const& resolvedFile, Error const& e);

protected:
	// Writes the connection string to the cluster file
	Future<bool> persist() override;

private:
	std::string filename;
};

#include "flow/unactorcompiler.h"
#endif