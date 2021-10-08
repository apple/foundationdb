/*
 * ClusterConnectionFile.h
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
#ifndef FDBCLIENT_CLUSTERCONNECTIONFILE_H
#define FDBCLIENT_CLUSTERCONNECTIONFILE_H

#include "fdbclient/CoordinationInterface.h"
#include "flow/actorcompiler.h" // has to be last include

class ClusterConnectionFile : public IClusterConnectionRecord, ReferenceCounted<ClusterConnectionFile>, NonCopyable {
public:
	ClusterConnectionFile() : IClusterConnectionRecord(false) {}
	// Loads and parses the file at 'path', throwing errors if the file cannot be read or the format is invalid.
	//
	// The format of the file is: description:id@[addrs]+
	//  The description and id together are called the "key"
	//
	// The following is enforced about the format of the file:
	//  - The key must contain one (and only one) ':' character
	//  - The description contains only allowed characters (a-z, A-Z, 0-9, _)
	//  - The ID contains only allowed characters (a-z, A-Z, 0-9)
	//  - At least one address is specified
	//  - There is no address present more than once
	explicit ClusterConnectionFile(std::string const& path);
	explicit ClusterConnectionFile(std::string const& filename, ClusterConnectionString const& contents);

	// returns <resolved name, was default file>
	static std::pair<std::string, bool> lookupClusterFileName(std::string const& filename);
	// get a human readable error message describing the error returned from the constructor
	static std::string getErrorString(std::pair<std::string, bool> const& resolvedFile, Error const& e);

	ClusterConnectionString const& getConnectionString() const override;
	Future<Void> setConnectionString(ClusterConnectionString const&) override;
	Future<ClusterConnectionString> getStoredConnectionString() override;

	std::string const& getFilename() const {
		ASSERT(filename.size());
		return filename;
	}
	Future<bool> upToDate(ClusterConnectionString& fileConnectionString) override;

	Standalone<StringRef> getLocation() const override;
	Reference<IClusterConnectionRecord> makeIntermediateRecord(
	    ClusterConnectionString const& connectionString) const override;

	bool isValid() const override;
	std::string toString() const override;

	void addref() override { ReferenceCounted<ClusterConnectionFile>::addref(); }
	void delref() override { ReferenceCounted<ClusterConnectionFile>::delref(); }

protected:
	Future<bool> persist() override;

private:
	ClusterConnectionString cs;
	std::string filename;
};

#include "flow/unactorcompiler.h"
#endif