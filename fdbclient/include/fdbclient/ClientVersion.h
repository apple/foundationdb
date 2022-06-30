/*
 * ClientVersion.h
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

#ifndef FDBCLIENT_CLIENT_VERSION_H
#define FDBCLIENT_CLIENT_VERSION_H
#pragma once

#include "flow/Arena.h"

struct ClientVersionRef {
	StringRef clientVersion;
	StringRef sourceVersion;
	StringRef protocolVersion;

	ClientVersionRef() { initUnknown(); }

	ClientVersionRef(Arena& arena, ClientVersionRef const& cv)
	  : clientVersion(arena, cv.clientVersion), sourceVersion(arena, cv.sourceVersion),
	    protocolVersion(arena, cv.protocolVersion) {}
	ClientVersionRef(StringRef clientVersion, StringRef sourceVersion, StringRef protocolVersion)
	  : clientVersion(clientVersion), sourceVersion(sourceVersion), protocolVersion(protocolVersion) {}
	ClientVersionRef(StringRef versionString) {
		std::vector<StringRef> parts = versionString.splitAny(LiteralStringRef(","));
		if (parts.size() != 3) {
			initUnknown();
			return;
		}
		clientVersion = parts[0];
		sourceVersion = parts[1];
		protocolVersion = parts[2];
	}

	void initUnknown() {
		clientVersion = LiteralStringRef("Unknown");
		sourceVersion = LiteralStringRef("Unknown");
		protocolVersion = LiteralStringRef("Unknown");
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, clientVersion, sourceVersion, protocolVersion);
	}

	size_t expectedSize() const { return clientVersion.size() + sourceVersion.size() + protocolVersion.size(); }

	bool operator<(const ClientVersionRef& rhs) const {
		if (protocolVersion != rhs.protocolVersion) {
			return protocolVersion < rhs.protocolVersion;
		}

		// These comparisons are arbitrary because they aren't ordered
		if (clientVersion != rhs.clientVersion) {
			return clientVersion < rhs.clientVersion;
		}

		return sourceVersion < rhs.sourceVersion;
	}
};

#endif