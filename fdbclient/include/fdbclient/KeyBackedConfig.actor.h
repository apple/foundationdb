/*
 * KeyBackedConfig.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_KEYBACKEDCONFIG_G_H)
#define FDBCLIENT_KEYBACKEDCONFIG_G_H
#include "fdbclient/KeyBackedConfig.actor.g.h"
#elif !defined(FDBCLIENT_KEYBACKEDCONFIG_ACTOR_H)
#define FDBCLIENT_KEYBACKEDCONFIG_ACTOR_H
#include "fdbclient/KeyBackedTypes.h"
#include "flow/actorcompiler.h" // has to be last include

class KeyBackedConfig {
public:
	KeyBackedConfig(StringRef prefix, UID uid = UID())
	  : uid(uid), prefix(prefix), configSpace(uidPrefixKey("uid->config/"_sr.withPrefix(prefix), uid)) {}

	KeyBackedProperty<std::string> tag() { return configSpace.pack(__FUNCTION__sr); }

	UID getUid() { return uid; }

	Key getUidAsKey() { return BinaryWriter::toValue(uid, Unversioned()); }

	template <class TrType>
	void clear(TrType tr) {
		tr->clear(configSpace.range());
	}

	// lastError is a pair of error message and timestamp expressed as an int64_t
	KeyBackedProperty<std::pair<std::string, Version>> lastError() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedMap<int64_t, std::pair<std::string, Version>> lastErrorPerType() {
		return configSpace.pack(__FUNCTION__sr);
	}

protected:
	UID uid;
	Key prefix;
	Subspace configSpace;
};

#include "flow/unactorcompiler.h"
#endif // FOUNDATIONDB_KEYBACKEDCONFIG_H
