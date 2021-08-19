/*
 * ConfigTransactionInterface.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ConfigTransactionInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/SystemData.h"
#include "flow/IRandom.h"

ConfigTransactionInterface::ConfigTransactionInterface() : _id(deterministicRandom()->randomUniqueID()) {}

void ConfigTransactionInterface::setupWellKnownEndpoints() {
	getGeneration.makeWellKnownEndpoint(WLTOKEN_CONFIGTXN_GETGENERATION, TaskPriority::Coordination);
	get.makeWellKnownEndpoint(WLTOKEN_CONFIGTXN_GET, TaskPriority::Coordination);
	getClasses.makeWellKnownEndpoint(WLTOKEN_CONFIGTXN_GETCLASSES, TaskPriority::Coordination);
	getKnobs.makeWellKnownEndpoint(WLTOKEN_CONFIGTXN_GETKNOBS, TaskPriority::Coordination);
	commit.makeWellKnownEndpoint(WLTOKEN_CONFIGTXN_COMMIT, TaskPriority::Coordination);
}

ConfigTransactionInterface::ConfigTransactionInterface(NetworkAddress const& remote)
  : getGeneration(Endpoint({ remote }, WLTOKEN_CONFIGTXN_GETGENERATION)),
    get(Endpoint({ remote }, WLTOKEN_CONFIGTXN_GET)), getClasses(Endpoint({ remote }, WLTOKEN_CONFIGTXN_GETCLASSES)),
    getKnobs(Endpoint({ remote }, WLTOKEN_CONFIGTXN_GETKNOBS)), commit(Endpoint({ remote }, WLTOKEN_CONFIGTXN_COMMIT)) {
}

bool ConfigTransactionInterface::operator==(ConfigTransactionInterface const& rhs) const {
	return _id == rhs._id;
}

bool ConfigTransactionInterface::operator!=(ConfigTransactionInterface const& rhs) const {
	return !(*this == rhs);
}

bool ConfigGeneration::operator==(ConfigGeneration const& rhs) const {
	return liveVersion == rhs.liveVersion && committedVersion == rhs.committedVersion;
}

bool ConfigGeneration::operator!=(ConfigGeneration const& rhs) const {
	return !(*this == rhs);
}

bool ConfigGeneration::operator<(ConfigGeneration const& rhs) const {
	if (committedVersion != rhs.committedVersion) {
		return committedVersion < rhs.committedVersion;
	} else {
		return liveVersion < rhs.liveVersion;
	}
}

bool ConfigGeneration::operator>(ConfigGeneration const& rhs) const {
	if (committedVersion != rhs.committedVersion) {
		return committedVersion > rhs.committedVersion;
	} else {
		return liveVersion > rhs.liveVersion;
	}
}

void ConfigTransactionCommitRequest::set(KeyRef key, ValueRef value) {
	if (key == configTransactionDescriptionKey) {
		annotation.description = KeyRef(arena, value);
	} else {
		ConfigKey configKey = ConfigKeyRef::decodeKey(key);
		auto knobValue = IKnobCollection::parseKnobValue(
		    configKey.knobName.toString(), value.toString(), IKnobCollection::Type::TEST);
		mutations.emplace_back_deep(arena, configKey, knobValue.contents());
	}
}

void ConfigTransactionCommitRequest::clear(KeyRef key) {
	if (key == configTransactionDescriptionKey) {
		annotation.description = ""_sr;
	} else {
		mutations.emplace_back_deep(arena, ConfigKeyRef::decodeKey(key), Optional<KnobValueRef>{});
	}
}
