/*
 * IKnobCollection.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/IKnobCollection.h"
#include "fdbclient/ClientKnobCollection.h"
#include "fdbclient/ServerKnobCollection.h"
#include "fdbclient/TestKnobCollection.h"

std::unique_ptr<IKnobCollection> IKnobCollection::create(Type type, Randomize randomize, IsSimulated isSimulated) {
	if (type == Type::CLIENT) {
		return std::make_unique<ClientKnobCollection>(randomize, isSimulated);
	} else if (type == Type::SERVER) {
		return std::make_unique<ServerKnobCollection>(randomize, isSimulated);
	} else if (type == Type::TEST) {
		return std::make_unique<TestKnobCollection>(randomize, isSimulated);
	}
	UNSTOPPABLE_ASSERT(false);
}

KnobValue IKnobCollection::parseKnobValue(std::string const& knobName, std::string const& knobValue) const {
	auto result = tryParseKnobValue(knobName, knobValue);
	if (!result.present()) {
		throw invalid_option();
	}
	return result.get();
}

void IKnobCollection::setKnob(std::string const& knobName, KnobValueRef const& knobValue) {
	if (!trySetKnob(knobName, knobValue)) {
		TraceEvent(SevWarnAlways, "FailedToSetKnob")
		    .detail("KnobName", knobName)
		    .detail("KnobValue", knobValue.toString());
		throw invalid_option_value();
	}
}

KnobValue IKnobCollection::parseKnobValue(std::string const& knobName, std::string const& knobValue, Type type) {
	// TODO: Ideally it should not be necessary to create a template object to parse knobs
	static std::unique_ptr<IKnobCollection> clientKnobCollection, serverKnobCollection, testKnobCollection;
	if (type == Type::CLIENT) {
		if (!clientKnobCollection) {
			clientKnobCollection = create(type, Randomize::False, IsSimulated::False);
		}
		return clientKnobCollection->parseKnobValue(knobName, knobValue);
	} else if (type == Type::SERVER) {
		if (!serverKnobCollection) {
			serverKnobCollection = create(type, Randomize::False, IsSimulated::False);
		}
		return serverKnobCollection->parseKnobValue(knobName, knobValue);
	} else if (type == Type::TEST) {
		if (!testKnobCollection) {
			testKnobCollection = create(type, Randomize::False, IsSimulated::False);
		}
		return testKnobCollection->parseKnobValue(knobName, knobValue);
	}
	UNSTOPPABLE_ASSERT(false);
}

std::unique_ptr<IKnobCollection>& IKnobCollection::globalKnobCollection() {
	static std::unique_ptr<IKnobCollection> res;
	if (!res) {
		res = IKnobCollection::create(IKnobCollection::Type::CLIENT, Randomize::False, IsSimulated::False);
	}
	return res;
}

void IKnobCollection::setGlobalKnobCollection(Type type, Randomize randomize, IsSimulated isSimulated) {
	globalKnobCollection() = create(type, randomize, isSimulated);
	ASSERT(FLOW_KNOBS == &bootstrapGlobalFlowKnobs);
	FLOW_KNOBS = &globalKnobCollection()->getFlowKnobs();
}

IKnobCollection const& IKnobCollection::getGlobalKnobCollection() {
	return *globalKnobCollection();
}

IKnobCollection& IKnobCollection::getMutableGlobalKnobCollection() {
	return *globalKnobCollection();
}

void IKnobCollection::setupKnobs(const std::vector<std::pair<std::string, std::string>>& knobs) {
	auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
	for (const auto& [knobName, knobValueString] : knobs) {
		try {
			auto knobValue = g_knobs.parseKnobValue(knobName, knobValueString);
			g_knobs.setKnob(knobName, knobValue);
		} catch (Error& e) {
			if (e.code() == error_code_invalid_option_value) {
				std::cerr << "ERROR: Invalid value '" << knobValueString << "' for knob option '" << knobName << "'\n";
				TraceEvent(SevError, "InvalidKnobValue")
				    .errorUnsuppressed(e)
				    .detail("Knob", printable(knobName))
				    .detail("Value", printable(knobValueString));
				throw e;
			} else if (e.code() == error_code_invalid_option) {
				std::cerr << "ERROR: Invalid knob option '" << knobName << "'\n";
				TraceEvent(SevError, "InvalidKnobName")
				    .errorUnsuppressed(e)
				    .detail("Knob", printable(knobName))
				    .detail("Value", printable(knobValueString));
				throw e;
			} else {
				std::cerr << "ERROR: Failed to set knob option '" << knobName << "': " << e.what() << "\n";
				TraceEvent(SevError, "FailedToSetKnob")
				    .errorUnsuppressed(e)
				    .detail("Knob", printable(knobName))
				    .detail("Value", printable(knobValueString));
				throw e;
			}
		}
	}
}

ConfigMutationRef IKnobCollection::createSetMutation(Arena arena, KeyRef key, ValueRef value) {
	ConfigKey configKey = ConfigKeyRef::decodeKey(key);
	auto knobValue =
	    IKnobCollection::parseKnobValue(configKey.knobName.toString(), value.toString(), IKnobCollection::Type::TEST);
	return ConfigMutationRef(arena, configKey, knobValue.contents());
}

ConfigMutationRef IKnobCollection::createClearMutation(Arena arena, KeyRef key) {
	return ConfigMutationRef(arena, ConfigKeyRef::decodeKey(key), {});
}
