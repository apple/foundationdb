/*
 * IKnobCollection.h
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

#ifndef FDBCLIENT_IKNOBCOLLECTION_H
#define FDBCLIENT_IKNOBCOLLECTION_H

#pragma once

#include <memory>

#include "fdbclient/ClientKnobs.h"
#include "fdbclient/ConfigKnobs.h"
#include "fdbclient/ServerKnobs.h"
#include "flow/Knobs.h"

/*
 * Each IKnobCollection instantiation stores several Knobs objects, and when parsing or setting a knob,
 * these Knobs objects will be traversed in order, until the requested knob name is found. The order of traversal is:
 *  - FlowKnobs
 *  - ClientKnobs
 *  - ServerKnobs
 *  - TestKnobs
 */
class IKnobCollection {
	static std::unique_ptr<IKnobCollection>& globalKnobCollection();

public:
	virtual ~IKnobCollection() = default;

	enum class Type {
		CLIENT,
		SERVER,
		TEST,
	};

	static std::unique_ptr<IKnobCollection> create(Type, Randomize, IsSimulated);
	virtual void initialize(Randomize randomize, IsSimulated isSimulated) = 0;
	virtual void reset(Randomize randomize, IsSimulated isSimulated) = 0;
	virtual FlowKnobs const& getFlowKnobs() const = 0;
	virtual ClientKnobs const& getClientKnobs() const = 0;
	virtual ServerKnobs const& getServerKnobs() const = 0;
	virtual class TestKnobs const& getTestKnobs() const = 0;
	virtual void clearTestKnobs() {}
	virtual Optional<KnobValue> tryParseKnobValue(std::string const& knobName, std::string const& knobValue) const = 0;
	KnobValue parseKnobValue(std::string const& knobName, std::string const& knobValue) const;
	static KnobValue parseKnobValue(std::string const& knobName, std::string const& knobValue, Type);
	// Result indicates whether or not knob was successfully set:
	virtual bool trySetKnob(std::string const& knobName, KnobValueRef const& knobValue) = 0;
	void setKnob(std::string const& knobName, KnobValueRef const& knobValue);
	virtual bool isAtomic(std::string const& knobName) const = 0;

	static void setGlobalKnobCollection(Type, Randomize, IsSimulated);
	static IKnobCollection const& getGlobalKnobCollection();
	static IKnobCollection& getMutableGlobalKnobCollection();
	static ConfigMutationRef createSetMutation(Arena, KeyRef, ValueRef);
	static ConfigMutationRef createClearMutation(Arena, KeyRef);
};

#endif // FDBCLIENT_IKNOBCOLLECTION_H