/*
 * FlowProcess.actor.h
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

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBRPC_FLOW_PROCESS_ACTOR_G_H)
#define FDBRPC_FLOW_PROCESS_ACTOR_G_H
#include "fdbrpc/FlowProcess.actor.g.h"
#elif !defined(FDBRPC_FLOW_PROCESS_ACTOR_H)
#define FDBRPC_FLOW_PROCESS_ACTOR_H

#include "fdbrpc/fdbrpc.h"

#include <string>
#include <map>

#include <flow/actorcompiler.h> // has to be last include

struct FlowProcessInterface {
	constexpr static FileIdentifier file_identifier = 3491839;
	RequestStream<struct FlowProcessRegistrationRequest> registerProcess;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, registerProcess);
	}
};

struct FlowProcessRegistrationRequest {
	constexpr static FileIdentifier file_identifier = 3411838;
	Standalone<StringRef> flowProcessInterface;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, flowProcessInterface);
	}
};

class FlowProcess {

public:
	virtual ~FlowProcess() {}
	virtual StringRef name() const = 0;
	virtual StringRef serializedInterface() const = 0;
	virtual Future<Void> run() = 0;
	virtual void registerEndpoint(Endpoint p) = 0;
};

struct IProcessFactory {
	static FlowProcess* create(std::string const& name) {
		auto it = factories().find(name);
		if (it == factories().end())
			return nullptr; // or throw?
		return it->second->create();
	}
	static std::map<std::string, IProcessFactory*>& factories() {
		static std::map<std::string, IProcessFactory*> theFactories;
		return theFactories;
	}

	virtual FlowProcess* create() = 0;

	virtual const char* getName() = 0;
};

template <class ProcessType>
struct ProcessFactory : IProcessFactory {
	ProcessFactory(const char* name) : name(name) { factories()[name] = this; }
	FlowProcess* create() override { return new ProcessType(); }
	const char* getName() override { return this->name; }

private:
	const char* name;
};

#include <flow/unactorcompiler.h>
#endif
