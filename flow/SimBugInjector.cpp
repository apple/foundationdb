/*
 * SimBugInjector.h
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

#include "flow/SimBugInjector.h"
#include "flow/network.h"

#include <typeindex>
#include <boost/core/demangle.hpp>

namespace {

struct SimBugInjectorImpl {
	bool isEnabled = true;
	std::unordered_map<std::type_index, std::shared_ptr<ISimBug>> bugs;
};

struct ISimBugImpl {
	unsigned numHits = 0;
	static ISimBugImpl* get(void* self) { return reinterpret_cast<ISimBugImpl*>(self); }
};

SimBugInjectorImpl* simBugInjector = nullptr;

} // namespace

ISimBug::ISimBug() : impl(new ISimBugImpl()) {}

ISimBug::~ISimBug() {
	delete ISimBugImpl::get(impl);
}

std::string ISimBug::name() const {
	auto const& typeInfo = typeid(*this);
	return boost::core::demangle(typeInfo.name());
}

void ISimBug::hit() {
	++ISimBugImpl::get(impl)->numHits;
	TraceEvent(SevWarnAlways, "BugInjected").detail("Name", name()).detail("NumHits", numHits()).log();
	this->onHit();
}

void ISimBug::onHit() {}

unsigned ISimBug::numHits() const {
	return ISimBugImpl::get(impl)->numHits;
}

IBugIdentifier::~IBugIdentifier() {}

bool SimBugInjector::isEnabled() const {
	return simBugInjector != nullptr && simBugInjector->isEnabled;
}

void SimBugInjector::enable() {
	// SimBugInjector is very dangerous. It will corrupt your data! Therefore, using it outside of simulation is
	// not allowed
	UNSTOPPABLE_ASSERT(g_network->isSimulated());
	if (simBugInjector == nullptr) {
		simBugInjector = new SimBugInjectorImpl();
	}
	simBugInjector->isEnabled = true;
}

void SimBugInjector::disable() {
	if (simBugInjector) {
		simBugInjector->isEnabled = false;
	}
}

void SimBugInjector::reset() {
	if (simBugInjector) {
		delete simBugInjector;
	}
}

std::shared_ptr<ISimBug> SimBugInjector::getImpl(const IBugIdentifier& id, bool getDisabled /* = false */) const {
	if (!simBugInjector) {
		return {};
	}
	if (!getDisabled && !isEnabled()) {
		return {};
	}
	auto it = simBugInjector->bugs.find(std::type_index(typeid(id)));
	if (it == simBugInjector->bugs.end()) {
		return {};
	} else {
		return it->second;
	}
}

std::shared_ptr<ISimBug> SimBugInjector::enableImpl(const IBugIdentifier& id) {
	UNSTOPPABLE_ASSERT(isEnabled());
	auto& res = simBugInjector->bugs[std::type_index(typeid(id))];
	if (!res) {
		res = id.create();
	}
	return res;
}