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

#ifndef FLOW_SIM_BUG_INJECTOR_H
#define FLOW_SIM_BUG_INJECTOR_H
#pragma once
#include <cstddef>
#include <memory>
#include <string>

/*
 * This file provides a general framework to control how bugs should be injected into FDB. This must not be confused
 * with Buggify. Buggify is intended to inject faults and set parameters in ways that FDB has to be able to handle
 * correctly. This framework is meant to be used to inject actual bugs into FDB. The use-case for this is to implement
 * negative tests. This way we can verify that tests actually catch the bugs that we're expecting them to find.
 */

/**
 * A ISimBug is a logical, polymorphic, representation of a bug and is the main means of communication for code across
 * multiple places within the codebase. A user isn't supposed to create instances of ISimBug themselves but instead they
 * should implement a corresponding IBugIdentifier subclass which can then be used for indexing through the
 * SimBugInjector.
 */
class ISimBug : std::enable_shared_from_this<ISimBug> {
	void* impl;
	virtual void onHit();

public:
	ISimBug();
	virtual ~ISimBug();
	/**
	 * The name of the bug. By default this will return the class name (using typeid and boost::core::demangle). This is
	 * supposed to be a human-readable string which can be used to identify the bug when it appears in traces.
	 *
	 * @return A human readable string for this bug.
	 */
	virtual std::string name() const;
	/**
	 * Should be called every time this bug is hit. This method can't be overridden. However, it will call `onHit` which
	 * can be overriden by a child.
	 */
	void hit();
	/**
	 * @return Number of times this bug has been hit (since last call to `SimBugInjector::reset`
	 */
	unsigned numHits() const;
};

/*
 * Each SimBug class should have a corresponding BugIdentifier
 */
class IBugIdentifier {
public:
	virtual ~IBugIdentifier();
	/**
	 * Creates an instance of the SimBug associated with this
	 * identifier. The reason we use shared_ptr instead of Reference here
	 * is so we (1) can use weak references and, more importantly,
	 * (2) shared_ptr has much better support for polymorphic types
	 */
	virtual std::shared_ptr<ISimBug> create() const = 0;
};

/*
 * SimBugInjector is a wrapper for a singleton and can therefore be instantiated cheaply as often as one wants.
 */
class SimBugInjector {
public:
	explicit SimBugInjector() {}
	/**
	 * Globally enable SimBugInjector
	 *
	 * Precondition: g_network->isSimulated()
	 */
	void enable();
	/**
	 * Globally disable SimBugInjector. If enable is called later, all current state is restored
	 *
	 * Precondition: true
	 */
	void disable();
	/**
	 * Globally disable SimBugInjector. Unlike disable, this will also remove all existing state
	 *
	 * Precondition: true
	 */
	void reset();
	/**
	 * Check whether SimBugInjector is globally enabled
	 */
	bool isEnabled() const;

	/**
	 * This method can be used to check whether a given bug has been enabled and then fetch the corresponding
	 * ISimBug object.
	 *
	 * @param id The IBugIdentifier corresponding to this bug
	 * @return A valid shared_ptr, if the bug has been enabled, nullptr otherwise
	 * @post enabled(bug(id)) -> result is valid else result is nullptr
	 */
	template <class T>
	std::shared_ptr<T> get(IBugIdentifier const& id, bool getDisabled = false) {
		auto res = getImpl(id, getDisabled);
		if (!res) {
			return {};
		}
		return std::dynamic_pointer_cast<T>(res);
	}

	std::shared_ptr<ISimBug> getImpl(IBugIdentifier const& id, bool getDisabled = false) const;

	/**
	 * Returns the ISimBug instance associated with id if it already exists. Otherwise it will first create it. It is a
	 * bug to call this method if bug injection isn't turned on.
	 *
	 * @param id
	 * @return the SimBug instance
	 * @pre isEnabled()
	 * @post result.get() != nullptr
	 */
	template <class T>
	std::shared_ptr<T> enable(IBugIdentifier const& id) {
		auto res = enableImpl(id);
		if (!res) {
			return {};
		}
		return std::dynamic_pointer_cast<T>(res);
	}

	std::shared_ptr<ISimBug> enableImpl(IBugIdentifier const& id);
};

#endif // FLOW_SIM_BUG_INJECTOR_H
