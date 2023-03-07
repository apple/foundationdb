/*
 * ProcessEvents.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "flow/ProcessEvents.h"

#include <unordered_map>
#include <vector>

namespace {

struct EventImpl {
	using Id = intptr_t;
	std::vector<StringRef> names;
	ProcessEvents::Callback callback;

	EventImpl(std::vector<StringRef> names, ProcessEvents::Callback callback)
	  : names(std::move(names)), callback(std::move(callback)) {
		addEvent();
	}
	~EventImpl() { removeEvent(); }
	Id id() const { return reinterpret_cast<intptr_t>(this); }
	void addEvent();
	void removeEvent();
};

struct ProcessEventsImpl {
	std::unordered_map<StringRef, std::unordered_map<EventImpl::Id, EventImpl*>> events;

	void trigger(StringRef name, StringRef msg, Error const& e) const {
		auto iter = events.find(name);
		// strictly speaking this isn't a bug, but having callbacks that aren't caught
		// by anyone could mean that something was misspelled. Therefore, the safe thing
		// to do is to abort.
		ASSERT(iter != events.end());
		std::unordered_map<EventImpl::Id, EventImpl*> callbacks;
		// These two iterations are necessary, since some callbacks might appear in multiple maps
		for (auto const& c : iter->second) {
			callbacks.insert(c);
		}
		// after we collected all unique callbacks we can call each
		for (auto const& c : callbacks) {
			c.second->callback(name, msg, e);
		}
	}
};

ProcessEventsImpl impl;

void EventImpl::addEvent() {
	for (auto const& name : names) {
		impl.events[name].emplace(this->id(), this);
	}
}

void EventImpl::removeEvent() {
	for (auto const& name : names) {
		impl.events[name].erase(this->id());
	}
}

} // namespace

namespace ProcessEvents {

void trigger(StringRef name, StringRef msg, Error const& e) {
	impl.trigger(name, msg, e);
}

Event::Event(StringRef name, Callback callback) {
	impl = new EventImpl({ std::move(name) }, std::move(callback));
}
Event::Event(std::vector<StringRef> names, Callback callback) {
	impl = new EventImpl(std::move(names), std::move(callback));
}
Event::~Event() {
	delete reinterpret_cast<EventImpl*>(impl);
}

} // namespace ProcessEvents