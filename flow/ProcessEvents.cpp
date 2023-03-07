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
	Id id() const { return reinterpret_cast<intptr_t>(this); }
	void addEvent();
	void removeEvent();
};

struct ProcessEventsImpl {
	struct Triggering {
		bool& value;
		explicit Triggering(bool& value) : value(value) {
			ASSERT(!value);
			value = true;
		}
		~Triggering() { value = false; }
	};
	using EventMap = std::unordered_map<StringRef, std::unordered_map<EventImpl::Id, EventImpl*>>;
	bool triggering = false;
	EventMap events;
	std::map<EventImpl::Id, std::vector<StringRef>> toRemove;
	EventMap toInsert;

	void trigger(StringRef name, StringRef msg, Error const& e) {
		Triggering _(triggering);
		auto iter = events.find(name);
		// strictly speaking this isn't a bug, but having callbacks that aren't caught
		// by anyone could mean that something was misspelled. Therefore, the safe thing
		// to do is to abort.
		ASSERT(iter != events.end());
		std::unordered_map<EventImpl::Id, EventImpl*> callbacks = iter->second;
		// after we collected all unique callbacks we can call each
		for (auto const& c : callbacks) {
			try {
				// it's possible that the callback has been removed in
				// which case attempting to call it will be undefined
				// behavior.
				if (toRemove.count(c.first) > 0) {
					c.second->callback(name, msg, e);
				}
			} catch (...) {
				// callbacks are not allowed to throw
				UNSTOPPABLE_ASSERT(false);
			}
		}
		// merge modifications back into the event map
		for (auto const& p : toRemove) {
			for (auto const& n : p.second) {
				events[n].erase(p.first);
			}
		}
		toRemove.clear();
		for (auto const& p : toInsert) {
			events[p.first].insert(p.second.begin(), p.second.end());
		}
		toInsert.clear();
	}

	void add(StringRef const& name, EventImpl* event) {
		EventMap& m = triggering ? toInsert : events;
		m[name].emplace(event->id(), event);
	}

	void add(std::vector<StringRef> const& names, EventImpl* event) {
		for (auto const& name : names) {
			add(name, event);
		}
	}

	void remove(std::vector<StringRef> names, EventImpl::Id id) {
		if (triggering) {
			toRemove.emplace(id, std::move(names));
		} else {
			for (auto const& name : names) {
				events[name].erase(id);
			}
		}
	}
};

ProcessEventsImpl impl;

void EventImpl::addEvent() {
	impl.add(names, this);
}

void EventImpl::removeEvent() {
	impl.remove(names, this->id());
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
	auto ptr = reinterpret_cast<EventImpl*>(impl);
	ptr->removeEvent();
	delete ptr;
}

} // namespace ProcessEvents