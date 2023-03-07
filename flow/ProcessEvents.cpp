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
#include "flow/UnitTest.h"

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
				if (toRemove.count(c.first) == 0) {
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

ProcessEventsImpl processEventsImpl;

void EventImpl::addEvent() {
	processEventsImpl.add(names, this);
}

void EventImpl::removeEvent() {
	processEventsImpl.remove(names, this->id());
}

} // namespace

namespace ProcessEvents {

void trigger(StringRef name, StringRef msg, Error const& e) {
	processEventsImpl.trigger(name, msg, e);
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

TEST_CASE("/flow/ProcessEvents") {
	{
		// Basic test
		unsigned numHits = 0;
		Event _("basic"_sr, [&numHits](StringRef n, StringRef, Error const& e) {
			ASSERT_EQ(n, "basic"_sr);
			ASSERT_EQ(e.code(), error_code_success);
			++numHits;
		});
		trigger("basic"_sr, ""_sr, success());
		ASSERT(numHits == 1);
		trigger("basic"_sr, ""_sr, success());
	}
	{
		// Test that Events can be added during a trigger
		unsigned hits1 = 0;
		std::vector<std::shared_ptr<Event>> createdEvents;
		std::vector<unsigned> numHits;
		numHits.reserve(2);
		Event _("create"_sr, [&](StringRef n, StringRef, Error const& e) {
			ASSERT_EQ(n, "create"_sr);
			ASSERT_EQ(e.code(), error_code_success);
			++hits1;
			numHits.push_back(0);
			createdEvents.push_back(
			    std::make_shared<Event>(std::vector<StringRef>{ "create"_sr, "secondaries"_sr },
			                            [&numHits, idx = numHits.size() - 1](StringRef n, StringRef, Error const& e) {
				                            ASSERT(n == "create"_sr || n == "secondaries");
				                            ASSERT_EQ(e.code(), error_code_success);
				                            ++numHits[idx];
			                            }));
		});
		trigger("create"_sr, ""_sr, success());
		ASSERT_EQ(hits1, 1);
		ASSERT_EQ(createdEvents.size(), 1);
		ASSERT_EQ(numHits.size(), 1);
		// an event that is created in a callback mustn't be called in the same trigger
		ASSERT_EQ(numHits[0], 0);
		trigger("create"_sr, ""_sr, success());
		ASSERT_EQ(hits1, 2);
		ASSERT_EQ(createdEvents.size(), 2);
		ASSERT_EQ(numHits.size(), 2);
		ASSERT_EQ(numHits[0], 1);
		ASSERT_EQ(numHits[1], 0);
		trigger("secondaries"_sr, ""_sr, success());
		ASSERT_EQ(hits1, 2);
		ASSERT_EQ(createdEvents.size(), 2);
		ASSERT_EQ(numHits.size(), 2);
		ASSERT_EQ(numHits[0], 2);
		ASSERT_EQ(numHits[1], 1);
	}
	{
		// Remove self
		unsigned called_self_delete = 0, called_non_delete = 0;
		std::unique_ptr<Event> ev;
		auto callback_del = [&](StringRef n, StringRef, Error const& e) {
			ASSERT_EQ(n, "deletion"_sr);
			ASSERT_EQ(e.code(), error_code_success);
			++called_self_delete;
			// remove Event
			ev.reset();
		};
		ev.reset(new Event("deletion"_sr, callback_del));
		Event _("deletion"_sr, [&](StringRef n, StringRef, Error const& e) {
			ASSERT_EQ(n, "deletion"_sr);
			ASSERT_EQ(e.code(), error_code_success);
			++called_non_delete;
		});
		trigger("deletion"_sr, ""_sr, success());
		trigger("deletion"_sr, ""_sr, success());
		ASSERT_EQ(called_self_delete, 1);
		ASSERT_EQ(called_non_delete, 2);
	}
	return Void();
}

} // namespace ProcessEvents