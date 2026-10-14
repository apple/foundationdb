/*
 * StorageHealthRegistry.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "flow/flow.h"
#include "flow/genericactors.h"

#include <cstdint>
#include <limits>
#include <utility>

// Registration survives an intentional storage reboot, but never its role owner.
class StorageHealthRegistry : public ReferenceCounted<StorageHealthRegistry>, NonCopyable {
public:
	class Registration;
	class Role;

private:
	class RoleState : public ReferenceCounted<RoleState>, NonCopyable {
	public:
		explicit RoleState(Reference<StorageHealthRegistry> registry) : registry(std::move(registry)) {}

		uint64_t beginChild() {
			ASSERT(!closed && generation != std::numeric_limits<uint64_t>::max());
			return ++generation;
		}

		void registered(uint64_t childGeneration) {
			if (!closed && childGeneration == generation && !hasRegistered) {
				hasRegistered = true;
				registry->registeredRoles.set(registry->registeredRoles.get() + 1);
			}
		}

		void retireCurrent(uint64_t childGeneration) {
			if (childGeneration == generation) {
				close();
			}
		}

		void close() {
			closed = true;
			if (hasRegistered) {
				hasRegistered = false;
				ASSERT(registry->registeredRoles.get() > 0);
				registry->registeredRoles.set(registry->registeredRoles.get() - 1);
			}
		}

	private:
		Reference<StorageHealthRegistry> registry;
		uint64_t generation = 0;
		bool hasRegistered = false;
		bool closed = false;
	};

public:
	class Registration {
	public:
		Registration(const Registration&) = default;
		Registration& operator=(const Registration&) = default;

		void registered() const {
			// A notification can release the caller's handle synchronously.
			auto retained = state;
			retained->registered(generation);
		}

		void retireCurrent() const {
			auto retained = state;
			retained->retireCurrent(generation);
		}

		void terminated(const Error& error) const {
			// I/O cancellation can become an intentional reboot; the role owner decides its final retirement.
			if (error.code() != error_code_actor_cancelled && error.code() != error_code_please_reboot &&
			    error.code() != error_code_please_reboot_kv_store) {
				retireCurrent();
			}
		}

	private:
		friend class Role;
		Registration(Reference<RoleState> state, uint64_t generation)
		  : state(std::move(state)), generation(generation) {}

		Reference<RoleState> state;
		uint64_t generation;
	};

	class Role {
	public:
		Role(const Role&) = delete;
		Role& operator=(const Role&) = delete;
		Role(Role&& other) noexcept : state(std::move(other.state)) {}
		Role& operator=(Role&& other) noexcept {
			if (this != &other) {
				auto previous = std::move(state);
				state = std::move(other.state);
				if (previous.isValid()) {
					previous->close();
				}
			}
			return *this;
		}
		~Role() { close(); }

		Registration beginChild() {
			ASSERT(state.isValid());
			return Registration(state, state->beginChild());
		}

		void close() {
			// Retire independently of handles retained by an old child.
			auto owned = std::move(state);
			if (owned.isValid()) {
				owned->close();
			}
		}

	private:
		friend class StorageHealthRegistry;
		explicit Role(Reference<RoleState> state) : state(std::move(state)) {}

		Reference<RoleState> state;
	};

	Role newRole() { return Role(makeReference<RoleState>(Reference<StorageHealthRegistry>::addRef(this))); }
	bool hasRegisteredStorage() const { return registeredRoles.get() != 0; }
	Future<Void> onChange() const { return registeredRoles.onChange(); }

private:
	AsyncVar<int> registeredRoles{ 0 };
};
