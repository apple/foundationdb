/*
 * SimpleConfigurationDatabase.actor.cpp
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

#include <algorithm>

#include "fdbclient/IConfigurationDatabase.h"
#include "fdbclient/CommitTransaction.h"
#include "flow/Arena.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class SimpleConfigurationTransactionImpl {
	Standalone<VectorRef<MutationRef>> mutations;
	Future<Version> version;
	ConfigDatabaseInterface cdbi;

	ACTOR static Future<Version> getVersion(SimpleConfigurationTransactionImpl* self) {
		ConfigDatabaseGetVersionRequest req;
		ConfigDatabaseGetVersionReply reply = wait(self->cdbi.getVersion.getReply(ConfigDatabaseGetVersionRequest{}));
		return reply.version;
	}

	ACTOR static Future<Optional<Value>> get(SimpleConfigurationTransactionImpl* self, KeyRef key) {
		if (!self->version.isValid()) {
			self->version = getVersion(self);
		}
		Version version = wait(self->version);
		ConfigDatabaseGetReply result = wait(self->cdbi.get.getReply(ConfigDatabaseGetRequest(version, key)));
		return result.value;
	}

	ACTOR static Future<Void> commit(SimpleConfigurationTransactionImpl* self) {
		if (!self->version.isValid()) {
			self->version = getVersion(self);
		}
		Version version = wait(self->version);
		wait(self->cdbi.commit.getReply(ConfigDatabaseCommitRequest(version, self->mutations)));
		return Void();
	}

public:
	SimpleConfigurationTransactionImpl(ClusterConnectionString const& ccs) {
		auto coordinators = ccs.coordinators();
		std::sort(coordinators.begin(), coordinators.end());
		cdbi = ConfigDatabaseInterface(coordinators[0]);
	}

	void set(KeyRef key, ValueRef value) {
		mutations.emplace_back_deep(mutations.arena(), MutationRef::Type::SetValue, key, value);
	}

	void clearRange(KeyRef begin, KeyRef end) {
		mutations.emplace_back_deep(mutations.arena(), MutationRef::Type::ClearRange, begin, end);
	}

	Future<Optional<Value>> get(KeyRef key) { return get(this, key); }

	Future<Void> commit() { return commit(this); }

	Future<Void> onError(Error const& e) { throw e; }

	void reset() {
		version.cancel();
		mutations = Standalone<VectorRef<MutationRef>>{};
	}
};

void SimpleConfigurationTransaction::set(KeyRef key, ValueRef value) {
	impl->set(key, value);
}

void SimpleConfigurationTransaction::clearRange(KeyRef begin, KeyRef end) {
	impl->clearRange(begin, end);
}

Future<Optional<Value>> SimpleConfigurationTransaction::get(KeyRef key) {
	return impl->get(key);
}

Future<Void> SimpleConfigurationTransaction::commit() {
	return impl->commit();
}

Future<Void> SimpleConfigurationTransaction::onError(Error const& e) {
	return impl->onError(e);
}

void SimpleConfigurationTransaction::reset() {
	return impl->reset();
}

SimpleConfigurationTransaction::SimpleConfigurationTransaction(ClusterConnectionString const& ccs)
  : impl(std::make_unique<SimpleConfigurationTransactionImpl>(ccs)) {}

SimpleConfigurationTransaction::~SimpleConfigurationTransaction() = default;
