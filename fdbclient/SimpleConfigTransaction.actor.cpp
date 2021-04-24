/*
 * SimpleConfigTransaction.actor.cpp
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

#include "fdbclient/IConfigTransaction.h"
#include "fdbclient/CommitTransaction.h"
#include "flow/Arena.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class SimpleConfigTransactionImpl {
	Standalone<VectorRef<MutationRef>> mutations;
	Future<Version> version;
	ConfigTransactionInterface cti;
	int numRetries{ 0 };

	ACTOR static Future<Version> getVersion(SimpleConfigTransactionImpl* self) {
		ConfigTransactionGetVersionRequest req;
		ConfigTransactionGetVersionReply reply =
		    wait(self->cti.getVersion.getReply(ConfigTransactionGetVersionRequest{}));
		return reply.version;
	}

	ACTOR static Future<Optional<Value>> get(SimpleConfigTransactionImpl* self, KeyRef key) {
		if (!self->version.isValid()) {
			self->version = getVersion(self);
		}
		Version version = wait(self->version);
		ConfigTransactionGetReply result = wait(self->cti.get.getReply(ConfigTransactionGetRequest(version, key)));
		return result.value;
	}

	ACTOR static Future<Standalone<RangeResultRef>> getRange(SimpleConfigTransactionImpl* self, KeyRangeRef keys) {
		if (!self->version.isValid()) {
			self->version = getVersion(self);
		}
		Version version = wait(self->version);
		ConfigTransactionGetRangeReply result =
		    wait(self->cti.getRange.getReply(ConfigTransactionGetRangeRequest(version, keys)));
		return result.range;
	}

	ACTOR static Future<Void> commit(SimpleConfigTransactionImpl* self) {
		if (!self->version.isValid()) {
			self->version = getVersion(self);
		}
		Version version = wait(self->version);
		wait(self->cti.commit.getReply(ConfigTransactionCommitRequest(version, self->mutations)));
		return Void();
	}

public:
	SimpleConfigTransactionImpl(ClusterConnectionString const& ccs) {
		auto coordinators = ccs.coordinators();
		std::sort(coordinators.begin(), coordinators.end());
		cti = ConfigTransactionInterface(coordinators[0]);
	}

	void set(KeyRef key, ValueRef value) {
		mutations.emplace_back_deep(mutations.arena(), MutationRef::Type::SetValue, key, value);
	}

	void clear(KeyRef key) {
		mutations.emplace_back_deep(mutations.arena(), MutationRef::Type::ClearRange, key, keyAfter(key));
		ASSERT(keyAfter(key) > key);
	}

	void clearRange(KeyRef begin, KeyRef end) {
		mutations.emplace_back_deep(mutations.arena(), MutationRef::Type::ClearRange, begin, end);
	}

	Future<Optional<Value>> get(KeyRef key) { return get(this, key); }

	Future<Standalone<RangeResultRef>> getRange(KeyRangeRef keys) { return getRange(this, keys); }

	Future<Void> commit() { return commit(this); }

	Future<Void> onError(Error const& e) {
		// TODO: Improve this:
		if (e.code() == error_code_transaction_too_old) {
			reset();
			return delay((1 << numRetries++) * 0.01 * deterministicRandom()->random01());
		}
		throw e;
	}

	Future<Version> getVersion() {
		if (!version.isValid())
			version = getVersion(this);
		return version;
	}

	void reset() {
		version = Future<Version>{};
		mutations = Standalone<VectorRef<MutationRef>>{};
	}

	void fullReset() {
		numRetries = 0;
		reset();
	}
};

void SimpleConfigTransaction::set(KeyRef key, ValueRef value) {
	impl->set(key, value);
}

void SimpleConfigTransaction::clear(KeyRef key) {
	impl->clear(key);
}

void SimpleConfigTransaction::clearRange(KeyRef begin, KeyRef end) {
	impl->clearRange(begin, end);
}

Future<Optional<Value>> SimpleConfigTransaction::get(KeyRef key) {
	return impl->get(key);
}

Future<Standalone<RangeResultRef>> SimpleConfigTransaction::getRange(KeyRangeRef keys) {
	return impl->getRange(keys);
}

Future<Void> SimpleConfigTransaction::commit() {
	return impl->commit();
}

Future<Void> SimpleConfigTransaction::onError(Error const& e) {
	return impl->onError(e);
}

Future<Version> SimpleConfigTransaction::getVersion() {
	return impl->getVersion();
}

void SimpleConfigTransaction::reset() {
	return impl->reset();
}

void SimpleConfigTransaction::fullReset() {
	return impl->reset();
}

SimpleConfigTransaction::SimpleConfigTransaction(ClusterConnectionString const& ccs)
  : impl(std::make_unique<SimpleConfigTransactionImpl>(ccs)) {}

SimpleConfigTransaction::~SimpleConfigTransaction() = default;
