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

#include "fdbclient/SimpleConfigTransaction.h"
#include "fdbclient/CommitTransaction.h"
#include "flow/Arena.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class SimpleConfigTransactionImpl {
	Standalone<VectorRef<MutationRef>> mutations;
	Future<Version> version;
	ConfigTransactionInterface cti;
	int numRetries{ 0 };
	Error deferredError{ success() };
	bool committed{ false };

	ACTOR static Future<Version> getReadVersion(SimpleConfigTransactionImpl* self) {
		ConfigTransactionGetVersionRequest req;
		ConfigTransactionGetVersionReply reply =
		    wait(self->cti.getVersion.getReply(ConfigTransactionGetVersionRequest{}));
		return reply.version;
	}

	ACTOR static Future<Optional<Value>> get(SimpleConfigTransactionImpl* self, KeyRef key) {
		if (!self->version.isValid()) {
			self->version = getReadVersion(self);
		}
		Version version = wait(self->version);
		ConfigTransactionGetReply result = wait(self->cti.get.getReply(ConfigTransactionGetRequest(version, key)));
		return result.value;
	}

	ACTOR static Future<Standalone<RangeResultRef>> getRange(SimpleConfigTransactionImpl* self, KeyRangeRef keys) {
		if (!self->version.isValid()) {
			self->version = getReadVersion(self);
		}
		Version version = wait(self->version);
		ConfigTransactionGetRangeReply result =
		    wait(self->cti.getRange.getReply(ConfigTransactionGetRangeRequest(version, keys)));
		return result.range;
	}

	ACTOR static Future<Void> commit(SimpleConfigTransactionImpl* self) {
		if (!self->version.isValid()) {
			self->version = getReadVersion(self);
		}
		Version version = wait(self->version);
		wait(self->cti.commit.getReply(ConfigTransactionCommitRequest(version, self->mutations)));
		self->committed = true;
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

	Future<Version> getReadVersion() {
		if (!version.isValid())
			version = getReadVersion(this);
		return version;
	}

	Optional<Version> getCachedReadVersion() {
		if (version.isValid() && version.isReady() && !version.isError()) {
			return version.get();
		} else {
			return {};
		}
	}

	Version getCommittedVersion() const { return committed ? version.get() : ::invalidVersion; }

	void reset() {
		version = Future<Version>{};
		mutations = Standalone<VectorRef<MutationRef>>{};
	}

	void fullReset() {
		numRetries = 0;
		reset();
	}

	Error& getMutableDeferredError() { return deferredError; }

}; // SimpleConfigTransactionImpl

Future<Version> SimpleConfigTransaction::getReadVersion() {
	return impl->getReadVersion();
}

Optional<Version> SimpleConfigTransaction::getCachedReadVersion() {
	return impl->getCachedReadVersion();
}

Future<Optional<Value>> SimpleConfigTransaction::get(Key const& key, bool snapshot) {
	return impl->get(key);
}

Future<Standalone<RangeResultRef>> SimpleConfigTransaction::getRange(KeySelector const& begin,
                                                                     KeySelector const& end,
                                                                     int limit,
                                                                     bool snapshot,
                                                                     bool reverse) {
	return impl->getRange(KeyRangeRef(begin.getKey(), end.getKey()));
}

Future<Standalone<RangeResultRef>> SimpleConfigTransaction::getRange(KeySelector begin,
                                                                     KeySelector end,
                                                                     GetRangeLimits limits,
                                                                     bool snapshot,
                                                                     bool reverse) {
	return impl->getRange(KeyRangeRef(begin.getKey(), end.getKey()));
}

void SimpleConfigTransaction::set(KeyRef const& key, ValueRef const& value) {
	impl->set(key, value);
}

void SimpleConfigTransaction::clear(KeyRef const& key) {
	impl->clear(key);
}

void SimpleConfigTransaction::clear(KeyRangeRef const& range) {
	impl->clearRange(range.begin, range.end);
}

Future<Void> SimpleConfigTransaction::commit() {
	return impl->commit();
}

Version SimpleConfigTransaction::getCommittedVersion() {
	return impl->getCommittedVersion();
}

int64_t SimpleConfigTransaction::getApproximateSize() {
	// TODO: Implement
	return 0;
}

void SimpleConfigTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	// TODO: Support using this option to determine atomicity
}

Future<Void> SimpleConfigTransaction::onError(Error const& e) {
	return impl->onError(e);
}

void SimpleConfigTransaction::cancel() {
	// TODO: Implement
	throw client_invalid_operation();
}

void SimpleConfigTransaction::reset() {
	return impl->reset();
}

void SimpleConfigTransaction::fullReset() {
	return impl->reset();
}

void SimpleConfigTransaction::debugTransaction(UID dID) {
	// TODO: Implement
}

void SimpleConfigTransaction::checkDeferredError() {
	// TODO: Implement
}

void SimpleConfigTransaction::getWriteConflicts(KeyRangeMap<bool>* result) {}

Error& SimpleConfigTransaction::getMutableDeferredError() {
	return impl->getMutableDeferredError();
}

SimpleConfigTransaction::SimpleConfigTransaction(ClusterConnectionString const& ccs)
  : impl(std::make_unique<SimpleConfigTransactionImpl>(ccs)) {}

SimpleConfigTransaction::~SimpleConfigTransaction() = default;
