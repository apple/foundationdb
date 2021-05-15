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
	Standalone<VectorRef<ConfigMutationRef>> mutations;
	Future<Version> version;
	Key description;
	ConfigTransactionInterface cti;
	int numRetries{ 0 };
	bool committed{ false };
	Optional<UID> dID;
	Promise<Void> resetPromise; // TODO: Make this a field of ISingleThreadTransaction?

	ACTOR static Future<Version> getReadVersion(SimpleConfigTransactionImpl* self) {
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGettingReadVersion", self->dID.get());
		}
		ConfigTransactionGetVersionRequest req;
		ConfigTransactionGetVersionReply reply =
		    wait(self->cti.getVersion.getReply(ConfigTransactionGetVersionRequest{}));
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGotReadVersion", self->dID.get()).detail("Version", reply.version);
		}
		return reply.version;
	}

	ACTOR static Future<Optional<Value>> get(SimpleConfigTransactionImpl* self, KeyRef key) {
		if (!self->version.isValid()) {
			self->version = getReadVersion(self);
		}
		Version version = wait(self->version);
		auto configKey = ConfigKey::decodeKey(key);
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGettingValue", self->dID.get())
			    .detail("ConfigClass", configKey.configClass)
			    .detail("KnobName", configKey.knobName);
		}
		ConfigTransactionGetReply result =
		    wait(self->cti.get.getReply(ConfigTransactionGetRequest(version, configKey)));
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGotValue", self->dID.get()).detail("Value", result.value);
		}
		return result.value;
	}

	ACTOR static Future<Standalone<RangeResultRef>> getRange(SimpleConfigTransactionImpl* self, KeyRangeRef keys) {
		// TODO: Implement
		wait(delay(0));
		return Standalone<RangeResultRef>{};
		/*
		if (!self->version.isValid()) {
		    self->version = getReadVersion(self);
		}
		Version version = wait(self->version);
		ConfigTransactionGetRangeReply result =
		    wait(self->cti.getRange.getReply(ConfigTransactionGetRangeRequest(version, keys)));
		return result.range;
		*/
	}

	ACTOR static Future<Void> commit(SimpleConfigTransactionImpl* self) {
		if (!self->version.isValid()) {
			self->version = getReadVersion(self);
		}
		Version version = wait(self->version);
		auto commitTime = now();
		for (auto& mutation : self->mutations) {
			mutation.setTimestamp(commitTime);
			mutation.setDescription(self->description);
		}
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

	SimpleConfigTransactionImpl(ConfigTransactionInterface const& cti) : cti(cti) {}

	void set(KeyRef key, ValueRef value) {
		if (key == "\xff\xff/description"_sr) {
			description = value;
		}
		mutations.push_back_deep(mutations.arena(), ConfigMutationRef::createConfigMutation(key, value));
	}

	void clear(KeyRef key) {
		mutations.emplace_back_deep(mutations.arena(), ConfigMutationRef::createConfigMutation(key, {}));
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
		mutations = Standalone<VectorRef<ConfigMutationRef>>{};
		committed = false;
	}

	void fullReset() {
		numRetries = 0;
		dID = {};
		reset();
	}

	size_t getApproximateSize() const { return mutations.expectedSize(); }

	void debugTransaction(UID dID) {
		printf("HERE_ImplSetDebugTransaction\n");
		this->dID = dID;
	}

}; // SimpleConfigTransactionImpl

Future<Version> SimpleConfigTransaction::getReadVersion() {
	return impl->getReadVersion();
}

Optional<Version> SimpleConfigTransaction::getCachedReadVersion() const {
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

Future<Void> SimpleConfigTransaction::commit() {
	return impl->commit();
}

Version SimpleConfigTransaction::getCommittedVersion() const {
	return impl->getCommittedVersion();
}

int64_t SimpleConfigTransaction::getApproximateSize() const {
	return impl->getApproximateSize();
}

void SimpleConfigTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	// TODO: Support using this option to determine atomicity
}

Future<Void> SimpleConfigTransaction::onError(Error const& e) {
	return impl->onError(e);
}

void SimpleConfigTransaction::cancel() {
	// TODO: Implement someday
	throw client_invalid_operation();
}

void SimpleConfigTransaction::reset() {
	return impl->reset();
}

void SimpleConfigTransaction::fullReset() {
	return impl->fullReset();
}

void SimpleConfigTransaction::debugTransaction(UID dID) {
	impl->debugTransaction(dID);
}

void SimpleConfigTransaction::checkDeferredError() const {
	// TODO: Also check for deferred error in database?
	if (deferredError.code() != invalid_error_code) {
		throw deferredError;
	}
}

void SimpleConfigTransaction::getWriteConflicts(KeyRangeMap<bool>* result) {}

SimpleConfigTransaction::SimpleConfigTransaction(ClusterConnectionString const& ccs)
  : impl(std::make_unique<SimpleConfigTransactionImpl>(ccs)) {}

SimpleConfigTransaction::SimpleConfigTransaction(ConfigTransactionInterface const& cti)
  : impl(std::make_unique<SimpleConfigTransactionImpl>(cti)) {}

SimpleConfigTransaction::~SimpleConfigTransaction() = default;
