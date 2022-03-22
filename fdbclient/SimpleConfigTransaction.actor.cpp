/*
 * SimpleConfigTransaction.actor.cpp
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

#include <algorithm>

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/SimpleConfigTransaction.h"
#include "fdbserver/Knobs.h"
#include "flow/Arena.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class SimpleConfigTransactionImpl {
	ConfigTransactionCommitRequest toCommit;
	Future<ConfigGeneration> getGenerationFuture;
	ConfigTransactionInterface cti;
	int numRetries{ 0 };
	bool committed{ false };
	Optional<UID> dID;
	Database cx;

	ACTOR static Future<ConfigGeneration> getGeneration(SimpleConfigTransactionImpl* self) {
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGettingReadVersion", self->dID.get());
		}
		ConfigTransactionGetGenerationRequest req;
		ConfigTransactionGetGenerationReply reply =
		    wait(retryBrokenPromise(self->cti.getGeneration, ConfigTransactionGetGenerationRequest{}));
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGotReadVersion", self->dID.get())
			    .detail("Version", reply.generation.liveVersion);
		}
		return reply.generation;
	}

	ACTOR static Future<Optional<Value>> get(SimpleConfigTransactionImpl* self, KeyRef key) {
		if (!self->getGenerationFuture.isValid()) {
			self->getGenerationFuture = getGeneration(self);
		}
		state ConfigKey configKey = ConfigKey::decodeKey(key);
		ConfigGeneration generation = wait(self->getGenerationFuture);
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGettingValue", self->dID.get())
			    .detail("ConfigClass", configKey.configClass)
			    .detail("KnobName", configKey.knobName);
		}
		ConfigTransactionGetReply reply =
		    wait(retryBrokenPromise(self->cti.get, ConfigTransactionGetRequest{ generation, configKey }));
		if (self->dID.present()) {
			TraceEvent("SimpleConfigTransactionGotValue", self->dID.get())
			    .detail("Value", reply.value.get().toString());
		}
		if (reply.value.present()) {
			return reply.value.get().toValue();
		} else {
			return Optional<Value>{};
		}
	}

	ACTOR static Future<RangeResult> getConfigClasses(SimpleConfigTransactionImpl* self) {
		if (!self->getGenerationFuture.isValid()) {
			self->getGenerationFuture = getGeneration(self);
		}
		ConfigGeneration generation = wait(self->getGenerationFuture);
		ConfigTransactionGetConfigClassesReply reply =
		    wait(retryBrokenPromise(self->cti.getClasses, ConfigTransactionGetConfigClassesRequest{ generation }));
		RangeResult result;
		for (const auto& configClass : reply.configClasses) {
			result.push_back_deep(result.arena(), KeyValueRef(configClass, ""_sr));
		}
		return result;
	}

	ACTOR static Future<RangeResult> getKnobs(SimpleConfigTransactionImpl* self, Optional<Key> configClass) {
		if (!self->getGenerationFuture.isValid()) {
			self->getGenerationFuture = getGeneration(self);
		}
		ConfigGeneration generation = wait(self->getGenerationFuture);
		ConfigTransactionGetKnobsReply reply =
		    wait(retryBrokenPromise(self->cti.getKnobs, ConfigTransactionGetKnobsRequest{ generation, configClass }));
		RangeResult result;
		for (const auto& knobName : reply.knobNames) {
			result.push_back_deep(result.arena(), KeyValueRef(knobName, ""_sr));
		}
		return result;
	}

	ACTOR static Future<Void> commit(SimpleConfigTransactionImpl* self) {
		if (!self->getGenerationFuture.isValid()) {
			self->getGenerationFuture = getGeneration(self);
		}
		wait(store(self->toCommit.generation, self->getGenerationFuture));
		self->toCommit.annotation.timestamp = now();
		wait(retryBrokenPromise(self->cti.commit, self->toCommit));
		self->committed = true;
		return Void();
	}

	ACTOR static Future<Void> onError(SimpleConfigTransactionImpl* self, Error e) {
		// TODO: Improve this:
		if (e.code() == error_code_transaction_too_old || e.code() == error_code_not_committed) {
			wait(delay((1 << self->numRetries++) * 0.01 * deterministicRandom()->random01()));
			self->reset();
			return Void();
		}
		throw e;
	}

public:
	SimpleConfigTransactionImpl(Database const& cx) : cx(cx) {
		auto coordinators = cx->getConnectionRecord()->getConnectionString().coordinators();
		std::sort(coordinators.begin(), coordinators.end());
		cti = ConfigTransactionInterface(coordinators[0]);
	}

	SimpleConfigTransactionImpl(ConfigTransactionInterface const& cti) : cti(cti) {}

	void set(KeyRef key, ValueRef value) {
		toCommit.mutations.push_back_deep(toCommit.arena,
		                                  IKnobCollection::createSetMutation(toCommit.arena, key, value));
	}

	void clear(KeyRef key) {
		toCommit.mutations.push_back_deep(toCommit.arena, IKnobCollection::createClearMutation(toCommit.arena, key));
	}

	Future<Optional<Value>> get(KeyRef key) { return get(this, key); }

	Future<RangeResult> getRange(KeyRangeRef keys) {
		if (keys == configClassKeys) {
			return getConfigClasses(this);
		} else if (keys == globalConfigKnobKeys) {
			return getKnobs(this, {});
		} else if (configKnobKeys.contains(keys) && keys.singleKeyRange()) {
			const auto configClass = keys.begin.removePrefix(configKnobKeys.begin);
			return getKnobs(this, configClass);
		} else {
			throw invalid_config_db_range_read();
		}
	}

	Future<Void> commit() { return commit(this); }

	Future<Void> onError(Error const& e) { return onError(this, e); }

	Future<Version> getReadVersion() {
		if (!getGenerationFuture.isValid())
			getGenerationFuture = getGeneration(this);
		return map(getGenerationFuture, [](auto const& gen) { return gen.committedVersion; });
	}

	Optional<Version> getCachedReadVersion() const {
		if (getGenerationFuture.isValid() && getGenerationFuture.isReady() && !getGenerationFuture.isError()) {
			return getGenerationFuture.get().committedVersion;
		} else {
			return {};
		}
	}

	Version getCommittedVersion() const { return committed ? getGenerationFuture.get().liveVersion : ::invalidVersion; }

	void reset() {
		getGenerationFuture = Future<ConfigGeneration>{};
		toCommit = {};
		committed = false;
	}

	void fullReset() {
		numRetries = 0;
		dID = {};
		reset();
	}

	size_t getApproximateSize() const { return toCommit.expectedSize(); }

	void debugTransaction(UID dID) { this->dID = dID; }

	void checkDeferredError(Error const& deferredError) const {
		if (deferredError.code() != invalid_error_code) {
			throw deferredError;
		}
		if (cx.getPtr()) {
			cx->checkDeferredError();
		}
	}
}; // SimpleConfigTransactionImpl

Future<Version> SimpleConfigTransaction::getReadVersion() {
	return impl->getReadVersion();
}

Optional<Version> SimpleConfigTransaction::getCachedReadVersion() const {
	return impl->getCachedReadVersion();
}

Future<Optional<Value>> SimpleConfigTransaction::get(Key const& key, Snapshot snapshot) {
	return impl->get(key);
}

Future<RangeResult> SimpleConfigTransaction::getRange(KeySelector const& begin,
                                                      KeySelector const& end,
                                                      int limit,
                                                      Snapshot snapshot,
                                                      Reverse reverse) {
	if (reverse) {
		throw client_invalid_operation();
	}
	return impl->getRange(KeyRangeRef(begin.getKey(), end.getKey()));
}

Future<RangeResult> SimpleConfigTransaction::getRange(KeySelector begin,
                                                      KeySelector end,
                                                      GetRangeLimits limits,
                                                      Snapshot snapshot,
                                                      Reverse reverse) {
	if (reverse) {
		throw client_invalid_operation();
	}
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
	impl->checkDeferredError(deferredError);
}

void SimpleConfigTransaction::construct(Database const& cx) {
	impl = PImpl<SimpleConfigTransactionImpl>::create(cx);
}

SimpleConfigTransaction::SimpleConfigTransaction(ConfigTransactionInterface const& cti)
  : impl(PImpl<SimpleConfigTransactionImpl>::create(cti)) {}

SimpleConfigTransaction::SimpleConfigTransaction() = default;

SimpleConfigTransaction::~SimpleConfigTransaction() = default;
