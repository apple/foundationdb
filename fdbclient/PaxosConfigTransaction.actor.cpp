/*
 * PaxosConfigTransaction.actor.cpp
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/PaxosConfigTransaction.h"
#include "flow/actorcompiler.h" // must be last include

class PaxosConfigTransactionImpl {
	ConfigTransactionCommitRequest toCommit;
	Future<ConfigGeneration> getGenerationFuture;
	std::vector<ConfigTransactionInterface> ctis;
	int numRetries{ 0 };
	bool committed{ false };
	Optional<UID> dID;
	Database cx;

	ACTOR static Future<ConfigGeneration> getGeneration(PaxosConfigTransactionImpl* self) {
		state std::vector<Future<ConfigTransactionGetGenerationReply>> getGenerationFutures;
		getGenerationFutures.reserve(self->ctis.size());
		for (auto const& cti : self->ctis) {
			getGenerationFutures.push_back(cti.getGeneration.getReply(ConfigTransactionGetGenerationRequest{}));
		}
		// FIXME: Must tolerate failures and disagreement
		wait(waitForAll(getGenerationFutures));
		return getGenerationFutures[0].get().generation;
	}

	ACTOR static Future<Optional<Value>> get(PaxosConfigTransactionImpl* self, Key key) {
		if (!self->getGenerationFuture.isValid()) {
			self->getGenerationFuture = getGeneration(self);
		}
		state ConfigKey configKey = ConfigKey::decodeKey(key);
		ConfigGeneration generation = wait(self->getGenerationFuture);
		// TODO: Load balance
		ConfigTransactionGetReply reply =
		    wait(self->ctis[0].get.getReply(ConfigTransactionGetRequest{ generation, configKey }));
		if (reply.value.present()) {
			return reply.value.get().toValue();
		} else {
			return Optional<Value>{};
		}
	}

	ACTOR static Future<RangeResult> getConfigClasses(PaxosConfigTransactionImpl* self) {
		if (!self->getGenerationFuture.isValid()) {
			self->getGenerationFuture = getGeneration(self);
		}
		ConfigGeneration generation = wait(self->getGenerationFuture);
		// TODO: Load balance
		ConfigTransactionGetConfigClassesReply reply =
		    wait(self->ctis[0].getClasses.getReply(ConfigTransactionGetConfigClassesRequest{ generation }));
		RangeResult result;
		result.reserve(result.arena(), reply.configClasses.size());
		for (const auto& configClass : reply.configClasses) {
			result.push_back_deep(result.arena(), KeyValueRef(configClass, ""_sr));
		}
		return result;
	}

	ACTOR static Future<RangeResult> getKnobs(PaxosConfigTransactionImpl* self, Optional<Key> configClass) {
		if (!self->getGenerationFuture.isValid()) {
			self->getGenerationFuture = getGeneration(self);
		}
		ConfigGeneration generation = wait(self->getGenerationFuture);
		// TODO: Load balance
		ConfigTransactionGetKnobsReply reply =
		    wait(self->ctis[0].getKnobs.getReply(ConfigTransactionGetKnobsRequest{ generation, configClass }));
		RangeResult result;
		result.reserve(result.arena(), reply.knobNames.size());
		for (const auto& knobName : reply.knobNames) {
			result.push_back_deep(result.arena(), KeyValueRef(knobName, ""_sr));
		}
		return result;
	}

	ACTOR static Future<Void> commit(PaxosConfigTransactionImpl* self) {
		if (!self->getGenerationFuture.isValid()) {
			self->getGenerationFuture = getGeneration(self);
		}
		wait(store(self->toCommit.generation, self->getGenerationFuture));
		self->toCommit.annotation.timestamp = now();
		std::vector<Future<Void>> commitFutures;
		commitFutures.reserve(self->ctis.size());
		for (const auto& cti : self->ctis) {
			commitFutures.push_back(cti.commit.getReply(self->toCommit));
		}
		// FIXME: Must tolerate failures and disagreement
		wait(quorum(commitFutures, commitFutures.size() / 2 + 1));
		self->committed = true;
		return Void();
	}

public:
	Future<Version> getReadVersion() {
		if (!getGenerationFuture.isValid()) {
			getGenerationFuture = getGeneration(this);
		}
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

	int64_t getApproximateSize() const { return toCommit.expectedSize(); }

	void set(KeyRef key, ValueRef value) { toCommit.set(key, value); }

	void clear(KeyRef key) { toCommit.clear(key); }

	Future<Optional<Value>> get(Key const& key) { return get(this, key); }

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

	Future<Void> onError(Error const& e) {
		// TODO: Improve this:
		if (e.code() == error_code_transaction_too_old) {
			reset();
			return delay((1 << numRetries++) * 0.01 * deterministicRandom()->random01());
		}
		throw e;
	}

	void debugTransaction(UID dID) { this->dID = dID; }

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

	void checkDeferredError(Error const& deferredError) const {
		if (deferredError.code() != invalid_error_code) {
			throw deferredError;
		}
		if (cx.getPtr()) {
			cx->checkDeferredError();
		}
	}

	Future<Void> commit() { return commit(this); }

	PaxosConfigTransactionImpl(Database const& cx) : cx(cx) {
		auto coordinators = cx->getConnectionFile()->getConnectionString().coordinators();
		ctis.reserve(coordinators.size());
		for (const auto& coordinator : coordinators) {
			ctis.emplace_back(coordinator);
		}
	}

	PaxosConfigTransactionImpl(std::vector<ConfigTransactionInterface> const& ctis) : ctis(ctis) {}
};

Future<Version> PaxosConfigTransaction::getReadVersion() {
	return impl->getReadVersion();
}

Optional<Version> PaxosConfigTransaction::getCachedReadVersion() const {
	return impl->getCachedReadVersion();
}

Future<Optional<Value>> PaxosConfigTransaction::get(Key const& key, Snapshot) {
	return impl->get(key);
}

Future<RangeResult> PaxosConfigTransaction::getRange(KeySelector const& begin,
                                                     KeySelector const& end,
                                                     int limit,
                                                     Snapshot snapshot,
                                                     Reverse reverse) {
	if (reverse) {
		throw client_invalid_operation();
	}
	return impl->getRange(KeyRangeRef(begin.getKey(), end.getKey()));
}

Future<RangeResult> PaxosConfigTransaction::getRange(KeySelector begin,
                                                     KeySelector end,
                                                     GetRangeLimits limits,
                                                     Snapshot snapshot,
                                                     Reverse reverse) {
	if (reverse) {
		throw client_invalid_operation();
	}
	return impl->getRange(KeyRangeRef(begin.getKey(), end.getKey()));
}

void PaxosConfigTransaction::set(KeyRef const& key, ValueRef const& value) {
	return impl->set(key, value);
}

void PaxosConfigTransaction::clear(KeyRef const& key) {
	return impl->clear(key);
}

Future<Void> PaxosConfigTransaction::commit() {
	return impl->commit();
}

Version PaxosConfigTransaction::getCommittedVersion() const {
	return impl->getCommittedVersion();
}

int64_t PaxosConfigTransaction::getApproximateSize() const {
	return impl->getApproximateSize();
}

void PaxosConfigTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	// TODO: Support using this option to determine atomicity
}

Future<Void> PaxosConfigTransaction::onError(Error const& e) {
	return impl->onError(e);
}

void PaxosConfigTransaction::cancel() {
	// TODO: Implement someday
	throw client_invalid_operation();
}

void PaxosConfigTransaction::reset() {
	impl->reset();
}

void PaxosConfigTransaction::fullReset() {
	impl->fullReset();
}

void PaxosConfigTransaction::debugTransaction(UID dID) {
	impl->debugTransaction(dID);
}

void PaxosConfigTransaction::checkDeferredError() const {
	impl->checkDeferredError(deferredError);
}

PaxosConfigTransaction::PaxosConfigTransaction(std::vector<ConfigTransactionInterface> const& ctis)
  : impl(PImpl<PaxosConfigTransactionImpl>::create(ctis)) {}

PaxosConfigTransaction::PaxosConfigTransaction() = default;

PaxosConfigTransaction::~PaxosConfigTransaction() = default;

void PaxosConfigTransaction::setDatabase(Database const& cx) {
	impl = PImpl<PaxosConfigTransactionImpl>::create(cx);
}
