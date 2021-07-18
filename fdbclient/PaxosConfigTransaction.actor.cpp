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

public:
	Future<Version> getReadVersion() {
		if (!getGenerationFuture.isValid()) {
			getGenerationFuture = getGeneration(this);
		}
		return map(getGenerationFuture, [](auto const& gen) { return gen.committedVersion; });
	}

	Optional<Version> getCachedReadVersion() const {
		if (getGenerationFuture.isValid() && getGenerationFuture.isReady() && !getGenerationFuture.isError()) {
			return getGenerationFuture.get().liveVersion;
		} else {
			return {};
		}
	}

	Version getCommittedVersion() const { return committed ? getGenerationFuture.get().liveVersion : ::invalidVersion; }

	int64_t getApproximateSize() const { return toCommit.expectedSize(); }

	void set(KeyRef key, ValueRef value) { toCommit.set(key, value); }

	void clear(KeyRef key) { toCommit.clear(key); }

	Future<Optional<Value>> get(Key const& key) { return get(this, key); }

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
	return impl().getReadVersion();
}

Optional<Version> PaxosConfigTransaction::getCachedReadVersion() const {
	return impl().getCachedReadVersion();
}

Future<Optional<Value>> PaxosConfigTransaction::get(Key const& key, Snapshot) {
	return impl().get(key);
}

Future<Standalone<RangeResultRef>> PaxosConfigTransaction::getRange(KeySelector const& begin,
                                                                    KeySelector const& end,
                                                                    int limit,
                                                                    Snapshot snapshot,
                                                                    Reverse reverse) {
	// TODO: Implement
	ASSERT(false);
	return Standalone<RangeResultRef>{};
}

Future<Standalone<RangeResultRef>> PaxosConfigTransaction::getRange(KeySelector begin,
                                                                    KeySelector end,
                                                                    GetRangeLimits limits,
                                                                    Snapshot snapshot,
                                                                    Reverse reverse) {
	// TODO: Implement
	ASSERT(false);
	return Standalone<RangeResultRef>{};
}

void PaxosConfigTransaction::set(KeyRef const& key, ValueRef const& value) {
	return impl().set(key, value);
}

void PaxosConfigTransaction::clear(KeyRef const& key) {
	return impl().clear(key);
}

Future<Void> PaxosConfigTransaction::commit() {
	// TODO: Implememnt
	ASSERT(false);
	return Void();
}

Version PaxosConfigTransaction::getCommittedVersion() const {
	return impl().getCommittedVersion();
}

int64_t PaxosConfigTransaction::getApproximateSize() const {
	return impl().getApproximateSize();
}

void PaxosConfigTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	// TODO: Support using this option to determine atomicity
}

Future<Void> PaxosConfigTransaction::onError(Error const& e) {
	return impl().onError(e);
}

void PaxosConfigTransaction::cancel() {
	// TODO: Implement someday
	throw client_invalid_operation();
}

void PaxosConfigTransaction::reset() {
	impl().reset();
}

void PaxosConfigTransaction::fullReset() {
	impl().fullReset();
}

void PaxosConfigTransaction::debugTransaction(UID dID) {
	impl().debugTransaction(dID);
}

void PaxosConfigTransaction::checkDeferredError() const {
	impl().checkDeferredError(deferredError);
}

PaxosConfigTransaction::PaxosConfigTransaction(std::vector<ConfigTransactionInterface> const& ctis)
  : _impl(std::make_unique<PaxosConfigTransactionImpl>(ctis)) {}

PaxosConfigTransaction::PaxosConfigTransaction() = default;

PaxosConfigTransaction::~PaxosConfigTransaction() = default;

void PaxosConfigTransaction::setDatabase(Database const& cx) {
	_impl = std::make_unique<PaxosConfigTransactionImpl>(cx);
}
