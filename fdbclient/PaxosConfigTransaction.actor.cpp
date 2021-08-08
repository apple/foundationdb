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

namespace {

// TODO: Some replicas may reply after quorum has already been achieved, and we may want to add them to the readReplicas
// list
class GetGenerationQuorum {
public:
	struct Result {
		ConfigGeneration generation;
		std::vector<ConfigTransactionInterface> readReplicas;
		Result(ConfigGeneration const& generation, std::vector<ConfigTransactionInterface> const& readReplicas)
		  : generation(generation), readReplicas(readReplicas) {}
		Result() = default;
	};

private:
	std::vector<Future<Void>> futures;
	std::map<ConfigGeneration, std::vector<ConfigTransactionInterface>> seenGenerations;
	Promise<Result> result;
	size_t totalRepliesReceived{ 0 };
	size_t maxAgreement{ 0 };
	size_t size{ 0 };
	Optional<Version> lastSeenLiveVersion;

	ACTOR static Future<Void> addRequestActor(GetGenerationQuorum* self, ConfigTransactionInterface cti) {
		ConfigTransactionGetGenerationReply reply =
		    wait(cti.getGeneration.getReply(ConfigTransactionGetGenerationRequest{ self->lastSeenLiveVersion }));
		++self->totalRepliesReceived;
		auto gen = reply.generation;
		self->lastSeenLiveVersion = std::max(gen.liveVersion, self->lastSeenLiveVersion.orDefault(::invalidVersion));
		auto& replicas = self->seenGenerations[gen];
		replicas.push_back(cti);
		self->maxAgreement = std::max(replicas.size(), self->maxAgreement);
		if (replicas.size() == self->size / 2 + 1) {
			self->result.send(Result{ gen, replicas });
		} else if (self->maxAgreement + (self->size - self->totalRepliesReceived) < (self->size / 2 + 1)) {
			self->result.sendError(failed_to_reach_quorum());
		}
		return Void();
	}

public:
	GetGenerationQuorum(size_t size, Optional<Version> const& lastSeenLiveVersion)
	  : size(size), lastSeenLiveVersion(lastSeenLiveVersion) {
		futures.reserve(size);
	}
	void addRequest(ConfigTransactionInterface cti) { futures.push_back(addRequestActor(this, cti)); }
	Future<Result> getResult() const { return result.getFuture(); }
	Optional<Version> getLastSeenLiveVersion() const { return lastSeenLiveVersion; }
};

} // namespace

class PaxosConfigTransactionImpl {
	ConfigTransactionCommitRequest toCommit;
	Future<GetGenerationQuorum::Result> getGenerationFuture;
	std::vector<ConfigTransactionInterface> ctis;
	int numRetries{ 0 };
	bool committed{ false };
	Optional<Version> lastSeenLiveVersion;
	Optional<UID> dID;
	Database cx;
	std::vector<ConfigTransactionInterface> readReplicas;

	ACTOR static Future<GetGenerationQuorum::Result> getGeneration(PaxosConfigTransactionImpl* self) {
		state GetGenerationQuorum quorum(self->ctis.size(), self->lastSeenLiveVersion);
		state int retries = 0;
		loop {
			for (auto const& cti : self->ctis) {
				quorum.addRequest(cti);
			}
			try {
				state GetGenerationQuorum::Result result = wait(quorum.getResult());
				wait(delay(0.0)); // Let reply callback actors finish before destructing quorum
				return result;
			} catch (Error& e) {
				if (e.code() == error_code_failed_to_reach_quorum) {
					TEST(true); // Failed to reach quorum getting generation
					wait(delayJittered(0.01 * (1 << retries)));
					++retries;
				} else {
					throw e;
				}
			}
			self->lastSeenLiveVersion = quorum.getLastSeenLiveVersion();
		}
	}

	ACTOR static Future<Optional<Value>> get(PaxosConfigTransactionImpl* self, Key key) {
		if (!self->getGenerationFuture.isValid()) {
			self->getGenerationFuture = getGeneration(self);
		}
		state ConfigKey configKey = ConfigKey::decodeKey(key);
		GetGenerationQuorum::Result genResult = wait(self->getGenerationFuture);
		// TODO: Load balance
		ConfigTransactionGetReply reply = wait(
		    genResult.readReplicas[0].get.getReply(ConfigTransactionGetRequest{ genResult.generation, configKey }));
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
		GetGenerationQuorum::Result genResult = wait(self->getGenerationFuture);
		// TODO: Load balance
		ConfigTransactionGetConfigClassesReply reply = wait(genResult.readReplicas[0].getClasses.getReply(
		    ConfigTransactionGetConfigClassesRequest{ genResult.generation }));
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
		GetGenerationQuorum::Result genResult = wait(self->getGenerationFuture);
		// TODO: Load balance
		ConfigTransactionGetKnobsReply reply = wait(genResult.readReplicas[0].getKnobs.getReply(
		    ConfigTransactionGetKnobsRequest{ genResult.generation, configClass }));
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
		GetGenerationQuorum::Result genResult = wait(self->getGenerationFuture);
		self->toCommit.generation = genResult.generation;
		self->toCommit.annotation.timestamp = now();
		std::vector<Future<Void>> commitFutures;
		commitFutures.reserve(self->ctis.size());
		// Send commit message to all replicas, even those that did not return the used replica.
		// This way, slow replicas are kept up date.
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
		return map(getGenerationFuture, [](auto const& genResult) { return genResult.generation.committedVersion; });
	}

	Optional<Version> getCachedReadVersion() const {
		if (getGenerationFuture.isValid() && getGenerationFuture.isReady() && !getGenerationFuture.isError()) {
			return getGenerationFuture.get().generation.committedVersion;
		} else {
			return {};
		}
	}

	Version getCommittedVersion() const {
		return committed ? getGenerationFuture.get().generation.liveVersion : ::invalidVersion;
	}

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
		getGenerationFuture = Future<GetGenerationQuorum::Result>{};
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
