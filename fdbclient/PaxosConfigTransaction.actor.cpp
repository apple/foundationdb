/*
 * PaxosConfigTransaction.actor.cpp
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/PaxosConfigTransaction.h"
#include "flow/actorcompiler.h" // must be last include

using ConfigTransactionInfo = ModelInterface<ConfigTransactionInterface>;

class CommitQuorum {
	ActorCollection actors{ false };
	std::vector<ConfigTransactionInterface> ctis;
	size_t failed{ 0 };
	size_t successful{ 0 };
	size_t maybeCommitted{ 0 };
	Promise<Void> result;
	Standalone<VectorRef<ConfigMutationRef>> mutations;
	ConfigCommitAnnotation annotation;

	ConfigTransactionCommitRequest getCommitRequest(ConfigGeneration generation,
	                                                CoordinatorsHash coordinatorsHash) const {
		return ConfigTransactionCommitRequest(coordinatorsHash, generation, mutations, annotation);
	}

	void updateResult() {
		if (successful >= ctis.size() / 2 + 1 && result.canBeSet()) {
			result.send(Void());
		} else if (failed >= ctis.size() / 2 + 1 && result.canBeSet()) {
			// Rollforwards could cause a version that didn't have quorum to
			// commit, so send commit_unknown_result instead of commit_failed.

			// Calling sendError could delete this
			auto local = this->result;
			local.sendError(commit_unknown_result());
		} else {
			// Check if it is possible to ever receive quorum agreement
			auto totalRequestsOutstanding = ctis.size() - (failed + successful + maybeCommitted);
			if ((failed + totalRequestsOutstanding < ctis.size() / 2 + 1) &&
			    (successful + totalRequestsOutstanding < ctis.size() / 2 + 1) && result.canBeSet()) {
				// Calling sendError could delete this
				auto local = this->result;
				local.sendError(commit_unknown_result());
			}
		}
	}

	ACTOR static Future<Void> addRequestActor(CommitQuorum* self,
	                                          ConfigGeneration generation,
	                                          CoordinatorsHash coordinatorsHash,
	                                          ConfigTransactionInterface cti) {
		try {
			if (cti.hostname.present()) {
				wait(timeoutError(retryGetReplyFromHostname(self->getCommitRequest(generation, coordinatorsHash),
				                                            cti.hostname.get(),
				                                            WLTOKEN_CONFIGTXN_COMMIT),
				                  CLIENT_KNOBS->COMMIT_QUORUM_TIMEOUT));
			} else {
				wait(timeoutError(cti.commit.getReply(self->getCommitRequest(generation, coordinatorsHash)),
				                  CLIENT_KNOBS->COMMIT_QUORUM_TIMEOUT));
			}
			++self->successful;
		} catch (Error& e) {
			// self might be destroyed if this actor is cancelled
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}

			if (e.code() == error_code_not_committed || e.code() == error_code_timed_out) {
				++self->failed;
			} else {
				++self->maybeCommitted;
			}
		}
		self->updateResult();
		return Void();
	}

public:
	CommitQuorum() = default;
	explicit CommitQuorum(std::vector<ConfigTransactionInterface> const& ctis) : ctis(ctis) {}
	void set(KeyRef key, ValueRef value) {
		if (key == configTransactionDescriptionKey) {
			annotation.description = ValueRef(annotation.arena(), value);
		} else {
			mutations.push_back_deep(mutations.arena(),
			                         IKnobCollection::createSetMutation(mutations.arena(), key, value));
		}
	}
	void clear(KeyRef key) {
		if (key == configTransactionDescriptionKey) {
			annotation.description = ""_sr;
		} else {
			mutations.push_back_deep(mutations.arena(), IKnobCollection::createClearMutation(mutations.arena(), key));
		}
	}
	void setTimestamp() { annotation.timestamp = now(); }
	size_t expectedSize() const { return annotation.expectedSize() + mutations.expectedSize(); }
	Future<Void> commit(ConfigGeneration generation, CoordinatorsHash coordinatorsHash) {
		// Send commit message to all replicas, even those that did not return the used replica.
		// This way, slow replicas are kept up date.
		for (const auto& cti : ctis) {
			actors.add(addRequestActor(this, generation, coordinatorsHash, cti));
		}
		return result.getFuture();
	}
	bool committed() const { return result.isSet() && !result.isError(); }
};

class GetGenerationQuorum {
	ActorCollection actors{ false };
	CoordinatorsHash coordinatorsHash{ 0 };
	std::vector<ConfigTransactionInterface> ctis;
	std::map<ConfigGeneration, std::vector<ConfigTransactionInterface>> seenGenerations;
	Promise<ConfigGeneration> result;
	size_t totalRepliesReceived{ 0 };
	size_t maxAgreement{ 0 };
	Future<Void> coordinatorsChangedFuture;
	Optional<Version> lastSeenLiveVersion;
	Future<ConfigGeneration> getGenerationFuture;

	ACTOR static Future<Void> addRequestActor(GetGenerationQuorum* self, ConfigTransactionInterface cti) {
		loop {
			try {
				state ConfigTransactionGetGenerationReply reply;
				if (cti.hostname.present()) {
					wait(timeoutError(store(reply,
					                        retryGetReplyFromHostname(
					                            ConfigTransactionGetGenerationRequest{ self->coordinatorsHash,
					                                                                   self->lastSeenLiveVersion },
					                            cti.hostname.get(),
					                            WLTOKEN_CONFIGTXN_GETGENERATION)),
					                  CLIENT_KNOBS->GET_GENERATION_QUORUM_TIMEOUT));
				} else {
					wait(timeoutError(store(reply,
					                        cti.getGeneration.getReply(ConfigTransactionGetGenerationRequest{
					                            self->coordinatorsHash, self->lastSeenLiveVersion })),
					                  CLIENT_KNOBS->GET_GENERATION_QUORUM_TIMEOUT));
				}

				++self->totalRepliesReceived;
				auto gen = reply.generation;
				self->lastSeenLiveVersion =
				    std::max(gen.liveVersion, self->lastSeenLiveVersion.orDefault(::invalidVersion));
				auto& replicas = self->seenGenerations[gen];
				replicas.push_back(cti);
				self->maxAgreement = std::max(replicas.size(), self->maxAgreement);
				// TraceEvent("ConfigTransactionGotGenerationReply")
				// 		.detail("From", cti.getGeneration.getEndpoint().getPrimaryAddress())
				// 		.detail("TotalRepliesReceived", self->totalRepliesReceived)
				// 		.detail("ReplyGeneration", gen.toString())
				// 		.detail("Replicas", replicas.size())
				// 		.detail("Coordinators", self->ctis.size())
				// 		.detail("MaxAgreement", self->maxAgreement)
				// 		.detail("LastSeenLiveVersion", self->lastSeenLiveVersion);
				if (replicas.size() >= self->ctis.size() / 2 + 1 && !self->result.isSet()) {
					self->result.send(gen);
				} else if (self->maxAgreement + (self->ctis.size() - self->totalRepliesReceived) <
				           (self->ctis.size() / 2 + 1)) {
					if (!self->result.isError()) {
						// Calling sendError could delete self
						auto local = self->result;
						local.sendError(failed_to_reach_quorum());
					}
				}
				break;
			} catch (Error& e) {
				if (e.code() == error_code_broken_promise) {
					continue;
				} else if (e.code() == error_code_timed_out) {
					++self->totalRepliesReceived;
					if (self->totalRepliesReceived == self->ctis.size() && self->result.canBeSet() &&
					    !self->result.isError()) {
						// Calling sendError could delete self
						auto local = self->result;
						local.sendError(failed_to_reach_quorum());
					}
					break;
				} else {
					throw;
				}
			}
		}
		return Void();
	}

	ACTOR static Future<ConfigGeneration> getGenerationActor(GetGenerationQuorum* self) {
		state int retries = 0;
		loop {
			for (const auto& cti : self->ctis) {
				self->actors.add(addRequestActor(self, cti));
			}
			try {
				choose {
					when(ConfigGeneration generation = wait(self->result.getFuture())) {
						return generation;
					}
					when(wait(self->actors.getResult())) {
						ASSERT(false);
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_failed_to_reach_quorum) {
					CODE_PROBE(true, "Failed to reach quorum getting generation");
					if (self->coordinatorsChangedFuture.isReady()) {
						throw coordinators_changed();
					}
					if (deterministicRandom()->random01() < 0.95) {
						// Add some random jitter to prevent clients from
						// contending.
						wait(delayJittered(std::clamp(
						    0.006 * (1 << std::min(retries, 30)), 0.0, CLIENT_KNOBS->TIMEOUT_RETRY_UPPER_BOUND)));
					}
					if (deterministicRandom()->random01() < 0.05) {
						// Randomly inject a delay of at least the generation
						// reply timeout, to try to prevent contention between
						// clients.
						wait(delay(CLIENT_KNOBS->GET_GENERATION_QUORUM_TIMEOUT *
						           (deterministicRandom()->random01() + 1.0)));
					}
					++retries;
					self->actors.clear(false);
					self->seenGenerations.clear();
					self->result.reset();
					self->totalRepliesReceived = 0;
					self->maxAgreement = 0;
				} else {
					throw e;
				}
			}
		}
	}

public:
	GetGenerationQuorum() = default;
	explicit GetGenerationQuorum(CoordinatorsHash coordinatorsHash,
	                             std::vector<ConfigTransactionInterface> const& ctis,
	                             Future<Void> coordinatorsChangedFuture,
	                             Optional<Version> const& lastSeenLiveVersion = {})
	  : coordinatorsHash(coordinatorsHash), ctis(ctis), coordinatorsChangedFuture(coordinatorsChangedFuture),
	    lastSeenLiveVersion(lastSeenLiveVersion) {}
	Future<ConfigGeneration> getGeneration() {
		if (!getGenerationFuture.isValid()) {
			getGenerationFuture = getGenerationActor(this);
		}
		return getGenerationFuture;
	}
	bool isReady() const {
		return getGenerationFuture.isValid() && getGenerationFuture.isReady() && !getGenerationFuture.isError();
	}
	Optional<ConfigGeneration> getCachedGeneration() const {
		return isReady() ? getGenerationFuture.get() : Optional<ConfigGeneration>{};
	}
	std::vector<ConfigTransactionInterface> getReadReplicas() const {
		ASSERT(isReady());
		return seenGenerations.at(getGenerationFuture.get());
	}
	Optional<Version> getLastSeenLiveVersion() const { return lastSeenLiveVersion; }
};

class PaxosConfigTransactionImpl {
	CoordinatorsHash coordinatorsHash{ 0 };
	std::vector<ConfigTransactionInterface> ctis;
	GetGenerationQuorum getGenerationQuorum;
	CommitQuorum commitQuorum;
	int numRetries{ 0 };
	Optional<UID> dID;
	Database cx;
	Future<Void> watchClusterFileFuture;

	ACTOR static Future<Optional<Value>> get(PaxosConfigTransactionImpl* self, Key key) {
		state ConfigKey configKey = ConfigKey::decodeKey(key);
		loop {
			try {
				state ConfigGeneration generation = wait(self->getGenerationQuorum.getGeneration());
				state std::vector<ConfigTransactionInterface> readReplicas =
				    self->getGenerationQuorum.getReadReplicas();
				std::vector<Future<Void>> fs;
				for (ConfigTransactionInterface& readReplica : readReplicas) {
					if (readReplica.hostname.present()) {
						fs.push_back(tryInitializeRequestStream(
						    &readReplica.get, readReplica.hostname.get(), WLTOKEN_CONFIGTXN_GET));
					}
				}
				wait(waitForAll(fs));
				state Reference<ConfigTransactionInfo> configNodes(new ConfigTransactionInfo(readReplicas));
				ConfigTransactionGetReply reply = wait(timeoutError(
				    basicLoadBalance(configNodes,
				                     &ConfigTransactionInterface::get,
				                     ConfigTransactionGetRequest{ self->coordinatorsHash, generation, configKey }),
				    CLIENT_KNOBS->GET_KNOB_TIMEOUT));
				if (reply.value.present()) {
					return reply.value.get().toValue();
				} else {
					return Optional<Value>{};
				}
			} catch (Error& e) {
				if (e.code() != error_code_timed_out && e.code() != error_code_broken_promise &&
				    e.code() != error_code_coordinators_changed) {
					throw;
				}
				self->reset();
			}
		}
	}

	ACTOR static Future<RangeResult> getConfigClasses(PaxosConfigTransactionImpl* self) {
		loop {
			try {
				state ConfigGeneration generation = wait(self->getGenerationQuorum.getGeneration());
				state std::vector<ConfigTransactionInterface> readReplicas =
				    self->getGenerationQuorum.getReadReplicas();
				std::vector<Future<Void>> fs;
				for (ConfigTransactionInterface& readReplica : readReplicas) {
					if (readReplica.hostname.present()) {
						fs.push_back(tryInitializeRequestStream(
						    &readReplica.getClasses, readReplica.hostname.get(), WLTOKEN_CONFIGTXN_GETCLASSES));
					}
				}
				wait(waitForAll(fs));
				state Reference<ConfigTransactionInfo> configNodes(new ConfigTransactionInfo(readReplicas));
				ConfigTransactionGetConfigClassesReply reply = wait(
				    basicLoadBalance(configNodes,
				                     &ConfigTransactionInterface::getClasses,
				                     ConfigTransactionGetConfigClassesRequest{ self->coordinatorsHash, generation }));
				RangeResult result;
				result.reserve(result.arena(), reply.configClasses.size());
				for (const auto& configClass : reply.configClasses) {
					result.push_back_deep(result.arena(), KeyValueRef(configClass, ""_sr));
				}
				return result;
			} catch (Error& e) {
				if (e.code() != error_code_coordinators_changed) {
					throw;
				}
				self->reset();
			}
		}
	}

	ACTOR static Future<RangeResult> getKnobs(PaxosConfigTransactionImpl* self, Optional<Key> configClass) {
		loop {
			try {
				state ConfigGeneration generation = wait(self->getGenerationQuorum.getGeneration());
				state std::vector<ConfigTransactionInterface> readReplicas =
				    self->getGenerationQuorum.getReadReplicas();
				std::vector<Future<Void>> fs;
				for (ConfigTransactionInterface& readReplica : readReplicas) {
					if (readReplica.hostname.present()) {
						fs.push_back(tryInitializeRequestStream(
						    &readReplica.getKnobs, readReplica.hostname.get(), WLTOKEN_CONFIGTXN_GETKNOBS));
					}
				}
				wait(waitForAll(fs));
				state Reference<ConfigTransactionInfo> configNodes(new ConfigTransactionInfo(readReplicas));
				ConfigTransactionGetKnobsReply reply = wait(basicLoadBalance(
				    configNodes,
				    &ConfigTransactionInterface::getKnobs,
				    ConfigTransactionGetKnobsRequest{ self->coordinatorsHash, generation, configClass }));
				RangeResult result;
				result.reserve(result.arena(), reply.knobNames.size());
				for (const auto& knobName : reply.knobNames) {
					result.push_back_deep(result.arena(), KeyValueRef(knobName, ""_sr));
				}
				return result;
			} catch (Error& e) {
				if (e.code() != error_code_coordinators_changed) {
					throw;
				}
				self->reset();
			}
		}
	}

	ACTOR static Future<Void> commit(PaxosConfigTransactionImpl* self) {
		loop {
			try {
				ConfigGeneration generation = wait(self->getGenerationQuorum.getGeneration());
				self->commitQuorum.setTimestamp();
				wait(self->commitQuorum.commit(generation, self->coordinatorsHash));
				return Void();
			} catch (Error& e) {
				if (e.code() != error_code_coordinators_changed) {
					throw;
				}
				self->reset();
			}
		}
	}

	ACTOR static Future<Void> onError(PaxosConfigTransactionImpl* self, Error e) {
		// TODO: Improve this:
		TraceEvent("ConfigIncrementOnError").error(e).detail("NumRetries", self->numRetries);
		if (e.code() == error_code_transaction_too_old || e.code() == error_code_not_committed) {
			wait(delay(std::clamp((1 << self->numRetries++) * 0.01 * deterministicRandom()->random01(),
			                      0.0,
			                      CLIENT_KNOBS->TIMEOUT_RETRY_UPPER_BOUND)));
			self->reset();
			return Void();
		}
		throw e;
	}

	// Returns when the cluster interface updates with a new connection string.
	ACTOR static Future<Void> watchClusterFile(Database cx) {
		state Future<Void> leaderMonitor =
		    monitorLeader<ClusterInterface>(cx->getConnectionRecord(), cx->statusClusterInterface);
		state std::string connectionString = cx->getConnectionRecord()->getConnectionString().toString();

		loop {
			wait(cx->statusClusterInterface->onChange());
			if (cx->getConnectionRecord()->getConnectionString().toString() != connectionString) {
				return Void();
			}
		}
	}

public:
	Future<Version> getReadVersion() {
		return map(getGenerationQuorum.getGeneration(), [](auto const& gen) { return gen.committedVersion; });
	}

	Optional<Version> getCachedReadVersion() const {
		auto gen = getGenerationQuorum.getCachedGeneration();
		if (gen.present()) {
			return gen.get().committedVersion;
		} else {
			return {};
		}
	}

	Version getCommittedVersion() const {
		return commitQuorum.committed() ? getGenerationQuorum.getCachedGeneration().get().liveVersion
		                                : ::invalidVersion;
	}

	int64_t getApproximateSize() const { return commitQuorum.expectedSize(); }

	void set(KeyRef key, ValueRef value) { commitQuorum.set(key, value); }

	void clear(KeyRef key) { commitQuorum.clear(key); }

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

	Future<Void> onError(Error const& e) { return onError(this, e); }

	void debugTransaction(UID dID) { this->dID = dID; }

	void reset() {
		ctis.clear();
		// Re-read connection string. If the cluster file changed, this will
		// return the updated value.
		const ClusterConnectionString& cs = cx->getConnectionRecord()->getConnectionString();
		ctis.reserve(cs.hostnames.size() + cs.coords.size());
		for (const auto& h : cs.hostnames) {
			ctis.emplace_back(h);
		}
		for (const auto& c : cs.coords) {
			ctis.emplace_back(c);
		}
		coordinatorsHash = std::hash<std::string>()(cx->getConnectionRecord()->getConnectionString().toString());
		if (!cx->statusLeaderMon.isValid() || cx->statusLeaderMon.isReady()) {
			cx->statusClusterInterface = makeReference<AsyncVar<Optional<ClusterInterface>>>();
			cx->statusLeaderMon = watchClusterFile(cx);
		}
		getGenerationQuorum = GetGenerationQuorum{
			coordinatorsHash, ctis, cx->statusLeaderMon, getGenerationQuorum.getLastSeenLiveVersion()
		};
		commitQuorum = CommitQuorum{ ctis };
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

	PaxosConfigTransactionImpl(Database const& cx) : cx(cx) { reset(); }

	PaxosConfigTransactionImpl(std::vector<ConfigTransactionInterface> const& ctis)
	  : ctis(ctis), getGenerationQuorum(0, ctis, Future<Void>()), commitQuorum(ctis) {}
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

int64_t PaxosConfigTransaction::getTotalCost() const {
	return 0;
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

void PaxosConfigTransaction::construct(Database const& cx) {
	impl = PImpl<PaxosConfigTransactionImpl>::create(cx);
}
