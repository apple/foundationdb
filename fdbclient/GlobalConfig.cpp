/*
 * GlobalConfig.cpp
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/GlobalConfig.h"
#include "fdbclient/SpecialKeySpace.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tuple.h"
#include "fdbrpc/FailureMonitor.h"
#include "flow/Error.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

const KeyRef fdbClientInfoTxnSampleRate = "config/fdb_client_info/client_txn_sample_rate"_sr;
const KeyRef fdbClientInfoTxnSizeLimit = "config/fdb_client_info/client_txn_size_limit"_sr;

const KeyRef transactionTagSampleRate = "config/transaction_tag_sample_rate"_sr;
const KeyRef transactionTagSampleCost = "config/transaction_tag_sample_cost"_sr;

const KeyRef samplingFrequency = "visibility/sampling/frequency"_sr;
const KeyRef samplingWindow = "visibility/sampling/window"_sr;

GlobalConfig::GlobalConfig(DatabaseContext* cx) : cx(cx), lastUpdate(0) {}

void GlobalConfig::applyChanges(Transaction& tr,
                                const VectorRef<KeyValueRef>& insertions,
                                const VectorRef<KeyRangeRef>& clears) {
	VersionHistory vh{ 0 };
	for (const auto& kv : insertions) {
		vh.mutations.emplace_back_deep(vh.mutations.arena(), MutationRef(MutationRef::SetValue, kv.key, kv.value));
		tr.set(kv.key.withPrefix(globalConfigKeysPrefix), kv.value);
	}
	for (const auto& range : clears) {
		vh.mutations.emplace_back_deep(vh.mutations.arena(),
		                               MutationRef(MutationRef::ClearRange, range.begin, range.end));
		tr.clear(
		    KeyRangeRef(range.begin.withPrefix(globalConfigKeysPrefix), range.end.withPrefix(globalConfigKeysPrefix)));
	}

	// Record the mutations in this commit into the global configuration history.
	Key historyKey = addVersionStampAtEnd(globalConfigHistoryPrefix);
	ObjectWriter historyWriter(IncludeVersion());
	historyWriter.serialize(vh);
	tr.atomicOp(historyKey, historyWriter.toStringRef(), MutationRef::SetVersionstampedKey);

	// Write version key to trigger update in cluster controller.
	tr.atomicOp(globalConfigVersionKey,
	            "0123456789\x00\x00\x00\x00"_sr, // versionstamp
	            MutationRef::SetVersionstampedValue);
}

Key GlobalConfig::prefixedKey(KeyRef key) {
	return key.withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::GLOBALCONFIG).begin);
}

Reference<ConfigValue> GlobalConfig::get(KeyRef name) {
	auto it = data.find(name);
	if (it == data.end()) {
		return Reference<ConfigValue>();
	}
	return it->second;
}

std::map<KeyRef, Reference<ConfigValue>> GlobalConfig::get(KeyRangeRef range) {
	std::map<KeyRef, Reference<ConfigValue>> results;
	for (const auto& [key, value] : data) {
		if (range.contains(key)) {
			results[key] = value;
		}
	}
	return results;
}

Future<Void> GlobalConfig::onInitialized() {
	return initialized.getFuture();
}

Future<Void> GlobalConfig::onChange() {
	return configChanged.onTrigger();
}

void GlobalConfig::trigger(KeyRef key, std::function<void(std::optional<std::any>)> fn) {
	callbacks.emplace(key, std::move(fn));
}

void GlobalConfig::insert(KeyRef key, ValueRef value) {
	// TraceEvent(SevInfo, "GlobalConfigInsert").detail("Key", key).detail("Value", value);
	data.erase(key);

	Arena arena(key.expectedSize() + value.expectedSize());
	KeyRef stableKey = KeyRef(arena, key);
	try {
		std::any any;
		Tuple t = Tuple::unpack(value);
		if (t.getType(0) == Tuple::ElementType::UTF8) {
			any = StringRef(arena, t.getString(0).contents());
		} else if (t.getType(0) == Tuple::ElementType::INT) {
			any = t.getInt(0);
		} else if (t.getType(0) == Tuple::ElementType::BOOL) {
			any = t.getBool(0);
		} else if (t.getType(0) == Tuple::ElementType::FLOAT) {
			any = t.getFloat(0);
		} else if (t.getType(0) == Tuple::ElementType::DOUBLE) {
			any = t.getDouble(0);
		} else if (t.getType(0) == Tuple::ElementType::VERSIONSTAMP) {
			any = t.getVersionstamp(0);
		} else {
			ASSERT(false);
		}
		data[stableKey] = makeReference<ConfigValue>(std::move(arena), std::move(any));

		if (callbacks.find(stableKey) != callbacks.end()) {
			callbacks[stableKey](data[stableKey]->value);
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "GlobalConfigTupleParseError").detail("What", e.what());
	}
}

void GlobalConfig::erase(Key key) {
	erase(KeyRangeRef(key, keyAfter(key)));
}

void GlobalConfig::erase(KeyRangeRef range) {
	// TraceEvent(SevInfo, "GlobalConfigErase").detail("Range", range);
	auto it = data.begin();
	while (it != data.end()) {
		if (range.contains(it->first)) {
			if (callbacks.find(it->first) != callbacks.end()) {
				callbacks[it->first](std::nullopt);
			}
			it = data.erase(it);
		} else {
			++it;
		}
	}
}

Future<Version> GlobalConfig::refresh(Version lastKnown, Version largestSeen) {
	// TraceEvent trace(SevInfo, "GlobalConfigRefresh");
	erase(KeyRangeRef(""_sr, "\xff"_sr));

	Backoff backoff(CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_BACKOFF, CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_MAX_BACKOFF);
	while (true) {
		Error err;
		try {
			GlobalConfigRefreshReply reply =
			    co_await timeoutError(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
			                                           &GrvProxyInterface::refreshGlobalConfig,
			                                           GlobalConfigRefreshRequest{ lastKnown }),
			                          CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_TIMEOUT);
			for (const auto& kv : reply.result) {
				KeyRef systemKey = kv.key.removePrefix(globalConfigKeysPrefix);
				insert(systemKey, kv.value);
			}
			if (reply.version >= largestSeen || largestSeen == std::numeric_limits<Version>::max()) {
				co_return reply.version;
			}
			co_await delay(0.25);
			continue;
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_actor_cancelled) {
			throw err;
		}
		co_await backoff.onError();
	}
}

// Applies updates to the local copy of the global configuration when this
// process receives an updated history.
Future<Void> GlobalConfig::updater(const ClientDBInfo* dbInfo) {
	while (true) {
		Error err;
		try {
			if (initialized.canBeSet()) {
				co_await cx->onConnected();

				lastUpdate = co_await refresh(-1, 0);

				cx->addref();
				initialized.send(Void());
				cx->delref();
			}

			while (true) {
				// run one iteration at the beginning
				co_await delay(0);
				// Subscribe before processing history so a reentrant DB info update cannot be lost.
				Future<Void> nextDbInfoChange = dbInfoChanged.onTrigger();
				if (!dbInfo->history.empty()) {
					if (lastUpdate < dbInfo->history[0].version) {
						// This process missed too many global configuration
						// history updates or the protocol version changed, so it
						// must re-read the entire configuration range.
						lastUpdate = co_await refresh(lastUpdate, dbInfo->history.back().version);
						// DBInfo could have changed after the wait. If
						// changes are present, re-run the loop to make
						// sure they are applied.
						if (!dbInfo->history.empty() &&
						    dbInfo->history[0].version != std::numeric_limits<Version>::max()) {
							continue;
						}
					} else {
						// Apply history in order, from lowest version to highest
						// version. Mutation history should already be stored in
						// ascending version order.
						for (const auto& vh : dbInfo->history) {
							if (vh.version <= lastUpdate) {
								continue; // already applied this mutation
							}

							for (const auto& mutation : vh.mutations.contents()) {
								if (mutation.type == MutationRef::SetValue) {
									insert(mutation.param1, mutation.param2);
								} else if (mutation.type == MutationRef::ClearRange) {
									erase(KeyRangeRef(mutation.param1, mutation.param2));
								} else {
									UNREACHABLE();
								}
							}

							ASSERT(vh.version > lastUpdate);
							lastUpdate = vh.version;
						}
					}
					configChanged.trigger();
				}
				// In case this actor is canceled in the d'tor of GlobalConfig we can exit here.
				co_await delay(0);
				co_await nextDbInfoChange;
			}
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_actor_cancelled) {
			throw err;
		}

		// There shouldn't be any uncaught errors that fall to this point,
		// but in case there are, catch them and restart the updater.
		TraceEvent("GlobalConfigUpdaterError").error(err);
		co_await delay(1.0);
	}
}

namespace {

Future<Void> serveGlobalConfigTestRefresh(GrvProxyInterface proxy, Version version) {
	GlobalConfigRefreshRequest request = co_await proxy.refreshGlobalConfig.getFuture();
	RangeResult result;
	request.reply.send(GlobalConfigRefreshReply{ result.arena(), version, result });
	co_return;
}

} // namespace

TEST_CASE("/fdbclient/GlobalConfig/PreservesReentrantHistoryUpdate") {
	GrvProxyInterface grvProxy;
	grvProxy.provisional = false;
	grvProxy.initEndpoints();
	IFailureMonitor::failureMonitor().setStatus(grvProxy.address(), FailureStatus(false));
	ASSERT(IFailureMonitor::failureMonitor().getState(grvProxy.address()).isAvailable());

	CommitProxyInterface commitProxy;
	commitProxy.provisional = false;
	commitProxy.initEndpoints();

	ClientDBInfo initialInfo;
	initialInfo.id = UID(1, 1);
	initialInfo.grvProxies.push_back(grvProxy);
	initialInfo.commitProxies.push_back(commitProxy);

	auto clientInfo = makeReference<AsyncVar<ClientDBInfo>>(initialInfo);
	Database db =
	    DatabaseContext::create(clientInfo, Future<Void>(Never()), LocalityData(), EnableLocalityLoadBalance::False);
	Future<Void> refresh = serveGlobalConfigTestRefresh(grvProxy, 1);
	db->globalConfig->init(Reference<AsyncVar<ClientDBInfo> const>(clientInfo), std::addressof(clientInfo->get()));

	co_await timeoutError(db->globalConfig->onInitialized(), 1.0);
	co_await timeoutError(refresh, 1.0);

	VersionHistory sentinel(1);
	VersionHistory enabled(2);
	enabled.mutations.emplace_back_deep(
	    enabled.mutations.arena(),
	    MutationRef(MutationRef::SetValue, fdbClientInfoTxnSampleRate, Tuple::makeTuple(0.1).pack()));

	VersionHistory disabled(3);
	disabled.mutations.emplace_back_deep(
	    disabled.mutations.arena(),
	    MutationRef(MutationRef::SetValue, fdbClientInfoTxnSampleRate, Tuple::makeTuple(0.0).pack()));

	ClientDBInfo enabledInfo = clientInfo->get();
	enabledInfo.id = UID(1, 2);
	enabledInfo.history.push_back(sentinel);
	enabledInfo.history.push_back(enabled);

	ClientDBInfo disabledInfo = enabledInfo;
	disabledInfo.id = UID(1, 3);
	disabledInfo.history.push_back(disabled);

	Future<Void> disabledApplied;
	Future<Void> deferredDisable;
	Future<Void> publishDisabled = trigger(
	    [&]() {
		    ASSERT_EQ(db->globalConfig->get<double>(fdbClientInfoTxnSampleRate, -1.0), 0.1);
		    disabledApplied = db->globalConfig->onChange();
		    deferredDisable = trigger([&]() { clientInfo->set(disabledInfo); }, delay(0));
	    },
	    db->globalConfig->onChange());

	clientInfo->set(enabledInfo);
	co_await timeoutError(publishDisabled, 1.0);
	ASSERT(deferredDisable.isValid());
	co_await timeoutError(deferredDisable, 1.0);
	ASSERT(disabledApplied.isValid());
	co_await timeoutError(disabledApplied, 1.0);
	ASSERT_EQ(db->globalConfig->get<double>(fdbClientInfoTxnSampleRate, -1.0), 0.0);
	co_return;
}
