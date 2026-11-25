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

#include <variant>

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/GlobalConfig.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SpecialKeySpace.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tuple.h"
#include "fdbrpc/FailureMonitor.h"
#include "flow/Error.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"
#include "flow/genericactors.h"

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

Optional<Versionstamp> GlobalConfig::decodeVersionstamp(ValueRef versionstamp, Version readVersion) {
	if (versionstamp.size() != sizeof(Version) + sizeof(Versionstamp::batchNumber)) {
		return {};
	}
	const auto stamp = BinaryReader::fromStringRef<Versionstamp>(versionstamp, Unversioned());
	if (stamp.version <= 0 || stamp.version > readVersion) {
		return {};
	}
	return stamp;
}

Key GlobalConfig::prefixedKey(KeyRef key) {
	return key.withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::GLOBALCONFIG).begin);
}

Reference<const ConfigValue> GlobalConfig::get(KeyRef name) {
	auto it = data.find(name);
	if (it == data.end()) {
		return Reference<const ConfigValue>();
	}
	return it->second;
}

std::map<KeyRef, Reference<const ConfigValue>> GlobalConfig::get(KeyRangeRef range) {
	std::map<KeyRef, Reference<const ConfigValue>> results;
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

GlobalConfig::Entry GlobalConfig::decode(KeyRef key, ValueRef value) {
	try {
		Tuple tuple = Tuple::unpack(value);
		// getString accepts end-of-input as its terminator. Fixed-width accessors need a complete, bounded element.
		if (tuple.getType(0) != Tuple::ElementType::UTF8) {
			tuple = Tuple::unpack(tuple.subTupleRawString(0), /* exclude_incomplete */ true);
		}
		Arena arena(key.expectedSize() + value.expectedSize());
		KeyRef stableKey(arena, key);
		std::any any;
		switch (tuple.getType(0)) {
		case Tuple::ElementType::UTF8:
			any = StringRef(arena, tuple.getString(0).contents());
			break;
		case Tuple::ElementType::INT:
			any = tuple.getInt(0);
			break;
		case Tuple::ElementType::BOOL:
			any = tuple.getBool(0);
			break;
		case Tuple::ElementType::FLOAT:
			any = tuple.getFloat(0);
			break;
		case Tuple::ElementType::DOUBLE:
			any = tuple.getDouble(0);
			break;
		case Tuple::ElementType::VERSIONSTAMP:
			any = tuple.getVersionstamp(0);
			break;
		default:
			throw invalid_tuple_data_type();
		}
		return { stableKey, makeReference<ConfigValue>(std::move(arena), std::move(any)) };
	} catch (Error& e) {
		if (e.code() != error_code_invalid_tuple_data_type && e.code() != error_code_invalid_tuple_index) {
			throw;
		}
		TraceEvent(SevWarn, "GlobalConfigTupleParseError").detail("Key", key).detail("What", e.what());
		return {};
	}
}

void GlobalConfig::notify(KeyRef key, const Reference<const ConfigValue>& value) {
	if (auto callback = callbacks.find(key); callback != callbacks.end()) {
		callback->second(value.isValid() ? std::optional(value->getValue()) : std::nullopt);
	}
}

bool GlobalConfig::applyHistory(const std::vector<VersionHistory>& history) {
	const Version latest = history.back().version;
	if (lastUpdate < history.front().version) {
		return false;
	}
	if (latest <= lastUpdate) {
		return true;
	}
	// A later unreadable value must not expose earlier mutations through the cache or callbacks.
	std::vector<std::variant<Entry, KeyRange>> changes;
	for (const auto& vh : history) {
		if (vh.version <= lastUpdate) {
			continue;
		}
		for (const auto& mutation : vh.mutations) {
			if (!mutation.corrupted && mutation.type == MutationRef::SetValue) {
				auto entry = decode(mutation.param1, mutation.param2);
				if (!entry.second.isValid()) {
					return false;
				}
				changes.emplace_back(std::move(entry));
			} else if (!mutation.corrupted && mutation.type == MutationRef::ClearRange &&
			           mutation.param1 <= mutation.param2) {
				changes.emplace_back(KeyRange(KeyRangeRef(mutation.param1, mutation.param2), vh.mutations.arena()));
			} else {
				return false;
			}
		}
	}
	for (const auto& change : changes) {
		if (const auto* entry = std::get_if<Entry>(&change)) {
			// Replacing only the value would leave the map key pointing into the old arena.
			data.erase(entry->first);
			data.insert(*entry);
			notify(entry->first, entry->second);
		} else {
			const auto& range = std::get<KeyRange>(change);
			std::erase_if(data, [&](const auto& kv) {
				if (!range.contains(kv.first)) {
					return false;
				}
				notify(kv.first);
				return true;
			});
		}
	}
	lastUpdate = latest;
	return true;
}

bool GlobalConfig::snapshotMatchesFinalMutations(const RangeResultRef& rows, const VectorRef<MutationRef>& mutations) {
	// A missing intermediate row could have changed any earlier set or clear.
	KeyRangeMap<const MutationRef*> effects(nullptr, globalConfigDataKeys.end);
	for (const auto& mutation : mutations) {
		auto begin = mutation.param1.withPrefix(globalConfigKeysPrefix);
		if (!mutation.corrupted && mutation.type == MutationRef::SetValue) {
			effects.insert(begin, &mutation);
		} else if (!mutation.corrupted && mutation.type == MutationRef::ClearRange &&
		           mutation.param1 <= mutation.param2) {
			effects.insert(KeyRangeRef(begin, mutation.param2.withPrefix(globalConfigKeysPrefix)), &mutation);
		} else {
			// A legacy controller can forward an unusable mutation. Only subsequent mutations are evidence.
			effects.insert(globalConfigDataKeys, nullptr);
		}
	}
	for (auto range : effects.ranges()) {
		const auto* mutation = range.value();
		if (!mutation) {
			continue;
		}
		auto row = std::lower_bound(rows.begin(), rows.end(), range.begin(), KeyValueRef::OrderByKey());
		const bool present = row != rows.end() && row->key < range.end();
		if (mutation->type == MutationRef::SetValue) {
			// An unreadable history value leaves this key to the snapshot.
			if ((!present || row->value != mutation->param2) &&
			    decode(mutation->param1, mutation->param2).second.isValid()) {
				return false;
			}
		} else if (present) {
			return false;
		}
	}
	return true;
}

struct GlobalConfig::RefreshState {
	// A singleton can reuse a label, even one for which we already replayed a row.
	Version marker = invalidVersion;
	Optional<GlobalConfigSnapshotProof> proof;
	// Keep the repair read bound until a complete snapshot establishes a replay point.
	Version repairReadFloor = invalidVersion;
	uint64_t observation = 0;
	bool markerPending = false;
	bool atMarker = false;
	bool canReplay = false;
	Optional<Version> pendingReadVersion;

	void observe(const ClientDBInfo& info) {
		if (info.history.size() == 1) {
			auto version = info.history.front().version;
			if (!atMarker || version != marker || proof != info.globalConfigSnapshot) {
				++observation;
				markerPending = true;
				marker = version;
				proof = info.globalConfigSnapshot;
				if (proof.present()) {
					repairReadFloor = std::max(repairReadFloor, proof.get().readVersion);
				}
			}
		}
		atMarker = info.history.size() == 1;
	}

	Future<Void> watch(const ClientDBInfo* info, AsyncTrigger& changed) {
		while (true) {
			observe(*info);
			co_await changed.onTrigger();
		}
	}
};

Future<GlobalConfig::ConfigData> GlobalConfig::getSnapshot(const ClientDBInfo* dbInfo,
                                                           Version lastKnown,
                                                           RefreshState& state) {
	const auto& history = dbInfo->history;
	auto publishedVersion = [&] { return history.empty() ? 0 : history.back().version; };
	auto finalRow = [&]() -> Optional<VersionHistory> {
		if (history.size() > 1) {
			return history.back();
		}
		return {};
	};
	auto retainedRow = [&](Version version) -> const VersionHistory* {
		// Older controllers discard the batch number. The last row at the version is the final batch.
		for (size_t i = history.size(); i > 1; --i) {
			if (history[i - 1].version == version) {
				return &history[i - 1];
			}
		}
		return nullptr;
	};
	auto sameMutation = [](const MutationRef& a, const MutationRef& b) {
		return a.type == b.type && a.corrupted == b.corrupted && a.param1 == b.param1 && a.param2 == b.param2;
	};
	auto getReadVersion = [this]() -> Future<Version> {
		GetReadVersionRequest request({}, 0, TransactionPriority::IMMEDIATE, cx->ssVersionVectorCache.getMaxVersion());
		auto reply = co_await timeoutError(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
		                                                    &GrvProxyInterface::getConsistentReadVersion,
		                                                    request),
		                                   CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_TIMEOUT);
		co_return reply.version;
	};
	Version fence = publishedVersion();
	auto fenceRow = finalRow();
	uint64_t boundObservation = state.observation;
	Optional<Version> readAfterPublication;
	Backoff backoff(CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_BACKOFF, CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_MAX_BACKOFF);
	while (true) {
		Error err;
		try {
			const auto requestObservation = state.observation;
			auto response =
			    co_await errorOr(timeoutError(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
			                                                   &GrvProxyInterface::refreshGlobalConfig,
			                                                   GlobalConfigRefreshRequest{ lastKnown }),
			                                  CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_TIMEOUT));
			if (response.isError()) {
				if (response.getError().code() == error_code_future_version) {
					// A repaired stamp can be lower than lastKnown even when the rows are from a newer read.
					lastKnown = invalidVersion;
				}
				throw response.getError();
			}
			const auto& reply = response.get();
			const auto& readVersion = reply.snapshotReadVersion;
			if (reply.result.more || reply.version < 0 || reply.version == MAX_VERSION ||
			    (readVersion.present() &&
			     (reply.version <= 0 || reply.version > readVersion.get() || readVersion.get() == MAX_VERSION))) {
				TraceEvent("GlobalConfigRefreshInvalidSnapshot")
				    .suppressFor(1.0)
				    .detail("ReplyVersion", reply.version)
				    .detail("SnapshotReadVersion", readVersion)
				    .detail("More", reply.result.more);
				throw serialization_failed();
			}
			state.observe(*dbInfo);
			const Version published = publishedVersion();
			if (published < fence) {
				fence = published;
				fenceRow = finalRow();
			}
			const auto observation = state.observation;
			const bool needsMarker = state.atMarker || state.markerPending;
			if (observation != boundObservation) {
				readAfterPublication.reset();
				boundObservation = observation;
			}
			const bool advanced = published > fence;
			const bool matchesPublication =
			    reply.version >= lastUpdate && (reply.version == published || published == MAX_VERSION);
			Version snapshotVersion = readVersion.orDefault(reply.version);
			const auto* row = retainedRow(reply.version);
			const bool originalRowReplaced =
			    reply.version == fence && fenceRow.present() &&
			    (!row || !std::ranges::equal(row->mutations, fenceRow.get().mutations, sameMutation));
			const Version minimum =
			    std::max({ lastUpdate, fence == MAX_VERSION ? Version(0) : fence, state.repairReadFloor });
			bool accept;
			if (readVersion.present()) {
				accept = snapshotVersion >= minimum;
				if (accept && !(needsMarker && state.proof.present()) &&
				    (needsMarker || (!advanced && (!matchesPublication || originalRowReplaced)))) {
					// A repaired label may predate the repair. Keep the read bound when ordinary writes follow it.
					if (!readAfterPublication.present()) {
						const Version bound = co_await getReadVersion();
						state.observe(*dbInfo);
						if (state.observation != observation) {
							continue;
						}
						readAfterPublication = bound;
						if (needsMarker) {
							state.repairReadFloor = std::max(state.repairReadFloor, bound);
						}
						if (publishedVersion() != published) {
							continue;
						}
					}
					accept = snapshotVersion >= readAfterPublication.get();
				}
			} else if (state.repairReadFloor != invalidVersion ||
			           (matchesPublication && dbInfo->globalConfigSnapshot.present())) {
				// A later history row says nothing about settings omitted from that row.
				RangeDigest digest;
				for (const auto& kv : reply.result) {
					digest.addKeyValue(kv.key, kv.value);
				}
				auto provenVersion = [&](const Optional<GlobalConfigSnapshotProof>& proof) {
					return proof.present() && proof.get().digest == digest ? proof.get().readVersion : invalidVersion;
				};
				snapshotVersion = std::max(provenVersion(state.proof), provenVersion(dbInfo->globalConfigSnapshot));
				accept = snapshotVersion >= minimum &&
				         (!matchesPublication || state.atMarker || snapshotVersion >= published);
			} else if (row) {
				accept = reply.version >= std::max(lastUpdate, fence) && !originalRowReplaced &&
				         (!needsMarker || matchesPublication) &&
				         snapshotMatchesFinalMutations(reply.result, row->mutations);
			} else {
				// Older peers send only a label. A request made after observing a bare marker retains
				// legacy initialization semantics when neither peer can attest to the snapshot.
				accept = matchesPublication && ((!needsMarker && history.empty()) ||
				                                (state.atMarker && state.observation == requestObservation));
			}
			if (!accept || (state.pendingReadVersion.present() && snapshotVersion < state.pendingReadVersion.get())) {
				TraceEvent("GlobalConfigRefreshVersionMismatch")
				    .suppressFor(1.0)
				    .detail("ReplayFloor", lastUpdate)
				    .detail("RefreshFence", fence)
				    .detail("PublishedVersion", published)
				    .detail("ReplyVersion", reply.version);
				co_await delay(0.25);
				continue;
			}
			// On a protocol mismatch an old controller can publish a single version from the same damaged
			// key as the proxy. An independent read version can rule out an impossible stamp.
			if (!readVersion.present() && history.size() == 1 && !state.proof.present() &&
			    reply.version > std::max(lastUpdate, cx->getCachedReadVersion())) {
				const Version grv = co_await getReadVersion();
				if (reply.version > grv) {
					TraceEvent("GlobalConfigRefreshFutureSnapshot")
					    .suppressFor(1.0)
					    .detail("ReplyVersion", reply.version)
					    .detail("ReadVersion", grv);
					throw future_version();
				}
				state.observe(*dbInfo);
				if (state.observation != observation || !state.atMarker || publishedVersion() != published) {
					continue;
				}
			}
			ConfigData snapshot;
			for (const auto& kv : reply.result) {
				auto entry = decode(kv.key.removePrefix(globalConfigKeysPrefix), kv.value);
				if (entry.second.isValid()) {
					snapshot.insert(std::move(entry));
				}
			}
			state.pendingReadVersion = !state.atMarker && !matchesPublication && advanced
			                               ? Optional<Version>(snapshotVersion)
			                               : Optional<Version>();
			state.markerPending = false;
			state.canReplay = matchesPublication && !(state.atMarker && reply.version > lastUpdate);
			if (state.canReplay) {
				lastUpdate = reply.version;
				state.repairReadFloor = invalidVersion;
			}
			co_return snapshot;
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
		RefreshState state;
		Error err;
		try {
			co_await cx->onConnected();
			Future<Void> observations = state.watch(dbInfo, dbInfoChanged);
			while (true) {
				co_await delay(0);
				// Subscribe before processing history so a reentrant DB info update cannot be lost.
				Future<Void> nextDbInfoChange = dbInfoChanged.onTrigger();
				const bool initializing = initialized.canBeSet();
				const auto& history = dbInfo->history;
				state.observe(*dbInfo);
				if (initializing || state.pendingReadVersion.present() || state.markerPending ||
				    (state.atMarker ? state.marker == MAX_VERSION
				                    : !history.empty() && (!state.canReplay || !applyHistory(history)))) {
					auto snapshot = co_await getSnapshot(dbInfo, initializing ? invalidVersion : lastUpdate, state);
					nextDbInfoChange = dbInfoChanged.onTrigger();
					data.swap(snapshot);
					for (const auto& [key, _] : snapshot) {
						notify(key);
					}
					for (const auto& [key, value] : data) {
						notify(key, value);
					}
				}
				if (initializing) {
					cx->addref();
					initialized.send(Void());
					cx->delref();
					co_await delay(0);
				}
				if (nextDbInfoChange.isReady()) {
					continue;
				}
				if (!dbInfo->history.empty()) {
					configChanged.trigger();
				}
				if (state.pendingReadVersion.present()) {
					continue;
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

struct GlobalConfigRefreshTest {
	GrvProxyInterface proxy;
	Reference<AsyncVar<ClientDBInfo>> clientInfo;
	Database db;

	explicit GlobalConfigRefreshTest(std::vector<VersionHistory> history = {}) {
		proxy.provisional = false;
		proxy.initEndpoints();
		IFailureMonitor::failureMonitor().setStatus(proxy.address(), FailureStatus(false));
		CommitProxyInterface commitProxy;
		commitProxy.provisional = false;
		commitProxy.initEndpoints();
		ClientDBInfo info;
		info.id = UID(1, 1);
		info.history = std::move(history);
		info.grvProxies.push_back(proxy);
		info.commitProxies.push_back(commitProxy);
		clientInfo = makeReference<AsyncVar<ClientDBInfo>>(info);
		db = DatabaseContext::create(clientInfo, Never(), LocalityData(), EnableLocalityLoadBalance::False);
		db->globalConfig->init(Reference<AsyncVar<ClientDBInfo> const>(clientInfo), std::addressof(clientInfo->get()));
	}

	Future<GlobalConfigRefreshRequest> nextRequest() {
		return timeoutError(waitAndForward(proxy.refreshGlobalConfig.getFuture()), 5.0);
	}

	Future<Void> serveReadVersions(Version version) {
		while (true) {
			auto request = co_await proxy.getConsistentReadVersion.getFuture();
			GetReadVersionReply reply;
			reply.version = version;
			request.reply.send(reply);
		}
	}

	Future<Void> serveSnapshots(Version version, RangeResult rows) {
		while (true) {
			auto request = co_await proxy.refreshGlobalConfig.getFuture();
			if (request.lastKnown <= version) {
				request.reply.send(GlobalConfigRefreshReply{ rows.arena(), version, rows });
			} else {
				request.reply.sendError(future_version());
			}
		}
	}

	Future<Void> publish(std::vector<VersionHistory> history, Optional<GlobalConfigSnapshotProof> proof = {}) {
		auto changed = db->globalConfig->onChange();
		auto info = clientInfo->get();
		info.id = deterministicRandom()->randomUniqueID();
		info.history = std::move(history);
		info.globalConfigSnapshot = proof;
		clientInfo->set(info);
		return changed;
	}
};

RangeResult snapshotWithSetting(KeyRef key, bool value = true) {
	RangeResult rows;
	rows.push_back_deep(rows.arena(),
	                    KeyValueRef(key.withPrefix(globalConfigKeysPrefix), Tuple::makeTuple(value).pack()));
	return rows;
}

GlobalConfigSnapshotProof snapshotProof(Version readVersion, const RangeResult& rows) {
	RangeDigest digest;
	for (const auto& kv : rows) {
		digest.addKeyValue(kv.key, kv.value);
	}
	return { readVersion, digest };
}

std::vector<VersionHistory> publishedChange(Version base, Version version, MutationRef mutation) {
	VersionHistory change(version);
	change.mutations.emplace_back_deep(change.mutations.arena(), mutation);
	return { VersionHistory(base), std::move(change) };
}

} // namespace

TEST_CASE("/fdbclient/GlobalConfig/RejectsInvalidInitialSnapshot") {
	int callbacks = 0;
	GlobalConfigRefreshTest test;
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ++callbacks; });
	auto initialized = test.db->globalConfig->onInitialized();
	auto rows = snapshotWithSetting("A"_sr);
	auto request = co_await test.nextRequest();
	for (auto [version, more] : { std::pair<Version, bool>{ 0, true },
	                              { MAX_VERSION, false },
	                              { invalidVersion, false },
	                              { (Version(1) << 62) + 42, false } }) {
		ASSERT_EQ(request.lastKnown, -1);
		rows.more = more;
		request.reply.send(GlobalConfigRefreshReply{ rows.arena(), version, rows });
		auto retry = co_await race(test.nextRequest(), initialized);
		ASSERT_EQ(callbacks, 0);
		ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
		ASSERT_EQ(retry.index(), 0);
		request = std::get<0>(retry);
	}
	ASSERT_EQ(request.lastKnown, -1);
	RangeResult empty;
	// An absent versionstamp can yield a snapshot at zero, but never a verified one.
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 0, rows, 50 });
	request = co_await test.nextRequest();
	ASSERT_EQ(callbacks, 0);
	ASSERT(!initialized.isReady());
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 0, empty });
	co_await timeoutError(initialized, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT_EQ(callbacks, 0);
}

TEST_CASE("/fdbclient/GlobalConfig/CancelDuringSnapshotRetry") {
	int callbacks = 0;
	GlobalConfigRefreshTest test;
	// The status updater holds its own Database reference while it waits for configuration initialization.
	test.db->clientStatusUpdater.actor.cancel();
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ++callbacks; });
	auto initialized = test.db->globalConfig->onInitialized();
	auto snapshot = co_await test.nextRequest();
	auto rows = snapshotWithSetting("A"_sr);
	snapshot.reply.send(GlobalConfigRefreshReply{ rows.arena(), 41, rows });
	auto retry = co_await test.nextRequest();
	co_await delay(0);
	ASSERT(!initialized.isReady());
	ASSERT_EQ(test.db->debugGetReferenceCount(), 1);
	test.db = Database();
	ASSERT(initialized.isError());
	retry.reply.send(GlobalConfigRefreshReply{ rows.arena(), 0, rows });
	co_await delay(0);
	ASSERT_EQ(callbacks, 0);
}

TEST_CASE("/fdbclient/GlobalConfig/RejectsStaleRefreshSnapshot") {
	const KeyRef tail = "\xff"_sr;
	int callbacks = 0;
	auto enabled = Tuple::makeTuple(true).pack();
	GlobalConfigRefreshTest test(publishedChange(40, 41, MutationRef(MutationRef::SetValue, tail, enabled)));
	auto grvs = test.serveReadVersions(50);
	auto initial = co_await test.nextRequest();
	RangeResult empty;
	initial.reply.send(GlobalConfigRefreshReply{ empty.arena(), 0, empty });
	auto initialized = test.db->globalConfig->onInitialized();
	auto next = co_await race(test.nextRequest(), initialized);
	ASSERT_EQ(next.index(), 0);
	initial = std::get<0>(next);
	auto rows = snapshotWithSetting(tail);
	initial.reply.send(GlobalConfigRefreshReply{ rows.arena(), 41, rows });
	co_await timeoutError(initialized, 5.0);
	ASSERT(test.db->globalConfig->get<bool>(tail, false));
	test.db->globalConfig->trigger(tail, [&](std::optional<std::any>) { ++callbacks; });
	auto historyAt44 = publishedChange(43, 44, MutationRef(MutationRef::SetValue, "A"_sr, enabled));
	auto changed = test.publish(historyAt44);
	auto request = co_await test.nextRequest();
	auto firstPage = snapshotWithSetting("A"_sr);
	for (auto [version, more] :
	     { std::pair<Version, bool>{ 43, false }, { (Version(1) << 62) + 44, false }, { 44, true } }) {
		ASSERT_EQ(request.lastKnown, 41);
		firstPage.more = more;
		request.reply.send(GlobalConfigRefreshReply{ firstPage.arena(), version, firstPage });
		auto retry = co_await race(test.nextRequest(), changed);
		ASSERT(test.db->globalConfig->get<bool>(tail, false));
		ASSERT_EQ(callbacks, 0);
		ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
		ASSERT_EQ(retry.index(), 0);
		request = std::get<0>(retry);
	}
	ASSERT_EQ(request.lastKnown, 41);
	firstPage.more = false;
	firstPage.push_back_deep(firstPage.arena(), rows[0]);
	// An unrelated DBInfo generation must not discard an in-flight reply for the same publication.
	test.publish(historyAt44);
	co_await delay(0);
	request.reply.send(GlobalConfigRefreshReply{ firstPage.arena(), 44, firstPage });
	co_await timeoutError(changed, 5.0);
	ASSERT(test.db->globalConfig->get<bool>(tail, false));
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	ASSERT_EQ(callbacks, 2);

	changed = test.publish({ VersionHistory(45) });
	request = co_await test.nextRequest();
	ASSERT_EQ(request.lastKnown, 44);
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 46, rows });
	auto retry = co_await race(test.nextRequest(), changed);
	ASSERT_EQ(retry.index(), 0);
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	test.publish(publishedChange(45, 46, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr)));
	std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ rows.arena(), 46, rows });
	auto at46 = test.serveSnapshots(46, rows);
	co_await timeoutError(changed, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	at46.cancel();

	changed = test.publish({ VersionHistory(MAX_VERSION) });
	request = co_await test.nextRequest();
	ASSERT_EQ(request.lastKnown, 46);
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 45, empty });
	retry = co_await race(test.nextRequest(), changed);
	ASSERT(test.db->globalConfig->get<bool>(tail, false));
	ASSERT_EQ(retry.index(), 0);
	request = std::get<0>(retry);
	ASSERT_EQ(request.lastKnown, 46);
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 46, empty });
	co_await timeoutError(changed, 5.0);
	ASSERT(!test.db->globalConfig->get(tail).isValid());
}

TEST_CASE("/fdbclient/GlobalConfig/LaterReadVersionCannotValidateCachedSnapshot") {
	int callbacks = 0;
	GlobalConfigRefreshTest test({ VersionHistory(41) });
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ++callbacks; });
	auto initialized = test.db->globalConfig->onInitialized();
	auto request = co_await test.nextRequest();
	auto at41 = snapshotWithSetting("A"_sr);
	// A legacy proxy read A at 41 with a damaged stamp of 46. The database passes 46 after A is cleared.
	request.reply.send(GlobalConfigRefreshReply{ at41.arena(), 46, at41 });
	test.publish(publishedChange(41, 44, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr)));
	auto grvs = test.serveReadVersions(50);
	auto repaired = test.serveSnapshots(44, RangeResult());
	co_await timeoutError(initialized, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT_EQ(callbacks, 0);
}

TEST_CASE("/fdbclient/GlobalConfig/RejectsSnapshotContradictingPublishedClear") {
	int callbacks = 0;
	GlobalConfigRefreshTest test({ VersionHistory(41) });
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ++callbacks; });
	auto initialized = test.db->globalConfig->onInitialized();
	auto request = co_await test.nextRequest();
	test.publish(publishedChange(41, 44, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr)));
	// The legacy proxy cached A at 41 under a damaged stamp with the same commit version as the clear.
	auto stale = snapshotWithSetting("A"_sr);
	request.reply.send(GlobalConfigRefreshReply{ stale.arena(), 44, stale });
	auto retry = co_await race(test.nextRequest(), initialized);
	ASSERT_EQ(callbacks, 0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT_EQ(retry.index(), 0);
	ASSERT_EQ(std::get<0>(retry).lastKnown, invalidVersion);
	RangeResult empty;
	std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ empty.arena(), 44, empty });
	co_await timeoutError(initialized, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT_EQ(callbacks, 0);
}

TEST_CASE("/fdbclient/GlobalConfig/SnapshotMatchesFinalPublishedMutations") {
	auto enabled = Tuple::makeTuple(true).pack();
	auto disabled = Tuple::makeTuple(false).pack();
	VersionHistory at44(44);
	at44.mutations.emplace_back_deep(at44.mutations.arena(), MutationRef(MutationRef::SetValue, "B"_sr, enabled));
	at44.mutations.emplace_back_deep(at44.mutations.arena(), MutationRef(MutationRef::ClearRange, "A"_sr, "C"_sr));
	at44.mutations.emplace_back_deep(at44.mutations.arena(), MutationRef(MutationRef::ClearRange, "B"_sr, "D"_sr));
	at44.mutations.emplace_back_deep(at44.mutations.arena(), MutationRef(MutationRef::SetValue, "C"_sr, disabled));
	at44.mutations.emplace_back_deep(at44.mutations.arena(), MutationRef(MutationRef::SetValue, "C"_sr, enabled));
	int callbacks = 0;
	GlobalConfigRefreshTest test({ VersionHistory(41), at44 });
	test.db->globalConfig->trigger("C"_sr, [&](std::optional<std::any>) { ++callbacks; });
	auto initialized = test.db->globalConfig->onInitialized();
	auto request = co_await test.nextRequest();
	auto overlapsClear = snapshotWithSetting("B"_sr);
	overlapsClear.push_back_deep(overlapsClear.arena(), snapshotWithSetting("C"_sr)[0]);
	for (const auto& rows : { RangeResult(), snapshotWithSetting("C"_sr, false), overlapsClear }) {
		request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 44, rows });
		auto retry = co_await race(test.nextRequest(), initialized);
		ASSERT_EQ(retry.index(), 0);
		ASSERT_EQ(callbacks, 0);
		request = std::get<0>(retry);
	}
	auto repaired = snapshotWithSetting("C"_sr);
	repaired.push_back_deep(repaired.arena(), snapshotWithSetting("D"_sr)[0]);
	request.reply.send(GlobalConfigRefreshReply{ repaired.arena(), 44, repaired });
	co_await timeoutError(initialized, 5.0);
	ASSERT(!test.db->globalConfig->get("B"_sr).isValid());
	ASSERT(test.db->globalConfig->get<bool>("C"_sr, false));
	ASSERT(test.db->globalConfig->get<bool>("D"_sr, false));
	ASSERT_EQ(callbacks, 1);
}

TEST_CASE("/fdbclient/GlobalConfig/AcceptsSnapshotWithLegacyProfilingWrite") {
	auto history = publishedChange(41, 42, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr));
	const auto limit = Tuple::makeTuple(int64_t(1024)).pack();
	// An API 710 transaction restored A and set the profiling limit. Only its profiling history row survived.
	auto& at44 = history.emplace_back(44);
	at44.mutations.emplace_back_deep(at44.mutations.arena(),
	                                 MutationRef(MutationRef::SetValue, fdbClientInfoTxnSizeLimit, limit));
	GlobalConfigRefreshTest test(std::move(history));
	auto initialized = test.db->globalConfig->onInitialized();
	auto request = co_await test.nextRequest();
	RangeResult empty;
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 44, empty });
	auto retry = co_await race(test.nextRequest(), initialized);
	ASSERT_EQ(retry.index(), 0);

	auto rows = snapshotWithSetting("A"_sr);
	rows.push_back_deep(rows.arena(), KeyValueRef(fdbClientInfoTxnSizeLimit.withPrefix(globalConfigKeysPrefix), limit));
	std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ rows.arena(), 44, rows });
	co_await timeoutError(initialized, 5.0);
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	ASSERT_EQ(test.db->globalConfig->get<int64_t>(fdbClientInfoTxnSizeLimit, -1), 1024);
}

TEST_CASE("/fdbclient/GlobalConfig/LegacySnapshotUsesLastBatchAtVersion") {
	auto first = Tuple::makeTuple(int64_t(100)).pack();
	auto last = Tuple::makeTuple(int64_t(200)).pack();
	auto history = publishedChange(41, 42, MutationRef(MutationRef::SetValue, fdbClientInfoTxnSizeLimit, first));
	auto& batch = history.emplace_back(42);
	batch.mutations.emplace_back_deep(batch.mutations.arena(),
	                                  MutationRef(MutationRef::SetValue, fdbClientInfoTxnSizeLimit, last));
	for (bool boundaryAt42 : { false, true }) {
		if (boundaryAt42) {
			history.erase(history.begin());
			history.front() = VersionHistory(42);
		}
		GlobalConfigRefreshTest test(history);
		auto initialized = test.db->globalConfig->onInitialized();
		auto request = co_await test.nextRequest();
		auto key = fdbClientInfoTxnSizeLimit.withPrefix(globalConfigKeysPrefix);
		RangeResult stale;
		stale.push_back_deep(stale.arena(), KeyValueRef(key, first));
		request.reply.send(GlobalConfigRefreshReply{ stale.arena(), 42, stale });
		auto retry = co_await race(test.nextRequest(), initialized);
		ASSERT_EQ(retry.index(), 0);
		ASSERT(!test.db->globalConfig->get(fdbClientInfoTxnSizeLimit).isValid());

		RangeResult current;
		current.push_back_deep(current.arena(), KeyValueRef(key, last));
		std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ current.arena(), 42, current });
		co_await timeoutError(initialized, 5.0);
		ASSERT_EQ(test.db->globalConfig->get<int64_t>(fdbClientInfoTxnSizeLimit, -1), 200);
	}
}

TEST_CASE("/fdbclient/GlobalConfig/LegacySnapshotAfterUnseenSet") {
	auto history = publishedChange(41, 42, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr));
	auto& at44 = history.emplace_back(44);
	at44.mutations.emplace_back_deep(at44.mutations.arena(),
	                                 MutationRef(MutationRef::SetValue, fdbClientInfoTxnSizeLimit, "\x21"_sr));
	GlobalConfigRefreshTest test(std::move(history));
	auto initialized = test.db->globalConfig->onInitialized();
	auto request = co_await test.nextRequest();
	// A set at 43 can restore A even when its history row is lost.
	auto rows = snapshotWithSetting("A"_sr);
	rows.push_back_deep(rows.arena(),
	                    KeyValueRef(fdbClientInfoTxnSizeLimit.withPrefix(globalConfigKeysPrefix),
	                                Tuple::makeTuple(int64_t(1024)).pack()));
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 44, rows });
	co_await timeoutError(initialized, 5.0);
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	ASSERT_EQ(test.db->globalConfig->get<int64_t>(fdbClientInfoTxnSizeLimit, -1), 1024);
}

TEST_CASE("/fdbclient/GlobalConfig/PublicationChangesDuringLegacyVersionCheck") {
	int callbacks = 0;
	GlobalConfigRefreshTest test({ VersionHistory(46) });
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ++callbacks; });
	auto request = co_await test.nextRequest();
	auto rows = snapshotWithSetting("A"_sr);
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 46, rows });
	auto grv = co_await timeoutError(waitAndForward(test.proxy.getConsistentReadVersion.getFuture()), 5.0);
	test.publish(publishedChange(41, 44, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr)));
	GetReadVersionReply version;
	version.version = 50;
	grv.reply.send(version);
	auto repaired = test.serveSnapshots(44, RangeResult());
	co_await timeoutError(test.db->globalConfig->onInitialized(), 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT_EQ(callbacks, 0);
}

TEST_CASE("/fdbclient/GlobalConfig/VerifiedSnapshotMakesProgressDuringInitialization") {
	auto snapshotAt = [](Version version) {
		RangeResult rows;
		auto counter = Tuple::makeTuple(version).pack();
		for (auto key : { "B"_sr, "C"_sr }) {
			rows.push_back_deep(rows.arena(), KeyValueRef(key.withPrefix(globalConfigKeysPrefix), counter));
		}
		return rows;
	};
	auto changeAt = [](Version version) {
		VersionHistory change(version);
		auto counter = Tuple::makeTuple(version).pack();
		for (auto key : { "B"_sr, "C"_sr }) {
			change.mutations.emplace_back_deep(change.mutations.arena(),
			                                   MutationRef(MutationRef::SetValue, key, counter));
		}
		return change;
	};
	enum class FirstReply { LegacyAtTarget, LegacyAboveTarget, WithReadVersion };
	for (auto first : { FirstReply::LegacyAtTarget, FirstReply::LegacyAboveTarget, FirstReply::WithReadVersion }) {
		const bool verified = first == FirstReply::WithReadVersion;
		Version published = 41;
		std::vector<VersionHistory> history{ VersionHistory(30), changeAt(published) };
		std::vector<Version> observed;
		GlobalConfigRefreshTest test(history);
		auto initialized = test.db->globalConfig->onInitialized();
		test.db->globalConfig->trigger("B"_sr, [&](std::optional<std::any> value) {
			if (value.has_value()) {
				auto counter = std::any_cast<int64_t>(value.value());
				ASSERT_EQ(test.db->globalConfig->get<int64_t>("C"_sr, 0), counter);
				observed.push_back(counter);
			}
		});

		auto request = co_await test.nextRequest();
		if (first == FirstReply::LegacyAboveTarget) {
			// The first legacy reply is newer than the original target and one write behind.
			published += 10;
			history.push_back(changeAt(published));
			test.publish(history);
		}
		for (int write = 0; write < 4; ++write) {
			// Every reply arrives after the next write. Its own history row is still replayable.
			const Version stamp = published;
			auto rows = snapshotAt(stamp);
			published += 10;
			history.push_back(changeAt(published));
			if (history.size() > 3) {
				history.erase(history.begin());
				history.front() = VersionHistory(history.front().version);
			}
			test.publish(history, snapshotProof(published, snapshotAt(published)));
			Optional<Version> readVersion;
			if (verified) {
				readVersion = stamp + 4;
			} else if (first == FirstReply::LegacyAboveTarget && write == 0) {
				auto stale = snapshotAt(41);
				request.reply.send(GlobalConfigRefreshReply{ stale.arena(), stamp, stale });
				request = co_await test.nextRequest();
				ASSERT(!initialized.isReady());
				ASSERT(observed.empty());
			}
			request.reply.send(GlobalConfigRefreshReply{ rows.arena(), stamp, rows, readVersion });
			request = co_await test.nextRequest();
			ASSERT(initialized.isReady());
			ASSERT(!observed.empty());
			ASSERT_EQ(observed.back(), stamp);
		}

		auto changed = test.db->globalConfig->onChange();
		auto latest = snapshotAt(published);
		Optional<Version> readVersion;
		if (verified) {
			readVersion = published;
		}
		request.reply.send(GlobalConfigRefreshReply{ latest.arena(), published, latest, readVersion });
		co_await timeoutError(changed, 5.0);
		ASSERT_EQ(observed.front(), first == FirstReply::LegacyAboveTarget ? 51 : 41);
		ASSERT_EQ(observed.back(), published);
		for (auto key : { "B"_sr, "C"_sr }) {
			ASSERT_EQ(test.db->globalConfig->get<int64_t>(key, 0), published);
		}
	}
}

TEST_CASE("/fdbclient/GlobalConfig/LegacySnapshotNeedsCorroboratedOriginalRow") {
	enum class OriginalRow { Retained, Replaced, Dropped, Marker };
	auto enabled = Tuple::makeTuple(true).pack();
	auto setC = MutationRef(MutationRef::SetValue, "C"_sr, enabled);
	auto setB = MutationRef(MutationRef::SetValue, "B"_sr, enabled);
	for (auto row : { OriginalRow::Retained, OriginalRow::Replaced, OriginalRow::Dropped, OriginalRow::Marker }) {
		auto original = row == OriginalRow::Marker ? std::vector<VersionHistory>{ VersionHistory(41) }
		                                           : publishedChange(40, 41, setC);
		int bCallbacks = 0;
		GlobalConfigRefreshTest test(original);
		auto grvs = test.serveReadVersions(55);
		auto initialized = test.db->globalConfig->onInitialized();
		test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ASSERT(false); });
		test.db->globalConfig->trigger("B"_sr, [&](std::optional<std::any> value) {
			ASSERT(value.has_value() && std::any_cast<bool>(value.value()));
			ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
			++bCallbacks;
		});
		auto request = co_await test.nextRequest();

		auto advanced = original;
		if (row == OriginalRow::Dropped) {
			advanced = { VersionHistory(41) };
		} else if (row == OriginalRow::Replaced) {
			// The old snapshot agrees with both rows, but the controller changed the row itself.
			advanced = publishedChange(40, 41, MutationRef(MutationRef::ClearRange, "Z"_sr, "z"_sr));
		}
		auto stale = snapshotWithSetting("A"_sr);
		if (row != OriginalRow::Retained) {
			stale.push_back_deep(stale.arena(), snapshotWithSetting("C"_sr)[0]);
		}
		if (row == OriginalRow::Dropped || row == OriginalRow::Replaced) {
			test.publish(advanced);
			request.reply.send(GlobalConfigRefreshReply{ stale.arena(), 41, stale });
			auto retry = co_await race(test.nextRequest(), initialized);
			ASSERT_EQ(retry.index(), 0);
			ASSERT_EQ(bCallbacks, 0);
			request = std::get<0>(retry);
		}
		advanced.push_back(publishedChange(41, 51, setB).back());
		test.publish(advanced);
		request.reply.send(GlobalConfigRefreshReply{ stale.arena(), 41, stale });
		auto retry = co_await race(test.nextRequest(), initialized);
		ASSERT_EQ(retry.index(), 0);
		ASSERT_EQ(bCallbacks, 0);
		auto latest = snapshotWithSetting("B"_sr);
		latest.push_back_deep(latest.arena(), snapshotWithSetting("C"_sr)[0]);
		std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ latest.arena(), 51, latest });
		co_await timeoutError(initialized, 5.0);
		ASSERT_EQ(bCallbacks, 1);
		ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
		ASSERT(test.db->globalConfig->get<bool>("B"_sr, false));
		ASSERT(test.db->globalConfig->get<bool>("C"_sr, false));
	}
}

TEST_CASE("/fdbclient/GlobalConfig/VerifiedSnapshotMustCoverRefreshFence") {
	auto enabled = Tuple::makeTuple(true).pack();
	auto history = publishedChange(41, 42, MutationRef(MutationRef::SetValue, "A"_sr, enabled));
	auto& final = history.emplace_back(45);
	final.mutations.emplace_back_deep(final.mutations.arena(), MutationRef(MutationRef::SetValue, "B"_sr, enabled));
	GlobalConfigRefreshTest test(history);
	auto initialized = test.db->globalConfig->onInitialized();
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ASSERT(false); });
	auto request = co_await test.nextRequest();
	RangeResult empty;
	// Neither read covers the publication at 45.
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 40, empty, 40 });
	request = co_await test.nextRequest();
	ASSERT(!initialized.isReady());
	// A repair restored stamp 41 after A was cleared. This read still predates the set of B.
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 41, empty, 44 });
	request = co_await test.nextRequest();
	ASSERT(!initialized.isReady());
	auto rows = snapshotWithSetting("B"_sr);
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 45, rows, 45 });
	co_await timeoutError(initialized, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT(test.db->globalConfig->get<bool>("B"_sr, false));
}

TEST_CASE("/fdbclient/GlobalConfig/LegacyMarkerProofMustCoverRefreshFence") {
	int callbacks = 0;
	GlobalConfigRefreshTest test;
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ++callbacks; });
	auto initialized = test.db->globalConfig->onInitialized();
	auto stale = snapshotWithSetting("A"_sr);
	// Both publications arrive before the updater issues its first request.
	test.publish({ VersionHistory(44) }, snapshotProof(54, stale));
	RangeResult empty;
	test.publish(publishedChange(44, 55, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr)),
	             snapshotProof(56, empty));
	auto request = co_await test.nextRequest();
	request.reply.send(GlobalConfigRefreshReply{ stale.arena(), 44, stale });
	auto retry = co_await race(test.nextRequest(), initialized);
	ASSERT_EQ(retry.index(), 0);
	ASSERT_EQ(callbacks, 0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());

	std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ empty.arena(), 55, empty });
	co_await timeoutError(initialized, 5.0);
	ASSERT_EQ(callbacks, 0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
}

TEST_CASE("/fdbclient/GlobalConfig/LaterLegacyLabelNeedsRepairProof") {
	auto enabled = Tuple::makeTuple(true).pack();
	auto latest = snapshotWithSetting("B"_sr);
	auto stale = snapshotWithSetting("A"_sr);
	stale.push_back_deep(stale.arena(), latest[0]);
	auto atRepair = snapshotWithSetting("B"_sr, false);
	enum class Marker { Pending, Installed, ReplacedByBare };
	for (auto marker : { Marker::Pending, Marker::Installed, Marker::ReplacedByBare }) {
		int callbacks = 0;
		GlobalConfigRefreshTest test;
		test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ++callbacks; });
		auto done = test.db->globalConfig->onInitialized();
		test.publish({ VersionHistory(44) }, snapshotProof(54, atRepair));
		if (marker == Marker::Installed) {
			auto request = co_await test.nextRequest();
			request.reply.send(GlobalConfigRefreshReply{ atRepair.arena(), 44, atRepair });
			co_await timeoutError(done, 5.0);
			ASSERT(!test.db->globalConfig->get<bool>("B"_sr, true));
			done = test.db->globalConfig->onChange();
		} else if (marker == Marker::ReplacedByBare) {
			test.publish({ VersionHistory(44) });
		}
		auto history = publishedChange(44, 55, MutationRef(MutationRef::SetValue, "B"_sr, enabled));
		test.publish(history);
		auto request = co_await test.nextRequest();
		// The proxy read A and B before the repair under the damaged label 55.
		request.reply.send(GlobalConfigRefreshReply{ stale.arena(), 55, stale });
		auto retry = co_await race(test.nextRequest(), done);
		ASSERT_EQ(retry.index(), 0);
		ASSERT_EQ(callbacks, 0);
		ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
		test.publish(history, snapshotProof(56, latest));
		std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ stale.arena(), 55, stale });
		retry = co_await race(test.nextRequest(), done);
		ASSERT_EQ(retry.index(), 0);
		ASSERT_EQ(callbacks, 0);
		std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ latest.arena(), 55, latest });
		co_await timeoutError(done, 5.0);
		ASSERT_EQ(callbacks, 0);
		ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
		ASSERT(test.db->globalConfig->get<bool>("B"_sr, false));
	}
}

TEST_CASE("/fdbclient/GlobalConfig/StableReplacementFinishesRefresh") {
	auto enabled = Tuple::makeTuple(true).pack();
	auto original = publishedChange(40, 41, MutationRef(MutationRef::SetValue, "C"_sr, enabled));
	enum class Peers { Legacy, ControllerProof, Both };
	for (auto peers : { Peers::Legacy, Peers::ControllerProof, Peers::Both }) {
		std::vector<bool> callbacks;
		GlobalConfigRefreshTest test(original);
		test.db->globalConfig->trigger("C"_sr,
		                               [&](std::optional<std::any> value) { callbacks.push_back(value.has_value()); });
		auto initialized = test.db->globalConfig->onInitialized();
		auto grvs = test.serveReadVersions(50);
		auto request = co_await test.nextRequest();
		auto current = snapshotWithSetting("C"_sr);
		auto marker = std::vector<VersionHistory>{ VersionHistory(41) };
		Optional<GlobalConfigSnapshotProof> replacement;
		if (peers != Peers::Legacy) {
			replacement = snapshotProof(50, current);
		}
		test.publish(marker, replacement);

		auto stale = snapshotWithSetting("A"_sr);
		stale.push_back_deep(stale.arena(), snapshotWithSetting("C"_sr)[0]);
		request.reply.send(GlobalConfigRefreshReply{ stale.arena(), 41, stale });
		auto retry = co_await race(test.nextRequest(), initialized);
		ASSERT_EQ(retry.index(), 0);
		ASSERT(!test.db->globalConfig->get("A"_sr).isValid());

		Optional<Version> readVersion;
		if (peers == Peers::Both) {
			readVersion = 50;
		}
		std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ current.arena(), 41, current, readVersion });
		co_await timeoutError(initialized, 5.0);
		ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
		ASSERT(test.db->globalConfig->get<bool>("C"_sr, false));
		ASSERT_EQ(callbacks.size(), 1);
		ASSERT(callbacks[0]);
		if (peers == Peers::Legacy) {
			// The retry can initialize on a matching label. Neither peer identifies when these rows were read.
			continue;
		}

		// A second repair at the same label cleared C. The proxy still serves the first repair.
		RangeResult empty;
		auto proof = snapshotProof(60, empty);
		auto changed = test.publish(marker, proof);
		request = co_await test.nextRequest();
		request.reply.send(GlobalConfigRefreshReply{ current.arena(), 41, current, readVersion });
		auto next = co_await race(test.nextRequest(), changed);
		ASSERT_EQ(next.index(), 0);
		ASSERT_EQ(callbacks.size(), 1);
		if (peers == Peers::Both) {
			readVersion = 60;
		}
		std::get<0>(next).reply.send(GlobalConfigRefreshReply{ empty.arena(), 41, empty, readVersion });
		co_await timeoutError(changed, 5.0);
		ASSERT(!test.db->globalConfig->get("C"_sr).isValid());
		ASSERT_EQ(callbacks.size(), 2);
		ASSERT(!callbacks[1]);

		auto unchanged = test.publish(marker, proof);
		auto redelivery = co_await race(test.nextRequest(), unchanged);
		ASSERT_EQ(redelivery.index(), 1);
		ASSERT_EQ(callbacks.size(), 2);
	}
}

TEST_CASE("/fdbclient/GlobalConfig/ReplacementProofCoversPendingRead") {
	auto enabled = Tuple::makeTuple(true).pack();
	auto history = publishedChange(41, 51, MutationRef(MutationRef::SetValue, "C"_sr, enabled));
	GlobalConfigRefreshTest test(history);
	auto request = co_await test.nextRequest();
	history.push_back(publishedChange(51, 56, MutationRef(MutationRef::SetValue, "B"_sr, enabled)).back());
	test.publish(history);
	auto at55 = snapshotWithSetting("C"_sr);
	request.reply.send(GlobalConfigRefreshReply{ at55.arena(), 51, at55, 55 });
	request = co_await test.nextRequest();
	ASSERT(test.db->globalConfig->onInitialized().isReady());
	ASSERT(test.db->globalConfig->get<bool>("C"_sr, false));

	// The old proxy's label is below the installed read at 55. Its rows match the controller's read at 60.
	RangeResult empty;
	auto changed = test.publish({ VersionHistory(41) }, snapshotProof(60, empty));
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 41, empty });
	co_await timeoutError(changed, 5.0);
	ASSERT(!test.db->globalConfig->get("C"_sr).isValid());
}

TEST_CASE("/fdbclient/GlobalConfig/VerifiedSnapshotAfterStampRepair") {
	auto enabled = Tuple::makeTuple(true).pack();
	GlobalConfigRefreshTest test(publishedChange(40, 41, MutationRef(MutationRef::SetValue, "A"_sr, enabled)));
	auto initial = co_await test.nextRequest();
	auto rows = snapshotWithSetting("A"_sr);
	initial.reply.send(GlobalConfigRefreshReply{ rows.arena(), 41, rows, 41 });
	co_await timeoutError(test.db->globalConfig->onInitialized(), 5.0);
	int removals = 0;
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any> value) {
		ASSERT(!value.has_value());
		++removals;
	});

	auto changed = test.publish({ VersionHistory(44) });
	auto request = co_await test.nextRequest();
	ASSERT_EQ(request.lastKnown, 41);
	// The replacement proxy has stamp 40. Its cached read at 50 predates the repair at 51.
	request.reply.sendError(future_version());
	request = co_await test.nextRequest();
	ASSERT_EQ(request.lastKnown, invalidVersion);
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 40, rows, 50 });
	auto grv = co_await timeoutError(waitAndForward(test.proxy.getConsistentReadVersion.getFuture()), 5.0);
	GetReadVersionReply afterRepair;
	afterRepair.version = 52;
	grv.reply.send(afterRepair);
	request = co_await test.nextRequest();
	ASSERT(!changed.isReady());
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	ASSERT_EQ(removals, 0);

	// The label is still 40, but the transaction that supplied these rows covers the repair.
	RangeResult empty;
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 40, empty, 53 });
	co_await timeoutError(changed, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT_EQ(removals, 1);
}

TEST_CASE("/fdbclient/GlobalConfig/LaterPublicationCannotValidatePreRepairSnapshot") {
	auto enabled = Tuple::makeTuple(true).pack();
	auto disabled = Tuple::makeTuple(false).pack();
	for (bool verified : { true, false }) {
		for (Version cachedStamp : { 40, 44, 46 }) {
			std::vector<bool> observed;
			GlobalConfigRefreshTest test({ VersionHistory(44) });
			auto initialized = test.db->globalConfig->onInitialized();
			test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ASSERT(false); });
			test.db->globalConfig->trigger("B"_sr, [&](std::optional<std::any> value) {
				if (value.has_value()) {
					ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
					observed.push_back(std::any_cast<bool>(value.value()));
				}
			});
			auto request = co_await test.nextRequest();
			// At 50 the proxy cached A. A repair at 51 cleared A and restored stamp 44.
			if (cachedStamp == 46) {
				// This second repair marker repeats the pre-repair snapshot's label.
				test.publish({ VersionHistory(cachedStamp) });
			} else {
				test.publish(publishedChange(44, 54, MutationRef(MutationRef::SetValue, "B"_sr, enabled)));
			}
			auto stale = snapshotWithSetting("A"_sr);
			Optional<Version> readVersion;
			if (verified) {
				readVersion = 50;
			}
			auto grvs = test.serveReadVersions(55);
			request.reply.send(GlobalConfigRefreshReply{ stale.arena(), cachedStamp, stale, readVersion });
			request = co_await test.nextRequest();
			ASSERT(!initialized.isReady());

			grvs.cancel();
			grvs = test.serveReadVersions(65);
			test.publish(publishedChange(54, 64, MutationRef(MutationRef::SetValue, "B"_sr, disabled)));
			request.reply.send(GlobalConfigRefreshReply{ stale.arena(), cachedStamp, stale, readVersion });
			request = co_await test.nextRequest();
			ASSERT(!initialized.isReady());
			ASSERT(observed.empty());

			if (verified) {
				// This transaction covers the original repair, even though another write has since committed.
				auto repaired = snapshotWithSetting("B"_sr);
				request.reply.send(GlobalConfigRefreshReply{ repaired.arena(), 54, repaired, 55 });
				co_await timeoutError(initialized, 5.0);
				ASSERT_EQ(observed.size(), 1);
				ASSERT(observed.front());
				request = co_await test.nextRequest();
				readVersion = 65;
			}
			auto done = verified ? test.db->globalConfig->onChange() : initialized;
			auto latest = snapshotWithSetting("B"_sr, false);
			request.reply.send(GlobalConfigRefreshReply{ latest.arena(), 64, latest, readVersion });
			co_await timeoutError(done, 5.0);
			ASSERT_EQ(observed.size(), verified ? 2 : 1);
			ASSERT(!observed.back());
			ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
			ASSERT(!test.db->globalConfig->get<bool>("B"_sr, true));
		}
	}
}

TEST_CASE("/fdbclient/GlobalConfig/RestoredMarkerNeedsNewSnapshot") {
	auto stale = snapshotWithSetting("A"_sr);
	auto enabled = Tuple::makeTuple(true).pack();
	for (bool alreadyInitialized : { false, true }) {
		auto marker = std::vector<VersionHistory>{ VersionHistory(44) };
		auto initial =
		    alreadyInitialized ? publishedChange(40, 44, MutationRef(MutationRef::SetValue, "A"_sr, enabled)) : marker;
		std::vector<bool> callbacks;
		GlobalConfigRefreshTest test(initial);
		auto grvs = test.serveReadVersions(52);
		auto done = test.db->globalConfig->onInitialized();
		if (alreadyInitialized) {
			auto request = co_await test.nextRequest();
			request.reply.send(GlobalConfigRefreshReply{ stale.arena(), 44, stale, 50 });
			co_await timeoutError(done, 5.0);
			ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
			done = test.publish(marker);
		}
		test.db->globalConfig->trigger("A"_sr,
		                               [&](std::optional<std::any> value) { callbacks.push_back(value.has_value()); });

		// The repair cleared A at 51. Both the old and the new rows carry label 44.
		auto request = co_await test.nextRequest();
		request.reply.send(GlobalConfigRefreshReply{ stale.arena(), 44, stale, 50 });
		auto retry = co_await race(test.nextRequest(), done);
		ASSERT_EQ(retry.index(), 0);
		ASSERT(callbacks.empty());
		RangeResult empty;
		std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ empty.arena(), 44, empty, 52 });
		co_await timeoutError(done, 5.0);
		ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
		ASSERT_EQ(callbacks.size(), alreadyInitialized ? 1 : 0);
		if (alreadyInitialized) {
			ASSERT(!callbacks.front());
		}
		// A change to another ClientDBInfo field carries the same publication.
		auto unchanged = test.publish(marker);
		auto next = co_await race(test.nextRequest(), unchanged);
		ASSERT_EQ(next.index(), 1);
		ASSERT_EQ(callbacks.size(), alreadyInitialized ? 1 : 0);
	}
}

TEST_CASE("/fdbclient/GlobalConfig/IncompleteSuffixRequiresNewerSnapshot") {
	const auto enabled = Tuple::makeTuple(true).pack();
	auto history = publishedChange(41, 42, MutationRef(MutationRef::SetValue, "A"_sr, enabled));
	auto& profiling = history.emplace_back(44);
	const auto limit = Tuple::makeTuple(int64_t(1024)).pack();
	profiling.mutations.emplace_back_deep(profiling.mutations.arena(),
	                                      MutationRef(MutationRef::SetValue, fdbClientInfoTxnSizeLimit, limit));
	GlobalConfigRefreshTest test(history);
	auto initialized = test.db->globalConfig->onInitialized();
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ASSERT(false); });
	auto request = co_await test.nextRequest();
	RangeResult empty;
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 41, empty, 41 });
	request = co_await test.nextRequest();
	ASSERT(!initialized.isReady());
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());

	// The profiling transaction also cleared A. Its history row contains only the profiling setting.
	RangeResult rows;
	rows.push_back_deep(rows.arena(), KeyValueRef(fdbClientInfoTxnSizeLimit.withPrefix(globalConfigKeysPrefix), limit));
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 44, rows, 45 });
	co_await timeoutError(initialized, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT_EQ(test.db->globalConfig->get<int64_t>(fdbClientInfoTxnSizeLimit, -1), 1024);
}

TEST_CASE("/fdbclient/GlobalConfig/VerifiedSnapshotWithSingleMarker") {
	for (bool verified : { false, true }) {
		std::vector<bool> present;
		GlobalConfigRefreshTest test({ VersionHistory(46) });
		auto initialized = test.db->globalConfig->onInitialized();
		test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any> value) {
			present.push_back(value.has_value());
			ASSERT_EQ(test.db->globalConfig->get("A"_sr).isValid(), value.has_value());
		});
		auto request = co_await test.nextRequest();
		auto rows = snapshotWithSetting("A"_sr);
		Optional<Version> readVersion;
		auto grvs = test.serveReadVersions(50);
		if (verified) {
			for (auto [version, more] :
			     { std::pair<Version, bool>{ 45, false }, { MAX_VERSION, false }, { 50, true } }) {
				rows.more = more;
				request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 46, rows, version });
				auto retry = co_await race(test.nextRequest(), initialized);
				ASSERT_EQ(retry.index(), 0);
				ASSERT(present.empty());
				request = std::get<0>(retry);
			}
			readVersion = 50;
		}
		rows.more = false;
		request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 46, rows, readVersion });
		co_await timeoutError(initialized, 5.0);
		ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
		ASSERT_EQ(present.size(), 1);
		ASSERT(present.front());

		// Either reply can repeat the marker's damaged stamp. A later repair must still take effect.
		auto changed = test.publish(publishedChange(41, 44, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr)));
		auto repair = co_await test.nextRequest();
		ASSERT_EQ(repair.lastKnown, 0);
		RangeResult empty;
		repair.reply.send(GlobalConfigRefreshReply{ empty.arena(), 44, empty, readVersion });
		co_await timeoutError(changed, 5.0);
		ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
		ASSERT_EQ(present.size(), 2);
		ASSERT(!present.back());
	}
}

TEST_CASE("/fdbclient/GlobalConfig/RepairedPublicationReleasesPendingRefresh") {
	int callbacks = 0;
	auto enabled = Tuple::makeTuple(true).pack();
	GlobalConfigRefreshTest test(publishedChange(40, 41, MutationRef(MutationRef::SetValue, "A"_sr, enabled)));
	auto grvs = test.serveReadVersions(50);
	auto initial = co_await test.nextRequest();
	auto at41 = snapshotWithSetting("A"_sr);
	initial.reply.send(GlobalConfigRefreshReply{ at41.arena(), 41, at41 });
	co_await timeoutError(test.db->globalConfig->onInitialized(), 5.0);
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any> value) {
		ASSERT(!value.has_value());
		++callbacks;
	});

	const Version damagedStamp = Version(1) << 62;
	auto changed = test.publish({ VersionHistory(damagedStamp) });
	auto request = co_await test.nextRequest();
	RangeResult empty;
	// An old controller and proxy can both read the same damaged versionstamp.
	for (Version version : { damagedStamp, Version(44) }) {
		ASSERT_EQ(request.lastKnown, 41);
		request.reply.send(GlobalConfigRefreshReply{ empty.arena(), version, empty });
		auto retry = co_await race(test.nextRequest(), changed);
		ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
		ASSERT_EQ(callbacks, 0);
		ASSERT_EQ(retry.index(), 0);
		request = std::get<0>(retry);
	}
	test.publish(publishedChange(43, 44, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr)));
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 44, empty });
	auto repaired = test.serveSnapshots(44, empty);
	co_await timeoutError(changed, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT_EQ(callbacks, 1);
}

TEST_CASE("/fdbclient/GlobalConfig/LowerRefreshFenceSurvivesNextPublication") {
	GlobalConfigRefreshTest test({ VersionHistory(Version(1) << 62) });
	auto initialized = test.db->globalConfig->onInitialized();
	int bCallbacks = 0;
	test.db->globalConfig->trigger("B"_sr, [&](std::optional<std::any> value) {
		ASSERT(value.has_value() && std::any_cast<bool>(value.value()));
		ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
		++bCallbacks;
	});
	auto request = co_await test.nextRequest();
	test.publish({ VersionHistory(44) });
	RangeResult empty;
	auto grvs = test.serveReadVersions(47);
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 42, empty, 43 });
	request = co_await test.nextRequest();
	ASSERT(!initialized.isReady());
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 44, empty, 45 });
	request = co_await test.nextRequest();
	ASSERT(!initialized.isReady());

	// A healthy write advances the repaired publication before the next reply arrives.
	grvs.cancel();
	grvs = test.serveReadVersions(55);
	auto enabled = Tuple::makeTuple(true).pack();
	test.publish(publishedChange(44, 54, MutationRef(MutationRef::SetValue, "B"_sr, enabled)));
	auto rows = snapshotWithSetting("A"_sr);
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 44, rows, 48 });
	co_await timeoutError(initialized, 5.0);
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	ASSERT(!test.db->globalConfig->get("B"_sr).isValid());
	ASSERT_EQ(bCallbacks, 0);
	request = co_await test.nextRequest();
	auto changed = test.db->globalConfig->onChange();
	rows.push_back_deep(rows.arena(), snapshotWithSetting("B"_sr)[0]);
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 54, rows, 54 });
	co_await timeoutError(changed, 5.0);
	ASSERT_EQ(bCallbacks, 1);
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	ASSERT(test.db->globalConfig->get<bool>("B"_sr, false));
}

TEST_CASE("/fdbclient/GlobalConfig/MatchingLegacyStampDoesNotSkipRepairedClear") {
	int removals = 0;
	GlobalConfigRefreshTest test;
	test.db->updateCachedReadVersion(now(), 50);
	auto grvs = test.serveReadVersions(50);
	auto initial = co_await test.nextRequest();
	auto unstamped = snapshotWithSetting("old"_sr);
	initial.reply.send(GlobalConfigRefreshReply{ unstamped.arena(), 0, unstamped });
	co_await timeoutError(test.db->globalConfig->onInitialized(), 5.0);
	ASSERT(test.db->globalConfig->get<bool>("old"_sr, false));

	// The old proxy and controller both used 46 from the same damaged version key.
	auto changed = test.publish({ VersionHistory(46) });
	auto request = co_await test.nextRequest();
	auto at41 = snapshotWithSetting("A"_sr);
	request.reply.send(GlobalConfigRefreshReply{ at41.arena(), 46, at41 });
	co_await timeoutError(changed, 5.0);
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	ASSERT(!test.db->globalConfig->get("old"_sr).isValid());
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any> value) {
		ASSERT(!value.has_value());
		++removals;
	});

	changed = test.publish(publishedChange(41, 44, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr)));
	auto repair = co_await race(test.nextRequest(), changed);
	ASSERT_EQ(repair.index(), 0);
	ASSERT_EQ(std::get<0>(repair).lastKnown, 0);
	RangeResult empty;
	std::get<0>(repair).reply.send(GlobalConfigRefreshReply{ empty.arena(), 44, empty });
	co_await timeoutError(changed, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT_EQ(removals, 1);
}

TEST_CASE("/fdbclient/GlobalConfig/LegacySentinelDoesNotSkipRepairedPublication") {
	int removals = 0;
	GlobalConfigRefreshTest test({ VersionHistory(MAX_VERSION) });
	auto grvs = test.serveReadVersions(50);
	auto snapshots = test.serveSnapshots(46, snapshotWithSetting("A"_sr));
	co_await timeoutError(test.db->globalConfig->onInitialized(), 5.0);
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	snapshots.cancel();
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any> value) {
		ASSERT(!value.has_value());
		++removals;
	});

	auto changed = test.publish({ VersionHistory(44) });
	auto repaired = test.serveSnapshots(44, RangeResult());
	co_await timeoutError(changed, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
	ASSERT_EQ(removals, 1);
}

namespace {

struct LegacyGlobalConfigRefreshReply {
	constexpr static FileIdentifier file_identifier = 12680327;
	Arena arena;
	Version version;
	RangeResultRef result;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, result, version, arena);
	}
};

template <class To, class From>
To readGlobalConfigReply(const From& reply) {
	auto protocol = AssumeVersion(currentProtocolVersion());
	auto bytes = ObjectWriter::toValue(ErrorOr<NetworkSenderTableT<From>>(reply), protocol);
	ArenaObjectReader reader(bytes.arena(), bytes, protocol);
	ErrorOr<EnsureTable<To>> received;
	reader.deserialize(received);
	return received.get().asUnderlyingType();
}

} // namespace

TEST_CASE("/fdbclient/GlobalConfig/RefreshReplyWireCompatibility") {
	auto rows = snapshotWithSetting("B"_sr);
	auto checkRows = [&](const auto& reply) {
		ASSERT_EQ(reply.version, 44);
		ASSERT(!reply.result.more);
		ASSERT_EQ(reply.result.size(), 1);
		ASSERT(reply.result[0] == rows[0]);
	};

	const LegacyGlobalConfigRefreshReply old{ rows.arena(), 44, rows };
	const auto fromOld = readGlobalConfigReply<GlobalConfigRefreshReply>(old);
	checkRows(fromOld);
	ASSERT(!fromOld.snapshotReadVersion.present());

	GlobalConfigRefreshReply current{ rows.arena(), 44, rows };
	for (auto readVersion : { Optional<Version>(), Optional<Version>(50) }) {
		current.snapshotReadVersion = readVersion;
		checkRows(readGlobalConfigReply<LegacyGlobalConfigRefreshReply>(current));
		const auto fromCurrent = readGlobalConfigReply<GlobalConfigRefreshReply>(current);
		checkRows(fromCurrent);
		ASSERT(fromCurrent.snapshotReadVersion == readVersion);
	}
	return Void();
}
