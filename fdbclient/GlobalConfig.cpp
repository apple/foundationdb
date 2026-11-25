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

void GlobalConfig::insert(Entry entry) {
	// Replacing only the value would leave the map key pointing into the old arena.
	data.erase(entry.first);
	data.insert(entry);
	if (auto callback = callbacks.find(entry.first); callback != callbacks.end()) {
		callback->second(entry.second->getValue());
	}
}

void GlobalConfig::erase(std::optional<KeyRangeRef> range) {
	auto it = data.begin();
	while (it != data.end()) {
		if (!range || range->contains(it->first)) {
			if (auto callback = callbacks.find(it->first); callback != callbacks.end()) {
				callback->second(std::nullopt);
			}
			it = data.erase(it);
		} else {
			++it;
		}
	}
}

bool GlobalConfig::applyHistory(const std::vector<VersionHistory>& history, Optional<Version> snapshotMarker) {
	const Version latest = history.back().version;
	if (snapshotMarker.present()) {
		// The same numeric marker needs only one snapshot. A legacy MAX_VERSION marker can cover new writes.
		return history.size() == 1 && latest == snapshotMarker.get() && latest != MAX_VERSION;
	}
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
			if (mutation.type == MutationRef::SetValue) {
				auto entry = decode(mutation.param1, mutation.param2);
				if (!entry.second.isValid()) {
					return false;
				}
				changes.emplace_back(std::move(entry));
			} else if (mutation.type == MutationRef::ClearRange) {
				changes.emplace_back(KeyRange(KeyRangeRef(mutation.param1, mutation.param2), vh.mutations.arena()));
			} else {
				UNREACHABLE();
			}
		}
	}
	for (auto& change : changes) {
		if (auto* entry = std::get_if<Entry>(&change)) {
			insert(std::move(*entry));
		} else {
			erase(std::get<KeyRange>(change));
		}
	}
	lastUpdate = latest;
	return true;
}

bool GlobalConfig::couldBeLegacyProfilingWrite(const VectorRef<MutationRef>& mutations) {
	if (mutations.size() > 2 || (mutations.size() == 2 && mutations.front().param1 == mutations.back().param1)) {
		return false;
	}
	return std::all_of(mutations.begin(), mutations.end(), [](const MutationRef& m) {
		if (m.param1 != fdbClientInfoTxnSizeLimit && m.param1 != fdbClientInfoTxnSampleRate) {
			return false;
		}
		if (m.type != MutationRef::SetValue) {
			return m.type == MutationRef::ClearRange && equalsKeyAfter(m.param1, m.param2);
		}
		auto value = decode(m.param1, m.param2).second;
		if (!value.isValid()) {
			return false;
		}
		if (m.param1 == fdbClientInfoTxnSizeLimit) {
			const auto* limit = std::any_cast<int64_t>(&value->getValue());
			return limit && Tuple::makeTuple(*limit).pack() == m.param2;
		}
		const auto* rate = std::any_cast<double>(&value->getValue());
		return rate && Tuple::makeTuple(*rate).pack() == m.param2;
	});
}

bool GlobalConfig::snapshotMatchesHistory(const RangeResultRef& rows, const std::vector<VersionHistory>& history) {
	// The first row is the replay boundary. Later writes replace earlier effects, including overlapping clears.
	KeyRangeMap<const MutationRef*> effects(nullptr, globalConfigDataKeys.end);
	for (size_t i = 1; i < history.size(); ++i) {
		const auto& mutations = history[i].mutations;
		// A legacy transaction can write both global config and profiling settings. Its profiling row
		// replaces the other history row at the same versionstamp, so earlier effects can be obsolete.
		if (i > 1 && couldBeLegacyProfilingWrite(mutations)) {
			effects.insert(globalConfigDataKeys, nullptr);
		}
		for (const auto& mutation : mutations) {
			auto begin = mutation.param1.withPrefix(globalConfigKeysPrefix);
			if (!mutation.corrupted && mutation.type == MutationRef::SetValue) {
				effects.insert(begin, &mutation);
			} else if (!mutation.corrupted && mutation.type == MutationRef::ClearRange &&
			           mutation.param1 <= mutation.param2) {
				effects.insert(KeyRangeRef(begin, mutation.param2.withPrefix(globalConfigKeysPrefix)), &mutation);
			} else {
				// A legacy controller can forward an unusable mutation. Only its replayable suffix is evidence.
				effects.insert(globalConfigDataKeys, nullptr);
			}
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

Future<GlobalConfigRefreshReply> GlobalConfig::getSnapshot(const ClientDBInfo* dbInfo, Version lastKnown) {
	Backoff backoff(CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_BACKOFF, CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_MAX_BACKOFF);
	while (true) {
		Error err;
		try {
			GlobalConfigRefreshReply reply =
			    co_await timeoutError(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
			                                           &GrvProxyInterface::refreshGlobalConfig,
			                                           GlobalConfigRefreshRequest{ lastKnown }),
			                          CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_TIMEOUT);
			const auto& readVersion = reply.snapshotReadVersion;
			if (reply.result.more || reply.version == MAX_VERSION ||
			    (readVersion.present() &&
			     (reply.version <= 0 || reply.version > readVersion.get() || readVersion.get() == MAX_VERSION))) {
				TraceEvent("GlobalConfigRefreshInvalidSnapshot")
				    .suppressFor(1.0)
				    .detail("ReplyVersion", reply.version)
				    .detail("SnapshotReadVersion", readVersion)
				    .detail("More", reply.result.more);
				throw serialization_failed();
			}
			// Check the current publication after each RPC, since the controller can replace it during a retry.
			const Version published = dbInfo->history.empty() ? 0 : dbInfo->history.back().version;
			if (reply.version < lastUpdate ||
			    (published != MAX_VERSION && reply.version != std::max(lastUpdate, published))) {
				TraceEvent("GlobalConfigRefreshVersionMismatch")
				    .suppressFor(1.0)
				    .detail("ReplayFloor", lastUpdate)
				    .detail("PublishedVersion", published)
				    .detail("ReplyVersion", reply.version);
				co_await delay(0.25);
				continue;
			}
			// A complete transaction read includes changes whose history rows were lost before publication.
			if (readVersion.present()) {
				co_return reply;
			}
			if (dbInfo->history.size() > 1 && reply.version == published &&
			    !snapshotMatchesHistory(reply.result, dbInfo->history)) {
				TraceEvent("GlobalConfigRefreshHistoryMismatch").suppressFor(1.0).detail("ReplyVersion", reply.version);
				co_await delay(0.25);
				continue;
			}
			// On a protocol mismatch an old controller can publish a single version from the same damaged
			// key as the proxy. An independent read version can rule out an impossible stamp.
			if (dbInfo->history.size() == 1 && reply.version > std::max(lastUpdate, cx->getCachedReadVersion())) {
				GetReadVersionRequest getVersion(
				    {}, 0, TransactionPriority::IMMEDIATE, cx->ssVersionVectorCache.getMaxVersion());
				auto grv = co_await timeoutError(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
				                                                  &GrvProxyInterface::getConsistentReadVersion,
				                                                  getVersion),
				                                 CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_TIMEOUT);
				if (reply.version > grv.version) {
					TraceEvent("GlobalConfigRefreshFutureSnapshot")
					    .suppressFor(1.0)
					    .detail("ReplyVersion", reply.version)
					    .detail("ReadVersion", grv.version);
					throw future_version();
				}
				if (dbInfo->history.size() != 1 || dbInfo->history.front().version != published) {
					continue;
				}
			}
			co_return reply;
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
		// Even a complete snapshot can contain a damaged, plausible stamp. A controller reading that
		// same stamp as a single marker cannot confirm its label as a replay floor.
		// MAX_VERSION requires a fresh snapshot, including on entry after a restart.
		Optional<Version> snapshotMarker = MAX_VERSION;
		Error err;
		try {
			co_await cx->onConnected();
			while (true) {
				co_await delay(0);
				// Subscribe before processing history so a reentrant DB info update cannot be lost.
				Future<Void> nextDbInfoChange = dbInfoChanged.onTrigger();
				const bool initializing = initialized.canBeSet();
				const auto& history = dbInfo->history;
				if (initializing || (!history.empty() && !applyHistory(history, snapshotMarker))) {
					auto reply = co_await getSnapshot(dbInfo, initializing ? invalidVersion : lastUpdate);
					// getSnapshot checked the latest publication. Watch for any change during callbacks.
					nextDbInfoChange = dbInfoChanged.onTrigger();
					const bool needsMarker = history.size() == 1 && reply.version > lastUpdate;
					snapshotMarker = needsMarker ? Optional<Version>(history.front().version) : Optional<Version>();
					erase();
					for (const auto& kv : reply.result) {
						auto entry = decode(kv.key.removePrefix(globalConfigKeysPrefix), kv.value);
						if (entry.second.isValid()) {
							insert(std::move(entry));
						}
					}
					if (!needsMarker) {
						lastUpdate = reply.version;
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

	Future<Void> publish(std::vector<VersionHistory> history) {
		auto changed = db->globalConfig->onChange();
		auto info = clientInfo->get();
		info.id = deterministicRandom()->randomUniqueID();
		info.history = std::move(history);
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
	VersionHistory at42(42);
	at42.mutations.emplace_back_deep(at42.mutations.arena(), MutationRef(MutationRef::SetValue, "B"_sr, enabled));
	at42.mutations.emplace_back_deep(at42.mutations.arena(), MutationRef(MutationRef::ClearRange, "A"_sr, "C"_sr));
	at42.mutations.emplace_back_deep(at42.mutations.arena(), MutationRef(MutationRef::ClearRange, "B"_sr, "D"_sr));
	VersionHistory at44(44);
	at44.mutations.emplace_back_deep(at44.mutations.arena(), MutationRef(MutationRef::SetValue, "C"_sr, disabled));
	at44.mutations.emplace_back_deep(at44.mutations.arena(), MutationRef(MutationRef::SetValue, "C"_sr, enabled));
	int callbacks = 0;
	GlobalConfigRefreshTest test({ VersionHistory(41), at42, at44 });
	test.db->globalConfig->trigger("C"_sr, [&](std::optional<std::any>) { ++callbacks; });
	auto initialized = test.db->globalConfig->onInitialized();
	auto request = co_await test.nextRequest();
	for (const auto& rows : { RangeResult(), snapshotWithSetting("C"_sr, false) }) {
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

TEST_CASE("/fdbclient/GlobalConfig/MalformedProfilingValuePreservesPublishedClear") {
	auto history = publishedChange(41, 42, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr));
	auto& at44 = history.emplace_back(44);
	at44.mutations.emplace_back_deep(at44.mutations.arena(),
	                                 MutationRef(MutationRef::SetValue, fdbClientInfoTxnSizeLimit, "\x21"_sr));
	GlobalConfigRefreshTest test(std::move(history));
	auto initialized = test.db->globalConfig->onInitialized();
	auto request = co_await test.nextRequest();
	auto stale = snapshotWithSetting("A"_sr);
	RangeResult corrected;
	corrected.push_back_deep(corrected.arena(),
	                         KeyValueRef(fdbClientInfoTxnSizeLimit.withPrefix(globalConfigKeysPrefix),
	                                     Tuple::makeTuple(int64_t(1024)).pack()));
	stale.push_back_deep(stale.arena(), corrected[0]);
	request.reply.send(GlobalConfigRefreshReply{ stale.arena(), 44, stale });
	auto retry = co_await race(test.nextRequest(), initialized);
	ASSERT_EQ(retry.index(), 0);
	std::get<0>(retry).reply.send(GlobalConfigRefreshReply{ corrected.arena(), 44, corrected });
	co_await timeoutError(initialized, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
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

TEST_CASE("/fdbclient/GlobalConfig/VerifiedSnapshotWithSingleMarker") {
	GlobalConfigRefreshTest test({ VersionHistory(46) });
	auto initialized = test.db->globalConfig->onInitialized();
	auto request = co_await test.nextRequest();
	auto rows = snapshotWithSetting("A"_sr);
	for (auto [readVersion, more] :
	     { std::pair<Version, bool>{ 45, false }, { MAX_VERSION, false }, { 50, true } }) {
		rows.more = more;
		request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 46, rows, readVersion });
		auto retry = co_await race(test.nextRequest(), initialized);
		ASSERT_EQ(retry.index(), 0);
		ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
		request = std::get<0>(retry);
	}
	rows.more = false;
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 46, rows, 50 });
	// No proxy serves a separate read-version request.
	co_await timeoutError(initialized, 5.0);
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));

	// The transaction saw stamp 46, but a repair can still replace that label with 44.
	auto changed = test.publish(publishedChange(41, 44, MutationRef(MutationRef::ClearRange, "A"_sr, "B"_sr)));
	auto repair = co_await test.nextRequest();
	ASSERT_EQ(repair.lastKnown, 0);
	RangeResult empty;
	repair.reply.send(GlobalConfigRefreshReply{ empty.arena(), 44, empty, 51 });
	co_await timeoutError(changed, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());
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
