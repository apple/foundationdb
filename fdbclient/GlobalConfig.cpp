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

bool GlobalConfig::applyHistory(const std::vector<VersionHistory>& history) {
	const Version latest = history.back().version;
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

Future<Version> GlobalConfig::refresh(Version lastKnown, Version largestSeen) {
	// Older controllers use MAX_VERSION to request a refresh without specifying a new version.
	const Version minimumVersion = largestSeen == MAX_VERSION ? lastKnown : largestSeen;
	Backoff backoff(CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_BACKOFF, CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_MAX_BACKOFF);
	while (true) {
		Error err;
		try {
			GlobalConfigRefreshReply reply =
			    co_await timeoutError(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
			                                           &GrvProxyInterface::refreshGlobalConfig,
			                                           GlobalConfigRefreshRequest{ lastKnown }),
			                          CLIENT_KNOBS->GLOBAL_CONFIG_REFRESH_TIMEOUT);
			if (reply.result.more || reply.version == MAX_VERSION) {
				TraceEvent("GlobalConfigRefreshInvalidSnapshot")
				    .suppressFor(1.0)
				    .detail("ReplyVersion", reply.version)
				    .detail("More", reply.result.more);
				throw serialization_failed();
			}
			if (reply.version < minimumVersion) {
				TraceEvent("GlobalConfigRefreshStaleSnapshot")
				    .suppressFor(1.0)
				    .detail("LastKnownVersion", lastKnown)
				    .detail("LargestSeenVersion", largestSeen)
				    .detail("ReplyVersion", reply.version);
				co_await delay(0.25);
				continue;
			}
			// Older proxies can decode a damaged versionstamp as a future commit. A proxy can also
			// legitimately be ahead of the controller, so use a database read version as the upper bound.
			if (reply.version > cx->getCachedReadVersion()) {
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
			}
			erase();
			for (const auto& kv : reply.result) {
				auto entry = decode(kv.key.removePrefix(globalConfigKeysPrefix), kv.value);
				if (entry.second.isValid()) {
					insert(std::move(entry));
				}
			}
			co_return reply.version;
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
					if (lastUpdate < dbInfo->history.front().version || !applyHistory(dbInfo->history)) {
						lastUpdate = co_await refresh(lastUpdate, dbInfo->history.back().version);
						// DBInfo could have changed after the wait. If
						// changes are present, re-run the loop to make
						// sure they are applied.
						if (!dbInfo->history.empty() &&
						    dbInfo->history[0].version != std::numeric_limits<Version>::max()) {
							continue;
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

struct GlobalConfigRefreshTest {
	GrvProxyInterface proxy;
	Reference<AsyncVar<ClientDBInfo>> clientInfo;
	Database db;

	GlobalConfigRefreshTest() {
		proxy.provisional = false;
		proxy.initEndpoints();
		IFailureMonitor::failureMonitor().setStatus(proxy.address(), FailureStatus(false));
		CommitProxyInterface commitProxy;
		commitProxy.provisional = false;
		commitProxy.initEndpoints();
		ClientDBInfo info;
		info.id = UID(1, 1);
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

	Future<Void> publish(Version version) {
		auto changed = db->globalConfig->onChange();
		auto info = clientInfo->get();
		info.id = deterministicRandom()->randomUniqueID();
		info.history = { VersionHistory(version) };
		clientInfo->set(info);
		return changed;
	}
};

RangeResult snapshotWithSetting(KeyRef key) {
	RangeResult rows;
	rows.push_back_deep(rows.arena(),
	                    KeyValueRef(key.withPrefix(globalConfigKeysPrefix), Tuple::makeTuple(true).pack()));
	return rows;
}

} // namespace

TEST_CASE("/fdbclient/GlobalConfig/RejectsInvalidInitialSnapshot") {
	int callbacks = 0;
	GlobalConfigRefreshTest test;
	auto grvs = test.serveReadVersions(50);
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
	rows.more = false;
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 0, rows });
	co_await timeoutError(initialized, 5.0);
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	ASSERT_EQ(callbacks, 1);
}

TEST_CASE("/fdbclient/GlobalConfig/CancelDuringSnapshotVersionCheck") {
	int callbacks = 0;
	GlobalConfigRefreshTest test;
	test.db->globalConfig->trigger("A"_sr, [&](std::optional<std::any>) { ++callbacks; });
	auto initialized = test.db->globalConfig->onInitialized();
	auto snapshot = co_await test.nextRequest();
	auto rows = snapshotWithSetting("A"_sr);
	snapshot.reply.send(GlobalConfigRefreshReply{ rows.arena(), 41, rows });
	auto grv = co_await timeoutError(waitAndForward(test.proxy.getConsistentReadVersion.getFuture()), 5.0);
	ASSERT(!initialized.isReady());
	test.db = Database();
	ASSERT(initialized.isError());
	GetReadVersionReply reply;
	reply.version = 50;
	grv.reply.send(reply);
	co_await delay(0);
	ASSERT_EQ(callbacks, 0);
}

TEST_CASE("/fdbclient/GlobalConfig/RejectsStaleRefreshSnapshot") {
	const KeyRef tail = "\xff"_sr;
	int callbacks = 0;
	GlobalConfigRefreshTest test;
	auto grvs = test.serveReadVersions(50);
	auto initial = co_await test.nextRequest();
	auto rows = snapshotWithSetting(tail);
	initial.reply.send(GlobalConfigRefreshReply{ rows.arena(), 41, rows });
	co_await timeoutError(test.db->globalConfig->onInitialized(), 5.0);
	test.db->globalConfig->trigger(tail, [&](std::optional<std::any>) { ++callbacks; });
	auto changed = test.publish(44);
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
	request.reply.send(GlobalConfigRefreshReply{ firstPage.arena(), 44, firstPage });
	co_await timeoutError(changed, 5.0);
	ASSERT(test.db->globalConfig->get<bool>(tail, false));
	ASSERT(test.db->globalConfig->get<bool>("A"_sr, false));
	ASSERT_EQ(callbacks, 2);

	changed = test.publish(45);
	request = co_await test.nextRequest();
	ASSERT_EQ(request.lastKnown, 44);
	request.reply.send(GlobalConfigRefreshReply{ rows.arena(), 46, rows });
	co_await timeoutError(changed, 5.0);
	ASSERT(!test.db->globalConfig->get("A"_sr).isValid());

	changed = test.publish(MAX_VERSION);
	request = co_await test.nextRequest();
	ASSERT_EQ(request.lastKnown, 46);
	RangeResult empty;
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 45, empty });
	auto retry = co_await race(test.nextRequest(), changed);
	ASSERT(test.db->globalConfig->get<bool>(tail, false));
	ASSERT_EQ(retry.index(), 0);
	request = std::get<0>(retry);
	ASSERT_EQ(request.lastKnown, 46);
	request.reply.send(GlobalConfigRefreshReply{ empty.arena(), 46, empty });
	co_await timeoutError(changed, 5.0);
	ASSERT(!test.db->globalConfig->get(tail).isValid());
}
