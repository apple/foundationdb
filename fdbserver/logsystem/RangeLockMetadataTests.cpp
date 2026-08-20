/*
 * RangeLockMetadataTests.cpp
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

#include "fdbclient/RangeLockConfiguration.h"
#include "fdbserver/logsystem/ApplyMetadataMutation.h"
#include "flow/UnitTest.h"

namespace {

class RangeLockMetadataTestStore final : public IKeyValueStore {
public:
	~RangeLockMetadataTestStore() override = default;
	Future<Void> getError() const override { return Never(); }
	Future<Void> onClosed() const override { return Void(); }
	void dispose() override { rows_.clear(); }
	void close() override {}
	KeyValueStoreType getType() const override { return KeyValueStoreType::MEMORY; }
	StorageBytes getStorageBytes() const override { return StorageBytes(0, 0, 0, 0); }
	void set(KeyValueRef row, const Arena* = nullptr) override {
		rows_.insert_or_assign(Key(row.key), Value(row.value));
	}
	void clear(KeyRangeRef range, const Arena* = nullptr) override {
		rows_.erase(rows_.lower_bound(range.begin), rows_.lower_bound(range.end));
	}
	Future<Void> commit(bool = false) override { return Void(); }
	Future<Optional<Value>> readValue(KeyRef key, Optional<ReadOptions> = Optional<ReadOptions>()) override {
		auto row = rows_.find(key);
		return row == rows_.end() ? Optional<Value>() : Optional<Value>(row->second);
	}
	Future<Optional<Value>> readValuePrefix(KeyRef key, int length, Optional<ReadOptions> options) override {
		Optional<Value> value = readValue(key, options).get();
		if (value.present() && value.get().size() > length) {
			return Optional<Value>(Value(value.get().substr(0, length)));
		}
		return value;
	}
	Future<RangeResult> readRange(KeyRangeRef range,
	                              int rowLimit,
	                              int byteLimit,
	                              Optional<ReadOptions> = Optional<ReadOptions>()) override {
		ASSERT(rowLimit >= 0);
		RangeResult result;
		for (auto row = rows_.lower_bound(range.begin);
		     row != rows_.end() && row->first < range.end && result.size() < rowLimit && byteLimit > 0;
		     ++row) {
			result.push_back_deep(result.arena(), KeyValueRef(row->first, row->second));
			byteLimit -= row->first.size() + row->second.size();
		}
		return result;
	}
	std::map<Key, Value> rows(const KeyRangeRef& range) const {
		return std::map<Key, Value>(rows_.lower_bound(range.begin), rows_.lower_bound(range.end));
	}
	void applyStorageMutations(const VectorRef<MutationRef>& mutations) {
		for (const auto& mutation : mutations) {
			if (mutation.type == MutationRef::SetValue) {
				set(KeyValueRef(mutation.param1, mutation.param2));
			} else {
				ASSERT_EQ(mutation.type, MutationRef::ClearRange);
				clear(KeyRangeRef(mutation.param1, mutation.param2));
			}
		}
	}

private:
	std::map<Key, Value> rows_;
};

class RangeLockMetadataTestView final : public ApplyMetadataRangeLock {
public:
	void setBoundary(const KeyRef& key, const ValueRef& value) override {
		rows_.set(KeyValueRef(key.withPrefix(rangeLockPrefix), value));
	}
	void clearBoundaries(const KeyRangeRef& range) override { rows_.clear(range & rangeLockKeys); }
	void resetBoundaries() override { rows_.clear(rangeLockKeys); }
	void setConfiguration(const RangeLockConfiguration& configuration) override { configuration_ = configuration; }
	const RangeLockConfiguration& configuration() const { return configuration_; }
	std::map<Key, Value> rows() const { return rows_.rows(rangeLockKeys); }

private:
	RangeLockMetadataTestStore rows_;
	RangeLockConfiguration configuration_;
};

Value testRangeLockValue(const KeyRange& range, const std::string& owner) {
	RangeLockStateSet locks;
	locks.insertIfNotExist(RangeLockState(RangeLockType::ExclusiveReadLock, owner, range));
	return rangeLockStateSetValue(locks);
}

} // namespace

TEST_CASE("/fdbserver/logsystem/RangeLock/DurableReconciliation") {
	RangeLockMetadataTestStore storage;
	RangeLockMetadataTestStore bareStore;
	RangeLockMetadataTestStore resolverStore;
	RangeLockMetadataTestStore proxyStore;
	RangeLockMetadataTestView proxyView;
	const UID owner(1, 2);
	const Value heldA = testRangeLockValue(KeyRangeRef("a"_sr, "d"_sr), "source-a");
	const Value heldM = testRangeLockValue(KeyRangeRef("m"_sr, normalKeys.end), "source-m");
	const Value stale = testRangeLockValue(KeyRangeRef("b"_sr, "c"_sr), "stale");
	const Value databaseLock = BinaryWriter::toValue(owner, Unversioned()).withPrefix("0123456789"_sr);
	Arena arena;
	CommitTransactionRef source;
	source.set(arena, normalKeys.begin.withPrefix(rangeLockPrefix), ""_sr);
	source.set(arena, "a"_sr.withPrefix(rangeLockPrefix), heldA);
	source.set(arena, "b"_sr.withPrefix(rangeLockPrefix), heldA); // Explicit equal-valued boundary.
	source.set(arena, "d"_sr.withPrefix(rangeLockPrefix), ""_sr);
	source.set(arena, "m"_sr.withPrefix(rangeLockPrefix), heldM);
	source.set(arena, normalKeys.end.withPrefix(rangeLockPrefix), ""_sr);
	source.set(arena, databaseLockedKey, databaseLock);
	storage.applyStorageMutations(source.mutations);
	const auto originalSource = storage.rows(rangeLockKeys);

	// Old disabled proxies could omit storage rows and retain boundaries
	// removed from storage. Seed both forms of drift without a runtime hook.
	for (auto store : { &bareStore, &resolverStore, &proxyStore }) {
		store->set(KeyValueRef("b"_sr.withPrefix(rangeLockPrefix), stale));
		store->set(KeyValueRef("c"_sr.withPrefix(rangeLockPrefix), ""_sr));
		store->set(KeyValueRef(databaseLockedKey, databaseLock));
	}
	proxyView.setBoundary("b"_sr, stale);
	proxyView.setBoundary("c"_sr, ""_sr);

	auto apply = [&](const CommitTransactionRef& transaction) {
		bool changed = false;
		applyMetadataMutations(SpanContext(), UID(), arena, transaction.mutations, &bareStore);
		ResolverData resolver(
		    UID(), Reference<LogSystemConsumer>(), &resolverStore, nullptr, nullptr, changed, 10, nullptr, nullptr);
		applyMetadataMutations(SpanContext(), resolver, transaction.mutations);
		ApplyMetadataProxyContext proxy;
		proxy.txnStateStore = &proxyStore;
		proxy.rangeLock = &proxyView;
		applyMetadataMutations(SpanContext(),
		                       proxy,
		                       arena,
		                       Reference<LogSystemConsumer>(),
		                       transaction.mutations,
		                       nullptr,
		                       changed,
		                       10,
		                       11,
		                       false,
		                       false);
		storage.applyStorageMutations(transaction.mutations);
	};

	const auto beginConfiguration = RangeLockConfiguration::migrating(owner, normalKeys.begin);
	CommitTransactionRef begin;
	begin.set(arena, rangeLockConfigurationKey, rangeLockConfigurationValue(beginConfiguration));
	ASSERT(containsMetadataMutation(begin.mutations));
	apply(begin);
	ASSERT(storage.rows(rangeLockKeys) == originalSource);
	ASSERT(bareStore.rows(rangeLockKeys).empty());
	ASSERT(resolverStore.rows(rangeLockKeys).empty());
	ASSERT(proxyStore.rows(rangeLockKeys).empty());
	ASSERT(proxyView.rows().empty());
	ASSERT(proxyView.configuration() == beginConfiguration);

	CommitTransactionRef firstPage;
	firstPage.clear(arena, KeyRangeRef(rangeLockPrefix, "d"_sr.withPrefix(rangeLockPrefix)));
	for (const auto& [key, value] : originalSource) {
		if (key <= "d"_sr.withPrefix(rangeLockPrefix)) {
			firstPage.set(arena, key, value);
		}
	}
	const auto middle = beginConfiguration.advance("d"_sr);
	firstPage.set(arena, rangeLockConfigurationKey, rangeLockConfigurationValue(middle));
	apply(firstPage);
	ASSERT(storage.rows(rangeLockKeys) == originalSource);

	// Recruitment hydrates the already recovered transaction-state contents.
	// It must not execute a reset again or discard an unfinished cursor.
	const auto partialRows = proxyStore.rows(rangeLockKeys);
	CommitTransactionRef recoveredRows;
	for (const auto& [key, value] : proxyStore.rows(allKeys)) {
		recoveredRows.set(arena, key, value);
	}
	RangeLockMetadataTestView recoveredView;
	ApplyMetadataProxyContext recoveredProxy;
	recoveredProxy.txnStateStore = &proxyStore;
	recoveredProxy.rangeLock = &recoveredView;
	bool changed = false;
	applyMetadataMutations(SpanContext(),
	                       recoveredProxy,
	                       arena,
	                       Reference<LogSystemConsumer>(),
	                       recoveredRows.mutations,
	                       nullptr,
	                       changed,
	                       0,
	                       0,
	                       true,
	                       false);
	ResolverData recoveredResolver(UID(), &resolverStore, nullptr, changed);
	applyMetadataMutations(SpanContext(), recoveredResolver, recoveredRows.mutations);
	ASSERT(proxyStore.rows(rangeLockKeys) == partialRows);
	ASSERT(resolverStore.rows(rangeLockKeys) == partialRows);
	ASSERT(recoveredView.rows() == partialRows);
	ASSERT(recoveredView.configuration() == middle);

	CommitTransactionRef lastPage;
	lastPage.clear(arena, KeyRangeRef("d"_sr.withPrefix(rangeLockPrefix), normalKeys.end.withPrefix(rangeLockPrefix)));
	for (const auto& [key, value] : originalSource) {
		if (key >= "d"_sr.withPrefix(rangeLockPrefix)) {
			lastPage.set(arena, key, value);
		}
	}
	lastPage.set(arena, rangeLockConfigurationKey, rangeLockConfigurationValue(middle.advance(normalKeys.end)));
	apply(lastPage);
	ASSERT(storage.rows(rangeLockKeys) == originalSource);
	ASSERT(bareStore.rows(rangeLockKeys) == originalSource);
	ASSERT(resolverStore.rows(rangeLockKeys) == originalSource);
	ASSERT(proxyStore.rows(rangeLockKeys) == originalSource);
	ASSERT(proxyView.rows() == originalSource);

	CommitTransactionRef finish;
	const auto ready = RangeLockConfiguration::ready(owner);
	finish.set(arena, rangeLockConfigurationKey, rangeLockConfigurationValue(ready));
	finish.clear(arena, singleKeyRange(databaseLockedKey));
	apply(finish);
	for (auto store : { &storage, &bareStore, &resolverStore, &proxyStore }) {
		ASSERT(!store->readValue(databaseLockedKey).get().present());
		ASSERT(decodeRangeLockConfiguration(store->readValue(rangeLockConfigurationKey).get().get()) == ready);
		ASSERT(store->rows(rangeLockKeys) == originalSource);
	}
	ASSERT(proxyView.configuration() == ready);
	co_return;
}
