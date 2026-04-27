/*
 * ApplyMetadataMutation.cpp
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

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/KeyBackedTypes.actor.h" // for key backed map codecs for tss mapping
#include "fdbclient/MetaclusterRegistration.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/Notified.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "fdbserver/AccumulativeChecksumUtil.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbclient/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogProtocolMessage.h"
#include "fdbserver/LogSystem.h"
#include "flow/Error.h"
#include "flow/Trace.h"

Reference<StorageInfo> getStorageInfo(UID id,
                                      std::map<UID, Reference<StorageInfo>>* storageCache,
                                      IKeyValueStore* txnStateStore) {
	Reference<StorageInfo> storageInfo;
	auto cacheItr = storageCache->find(id);
	if (cacheItr == storageCache->end()) {
		storageInfo = makeReference<StorageInfo>();
		storageInfo->tag = decodeServerTagValue(txnStateStore->readValue(serverTagKeyFor(id)).get().get());
		storageInfo->interf = decodeServerListValue(txnStateStore->readValue(serverListKeyFor(id)).get().get());
		(*storageCache)[id] = storageInfo;
	} else {
		storageInfo = cacheItr->second;
	}
	return storageInfo;
}
namespace {

// It is incredibly important that any modifications to txnStateStore are done in such a way that the same operations
// will be done on all commit proxies at the same time. Otherwise, the data stored in txnStateStore will become
// corrupted.
class ApplyMetadataMutationsImpl {

public:
	ApplyMetadataMutationsImpl(const SpanContext& spanContext_,
	                           const UID& dbgid_,
	                           Arena& arena_,
	                           const VectorRef<MutationRef>& mutations_,
	                           IKeyValueStore* txnStateStore_)
	  : spanContext(spanContext_), dbgid(dbgid_), arena(arena_), mutations(mutations_), txnStateStore(txnStateStore_),
	    confChange(dummyConfChange), encryptMode(EncryptionAtRestMode::DISABLED) {}

	ApplyMetadataMutationsImpl(const SpanContext& spanContext_,
	                           Arena& arena_,
	                           const VectorRef<MutationRef>& mutations_,
	                           ProxyCommitData& proxyCommitData_,
	                           Reference<ILogSystem> logSystem_,
	                           LogPushData* toCommit_,
	                           const std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>* cipherKeys_,
	                           EncryptionAtRestMode encryptMode,
	                           bool& confChange_,
	                           Version version,
	                           Version popVersion_,
	                           bool initialCommit_,
	                           bool provisionalCommitProxy_)
	  : spanContext(spanContext_), dbgid(proxyCommitData_.dbgid), arena(arena_), mutations(mutations_),
	    txnStateStore(proxyCommitData_.txnStateStore), toCommit(toCommit_), cipherKeys(cipherKeys_),
	    encryptMode(encryptMode), confChange(confChange_), logSystem(logSystem_), version(version),
	    popVersion(popVersion_), vecBackupKeys(&proxyCommitData_.vecBackupKeys), keyInfo(&proxyCommitData_.keyInfo),
	    cacheInfo(&proxyCommitData_.cacheInfo),
	    uid_applyMutationsData(proxyCommitData_.firstProxy ? &proxyCommitData_.uid_applyMutationsData : nullptr),
	    commit(proxyCommitData_.commit), cx(proxyCommitData_.cx), committedVersion(&proxyCommitData_.committedVersion),
	    storageCache(&proxyCommitData_.storageCache), tag_popped(&proxyCommitData_.tag_popped),
	    tssMapping(&proxyCommitData_.tssMapping), tenantMap(&proxyCommitData_.tenantMap),
	    tenantNameIndex(&proxyCommitData_.tenantNameIndex), lockedTenants(&proxyCommitData_.lockedTenants),
	    initialCommit(initialCommit_), provisionalCommitProxy(provisionalCommitProxy_),
	    accumulativeChecksumIndex(getCommitProxyAccumulativeChecksumIndex(proxyCommitData_.commitProxyIndex)) {
		if (encryptMode.isEncryptionEnabled()) {
			ASSERT(cipherKeys != nullptr);
			ASSERT(cipherKeys->count(SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID) > 0);
			if (FLOW_KNOBS->ENCRYPT_HEADER_AUTH_TOKEN_ENABLED) {
				ASSERT(cipherKeys->count(ENCRYPT_HEADER_DOMAIN_ID));
			}
		}
	}

	ApplyMetadataMutationsImpl(const SpanContext& spanContext_,
	                           ResolverData& resolverData_,
	                           const VectorRef<MutationRef>& mutations_,
	                           const std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>* cipherKeys_,
	                           EncryptionAtRestMode encryptMode)
	  : spanContext(spanContext_), dbgid(resolverData_.dbgid), arena(resolverData_.arena), mutations(mutations_),
	    cipherKeys(cipherKeys_), encryptMode(encryptMode), txnStateStore(resolverData_.txnStateStore),
	    toCommit(resolverData_.toCommit), confChange(resolverData_.confChanges), logSystem(resolverData_.logSystem),
	    popVersion(resolverData_.popVersion), keyInfo(resolverData_.keyInfo), storageCache(resolverData_.storageCache),
	    initialCommit(resolverData_.initialCommit), forResolver(true),
	    accumulativeChecksumIndex(resolverAccumulativeChecksumIndex) {
		if (encryptMode.isEncryptionEnabled()) {
			ASSERT(cipherKeys != nullptr);
			ASSERT(cipherKeys->count(SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID) > 0);
			if (FLOW_KNOBS->ENCRYPT_HEADER_AUTH_TOKEN_ENABLED) {
				ASSERT(cipherKeys->count(ENCRYPT_HEADER_DOMAIN_ID));
			}
		}
	}

private:
	// The following variables are incoming parameters

	const SpanContext& spanContext;

	const UID& dbgid;

	Arena& arena;

	const VectorRef<MutationRef>& mutations;

	// Transaction KV store
	IKeyValueStore* txnStateStore;

	// non-null if these mutations were part of a new commit handled by this commit proxy
	LogPushData* toCommit = nullptr;

	// Cipher keys used to encrypt to be committed mutations
	const std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>* cipherKeys = nullptr;

	// Flag indicates if the configure is changed
	bool& confChange;

	Reference<ILogSystem> logSystem = Reference<ILogSystem>();
	Version version = invalidVersion;
	Version popVersion = 0;
	KeyRangeMap<std::set<Key>>* vecBackupKeys = nullptr;
	KeyRangeMap<ServerCacheInfo>* keyInfo = nullptr;
	KeyRangeMap<bool>* cacheInfo = nullptr;
	std::map<Key, ApplyMutationsData>* uid_applyMutationsData = nullptr;
	PublicRequestStream<CommitTransactionRequest> commit = PublicRequestStream<CommitTransactionRequest>();
	Database cx = Database();
	NotifiedVersion* committedVersion = nullptr;
	std::map<UID, Reference<StorageInfo>>* storageCache = nullptr;
	std::map<Tag, Version>* tag_popped = nullptr;
	std::unordered_map<UID, StorageServerInterface>* tssMapping = nullptr;

	std::map<int64_t, TenantName>* tenantMap = nullptr;
	std::unordered_map<TenantName, int64_t>* tenantNameIndex = nullptr;
	std::set<int64_t>* lockedTenants = nullptr;
	EncryptionAtRestMode encryptMode;

	// true if the mutations were already written to the txnStateStore as part of recovery
	bool initialCommit = false;

	// true if called from Resolver
	bool forResolver = false;

	// true if called from a provisional commit proxy
	bool provisionalCommitProxy = false;

	// indicate which commit proxy / resolver applies mutations
	uint16_t accumulativeChecksumIndex = invalidAccumulativeChecksumIndex;

private:
	// The following variables are used internally

	std::map<KeyRef, MutationRef> cachedRangeInfo;

	// Testing Storage Server removal (clearing serverTagKey) needs to read tss server list value to determine it is a
	// tss + find partner's tag to send the private mutation. Since the removeStorageServer transaction clears both the
	// storage list and server tag, we have to enforce ordering, proccessing the server tag first, and postpone the
	// server list clear until the end;
	std::vector<KeyRangeRef> tssServerListToRemove;

	// Similar to tssServerListToRemove, the TSS mapping change key needs to read the server list at the end of the
	// commit
	std::vector<std::pair<UID, UID>> tssMappingToAdd;

private:
	bool dummyConfChange = false;

private:
	void writeMutation(const MutationRef& m) {
		if (!encryptMode.isEncryptionEnabled()) {
			toCommit->writeTypedMessage(m);
		} else {
			ASSERT(cipherKeys != nullptr);
			Arena arena;
			CODE_PROBE(!forResolver, "encrypting metadata mutations");
			CODE_PROBE(forResolver, "encrypting resolver mutations");
			toCommit->writeTypedMessage(m.encryptMetadata(*cipherKeys, arena, BlobCipherMetrics::TLOG));
		}
	}

	void checkSetKeyServersPrefix(MutationRef m) {
		if (!m.param1.startsWith(keyServersPrefix)) {
			return;
		}

		if (!initialCommit)
			txnStateStore->set(KeyValueRef(m.param1, m.param2));

		if (!keyInfo) {
			return;
		}
		KeyRef k = m.param1.removePrefix(keyServersPrefix);
		if (k == allKeys.end) {
			return;
		}

		KeyRef end = keyInfo->rangeContaining(k).end();
		KeyRangeRef insertRange(k, end);
		std::vector<UID> src, dest;
		// txnStateStore is always an in-memory KVS, and must always be recovered before
		// applyMetadataMutations is called, so a wait here should never be needed.
		Future<RangeResult> fResult = txnStateStore->readRange(serverTagKeys);
		decodeKeyServersValue(fResult.get(), m.param2, src, dest);

		ASSERT(storageCache);
		ServerCacheInfo info;
		info.tags.reserve(src.size() + dest.size());
		info.src_info.reserve(src.size());
		info.dest_info.reserve(dest.size());

		for (const auto& id : src) {
			auto storageInfo = getStorageInfo(id, storageCache, txnStateStore);
			ASSERT(!storageInfo->interf.isTss());
			ASSERT(storageInfo->tag != invalidTag);
			info.tags.push_back(storageInfo->tag);
			info.src_info.push_back(storageInfo);
		}
		for (const auto& id : dest) {
			auto storageInfo = getStorageInfo(id, storageCache, txnStateStore);
			ASSERT(!storageInfo->interf.isTss());
			ASSERT(storageInfo->tag != invalidTag);
			info.tags.push_back(storageInfo->tag);
			info.dest_info.push_back(storageInfo);
		}
		uniquify(info.tags);
		keyInfo->insert(insertRange, info);
		if (toCommit && SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
			toCommit->setShardChanged();
		}
	}

	void checkSetServerKeysPrefix(MutationRef m) {
		if (!m.param1.startsWith(serverKeysPrefix)) {
			return;
		}

		if (toCommit) {
			Tag tag = decodeServerTagValue(
			    txnStateStore->readValue(serverTagKeyFor(serverKeysDecodeServer(m.param1))).get().get());
			MutationRef privatized = m;
			privatized.clearChecksumAndAccumulativeIndex();
			privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
			privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
			TraceEvent(SevDebug, "SendingPrivateMutation", dbgid)
			    .detail("Original", m)
			    .detail("Privatized", privatized)
			    .detail("Server", serverKeysDecodeServer(m.param1))
			    .detail("TagKey", serverTagKeyFor(serverKeysDecodeServer(m.param1)))
			    .detail("Tag", tag.toString());

			toCommit->addTag(tag);
			writeMutation(privatized);
		}
	}

	void checkSetServerTagsPrefix(MutationRef m) {
		if (!m.param1.startsWith(serverTagPrefix)) {
			return;
		}

		UID id = decodeServerTagKey(m.param1);
		Tag tag = decodeServerTagValue(m.param2);

		if (toCommit) {
			MutationRef privatized = m;
			privatized.clearChecksumAndAccumulativeIndex();
			privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
			privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
			TraceEvent("ServerTag", dbgid).detail("Server", id).detail("Tag", tag.toString());

			TraceEvent(SevDebug, "SendingPrivatized_ServerTag", dbgid).detail("M", "LogProtocolMessage");
			toCommit->addTag(tag);
			toCommit->writeTypedMessage(LogProtocolMessage(), true);
			TraceEvent(SevDebug, "SendingPrivatized_ServerTag", dbgid).detail("M", privatized);
			toCommit->addTag(tag);
			writeMutation(privatized);
		}
		if (!initialCommit) {
			txnStateStore->set(KeyValueRef(m.param1, m.param2));
			if (storageCache) {
				auto cacheItr = storageCache->find(id);
				if (cacheItr == storageCache->end()) {
					Reference<StorageInfo> storageInfo = makeReference<StorageInfo>();
					storageInfo->tag = tag;
					Optional<Key> interfKey = txnStateStore->readValue(serverListKeyFor(id)).get();
					if (interfKey.present()) {
						storageInfo->interf = decodeServerListValue(interfKey.get());
					}
					(*storageCache)[id] = storageInfo;
				} else {
					cacheItr->second->tag = tag;
					// These tag vectors will be repopulated by the proxy when it detects their sizes are 0.
					for (auto& it : keyInfo->ranges()) {
						it.value().tags.clear();
					}
				}
			}
		}
	}

	void checkSetStorageCachePrefix(MutationRef m) {
		if (!m.param1.startsWith(storageCachePrefix))
			return;
		if (cacheInfo || forResolver) {
			KeyRef k = m.param1.removePrefix(storageCachePrefix);

			// Create a private mutation for storage servers
			// This is done to make the storage servers aware of the cached key-ranges
			if (toCommit) {
				MutationRef privatized = m;
				privatized.clearChecksumAndAccumulativeIndex();
				privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
				privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
				//TraceEvent(SevDebug, "SendingPrivateMutation", dbgid).detail("Original", m.toString()).detail("Privatized", privatized.toString());
				cachedRangeInfo[k] = privatized;
			}
			if (cacheInfo && k != allKeys.end) {
				KeyRef end = cacheInfo->rangeContaining(k).end();
				std::vector<uint16_t> serverIndices;
				decodeStorageCacheValue(m.param2, serverIndices);
				cacheInfo->insert(KeyRangeRef(k, end), serverIndices.size() > 0);
			}
		}
		if (!initialCommit)
			txnStateStore->set(KeyValueRef(m.param1, m.param2));
	}

	void checkSetCacheKeysPrefix(MutationRef m) {
		if (!m.param1.startsWith(cacheKeysPrefix) || toCommit == nullptr) {
			return;
		}
		// Create a private mutation for cache servers
		// This is done to make the cache servers aware of the cached key-ranges
		MutationRef privatized = m;
		privatized.clearChecksumAndAccumulativeIndex();
		privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
		privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
		TraceEvent(SevDebug, "SendingPrivatized_CacheTag", dbgid).detail("M", privatized);
		toCommit->addTag(cacheTag);
		writeMutation(privatized);
	}

	void checkSetConfigKeys(MutationRef m) {
		if (!m.param1.startsWith(configKeysPrefix) && m.param1 != coordinatorsKey &&
		    m.param1 != previousCoordinatorsKey) {
			return;
		}
		if (Optional<StringRef>(m.param2) !=
		    txnStateStore->readValue(m.param1)
		        .get()
		        .castTo<StringRef>()) { // FIXME: Make this check more specific, here or by reading
			                            // configuration whenever there is a change
			if ((!m.param1.startsWith(excludedServersPrefix) && m.param1 != excludedServersVersionKey) &&
			    (!m.param1.startsWith(failedServersPrefix) && m.param1 != failedServersVersionKey) &&
			    (!m.param1.startsWith(excludedLocalityPrefix) && m.param1 != excludedLocalityVersionKey) &&
			    (!m.param1.startsWith(failedLocalityPrefix) && m.param1 != failedLocalityVersionKey)) {
				auto t = txnStateStore->readValue(m.param1).get();
				TraceEvent("MutationRequiresRestart", dbgid)
				    .detail("M", m)
				    .detail("PrevValue", t.orDefault("(none)"_sr))
				    .detail("ToCommit", toCommit != nullptr)
				    .detail("InitialCommit", initialCommit);
				confChange = true;
				if (m.param1 == configUsableRegionsKey && m.param2 == "2"_sr && toCommit) {
					TraceEvent("MutationRequiresRestartEnableHA", dbgid);
					toCommit->setChangedToHA();
				}
			}
		}
		if (!initialCommit)
			txnStateStore->set(KeyValueRef(m.param1, m.param2));
	}

	void checkSetChangeFeedPrefix(MutationRef m) {
		if (!m.param1.startsWith(changeFeedPrefix)) {
			return;
		}
		if (toCommit && keyInfo) {
			KeyRange r = std::get<0>(decodeChangeFeedValue(m.param2));
			MutationRef privatized = m;
			privatized.clearChecksumAndAccumulativeIndex();
			privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
			privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
			auto ranges = keyInfo->intersectingRanges(r);
			auto firstRange = ranges.begin();
			++firstRange;
			if (firstRange == ranges.end()) {
				ranges.begin().value().populateTags();
				toCommit->addTags(ranges.begin().value().tags);
			} else {
				std::set<Tag> allSources;
				for (auto r : ranges) {
					r.value().populateTags();
					allSources.insert(r.value().tags.begin(), r.value().tags.end());
				}
				toCommit->addTags(allSources);
			}
			TraceEvent(SevDebug, "SendingPrivatized_ChangeFeed", dbgid).detail("M", privatized);
			writeMutation(privatized);
		}
	}

	void checkSetServerListPrefix(MutationRef m) {
		if (!m.param1.startsWith(serverListPrefix)) {
			return;
		}
		if (!initialCommit) {
			txnStateStore->set(KeyValueRef(m.param1, m.param2));
			if (storageCache) {
				UID id = decodeServerListKey(m.param1);
				StorageServerInterface interf = decodeServerListValue(m.param2);

				auto cacheItr = storageCache->find(id);
				if (cacheItr == storageCache->end()) {
					Reference<StorageInfo> storageInfo = makeReference<StorageInfo>();
					storageInfo->interf = interf;
					Optional<Key> tagKey = txnStateStore->readValue(serverTagKeyFor(id)).get();
					if (tagKey.present()) {
						storageInfo->tag = decodeServerTagValue(tagKey.get());
					}
					(*storageCache)[id] = storageInfo;
				} else {
					cacheItr->second->interf = interf;
				}
			}
		}
	}

	void checkSetTSSMappingKeys(MutationRef m) {
		if (!m.param1.startsWith(tssMappingKeys.begin)) {
			return;
		}

		// Normally uses key backed map, so have to use same unpacking code here.
		UID ssId = TupleCodec<UID>::unpack(m.param1.removePrefix(tssMappingKeys.begin));
		UID tssId = TupleCodec<UID>::unpack(m.param2);
		if (!initialCommit) {
			txnStateStore->set(KeyValueRef(m.param1, m.param2));
		}
		if (tssMapping) {
			tssMappingToAdd.push_back(std::pair(ssId, tssId));
		}

		if (toCommit) {
			// send private mutation to SS that it now has a TSS pair
			MutationRef privatized = m;
			privatized.clearChecksumAndAccumulativeIndex();
			privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
			privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);

			Optional<Value> tagV = txnStateStore->readValue(serverTagKeyFor(ssId)).get();
			if (tagV.present()) {
				TraceEvent(SevDebug, "SendingPrivatized_TSSID", dbgid).detail("M", privatized);
				toCommit->addTag(decodeServerTagValue(tagV.get()));
				writeMutation(privatized);
			}
		}
	}

	void checkSetTSSQuarantineKeys(MutationRef m) {
		if (!m.param1.startsWith(tssQuarantineKeys.begin) || initialCommit) {
			return;
		}
		txnStateStore->set(KeyValueRef(m.param1, m.param2));

		if (!toCommit) {
			return;
		}
		UID tssId = decodeTssQuarantineKey(m.param1);
		Optional<Value> ssiV = txnStateStore->readValue(serverListKeyFor(tssId)).get();
		if (!ssiV.present()) {
			return;
		}
		StorageServerInterface ssi = decodeServerListValue(ssiV.get());
		if (!ssi.isTss()) {
			return;
		}
		Optional<Value> tagV = txnStateStore->readValue(serverTagKeyFor(ssi.tssPairID.get())).get();
		if (tagV.present()) {
			MutationRef privatized = m;
			privatized.clearChecksumAndAccumulativeIndex();
			privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
			privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
			TraceEvent(SevDebug, "SendingPrivatized_TSSQuarantine", dbgid).detail("M", privatized);
			toCommit->addTag(decodeServerTagValue(tagV.get()));
			writeMutation(privatized);
		}
	}

	void checkSetApplyMutationsEndRange(MutationRef m) {
		if (!m.param1.startsWith(applyMutationsEndRange.begin)) {
			return;
		}

		if (!initialCommit)
			txnStateStore->set(KeyValueRef(m.param1, m.param2));

		if (uid_applyMutationsData == nullptr) {
			return;
		}

		Key uid = m.param1.removePrefix(applyMutationsEndRange.begin);
		auto& p = (*uid_applyMutationsData)[uid];
		p.endVersion = BinaryReader::fromStringRef<Version>(m.param2, Unversioned());
		if (p.keyVersion == Reference<KeyRangeMap<Version>>())
			p.keyVersion = makeReference<KeyRangeMap<Version>>();
		if (p.worker.isValid() && !p.worker.isReady()) {
			return;
		}
		auto addPrefixValue = txnStateStore->readValue(uid.withPrefix(applyMutationsAddPrefixRange.begin)).get();
		auto removePrefixValue = txnStateStore->readValue(uid.withPrefix(applyMutationsRemovePrefixRange.begin)).get();
		auto beginValue = txnStateStore->readValue(uid.withPrefix(applyMutationsBeginRange.begin)).get();
		p.worker = applyMutations(
		    cx,
		    uid,
		    addPrefixValue.present() ? addPrefixValue.get() : Key(),
		    removePrefixValue.present() ? removePrefixValue.get() : Key(),
		    beginValue.present() ? BinaryReader::fromStringRef<Version>(beginValue.get(), Unversioned()) : 0,
		    &p.endVersion,
		    commit,
		    committedVersion,
		    p.keyVersion,
		    tenantMap,
		    provisionalCommitProxy);
	}

	void checkSetApplyMutationsKeyVersionMapRange(MutationRef m) {
		if (!m.param1.startsWith(applyMutationsKeyVersionMapRange.begin)) {
			return;
		}
		if (!initialCommit)
			txnStateStore->set(KeyValueRef(m.param1, m.param2));

		if (uid_applyMutationsData == nullptr) {
			return;
		}
		if (m.param1.size() >= applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID)) {
			Key uid = m.param1.substr(applyMutationsKeyVersionMapRange.begin.size(), sizeof(UID));
			Key k = m.param1.substr(applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID));
			auto& p = (*uid_applyMutationsData)[uid];
			if (p.keyVersion == Reference<KeyRangeMap<Version>>())
				p.keyVersion = makeReference<KeyRangeMap<Version>>();
			p.keyVersion->rawInsert(k, BinaryReader::fromStringRef<Version>(m.param2, Unversioned()));
		}
	}

	void checkSetLogRangesRange(MutationRef m) {
		if (!m.param1.startsWith(logRangesRange.begin)) {
			return;
		}
		if (!initialCommit)
			txnStateStore->set(KeyValueRef(m.param1, m.param2));
		if (!vecBackupKeys) {
			return;
		}
		Key logDestination;
		KeyRef logRangeBegin = logRangesDecodeKey(m.param1, nullptr);
		Key logRangeEnd = logRangesDecodeValue(m.param2, &logDestination);

		// Insert the logDestination into each range of vecBackupKeys overlapping the decoded range
		for (auto& logRange : vecBackupKeys->modify(KeyRangeRef(logRangeBegin, logRangeEnd))) {
			logRange->value().insert(logDestination);
		}
		for (auto& logRange : vecBackupKeys->modify(singleKeyRange(metadataVersionKey))) {
			logRange->value().insert(logDestination);
		}

		TraceEvent("LogRangeAdd")
		    .detail("LogRanges", vecBackupKeys->size())
		    .detail("MutationKey", m.param1)
		    .detail("LogRangeBegin", logRangeBegin)
		    .detail("LogRangeEnd", logRangeEnd);
	}

	void checkSetConstructKeys(MutationRef m) {
		if (!SERVER_KNOBS->GENERATE_DATA_ENABLED) {
			return;
		}
		if (!m.param1.startsWith(globalKeysPrefix)) {
			return;
		}
		if (!toCommit) {
			return;
		}

		if (m.param1.startsWith(constructDataKey)) {
			uint64_t valSize, keyCount, seed;
			Standalone<StringRef> prefix;
			std::tie(prefix, valSize, keyCount, seed) = decodeConstructKeys(m.param2);
			if (prefix.size() == 0 || keyCount >= UINT16_MAX || valSize >= CLIENT_KNOBS->VALUE_SIZE_LIMIT) {
				return;
			}
			uint8_t keyBuf[prefix.size() + sizeof(uint16_t)];
			uint8_t* keyPos = prefix.copyTo(keyBuf);
			*keyPos = '\xff';
			StringRef keyEnd(keyBuf, keyPos - keyBuf + 1);
			std::set<Tag> allTags;
			for (auto it : keyInfo->intersectingRanges(KeyRangeRef(prefix, keyEnd))) {
				auto& r = it.value();
				for (auto info : r.src_info) {
					allTags.insert(info->tag);
				}
				for (auto info : r.dest_info) {
					allTags.insert(info->tag);
				}
			}
			toCommit->addTags(allTags);
			TraceEvent(SevInfo, "ConstructDataRequest")
			    .detail("Prefix", prefix)
			    .detail("KeyCount", keyCount)
			    .detail("ValSize", valSize);
		}
	}

	void checkSetGlobalKeys(MutationRef m) {
		if (!m.param1.startsWith(globalKeysPrefix)) {
			return;
		}
		if (!toCommit) {
			return;
		}
		// Notifies all servers that a Master's server epoch ends
		auto allServers = txnStateStore->readRange(serverTagKeys).get();
		std::set<Tag> allTags;

		if (m.param1 == killStorageKey) {
			int8_t safeLocality = BinaryReader::fromStringRef<int8_t>(m.param2, Unversioned());
			for (auto& kv : allServers) {
				Tag t = decodeServerTagValue(kv.value);
				if (t.locality != safeLocality) {
					allTags.insert(t);
				}
			}
		} else {
			for (auto& kv : allServers) {
				allTags.insert(decodeServerTagValue(kv.value));
			}
		}
		allTags.insert(cacheTag);

		if (m.param1 == lastEpochEndKey) {
			toCommit->addTags(allTags);
			toCommit->writeTypedMessage(LogProtocolMessage(), true);
			TraceEvent(SevDebug, "SendingPrivatized_GlobalKeys", dbgid).detail("M", "LogProtocolMessage");
		}

		MutationRef privatized = m;
		privatized.clearChecksumAndAccumulativeIndex();
		privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
		privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
		TraceEvent(SevDebug, "SendingPrivatized_GlobalKeys", dbgid).detail("M", privatized);
		toCommit->addTags(allTags);
		writeMutation(privatized);
	}

	void checkClientInfo(MutationRef m) {
		if (!SERVER_KNOBS->WRITE_CLIENT_LATENCY_TRACEEVENT) {
			return;
		}
		if (!m.param1.startsWith(fdbClientInfoPrefixRange.begin)) {
			return;
		}

		TraceEvent("ApplyMetaDataMutationCheckClientInfo").detail("Key", m.param1).detail("ValueSize", m.param2.size());
	}

	// Generates private mutations for the target storage server, instructing it to create a checkpoint.
	void checkSetCheckpointKeys(MutationRef m) {
		if (!m.param1.startsWith(checkpointPrefix)) {
			return;
		}
		if (toCommit) {
			CheckpointMetaData checkpoint = decodeCheckpointValue(m.param2);
			for (const auto& ssID : checkpoint.src) {
				Optional<Value> tagValue = txnStateStore->readValue(serverTagKeyFor(ssID)).get();
				if (!tagValue.present()) {
					TraceEvent(SevWarn, "CheckpointServerTagNotFound", dbgid)
					    .detail("StorageServerID", ssID)
					    .detail("Checkpoint", checkpoint.toString());
					continue;
				}
				const Tag tag = decodeServerTagValue(tagValue.get());
				MutationRef privatized = m;
				privatized.clearChecksumAndAccumulativeIndex();
				privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
				privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
				TraceEvent("SendingPrivateMutationCheckpoint", dbgid)
				    .detail("Original", m)
				    .detail("Privatized", privatized)
				    .detail("Server", ssID)
				    .detail("TagKey", serverTagKeyFor(ssID))
				    .detail("Tag", tag.toString())
				    .detail("Checkpoint", checkpoint.toString());

				toCommit->addTag(tag);
				writeMutation(privatized);
			}
		}
	}

	void checkSetOtherKeys(MutationRef m) {
		if (initialCommit)
			return;
		if (m.param1 == databaseLockedKey || m.param1 == metadataVersionKey ||
		    m.param1 == mustContainSystemMutationsKey || m.param1.startsWith(applyMutationsBeginRange.begin) ||
		    m.param1.startsWith(applyMutationsAddPrefixRange.begin) ||
		    m.param1.startsWith(applyMutationsRemovePrefixRange.begin) || m.param1.startsWith(tagLocalityListPrefix) ||
		    m.param1.startsWith(serverTagHistoryPrefix) ||
		    m.param1.startsWith(testOnlyTxnStateStorePrefixRange.begin)) {

			txnStateStore->set(KeyValueRef(m.param1, m.param2));
		}
	}

	void checkSetMinRequiredCommitVersionKey(MutationRef m) {
		if (m.param1 != minRequiredCommitVersionKey) {
			return;
		}
		Version requested = BinaryReader::fromStringRef<Version>(m.param2, Unversioned());
		TraceEvent("MinRequiredCommitVersion", dbgid).detail("Min", requested).detail("Current", popVersion);
		if (!initialCommit)
			txnStateStore->set(KeyValueRef(m.param1, m.param2));
		confChange = true;
		CODE_PROBE(true, "Recovering at a higher version.");
	}

	void checkSetVersionEpochKey(MutationRef m) {
		if (m.param1 != versionEpochKey) {
			return;
		}
		int64_t versionEpoch = BinaryReader::fromStringRef<int64_t>(m.param2, Unversioned());
		TraceEvent("VersionEpoch", dbgid).detail("Epoch", versionEpoch);
		if (!initialCommit)
			txnStateStore->set(KeyValueRef(m.param1, m.param2));
		confChange = true;
		CODE_PROBE(true, "Setting version epoch", probe::decoration::rare);
	}

	void checkSetWriteRecoverKey(MutationRef m) {
		if (m.param1 != writeRecoveryKey) {
			return;
		}
		TraceEvent("WriteRecoveryKeySet", dbgid).log();
		if (!initialCommit)
			txnStateStore->set(KeyValueRef(m.param1, m.param2));
		CODE_PROBE(true, "Snapshot created, setting writeRecoveryKey in txnStateStore", probe::decoration::rare);
	}

	void checkSetTenantMapPrefix(MutationRef m) {
		KeyRef prefix = TenantMetadata::tenantMap().subspace.begin;
		if (m.param1.startsWith(prefix)) {
			TenantMapEntry tenantEntry;
			if (initialCommit) {
				TenantMapEntryTxnStateStore txnStateStoreEntry = TenantMapEntryTxnStateStore::decode(m.param2);
				tenantEntry.setId(txnStateStoreEntry.id);
				tenantEntry.tenantName = txnStateStoreEntry.tenantName;
				tenantEntry.tenantLockState = txnStateStoreEntry.tenantLockState;
			} else {
				tenantEntry = TenantMapEntry::decode(m.param2);
			}

			if (tenantMap) {
				ASSERT(version != invalidVersion);

				TraceEvent("CommitProxyInsertTenant", dbgid)
				    .detail("Tenant", tenantEntry.tenantName)
				    .detail("Id", tenantEntry.id)
				    .detail("Version", version);

				(*tenantMap)[tenantEntry.id] = tenantEntry.tenantName;
				if (tenantNameIndex) {
					(*tenantNameIndex)[tenantEntry.tenantName] = tenantEntry.id;
				}
			}
			if (lockedTenants) {
				if (tenantEntry.tenantLockState == TenantAPI::TenantLockState::UNLOCKED) {
					lockedTenants->erase(tenantEntry.id);
				} else {
					lockedTenants->insert(tenantEntry.id);
				}
			}

			if (!initialCommit) {
				txnStateStore->set(KeyValueRef(m.param1, tenantEntry.toTxnStateStoreEntry().encode()));
			}

			// For now, this goes to all storage servers.
			// Eventually, we can have each SS store tenants that apply only to the data stored on it.
			if (toCommit) {
				std::set<Tag> allTags;
				auto allServers = txnStateStore->readRange(serverTagKeys).get();
				for (auto& kv : allServers) {
					allTags.insert(decodeServerTagValue(kv.value));
				}

				toCommit->addTags(allTags);

				MutationRef privatized = m;
				privatized.clearChecksumAndAccumulativeIndex();
				privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
				privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
				writeMutation(privatized);
			}

			CODE_PROBE(true, "Tenant added to map");
		}
	}

	void checkSetMetaclusterRegistration(MutationRef m) {
		if (m.param1 == metacluster::metadata::metaclusterRegistration().key) {
			MetaclusterRegistrationEntry entry = MetaclusterRegistrationEntry::decode(m.param2);

			TraceEvent("SetMetaclusterRegistration", dbgid)
			    .detail("ClusterType", entry.clusterType)
			    .detail("MetaclusterID", entry.metaclusterId)
			    .detail("MetaclusterName", entry.metaclusterName)
			    .detail("ClusterID", entry.id)
			    .detail("ClusterName", entry.name);

			Optional<Value> value =
			    txnStateStore->readValue(metacluster::metadata::metaclusterRegistration().key).get();
			if (!initialCommit) {
				txnStateStore->set(KeyValueRef(m.param1, m.param2));
			}

			if (!value.present() || value.get() != m.param2) {
				confChange = true;
			}

			CODE_PROBE(true, "Metacluster registration set");
		}
	}

	void checkClearKeyServerKeys(KeyRangeRef range) {
		if (!keyServersKeys.intersects(range)) {
			return;
		}
		KeyRangeRef r = range & keyServersKeys;
		if (keyInfo) {
			KeyRangeRef clearRange(r.begin.removePrefix(keyServersPrefix), r.end.removePrefix(keyServersPrefix));
			keyInfo->insert(clearRange,
			                clearRange.begin == StringRef()
			                    ? ServerCacheInfo()
			                    : keyInfo->rangeContainingKeyBefore(clearRange.begin).value());
			if (toCommit && SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
				toCommit->setShardChanged();
			}
		}

		if (!initialCommit)
			txnStateStore->clear(r);
	}

	void checkClearConfigKeys(MutationRef m, KeyRangeRef range) {
		if (!configKeys.intersects(range)) {
			return;
		}
		if (!initialCommit)
			txnStateStore->clear(range & configKeys);
		if (!excludedServersKeys.contains(range) && !failedServersKeys.contains(range) &&
		    !excludedLocalityKeys.contains(range) && !failedLocalityKeys.contains(range)) {
			TraceEvent("MutationRequiresRestart", dbgid).detail("M", m);
			confChange = true;
		}
	}

	void checkClearServerListKeys(KeyRangeRef range) {
		if (!serverListKeys.intersects(range)) {
			return;
		}
		if (initialCommit) {
			return;
		}
		KeyRangeRef rangeToClear = range & serverListKeys;
		if (rangeToClear.singleKeyRange()) {
			UID id = decodeServerListKey(rangeToClear.begin);
			Optional<Value> ssiV = txnStateStore->readValue(serverListKeyFor(id)).get();
			if (ssiV.present() && decodeServerListValue(ssiV.get()).isTss()) {
				tssServerListToRemove.push_back(rangeToClear);
			} else {
				txnStateStore->clear(rangeToClear);
			}
		} else {
			txnStateStore->clear(rangeToClear);
		}
	}

	void checkClearTagLocalityListKeys(KeyRangeRef range) {
		if (!tagLocalityListKeys.intersects(range) || initialCommit) {
			return;
		}
		txnStateStore->clear(range & tagLocalityListKeys);
	}

	void checkClearServerTagKeys(MutationRef m, KeyRangeRef range) {
		if (!serverTagKeys.intersects(range)) {
			return;
		}
		// Storage server removal always happens in a separate version from any prior writes (or any subsequent
		// reuse of the tag) so we can safely destroy the tag here without any concern about intra-batch
		// ordering
		if (logSystem && popVersion) {
			auto serverKeysCleared =
			    txnStateStore->readRange(range & serverTagKeys).get(); // read is expected to be immediately available
			for (auto& kv : serverKeysCleared) {
				Tag tag = decodeServerTagValue(kv.value);
				TraceEvent("ServerTagRemove")
				    .detail("PopVersion", popVersion)
				    .detail("Tag", tag.toString())
				    .detail("Server", decodeServerTagKey(kv.key));
				if (!forResolver) {
					logSystem->pop(popVersion, tag);
					(*tag_popped)[tag] = popVersion;
				}
				ASSERT_WE_THINK(forResolver ^ (tag_popped != nullptr));

				if (toCommit) {
					MutationRef privatized = m;
					privatized.clearChecksumAndAccumulativeIndex();
					privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
					privatized.param1 = kv.key.withPrefix(systemKeys.begin, arena);
					privatized.param2 = keyAfter(privatized.param1, arena);

					TraceEvent(SevDebug, "SendingPrivatized_ClearServerTag", dbgid).detail("M", privatized);

					toCommit->addTag(tag);
					writeMutation(privatized);
				}
			}
			// Might be a tss removal, which doesn't store a tag there.
			// Chained if is a little verbose, but avoids unnecessary work
			if (toCommit && !initialCommit && !serverKeysCleared.size()) {
				KeyRangeRef maybeTssRange = range & serverTagKeys;
				if (maybeTssRange.singleKeyRange()) {
					UID id = decodeServerTagKey(maybeTssRange.begin);
					Optional<Value> ssiV = txnStateStore->readValue(serverListKeyFor(id)).get();

					if (ssiV.present()) {
						StorageServerInterface ssi = decodeServerListValue(ssiV.get());
						if (ssi.isTss()) {
							Optional<Value> tagV = txnStateStore->readValue(serverTagKeyFor(ssi.tssPairID.get())).get();
							if (tagV.present()) {
								MutationRef privatized = m;
								privatized.clearChecksumAndAccumulativeIndex();
								privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
								privatized.param1 = maybeTssRange.begin.withPrefix(systemKeys.begin, arena);
								privatized.param2 =
								    keyAfter(maybeTssRange.begin, arena).withPrefix(systemKeys.begin, arena);

								TraceEvent(SevDebug, "SendingPrivatized_TSSClearServerTag", dbgid)
								    .detail("M", privatized);
								toCommit->addTag(decodeServerTagValue(tagV.get()));
								writeMutation(privatized);
							}
						}
					}
				}
			}
		}

		if (!initialCommit) {
			KeyRangeRef clearRange = range & serverTagKeys;
			txnStateStore->clear(clearRange);
			if (storageCache && clearRange.singleKeyRange()) {
				storageCache->erase(decodeServerTagKey(clearRange.begin));
			}
		}
	}

	void checkClearServerTagHistoryKeys(KeyRangeRef range) {
		if (!serverTagHistoryKeys.intersects(range)) {
			return;
		}
		// Once a tag has been removed from history we should pop it, since we no longer have a record of the
		// tag once it has been removed from history
		if (logSystem && popVersion) {
			auto serverKeysCleared = txnStateStore->readRange(range & serverTagHistoryKeys)
			                             .get(); // read is expected to be immediately available
			for (auto& kv : serverKeysCleared) {
				Tag tag = decodeServerTagValue(kv.value);
				TraceEvent("ServerTagHistoryRemove")
				    .detail("PopVersion", popVersion)
				    .detail("Tag", tag.toString())
				    .detail("Version", decodeServerTagHistoryKey(kv.key));
				if (!forResolver) {
					logSystem->pop(popVersion, tag);
					(*tag_popped)[tag] = popVersion;
				}
				ASSERT_WE_THINK(forResolver ^ (tag_popped != nullptr));
			}
		}
		if (!initialCommit)
			txnStateStore->clear(range & serverTagHistoryKeys);
	}

	void checkClearApplyMutationsEndRange(MutationRef m, KeyRangeRef range) {
		if (!range.intersects(applyMutationsEndRange)) {
			return;
		}
		KeyRangeRef commonEndRange(range & applyMutationsEndRange);
		if (!initialCommit)
			txnStateStore->clear(commonEndRange);
		if (uid_applyMutationsData != nullptr) {
			uid_applyMutationsData->erase(
			    uid_applyMutationsData->lower_bound(m.param1.substr(applyMutationsEndRange.begin.size())),
			    m.param2 == applyMutationsEndRange.end
			        ? uid_applyMutationsData->end()
			        : uid_applyMutationsData->lower_bound(m.param2.substr(applyMutationsEndRange.begin.size())));
		}
	}

	void checkClearApplyMutationKeyVersionMapRange(MutationRef m, KeyRangeRef range) {
		if (!range.intersects(applyMutationsKeyVersionMapRange)) {
			return;
		}
		KeyRangeRef commonApplyRange(range & applyMutationsKeyVersionMapRange);
		if (!initialCommit)
			txnStateStore->clear(commonApplyRange);
		if (uid_applyMutationsData == nullptr) {
			return;
		}
		if (m.param1.size() >= applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID) &&
		    m.param2.size() >= applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID)) {
			Key uid = m.param1.substr(applyMutationsKeyVersionMapRange.begin.size(), sizeof(UID));
			Key uid2 = m.param2.substr(applyMutationsKeyVersionMapRange.begin.size(), sizeof(UID));

			if (uid == uid2) {
				auto& p = (*uid_applyMutationsData)[uid];
				if (p.keyVersion == Reference<KeyRangeMap<Version>>())
					p.keyVersion = makeReference<KeyRangeMap<Version>>();
				p.keyVersion->rawErase(
				    KeyRangeRef(m.param1.substr(applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID)),
				                m.param2.substr(applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID))));
			}
		}
	}

	void checkClearLogRangesRange(KeyRangeRef range) {
		if (!range.intersects(logRangesRange)) {
			return;
		}
		KeyRangeRef commonLogRange(range & logRangesRange);

		TraceEvent("LogRangeClear")
		    .detail("RangeBegin", range.begin)
		    .detail("RangeEnd", range.end)
		    .detail("IntersectBegin", commonLogRange.begin)
		    .detail("IntersectEnd", commonLogRange.end);

		// Remove the key range from the vector, if defined
		if (vecBackupKeys) {
			KeyRef logKeyBegin;
			Key logKeyEnd, logDestination;

			// Identify the backup keys being removed
			// read is expected to be immediately available
			auto logRangesAffected = txnStateStore->readRange(commonLogRange).get();

			TraceEvent("LogRangeClearBegin").detail("AffectedLogRanges", logRangesAffected.size());

			// Add the backup name to the backup locations that do not have it
			for (auto logRangeAffected : logRangesAffected) {
				// Parse the backup key and name
				logKeyBegin = logRangesDecodeKey(logRangeAffected.key, nullptr);

				// Decode the log destination and key value
				logKeyEnd = logRangesDecodeValue(logRangeAffected.value, &logDestination);

				TraceEvent("LogRangeErase")
				    .detail("AffectedKey", logRangeAffected.key)
				    .detail("AffectedValue", logRangeAffected.value)
				    .detail("LogKeyBegin", logKeyBegin)
				    .detail("LogKeyEnd", logKeyEnd)
				    .detail("LogDestination", logDestination);

				// Identify the locations to place the backup key
				auto logRanges = vecBackupKeys->modify(KeyRangeRef(logKeyBegin, logKeyEnd));

				// Remove the log prefix from the ranges which include it
				for (auto logRange : logRanges) {
					auto& logRangeMap = logRange->value();

					// Remove the backup name from the range
					logRangeMap.erase(logDestination);
				}

				bool foundKey = false;
				for (auto& it : vecBackupKeys->intersectingRanges(normalKeys)) {
					if (it.value().count(logDestination) > 0) {
						foundKey = true;
						break;
					}
				}
				auto& systemBackupRanges = getSystemBackupRanges();
				for (auto r = systemBackupRanges.begin(); !foundKey && r != systemBackupRanges.end(); ++r) {
					for (auto& it : vecBackupKeys->intersectingRanges(*r)) {
						if (it.value().count(logDestination) > 0) {
							foundKey = true;
							break;
						}
					}
				}
				if (!foundKey) {
					auto logRanges = vecBackupKeys->modify(singleKeyRange(metadataVersionKey));
					for (auto logRange : logRanges) {
						auto& logRangeMap = logRange->value();
						logRangeMap.erase(logDestination);
					}
				}
			}

			// Coallesce the entire range
			vecBackupKeys->coalesce(allKeys);
		}

		if (!initialCommit)
			txnStateStore->clear(commonLogRange);
	}

	void checkClearTssMappingKeys(MutationRef m, KeyRangeRef range) {
		if (!tssMappingKeys.intersects(range)) {
			return;
		}
		KeyRangeRef rangeToClear = range & tssMappingKeys;
		ASSERT(rangeToClear.singleKeyRange());

		// Normally uses key backed map, so have to use same unpacking code here.
		UID ssId = TupleCodec<UID>::unpack(m.param1.removePrefix(tssMappingKeys.begin));
		if (!initialCommit) {
			txnStateStore->clear(rangeToClear);
		}

		if (tssMapping) {
			tssMapping->erase(ssId);
		}

		if (!toCommit) {
			return;
		}
		// send private mutation to SS to notify that it no longer has a tss pair
		if (Optional<Value> tagV = txnStateStore->readValue(serverTagKeyFor(ssId)).get(); tagV.present()) {
			MutationRef privatized = m;
			privatized.clearChecksumAndAccumulativeIndex();
			privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
			privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
			privatized.param2 = m.param2.withPrefix(systemKeys.begin, arena);
			TraceEvent(SevDebug, "SendingPrivatized_ClearTSSMapping", dbgid).detail("M", privatized);
			toCommit->addTag(decodeServerTagValue(tagV.get()));
			writeMutation(privatized);
		}
	}

	void checkClearTssQuarantineKeys(MutationRef m, KeyRangeRef range) {
		if (!tssQuarantineKeys.intersects(range) || initialCommit) {
			return;
		}

		KeyRangeRef rangeToClear = range & tssQuarantineKeys;
		ASSERT(rangeToClear.singleKeyRange());
		txnStateStore->clear(rangeToClear);

		if (!toCommit) {
			return;
		}
		UID tssId = decodeTssQuarantineKey(m.param1);
		if (Optional<Value> ssiV = txnStateStore->readValue(serverListKeyFor(tssId)).get(); ssiV.present()) {
			if (StorageServerInterface ssi = decodeServerListValue(ssiV.get()); ssi.isTss()) {
				if (Optional<Value> tagV = txnStateStore->readValue(serverTagKeyFor(ssi.tssPairID.get())).get();
				    tagV.present()) {

					MutationRef privatized = m;
					privatized.clearChecksumAndAccumulativeIndex();
					privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
					privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
					privatized.param2 = m.param2.withPrefix(systemKeys.begin, arena);
					TraceEvent(SevDebug, "SendingPrivatized_ClearTSSQuarantine", dbgid).detail("M", privatized);
					toCommit->addTag(decodeServerTagValue(tagV.get()));
					writeMutation(privatized);
				}
			}
		}
	}

	void checkClearVersionEpochKeys(MutationRef m, KeyRangeRef range) {
		if (!range.contains(versionEpochKey)) {
			return;
		}
		if (!initialCommit)
			txnStateStore->clear(singleKeyRange(versionEpochKey));
		TraceEvent("MutationRequiresRestart", dbgid).detail("M", m);
		confChange = true;
	}

	void checkClearTenantMapPrefix(KeyRangeRef range) {
		KeyRangeRef subspace = TenantMetadata::tenantMap().subspace;
		if (subspace.intersects(range)) {
			if (tenantMap && tenantNameIndex) {
				ASSERT(version != invalidVersion);

				Optional<int64_t> startId = 0;
				Optional<int64_t> endId;

				if (range.begin > subspace.begin) {
					startId = TenantIdCodec::lowerBound(range.begin.removePrefix(subspace.begin));
				}
				if (range.end.startsWith(subspace.begin)) {
					endId = TenantIdCodec::lowerBound(range.end.removePrefix(subspace.begin));
				}

				TraceEvent("CommitProxyEraseTenants", dbgid)
				    .detail("BeginTenant", startId)
				    .detail("EndTenant", endId)
				    .detail("Version", version);

				auto startItr = startId.present() ? tenantMap->lower_bound(startId.get()) : tenantMap->end();
				auto endItr = endId.present() ? tenantMap->lower_bound(endId.get()) : tenantMap->end();

				auto itr = startItr;
				while (itr != endItr) {
					tenantNameIndex->erase(itr->second);
					itr++;
				}

				tenantMap->erase(startItr, endItr);

				if (lockedTenants) {
					auto startItr = startId.present()
					                    ? std::lower_bound(lockedTenants->begin(), lockedTenants->end(), startId.get())
					                    : lockedTenants->end();
					auto endItr = endId.present()
					                  ? std::lower_bound(lockedTenants->begin(), lockedTenants->end(), endId.get())
					                  : lockedTenants->end();
					lockedTenants->erase(startItr, endItr);
				}
			}

			if (!initialCommit) {
				txnStateStore->clear(range);
			}

			// For now, this goes to all storage servers.
			// Eventually, we can have each SS store tenants that apply only to the data stored on it.
			if (toCommit) {
				std::set<Tag> allTags;
				auto allServers = txnStateStore->readRange(serverTagKeys).get();
				for (auto& kv : allServers) {
					allTags.insert(decodeServerTagValue(kv.value));
				}

				toCommit->addTags(allTags);

				MutationRef privatized;
				privatized.clearChecksumAndAccumulativeIndex();
				privatized.setAccumulativeChecksumIndex(accumulativeChecksumIndex);
				privatized.type = MutationRef::ClearRange;
				privatized.param1 = systemKeys.begin.withSuffix(std::max(range.begin, subspace.begin), arena);
				if (range.end < subspace.end) {
					privatized.param2 = systemKeys.begin.withSuffix(range.end, arena);
				} else {
					privatized.param2 = systemKeys.begin.withSuffix(subspace.begin).withSuffix("\xff\xff"_sr, arena);
				}
				writeMutation(privatized);
			}

			CODE_PROBE(true, "Tenant cleared from map");
		}
	}

	void checkClearMetaclusterRegistration(KeyRangeRef range) {
		if (range.contains(metacluster::metadata::metaclusterRegistration().key)) {
			TraceEvent("ClearMetaclusterRegistration", dbgid);

			Optional<Value> value =
			    txnStateStore->readValue(metacluster::metadata::metaclusterRegistration().key).get();
			if (!initialCommit) {
				txnStateStore->clear(singleKeyRange(metacluster::metadata::metaclusterRegistration().key));
			}

			if (value.present()) {
				confChange = true;
			}

			CODE_PROBE(true, "Metacluster registration cleared");
		}
	}

	void checkClearMiscRangeKeys(KeyRangeRef range) {
		if (initialCommit) {
			return;
		}
		if (range.contains(previousCoordinatorsKey)) {
			txnStateStore->clear(singleKeyRange(previousCoordinatorsKey));
		}
		if (range.contains(coordinatorsKey)) {
			txnStateStore->clear(singleKeyRange(coordinatorsKey));
		}
		if (range.contains(databaseLockedKey)) {
			txnStateStore->clear(singleKeyRange(databaseLockedKey));
		}
		if (range.contains(metadataVersionKey)) {
			txnStateStore->clear(singleKeyRange(metadataVersionKey));
		}
		if (range.contains(mustContainSystemMutationsKey)) {
			txnStateStore->clear(singleKeyRange(mustContainSystemMutationsKey));
		}
		if (range.contains(writeRecoveryKey)) {
			txnStateStore->clear(singleKeyRange(writeRecoveryKey));
		}
		if (range.intersects(testOnlyTxnStateStorePrefixRange)) {
			txnStateStore->clear(range & testOnlyTxnStateStorePrefixRange);
		}
	}

	// If we accumulated private mutations for cached key-ranges, we also need to
	// tag them with the relevant storage servers. This is done to make the storage
	// servers aware of the cached key-ranges
	// NOTE: we are assuming non-colliding cached key-ranges

	// TODO Note that, we are currently not handling the case when cached key-ranges move out
	// to different storage servers. This would require some checking when keys in the keyServersPrefix change.
	// For the first implementation, we could just send the entire map to every storage server. Revisit!
	void tagStorageServersForCachedKeyRanges() {
		if (cachedRangeInfo.size() == 0 || !toCommit) {
			return;
		}

		std::map<KeyRef, MutationRef>::iterator itr;
		KeyRef keyBegin, keyEnd;
		std::vector<uint16_t> serverIndices;
		MutationRef mutationBegin, mutationEnd;

		for (itr = cachedRangeInfo.begin(); itr != cachedRangeInfo.end(); ++itr) {
			// first figure out the begin and end keys for the cached-range,
			// the begin and end mutations can be in any order
			decodeStorageCacheValue(itr->second.param2, serverIndices);
			// serverIndices count should be greater than zero for beginKey mutations
			if (serverIndices.size() > 0) {
				keyBegin = itr->first;
				mutationBegin = itr->second;
				++itr;
				if (itr != cachedRangeInfo.end()) {
					keyEnd = itr->first;
					mutationEnd = itr->second;
				} else {
					//TraceEvent(SevDebug, "EndKeyNotFound", dbgid).detail("KeyBegin", keyBegin.toString());
					break;
				}
			} else {
				keyEnd = itr->first;
				mutationEnd = itr->second;
				++itr;
				if (itr != cachedRangeInfo.end()) {
					keyBegin = itr->first;
					mutationBegin = itr->second;
				} else {
					//TraceEvent(SevDebug, "BeginKeyNotFound", dbgid).detail("KeyEnd", keyEnd.toString());
					break;
				}
			}

			// Now get all the storage server tags for the cached key-ranges
			std::set<Tag> allTags;
			auto ranges = keyInfo->intersectingRanges(KeyRangeRef(keyBegin, keyEnd));
			for (auto it : ranges) {
				auto& r = it.value();
				for (auto info : r.src_info) {
					allTags.insert(info->tag);
				}
				for (auto info : r.dest_info) {
					allTags.insert(info->tag);
				}
			}

			// Add the tags to both begin and end mutations
			TraceEvent(SevDebug, "SendingPrivatized_CachedKeyRange", dbgid)
			    .detail("MBegin", mutationBegin)
			    .detail("MEnd", mutationEnd);
			toCommit->addTags(allTags);
			writeMutation(mutationBegin);
			toCommit->addTags(allTags);
			writeMutation(mutationEnd);
		}
	}

public:
	void apply() {
		for (auto const& m : mutations) {
			if (toCommit) {
				toCommit->addTransactionInfo(spanContext);
			}

			checkClientInfo(m);

			if (m.type == MutationRef::SetValue && isSystemKey(m.param1)) {
				checkSetKeyServersPrefix(m);
				checkSetServerKeysPrefix(m);
				checkSetCheckpointKeys(m);
				checkSetServerTagsPrefix(m);
				checkSetStorageCachePrefix(m);
				checkSetCacheKeysPrefix(m);
				checkSetConfigKeys(m);
				checkSetServerListPrefix(m);
				checkSetChangeFeedPrefix(m);
				checkSetTSSMappingKeys(m);
				checkSetTSSQuarantineKeys(m);
				checkSetApplyMutationsEndRange(m);
				checkSetApplyMutationsKeyVersionMapRange(m);
				checkSetLogRangesRange(m);
				checkSetGlobalKeys(m);
				checkSetWriteRecoverKey(m);
				checkSetMinRequiredCommitVersionKey(m);
				checkSetVersionEpochKey(m);
				checkSetTenantMapPrefix(m);
				checkSetMetaclusterRegistration(m);
				checkSetOtherKeys(m);
			} else if (m.type == MutationRef::ClearRange && isSystemKey(m.param2)) {
				KeyRangeRef range(m.param1, m.param2);

				checkClearKeyServerKeys(range);
				checkClearConfigKeys(m, range);
				checkClearServerListKeys(range);
				checkClearTagLocalityListKeys(range);
				checkClearServerTagKeys(m, range);
				checkClearServerTagHistoryKeys(range);
				checkClearApplyMutationsEndRange(m, range);
				checkClearApplyMutationKeyVersionMapRange(m, range);
				checkClearLogRangesRange(range);
				checkClearTssMappingKeys(m, range);
				checkClearTssQuarantineKeys(m, range);
				checkClearVersionEpochKeys(m, range);
				checkClearTenantMapPrefix(range);
				checkClearMetaclusterRegistration(range);
				checkClearMiscRangeKeys(range);
			}
		}

		for (KeyRangeRef& range : tssServerListToRemove) {
			txnStateStore->clear(range);
		}

		for (auto& tssPair : tssMappingToAdd) {
			// read tss server list from txn state store and add it to tss mapping
			StorageServerInterface tssi =
			    decodeServerListValue(txnStateStore->readValue(serverListKeyFor(tssPair.second)).get().get());
			(*tssMapping)[tssPair.first] = tssi;
		}

		tagStorageServersForCachedKeyRanges();
	}
};

} // anonymous namespace

void applyMetadataMutations(SpanContext const& spanContext,
                            ProxyCommitData& proxyCommitData,
                            Arena& arena,
                            Reference<ILogSystem> logSystem,
                            const VectorRef<MutationRef>& mutations,
                            LogPushData* toCommit,
                            const std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>* pCipherKeys,
                            EncryptionAtRestMode encryptMode,
                            bool& confChange,
                            Version version,
                            Version popVersion,
                            bool initialCommit,
                            bool provisionalCommitProxy) {
	ApplyMetadataMutationsImpl(spanContext,
	                           arena,
	                           mutations,
	                           proxyCommitData,
	                           logSystem,
	                           toCommit,
	                           pCipherKeys,
	                           encryptMode,
	                           confChange,
	                           version,
	                           popVersion,
	                           initialCommit,
	                           provisionalCommitProxy)
	    .apply();
}

void applyMetadataMutations(SpanContext const& spanContext,
                            ResolverData& resolverData,
                            const VectorRef<MutationRef>& mutations,
                            const std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>* pCipherKeys,
                            EncryptionAtRestMode encryptMode) {
	ApplyMetadataMutationsImpl(spanContext, resolverData, mutations, pCipherKeys, encryptMode).apply();
}

void applyMetadataMutations(SpanContext const& spanContext,
                            const UID& dbgid,
                            Arena& arena,
                            const VectorRef<MutationRef>& mutations,
                            IKeyValueStore* txnStateStore) {
	ApplyMetadataMutationsImpl(spanContext, dbgid, arena, mutations, txnStateStore).apply();
}
