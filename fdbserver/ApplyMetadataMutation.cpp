/*
 * ApplyMetadataMutation.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/MutationList.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/Notified.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogProtocolMessage.h"

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

// It is incredibly important that any modifications to txnStateStore are done in such a way that
// the same operations will be done on all commit proxies at the same time. Otherwise, the data
// stored in txnStateStore will become corrupted.
void applyMetadataMutations(SpanID const& spanContext,
                            UID const& dbgid,
                            Arena& arena,
                            VectorRef<MutationRef> const& mutations,
                            IKeyValueStore* txnStateStore,
                            LogPushData* toCommit,
                            bool& confChange,
                            Reference<ILogSystem> logSystem,
                            Version popVersion,
                            KeyRangeMap<std::set<Key>>* vecBackupKeys,
                            KeyRangeMap<ServerCacheInfo>* keyInfo,
                            KeyRangeMap<bool>* cacheInfo,
                            std::map<Key, ApplyMutationsData>* uid_applyMutationsData,
                            RequestStream<CommitTransactionRequest> commit,
                            Database cx,
                            NotifiedVersion* commitVersion,
                            std::map<UID, Reference<StorageInfo>>* storageCache,
                            std::map<Tag, Version>* tag_popped,
                            bool initialCommit) {
	// std::map<keyRef, vector<uint16_t>> cacheRangeInfo;
	std::map<KeyRef, MutationRef> cachedRangeInfo;

	// Testing Storage Server removal (clearing serverTagKey) needs to read tss server list value to determine it is a
	// tss + find partner's tag to send the private mutation. Since the removeStorageServer transaction clears both the
	// storage list and server tag, we have to enforce ordering, proccessing the server tag first, and postpone the
	// server list clear until the end;
	std::vector<KeyRangeRef> tssServerListToRemove;

	for (auto const& m : mutations) {
		//TraceEvent("MetadataMutation", dbgid).detail("M", m.toString());
		if (toCommit) {
			toCommit->addTransactionInfo(spanContext);
		}

		if (m.param1.size() && m.param1[0] == systemKeys.begin[0] && m.type == MutationRef::SetValue) {
			if (m.param1.startsWith(keyServersPrefix)) {
				if (keyInfo) {
					KeyRef k = m.param1.removePrefix(keyServersPrefix);
					if (k != allKeys.end) {
						KeyRef end = keyInfo->rangeContaining(k).end();
						KeyRangeRef insertRange(k, end);
						vector<UID> src, dest;
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
					}
				}
				if (!initialCommit)
					txnStateStore->set(KeyValueRef(m.param1, m.param2));
			} else if (m.param1.startsWith(serverKeysPrefix)) {
				if (toCommit) {
					Tag tag = decodeServerTagValue(
					    txnStateStore->readValue(serverTagKeyFor(serverKeysDecodeServer(m.param1))).get().get());
					MutationRef privatized = m;
					privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
					TraceEvent(SevDebug, "SendingPrivateMutation", dbgid)
					    .detail("Original", m.toString())
					    .detail("Privatized", privatized.toString())
					    .detail("Server", serverKeysDecodeServer(m.param1))
					    .detail("TagKey", serverTagKeyFor(serverKeysDecodeServer(m.param1)))
					    .detail("Tag", tag.toString());

					toCommit->addTag(tag);
					toCommit->writeTypedMessage(privatized);
				}
			} else if (m.param1.startsWith(serverTagPrefix)) {
				UID id = decodeServerTagKey(m.param1);
				Tag tag = decodeServerTagValue(m.param2);

				if (toCommit) {
					MutationRef privatized = m;
					privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
					TraceEvent("ServerTag", dbgid).detail("Server", id).detail("Tag", tag.toString());

					toCommit->addTag(tag);
					toCommit->writeTypedMessage(LogProtocolMessage(), true);
					toCommit->addTag(tag);
					toCommit->writeTypedMessage(privatized);
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
			} else if (m.param1.startsWith(storageCachePrefix)) {
				if (cacheInfo) {
					KeyRef k = m.param1.removePrefix(storageCachePrefix);

					// Create a private mutation for storage servers
					// This is done to make the storage servers aware of the cached key-ranges
					if (toCommit) {
						MutationRef privatized = m;
						privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
						//TraceEvent(SevDebug, "SendingPrivateMutation", dbgid).detail("Original", m.toString()).detail("Privatized", privatized.toString());
						cachedRangeInfo[k] = privatized;
					}
					if (k != allKeys.end) {
						KeyRef end = cacheInfo->rangeContaining(k).end();
						vector<uint16_t> serverIndices;
						decodeStorageCacheValue(m.param2, serverIndices);
						cacheInfo->insert(KeyRangeRef(k, end), serverIndices.size() > 0);
					}
				}
				if (!initialCommit)
					txnStateStore->set(KeyValueRef(m.param1, m.param2));
			} else if (m.param1.startsWith(cacheKeysPrefix)) {
				// Create a private mutation for cache servers
				// This is done to make the cache servers aware of the cached key-ranges
				if (toCommit) {
					MutationRef privatized = m;
					privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
					//TraceEvent(SevDebug, "SendingPrivateMutation", dbgid).detail("Original", m.toString()).detail("Privatized", privatized.toString());
					toCommit->addTag(cacheTag);
					toCommit->writeTypedMessage(privatized);
				}
			} else if (m.param1.startsWith(configKeysPrefix) || m.param1 == coordinatorsKey) {
				if (Optional<StringRef>(m.param2) !=
				    txnStateStore->readValue(m.param1)
				        .get()
				        .castTo<StringRef>()) { // FIXME: Make this check more specific, here or by reading
					                            // configuration whenever there is a change
					if ((!m.param1.startsWith(excludedServersPrefix) && m.param1 != excludedServersVersionKey) &&
					    (!m.param1.startsWith(failedServersPrefix) && m.param1 != failedServersVersionKey)) {
						auto t = txnStateStore->readValue(m.param1).get();
						TraceEvent("MutationRequiresRestart", dbgid)
						    .detail("M", m.toString())
						    .detail("PrevValue", t.present() ? t.get() : LiteralStringRef("(none)"))
						    .detail("ToCommit", toCommit != nullptr);
						confChange = true;
					}
				}
				if (!initialCommit)
					txnStateStore->set(KeyValueRef(m.param1, m.param2));
			} else if (m.param1.startsWith(serverListPrefix)) {
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
			} else if (m.param1 == databaseLockedKey || m.param1 == metadataVersionKey ||
			           m.param1 == mustContainSystemMutationsKey ||
			           m.param1.startsWith(applyMutationsBeginRange.begin) ||
			           m.param1.startsWith(applyMutationsAddPrefixRange.begin) ||
			           m.param1.startsWith(applyMutationsRemovePrefixRange.begin) ||
			           m.param1.startsWith(tagLocalityListPrefix) || m.param1.startsWith(serverTagHistoryPrefix) ||
			           m.param1.startsWith(testOnlyTxnStateStorePrefixRange.begin)) {
				if (!initialCommit)
					txnStateStore->set(KeyValueRef(m.param1, m.param2));
			} else if (m.param1.startsWith(applyMutationsEndRange.begin)) {
				if (!initialCommit)
					txnStateStore->set(KeyValueRef(m.param1, m.param2));
				if (uid_applyMutationsData != nullptr) {
					Key uid = m.param1.removePrefix(applyMutationsEndRange.begin);
					auto& p = (*uid_applyMutationsData)[uid];
					p.endVersion = BinaryReader::fromStringRef<Version>(m.param2, Unversioned());
					if (p.keyVersion == Reference<KeyRangeMap<Version>>())
						p.keyVersion = makeReference<KeyRangeMap<Version>>();
					if (!p.worker.isValid() || p.worker.isReady()) {
						auto addPrefixValue =
						    txnStateStore->readValue(uid.withPrefix(applyMutationsAddPrefixRange.begin)).get();
						auto removePrefixValue =
						    txnStateStore->readValue(uid.withPrefix(applyMutationsRemovePrefixRange.begin)).get();
						auto beginValue =
						    txnStateStore->readValue(uid.withPrefix(applyMutationsBeginRange.begin)).get();
						p.worker = applyMutations(
						    cx,
						    uid,
						    addPrefixValue.present() ? addPrefixValue.get() : Key(),
						    removePrefixValue.present() ? removePrefixValue.get() : Key(),
						    beginValue.present() ? BinaryReader::fromStringRef<Version>(beginValue.get(), Unversioned())
						                         : 0,
						    &p.endVersion,
						    commit,
						    commitVersion,
						    p.keyVersion);
					}
				}
			} else if (m.param1.startsWith(applyMutationsKeyVersionMapRange.begin)) {
				if (!initialCommit)
					txnStateStore->set(KeyValueRef(m.param1, m.param2));
				if (uid_applyMutationsData != nullptr) {
					if (m.param1.size() >= applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID)) {
						Key uid = m.param1.substr(applyMutationsKeyVersionMapRange.begin.size(), sizeof(UID));
						Key k = m.param1.substr(applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID));
						auto& p = (*uid_applyMutationsData)[uid];
						if (p.keyVersion == Reference<KeyRangeMap<Version>>())
							p.keyVersion = makeReference<KeyRangeMap<Version>>();
						p.keyVersion->rawInsert(k, BinaryReader::fromStringRef<Version>(m.param2, Unversioned()));
					}
				}
			} else if (m.param1.startsWith(logRangesRange.begin)) {
				if (!initialCommit)
					txnStateStore->set(KeyValueRef(m.param1, m.param2));
				if (vecBackupKeys) {
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

					// Log the modification
					TraceEvent("LogRangeAdd")
					    .detail("LogRanges", vecBackupKeys->size())
					    .detail("MutationKey", m.param1)
					    .detail("LogRangeBegin", logRangeBegin)
					    .detail("LogRangeEnd", logRangeEnd);
				}
			} else if (m.param1.startsWith(globalKeysPrefix)) {
				if (toCommit) {
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
					}

					MutationRef privatized = m;
					privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
					toCommit->addTags(allTags);
					toCommit->writeTypedMessage(privatized);
				}
			} else if (m.param1 == minRequiredCommitVersionKey) {
				Version requested = BinaryReader::fromStringRef<Version>(m.param2, Unversioned());
				TraceEvent("MinRequiredCommitVersion", dbgid).detail("Min", requested).detail("Current", popVersion);
				if (!initialCommit)
					txnStateStore->set(KeyValueRef(m.param1, m.param2));
				confChange = true;
				TEST(true); // Recovering at a higher version.
			} else if (m.param1 == writeRecoveryKey) {
				TraceEvent("WriteRecoveryKeySet", dbgid);
				if (!initialCommit)
					txnStateStore->set(KeyValueRef(m.param1, m.param2));
				TEST(true); // Snapshot created, setting writeRecoveryKey in txnStateStore
			}
		} else if (m.param2.size() > 1 && m.param2[0] == systemKeys.begin[0] && m.type == MutationRef::ClearRange) {
			KeyRangeRef range(m.param1, m.param2);

			if (keyServersKeys.intersects(range)) {
				KeyRangeRef r = range & keyServersKeys;
				if (keyInfo) {
					KeyRangeRef clearRange(r.begin.removePrefix(keyServersPrefix),
					                       r.end.removePrefix(keyServersPrefix));
					keyInfo->insert(clearRange,
					                clearRange.begin == StringRef()
					                    ? ServerCacheInfo()
					                    : keyInfo->rangeContainingKeyBefore(clearRange.begin).value());
				}

				if (!initialCommit)
					txnStateStore->clear(r);
			}
			if (configKeys.intersects(range)) {
				if (!initialCommit)
					txnStateStore->clear(range & configKeys);
				if (!excludedServersKeys.contains(range) && !failedServersKeys.contains(range)) {
					TraceEvent("MutationRequiresRestart", dbgid).detail("M", m.toString());
					confChange = true;
				}
			}
			if (serverListKeys.intersects(range)) {
				if (!initialCommit) {
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
			}
			if (tagLocalityListKeys.intersects(range)) {
				if (!initialCommit)
					txnStateStore->clear(range & tagLocalityListKeys);
			}
			if (serverTagKeys.intersects(range)) {
				// Storage server removal always happens in a separate version from any prior writes (or any subsequent
				// reuse of the tag) so we can safely destroy the tag here without any concern about intra-batch
				// ordering
				if (logSystem && popVersion) {
					auto serverKeysCleared = txnStateStore->readRange(range & serverTagKeys)
					                             .get(); // read is expected to be immediately available
					for (auto& kv : serverKeysCleared) {
						Tag tag = decodeServerTagValue(kv.value);
						TraceEvent("ServerTagRemove")
						    .detail("PopVersion", popVersion)
						    .detail("Tag", tag.toString())
						    .detail("Server", decodeServerTagKey(kv.key));
						logSystem->pop(popVersion, decodeServerTagValue(kv.value));
						(*tag_popped)[tag] = popVersion;

						if (toCommit) {
							MutationRef privatized = m;
							privatized.param1 = kv.key.withPrefix(systemKeys.begin, arena);
							privatized.param2 = keyAfter(kv.key, arena).withPrefix(systemKeys.begin, arena);

							toCommit->addTag(decodeServerTagValue(kv.value));
							toCommit->writeTypedMessage(privatized);
						}
					}
					// Might be a tss removal, which doesn't store a tag there.
					// Chained if is a little verbose, but avoids unecessary work
					if (!initialCommit && !serverKeysCleared.size()) {
						KeyRangeRef maybeTssRange = range & serverTagKeys;
						if (maybeTssRange.singleKeyRange()) {
							UID id = decodeServerTagKey(maybeTssRange.begin);
							Optional<Value> ssiV = txnStateStore->readValue(serverListKeyFor(id)).get();

							if (ssiV.present()) {
								StorageServerInterface ssi = decodeServerListValue(ssiV.get());
								if (ssi.isTss()) {
									Optional<Value> tagV =
									    txnStateStore->readValue(serverTagKeyFor(ssi.tssPairID.get())).get();
									if (tagV.present()) {
										MutationRef privatized = m;
										privatized.param1 = maybeTssRange.begin.withPrefix(systemKeys.begin, arena);
										privatized.param2 =
										    keyAfter(maybeTssRange.begin, arena).withPrefix(systemKeys.begin, arena);

										toCommit->addTag(decodeServerTagValue(tagV.get()));
										toCommit->writeTypedMessage(privatized);
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
			if (serverTagHistoryKeys.intersects(range)) {
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
						logSystem->pop(popVersion, tag);
						(*tag_popped)[tag] = popVersion;
					}
				}
				if (!initialCommit)
					txnStateStore->clear(range & serverTagHistoryKeys);
			}
			if (range.contains(coordinatorsKey)) {
				if (!initialCommit)
					txnStateStore->clear(singleKeyRange(coordinatorsKey));
			}
			if (range.contains(databaseLockedKey)) {
				if (!initialCommit)
					txnStateStore->clear(singleKeyRange(databaseLockedKey));
			}
			if (range.contains(metadataVersionKey)) {
				if (!initialCommit)
					txnStateStore->clear(singleKeyRange(metadataVersionKey));
			}
			if (range.contains(mustContainSystemMutationsKey)) {
				if (!initialCommit)
					txnStateStore->clear(singleKeyRange(mustContainSystemMutationsKey));
			}
			if (range.contains(writeRecoveryKey)) {
				if (!initialCommit)
					txnStateStore->clear(singleKeyRange(writeRecoveryKey));
			}
			if (range.intersects(testOnlyTxnStateStorePrefixRange)) {
				if (!initialCommit)
					txnStateStore->clear(range & testOnlyTxnStateStorePrefixRange);
			}
			if (range.intersects(applyMutationsEndRange)) {
				KeyRangeRef commonEndRange(range & applyMutationsEndRange);
				if (!initialCommit)
					txnStateStore->clear(commonEndRange);
				if (uid_applyMutationsData != nullptr) {
					uid_applyMutationsData->erase(
					    uid_applyMutationsData->lower_bound(m.param1.substr(applyMutationsEndRange.begin.size())),
					    m.param2 == applyMutationsEndRange.end ? uid_applyMutationsData->end()
					                                           : uid_applyMutationsData->lower_bound(m.param2.substr(
					                                                 applyMutationsEndRange.begin.size())));
				}
			}
			if (range.intersects(applyMutationsKeyVersionMapRange)) {
				KeyRangeRef commonApplyRange(range & applyMutationsKeyVersionMapRange);
				if (!initialCommit)
					txnStateStore->clear(commonApplyRange);
				if (uid_applyMutationsData != nullptr) {
					if (m.param1.size() >= applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID) &&
					    m.param2.size() >= applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID)) {
						Key uid = m.param1.substr(applyMutationsKeyVersionMapRange.begin.size(), sizeof(UID));
						Key uid2 = m.param2.substr(applyMutationsKeyVersionMapRange.begin.size(), sizeof(UID));

						if (uid == uid2) {
							auto& p = (*uid_applyMutationsData)[uid];
							if (p.keyVersion == Reference<KeyRangeMap<Version>>())
								p.keyVersion = makeReference<KeyRangeMap<Version>>();
							p.keyVersion->rawErase(KeyRangeRef(
							    m.param1.substr(applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID)),
							    m.param2.substr(applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID))));
						}
					}
				}
			}
			if (range.intersects(logRangesRange)) {
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
		}
	}

	for (KeyRangeRef& range : tssServerListToRemove) {
		txnStateStore->clear(range);
	}

	// If we accumulated private mutations for cached key-ranges, we also need to
	// tag them with the relevant storage servers. This is done to make the storage
	// servers aware of the cached key-ranges
	// NOTE: we are assuming non-colliding cached key-ranges

	// TODO Note that, we are currently not handling the case when cached key-ranges move out
	// to different storage servers. This would require some checking when keys in the keyServersPrefix change.
	// For the first implementation, we could just send the entire map to every storage server. Revisit!
	if (cachedRangeInfo.size() != 0 && toCommit) {
		std::map<KeyRef, MutationRef>::iterator itr;
		KeyRef keyBegin, keyEnd;
		vector<uint16_t> serverIndices;
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
			toCommit->addTags(allTags);
			toCommit->writeTypedMessage(mutationBegin);
			toCommit->addTags(allTags);
			toCommit->writeTypedMessage(mutationEnd);
		}
	}
}

void applyMetadataMutations(SpanID const& spanContext,
                            ProxyCommitData& proxyCommitData,
                            Arena& arena,
                            Reference<ILogSystem> logSystem,
                            const VectorRef<MutationRef>& mutations,
                            LogPushData* toCommit,
                            bool& confChange,
                            Version popVersion,
                            bool initialCommit) {

	std::map<Key, ApplyMutationsData>* uid_applyMutationsData = nullptr;
	if (proxyCommitData.firstProxy) {
		uid_applyMutationsData = &proxyCommitData.uid_applyMutationsData;
	}

	applyMetadataMutations(spanContext,
	                       proxyCommitData.dbgid,
	                       arena,
	                       mutations,
	                       proxyCommitData.txnStateStore,
	                       toCommit,
	                       confChange,
	                       logSystem,
	                       popVersion,
	                       &proxyCommitData.vecBackupKeys,
	                       &proxyCommitData.keyInfo,
	                       &proxyCommitData.cacheInfo,
	                       uid_applyMutationsData,
	                       proxyCommitData.commit,
	                       proxyCommitData.cx,
	                       &proxyCommitData.committedVersion,
	                       &proxyCommitData.storageCache,
	                       &proxyCommitData.tag_popped,
	                       initialCommit);
}

void applyMetadataMutations(SpanID const& spanContext,
                            const UID& dbgid,
                            Arena& arena,
                            const VectorRef<MutationRef>& mutations,
                            IKeyValueStore* txnStateStore) {

	bool confChange; // Dummy variable, not used.

	applyMetadataMutations(spanContext,
	                       dbgid,
	                       arena,
	                       mutations,
	                       txnStateStore,
	                       /* toCommit= */ nullptr,
	                       confChange,
	                       Reference<ILogSystem>(),
	                       /* popVersion= */ 0,
	                       /* vecBackupKeys= */ nullptr,
	                       /* keyInfo= */ nullptr,
	                       /* cacheInfo= */ nullptr,
	                       /* uid_applyMutationsData= */ nullptr,
	                       RequestStream<CommitTransactionRequest>(),
	                       Database(),
	                       /* commitVersion= */ nullptr,
	                       /* storageCache= */ nullptr,
	                       /* tag_popped= */ nullptr,
	                       /* initialCommit= */ false);
}
