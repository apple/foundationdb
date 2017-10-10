/*
 * ApplyMetadataMutation.h
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

#ifndef FDBSERVER_APPLYMETADATAMUTATION_H
#define FDBSERVER_APPLYMETADATAMUTATION_H
#pragma once

#include "fdbclient/MutationList.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/Notified.h"
#include "IKeyValueStore.h"
#include "LogSystem.h"
#include "LogProtocolMessage.h"

static bool isMetadataMutation(MutationRef const& m) {
	// FIXME: This is conservative - not everything in system keyspace is necessarily processed by applyMetadataMutations
	return (m.type == MutationRef::SetValue && m.param1.size() && m.param1[0] == systemKeys.begin[0] && !m.param1.startsWith(nonMetadataSystemKeys.begin)) ||
		(m.type == MutationRef::ClearRange && m.param2.size() && m.param2[0] == systemKeys.begin[0] && !nonMetadataSystemKeys.contains(KeyRangeRef(m.param1, m.param2)) );
}

struct applyMutationsData {
	Future<Void> worker;
	Version endVersion;
	Reference<KeyRangeMap<Version>> keyVersion;
};

static void applyMetadataMutations(UID const& dbgid, Arena &arena, VectorRef<MutationRef> const& mutations, IKeyValueStore* txnStateStore, LogPushData* toCommit, bool *confChange, Reference<ILogSystem> logSystem = Reference<ILogSystem>(), Version popVersion = 0,
	KeyRangeMap<std::set<Key> >* vecBackupKeys = NULL, KeyRangeMap<std::vector<Tag>>* keyTags = NULL, std::map<Key, applyMutationsData>* uid_applyMutationsData = NULL,
	RequestStream<CommitTransactionRequest> commit = RequestStream<CommitTransactionRequest>(), Database cx = Database(), NotifiedVersion* commitVersion = NULL, std::map<UID, Tag> *tagCache = NULL, bool initialCommit = false) {
	for (auto const& m : mutations) {
		//TraceEvent("MetadataMutation", dbgid).detail("M", m.toString());

		if (m.param1.size() && m.param1[0] == systemKeys.begin[0] && m.type == MutationRef::SetValue) {
			if(m.param1.startsWith(keyServersPrefix)) {
				if(keyTags) {
					KeyRef k = m.param1.removePrefix(keyServersPrefix);
					if(k != allKeys.end) {
						KeyRef end = keyTags->rangeContaining(k).end();
						KeyRangeRef insertRange(k,end);
						vector<UID> src, dest;
						decodeKeyServersValue(m.param2, src, dest);
						std::set<Tag> tags;

						if(!tagCache) {
							for(auto id : src)
								tags.insert(decodeServerTagValue(txnStateStore->readValue(serverTagKeyFor(id)).get().get()));
							for(auto id : dest)
								tags.insert(decodeServerTagValue(txnStateStore->readValue(serverTagKeyFor(id)).get().get()));
						}
						else {
							Tag tag;
							for(auto id : src) {
								auto tagItr = tagCache->find(id);
								if(tagItr == tagCache->end()) {
									tag = decodeServerTagValue( txnStateStore->readValue( serverTagKeyFor(id) ).get().get() );
									(*tagCache)[id] = tag;
								} else {
									tag = tagItr->second;
								}
								tags.insert( tag );
							}
							for(auto id : dest) {
								auto tagItr = tagCache->find(id);
								if(tagItr == tagCache->end()) {
									tag = decodeServerTagValue( txnStateStore->readValue( serverTagKeyFor(id) ).get().get() );
									(*tagCache)[id] = tag;
								} else {
									tag = tagItr->second;
								}
								tags.insert( tag );
							}
						}

						keyTags->insert(insertRange,std::vector<Tag>(tags.begin(), tags.end()));
					}
				}
				if(!initialCommit) txnStateStore->set(KeyValueRef(m.param1, m.param2));
			} else if (m.param1.startsWith(serverKeysPrefix)) {
				if(toCommit) {
					MutationRef privatized = m;
					privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
					TraceEvent(SevDebug, "SendingPrivateMutation", dbgid).detail("Original", m.toString()).detail("Privatized", privatized.toString()).detail("Server", serverKeysDecodeServer(m.param1))
						.detail("tagKey", printable(serverTagKeyFor( serverKeysDecodeServer(m.param1) ))).detail("tag", decodeServerTagValue( txnStateStore->readValue( serverTagKeyFor( serverKeysDecodeServer(m.param1) ) ).get().get() ).toString());

					toCommit->addTag( decodeServerTagValue( txnStateStore->readValue( serverTagKeyFor( serverKeysDecodeServer(m.param1) ) ).get().get() ) );
					toCommit->addTypedMessage(privatized);
				}
			} else if (m.param1.startsWith(serverTagPrefix)) {
				UID id = decodeServerTagKey(m.param1);
				Tag tag = decodeServerTagValue(m.param2);

				if(toCommit) {
					MutationRef privatized = m;
					privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
					TraceEvent("ServerTag", dbgid).detail("server", id).detail("tag", tag.toString());

					toCommit->addTag(tag);
					toCommit->addTypedMessage(LogProtocolMessage());
					toCommit->addTag(tag);
					toCommit->addTypedMessage(privatized);
				}
				if(!initialCommit) {
					txnStateStore->set(KeyValueRef(m.param1, m.param2));
					if(tagCache) {
						(*tagCache)[id] = tag;
					}
				}
			}
			else if (m.param1.startsWith(configKeysPrefix) || m.param1 == coordinatorsKey) {
				if(Optional<StringRef>(m.param2) != txnStateStore->readValue(m.param1).get().cast_to<StringRef>()) { // FIXME: Make this check more specific, here or by reading configuration whenever there is a change
					auto t = txnStateStore->readValue(m.param1).get();
					if (logSystem && m.param1.startsWith( excludedServersPrefix )) {
						// If one of our existing tLogs is now excluded, we have to die and recover
						auto addr = decodeExcludedServersKey(m.param1);
						for( auto& logs : logSystem->getLogSystemConfig().tLogs ) {
							for( auto& tl : logs.tLogs ) {
								if(!tl.present() || addr.excludes(tl.interf().commit.getEndpoint().address)) {
									TraceEvent("MutationRequiresRestart", dbgid).detail("M", m.toString()).detail("PrevValue", t.present() ? printable(t.get()) : "(none)").detail("toCommit", toCommit!=NULL).detail("addr", addr.toString());
									if(confChange) *confChange = true;
								}
							}
						}
					} else if(m.param1 != excludedServersVersionKey) {
						TraceEvent("MutationRequiresRestart", dbgid).detail("M", m.toString()).detail("PrevValue", t.present() ? printable(t.get()) : "(none)").detail("toCommit", toCommit!=NULL);
						if(confChange) *confChange = true;
					}
				}
				if(!initialCommit) txnStateStore->set(KeyValueRef(m.param1, m.param2));
			}
			else if (m.param1.startsWith(serverListPrefix) || m.param1 == databaseLockedKey || m.param1.startsWith(applyMutationsBeginRange.begin) ||
				m.param1.startsWith(applyMutationsAddPrefixRange.begin) || m.param1.startsWith(applyMutationsRemovePrefixRange.begin) || m.param1.startsWith(tagLocalityListPrefix) ) {
				if(!initialCommit) txnStateStore->set(KeyValueRef(m.param1, m.param2));
			}
			else if (m.param1.startsWith(applyMutationsEndRange.begin)) {
				if(!initialCommit) txnStateStore->set(KeyValueRef(m.param1, m.param2));
				if(uid_applyMutationsData != NULL) {
					Key uid = m.param1.removePrefix(applyMutationsEndRange.begin);
					auto &p = (*uid_applyMutationsData)[uid];
					p.endVersion = BinaryReader::fromStringRef<Version>(m.param2, Unversioned());
					if(p.keyVersion == Reference<KeyRangeMap<Version>>())
						p.keyVersion = Reference<KeyRangeMap<Version>>( new KeyRangeMap<Version>() );
					if(!p.worker.isValid() || p.worker.isReady()) {
						auto addPrefixValue = txnStateStore->readValue(uid.withPrefix(applyMutationsAddPrefixRange.begin)).get();
						auto removePrefixValue = txnStateStore->readValue(uid.withPrefix(applyMutationsRemovePrefixRange.begin)).get();
						auto beginValue = txnStateStore->readValue(uid.withPrefix(applyMutationsBeginRange.begin)).get();
						p.worker = applyMutations( cx, uid, addPrefixValue.present() ? addPrefixValue.get() : Key(),
							removePrefixValue.present() ? removePrefixValue.get() : Key(),
							beginValue.present() ? BinaryReader::fromStringRef<Version>(beginValue.get(), Unversioned()) : 0,
							&p.endVersion, commit, commitVersion, p.keyVersion );
					}
				}
			}
			else if (m.param1.startsWith(applyMutationsKeyVersionMapRange.begin)) {
				if(!initialCommit) txnStateStore->set(KeyValueRef(m.param1, m.param2));
				if(uid_applyMutationsData != NULL) {
					if(m.param1.size() >= applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID)) {
						Key uid = m.param1.substr(applyMutationsKeyVersionMapRange.begin.size(), sizeof(UID));
						Key k = m.param1.substr(applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID));
						auto &p = (*uid_applyMutationsData)[uid];
						if(p.keyVersion == Reference<KeyRangeMap<Version>>())
							p.keyVersion = Reference<KeyRangeMap<Version>>( new KeyRangeMap<Version>() );
						p.keyVersion->rawInsert( k, BinaryReader::fromStringRef<Version>(m.param2, Unversioned()) );
					}
				}
			}
			else if (m.param1.startsWith(logRangesRange.begin)) {
				if(!initialCommit) txnStateStore->set(KeyValueRef(m.param1, m.param2));
				if (vecBackupKeys) {
					Key logDestination;
					KeyRef logRangeBegin = logRangesDecodeKey(m.param1, NULL);
					Key	logRangeEnd = logRangesDecodeValue(m.param2, &logDestination);

					// Insert the logDestination into each range of vecBackupKeys overlapping the decoded range
					for (auto& logRange : vecBackupKeys->modify(KeyRangeRef(logRangeBegin, logRangeEnd))) {
						logRange->value().insert(logDestination);
					}

					// Log the modification
					TraceEvent("LogRangeAdd").detail("logRanges", vecBackupKeys->size()).detail("mutationKey", printable(m.param1))
						.detail("logRangeBegin", printable(logRangeBegin)).detail("logRangeEnd", printable(logRangeEnd));
				}
			}
			else if (m.param1.startsWith(globalKeysPrefix)) {
				if(toCommit) {
					// Notifies all servers that a Master's server epoch ends
					auto allServers = txnStateStore->readRange(serverTagKeys).get();
					std::set<Tag> allTags;
					for (auto &kv : allServers)
						allTags.insert(decodeServerTagValue(kv.value));

					if (m.param1 == lastEpochEndKey) {
						for (auto t : allTags)
							toCommit->addTag(t);
						toCommit->addTypedMessage(LogProtocolMessage());
					}

					MutationRef privatized = m;
					privatized.param1 = m.param1.withPrefix(systemKeys.begin, arena);
					for (auto t : allTags)
						toCommit->addTag(t);
					toCommit->addTypedMessage(privatized);
				}
			}
			else if (m.param1 == minRequiredCommitVersionKey) {
				Version requested = BinaryReader::fromStringRef<Version>(m.param2, Unversioned());
				TraceEvent("MinRequiredCommitVersion", dbgid).detail("min", requested).detail("current", popVersion).detail("hasConf", !!confChange);
				if(!initialCommit) txnStateStore->set(KeyValueRef(m.param1, m.param2));
				if (confChange) *confChange = true;
				TEST(true);  // Recovering at a higher version.
			}
		}
		else if (m.param2.size() && m.param2[0] == systemKeys.begin[0] && m.type == MutationRef::ClearRange) {
			KeyRangeRef range(m.param1, m.param2);

			if (keyServersKeys.intersects(range)) {
				KeyRangeRef r = range & keyServersKeys;
				if(keyTags) {
					KeyRangeRef clearRange(r.begin.removePrefix(keyServersPrefix), r.end.removePrefix(keyServersPrefix));
					keyTags->insert(clearRange, clearRange.begin == StringRef() ? vector<Tag>() : keyTags->rangeContainingKeyBefore(clearRange.begin).value());
				}

				if(!initialCommit) txnStateStore->clear(r);
			}
			if (configKeys.intersects(range)) {
				if(!initialCommit) txnStateStore->clear(range & configKeys);
				if(!excludedServersKeys.contains(range)) {
					TraceEvent("MutationRequiresRestart", dbgid).detail("M", m.toString());
					if(confChange) *confChange = true;
				}
			}
			if ( serverListKeys.intersects( range )) {
				if(!initialCommit) txnStateStore->clear( range & serverListKeys );
			}
			if ( tagLocalityListKeys.intersects( range )) {
				if(!initialCommit) txnStateStore->clear( range & tagLocalityListKeys );
			}
			if ( serverTagKeys.intersects( range )) {
				// Storage server removal always happens in a separate version from any prior writes (or any subsequent reuse of the tag) so we
				// can safely destroy the tag here without any concern about intra-batch ordering
				if (logSystem && popVersion) {
					auto serverKeysCleared = txnStateStore->readRange( range & serverTagKeys ).get();	// read is expected to be immediately available
					for(auto &kv : serverKeysCleared) {
						TraceEvent("ServerTagRemove").detail("popVersion", popVersion).detail("tag", decodeServerTagValue(kv.value).toString()).detail("server", decodeServerTagKey(kv.key));
						logSystem->pop( popVersion, decodeServerTagValue(kv.value) );

						if(toCommit) {
							MutationRef privatized = m;
							privatized.param1 = kv.key.withPrefix(systemKeys.begin, arena);
							privatized.param2 = keyAfter(kv.key, arena).withPrefix(systemKeys.begin, arena);

							toCommit->addTag(decodeServerTagValue(kv.value));
							toCommit->addTypedMessage(privatized);
						}
					}
				}
				if(!initialCommit) txnStateStore->clear( range & serverTagKeys );
			}
			if (range.contains(coordinatorsKey)) {
				if(!initialCommit) txnStateStore->clear(singleKeyRange(coordinatorsKey));
			}
			if (range.contains(databaseLockedKey)) {
				if(!initialCommit) txnStateStore->clear(singleKeyRange(databaseLockedKey));
			}
			if(range.intersects(applyMutationsEndRange)) {
				KeyRangeRef commonEndRange(range & applyMutationsEndRange);
				if(!initialCommit) txnStateStore->clear(commonEndRange);
				if(uid_applyMutationsData != NULL) {
					uid_applyMutationsData->erase(uid_applyMutationsData->lower_bound(m.param1.substr(applyMutationsEndRange.begin.size())),
						m.param2 == applyMutationsEndRange.end ? uid_applyMutationsData->end() : uid_applyMutationsData->lower_bound(m.param2.substr(applyMutationsEndRange.begin.size())));
				}
			}
			if(range.intersects(applyMutationsKeyVersionMapRange)) {
				KeyRangeRef commonApplyRange(range & applyMutationsKeyVersionMapRange);
				if(!initialCommit) txnStateStore->clear(commonApplyRange);
				if(uid_applyMutationsData != NULL) {
					if(m.param1.size() >= applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID) && m.param2.size() >= applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID)) {
						Key uid = m.param1.substr(applyMutationsKeyVersionMapRange.begin.size(), sizeof(UID));
						Key uid2 = m.param2.substr(applyMutationsKeyVersionMapRange.begin.size(), sizeof(UID));

						if(uid == uid2) {
							auto &p = (*uid_applyMutationsData)[uid];
							if(p.keyVersion == Reference<KeyRangeMap<Version>>())
								p.keyVersion = Reference<KeyRangeMap<Version>>( new KeyRangeMap<Version>() );
							p.keyVersion->rawErase( KeyRangeRef( m.param1.substr(applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID)), m.param2.substr(applyMutationsKeyVersionMapRange.begin.size() + sizeof(UID))) );
						}
					}
				}
			}
			if (range.intersects(logRangesRange)) {
				KeyRangeRef commonLogRange(range & logRangesRange);

				TraceEvent("LogRangeClear")
					.detail("rangeBegin", printable(range.begin)).detail("rangeEnd", printable(range.end))
					.detail("intersectBegin", printable(commonLogRange.begin)).detail("intersectEnd", printable(commonLogRange.end));

				// Remove the key range from the vector, if defined
				if (vecBackupKeys) {
					KeyRef	logKeyBegin;
					Key		logKeyEnd, logDestination;

					// Identify the backup keys being removed
					// read is expected to be immediately available
					auto logRangesAffected = txnStateStore->readRange(commonLogRange).get();

					TraceEvent("LogRangeClearBegin").detail("affectedLogRanges", logRangesAffected.size());

					// Add the backup name to the backup locations that do not have it
					for (auto logRangeAffected : logRangesAffected)
					{
						// Parse the backup key and name
						logKeyBegin = logRangesDecodeKey(logRangeAffected.key, NULL);

						// Decode the log destination and key value
						logKeyEnd = logRangesDecodeValue(logRangeAffected.value, &logDestination);

						TraceEvent("LogRangeErase").detail("affectedKey", printable(logRangeAffected.key)).detail("affectedValue", printable(logRangeAffected.value))
							.detail("logKeyBegin", printable(logKeyBegin)).detail("logKeyEnd", printable(logKeyEnd))
							.detail("logDestination", printable(logDestination));

						// Identify the locations to place the backup key
						auto logRanges = vecBackupKeys->modify(KeyRangeRef(logKeyBegin, logKeyEnd));

						// Remove the log prefix from the ranges which include it
						for (auto logRange : logRanges)
						{
							auto	&logRangeMap = logRange->value();

							// Remove the backup name from the range
							logRangeMap.erase(logDestination);
						}
					}

					// Coallesce the entire range
					vecBackupKeys->coalesce(allKeys);
				}

				if(!initialCommit) txnStateStore->clear(commonLogRange);
			}
		}
	}
}

#endif
