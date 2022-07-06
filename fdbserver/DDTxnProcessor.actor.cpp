/*
 * DDTxnProcessor.actor.cpp
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

#include "fdbserver/DDTxnProcessor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class DDTxnProcessorImpl {
	friend class DDTxnProcessor;

	// return {sourceServers, completeSources}
	ACTOR static Future<IDDTxnProcessor::SourceServers> getSourceServersForRange(Database cx, KeyRangeRef keys) {
		state std::set<UID> servers;
		state std::vector<UID> completeSources;
		state Transaction tr(cx);

		loop {
			servers.clear();
			completeSources.clear();

			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			try {
				state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
				RangeResult keyServersEntries = wait(tr.getRange(lastLessOrEqual(keyServersKey(keys.begin)),
				                                                 firstGreaterOrEqual(keyServersKey(keys.end)),
				                                                 SERVER_KNOBS->DD_QUEUE_MAX_KEY_SERVERS));

				if (keyServersEntries.size() < SERVER_KNOBS->DD_QUEUE_MAX_KEY_SERVERS) {
					for (int shard = 0; shard < keyServersEntries.size(); shard++) {
						std::vector<UID> src, dest;
						decodeKeyServersValue(UIDtoTagMap, keyServersEntries[shard].value, src, dest);
						ASSERT(src.size());
						for (int i = 0; i < src.size(); i++) {
							servers.insert(src[i]);
						}
						if (shard == 0) {
							completeSources = src;
						} else {
							for (int i = 0; i < completeSources.size(); i++) {
								if (std::find(src.begin(), src.end(), completeSources[i]) == src.end()) {
									swapAndPop(&completeSources, i--);
								}
							}
						}
					}

					ASSERT(servers.size() > 0);
				}

				// If the size of keyServerEntries is large, then just assume we are using all storage servers
				// Why the size can be large?
				// When a shard is inflight and DD crashes, some destination servers may have already got the data.
				// The new DD will treat the destination servers as source servers. So the size can be large.
				else {
					RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

					for (auto s = serverList.begin(); s != serverList.end(); ++s)
						servers.insert(decodeServerListValue(s->value).id());

					ASSERT(servers.size() > 0);
				}

				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return IDDTxnProcessor::SourceServers{ std::vector<UID>(servers.begin(), servers.end()), completeSources };
	}
};

Future<IDDTxnProcessor::SourceServers> DDTxnProcessor::getSourceServersForRange(const KeyRangeRef range) {
	return DDTxnProcessorImpl::getSourceServersForRange(cx, range);
}

Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> DDTxnProcessor::getServerListAndProcessClasses() {
	Transaction tr(cx);
	return NativeAPI::getServerListAndProcessClasses(&tr);
}