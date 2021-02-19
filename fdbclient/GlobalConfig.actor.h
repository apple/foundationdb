/*
 * GlobalConfig.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_GLOBALCONFIG_ACTOR_G_H)
#define FDBCLIENT_GLOBALCONFIG_ACTOR_G_H
#include "fdbclient/GlobalConfig.actor.g.h"
#elif !defined(FDBCLIENT_GLOBALCONFIG_ACTOR_H)
#define FDBCLIENT_GLOBALCONFIG_ACTOR_H

#include <any>
#include <map>
#include <unordered_map>

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tuple.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/Knobs.h"

#include "flow/actorcompiler.h" // has to be last include

class GlobalConfig {
public:
	GlobalConfig();
	GlobalConfig(const GlobalConfig&) = delete;
	GlobalConfig& operator=(const GlobalConfig&) = delete;

	static void create(DatabaseContext* cx, Reference<AsyncVar<ClientDBInfo>> dbInfo);
	static GlobalConfig& globalConfig();
	const std::any get(KeyRef name);
	const std::map<KeyRef, std::any> get(KeyRangeRef range);
	Future<Void> onInitialized();

private:
	void insert(KeyRef key, ValueRef value);
	void erase(KeyRef key);
	void erase(KeyRangeRef range);

	ACTOR static Future<Void> refresh(GlobalConfig* self) {
		Transaction tr(self->cx);
		Standalone<RangeResultRef> result = wait(tr.getRange(globalConfigDataKeys, CLIENT_KNOBS->TOO_MANY));
		for (const auto& kv : result) {
			KeyRef systemKey = kv.key.removePrefix(globalConfigDataPrefix);
			self->insert(systemKey, kv.value);
		}
		return Void();
	}

	ACTOR static Future<Void> updater(GlobalConfig* self, Reference<AsyncVar<ClientDBInfo>> dbInfo) {
		wait(refresh(self));
		self->initialized.send(Void());

		loop {
			try {
				wait(dbInfo->onChange());

				auto& history = dbInfo->get().history;
				if (history.size() == 0 || (self->lastUpdate < history[0].first && self->lastUpdate != 0)) {
					// This process missed too many global configuration
					// history updates or the protocol version changed, so it
					// must re-read the entire configuration range.
					wait(refresh(self));
					self->lastUpdate = dbInfo->get().history.back().contents().first;
				} else {
					// Apply history in order, from lowest version to highest
					// version. Mutation history should already be stored in
					// ascending version order.
					for (int i = 0; i < history.size(); ++i) {
						const std::pair<Version, VectorRef<MutationRef>>& pair = history[i].contents();

						Version version = pair.first;
						if (version <= self->lastUpdate) {
							continue;  // already applied this mutation
						}

						const VectorRef<MutationRef>& mutations = pair.second;
						for (const auto& mutation : mutations) {
							if (mutation.type == MutationRef::SetValue) {
								self->insert(mutation.param1, mutation.param2);
							} else if (mutation.type == MutationRef::ClearRange) {
								self->erase(KeyRangeRef(mutation.param1, mutation.param2));
							} else {
								ASSERT(false);
							}
						}

						ASSERT(version > self->lastUpdate);
						self->lastUpdate = version;
					}
				}
			} catch (Error& e) {
				throw;
			}
		}
	}

	Database cx;
	Future<Void> _updater;
	Promise<Void> initialized;
	Arena arena;
	std::unordered_map<StringRef, std::any> data;
	Version lastUpdate;
};

#endif
