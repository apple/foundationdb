/*
 * ConfigurationDatabase.actor.cpp
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

#include "fdbclient/IConfigTransaction.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/ConfigFollowerInterface.h"
#include "fdbserver/IConfigBroadcaster.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // has to be last include

class ConfigurationDatabaseWorkload : public TestWorkload {

	ACTOR static Future<int> getCurrentValue(Database cx, Key key) {
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		state int result = 0;
		loop {
			try {
				Optional<Value> value = wait(tr.get(key));
				if (value.present()) {
					result = BinaryReader::fromStringRef<int>(value.get(), IncludeVersion());
				}
				return result;
			} catch (Error &e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> increment(Database cx, Key key) {
		state SimpleConfigTransaction tr(cx->getConnectionFile()->getConnectionString());
		loop {
			try {
				state int currentValue = 0;
				Optional<Value> value = wait(tr.get(key));
				if (value.present()) {
					currentValue = BinaryReader::fromStringRef<int>(value.get(), IncludeVersion());
				}
				++currentValue;
				tr.set(key, BinaryWriter::toValue(currentValue, IncludeVersion()));
				wait(tr.commit());
				return Void();
			} catch (Error &e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> runClient(Database cx) {
		state Key key = LiteralStringRef("key");
		int currentValue = wait(getCurrentValue(cx, key));
		ASSERT(currentValue == 0);
		wait(increment(cx, key));
		int newValue = wait(getCurrentValue(cx, key));
		ASSERT(newValue == 1);
		return Void();
	}

	ACTOR static Future<Void> runBroadcaster(Database cx, ConfigFollowerInterface cfi) {
		state SimpleConfigBroadcaster broadcaster(cx->getConnectionFile()->getConnectionString());
		wait(success(timeout(broadcaster.serve(cfi), 60.0)));
		return Void();
	}

	ACTOR static Future<Version> getCurrentVersion(ConfigFollowerInterface cfi) {
		ConfigFollowerGetVersionReply versionReply = wait(cfi.getVersion.getReply(ConfigFollowerGetVersionRequest{}));
		return versionReply.version;
	}

	ACTOR static Future<Void> runConsumer(ConfigFollowerInterface cfi) {
		state Version mostRecentVersion = wait(getCurrentVersion(cfi));
		ConfigFollowerGetFullDatabaseReply fullDBReply = wait(cfi.getFullDatabase.getReply(ConfigFollowerGetFullDatabaseRequest{mostRecentVersion, {}}));
		state std::map<Key, Value> database = fullDBReply.database;
		state int runs = 0;
		loop {
			state ConfigFollowerGetChangesReply changesReply = wait(cfi.getChanges.getReply(ConfigFollowerGetChangesRequest{mostRecentVersion, {}}));
			wait(delay(1.0));
			if (++runs > 5) {
				return Void();
			}
		}
	}

	ACTOR static Future<Void> start(ConfigurationDatabaseWorkload *self, Database cx) {
		state std::vector<Future<Void>> futures;
		state ConfigFollowerInterface cfi;
		futures.push_back(runClient(cx));
		futures.push_back(runBroadcaster(cx, cfi));
		futures.push_back(runConsumer(cfi));
		wait(waitForAll(futures));
		return Void();
	}

public:
	ConfigurationDatabaseWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : start(this, cx); }
	Future<bool> check(Database const& cx) override { return true; }
	std::string description() const override { return "ConfigurationDatabase"; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ConfigurationDatabaseWorkload> ConfigurationDatabaseWorkloadFactory("ConfigurationDatabase");
