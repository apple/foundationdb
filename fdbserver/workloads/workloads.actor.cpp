/*
 * workloads.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include <cinttypes>

#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/DataDistributionConfig.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Void> customShardConfigWorkload(Database cx) {
	state ReadYourWritesTransaction tr(cx);
	state bool verbose = (KEYBACKEDTYPES_DEBUG != 0);

	// This has to be optional because state vars need default constructors and VersionOptions types can't be
	// default constructed and RangeConfigMap uses ObjectWriter which needs VersionOptions.
	state Optional<DDConfiguration::RangeConfigMap> rangeConfig = DDConfiguration().userRangeConfig();

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			// Map logic should work with or without allKeys endpoints initialized
			if (deterministicRandom()->coinflip()) {
				TraceEvent("KeyRangeConfigSetDefault").log();
				CODE_PROBE(true, "Set default shard range");
				wait(rangeConfig->updateRange(&tr, allKeys.begin, allKeys.end, DDRangeConfig()));
			}

			if (deterministicRandom()->coinflip()) {
				CODE_PROBE(true, "Set range config test cases");
				TraceEvent("KeyRangeConfigSetTestRanges").log();

				wait(rangeConfig->updateRange(&tr, "\xff\x03"_sr, "\xff\x04"_sr, DDRangeConfig(3)));
				wait(rangeConfig->updateRange(&tr, "\xff\x06"_sr, "\xff\x07"_sr, DDRangeConfig(6)));
				wait(rangeConfig->updateRange(&tr, "\xff\x04"_sr, "\xff\x05"_sr, DDRangeConfig(4)));

				wait(rangeConfig->updateRange(&tr, "\xff\x03k"_sr, "\xff\x03z"_sr, DDRangeConfig(3)));
				wait(rangeConfig->updateRange(&tr, "\xff\x04t"_sr, "\xff\x05h"_sr, DDRangeConfig(3, 1), true));
				wait(rangeConfig->updateRange(&tr, "\xff\x06u"_sr, "\xff\x07m"_sr, DDRangeConfig({}, 2)));

				wait(rangeConfig->updateRange(&tr, "a"_sr, "b"_sr, DDRangeConfig(3, 20), true));
				wait(rangeConfig->updateRange(&tr, "a"_sr, "a10"_sr, DDRangeConfig(3, 20), true));

				// Key, entryRequiredInDB, expectedRangeConfig
				state std::vector<std::tuple<Key, bool, DDRangeConfig>> rangeTests = {
					{ "\x01"_sr, false, DDRangeConfig() },
					{ "\xff\x10"_sr, false, DDRangeConfig() },
					{ "\xff\x03x"_sr, true, DDRangeConfig(3) },
					{ "\xff\x04z"_sr, true, DDRangeConfig(3, 1) },
					{ "\xff\x05"_sr, true, DDRangeConfig(3, 1) },
					{ "\xff\x06"_sr, true, DDRangeConfig(6) },
					{ "\xff\x06u"_sr, true, DDRangeConfig(6, 2) },
					{ "\xff\x06v"_sr, true, DDRangeConfig(6, 2) },
					{ "\xff\x07m"_sr, false, DDRangeConfig() },
					{ "\xff\x07k"_sr, true, DDRangeConfig({}, 2) },
					{ "a"_sr, true, DDRangeConfig(3, 20) },
					{ "a10"_sr, true, DDRangeConfig(3, 20) },
					{ "a11"_sr, true, DDRangeConfig(3, 20) },
					{ "b"_sr, false, DDRangeConfig() },
					{ "b1"_sr, false, DDRangeConfig() }
				};

				state Reference<DDConfiguration::RangeConfigMapSnapshot> snapshot =
				    wait(rangeConfig->getSnapshot(&tr, allKeys.begin, allKeys.end));

				if (verbose) {
					fmt::print(
					    "DD User Range Config:\n{}\n",
					    json_spirit::write_string(DDConfiguration::toJSON(*snapshot, true), json_spirit::pretty_print));
				}

				state int i;
				for (i = 0; i < rangeTests.size(); ++i) {
					state Key query = std::get<0>(rangeTests[i]);
					Optional<DDConfiguration::RangeConfigMap::RangeValue> verify =
					    wait(rangeConfig->getRangeForKey(&tr, query));

					if (verbose) {
						if (verify.present()) {
							fmt::print("'{}' is in '{}' to '{}' with config {}\n",
							           query.printable(),
							           verify->range.begin,
							           verify->range.end,
							           verify->value.toString());
						} else {
							fmt::print("'{}' is not in a range in the config\n", query.printable());
						}
					}

					// Range value is either present or not required by test
					ASSERT(verify.present() || !std::get<1>(rangeTests[i]));
					DDRangeConfig rc = std::get<2>(rangeTests[i]);
					ASSERT(!verify.present() || verify->value == rc);

					auto snapshotRange = snapshot->rangeContaining(query);
					ASSERT(snapshotRange.value() == rc);
					// The snapshot has all ranges covered but the db may not
					if (verify.present()) {
						ASSERT(snapshotRange.range().begin == verify->range.begin);
						ASSERT(snapshotRange.range().end == verify->range.end);
					}
				}
			}

			wait(tr.commit());
			TraceEvent("KeyRangeConfigCommitted").log();
			break;
		} catch (Error& e) {
			TraceEvent("KeyRangeConfigCommitError").error(e);
			wait(tr.onError(e));
		}
	}
	return Void();
}
