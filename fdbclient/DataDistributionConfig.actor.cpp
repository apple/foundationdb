/*
 * DataDistributionConfig.actor.cpp
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

#include "fdbclient/DataDistributionConfig.actor.h"
#include "flow/actorcompiler.h"

// Read all key ranges of a RangeConfigMap and return it as a RangeConfigMapSnapshot
// allKeys.begin/end will be set to default values in the result if they do not exist in the database
//
// TODO: Move this to a more generic function in KeyBackedRangeMap, which will need to understand more than it currently
// does about the KeyType.
ACTOR Future<DDConfiguration::RangeConfigMapSnapshot> DDConfiguration::readRangeMap(
    Reference<ReadYourWritesTransaction> tr,
    DDConfiguration::RangeConfigMap map) {
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

	state DDConfiguration::RangeConfigMapSnapshot result;
	result.map[allKeys.begin] = DDRangeConfig();
	result.map[allKeys.end] = DDRangeConfig();
	state Key begin = allKeys.begin;

	loop {
		DDConfiguration::RangeConfigMap::RangeMapResult boundaries =
		    wait(DDConfiguration().userRangeConfig().getRanges(tr, begin, allKeys.end, false));
		for (auto& kv : boundaries.results) {
			result.map[kv.key] = kv.value;
		}

		if (!boundaries.more) {
			break;
		}
		ASSERT(!boundaries.results.empty());
		begin = keyAfter(boundaries.results.back().key);
	}

	return result;
}
