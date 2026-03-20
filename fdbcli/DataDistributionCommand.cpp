/*
 * DataDistributionCommand.cpp
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

#include "boost/lexical_cast.hpp"

#include "fdbcli/fdbcli.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

namespace {

Future<Void> setDDMode(Reference<IDatabase> db, int mode) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		Error err;
		try {
			tr->set(fdb_cli::ddModeSpecialKey, boost::lexical_cast<std::string>(mode));
			if (mode) {
				// set DDMode to 1 will enable all disabled parts, for instance the SS failure monitors.
				// hold the returned standalone object's memory
				ThreadFuture<RangeResult> resultFuture =
				    tr->getRange(fdb_cli::maintenanceSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
				RangeResult res = co_await safeThreadFutureToFuture(resultFuture);
				ASSERT(res.size() <= 1);
				if (res.size() == 1 && res[0].key == fdb_cli::ignoreSSFailureSpecialKey) {
					// only clear the key if it is currently being used to disable all SS failure data movement
					tr->clear(fdb_cli::maintenanceSpecialKeyRange);
				}
				tr->clear(fdb_cli::ddIgnoreRebalanceSpecialKey);
			}
			co_await safeThreadFutureToFuture(tr->commit());
			co_return;
		} catch (Error& e) {
			err = e;
		}
		TraceEvent("SetDDModeRetrying").error(err);
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

Future<Void> setDDIgnoreRebalanceSwitch(Reference<IDatabase> db, uint8_t DDIgnoreOptionMask, bool setMaskedBit) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		Error err;
		try {
			ThreadFuture<Optional<Value>> resultFuture = tr->get(rebalanceDDIgnoreKey);
			Optional<Value> v = co_await safeThreadFutureToFuture(resultFuture);
			uint8_t oldValue = DDIgnore::NONE; // nothing is disabled
			if (v.present()) {
				if (v.get().size() > 0) {
					oldValue = BinaryReader::fromStringRef<uint8_t>(v.get(), Unversioned());
				} else {
					// In old version (<= 7.1), the value is an empty string, which means all DD rebalance functions
					// are disabled
					oldValue = DDIgnore::ALL;
				}
				// printf("oldValue: %d Mask: %d V:%d\n", oldValue, DDIgnoreOptionMask, v.get().size());
			}
			uint8_t newValue = setMaskedBit ? (oldValue | DDIgnoreOptionMask) : (oldValue & ~DDIgnoreOptionMask);
			if (newValue > 0) {
				tr->set(fdb_cli::ddIgnoreRebalanceSpecialKey, BinaryWriter::toValue(newValue, Unversioned()));
			} else {
				tr->clear(fdb_cli::ddIgnoreRebalanceSpecialKey);
			}
			co_await safeThreadFutureToFuture(tr->commit());
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

// set masked bit
Future<Void> setDDIgnoreRebalanceOn(Reference<IDatabase> db, uint8_t DDIgnoreOptionMask) {
	return setDDIgnoreRebalanceSwitch(db, DDIgnoreOptionMask, true);
}

// reset masked bit
Future<Void> setDDIgnoreRebalanceOff(Reference<IDatabase> db, uint8_t DDIgnoreOptionMask) {
	return setDDIgnoreRebalanceSwitch(db, DDIgnoreOptionMask, false);
}

} // namespace

namespace fdb_cli {

const KeyRef ddModeSpecialKey = "\xff\xff/management/data_distribution/mode"_sr;
const KeyRef ddIgnoreRebalanceSpecialKey = "\xff\xff/management/data_distribution/rebalance_ignored"_sr;
constexpr auto usage =
    "Usage: datadistribution <on|off|disable <ssfailure|rebalance|rebalance_disk|rebalance_read>|enable "
    "<ssfailure|rebalance|rebalance_disk|rebalance_read>>\n";
Future<bool> dataDistributionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	bool result = true;
	if (tokens.size() != 2 && tokens.size() != 3) {
		printf(usage);
		result = false;
	} else {
		if (tokencmp(tokens[1], "on")) {
			co_await setDDMode(db, 1);
			printf("Data distribution is turned on.\n");
		} else if (tokencmp(tokens[1], "off")) {
			co_await setDDMode(db, 0);
			printf("Data distribution is turned off.\n");
		} else if (tokencmp(tokens[1], "disable")) {
			if (tokencmp(tokens[2], "ssfailure")) {
				co_await (setHealthyZone(db, "IgnoreSSFailures"_sr, 0));
				printf("Data distribution is disabled for storage server failures.\n");
			} else if (tokencmp(tokens[2], "rebalance")) {
				co_await setDDIgnoreRebalanceOn(db, DDIgnore::REBALANCE_DISK | DDIgnore::REBALANCE_READ);
				printf("Data distribution is disabled for rebalance.\n");
			} else if (tokencmp(tokens[2], "rebalance_disk")) {
				co_await setDDIgnoreRebalanceOn(db, DDIgnore::REBALANCE_DISK);
				printf("Data distribution is disabled for rebalance_disk.\n");
			} else if (tokencmp(tokens[2], "rebalance_read")) {
				co_await setDDIgnoreRebalanceOn(db, DDIgnore::REBALANCE_READ);
				printf("Data distribution is disabled for rebalance_read.\n");
			} else {
				printf(usage);
				result = false;
			}
		} else if (tokencmp(tokens[1], "enable")) {
			if (tokencmp(tokens[2], "ssfailure")) {
				co_await (clearHealthyZone(db, false, true));
				printf("Data distribution is enabled for storage server failures.\n");
			} else if (tokencmp(tokens[2], "rebalance")) {
				co_await setDDIgnoreRebalanceOff(db, DDIgnore::REBALANCE_DISK | DDIgnore::REBALANCE_READ);
				printf("Data distribution is enabled for rebalance.\n");
			} else if (tokencmp(tokens[2], "rebalance_disk")) {
				co_await setDDIgnoreRebalanceOff(db, DDIgnore::REBALANCE_DISK);
				printf("Data distribution is enabled for rebalance_disk.\n");
			} else if (tokencmp(tokens[2], "rebalance_read")) {
				co_await setDDIgnoreRebalanceOff(db, DDIgnore::REBALANCE_READ);
				printf("Data distribution is enabled for rebalance_read.\n");
			} else {
				printf(usage);
				result = false;
			}
		} else {
			printf(usage);
			result = false;
		}
	}
	co_return result;
}

// hidden commands, no help text for now
CommandFactory dataDistributionFactory("datadistribution");
} // namespace fdb_cli
