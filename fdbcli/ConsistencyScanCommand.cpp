/*
 * ConsistencyScanCommand.cpp
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

#include <boost/lexical_cast.hpp>
#include <list>
#include "fdbcli/fdbcli.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.h"
#include "fdbclient/ConsistencyScanInterface.actor.h"

namespace fdb_cli {

Future<Void> dumpStats(ConsistencyScanState* cs, Reference<ReadYourWritesTransaction> tr) {
	ConsistencyScanState::LifetimeStats statsLifetime;
	ConsistencyScanState::RoundStats statsCurrentRound;
	co_await (store(statsLifetime, cs->lifetimeStats().getD(tr)) &&
	          store(statsCurrentRound, cs->currentRoundStats().getD(tr)));
	printf(
	    "Current Round:\n%s\n",
	    json_spirit::write_string(json_spirit::mValue(statsCurrentRound.toJSON()), json_spirit::pretty_print).c_str());
	printf("Lifetime:\n%s\n",
	       json_spirit::write_string(json_spirit::mValue(statsLifetime.toJSON()), json_spirit::pretty_print).c_str());
	co_return;
}

Future<bool> consistencyScanCommandActor(Database db, std::vector<StringRef> const& tokens) {
	// Skip the command token so start at begin+1
	std::list<StringRef> args(tokens.begin() + 1, tokens.end());

	ConsistencyScanState cs = ConsistencyScanState();
	auto tr = makeReference<ReadYourWritesTransaction>(db);
	bool error = false;

	while (true) {
		Error err;
		try {
			SystemDBWriteLockedNow(db.getReference())->setOptions(tr);

			ConsistencyScanState::Config config = co_await ConsistencyScanState().config().getD(tr);

			if (args.empty()) {
				printf(
				    "%s\n",
				    json_spirit::write_string(json_spirit::mValue(config.toJSON()), json_spirit::pretty_print).c_str());
				break;
			}

			// TODO:  Expose/document additional configuration options
			// TODO:  Range configuration.
			while (!error && !args.empty()) {
				auto next = args.front();
				args.pop_front();
				if (next == "on") {
					config.enabled = true;
				} else if (next == "off") {
					config.enabled = false;
				} else if (next == "restart") {
					config.minStartVersion = tr->getReadVersion().get();
				} else if (next == "stats") {
					co_await dumpStats(&cs, tr);
				} else if (next == "clearstats") {
					co_await cs.clearStats(tr);
				} else if (next == "maxRate") {
					error = args.empty();
					if (!error) {
						config.maxReadByteRate = boost::lexical_cast<int>(args.front().toString());
						args.pop_front();
					}
				} else if (next == "targetInterval") {
					error = args.empty();
					if (!error) {
						config.targetRoundTimeSeconds = boost::lexical_cast<int>(args.front().toString());
						args.pop_front();
					}
				}
			}

			if (error) {
				break;
			}
			cs.config().set(tr, config);
			co_await tr->commit();
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}

	if (error) {
		printUsage(tokens[0]);
		co_return false;
	}

	co_return true;
}

CommandFactory consistencyScanFactory(
    "consistencyscan",
    CommandHelp(
        // TODO:  Expose/document additional configuration options
        "consistencyscan [on|off] [restart] [stats] [clearstats] [maxRate <BYTES_PER_SECOND>] [targetInterval "
        "<SECONDS>]",
        "Enables, disables, or sets options for the Consistency Scan role which repeatedly scans "
        "shard replicas for consistency.",
        "`on' enables the scan.\n\n"
        "`off' disables the scan but keeps the current cycle's progress so it will resume later if enabled again.\n\n"
        "`restart' will end the current scan cycle.  A new cycle will start if the scan is enabled, or later when "
        "it is enabled.\n\n"
        "`maxRate <BYTES_PER_SECOND>' sets the maximum scan read speed rate to BYTES_PER_SECOND, post-replication.\n\n"
        "`targetInterval <SECONDS>' sets the target interval for the scan to SECONDS.  The scan will adjust speed "
        "to attempt to complete in that amount of time but it will not exceed BYTES_PER_SECOND\n\n"
        "`stats` dumps the current round and lifetime stats of the consistency scan. It is a convenience method to "
        "expose the stats which are also in status json.\n\n"
        "`clearstats` will clear all of the stats for the consistency scan but otherwise leave the configuration as "
        "is. This can be used to clear errors or reset stat counts, for example.\n\n"
        "The consistency scan role publishes its configuration and metrics in Status JSON under the path "
        "`.cluster.consistency_scan'\n"
        // TODO:  Syntax hint generator
        ));

} // namespace fdb_cli
