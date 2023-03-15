/*
 * HotRangeCommand.actor.cpp
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

#include <boost/lexical_cast.hpp>

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/StorageServerInterface.h"

#include "fdbclient/json_spirit/json_spirit_value.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/NetworkAddress.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ReadHotSubRangeRequest::SplitType parseSplitType(const std::string& typeStr) {
	auto type = ReadHotSubRangeRequest::SplitType::BYTES;
	if (typeStr == "bytes") {
		type = ReadHotSubRangeRequest::BYTES;
	} else if (typeStr == "readBytes") {
		type = ReadHotSubRangeRequest::READ_BYTES;
	} else if (typeStr == "readOps") {
		type = ReadHotSubRangeRequest::READ_OPS;
	} else {
		fmt::print("Error: {} is not a valid split type. Will use bytes as the default split type\n", typeStr);
	}
	return type;
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> hotRangeCommandActor(Database localdb,
                                        Reference<IDatabase> db,
                                        std::vector<StringRef> tokens,
                                        std::map<std::string, StorageServerInterface>* storage_interface) {

	if (tokens.size() == 1) {
		// initialize storage interfaces
		storage_interface->clear();
		wait(getStorageServerInterfaces(db, storage_interface));
		fmt::print("\nThe following {} storage servers can be queried:\n", storage_interface->size());
		for (const auto& [addr, _] : *storage_interface) {
			fmt::print("{}\n", addr);
		}
		fmt::print("\n");
	} else if (tokens.size() == 6) {
		if (storage_interface->size() == 0) {
			fprintf(stderr, "ERROR: no storage processes to query. You must run the `hotrange’ command first.\n");
			return false;
		}
		state Key address = tokens[1];
		// At present we only support one process(IP:Port) at a time
		if (!storage_interface->count(address.toString())) {
			fprintf(stderr, "ERROR: storage process `%s' not recognized.\n", printable(address).c_str());
			return false;
		}
		state int splitCount;
		try {
			splitCount = boost::lexical_cast<int>(tokens[5].toString());
		} catch (...) {
			fmt::print("Error: splitCount value: '{}', cannot be parsed to an Integer\n", tokens[5].toString());
			return false;
		}
		ReadHotSubRangeRequest::SplitType splitType = parseSplitType(tokens[2].toString());
		KeyRangeRef range(tokens[3], tokens[4]);
		// TODO: refactor this to support multiversion fdbcli in the future
		Standalone<VectorRef<ReadHotRangeWithMetrics>> metrics =
		    wait(localdb->getHotRangeMetrics((*storage_interface)[address.toString()], range, splitType, splitCount));
		// next parse the result and form a json array for each object
		json_spirit::mArray resultArray;
		for (const auto& metric : metrics) {
			json_spirit::mObject metricObj;
			metricObj["begin"] = metric.keys.begin.toString();
			metricObj["end"] = metric.keys.end.toString();
			metricObj["readBytesPerSec"] = metric.readBandwidthSec;
			metricObj["readOpsPerSec"] = metric.readOpsSec;
			metricObj["bytes"] = metric.bytes;
			resultArray.push_back(metricObj);
		}
		// print out the json array
		const std::string result =
		    json_spirit::write_string(json_spirit::mValue(resultArray), json_spirit::pretty_print);
		fmt::print("\n{}\n", result);
	} else {
		printUsage(tokens[0]);
		return false;
	}

	return true;
}

CommandFactory hotRangeFactory(
    "hotrange",
    CommandHelp(
        "hotrange <IP:PORT> <bytes|readBytes|readOps> <begin> <end> <splitCount>",
        "Fetch read metrics from a given storage server to detect hot range",
        "If no arguments are specified, populates the list of storage processes that can be queried. "
        "<begin> <end> specify the range you are interested in, "
        "<bytes|readBytes|readOps> is the metric used to divide ranges, "
        "splitCount is the number of returned ranges divided by the given metric. "
        "The command will return an array of json object for each range with their metrics."
        "Notice: the three metrics are sampled by a different way, so their values are not perfectly matched.\n"));

} // namespace fdb_cli
