/*
 * RedistributeCommand.actor.cpp
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

#include "fdbcli/fdbcli.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> redistributeCommandActor(Reference<IClusterConnectionRecord> clusterFile,
                                            std::vector<StringRef> tokens) {
	state bool result = true;
	if (tokens.size() == 3) {
		Key begin = tokens[1];
		Key end = tokens[2];
		if (begin == end) {
			printUsage(tokens[0]);
			result = false;
		}
		wait(redistribute(clusterFile, KeyRangeRef(begin, end), /*timeoutSeconds=*/30));
	} else {
		printUsage(tokens[0]);
		result = false;
	}
	return result;
}

CommandFactory RedistributeFactory(
    "redistribute",
    CommandHelp("redistribute [Begin] [End]",
                "Redistribute data of the range<Begin, End> among the cluster, where [Begin]!=[End]\n",
                "Given an input range, say <b, d>, where b!=d and we suppose the current FDB"
                "internal shard boundary is [a, c), [c, d), [d, e),"
                "our splitting algorithm splits the input range into [b, c) and [c, d)."
                "Then the two ranges will be redistributed among the cluster.\n"));
} // namespace fdb_cli
