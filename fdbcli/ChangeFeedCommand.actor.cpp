/*
 * ChangeFeedCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fmt/format.h"
#include "fdbcli/fdbcli.actor.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Void> changeFeedList(Database db) {
    state ReadYourWritesTransaction tr(db);
    loop {
        try {
            tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
            tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
            tr.setOption(FDBTransactionOptions::RAW_ACCESS);

            RangeResult result = wait(tr.getRange(changeFeedKeys, CLIENT_KNOBS->TOO_MANY));
            ASSERT(!result.more);
            printf("Found %d change feeds%s\n", result.size(), result.size() == 0 ? "." : ":");
            for (const auto& it : result) {
                auto range = std::get<0>(decodeChangeFeedValue(it.value));
                printf("  %s: `%s' - `%s'\n",
                       it.key.removePrefix(changeFeedPrefix).toString().c_str(),
                       printable(range.begin).c_str(),
                       printable(range.end).c_str());
            }
            return Void();
        } catch (const Error& e) {
            wait(tr.onError(e));
        }
    }
}

ACTOR Future<Void> requestVersionUpdate(Database localDb, Reference<ChangeFeedData> feedData) {
    loop {
        wait(delay(5.0));
        Transaction tr(localDb);
        tr.setOption(FDBTransactionOptions::RAW_ACCESS);
        Version ver = wait(tr.getReadVersion());
        fmt::print("Requesting version {}\n", ver);
        wait(feedData->whenAtLeast(ver));
        fmt::print("Feed at version {}\n", ver);
    }
}

ACTOR Future<bool> handleStreamCommand(Database localDb, std::vector<StringRef> tokens, Future<Void> warn) {
    if (tokens.size() < 3 || tokens.size() > 5) {
        printUsage(tokens[0]);
        return false;
    }
    Version begin = 0;
    Version end = std::numeric_limits<Version>::max();
    if (tokens.size() > 3) {
        int n = 0;
        if (sscanf(tokens[3].toString().c_str(), "%" PRId64 "%n", &begin, &n) != 1 || n != tokens[3].size()) {
            printUsage(tokens[0]);
            return false;
        }
    }
    if (tokens.size() > 4) {
        int n = 0;
        if (sscanf(tokens[4].toString().c_str(), "%" PRId64 "%n", &end, &n) != 1 || n != tokens[4].size()) {
            printUsage(tokens[0]);
            return false;
        }
    }
    if (warn.isValid()) {
        warn.cancel();
    }
    state Reference<ChangeFeedData> feedData = makeReference<ChangeFeedData>();
    state Future<Void> feed = localDb->getChangeFeedStream(feedData, tokens[2], begin, end);
    state Future<Void> versionUpdates = requestVersionUpdate(localDb, feedData);
    printf("\n");
    try {
        state Future<Void> feedInterrupt = LineNoise::onKeyboardInterrupt();
        loop {
            choose {
                when(Standalone<VectorRef<MutationsAndVersionRef>> res = waitNext(feedData->mutations.getFuture())) {
                    for (const auto& it : res) {
                        for (const auto& it2 : it.mutations) {
                            fmt::print("{0} {1}\n", it.version, it2.toString());
                        }
                    }
                }
                when(wait(feedInterrupt)) {
                    feedInterrupt = Future<Void>();
                    feed.cancel();
                    feedData = makeReference<ChangeFeedData>();
                    break;
                }
            }
        }
        return true;
    } catch (const Error& e) {
        if (e.code() == error_code_end_of_stream) {
            return true;
        }
        throw;
    }
}

ACTOR Future<bool> handlePopCommand(Database localDb, std::vector<StringRef> tokens) {
    if (tokens.size() != 4) {
        printUsage(tokens[0]);
        return false;
    }
    Version v;
    int n = 0;
    if (sscanf(tokens[3].toString().c_str(), "%" PRId64 "%n", &v, &n) != 1 || n != tokens[3].size()) {
        printUsage(tokens[0]);
        return false;
    } else {
        wait(localDb->popChangeFeedMutations(tokens[2], v));
    }
    return true;
}

} // namespace

namespace fdb_cli {

using CommandHandler = Future<bool> (*)(Database localDb, std::vector<StringRef> tokens, Future<Void> warn);

const std::unordered_map<std::string_view, CommandHandler> commandHandlers = {
    {"list", [](Database db, std::vector<StringRef> tokens, Future<Void> warn) {
        if (tokens.size() != 2) {
            printUsage(tokens[0]);
            return false;
        }
        return changeFeedList(db);
    }},
    {"register", [](Database db, std::vector<StringRef> tokens, Future<Void> warn) {
        if (tokens.size() != 5) {
            printUsage(tokens[0]);
            return false;
        }
        KeyRange range = KeyRangeRef(tokens[3], tokens[4]);
        return updateChangeFeed(db, tokens[2], ChangeFeedStatus::CHANGE_FEED_CREATE, range);
    }},
    {"stop", [](Database db, std::vector<StringRef> tokens, Future<Void> warn) {
        if (tokens.size() != 3) {
            printUsage(tokens[0]);
            return false;
        }
        return updateChangeFeed(db, tokens[2], ChangeFeedStatus::CHANGE_FEED_STOP);
    }},
    {"destroy", [](Database db, std::vector<StringRef> tokens, Future<Void> warn) {
        if (tokens.size() != 3) {
            printUsage(tokens[0]);
            return false;
        }
        return updateChangeFeed(db, tokens[2], ChangeFeedStatus::CHANGE_FEED_DESTROY);
    }},
    {"stream", handleStreamCommand},
    {"pop", handlePopCommand}
};

ACTOR Future<bool> changeFeedCommandActor(Database localDb,
                                          Optional<TenantMapEntry> tenantEntry,
                                          std::vector<StringRef> tokens,
                                          Future<Void> warn) {
    if (tokens.size() == 1) {
        printUsage(tokens[0]);
        return false;
    }

    auto commandHandlerIt = commandHandlers.find(tokens[1].toString());
    if (commandHandlerIt != commandHandlers.end()) {
        return commandHandlerIt->second(localDb, tokens, warn);
    } else {
        printUsage(tokens[0]);
        return false;
    }
}

CommandFactory changeFeedFactory(
    "changefeed",
    CommandHelp("changefeed <register|destroy|stop|stream|pop|list> <RANGEID> <BEGIN> <END>", "", "")
);

} // namespace fdb_cli
