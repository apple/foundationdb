/*
 * RangeConfigCommand.actor.cpp
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

#include <boost/lexical_cast.hpp>
#include <algorithm>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <list>

#include "fdbcli/fdbcli.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/DataDistributionConfig.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> rangeConfigCommandActor(Database cx, std::vector<StringRef> tokens) {
	state std::function<bool(std::string)> fail = [&](std::string msg) {
		if (!msg.empty()) {
			fmt::print(stderr, "ERROR: {}\n", msg);
		}
		printUsage(tokens[0]);
		return false;
	};

	state std::list<StringRef> args(tokens.begin() + 1, tokens.end());
	state std::function<StringRef()> nextArg = [&]() {
		ASSERT(!args.empty());
		auto s = args.front();
		args.pop_front();
		return s;
	};
	state std::function<int()> nextArgInt = [&]() {
		if (args.empty()) {
			throw boost::bad_lexical_cast();
		}
		return boost::lexical_cast<int>(nextArg().toString());
	};

	if (args.size() < 1) {
		return fail("No subcommand given.");
	}

	state StringRef cmd = nextArg();

	if (cmd == "show"_sr) {
		state bool includeDefault = false;

		while (!args.empty()) {
			auto arg = nextArg();
			if (arg == "includeDefault"_sr) {
				includeDefault = true;
			} else {
				return fail(fmt::format("Unknown argument: '{}'", arg.printable()));
			}
		}

		Reference<DDConfiguration::RangeConfigMapSnapshot> config =
		    wait(DDConfiguration().userRangeConfig().getSnapshot(
		        SystemDBWriteLockedNow(cx.getReference()), allKeys.begin, allKeys.end));
		fmt::print(
		    "{}\n",
		    json_spirit::write_string(DDConfiguration::toJSON(*config, includeDefault), json_spirit::pretty_print));

	} else if (cmd == "update"_sr || cmd == "set"_sr) {
		if (args.size() < 3) {
			return fail("Begin, end, and at least one configuration option are required.");
		}

		state KeyRef begin = nextArg();
		state KeyRef end = nextArg();
		if (end <= begin) {
			return fail("Range end must be > range begin.");
		}

		state DDRangeConfig rangeConfig;

		while (!args.empty()) {
			state StringRef option = nextArg();

			try {
				if (option == "replication"_sr) {
					rangeConfig.replicationFactor = nextArgInt();
				} else if (option == "teamID"_sr) {
					rangeConfig.teamID = nextArgInt();
				} else if (option == "default"_sr) {
					rangeConfig = DDRangeConfig();
				} else {
					return fail(fmt::format("Unknown range option: '{}'", option.printable()));
				}
			} catch (...) {
				return fail(
				    fmt::format("Required argument for range option '{}' missing or invalid.", option.toString()));
			}

			wait(DDConfiguration().userRangeConfig().updateRange(
			    SystemDBWriteLockedNow(cx.getReference()), begin, end, rangeConfig, cmd == "set"_sr));
		}
	} else {
		return fail(fmt::format("Unknown command: '{}'", cmd.printable()));
	}

	return true;
}

std::vector<const char*> rangeConfigGenerator(std::vector<StringRef> const& tokens, bool inArgument) {
	if (tokens.size() == 1) {
		return { "<show|set|update>", "[ARGS]" };
	}
	auto cmd = tokens[1];
	if (cmd == "show"_sr && tokens.size() == 2) {
		return { "[includeDefault]" };
	} else if (cmd == "set"_sr || cmd == "update"_sr) {
		static std::vector<const char*> opts = {
			"<BEGINKEY>", "<ENDKEY>", "[default]", "[replication <N>]", "[teamID <N>]"
		};
		// Subtract the two known tokens, command and subcommand, and then possibly two more tokens for begin and end if
		// present
		return std::vector<const char*>(opts.begin() + std::min<size_t>(tokens.size(), 4) - 2, opts.end());
	} else {
		return {};
	}
}

CommandFactory rangeConfigFactory(
    "rangeconfig",
    CommandHelp(
        "rangeconfig <show> [includeDefault] | <update|set> <BEGINKEY> <ENDKEY> [default] [replication <N>] [teamID "
        "<N>]",
        "Show or change the per-keyrange configuration options.",
        "The `show' command will print the range configuration in JSON.  By default, ranges with no configured "
        "options are not shown, these are called 'default ranges' and can be shown with the `includeDefault' flag.\n\n"
        "The `update' command will apply the given options to the given key range.  Any option not explicitly given "
        "will remain at its present setting for the range in the configuration.\n\n"
        "The `set' command will change the given key range to be exactly given configuration, meaning that any option "
        "not explicitly given will be changed to unset for the range.\n\n"
        "Range Options:\n"
        "  default - Resets the configuration to apply to have no options set.  This can be used with `set' to "
        "explicitly clear all configured options for a range.\n\n"
        "  replication <N> - Set replication factor for the range.  Ranges set to a replication factor lower than the "
        "cluster's configured replication level will be treated as the same as the cluster's replication level.\n\n"
        "  teamID <N> - This provides a way to indicate that shards should be on different teams.  Ranges with "
        "different teamID settings should be assigned to different storage teams.  Shards with the same team ID can be "
        "assigned to the same storage team, but nothing enforces this.\n\n"
        "Note that key range configuration options do not alter the shard map directly, rather they are hints which "
        "DataDistribution should honor.\n\n"
        "For help with escaping BEGINKEY or ENDKEY, see `help escaping'.\n"),
    nullptr,
    &rangeConfigGenerator);
} // namespace fdb_cli
