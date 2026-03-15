/*
 * ThrottleCommand.actor.cpp
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

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/IClientApi.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/CommitTransaction.h"

#include "flow/Arena.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/genericactors.actor.h"
#include "fmt/format.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

static constexpr int defaultThrottleListLimit = 100;

ACTOR Future<bool> throttleCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {

	if (tokens.size() == 1) {
		printUsage(tokens[0]);
		return false;
	} else if (tokencmp(tokens[1], "list")) {
		if (tokens.size() > 4) {
			fmt::print("Usage: throttle list [throttled|recommended|all] [LIMIT]\n\n");
			fmt::println("Lists tags that are currently throttled.");
			fmt::println("The default LIMIT is {} tags.", defaultThrottleListLimit);
			return false;
		}

		state bool reportThrottled = true;
		state bool reportRecommended = false;
		if (tokens.size() >= 3) {
			if (tokencmp(tokens[2], "recommended")) {
				reportThrottled = false;
				reportRecommended = true;
			} else if (tokencmp(tokens[2], "all")) {
				reportThrottled = true;
				reportRecommended = true;
			} else if (!tokencmp(tokens[2], "throttled")) {
				fmt::println("ERROR: failed to parse `{}'.", printable(tokens[2]));
				return false;
			}
		}

		state int throttleListLimit = defaultThrottleListLimit;
		if (tokens.size() >= 4) {
			char* end;
			throttleListLimit = std::strtol((const char*)tokens[3].begin(), &end, 10);
			if ((tokens.size() > 4 && !std::isspace(*end)) || (tokens.size() == 4 && *end != '\0')) {
				fmt::println(stderr, "ERROR: failed to parse limit `{}'.", printable(tokens[3]));
				return false;
			}
		}

		state std::vector<TagThrottleInfo> tags;
		if (reportThrottled && reportRecommended) {
			wait(store(tags, ThrottleApi::getThrottledTags(db, throttleListLimit, ContainsRecommended::True)));
		} else if (reportThrottled) {
			wait(store(tags, ThrottleApi::getThrottledTags(db, throttleListLimit)));
		} else if (reportRecommended) {
			wait(store(tags, ThrottleApi::getRecommendedTags(db, throttleListLimit)));
		}

		bool anyLogged = false;
		for (auto itr = tags.begin(); itr != tags.end(); ++itr) {
			if (itr->expirationTime > now()) {
				if (!anyLogged) {
					fmt::print("Throttled tags:\n\n");
					fmt::println("  Rate (txn/s) | Expiration (s) | Priority  | Type   | Reason     |Tag");
					fmt::println(" --------------+----------------+-----------+--------+------------+------");

					anyLogged = true;
				}

				std::string reasonStr = "unset";
				if (itr->reason == TagThrottledReason::MANUAL) {
					reasonStr = "manual";
				} else if (itr->reason == TagThrottledReason::BUSY_WRITE) {
					reasonStr = "busy write";
				} else if (itr->reason == TagThrottledReason::BUSY_READ) {
					reasonStr = "busy read";
				}

				fmt::println("  {:12} | {:13}s | {:>9} | {:>6} | {:>10} |{}",
				             (int)(itr->tpsRate),
				             std::min((int)(itr->expirationTime - now()), (int)(itr->initialDuration)),
				             transactionPriorityToString(itr->priority, false),
				             itr->throttleType == TagThrottleType::AUTO ? "auto" : "manual",
				             reasonStr,
				             itr->tag.toString());
			}
		}

		if (tags.size() == throttleListLimit) {
			fmt::print("\nThe tag limit `{}' was reached. Use the [LIMIT] argument to view additional tags.\n",
			           throttleListLimit);
			fmt::println("Usage: throttle list [LIMIT]");
		}
		if (!anyLogged) {
			fmt::println("There are no {} tags", reportThrottled ? "throttled" : "recommended");
		}
	} else if (tokencmp(tokens[1], "on")) {
		if (tokens.size() < 4 || !tokencmp(tokens[2], "tag") || tokens.size() > 7) {
			fmt::println("Usage: throttle on tag <TAG> [RATE] [DURATION] [PRIORITY]");
			fmt::println("");
			fmt::println("Enables throttling for transactions with the specified tag.");
			fmt::println("An optional transactions per second rate can be specified (default 0).");
			fmt::println(
			    "An optional duration can be specified, which must include a time suffix (s, m, h, d) (default 1h).");
			fmt::println("An optional priority can be specified. Choices are `default', `immediate', and `batch' "
			             "(default `default').");
			return false;
		}

		double tpsRate = 0.0;
		uint64_t duration = 3600;
		TransactionPriority priority = TransactionPriority::DEFAULT;

		if (tokens.size() >= 5) {
			char* end;
			tpsRate = std::strtod((const char*)tokens[4].begin(), &end);
			if ((tokens.size() > 5 && !std::isspace(*end)) || (tokens.size() == 5 && *end != '\0')) {
				fmt::println(stderr, "ERROR: failed to parse rate `{}'.", printable(tokens[4]));
				return false;
			}
			if (tpsRate < 0) {
				fmt::println(stderr, "ERROR: rate cannot be negative `{:f}'", tpsRate);
				return false;
			}
		}
		if (tokens.size() == 6) {
			Optional<uint64_t> parsedDuration = parseDuration(tokens[5].toString());
			if (!parsedDuration.present()) {
				fmt::println(stderr, "ERROR: failed to parse duration `{}'.", printable(tokens[5]));
				return false;
			}
			duration = parsedDuration.get();

			if (duration == 0) {
				fmt::println(stderr, "ERROR: throttle duration cannot be 0");
				return false;
			}
		}
		if (tokens.size() == 7) {
			if (tokens[6] == "default"_sr) {
				priority = TransactionPriority::DEFAULT;
			} else if (tokens[6] == "immediate"_sr) {
				priority = TransactionPriority::IMMEDIATE;
			} else if (tokens[6] == "batch"_sr) {
				priority = TransactionPriority::BATCH;
			} else {
				fmt::print(stderr,
				           "ERROR: unrecognized priority `{}'. Must be one of `default',\n  `immediate', or `batch'.\n",
				           tokens[6].toString());
				return false;
			}
		}

		TagSet tagSet;
		tagSet.addTag(tokens[3]);

		wait(ThrottleApi::throttleTags(db, tagSet, tpsRate, duration, TagThrottleType::MANUAL, priority));
		fmt::println("Tag `{}' has been throttled", tokens[3].toString());
	} else if (tokencmp(tokens[1], "off")) {
		int nextIndex = 2;
		state TagSet tagSet;
		bool throttleTypeSpecified = false;
		bool is_error = false;
		Optional<TagThrottleType> throttleType = TagThrottleType::MANUAL;
		Optional<TransactionPriority> priority;

		if (tokens.size() == 2) {
			is_error = true;
		}

		while (nextIndex < tokens.size() && !is_error) {
			if (tokencmp(tokens[nextIndex], "all")) {
				if (throttleTypeSpecified) {
					is_error = true;
					continue;
				}
				throttleTypeSpecified = true;
				throttleType = Optional<TagThrottleType>();
				++nextIndex;
			} else if (tokencmp(tokens[nextIndex], "auto")) {
				if (throttleTypeSpecified) {
					is_error = true;
					continue;
				}
				throttleTypeSpecified = true;
				throttleType = TagThrottleType::AUTO;
				++nextIndex;
			} else if (tokencmp(tokens[nextIndex], "manual")) {
				if (throttleTypeSpecified) {
					is_error = true;
					continue;
				}
				throttleTypeSpecified = true;
				throttleType = TagThrottleType::MANUAL;
				++nextIndex;
			} else if (tokencmp(tokens[nextIndex], "default")) {
				if (priority.present()) {
					is_error = true;
					continue;
				}
				priority = TransactionPriority::DEFAULT;
				++nextIndex;
			} else if (tokencmp(tokens[nextIndex], "immediate")) {
				if (priority.present()) {
					is_error = true;
					continue;
				}
				priority = TransactionPriority::IMMEDIATE;
				++nextIndex;
			} else if (tokencmp(tokens[nextIndex], "batch")) {
				if (priority.present()) {
					is_error = true;
					continue;
				}
				priority = TransactionPriority::BATCH;
				++nextIndex;
			} else if (tokencmp(tokens[nextIndex], "tag")) {
				if (tagSet.size() > 0 || nextIndex == tokens.size() - 1) {
					is_error = true;
					continue;
				}
				tagSet.addTag(tokens[nextIndex + 1]);
				nextIndex += 2;
			} else {
				is_error = true;
			}
		}

		if (!is_error) {
			state const char* throttleTypeString =
			    !throttleType.present() ? "" : (throttleType.get() == TagThrottleType::AUTO ? "auto-" : "manually ");
			state std::string priorityString =
			    priority.present() ? fmt::format(" at {} priority", transactionPriorityToString(priority.get(), false))
			                       : "";

			if (tagSet.size() > 0) {
				bool success = wait(ThrottleApi::unthrottleTags(db, tagSet, throttleType, priority));
				if (success) {
					fmt::println("Unthrottled {0}{1}", tagSet.toString(), priorityString);
				} else {
					fmt::println("{0} was not {1}throttled{2}",
					             tagSet.toString(Capitalize::True),
					             throttleTypeString,
					             priorityString);
				}
			} else {
				bool unthrottled = wait(ThrottleApi::unthrottleAll(db, throttleType, priority));
				if (unthrottled) {
					fmt::println("Unthrottled all {}throttled tags{}", throttleTypeString, priorityString);
				} else {
					fmt::println("There were no tags being {}throttled{}", throttleTypeString, priorityString);
				}
			}
		} else {
			fmt::println("Usage: throttle off [all|auto|manual] [tag <TAG>] [PRIORITY]");
			fmt::println("");
			fmt::print("Disables throttling for throttles matching the specified filters. At least one filter must be "
			           "used.\n\n");
			fmt::println(
			    "An optional qualifier `all', `auto', or `manual' can be used to specify the type of throttle");
			fmt::println("affected. `all' targets all throttles, `auto' targets those created by the cluster, and");
			fmt::print("`manual' targets those created manually (default `manual').\n\n");
			fmt::print("The `tag' filter can be use to turn off only a specific tag.\n\n");
			fmt::println(
			    "The priority filter can be used to turn off only throttles at specific priorities. Choices are");
			fmt::println("`default', `immediate', or `batch'. By default, all priorities are targeted.");
		}
	} else if (tokencmp(tokens[1], "enable") || tokencmp(tokens[1], "disable")) {
		if (tokens.size() != 3 || !tokencmp(tokens[2], "auto")) {
			fmt::println("Usage: throttle <enable|disable> auto");
			fmt::println("");
			fmt::println("Enables or disable automatic tag throttling.");
			return false;
		}
		state bool autoTagThrottlingEnabled = tokencmp(tokens[1], "enable");
		wait(ThrottleApi::enableAuto(db, autoTagThrottlingEnabled));
		fmt::println("Automatic tag throttling has been {}", autoTagThrottlingEnabled ? "enabled" : "disabled");
	} else {
		printUsage(tokens[0]);
		return false;
	}

	return true;
}

void throttleGenerator(const char* text,
                       const char* line,
                       std::vector<std::string>& lc,
                       std::vector<StringRef> const& tokens) {
	if (tokens.size() == 1) {
		const char* opts[] = { "on tag", "off", "enable auto", "disable auto", "list", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokens.size() >= 2 && tokencmp(tokens[1], "on")) {
		if (tokens.size() == 2) {
			const char* opts[] = { "tag", nullptr };
			arrayGenerator(text, line, opts, lc);
		} else if (tokens.size() == 6) {
			const char* opts[] = { "default", "immediate", "batch", nullptr };
			arrayGenerator(text, line, opts, lc);
		}
	} else if (tokens.size() >= 2 && tokencmp(tokens[1], "off") && !tokencmp(tokens[tokens.size() - 1], "tag")) {
		const char* opts[] = { "all", "auto", "manual", "tag", "default", "immediate", "batch", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokens.size() == 2 && (tokencmp(tokens[1], "enable") || tokencmp(tokens[1], "disable"))) {
		const char* opts[] = { "auto", nullptr };
		arrayGenerator(text, line, opts, lc);
	} else if (tokens.size() >= 2 && tokencmp(tokens[1], "list")) {
		if (tokens.size() == 2) {
			const char* opts[] = { "throttled", "recommended", "all", nullptr };
			arrayGenerator(text, line, opts, lc);
		} else if (tokens.size() == 3) {
			const char* opts[] = { "LIMITS", nullptr };
			arrayGenerator(text, line, opts, lc);
		}
	}
}

std::vector<const char*> throttleHintGenerator(std::vector<StringRef> const& tokens, bool inArgument) {
	if (tokens.size() == 1) {
		return { "<on|off|enable auto|disable auto|list>", "[ARGS]" };
	} else if (tokencmp(tokens[1], "on")) {
		std::vector<const char*> opts = { "tag", "<TAG>", "[RATE]", "[DURATION]", "[default|immediate|batch]" };
		if (tokens.size() == 2) {
			return opts;
		} else if (((tokens.size() == 3 && inArgument) || tokencmp(tokens[2], "tag")) && tokens.size() < 7) {
			return std::vector<const char*>(opts.begin() + tokens.size() - 2, opts.end());
		}
	} else if (tokencmp(tokens[1], "off")) {
		if (tokencmp(tokens[tokens.size() - 1], "tag")) {
			return { "<TAG>" };
		} else {
			bool hasType = false;
			bool hasTag = false;
			bool hasPriority = false;
			for (int i = 2; i < tokens.size(); ++i) {
				if (tokencmp(tokens[i], "all") || tokencmp(tokens[i], "auto") || tokencmp(tokens[i], "manual")) {
					hasType = true;
				} else if (tokencmp(tokens[i], "default") || tokencmp(tokens[i], "immediate") ||
				           tokencmp(tokens[i], "batch")) {
					hasPriority = true;
				} else if (tokencmp(tokens[i], "tag")) {
					hasTag = true;
					++i;
				} else {
					return {};
				}
			}

			std::vector<const char*> options;
			if (!hasType) {
				options.push_back("[all|auto|manual]");
			}
			if (!hasTag) {
				options.push_back("[tag <TAG>]");
			}
			if (!hasPriority) {
				options.push_back("[default|immediate|batch]");
			}

			return options;
		}
	} else if ((tokencmp(tokens[1], "enable") || tokencmp(tokens[1], "disable")) && tokens.size() == 2) {
		return { "auto" };
	} else if (tokens.size() >= 2 && tokencmp(tokens[1], "list")) {
		if (tokens.size() == 2) {
			return { "[throttled|recommended|all]", "[LIMITS]" };
		} else if (tokens.size() == 3 && (tokencmp(tokens[2], "throttled") || tokencmp(tokens[2], "recommended") ||
		                                  tokencmp(tokens[2], "all"))) {
			return { "[LIMITS]" };
		}
	} else if (tokens.size() == 2 && inArgument) {
		return { "[ARGS]" };
	}

	return std::vector<const char*>();
}

CommandFactory throttleFactory(
    "throttle",
    CommandHelp("throttle <on|off|enable auto|disable auto|list> [ARGS]",
                "view and control throttled tags",
                "Use `on' and `off' to manually throttle or unthrottle tags. Use `enable auto' or `disable auto' "
                "to enable or disable automatic tag throttling. Use `list' to print the list of throttled tags.\n"),
    &throttleGenerator,
    &throttleHintGenerator);
} // namespace fdb_cli
