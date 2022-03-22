/*
 * ThrottleCommand.actor.cpp
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

#include "fdbclient/IClientApi.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/CommitTransaction.h"

#include "flow/Arena.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/genericactors.actor.h"
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
			fmt::print("Lists tags that are currently throttled.\n");
			fmt::print("The default LIMIT is {} tags.\n", defaultThrottleListLimit);
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
				printf("ERROR: failed to parse `%s'.\n", printable(tokens[2]).c_str());
				return false;
			}
		}

		state int throttleListLimit = defaultThrottleListLimit;
		if (tokens.size() >= 4) {
			char* end;
			throttleListLimit = std::strtol((const char*)tokens[3].begin(), &end, 10);
			if ((tokens.size() > 4 && !std::isspace(*end)) || (tokens.size() == 4 && *end != '\0')) {
				fprintf(stderr, "ERROR: failed to parse limit `%s'.\n", printable(tokens[3]).c_str());
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
					printf("Throttled tags:\n\n");
					printf("  Rate (txn/s) | Expiration (s) | Priority  | Type   | Reason     |Tag\n");
					printf(" --------------+----------------+-----------+--------+------------+------\n");

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

				printf("  %12d | %13ds | %9s | %6s | %10s |%s\n",
				       (int)(itr->tpsRate),
				       std::min((int)(itr->expirationTime - now()), (int)(itr->initialDuration)),
				       transactionPriorityToString(itr->priority, false),
				       itr->throttleType == TagThrottleType::AUTO ? "auto" : "manual",
				       reasonStr.c_str(),
				       itr->tag.toString().c_str());
			}
		}

		if (tags.size() == throttleListLimit) {
			printf("\nThe tag limit `%d' was reached. Use the [LIMIT] argument to view additional tags.\n",
			       throttleListLimit);
			printf("Usage: throttle list [LIMIT]\n");
		}
		if (!anyLogged) {
			printf("There are no %s tags\n", reportThrottled ? "throttled" : "recommended");
		}
	} else if (tokencmp(tokens[1], "on")) {
		if (tokens.size() < 4 || !tokencmp(tokens[2], "tag") || tokens.size() > 7) {
			printf("Usage: throttle on tag <TAG> [RATE] [DURATION] [PRIORITY]\n");
			printf("\n");
			printf("Enables throttling for transactions with the specified tag.\n");
			printf("An optional transactions per second rate can be specified (default 0).\n");
			printf("An optional duration can be specified, which must include a time suffix (s, m, h, "
			       "d) (default 1h).\n");
			printf("An optional priority can be specified. Choices are `default', `immediate', and "
			       "`batch' (default `default').\n");
			return false;
		}

		double tpsRate = 0.0;
		uint64_t duration = 3600;
		TransactionPriority priority = TransactionPriority::DEFAULT;

		if (tokens.size() >= 5) {
			char* end;
			tpsRate = std::strtod((const char*)tokens[4].begin(), &end);
			if ((tokens.size() > 5 && !std::isspace(*end)) || (tokens.size() == 5 && *end != '\0')) {
				fprintf(stderr, "ERROR: failed to parse rate `%s'.\n", printable(tokens[4]).c_str());
				return false;
			}
			if (tpsRate < 0) {
				fprintf(stderr, "ERROR: rate cannot be negative `%f'\n", tpsRate);
				return false;
			}
		}
		if (tokens.size() == 6) {
			Optional<uint64_t> parsedDuration = parseDuration(tokens[5].toString());
			if (!parsedDuration.present()) {
				fprintf(stderr, "ERROR: failed to parse duration `%s'.\n", printable(tokens[5]).c_str());
				return false;
			}
			duration = parsedDuration.get();

			if (duration == 0) {
				fprintf(stderr, "ERROR: throttle duration cannot be 0\n");
				return false;
			}
		}
		if (tokens.size() == 7) {
			if (tokens[6] == LiteralStringRef("default")) {
				priority = TransactionPriority::DEFAULT;
			} else if (tokens[6] == LiteralStringRef("immediate")) {
				priority = TransactionPriority::IMMEDIATE;
			} else if (tokens[6] == LiteralStringRef("batch")) {
				priority = TransactionPriority::BATCH;
			} else {
				fprintf(stderr,
				        "ERROR: unrecognized priority `%s'. Must be one of `default',\n  `immediate', "
				        "or `batch'.\n",
				        tokens[6].toString().c_str());
				return false;
			}
		}

		TagSet tagSet;
		tagSet.addTag(tokens[3]);

		wait(ThrottleApi::throttleTags(db, tagSet, tpsRate, duration, TagThrottleType::MANUAL, priority));
		printf("Tag `%s' has been throttled\n", tokens[3].toString().c_str());
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
			    priority.present() ? format(" at %s priority", transactionPriorityToString(priority.get(), false)) : "";

			if (tagSet.size() > 0) {
				bool success = wait(ThrottleApi::unthrottleTags(db, tagSet, throttleType, priority));
				if (success) {
					fmt::print("Unthrottled {0}{1}\n", tagSet.toString(), priorityString);
				} else {
					fmt::print("{0} was not {1}throttled{2}\n",
					           tagSet.toString(Capitalize::True),
					           throttleTypeString,
					           priorityString);
				}
			} else {
				bool unthrottled = wait(ThrottleApi::unthrottleAll(db, throttleType, priority));
				if (unthrottled) {
					printf("Unthrottled all %sthrottled tags%s\n", throttleTypeString, priorityString.c_str());
				} else {
					printf("There were no tags being %sthrottled%s\n", throttleTypeString, priorityString.c_str());
				}
			}
		} else {
			printf("Usage: throttle off [all|auto|manual] [tag <TAG>] [PRIORITY]\n");
			printf("\n");
			printf("Disables throttling for throttles matching the specified filters. At least one "
			       "filter must be used.\n\n");
			printf("An optional qualifier `all', `auto', or `manual' can be used to specify the type "
			       "of throttle\n");
			printf("affected. `all' targets all throttles, `auto' targets those created by the "
			       "cluster, and\n");
			printf("`manual' targets those created manually (default `manual').\n\n");
			printf("The `tag' filter can be use to turn off only a specific tag.\n\n");
			printf("The priority filter can be used to turn off only throttles at specific priorities. "
			       "Choices are\n");
			printf("`default', `immediate', or `batch'. By default, all priorities are targeted.\n");
		}
	} else if (tokencmp(tokens[1], "enable") || tokencmp(tokens[1], "disable")) {
		if (tokens.size() != 3 || !tokencmp(tokens[2], "auto")) {
			printf("Usage: throttle <enable|disable> auto\n");
			printf("\n");
			printf("Enables or disable automatic tag throttling.\n");
			return false;
		}
		state bool autoTagThrottlingEnabled = tokencmp(tokens[1], "enable");
		wait(ThrottleApi::enableAuto(db, autoTagThrottlingEnabled));
		printf("Automatic tag throttling has been %s\n", autoTagThrottlingEnabled ? "enabled" : "disabled");
	} else {
		printUsage(tokens[0]);
		return false;
	}

	return true;
}

CommandFactory throttleFactory(
    "throttle",
    CommandHelp("throttle <on|off|enable auto|disable auto|list> [ARGS]",
                "view and control throttled tags",
                "Use `on' and `off' to manually throttle or unthrottle tags. Use `enable auto' or `disable auto' "
                "to enable or disable automatic tag throttling. Use `list' to print the list of throttled tags.\n"));
} // namespace fdb_cli
