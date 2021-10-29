/*
 * ThrottleCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/TagThrottle.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/CommitTransaction.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

// Helper functions copied from TagThrottle.actor.cpp
// The only difference is transactions are changed to go through MultiversionTransaction,
// instead of the native Transaction(i.e., RYWTransaction)

ACTOR Future<bool> getValidAutoEnabled(Reference<ITransaction> tr) {
	state bool result;
	loop {
		Optional<Value> value = wait(safeThreadFutureToFuture(tr->get(tagThrottleAutoEnabledKey)));
		if (!value.present()) {
			tr->reset();
			wait(delay(CLIENT_KNOBS->DEFAULT_BACKOFF));
			continue;
		} else if (value.get() == LiteralStringRef("1")) {
			result = true;
		} else if (value.get() == LiteralStringRef("0")) {
			result = false;
		} else {
			TraceEvent(SevWarnAlways, "InvalidAutoTagThrottlingValue").detail("Value", value.get());
			tr->reset();
			wait(delay(CLIENT_KNOBS->DEFAULT_BACKOFF));
			continue;
		}
		return result;
	};
}

ACTOR Future<std::vector<TagThrottleInfo>> getThrottledTags(Reference<IDatabase> db,
                                                            int limit,
                                                            bool containsRecommend = false) {
	state Reference<ITransaction> tr = db->createTransaction();
	state bool reportAuto = containsRecommend;
	loop {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		try {
			if (!containsRecommend) {
				wait(store(reportAuto, getValidAutoEnabled(tr)));
			}
			state ThreadFuture<RangeResult> f = tr->getRange(
			    reportAuto ? tagThrottleKeys : KeyRangeRef(tagThrottleKeysPrefix, tagThrottleAutoKeysPrefix), limit);
			RangeResult throttles = wait(safeThreadFutureToFuture(f));
			std::vector<TagThrottleInfo> results;
			for (auto throttle : throttles) {
				results.push_back(TagThrottleInfo(TagThrottleKey::fromKey(throttle.key),
				                                  TagThrottleValue::fromValue(throttle.value)));
			}
			return results;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<std::vector<TagThrottleInfo>> getRecommendedTags(Reference<IDatabase> db, int limit) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		try {
			bool enableAuto = wait(getValidAutoEnabled(tr));
			if (enableAuto) {
				return std::vector<TagThrottleInfo>();
			}
			state ThreadFuture<RangeResult> f =
			    tr->getRange(KeyRangeRef(tagThrottleAutoKeysPrefix, tagThrottleKeys.end), limit);
			RangeResult throttles = wait(safeThreadFutureToFuture(f));
			std::vector<TagThrottleInfo> results;
			for (auto throttle : throttles) {
				results.push_back(TagThrottleInfo(TagThrottleKey::fromKey(throttle.key),
				                                  TagThrottleValue::fromValue(throttle.value)));
			}
			return results;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<Void> updateThrottleCount(Reference<ITransaction> tr, int64_t delta) {
	state ThreadFuture<Optional<Value>> countVal = tr->get(tagThrottleCountKey);
	state ThreadFuture<Optional<Value>> limitVal = tr->get(tagThrottleLimitKey);

	wait(success(safeThreadFutureToFuture(countVal)) && success(safeThreadFutureToFuture(limitVal)));

	int64_t count = 0;
	int64_t limit = 0;

	if (countVal.get().present()) {
		BinaryReader reader(countVal.get().get(), Unversioned());
		reader >> count;
	}

	if (limitVal.get().present()) {
		BinaryReader reader(limitVal.get().get(), Unversioned());
		reader >> limit;
	}

	count += delta;

	if (count > limit) {
		throw too_many_tag_throttles();
	}

	BinaryWriter writer(Unversioned());
	writer << count;

	tr->set(tagThrottleCountKey, writer.toValue());
	return Void();
}

void signalThrottleChange(Reference<ITransaction> tr) {
	tr->atomicOp(
	    tagThrottleSignalKey, LiteralStringRef("XXXXXXXXXX\x00\x00\x00\x00"), MutationRef::SetVersionstampedValue);
}

ACTOR Future<Void> throttleTags(Reference<IDatabase> db,
                                TagSet tags,
                                double tpsRate,
                                double initialDuration,
                                TagThrottleType throttleType,
                                TransactionPriority priority,
                                Optional<double> expirationTime = Optional<double>(),
                                Optional<TagThrottledReason> reason = Optional<TagThrottledReason>()) {
	state Reference<ITransaction> tr = db->createTransaction();
	state Key key = TagThrottleKey(tags, throttleType, priority).toKey();

	ASSERT(initialDuration > 0);

	if (throttleType == TagThrottleType::MANUAL) {
		reason = TagThrottledReason::MANUAL;
	}
	TagThrottleValue throttle(tpsRate,
	                          expirationTime.present() ? expirationTime.get() : 0,
	                          initialDuration,
	                          reason.present() ? reason.get() : TagThrottledReason::UNSET);
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withTagThrottleValueReason()));
	wr << throttle;
	state Value value = wr.toValue();

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			if (throttleType == TagThrottleType::MANUAL) {
				Optional<Value> oldThrottle = wait(safeThreadFutureToFuture(tr->get(key)));
				if (!oldThrottle.present()) {
					wait(updateThrottleCount(tr, 1));
				}
			}

			tr->set(key, value);

			if (throttleType == TagThrottleType::MANUAL) {
				signalThrottleChange(tr);
			}

			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<bool> unthrottleTags(Reference<IDatabase> db,
                                  TagSet tags,
                                  Optional<TagThrottleType> throttleType,
                                  Optional<TransactionPriority> priority) {
	state Reference<ITransaction> tr = db->createTransaction();

	state std::vector<Key> keys;
	for (auto p : allTransactionPriorities) {
		if (!priority.present() || priority.get() == p) {
			if (!throttleType.present() || throttleType.get() == TagThrottleType::AUTO) {
				keys.push_back(TagThrottleKey(tags, TagThrottleType::AUTO, p).toKey());
			}
			if (!throttleType.present() || throttleType.get() == TagThrottleType::MANUAL) {
				keys.push_back(TagThrottleKey(tags, TagThrottleType::MANUAL, p).toKey());
			}
		}
	}

	state bool removed = false;

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			state std::vector<Future<Optional<Value>>> values;
			values.reserve(keys.size());
			for (auto key : keys) {
				values.push_back(safeThreadFutureToFuture(tr->get(key)));
			}

			wait(waitForAll(values));

			int delta = 0;
			for (int i = 0; i < values.size(); ++i) {
				if (values[i].get().present()) {
					if (TagThrottleKey::fromKey(keys[i]).throttleType == TagThrottleType::MANUAL) {
						delta -= 1;
					}

					tr->clear(keys[i]);

					// Report that we are removing this tag if we ever see it present.
					// This protects us from getting confused if the transaction is maybe committed.
					// It's ok if someone else actually ends up removing this tag at the same time
					// and we aren't the ones to actually do it.
					removed = true;
				}
			}

			if (delta != 0) {
				wait(updateThrottleCount(tr, delta));
			}
			if (removed) {
				signalThrottleChange(tr);
				wait(safeThreadFutureToFuture(tr->commit()));
			}

			return removed;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<Void> enableAuto(Reference<IDatabase> db, bool enabled) {
	state Reference<ITransaction> tr = db->createTransaction();

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			Optional<Value> value = wait(safeThreadFutureToFuture(tr->get(tagThrottleAutoEnabledKey)));
			if (!value.present() || (enabled && value.get() != LiteralStringRef("1")) ||
			    (!enabled && value.get() != LiteralStringRef("0"))) {
				tr->set(tagThrottleAutoEnabledKey, LiteralStringRef(enabled ? "1" : "0"));
				signalThrottleChange(tr);

				wait(safeThreadFutureToFuture(tr->commit()));
			}
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<bool> unthrottleMatchingThrottles(Reference<IDatabase> db,
                                               KeyRef beginKey,
                                               KeyRef endKey,
                                               Optional<TransactionPriority> priority,
                                               bool onlyExpiredThrottles) {
	state Reference<ITransaction> tr = db->createTransaction();

	state KeySelector begin = firstGreaterOrEqual(beginKey);
	state KeySelector end = firstGreaterOrEqual(endKey);

	state bool removed = false;

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			// holds memory of the RangeResult
			state ThreadFuture<RangeResult> f = tr->getRange(begin, end, 1000);
			state RangeResult tags = wait(safeThreadFutureToFuture(f));
			state uint64_t unthrottledTags = 0;
			uint64_t manualUnthrottledTags = 0;
			for (auto tag : tags) {
				if (onlyExpiredThrottles) {
					double expirationTime = TagThrottleValue::fromValue(tag.value).expirationTime;
					if (expirationTime == 0 || expirationTime > now()) {
						continue;
					}
				}

				TagThrottleKey key = TagThrottleKey::fromKey(tag.key);
				if (priority.present() && key.priority != priority.get()) {
					continue;
				}

				if (key.throttleType == TagThrottleType::MANUAL) {
					++manualUnthrottledTags;
				}

				removed = true;
				tr->clear(tag.key);
				unthrottledTags++;
			}

			if (manualUnthrottledTags > 0) {
				wait(updateThrottleCount(tr, -manualUnthrottledTags));
			}

			if (unthrottledTags > 0) {
				signalThrottleChange(tr);
			}

			wait(safeThreadFutureToFuture(tr->commit()));

			if (!tags.more) {
				return removed;
			}

			ASSERT(tags.size() > 0);
			begin = KeySelector(firstGreaterThan(tags[tags.size() - 1].key), tags.arena());
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

Future<bool> unthrottleAll(Reference<IDatabase> db,
                           Optional<TagThrottleType> tagThrottleType,
                           Optional<TransactionPriority> priority) {
	KeyRef begin = tagThrottleKeys.begin;
	KeyRef end = tagThrottleKeys.end;

	if (tagThrottleType.present() && tagThrottleType == TagThrottleType::AUTO) {
		begin = tagThrottleAutoKeysPrefix;
	} else if (tagThrottleType.present() && tagThrottleType == TagThrottleType::MANUAL) {
		end = tagThrottleAutoKeysPrefix;
	}

	return unthrottleMatchingThrottles(db, begin, end, priority, false);
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> throttleCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {

	if (tokens.size() == 1) {
		printUsage(tokens[0]);
		return false;
	} else if (tokencmp(tokens[1], "list")) {
		if (tokens.size() > 4) {
			printf("Usage: throttle list [throttled|recommended|all] [LIMIT]\n");
			printf("\n");
			printf("Lists tags that are currently throttled.\n");
			printf("The default LIMIT is 100 tags.\n");
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

		state int throttleListLimit = 100;
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
			wait(store(tags, getThrottledTags(db, throttleListLimit, true)));
		} else if (reportThrottled) {
			wait(store(tags, getThrottledTags(db, throttleListLimit)));
		} else if (reportRecommended) {
			wait(store(tags, getRecommendedTags(db, throttleListLimit)));
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

		TagSet tags;
		tags.addTag(tokens[3]);

		wait(throttleTags(db, tags, tpsRate, duration, TagThrottleType::MANUAL, priority));
		printf("Tag `%s' has been throttled\n", tokens[3].toString().c_str());
	} else if (tokencmp(tokens[1], "off")) {
		int nextIndex = 2;
		TagSet tags;
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
				if (tags.size() > 0 || nextIndex == tokens.size() - 1) {
					is_error = true;
					continue;
				}
				tags.addTag(tokens[nextIndex + 1]);
				nextIndex += 2;
			}
		}

		if (!is_error) {
			state const char* throttleTypeString =
			    !throttleType.present() ? "" : (throttleType.get() == TagThrottleType::AUTO ? "auto-" : "manually ");
			state std::string priorityString =
			    priority.present() ? format(" at %s priority", transactionPriorityToString(priority.get(), false)) : "";

			if (tags.size() > 0) {
				bool success = wait(unthrottleTags(db, tags, throttleType, priority));
				if (success) {
					printf("Unthrottled tag `%s'%s\n", tokens[3].toString().c_str(), priorityString.c_str());
				} else {
					printf("Tag `%s' was not %sthrottled%s\n",
					       tokens[3].toString().c_str(),
					       throttleTypeString,
					       priorityString.c_str());
				}
			} else {
				bool unthrottled = wait(unthrottleAll(db, throttleType, priority));
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
		wait(enableAuto(db, autoTagThrottlingEnabled));
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
