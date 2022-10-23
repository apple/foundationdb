/*
 * QuotaCommand.actor.cpp
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
#include "flow/actorcompiler.h" // This must be the last include

namespace {

enum class LimitType { RESERVED, TOTAL };

Optional<TransactionTag> parseTag(StringRef token) {
	if (token.size() > CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH) {
		return {};
	} else {
		return token;
	}
}

Optional<LimitType> parseLimitType(StringRef token) {
	if (token == "reserved_throughput"_sr) {
		return LimitType::RESERVED;
	} else if (token == "total_throughput"_sr) {
		return LimitType::TOTAL;
	} else {
		return {};
	}
}

Optional<double> parseLimitValue(StringRef token) {
	try {
		return std::stod(token.toString());
	} catch (...) {
		return {};
	}
}

ACTOR Future<Void> getQuota(Reference<IDatabase> db, TransactionTag tag, LimitType limitType) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		try {
			state ThreadFuture<Optional<Value>> resultFuture = tr->get(ThrottleApi::getTagQuotaKey(tag));
			Optional<Value> v = wait(safeThreadFutureToFuture(resultFuture));
			if (!v.present()) {
				fmt::print("<empty>\n");
			} else {
				auto const quota = ThrottleApi::TagQuotaValue::fromValue(v.get());
				if (limitType == LimitType::TOTAL) {
					fmt::print("{}\n", quota.totalQuota * CLIENT_KNOBS->READ_COST_BYTE_FACTOR);
				} else if (limitType == LimitType::RESERVED) {
					fmt::print("{}\n", quota.reservedQuota * CLIENT_KNOBS->READ_COST_BYTE_FACTOR);
				}
			}
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<Void> setQuota(Reference<IDatabase> db, TransactionTag tag, LimitType limitType, double value) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			state ThreadFuture<Optional<Value>> resultFuture = tr->get(ThrottleApi::getTagQuotaKey(tag));
			Optional<Value> v = wait(safeThreadFutureToFuture(resultFuture));
			ThrottleApi::TagQuotaValue quota;
			if (v.present()) {
				quota = ThrottleApi::TagQuotaValue::fromValue(v.get());
			}
			// Internally, costs are stored in terms of pages, but in the API,
			// costs are specified in terms of bytes
			if (limitType == LimitType::TOTAL) {
				quota.totalQuota = (value - 1) / CLIENT_KNOBS->READ_COST_BYTE_FACTOR + 1;
			} else if (limitType == LimitType::RESERVED) {
				quota.reservedQuota = (value - 1) / CLIENT_KNOBS->READ_COST_BYTE_FACTOR + 1;
			}
			if (!quota.isValid()) {
				throw invalid_throttle_quota_value();
			}
			ThrottleApi::setTagQuota(tr, tag, quota.reservedQuota, quota.totalQuota);
			wait(safeThreadFutureToFuture(tr->commit()));
			fmt::print("Successfully updated quota.\n");
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<Void> clearQuota(Reference<IDatabase> db, TransactionTag tag) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			tr->clear(ThrottleApi::getTagQuotaKey(tag));
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

constexpr auto usage = "quota [get <tag> [reserved_throughput|total_throughput] | set <tag> "
                       "[reserved_throughput|total_throughput] <value> | clear <tag>]";

bool exitFailure() {
	fmt::print(usage);
	return false;
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> quotaCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	state bool result = true;
	if (tokens.size() < 3 || tokens.size() > 5) {
		return exitFailure();
	} else {
		auto const tag = parseTag(tokens[2]);
		if (!tag.present()) {
			return exitFailure();
		}
		if (tokens[1] == "get"_sr) {
			if (tokens.size() != 4) {
				return exitFailure();
			}
			auto const limitType = parseLimitType(tokens[3]);
			if (!limitType.present()) {
				return exitFailure();
			}
			wait(getQuota(db, tag.get(), limitType.get()));
			return true;
		} else if (tokens[1] == "set"_sr) {
			if (tokens.size() != 5) {
				return exitFailure();
			}
			auto const limitType = parseLimitType(tokens[3]);
			auto const limitValue = parseLimitValue(tokens[4]);
			if (!limitType.present() || !limitValue.present()) {
				return exitFailure();
			}
			wait(setQuota(db, tag.get(), limitType.get(), limitValue.get()));
			return true;
		} else if (tokens[1] == "clear"_sr) {
			if (tokens.size() != 3) {
				return exitFailure();
			}
			wait(clearQuota(db, tag.get()));
			return true;
		} else {
			return exitFailure();
		}
	}
}

} // namespace fdb_cli
