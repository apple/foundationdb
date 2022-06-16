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

enum class OpType { READ, WRITE };

Optional<TransactionTag> parseTag(StringRef token) {
	if (token.size() > CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH) {
		return {};
	} else {
		return token;
	}
}

Optional<LimitType> parseLimitType(StringRef token) {
	if (token == "reserved"_sr) {
		return LimitType::RESERVED;
	} else if (token == "total"_sr) {
		return LimitType::TOTAL;
	} else {
		return {};
	}
}

Optional<OpType> parseOpType(StringRef token) {
	if (token == "read"_sr) {
		return OpType::READ;
	} else if (token == "write"_sr) {
		return OpType::WRITE;
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

ACTOR Future<Void> getQuota(Reference<IDatabase> db, TransactionTag tag, LimitType limitType, OpType opType) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		try {
			state ThreadFuture<Optional<Value>> resultFuture = tr->get(tag.withPrefix(tagQuotaPrefix));
			Optional<Value> v = wait(safeThreadFutureToFuture(resultFuture));
			if (!v.present()) {
				fmt::print("<empty>\n");
			} else {
				auto const quota = ThrottleApi::TagQuotaValue::fromValue(v.get());
				if (limitType == LimitType::TOTAL && opType == OpType::READ) {
					fmt::print("{}\n", quota.totalReadQuota);
				} else if (limitType == LimitType::TOTAL && opType == OpType::WRITE) {
					fmt::print("{}\n", quota.totalWriteQuota);
				} else if (limitType == LimitType::RESERVED && opType == OpType::READ) {
					fmt::print("{}\n", quota.reservedReadQuota);
				} else if (limitType == LimitType::RESERVED && opType == OpType::WRITE) {
					fmt::print("{}\n", quota.reservedWriteQuota);
				}
			}
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<Void> setQuota(Reference<IDatabase> db,
                            TransactionTag tag,
                            LimitType limitType,
                            OpType opType,
                            double value) {
	state Reference<ITransaction> tr = db->createTransaction();
	state Key key = tag.withPrefix(tagQuotaPrefix);
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			state ThreadFuture<Optional<Value>> resultFuture = tr->get(key);
			Optional<Value> v = wait(safeThreadFutureToFuture(resultFuture));
			ThrottleApi::TagQuotaValue quota;
			if (v.present()) {
				quota = ThrottleApi::TagQuotaValue::fromValue(v.get());
			}
			if (limitType == LimitType::TOTAL && opType == OpType::READ) {
				quota.totalReadQuota = value;
			} else if (limitType == LimitType::TOTAL && opType == OpType::WRITE) {
				quota.totalWriteQuota = value;
			} else if (limitType == LimitType::RESERVED && opType == OpType::READ) {
				quota.reservedReadQuota = value;
			} else if (limitType == LimitType::RESERVED && opType == OpType::WRITE) {
				quota.reservedWriteQuota = value;
			}
			ThrottleApi::setTagQuota(tr,
			                         tag,
			                         quota.reservedReadQuota,
			                         quota.totalReadQuota,
			                         quota.reservedWriteQuota,
			                         quota.totalWriteQuota);
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

constexpr auto usage =
    "quota [get <tag> [reserved|total] [read|write]|set <tag> [reserved|total] [read|write] <value>]";

bool exitFailure() {
	fmt::print(usage);
	return false;
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> quotaCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	state bool result = true;
	if (tokens.size() != 5 && tokens.size() != 6) {
		return exitFailure();
	} else {
		auto tag = parseTag(tokens[2]);
		auto limitType = parseLimitType(tokens[3]);
		auto opType = parseOpType(tokens[4]);
		if (!tag.present() || !limitType.present() || !opType.present()) {
			return exitFailure();
		}
		if (tokens[1] == "get"_sr) {
			if (tokens.size() != 5) {
				return exitFailure();
			}
			wait(getQuota(db, tag.get(), limitType.get(), opType.get()));
			return true;
		} else if (tokens[1] == "set"_sr) {
			if (tokens.size() != 6) {
				return exitFailure();
			}
			auto const limitValue = parseLimitValue(tokens[5]);
			if (!limitValue.present()) {
				return exitFailure();
			}
			wait(setQuota(db, tag.get(), limitType.get(), opType.get(), limitValue.get()));
			return true;
		} else {
			return exitFailure();
		}
	}
}

} // namespace fdb_cli
