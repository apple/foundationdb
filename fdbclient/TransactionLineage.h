/*
 * TransactionLineage.h
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

#pragma once

#include "fdbclient/ActorLineageProfiler.h"

struct TransactionLineage : LineageProperties<TransactionLineage> {
	enum class Operation {
		Unset,
		GetValue,
		GetKey,
		GetKeyValues,
		WatchValue,
		GetConsistentReadVersion,
		Commit,
		GetKeyServersLocations
	};
	static constexpr std::string_view name = "Transaction"sv;
	uint64_t txID;
	Operation operation = Operation::Unset;

	bool isSet(uint64_t TransactionLineage::*member) const { return this->*member > 0; }
	bool isSet(Operation TransactionLineage::*member) const { return this->*member != Operation::Unset; }
};

struct TransactionLineageCollector : IALPCollector<TransactionLineage> {
	using Operation = TransactionLineage::Operation;
	std::optional<std::any> collect(ActorLineage* lineage) {
		std::map<std::string_view, std::any> res;
		auto txID = lineage->get(&TransactionLineage::txID);
		if (txID.has_value()) {
			res["ID"sv] = txID.value();
		}
		auto operation = lineage->get(&TransactionLineage::operation);
		if (operation.has_value()) {
			switch (operation.value()) {
			case Operation::Unset:
				res["operation"sv] = "Unset"sv;
				break;
			case Operation::GetValue:
				res["operation"sv] = "GetValue"sv;
				break;
			case Operation::GetKey:
				res["operation"sv] = "GetKey"sv;
				break;
			case Operation::GetKeyValues:
				res["operation"sv] = "GetKeyValues"sv;
				break;
			case Operation::WatchValue:
				res["operation"sv] = "WatchValue"sv;
				break;
			case Operation::GetConsistentReadVersion:
				res["operation"sv] = "GetConsistentReadVersion"sv;
				break;
			case Operation::Commit:
				res["operation"sv] = "Commit"sv;
				break;
			case Operation::GetKeyServersLocations:
				res["operation"sv] = "GetKeyServersLocations"sv;
				break;
			}
		}
		if (res.empty()) {
			return std::optional<std::any>{};
		} else {
			return res;
		}
	}
};

#ifdef ENABLE_SAMPLING
template <class T, class V>
class ScopedLineage {
	V before;
	V T::*member;
	bool valid = true;

public:
	ScopedLineage(V T::*member, V const& value) : member(member) {
		auto& val = getCurrentLineage()->modify(member);
		before = val;
		val = value;
	}
	~ScopedLineage() {
		if (!valid) {
			return;
		}
		getCurrentLineage()->modify(member) = before;
	}
	ScopedLineage(ScopedLineage<T, V>&& o) : before(std::move(o.before)), member(o.member), valid(o.valid) {
		o.release();
	}
	ScopedLineage& operator=(ScopedLineage<T, V>&& o) {
		if (valid) {
			getCurrentLineage()->modify(member) = before;
		}
		before = std::move(o.before);
		member = o.member;
		valid = o.valid;
		o.release();
		return *this;
	}
	ScopedLineage(const ScopedLineage<T, V>&) = delete;
	ScopedLineage& operator=(const ScopedLineage<T, V>&) = delete;
	void release() { valid = false; }
};

template <class T, class V>
ScopedLineage<T, V> make_scoped_lineage(V T::*member, V const& value) {
	return ScopedLineage<T, V>(member, value);
}
#endif
