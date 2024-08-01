/*
 * operations.hpp
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

#ifndef MAKO_OPERATIONS_HPP
#define MAKO_OPERATIONS_HPP

#include <fdb_api.hpp>
#include <array>
#include <cassert>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>
#include "macro.hpp"
#include "mako.hpp"

namespace mako {

// determines how resultant future will be handled
enum class StepKind {
	NONE, ///< not part of the table: OP_TRANSACTION, OP_COMMIT
	IMM, ///< non-future ops that return immediately: e.g. set, clear_range
	READ, ///< blockable reads: get(), get_range(), get_read_version, ...
	COMMIT, ///< self-explanatory
	ON_ERROR ///< future is a result of tx.on_error()
};

// Ops that doesn't have concrete steps to execute and are there for measurements only
force_inline bool isAbstractOp(int op) noexcept {
	return op == OP_COMMIT || op == OP_TRANSACTION;
}

using StepFunction = fdb::Future (*)(fdb::Transaction& tx,
                                     Arguments const&,
                                     fdb::ByteString& /*key1*/,
                                     fdb::ByteString& /*key2*/,
                                     fdb::ByteString& /*value*/);

using PostStepFunction = void (*)(fdb::Future&,
                                  fdb::Transaction& tx,
                                  Arguments const&,
                                  fdb::ByteString& /*key1*/,
                                  fdb::ByteString& /*key2*/,
                                  fdb::ByteString& /*value*/);

struct Step {
	StepKind kind;
	StepFunction step_func_;
	PostStepFunction post_step_func_{ nullptr };
};

struct Operation {
	std::string_view name_;
	Step steps_[2];
	int num_steps_;
	bool needs_commit_;

	std::string_view name() const noexcept { return name_; }

	StepKind stepKind(int step) const noexcept {
		assert(step < steps());
		return steps_[step].kind;
	}

	StepFunction stepFunction(int step) const noexcept { return steps_[step].step_func_; }

	PostStepFunction postStepFunction(int step) const noexcept { return steps_[step].post_step_func_; }
	// how many steps in this op?
	int steps() const noexcept { return num_steps_; }
	// does the op needs to commit some time after its final step?
	bool needsCommit() const noexcept { return needs_commit_; }
};

extern const std::array<Operation, MAX_OP> opTable;

force_inline char const* getOpName(int ops_code) {
	if (ops_code >= 0 && ops_code < MAX_OP)
		return opTable[ops_code].name().data();
	return "";
}

struct OpIterator {
	int op, count, step;

	bool operator==(const OpIterator& other) const noexcept {
		return op == other.op && count == other.count && step == other.step;
	}

	bool operator!=(const OpIterator& other) const noexcept { return !(*this == other); }

	StepKind stepKind() const noexcept { return opTable[op].stepKind(step); }

	char const* opName() const noexcept { return getOpName(op); }
};

constexpr const OpIterator OpEnd = OpIterator{ MAX_OP, -1, -1 };

force_inline OpIterator getOpBegin(Arguments const& args) noexcept {
	for (auto op = 0; op < MAX_OP; op++) {
		if (isAbstractOp(op) || args.txnspec.ops[op][OP_COUNT] == 0)
			continue;
		return OpIterator{ op, 0, 0 };
	}
	return OpEnd;
}

force_inline OpIterator getOpNext(Arguments const& args, OpIterator current) noexcept {
	auto& [op, count, step] = current;
	assert(op < MAX_OP && !isAbstractOp(op));
	if (opTable[op].steps() > step + 1)
		return OpIterator{ op, count, step + 1 };
	count++;
	for (; op < MAX_OP; op++, count = 0) {
		if (isAbstractOp(op) || args.txnspec.ops[op][OP_COUNT] <= count)
			continue;
		return OpIterator{ op, count, 0 };
	}
	return OpEnd;
}

} // namespace mako

#endif /* MAKO_OPERATIONS_HPP */
