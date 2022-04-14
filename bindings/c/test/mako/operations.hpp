/*
 * operations.hpp
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

#ifndef MAKO_OPERATIONS_HPP
#define MAKO_OPERATIONS_HPP

#include <fdb_api.hpp>
#include <array>
#include <cassert>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace mako {

struct Arguments;

/* transaction specification */
enum OpKind {
	OP_GETREADVERSION,
	OP_GET,
	OP_GETRANGE,
	OP_SGET,
	OP_SGETRANGE,
	OP_UPDATE,
	OP_INSERT,
	OP_INSERTRANGE,
	OP_OVERWRITE,
	OP_CLEAR,
	OP_SETCLEAR,
	OP_CLEARRANGE,
	OP_SETCLEARRANGE,
	OP_COMMIT,
	OP_TRANSACTION, /* pseudo-operation - cumulative time for the operation + commit */
	OP_TASK, /* pseudo-operation - cumulative time for each iteraton in run_workload */
	OP_READ_BG,
	MAX_OP /* must be the last item */
};

constexpr const int OP_COUNT = 0;
constexpr const int OP_RANGE = 1;
constexpr const int OP_REVERSE = 2;

// determines how resultant future will be handled
enum class StepKind {
	NONE, ///< not part of the table: OP_TRANSACTION, OP_COMMIT
	IMM, ///< non-future ops that return immediately: e.g. set, clear_range
	READ, ///< blockable reads: get(), get_range(), get_read_version, ...
	COMMIT, ///< self-explanatory
	ON_ERROR ///< future is a result of tx.on_error()
};

// Ops that doesn't have concrete steps to execute and are there for measurements only
inline bool isAbstractOp(int op) noexcept {
	return op == OP_COMMIT || op == OP_TRANSACTION; // || op == OP_TASK;
}

using StepFunction = fdb::Future (*)(fdb::Transaction tx,
                                     Arguments const&,
                                     fdb::ByteString& /*key1*/,
                                     fdb::ByteString& /*key2*/,
                                     fdb::ByteString& /*value*/);

using PostStepFunction = void (*)(fdb::Future,
                                  fdb::Transaction tx,
                                  Arguments const&,
                                  fdb::ByteString& /*key1*/,
                                  fdb::ByteString& /*key2*/,
                                  fdb::ByteString& /*value*/);

struct Step {
	StepKind kind;
	StepFunction step_func_;
	PostStepFunction post_step_func_{ nullptr };
};

class Operation {
	std::string_view name_;
	std::vector<Step> steps_;
	bool needs_commit_;

public:
	Operation(std::string_view name, std::vector<Step>&& steps, bool needs_commit)
	  : name_(name), steps_(std::move(steps)), needs_commit_(needs_commit) {}

	std::string_view name() const noexcept { return name_; }

	StepKind stepKind(int step) const noexcept {
		assert(step < steps());
		return steps_[step].kind;
	}

	StepFunction stepFunction(int step) const noexcept { return steps_[step].step_func_; }

	PostStepFunction postStepFunction(int step) const noexcept { return steps_[step].post_step_func_; }
	// how many steps in this op?
	int steps() const noexcept { return static_cast<int>(steps_.size()); }
	// does the op needs to commit some time after its final step?
	bool needsCommit() const noexcept { return needs_commit_; }
};

char const* getOpName(int ops_code);

extern const std::array<Operation, MAX_OP> opTable;

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

OpIterator getOpBegin(Arguments const& args) noexcept;

OpIterator getOpNext(Arguments const& args, OpIterator current) noexcept;

} // namespace mako

#endif /* MAKO_OPERATIONS_HPP */
