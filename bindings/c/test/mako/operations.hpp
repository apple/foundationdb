#ifndef MAKO_OPERATIONS_HPP
#define MAKO_OPERATIONS_HPP

#include <vector>
#include <string_view>

namespace mako {

/* transaction specification */
enum Operations {
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

class OpDesc {
	std::string_view name_;
	std::vector<StepKind> steps_;
	bool needs_commit_;

public:
	OpDesc(std::string_view name, std::vector<StepKind>&& steps, bool needs_commit)
	  : name_(name), steps_(std::move(steps)), needs_commit_(needs_commit) {}

	std::string_view name() const noexcept { return name_; }
	// what
	StepKind step_kind(int step) const noexcept {
		assert(step < steps());
		return steps_[step];
	}
	// how many steps in this op?
	int steps() const noexcept { return static_cast<int>(steps_.size()); }
	// does the op needs to commit some time after its final step?
	bool needs_commit() const noexcept { return needs_commit_; }
};

} // namespace mako

#endif /* MAKO_OPERATIONS_HPP */
