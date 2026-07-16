/*
 * RangeLockCommand.cpp
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

#include "fdbcli/fdbcli.h"
#include "fdbclient/RangeLock.h"
#include "flow/Arena.h"

namespace fdb_cli {

static const std::string RANGELOCK_REGISTER_USAGE =
    "To register an owner: rangelock register <OWNER_ID> <DESCRIPTION>\n";
static const std::string RANGELOCK_UNREGISTER_USAGE = "To unregister an owner: rangelock unregister <OWNER_ID>\n";
static const std::string RANGELOCK_OWNERS_USAGE = "To list owners: rangelock owners\n";
static const std::string RANGELOCK_TAKE_USAGE = "To lock a range: rangelock take <BEGIN_KEY> <END_KEY> <OWNER_ID>\n";
static const std::string RANGELOCK_RELEASE_USAGE =
    "To release a lock: rangelock release <BEGIN_KEY> <END_KEY> <OWNER_ID>\n";
static const std::string RANGELOCK_RELEASE_ALL_USAGE =
    "To release every lock held by an owner: rangelock release-all <OWNER_ID>\n";
static const std::string RANGELOCK_LIST_USAGE =
    "To list locked ranges: rangelock list [<BEGIN_KEY> <END_KEY>]\n"
    "  Omit both keys to list every locked range. If supplied, BEGIN_KEY and END_KEY must be given together.\n";

static const std::string RANGELOCK_HELP_MESSAGE =
    RANGELOCK_REGISTER_USAGE + RANGELOCK_UNREGISTER_USAGE + RANGELOCK_OWNERS_USAGE + RANGELOCK_TAKE_USAGE +
    RANGELOCK_RELEASE_USAGE + RANGELOCK_RELEASE_ALL_USAGE + RANGELOCK_LIST_USAGE;

// Range locks only take effect when commit proxies are started with
// knob_enable_read_lock_on_range=true. The client cannot probe the knob
// (server knobs aren't exposed to fdbclient), so we print this notice
// after any operation that the user might assume blocks writes.
static void printKnobReminder() {
	fmt::println("NOTE: Range locks take effect only when commit proxies were started with");
	fmt::println("      knob_enable_read_lock_on_range=true. If that knob is off cluster-wide,");
	fmt::println("      this operation persists metadata but writes are not actually rejected.");
	fmt::println("      Verify the knob in commit-proxy startup logs.");
}

// Validate range bounds and return a Standalone KeyRange. Prints a user-facing
// error and returns an empty Optional if the range is invalid (empty, inverted,
// or outside the normal key space).
static Optional<KeyRange> parseNormalKeyRange(Key rangeBegin, Key rangeEnd) {
	if (rangeBegin >= rangeEnd) {
		fmt::println("ERROR: BEGIN_KEY ({}) must be strictly less than END_KEY ({})",
		             rangeBegin.toString(),
		             rangeEnd.toString());
		return {};
	}
	KeyRangeRef range(rangeBegin, rangeEnd);
	if (!normalKeys.contains(range)) {
		fmt::println("ERROR: Range {} is not within the normal key space [\"\", \\xff)", range.toString());
		return {};
	}
	return Standalone<KeyRangeRef>(range);
}

// Map a server-side rangelock error to a user-friendly message and return false.
// actor_cancelled is rethrown by the caller before this is reached.
static bool reportRangeLockError(const Error& e, std::string_view operation) {
	switch (e.code()) {
	case error_code_range_lock_reject:
		fmt::println("ERROR: cannot {}: range overlaps a lock held by another owner", operation);
		break;
	case error_code_range_unlock_reject:
		fmt::println("ERROR: cannot {}: range is not held by this owner (or held with a different range)", operation);
		break;
	case error_code_range_lock_failed:
		fmt::println("ERROR: cannot {}: invalid range, unregistered owner, or empty argument", operation);
		break;
	default:
		fmt::println("ERROR: cannot {}: {} ({})", operation, e.what(), e.code());
		break;
	}
	return false;
}

Future<bool> rangeLockCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokens.size() < 2) {
		fmt::print("{}", RANGELOCK_HELP_MESSAGE);
		co_return false;
	}

	if (tokencmp(tokens[1], "register")) {
		if (tokens.size() != 4) {
			fmt::print("{}", RANGELOCK_REGISTER_USAGE);
			co_return false;
		}
		std::string ownerId = tokens[2].toString();
		std::string description = tokens[3].toString();
		if (ownerId.empty() || description.empty()) {
			fmt::println("ERROR: Owner ID and description must be non-empty");
			fmt::print("{}", RANGELOCK_REGISTER_USAGE);
			co_return false;
		}
		try {
			co_await registerRangeLockOwner(cx, ownerId, description);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			co_return reportRangeLockError(e, "register owner");
		}
		fmt::println("Registered range lock owner: {}", ownerId);
		co_return true;
	}

	if (tokencmp(tokens[1], "unregister")) {
		if (tokens.size() != 3) {
			fmt::print("{}", RANGELOCK_UNREGISTER_USAGE);
			co_return false;
		}
		std::string ownerId = tokens[2].toString();
		if (ownerId.empty()) {
			fmt::println("ERROR: Owner ID must be non-empty");
			fmt::print("{}", RANGELOCK_UNREGISTER_USAGE);
			co_return false;
		}
		try {
			co_await removeRangeLockOwner(cx, ownerId);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			co_return reportRangeLockError(e, "unregister owner");
		}
		fmt::println("Unregistered range lock owner: {}", ownerId);
		co_return true;
	}

	if (tokencmp(tokens[1], "owners")) {
		if (tokens.size() != 2) {
			fmt::print("{}", RANGELOCK_OWNERS_USAGE);
			co_return false;
		}
		std::vector<RangeLockOwner> owners;
		try {
			owners = co_await getAllRangeLockOwners(cx);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			co_return reportRangeLockError(e, "list owners");
		}
		fmt::println("Total {} range lock owners", owners.size());
		for (const auto& owner : owners) {
			fmt::println("  {}", owner.toString());
		}
		co_return true;
	}

	if (tokencmp(tokens[1], "take")) {
		if (tokens.size() != 5) {
			fmt::print("{}", RANGELOCK_TAKE_USAGE);
			co_return false;
		}
		std::string ownerId = tokens[4].toString();
		if (ownerId.empty()) {
			fmt::println("ERROR: Owner ID must be non-empty");
			fmt::print("{}", RANGELOCK_TAKE_USAGE);
			co_return false;
		}
		Optional<KeyRange> range = parseNormalKeyRange(tokens[2], tokens[3]);
		if (!range.present()) {
			co_return false;
		}
		try {
			co_await takeExclusiveReadLockOnRange(cx, range.get(), ownerId);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			co_return reportRangeLockError(e, "take lock");
		}
		fmt::println("Locked range {} for owner {}", range.get().toString(), ownerId);
		printKnobReminder();
		co_return true;
	}

	if (tokencmp(tokens[1], "release")) {
		if (tokens.size() != 5) {
			fmt::print("{}", RANGELOCK_RELEASE_USAGE);
			co_return false;
		}
		std::string ownerId = tokens[4].toString();
		if (ownerId.empty()) {
			fmt::println("ERROR: Owner ID must be non-empty");
			fmt::print("{}", RANGELOCK_RELEASE_USAGE);
			co_return false;
		}
		Optional<KeyRange> range = parseNormalKeyRange(tokens[2], tokens[3]);
		if (!range.present()) {
			co_return false;
		}
		try {
			co_await releaseExclusiveReadLockOnRange(cx, range.get(), ownerId);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			co_return reportRangeLockError(e, "release lock");
		}
		fmt::println("Released range {} for owner {}", range.get().toString(), ownerId);
		co_return true;
	}

	if (tokencmp(tokens[1], "release-all")) {
		if (tokens.size() != 3) {
			fmt::print("{}", RANGELOCK_RELEASE_ALL_USAGE);
			co_return false;
		}
		std::string ownerId = tokens[2].toString();
		if (ownerId.empty()) {
			fmt::println("ERROR: Owner ID must be non-empty");
			fmt::print("{}", RANGELOCK_RELEASE_ALL_USAGE);
			co_return false;
		}
		try {
			co_await releaseExclusiveReadLockByUser(cx, ownerId);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			co_return reportRangeLockError(e, "release all locks");
		}
		fmt::println("Released all locks held by owner {}", ownerId);
		co_return true;
	}

	if (tokencmp(tokens[1], "list")) {
		KeyRange range = normalKeys;
		if (tokens.size() == 4) {
			Optional<KeyRange> parsed = parseNormalKeyRange(tokens[2], tokens[3]);
			if (!parsed.present()) {
				co_return false;
			}
			range = parsed.get();
		} else if (tokens.size() != 2) {
			fmt::print("{}", RANGELOCK_LIST_USAGE);
			co_return false;
		}
		std::vector<std::pair<KeyRange, RangeLockState>> locks;
		try {
			locks = co_await findExclusiveReadLockOnRange(cx, range);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			co_return reportRangeLockError(e, "list locks");
		}
		fmt::println("Total {} locked ranges in {}", locks.size(), range.toString());
		for (const auto& lock : locks) {
			fmt::println("  {} -> {}", lock.first.toString(), lock.second.toString());
		}
		co_return true;
	}

	fmt::print("{}", RANGELOCK_HELP_MESSAGE);
	co_return false;
}

CommandFactory rangeLockFactory(
    "rangelock",
    CommandHelp("rangelock [register|unregister|owners|take|release|release-all|list] [ARGs]",
                "manage exclusive read locks on key ranges",
                RANGELOCK_HELP_MESSAGE.c_str()));

} // namespace fdb_cli
