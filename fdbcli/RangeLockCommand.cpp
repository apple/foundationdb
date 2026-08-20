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
#include "fdbclient/RangeLockConfiguration.h"
#include "flow/Arena.h"

#include <algorithm>

namespace fdb_cli {

static const std::string RANGELOCK_STATUS_USAGE = "To inspect readiness and current proxies: rangelock status\n";
static const std::string RANGELOCK_RECONCILE_USAGE =
    "To reconcile durable lock state: rangelock reconcile [<MIGRATION_ID>]\n"
    "  Requires a homogeneous upgraded transaction system. The database is locked until reconciliation completes.\n"
    "  Omit MIGRATION_ID to resume an interrupted migration or start a new one.\n";
static const std::string RANGELOCK_REGISTER_USAGE =
    "To register an owner: rangelock register <OWNER_ID> <DESCRIPTION>\n";
static const std::string RANGELOCK_UNREGISTER_USAGE = "To unregister an owner: rangelock unregister <OWNER_ID>\n";
static const std::string RANGELOCK_OWNERS_USAGE = "To list owners: rangelock owners\n";
static const std::string RANGELOCK_TAKE_USAGE = "To lock a range: rangelock take <BEGIN_KEY> <END_KEY> <OWNER_ID>\n";
static const std::string RANGELOCK_RELEASE_USAGE =
    "To release a lock: rangelock release <BEGIN_KEY> <END_KEY> <OWNER_ID>\n";
static const std::string RANGELOCK_RELEASE_ALL_USAGE =
    "To release every lock held by an owner: rangelock release-all <OWNER_ID>\n";
static const std::string RANGELOCK_FORCE_RELEASE_USAGE =
    "Emergency only: rangelock force-release <BEGIN_KEY> <END_KEY> <OWNER_ID>\n"
    "  This can remove a live BulkLoad fence. Prefer bulkload cancel <JOBID>.\n";
static const std::string RANGELOCK_FORCE_RELEASE_ALL_USAGE =
    "Emergency only: rangelock force-release-all <OWNER_ID>\n"
    "  This can remove a live BulkLoad fence. Prefer bulkload cancel <JOBID>.\n";
static const std::string RANGELOCK_LIST_USAGE =
    "To list locked ranges: rangelock list [<BEGIN_KEY> <END_KEY>]\n"
    "  Omit both keys to list every locked range. If supplied, BEGIN_KEY and END_KEY must be given together.\n";

static const std::string RANGELOCK_HELP_MESSAGE =
    RANGELOCK_STATUS_USAGE + RANGELOCK_RECONCILE_USAGE + RANGELOCK_REGISTER_USAGE + RANGELOCK_UNREGISTER_USAGE +
    RANGELOCK_OWNERS_USAGE + RANGELOCK_TAKE_USAGE + RANGELOCK_RELEASE_USAGE + RANGELOCK_RELEASE_ALL_USAGE +
    RANGELOCK_LIST_USAGE + RANGELOCK_FORCE_RELEASE_USAGE + RANGELOCK_FORCE_RELEASE_ALL_USAGE;

static Optional<UID> parseMigrationId(StringRef text) {
	if (text.size() != 32 || !std::all_of(text.begin(), text.end(), [](uint8_t c) {
		    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
	    })) {
		return {};
	}
	const UID id = UID::fromString(text.toString());
	return id.isValid() ? Optional<UID>(id) : Optional<UID>();
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
		fmt::println("ERROR: cannot {}: range conflicts with an existing lock, or the owner still holds locks",
		             operation);
		break;
	case error_code_range_unlock_reject:
		fmt::println("ERROR: cannot {}: acquisition differs or an active BulkLoad job owns the fence", operation);
		fmt::println("       Cancel an active BulkLoad job with bulkload cancel <JOBID>.");
		break;
	case error_code_range_lock_failed:
		fmt::println("ERROR: cannot {}: invalid range, unregistered owner, or empty argument", operation);
		break;
	case error_code_range_lock_not_ready:
		fmt::println("ERROR: cannot {}: range-lock state or commit-proxy admission is not ready", operation);
		fmt::println("       Check rangelock status and the homogeneous-upgrade requirements before reconciling.");
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

	if (tokencmp(tokens[1], "status")) {
		if (tokens.size() != 2) {
			fmt::print("{}", RANGELOCK_STATUS_USAGE);
			co_return false;
		}
		try {
			const RangeLockConfiguration configuration = co_await getRangeLockConfiguration(cx);
			fmt::println("Range-lock state: {} (format {})", configuration.toString(), configuration.formatRevision());
			const RangeLockAdmissionStatus status = co_await getRangeLockAdmissionStatus(cx);
			fmt::println("All current commit proxies have valid enforcement state: {}",
			             status.allProxiesHaveValidState);
			fmt::println("New-lock mode enabled on all current commit proxies: {}", status.allProxiesEnableAcquisition);
			fmt::println("All current commit proxies encode shard locations: {}",
			             status.allProxiesEncodeShardLocations);
			fmt::println("NOTE: Proxy status does not verify resolver or cluster-controller binary versions.");
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			co_return reportRangeLockError(e, "inspect readiness");
		}
		co_return true;
	}

	if (tokencmp(tokens[1], "reconcile")) {
		if (tokens.size() != 2 && tokens.size() != 3) {
			fmt::print("{}", RANGELOCK_RECONCILE_USAGE);
			co_return false;
		}
		Optional<UID> requestedId;
		if (tokens.size() == 3) {
			requestedId = parseMigrationId(tokens[2]);
			if (!requestedId.present()) {
				fmt::println("ERROR: MIGRATION_ID must be a nonzero 32-digit hexadecimal UID");
				co_return false;
			}
		}
		UID migrationId;
		try {
			const RangeLockConfiguration configuration = co_await getRangeLockConfiguration(cx);
			migrationId = requestedId.present()         ? requestedId.get()
			              : configuration.isMigrating() ? configuration.migrationId()
			                                            : deterministicRandom()->randomUniqueID();
			fmt::println("WARNING: All transaction-system roles must be upgraded before reconciliation.");
			fmt::println("Reconciliation {} locks the database until it completes.", migrationId.toString());
			co_await reconcileRangeLocks(cx, migrationId);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			reportRangeLockError(e, "reconcile lock state");
			if (migrationId.isValid()) {
				fmt::println("If reconciliation started, its database lock remains held. Resume with:");
				fmt::println("  rangelock reconcile {}", migrationId.toString());
			}
			co_return false;
		}
		fmt::println("Range-lock reconciliation {} completed; its database lock is released.", migrationId.toString());
		co_return true;
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
		co_return true;
	}

	if (tokencmp(tokens[1], "release") || tokencmp(tokens[1], "force-release")) {
		const bool force = tokencmp(tokens[1], "force-release");
		if (tokens.size() != 5) {
			fmt::print("{}", force ? RANGELOCK_FORCE_RELEASE_USAGE : RANGELOCK_RELEASE_USAGE);
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
			if (force) {
				fmt::println("WARNING: Emergency release may invalidate an active BulkLoad job.");
			}
			co_await releaseExclusiveReadLockOnRange(cx, range.get(), ownerId, force);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			co_return reportRangeLockError(e, "release lock");
		}
		fmt::println("Released range {} for owner {}", range.get().toString(), ownerId);
		co_return true;
	}

	if (tokencmp(tokens[1], "release-all") || tokencmp(tokens[1], "force-release-all")) {
		const bool force = tokencmp(tokens[1], "force-release-all");
		if (tokens.size() != 3) {
			fmt::print("{}", force ? RANGELOCK_FORCE_RELEASE_ALL_USAGE : RANGELOCK_RELEASE_ALL_USAGE);
			co_return false;
		}
		std::string ownerId = tokens[2].toString();
		if (ownerId.empty()) {
			fmt::println("ERROR: Owner ID must be non-empty");
			fmt::print("{}", RANGELOCK_RELEASE_ALL_USAGE);
			co_return false;
		}
		try {
			if (force) {
				fmt::println("WARNING: Emergency release may invalidate an active BulkLoad job.");
			}
			co_await releaseExclusiveReadLockByUser(cx, ownerId, force);
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

CommandFactory rangeLockFactory("rangelock",
                                CommandHelp("rangelock "
                                            "[status|reconcile|register|unregister|owners|take|release|release-all|"
                                            "list|force-release|force-release-all] [ARGs]",
                                            "manage exclusive read locks on key ranges",
                                            RANGELOCK_HELP_MESSAGE.c_str()));

} // namespace fdb_cli
