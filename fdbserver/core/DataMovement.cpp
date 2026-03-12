/*
 * DataMovement.cpp
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

#include "fdbserver/core/DataMovement.h"

#include <map>

#include "fdbserver/core/Knobs.h"

namespace {
using DmReasonPriorityMapping = std::map<DataMovementReason, int>;
using PriorityDmReasonMapping = std::map<int, DataMovementReason>;

std::pair<const DmReasonPriorityMapping*, const PriorityDmReasonMapping*> buildPriorityMappings() {
	static DmReasonPriorityMapping reasonPriority{
		{ DataMovementReason::INVALID, -1 },
		{ DataMovementReason::RECOVER_MOVE, SERVER_KNOBS->PRIORITY_RECOVER_MOVE },
		{ DataMovementReason::REBALANCE_UNDERUTILIZED_TEAM, SERVER_KNOBS->PRIORITY_REBALANCE_UNDERUTILIZED_TEAM },
		{ DataMovementReason::REBALANCE_OVERUTILIZED_TEAM, SERVER_KNOBS->PRIORITY_REBALANCE_OVERUTILIZED_TEAM },
		{ DataMovementReason::REBALANCE_READ_OVERUTIL_TEAM, SERVER_KNOBS->PRIORITY_REBALANCE_READ_OVERUTIL_TEAM },
		{ DataMovementReason::REBALANCE_READ_UNDERUTIL_TEAM, SERVER_KNOBS->PRIORITY_REBALANCE_READ_UNDERUTIL_TEAM },
		{ DataMovementReason::PERPETUAL_STORAGE_WIGGLE, SERVER_KNOBS->PRIORITY_PERPETUAL_STORAGE_WIGGLE },
		{ DataMovementReason::TEAM_HEALTHY, SERVER_KNOBS->PRIORITY_TEAM_HEALTHY },
		{ DataMovementReason::TEAM_CONTAINS_UNDESIRED_SERVER, SERVER_KNOBS->PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER },
		{ DataMovementReason::TEAM_REDUNDANT, SERVER_KNOBS->PRIORITY_TEAM_REDUNDANT },
		{ DataMovementReason::MERGE_SHARD, SERVER_KNOBS->PRIORITY_MERGE_SHARD },
		{ DataMovementReason::POPULATE_REGION, SERVER_KNOBS->PRIORITY_POPULATE_REGION },
		{ DataMovementReason::TEAM_UNHEALTHY, SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY },
		{ DataMovementReason::TEAM_2_LEFT, SERVER_KNOBS->PRIORITY_TEAM_2_LEFT },
		{ DataMovementReason::TEAM_1_LEFT, SERVER_KNOBS->PRIORITY_TEAM_1_LEFT },
		{ DataMovementReason::TEAM_FAILED, SERVER_KNOBS->PRIORITY_TEAM_FAILED },
		{ DataMovementReason::TEAM_0_LEFT, SERVER_KNOBS->PRIORITY_TEAM_0_LEFT },
		{ DataMovementReason::SPLIT_SHARD, SERVER_KNOBS->PRIORITY_SPLIT_SHARD },
		{ DataMovementReason::ENFORCE_MOVE_OUT_OF_PHYSICAL_SHARD,
		  SERVER_KNOBS->PRIORITY_ENFORCE_MOVE_OUT_OF_PHYSICAL_SHARD },
		{ DataMovementReason::REBALANCE_STORAGE_QUEUE, SERVER_KNOBS->PRIORITY_REBALANCE_STORAGE_QUEUE },
		{ DataMovementReason::ASSIGN_EMPTY_RANGE, -2 },
		{ DataMovementReason::SEED_SHARD_SERVER, -3 },
		{ DataMovementReason::NUMBER_OF_REASONS, -4 },
	};

	static PriorityDmReasonMapping priorityReason;
	if (priorityReason.empty()) {
		for (const auto& [reason, priority] : reasonPriority) {
			priorityReason[priority] = reason;
		}
		if (priorityReason.size() != reasonPriority.size()) {
			TraceEvent(SevError, "DuplicateDataMovementPriority").log();
			ASSERT(false);
		}
	}

	return { &reasonPriority, &priorityReason };
}
} // namespace

int dataMovementPriority(DataMovementReason reason) {
	auto [reasonPriority, _] = buildPriorityMappings();
	return reasonPriority->at(reason);
}

DataMovementReason priorityToDataMovementReason(int priority) {
	auto [_, priorityReason] = buildPriorityMappings();
	return priorityReason->at(priority);
}
