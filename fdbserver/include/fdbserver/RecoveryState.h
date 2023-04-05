/*
 * RecoveryState.h
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

#ifndef FDBSERVER_RECOVERYSTATE_H
#define FDBSERVER_RECOVERYSTATE_H
#pragma once

#include "flow/serialize.h"

// RecoveryState and RecoveryStatus should probably be merged.  The former is passed through ServerDBInfo and used for
// "real" decisions in the system; the latter is slightly more detailed and is used by the status infrastructure.  But
// I'm scared to make changes to the former so close to 1.0 release, so I'm making the latter.

enum class RecoveryState {
	UNINITIALIZED = 0,
	READING_CSTATE = 1,
	LOCKING_CSTATE = 2,
	RECRUITING = 3,
	RECOVERY_TRANSACTION = 4,
	WRITING_CSTATE = 5,
	ACCEPTING_COMMITS = 6,
	ALL_LOGS_RECRUITED = 7,
	STORAGE_RECOVERED = 8,
	FULLY_RECOVERED = 9
};

namespace RecoveryStatus {
enum RecoveryStatus {
	reading_coordinated_state,
	locking_coordinated_state,
	locking_old_transaction_servers,
	reading_transaction_system_state,
	configuration_missing,
	configuration_never_created,
	configuration_invalid,
	recruiting_transaction_servers,
	initializing_transaction_servers,
	recovery_transaction,
	writing_coordinated_state,
	accepting_commits,
	all_logs_recruited,
	storage_recovered,
	fully_recovered,
	END
};

// in Status.actor.cpp
extern const char* names[];
extern const char* descriptions[];
}; // namespace RecoveryStatus

#endif
