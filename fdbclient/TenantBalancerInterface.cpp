/*
 * TenantBalancerInterface.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache Lic---ense, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,+
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/TenantBalancerInterface.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include <string>
#include <unordered_map>

std::string TenantBalancerInterface::movementStateToString(MovementState movementState) {
	switch (movementState) {
	case MovementState::INITIALIZING:
		return "Initializing";
	case MovementState::STARTED:
		return "Started";
	case MovementState::READY_FOR_SWITCH:
		return "ReadyForSwitch";
	case MovementState::SWITCHING:
		return "Switching";
	case MovementState::COMPLETED:
		return "Completed";
	case MovementState::ERROR:
		return "Error";
	default:
		ASSERT(false);
	}
}

std::string TenantBalancerInterface::movementLocationToString(MovementLocation movementLocation) {
	switch (movementLocation) {
	case MovementLocation::SOURCE:
		return "Source";
	case MovementLocation::DEST:
		return "Destination";
	default:
		ASSERT(false);
	}
}

std::string TenantMovementInfo::toString() const {
	std::unordered_map<std::string, std::string> infoMap;
	infoMap["movementId"] = movementId.toString();
	infoMap["peerConnectionString"] = peerConnectionString;
	infoMap["sourcePrefix"] = sourcePrefix.toString();
	infoMap["destinationPrefix"] = destinationPrefix.toString();
	std::string movementInfo;
	for (const auto& itr : infoMap) {
		movementInfo += itr.first + " : " + itr.second + "\n";
	}
	return movementInfo;
}

std::string TenantMovementStatus::toString() const {
	std::unordered_map<std::string, std::string> statusInfoMap;
	statusInfoMap["isSourceLocked"] = std::to_string(isSourceLocked);
	statusInfoMap["isDestinationLocked"] = std::to_string(isDestinationLocked);
	statusInfoMap["movementState"] = TenantBalancerInterface::movementStateToString(movementState);
	statusInfoMap["databaseTimingDelay"] = std::to_string(databaseTimingDelay);
	statusInfoMap["databaseBackupStatus"] = databaseBackupStatus;
	if (mutationLag.present()) {
		statusInfoMap["mutationLag"] = std::to_string(mutationLag.get());
	}
	if (switchVersion.present()) {
		statusInfoMap["switchVersion"] = std::to_string(switchVersion.get());
	}
	if (errorMessage.present()) {
		statusInfoMap["errorMessage"] = errorMessage.get();
	}
	std::string movementInfo = tenantMovementInfo.toString();
	for (const auto& itr : statusInfoMap) {
		movementInfo += itr.first + " : " + itr.second + "\n";
	}
	return movementInfo;
}

std::string TenantMovementStatus::toJson() const {
	json_spirit::mValue statusRootValue;
	JSONDoc statusRoot(statusRootValue);

	// Insert tenantMoveInfo into JSON
	statusRoot.create("movementId") = tenantMovementInfo.movementId.toString();
	statusRoot.create("peerConnectionString") = tenantMovementInfo.peerConnectionString;
	statusRoot.create("sourcePrefix") = tenantMovementInfo.sourcePrefix.toString();
	statusRoot.create("destinationPrefix") = tenantMovementInfo.destinationPrefix.toString();

	// Insert movement status into JSON
	statusRoot.create("isSourceLocked") = isSourceLocked;
	statusRoot.create("isDestinationLocked") = isDestinationLocked;
	statusRoot.create("movementState") = TenantBalancerInterface::movementStateToString(movementState);
	statusRoot.create("databaseTimingDelay") = databaseTimingDelay;
	statusRoot.create("databaseBackupStatus") = databaseBackupStatus;
	if (mutationLag.present()) {
		statusRoot.create("mutationLag") = mutationLag.get();
	}
	if (switchVersion.present()) {
		statusRoot.create("switchVersion") = switchVersion.get();
	}
	if (errorMessage.present()) {
		statusRoot.create("errorMessage") = errorMessage.get();
	}
	return json_spirit::write_string(statusRootValue);
}
