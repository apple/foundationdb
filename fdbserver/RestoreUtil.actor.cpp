/*
 * RestoreUtil.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/RestoreUtil.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

const std::vector<std::string> RestoreRoleStr = {"Invalid", "Master", "Loader", "Applier"};
int numRoles = RestoreRoleStr.size();

std::string getRoleStr(RestoreRole role) {
	if ( (int) role >= numRoles || (int) role < 0) {
		printf("[ERROR] role:%d is out of scope\n", (int) role);
		return "[Unset]";
	}
	return RestoreRoleStr[(int)role];
}

// CMDUID implementation
void CMDUID::initPhase(RestoreCommandEnum newPhase) {
	printf("CMDID, current phase:%d, new phase:%d\n", phase, newPhase);
	phase = (uint16_t) newPhase;
	cmdID = 0;
}

void CMDUID::nextPhase() {
	phase++;
	cmdID = 0;
}

void CMDUID::nextCmd() {
	cmdID++;
}

RestoreCommandEnum CMDUID::getPhase() {
	return (RestoreCommandEnum) phase;
}

void CMDUID::setPhase(RestoreCommandEnum newPhase) {
	phase = (uint16_t) newPhase;
}

void CMDUID::setBatch(int newBatchIndex) {
	batch = newBatchIndex;
}

uint64_t CMDUID::getIndex() {
	return cmdID;
}

std::string CMDUID::toString() const {
	return format("%04ld|%04ld|%016lld", batch, phase, cmdID);
}
