/*
 * RestoreUtil.cpp
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

#include "fdbserver/restoreworker/RestoreUtil.h"

#include <cstdio>
#include <iomanip>

const std::vector<std::string> RestoreRoleStr = { "Invalid", "Controller", "Loader", "Applier" };
int numRoles = RestoreRoleStr.size();

std::string getRoleStr(RestoreRole role) {
	if ((int)role >= numRoles || (int)role < 0) {
		printf("[ERROR] role:%d is out of scope\n", (int)role);
		return "[Unset]";
	}
	return RestoreRoleStr[(int)role];
}

std::string getHexString(StringRef input) {
	std::stringstream ss;
	for (int i = 0; i < input.size(); i++) {
		if (i % 4 == 0)
			ss << " ";
		if (i == 12) { // The end of 12bytes, which is the version size for value
			ss << "|";
		}
		if (i == (12 + 12)) { // The end of version + header
			ss << "@";
		}
		ss << std::setfill('0') << std::setw(2) << std::hex
		   << (int)input[i]; // [] operator moves the pointer in step of unit8
	}
	return ss.str();
}
