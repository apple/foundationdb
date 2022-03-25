/*
 * RestoreUtil.cpp
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

// This file implements the functions defined in RestoreUtil.h

#include "fdbserver/RestoreUtil.h"

#include "flow/actorcompiler.h" // This must be the last #include.

const std::vector<std::string> RestoreRoleStr = { "Invalid", "Controller", "Loader", "Applier" };
int numRoles = RestoreRoleStr.size();

// Similar to debugMutation(), we use debugFRMutation to track mutations for fast restore systems only.
#if CENABLED(0, NOT_IN_CLEAN)
StringRef debugFRKey = LiteralStringRef("\xff\xff\xff\xff");

// Track any mutation in fast restore that has overlap with debugFRKey
bool debugFRMutation(const char* context, Version version, MutationRef const& mutation) {
	if (mutation.type != mutation.ClearRange && mutation.param1 == debugFRKey) { // Single key mutation
		TraceEvent("FastRestoreMutationTracking")
		    .detail("At", context)
		    .detail("Version", version)
		    .detail("MutationType", getTypeString((MutationRef::Type)mutation.type))
		    .detail("Key", mutation.param1)
		    .detail("Value", mutation.param2);
	} else if (mutation.type == mutation.ClearRange && debugFRKey >= mutation.param1 &&
	           debugFRKey < mutation.param2) { // debugFRKey is in the range mutation
		TraceEvent("FastRestoreMutationTracking")
		    .detail("At", context)
		    .detail("Version", version)
		    .detail("MutationType", getTypeString((MutationRef::Type)mutation.type))
		    .detail("Begin", mutation.param1)
		    .detail("End", mutation.param2);
	} else
		return false;

	return true;
}
#else
// Default implementation.
bool debugFRMutation(const char* context, Version version, MutationRef const& mutation) {
	return false;
}
#endif

std::string getRoleStr(RestoreRole role) {
	if ((int)role >= numRoles || (int)role < 0) {
		printf("[ERROR] role:%d is out of scope\n", (int)role);
		return "[Unset]";
	}
	return RestoreRoleStr[(int)role];
}

bool isRangeMutation(MutationRef m) {
	if (m.type == MutationRef::Type::ClearRange) {
		ASSERT(m.type != MutationRef::Type::DebugKeyRange);
		return true;
	} else {
		ASSERT(m.type == MutationRef::Type::SetValue || isAtomicOp((MutationRef::Type)m.type));
		return false;
	}
}
