/*
 * ArgParseUtil.h
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

#pragma once
#ifndef FLOW_ARGPARSEUTIL_H
#define FLOW_ARGPARSEUTIL_H

#include "flow/Arena.h"

// Extracts the key for command line arguments that are specified with a prefix (e.g. --knob-).
// This function converts any hyphens in the extracted key to underscores.
Optional<std::string> extractPrefixedArgument(std::string prefix, std::string arg) {
	if (arg.size() <= prefix.size() || arg.find(prefix) != 0 ||
	    (arg[prefix.size()] != '-' && arg[prefix.size()] != '_')) {
		return Optional<std::string>();
	}

	arg = arg.substr(prefix.size() + 1);
	std::transform(arg.begin(), arg.end(), arg.begin(), [](int c) { return c == '-' ? '_' : c; });
	return arg;
}

#endif
