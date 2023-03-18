/*
 * ResolverBug.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_RESOLVER_BUG_H
#define FDBSERVER_RESOLVER_BUG_H
#pragma once

#include "flow/SimBugInjector.h"
#include <vector>

struct ResolverBug : public ISimBug {
	double ignoreTooOldProbability = 0.0;
	double ignoreWriteSetProbability = 0.0;
	double ignoreReadSetProbability = 0.0;

	// data used to control lifetime of cycle clients
	bool bugFound = false;
	unsigned currentPhase = 0;
	std::vector<unsigned> cycleState;
};

class ResolverBugID : public IBugIdentifier {
public:
	std::shared_ptr<ISimBug> create() const override;
};

#endif // FDBSERVER_RESOLVER_BUG_H
