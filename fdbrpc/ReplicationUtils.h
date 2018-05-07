/*
 * ReplicationUtils.h
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

#ifndef FLOW_REPLICATIONTEST_H
#define FLOW_REPLICATIONTEST_H
#pragma once

#include "flow/flow.h"
#include "ReplicationTypes.h"


typedef std::string	repTestType;

extern repTestType	convertToTestType(int	iValue);


extern int testReplication();

extern double ratePolicy(
	LocalitySetRef &					localitySet,
	IRepPolicyRef	const&			policy,
	unsigned int							nSelectTests);

extern bool findBestPolicySet(
	std::vector<LocalityEntry>&	bestResults,
	LocalitySetRef &						localitySet,
	IRepPolicyRef	const&				policy,
	unsigned int								nMinItems,
	unsigned int								nSelectTests,
	unsigned int								nPolicyTests);

extern bool findBestUniquePolicySet(
	std::vector<LocalityEntry>&	bestResults,
	LocalitySetRef &						localitySet,
	IRepPolicyRef	const&				policy,
	StringRef										localityUniquenessKey,
	unsigned int								nMinItems,
	unsigned int								nSelectTests,
	unsigned int								nPolicyTests);

// The following function will return TRUE if all possible combinations
// of the new Item array will not pass the specified policy
extern bool validateAllCombinations(
	std::vector<LocalityData> &				offendingCombo,
	LocalityGroup const&							localitySet,
	IRepPolicyRef	const&							policy,
	std::vector<LocalityData> const&	newItems,
	unsigned int											nCombinationSize,
	bool															bCheckIfValid = true);

extern bool validateAllCombinations(
	LocalityGroup const&							localitySet,
	IRepPolicyRef	const&							policy,
	std::vector<LocalityData> const&	newItems,
	unsigned int											nCombinationSize,
	bool															bCheckIfValid = true);

/// Remove all pieces of locality information from the LocalityData that will not be used when validating the policy.
void filterLocalityDataForPolicy(IRepPolicyRef policy, LocalityData* ld);
void filterLocalityDataForPolicy(IRepPolicyRef policy, std::vector<LocalityData>* vld);

#endif
