/*
 * ReplicationUtils.h
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

#ifndef FLOW_REPLICATIONTEST_H
#define FLOW_REPLICATIONTEST_H
#pragma once

#include "flow/flow.h"
#include "fdbrpc/ReplicationTypes.h"

typedef std::string repTestType;
// string value defining test type

extern repTestType convertToTestType(int iValue);
// converts integer value to a test type

extern int testReplication();

extern double ratePolicy(Reference<LocalitySet>& localitySet,
                         Reference<IReplicationPolicy> const& policy,
                         unsigned int nSelectTests);
// returns the value for the rate policy
// given a localitySet, replication policy and number of selected tests, apply the
// policy and return the rating
// rating can be -1 there are no unique results failing while applying the replication
// policy, otherwise largest mode from the items per unique set of locaility entry
// are returned.

extern bool findBestPolicySet(std::vector<LocalityEntry>& bestResults,
                              Reference<LocalitySet>& localitySet,
                              Reference<IReplicationPolicy> const& policy,
                              unsigned int nMinItems,
                              unsigned int nSelectTests,
                              unsigned int nPolicyTests);
// returns the best policy set
// given locality set, replication policy, number of min items, number of select
// test, number of policy tests, find the best from locality set, including few
// random items, get the rate policy having test rate, best rate and returning
// the success state.

extern bool findBestUniquePolicySet(std::vector<LocalityEntry>& bestResults,
                                    Reference<LocalitySet>& localitySet,
                                    Reference<IReplicationPolicy> const& policy,
                                    StringRef localityUniquenessKey,
                                    unsigned int nMinItems,
                                    unsigned int nSelectTests,
                                    unsigned int nPolicyTests);

// The following function will return TRUE if all possible combinations
// of the new Item array will not pass the specified policy
extern bool validateAllCombinations(std::vector<LocalityData>& offendingCombo,
                                    LocalityGroup const& localitySet,
                                    Reference<IReplicationPolicy> const& policy,
                                    std::vector<LocalityData> const& newItems,
                                    unsigned int nCombinationSize,
                                    bool bCheckIfValid = true);

extern bool validateAllCombinations(LocalityGroup const& localitySet,
                                    Reference<IReplicationPolicy> const& policy,
                                    std::vector<LocalityData> const& newItems,
                                    unsigned int nCombinationSize,
                                    bool bCheckIfValid = true);

/// Remove all pieces of locality information from the LocalityData that will not be used when validating the policy.
void filterLocalityDataForPolicyDcAndProcess(Reference<IReplicationPolicy> policy, LocalityData* ld);
void filterLocalityDataForPolicy(Reference<IReplicationPolicy> policy, LocalityData* ld);
void filterLocalityDataForPolicy(Reference<IReplicationPolicy> policy, std::vector<LocalityData>* vld);

#endif
