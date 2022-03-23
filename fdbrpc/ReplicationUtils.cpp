/*
 * ReplicationUtils.cpp
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

#include "fdbrpc/ReplicationUtils.h"
#include "flow/Hash3.h"
#include "flow/UnitTest.h"
#include "flow/Platform.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbrpc/Replication.h"

double ratePolicy(Reference<LocalitySet>& localitySet,
                  Reference<IReplicationPolicy> const& policy,
                  unsigned int nTestTotal) {
	double rating = -1.0;
	unsigned int uniqueResults = 0;
	int uniqueSet;
	std::map<std::set<LocalityEntry>, int> setMap;
	std::map<LocalityEntry, int> counterMap;
	std::vector<LocalityEntry> results;

	for (auto testIndex = 0u; testIndex < nTestTotal; testIndex++) {
		results.clear();
		if (!policy->selectReplicas(localitySet, results)) {
			printf("Failed to apply policy: %s to %d entries\n", policy->info().c_str(), localitySet->size());
			localitySet->DisplayEntries("rate");
			ASSERT(0);
			continue;
		}

		uniqueSet = setMap[std::set<LocalityEntry>(results.begin(), results.end())]++;

		if (!uniqueSet) {
			uniqueResults++;
			for (auto& result : results) {
				counterMap[result]++;
			}
		}
	}

	if (uniqueResults) {
		int largestMode = 0;
		LocalityEntry largestEntry;

		for (auto& counterItem : counterMap) {
			if (counterItem.second > largestMode) {
				largestMode = counterItem.second;
				largestEntry = counterItem.first;
			}
		}
		rating = (double)largestMode / (double)uniqueResults;
		if (g_replicationdebug > 4) {
			printf("Rate entries:\n");
			localitySet->DisplayEntries("rate");
		}
		if (g_replicationdebug > 3) {
			printf("  largest: (%5d) %7.5f  %7d of%7u  %s",
			       largestMode,
			       rating,
			       uniqueResults,
			       nTestTotal,
			       localitySet->getEntryInfo(largestEntry).c_str());
		}
	}

	return rating;
}

int mostUsedZoneCount(Reference<LocalitySet>& logServerSet, std::vector<LocalityEntry>& bestSet) {
	AttribKey indexKey = logServerSet->keyIndex("zoneid");
	std::map<AttribValue, int> entries;
	for (int i = 0; i < bestSet.size(); i++) {
		Optional<AttribValue> value = logServerSet->getRecordViaEntry(bestSet[i])->getValue(indexKey);
		entries[value.get()]++;
	}
	int maxEntries = 0;
	for (auto it : entries) {
		maxEntries = std::max(maxEntries, it.second);
	}
	return maxEntries;
}

bool findBestPolicySetSimple(int targetUniqueValueCount,
                             Reference<LocalitySet>& logServerSet,
                             std::vector<LocalityEntry>& bestSet,
                             int desired) {
	auto& mutableEntries = logServerSet->getMutableEntries();
	// First make sure the current localitySet is able to fulfuill the policy
	AttribKey indexKey = logServerSet->keyIndex("zoneid");
	int uniqueValueCount = logServerSet->getKeyValueArray()[indexKey._id].size();

	if (uniqueValueCount < targetUniqueValueCount) {
		// logServerSet won't be able to fulfill the policy
		return false;
	}

	std::map<AttribValue, std::vector<int>> entries;
	for (int i = 0; i < mutableEntries.size(); i++) {
		Optional<AttribValue> value = logServerSet->getRecord(mutableEntries[i]._id)->getValue(indexKey);
		if (value.present()) {
			entries[value.get()].push_back(i);
		}
	}

	ASSERT_WE_THINK(uniqueValueCount == entries.size());
	std::vector<std::vector<int>> randomizedEntries;
	randomizedEntries.resize(entries.size());
	for (auto it : entries) {
		randomizedEntries.push_back(it.second);
	}
	deterministicRandom()->randomShuffle(randomizedEntries);

	desired = std::max(desired, targetUniqueValueCount);
	auto it = randomizedEntries.begin();
	while (bestSet.size() < desired) {
		if (it->size()) {
			bestSet.push_back(mutableEntries[it->back()]);
			it->pop_back();
		}

		++it;
		if (it == randomizedEntries.end()) {
			it = randomizedEntries.begin();
		}
	}

	return true;
}

bool findBestPolicySetExpensive(std::vector<LocalityEntry>& bestResults,
                                Reference<LocalitySet>& localitySet,
                                Reference<IReplicationPolicy> const& policy,
                                unsigned int nMinItems,
                                unsigned int nSelectTests,
                                unsigned int nPolicyTests) {
	bool bSucceeded = true;
	Reference<LocalitySet> bestLocalitySet, testLocalitySet;
	std::vector<LocalityEntry> results;
	double testRate, bestRate = -1.0;

	if (g_replicationdebug > 3) {
		printf("Finding best from LocalitySet:\n");
		localitySet->DisplayEntries();
	}

	for (auto policyTest = 0u; policyTest < nPolicyTests; policyTest++) {
		results.clear();
		if (!policy->selectReplicas(localitySet, results)) {
			bSucceeded = false;
			break;
		}

		if (g_replicationdebug > 5) {
			printf("policy set #%5d:\n", policyTest);
			LocalitySet::staticDisplayEntries(localitySet, results, "result");
		}

		// Get some additional random items, if needed
		if ((nMinItems > results.size()) && (!localitySet->random(results, results, nMinItems - results.size()))) {
			bSucceeded = false;
			break;
		}

		if (g_replicationdebug > 4) {
			printf("policy with extras #%5d:\n", policyTest);
			LocalitySet::staticDisplayEntries(localitySet, results, "extra ");
		}

		// Create the test locality Set
		testLocalitySet = localitySet->restrict(results);

		// Get the test rate
		testRate = ratePolicy(testLocalitySet, policy, nSelectTests);

		if (g_replicationdebug > 3) {
			printf("   rate: %7.5f\n", testRate);
		}

		if (bestRate < 0.0) {
			bestResults = results;
			bestRate = testRate;
			bestLocalitySet = testLocalitySet;
		}
		// Allow the occasional bad comparison, if buggified
		else if (!BUGGIFY ? (testRate < bestRate) : (testRate > bestRate)) {
			bestResults = results;
			bestRate = testRate;
			bestLocalitySet = testLocalitySet;
		}
	}

	if (g_replicationdebug > 2) {
		printf("BestSet: %7.5f\n", bestRate);
		if (bestRate >= 0.0)
			bestLocalitySet->DisplayEntries();
	}

	return bSucceeded;
}

bool findBestPolicySet(std::vector<LocalityEntry>& bestResults,
                       Reference<LocalitySet>& localitySet,
                       Reference<IReplicationPolicy> const& policy,
                       unsigned int nMinItems,
                       unsigned int nSelectTests,
                       unsigned int nPolicyTests) {

	bool bestFound = false;

	// Specialization for policies of shape:
	//    - PolicyOne()
	//    - PolicyAcross(,"zoneId",PolicyOne())
	//    - TODO: More specializations for common policies
	if (policy->name() == "One") {
		bestFound = true;
		int count = 0;
		auto& mutableEntries = localitySet->getMutableEntries();
		deterministicRandom()->randomShuffle(mutableEntries);
		for (auto const& entry : mutableEntries) {
			bestResults.push_back(entry);
			if (++count == nMinItems)
				break;
		}
	} else if (policy->name() == "Across") {
		PolicyAcross* pa = (PolicyAcross*)policy.getPtr();
		std::set<std::string> attributeKeys;
		pa->attributeKeys(&attributeKeys);
		if (pa->embeddedPolicyName() == "One" && attributeKeys.size() == 1 &&
		    *attributeKeys.begin() == "zoneid" // This algorithm can actually apply to any field
		) {
			bestFound = findBestPolicySetSimple(pa->getCount(), localitySet, bestResults, nMinItems);
			if (bestFound && g_network->isSimulated()) {
				std::vector<LocalityEntry> oldBest;
				auto oldBestFound =
				    findBestPolicySetExpensive(oldBest, localitySet, policy, nMinItems, nSelectTests, nPolicyTests);
				if (!oldBestFound) {
					TraceEvent(SevError, "FBPSMissmatch").detail("Policy", policy->info());
				} else {
					ASSERT(mostUsedZoneCount(localitySet, bestResults) <= mostUsedZoneCount(localitySet, oldBest));
				}
			}
		} else {
			bestFound =
			    findBestPolicySetExpensive(bestResults, localitySet, policy, nMinItems, nSelectTests, nPolicyTests);
		}
	} else {
		bestFound = findBestPolicySetExpensive(bestResults, localitySet, policy, nMinItems, nSelectTests, nPolicyTests);
	}
	return bestFound;
}

bool findBestUniquePolicySet(std::vector<LocalityEntry>& bestResults,
                             Reference<LocalitySet>& localitySet,
                             Reference<IReplicationPolicy> const& policy,
                             StringRef localityUniquenessKey,
                             unsigned int nMinItems,
                             unsigned int nSelectTests,
                             unsigned int nPolicyTests) {
	bool bSucceeded = true;
	Reference<LocalitySet> bestLocalitySet, testLocalitySet;
	std::vector<LocalityEntry> results;
	double testRate, bestRate = -1.0;

	if (g_replicationdebug > 3) {
		printf("Finding best unique from LocalitySet:  %3d\n", localitySet->size());
		localitySet->DisplayEntries();
	}

	for (auto policyTest = 0u; policyTest < nPolicyTests; policyTest++) {
		results.clear();
		if (!policy->selectReplicas(localitySet, results)) {
			bSucceeded = false;
			break;
		}

		if (g_replicationdebug > 5) {
			printf("policy set #%5d:\n", policyTest);
			LocalitySet::staticDisplayEntries(localitySet, results, "result");
		}

		// Get some additional random unique items, if needed
		if (nMinItems > results.size()) {
			std::vector<LocalityEntry> exclusionList;
			auto keyIndex = localitySet->keyIndex(localityUniquenessKey);

			for (auto& result : results) {
				auto& entryValue = localitySet->getValueViaEntry(result, keyIndex);
				localitySet->getMatches(exclusionList, keyIndex, entryValue.get());
			}

			if (g_replicationdebug > 7) {
				printf("Excluded: %3lu\n", exclusionList.size());
				LocalitySet::staticDisplayEntries(localitySet, exclusionList, "exclude ");
			}

			while ((nMinItems > results.size()) && (localitySet->random(results, exclusionList, 1))) {
				auto& entryValue = localitySet->getValueViaEntry(results.back(), keyIndex);
				localitySet->getMatches(exclusionList, keyIndex, entryValue.get());
			}

			if (g_replicationdebug > 6) {
				printf("Final:    %3lu\n", results.size());
				LocalitySet::staticDisplayEntries(localitySet, results, "final   ");
			}
		}

		if (g_replicationdebug > 4) {
			printf("policy with extras #%5d:\n", policyTest);
			LocalitySet::staticDisplayEntries(localitySet, results, "extra ");
		}

		// Create the test locality Set
		testLocalitySet = localitySet->restrict(results);

		// Get the test rate
		testRate = ratePolicy(testLocalitySet, policy, nSelectTests);

		if (g_replicationdebug > 3) {
			printf("   rate: %7.5f\n", testRate);
		}

		if (bestRate < 0.0) {
			bestResults = results;
			bestRate = testRate;
			bestLocalitySet = testLocalitySet;
		}
		// Allow the occasional bad comparison, if buggified
		else if (!BUGGIFY ? (testRate < bestRate) : (testRate > bestRate)) {
			bestResults = results;
			bestRate = testRate;
			bestLocalitySet = testLocalitySet;
		}
	}

	if (g_replicationdebug > 2) {
		printf("BestSet: %7.5f\n", bestRate);
		bestLocalitySet->DisplayEntries();
	}

	return bSucceeded;
}

bool validateAllCombinations(std::vector<LocalityData>& offendingCombo,
                             LocalityGroup const& localitySet,
                             Reference<IReplicationPolicy> const& policy,
                             std::vector<LocalityData> const& newItems,
                             unsigned int nCombinationSize,
                             bool bCheckIfValid) {
	bool bValid = true;

	if (newItems.size() < nCombinationSize) {
		bValid = false;
	}
	// Ensure that the current set alone does not satisfy the
	// specified policy
	else if ((bCheckIfValid) && (!localitySet.validate(policy))) {
		bValid = false;
	} else if ((!bCheckIfValid) && (localitySet.validate(policy))) {
		bValid = false;
	} else {
		bool bIsValidGroup;
		Reference<LocalitySet> localSet = Reference<LocalitySet>(new LocalityGroup());
		LocalityGroup* localGroup = (LocalityGroup*)localSet.getPtr();
		localGroup->deep_copy(localitySet);

		std::vector<LocalityEntry> localityGroupEntries = localGroup->getEntries();
		int originalSize = localityGroupEntries.size();

		for (int i = 0; i < newItems.size(); ++i) {
			localGroup->add(newItems[i]);
		}

		std::string bitmask(nCombinationSize, 1); // K leading 1's
		bitmask.resize(newItems.size(), 0); // N-K trailing 0's

		std::vector<LocalityEntry> resultEntries;
		do {
			localityGroupEntries.resize(originalSize);
			// [0..N-1] integers
			for (int i = 0; i < bitmask.size(); ++i) {
				if (bitmask[i]) {
					localityGroupEntries.push_back(localGroup->getEntry(originalSize + i));
				}
			}

			resultEntries.clear();

			// Run the policy, assert if unable to satisfy
			bool result = localSet->selectReplicas(policy, localityGroupEntries, resultEntries);
			ASSERT(result);

			bIsValidGroup = resultEntries.size() == 0;

			if (((bCheckIfValid) && (!bIsValidGroup)) || ((!bCheckIfValid) && (bIsValidGroup))) {
				offendingCombo.reserve(nCombinationSize);
				for (int i = 0; i < newItems.size(); ++i) {
					if (bitmask[i]) {
						offendingCombo.push_back(newItems[i]);
					}
				}
				if (g_replicationdebug > 2) {
					printf("Invalid group\n");
					localGroup->DisplayEntries();
				}
				if (g_replicationdebug > 3) {
					printf("Full set\n");
					localitySet.DisplayEntries();
				}
				bValid = false;
				break;
			}
		}
		// permute bitmask
		while (std::prev_permutation(bitmask.begin(), bitmask.end()));
	}
	return bValid;
}

bool validateAllCombinations(LocalityGroup const& localitySet,
                             Reference<IReplicationPolicy> const& policy,
                             std::vector<LocalityData> const& newItems,
                             unsigned int nCombinationSize,
                             bool bCheckIfValid) {
	std::vector<LocalityData> invalidCombo;
	return validateAllCombinations(invalidCombo, localitySet, policy, newItems, nCombinationSize, bCheckIfValid);
}

repTestType convertToTestType(int iValue) {
	std::string sValue;
	char cValue;
	do {
		cValue = char(int('A') + (iValue % 26));
		sValue += std::string(1, cValue);
		iValue /= 26;
	} while (iValue > 0);
	return sValue;
}

Reference<LocalitySet> createTestLocalityMap(std::vector<repTestType>& indexes,
                                             int dcTotal,
                                             int szTotal,
                                             int rackTotal,
                                             int slotTotal,
                                             int independentItems,
                                             int independentTotal) {
	Reference<LocalitySet> buildServer(new LocalityMap<repTestType>());
	LocalityMap<repTestType>* serverMap = (LocalityMap<repTestType>*)buildServer.getPtr();
	int serverValue;
	std::string dcText, szText, rackText, slotText, independentName, independentText;

	// Determine the total size
	serverValue = dcTotal * ((szTotal * rackTotal) + (szTotal + 2) * (rackTotal + 2)) * slotTotal;

	if (g_replicationdebug > 0) {
		printf("DC:%2d  SZ:%2d  AZ:%2d  Rack:%2d  Slot:%2d  Extra:%2d  Xitems:%2d  Size:%4d\n",
		       dcTotal,
		       szTotal,
		       szTotal + 2,
		       rackTotal,
		       slotTotal,
		       independentItems,
		       independentTotal,
		       serverValue);
	}
	indexes.reserve(serverValue);

	for (int dcLoop = 0; dcLoop < dcTotal; dcLoop++) {
		serverValue = dcLoop;
		dcText = format("dc%d", dcLoop);
		for (int szLoop = 0; szLoop < szTotal; szLoop++) {
			serverValue = dcLoop + szLoop * 10;
			szText = format(".s%d", szLoop);
			for (int rackLoop = 0; rackLoop < rackTotal; rackLoop++) {
				serverValue = dcLoop + szLoop * 10 + rackLoop * 100;
				rackText = format(".%d", rackLoop);
				for (int slotLoop = 0; slotLoop < slotTotal; slotLoop++) {
					serverValue = dcLoop + szLoop * 10 + rackLoop * 100 + slotLoop * 1000;
					slotText = format(".%d", slotLoop);
					LocalityData data;
					data.set(LiteralStringRef("dc"), StringRef(dcText));
					data.set(LiteralStringRef("sz"), StringRef(dcText + szText));
					data.set(LiteralStringRef("rack"), StringRef(dcText + szText + rackText));
					data.set(LiteralStringRef("zoneid"), StringRef(dcText + szText + rackText + slotText));
					for (int independentLoop = 0; independentLoop < independentItems; independentLoop++) {
						independentName = format("indiv%02d", independentLoop + 1);
						for (int totalLoop = 0; totalLoop < independentTotal; totalLoop++) {
							independentText = format("i%02d", totalLoop + 1);
							data.set(StringRef(independentName), StringRef(independentText));
						}
					}
					indexes.push_back(convertToTestType(indexes.size()));
					serverMap->add(data, &indexes.back());
				}
			}
		}

		for (int szLoop = 0; szLoop < szTotal + 2; szLoop++) {
			serverValue = (dcLoop + 2) + szLoop * 10;
			szText = format(".a%d", szLoop);
			for (int rackLoop = 0; rackLoop < rackTotal + 2; rackLoop++) {
				serverValue = (dcLoop + 2) + szLoop * 10 + rackLoop * 100;
				rackText = format(".%d", rackLoop);
				for (int slotLoop = 0; slotLoop < slotTotal; slotLoop++) {
					serverValue = (dcLoop + 2) + szLoop * 10 + rackLoop * 100 + slotLoop * 1000;
					slotText = format(".%d", slotLoop);
					LocalityData data;
					data.set(LiteralStringRef("dc"), StringRef(dcText));
					data.set(LiteralStringRef("az"), StringRef(dcText + szText));
					data.set(LiteralStringRef("rack"), StringRef(dcText + szText + rackText));
					data.set(LiteralStringRef("zoneid"), StringRef(dcText + szText + rackText + slotText));
					for (int independentLoop = 0; independentLoop < independentItems; independentLoop++) {
						independentName = format("indiv%02d", independentLoop);
						for (int totalLoop = 0; totalLoop < independentTotal; totalLoop++) {
							independentText = format("i%02d", totalLoop);
							data.set(StringRef(independentName), StringRef(independentText));
						}
					}
					indexes.push_back(convertToTestType(indexes.size()));
					serverMap->add(data, &indexes.back());
				}
			}
		}
	}

	if (g_replicationdebug > 1)
		printf("Created: %3d servers\n", buildServer->size());

	if (g_replicationdebug > 1) {
		buildServer->DisplayEntries();
	}

	return buildServer;
}

bool testPolicy(Reference<LocalitySet> servers,
                Reference<IReplicationPolicy> const& policy,
                std::vector<LocalityEntry> const& including,
                bool validate) {
	LocalityMap<repTestType>* serverMap = (LocalityMap<repTestType>*)servers.getPtr();
	std::string outputText, includeText;
	std::vector<LocalityEntry> entryResults;
	std::vector<repTestType*> results;
	bool valid, solved;

	if (g_replicationdebug > 1) {
		printf("Policy test:   include:%4lu  policy: %-10s => %s\n",
		       including.size(),
		       policy->name().c_str(),
		       policy->info().c_str());
	}
	if (g_replicationdebug > 2) {
		for (auto& entry : including) {
			printf("   also:  %s\n", servers->getEntryInfo(entry).c_str());
		}
	}

	solved = serverMap->selectReplicas(policy, including, entryResults, results);

	if (g_replicationdebug > 1) {
		printf("%-10s solution:%3lu  policy: %-10s => %s    include:%4lu\n",
		       ((solved) ? "Solved" : "Unsolved"),
		       results.size(),
		       policy->name().c_str(),
		       policy->info().c_str(),
		       including.size());
	}
	if (g_replicationdebug > 2) {
		for (auto& entry : entryResults) {
			printf("   item:  %s\n", servers->getEntryInfo(entry).c_str());
		}
		for (auto& entry : including) {
			printf("   also:  %s\n", servers->getEntryInfo(entry).c_str());
		}
	}

	valid = (validate) ? policy->validateFull(solved, entryResults, including, servers) : true;

	if (g_replicationdebug > 0) {
		if (including.size()) {
			includeText = " with ";
			for (auto& entry : including) {
				includeText += " " + servers->getEntryInfo(entry);
			}
		}

		if (results.size()) {
			outputText = policy->info() + includeText + " -> ";
			int count = 0;
			for (auto& entry : entryResults) {
				outputText += " " + *results[count] + "-" + servers->getEntryInfo(entry);
				count++;
			}
		} else {
			outputText = policy->info() + includeText + ((solved) ? " -> None" : " -> No solution");
		}

		printf("%-5s:%3d  %s\n", (valid) ? "Valid" : "Error", 0, outputText.c_str());
	}

	return valid;
}

bool testPolicy(Reference<LocalitySet> servers, Reference<IReplicationPolicy> const& policy, bool validate) {
	return testPolicy(servers, policy, emptyEntryArray, validate);
}

std::vector<Reference<IReplicationPolicy>> const& getStaticPolicies() {
	static std::vector<Reference<IReplicationPolicy>> staticPolicies;

	if (staticPolicies.empty()) {
		staticPolicies = {

			Reference<IReplicationPolicy>(new PolicyOne()),

			// 1 'dc^2 x 1'
			Reference<IReplicationPolicy>(new PolicyAcross(2, "dc", Reference<IReplicationPolicy>(new PolicyOne()))),

			// 2 'dc^3 x 1'
			Reference<IReplicationPolicy>(new PolicyAcross(3, "dc", Reference<IReplicationPolicy>(new PolicyOne()))),

			// 3 'sz^3 x 1'
			Reference<IReplicationPolicy>(new PolicyAcross(3, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),

			// 4 'dc^1 x az^3 x 1'
			Reference<IReplicationPolicy>(
			    new PolicyAcross(1,
			                     "dc",
			                     Reference<IReplicationPolicy>(
			                         new PolicyAcross(3, "az", Reference<IReplicationPolicy>(new PolicyOne()))))),

			// 5 '(sz^3 x rack^2 x 1) + (dc^2 x az^3 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(3,
			                                         "sz",
			                                         Reference<IReplicationPolicy>(new PolicyAcross(
			                                             2, "rack", Reference<IReplicationPolicy>(new PolicyOne()))))),
			                    Reference<IReplicationPolicy>(new PolicyAcross(
			                        2,
			                        "dc",
			                        Reference<IReplicationPolicy>(new PolicyAcross(
			                            3, "az", Reference<IReplicationPolicy>(new PolicyOne()))))) })),

			// 6 '(sz^1 x 1)'
			Reference<IReplicationPolicy>(new PolicyAcross(1, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),

			// 7 '(sz^1 x 1) + (sz^1 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(1, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(
			                        new PolicyAcross(1, "sz", Reference<IReplicationPolicy>(new PolicyOne()))) })),

			// 8 '(sz^2 x 1) + (sz^2 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(
			                        new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))) })),

			// 9 '(dc^1 x sz^2 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAcross(1,
			                     "dc",
			                     Reference<IReplicationPolicy>(
			                         new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))),

			// 10 '(dc^2 x sz^2 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAcross(2,
			                     "dc",
			                     Reference<IReplicationPolicy>(
			                         new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))),

			// 11 '(dc^1 x sz^2 x 1) + (dc^2 x sz^2 x 1)'
			Reference<IReplicationPolicy>(new PolicyAnd(
			    { Reference<IReplicationPolicy>(
			          new PolicyAcross(1,
			                           "dc",
			                           Reference<IReplicationPolicy>(
			                               new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))),
			      Reference<IReplicationPolicy>(
			          new PolicyAcross(2,
			                           "dc",
			                           Reference<IReplicationPolicy>(new PolicyAcross(
			                               2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))) })),

			// 12 '(dc^2 x sz^2 x 1) + (dc^1 x sz^2 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(2,
			                                         "dc",
			                                         Reference<IReplicationPolicy>(new PolicyAcross(
			                                             2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))),
			                    Reference<IReplicationPolicy>(new PolicyAcross(
			                        1,
			                        "dc",
			                        Reference<IReplicationPolicy>(new PolicyAcross(
			                            2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))) })),

			// 13 '(sz^2 x 1) + (dc^1 x sz^2 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(new PolicyAcross(
			                        1,
			                        "dc",
			                        Reference<IReplicationPolicy>(new PolicyAcross(
			                            2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))) })),

			// 14 '(sz^2 x 1) + (dc^2 x sz^2 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(new PolicyAcross(
			                        2,
			                        "dc",
			                        Reference<IReplicationPolicy>(new PolicyAcross(
			                            2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))) })),

			// 15 '(sz^3 x 1) + (dc^2 x sz^2 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(3, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(new PolicyAcross(
			                        2,
			                        "dc",
			                        Reference<IReplicationPolicy>(new PolicyAcross(
			                            2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))) })),

			// 16 '(sz^1 x 1) + (sz^2 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(1, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(
			                        new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))) })),

			// 17 '(sz^2 x 1) + (sz^3 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(
			                        new PolicyAcross(3, "sz", Reference<IReplicationPolicy>(new PolicyOne()))) })),

			// 18 '(sz^1 x 1) + (sz^2 x 1) + (sz^3 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(1, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(
			                        new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(
			                        new PolicyAcross(3, "sz", Reference<IReplicationPolicy>(new PolicyOne()))) })),

			// 19 '(sz^1 x 1) + (machine^1 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(1, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(
			                        new PolicyAcross(1, "zoneid", Reference<IReplicationPolicy>(new PolicyOne()))) })),

			// '(dc^1 x 1) + (sz^1 x 1) + (machine^1 x 1)'
			//	Reference<IReplicationPolicy>( new PolicyAnd( { Reference<IReplicationPolicy>(new PolicyAcross(1, "dc",
			// Reference<IReplicationPolicy>(new PolicyOne()))), Reference<IReplicationPolicy>(new PolicyAcross(1, "sz",
			// Reference<IReplicationPolicy>(new PolicyOne()))), Reference<IReplicationPolicy>(new PolicyAcross(1,
			//"zoneid", Reference<IReplicationPolicy>(new PolicyOne()))) } ) ),

			// '(dc^1 x sz^3 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAcross(1,
			                     "dc",
			                     Reference<IReplicationPolicy>(
			                         new PolicyAcross(3, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))),

			// '(dc^2 x sz^3 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAcross(2,
			                     "dc",
			                     Reference<IReplicationPolicy>(
			                         new PolicyAcross(3, "sz", Reference<IReplicationPolicy>(new PolicyOne()))))),

			// '(dc^2 x az^3 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAcross(2,
			                     "dc",
			                     Reference<IReplicationPolicy>(
			                         new PolicyAcross(3, "az", Reference<IReplicationPolicy>(new PolicyOne()))))),

			// '(sz^1 x 1) + (dc^2 x az^3 x 1)'
			Reference<IReplicationPolicy>(
			    new PolicyAnd({ Reference<IReplicationPolicy>(
			                        new PolicyAcross(1, "sz", Reference<IReplicationPolicy>(new PolicyOne()))),
			                    Reference<IReplicationPolicy>(new PolicyAcross(
			                        2,
			                        "dc",
			                        Reference<IReplicationPolicy>(new PolicyAcross(
			                            3, "az", Reference<IReplicationPolicy>(new PolicyOne()))))) })),

			// 'dc^1 x (az^2 x 1) + (sz^2 x 1)'
			//	Reference<IReplicationPolicy>( new PolicyAcross(1, "dc", Reference<IReplicationPolicy>(new
			// PolicyAnd({Reference<IReplicationPolicy>(new PolicyAcross(2, "az", Reference<IReplicationPolicy>(new
			// PolicyOne()))), Reference<IReplicationPolicy>(new PolicyAcross(2, "sz", Reference<IReplicationPolicy>(new
			// PolicyOne())))}))) ),

			// Require backtracking
			Reference<IReplicationPolicy>(new PolicyAcross(
			    8,
			    "zoneid",
			    Reference<IReplicationPolicy>(
			        new PolicyAcross(1, "az", Reference<IReplicationPolicy>(new PolicyOne()))))),
			Reference<IReplicationPolicy>(new PolicyAcross(
			    8,
			    "zoneid",
			    Reference<IReplicationPolicy>(
			        new PolicyAcross(1, "sz", Reference<IReplicationPolicy>(new PolicyOne())))))
		};
	}
	return staticPolicies;
}

Reference<IReplicationPolicy> const randomAcrossPolicy(LocalitySet const& serverSet) {
	int usedKeyTotal, keysUsed, keyIndex, valueTotal, maxValueTotal, maxKeyTotal, skips, lastKeyIndex;
	std::vector<std::string> keyArray(serverSet.getGroupKeyMap()->_lookuparray);
	std::set<std::string> valueSet;
	AttribKey indexKey;
	Optional<AttribValue> keyValue;
	std::string keyText;
	Reference<IReplicationPolicy> policy(new PolicyOne());

	// Determine the number of keys to used within the policy
	usedKeyTotal = deterministicRandom()->randomInt(1, keyArray.size() + 1);
	maxKeyTotal = deterministicRandom()->randomInt(1, 4);
	if ((usedKeyTotal > maxKeyTotal) && (deterministicRandom()->random01() > .1)) {
		usedKeyTotal = maxKeyTotal;
	}
	maxValueTotal = deterministicRandom()->randomInt(1, 10);
	keysUsed = skips = 0;

	if (g_replicationdebug > 6) {
		keyIndex = 0;
		for (auto& key : keyArray) {
			keyIndex++;
			printf("%s  key: (%2d) %-10s\n", ((keyIndex > 1) ? "" : "\n"), keyIndex, key.c_str());
		}
	}

	if (g_replicationdebug > 2)
		printf("Policy using%3d of%3lu keys  Max values:%3d\n", usedKeyTotal, keyArray.size(), maxValueTotal);
	while (keysUsed < usedKeyTotal) {
		keyIndex = deterministicRandom()->randomInt(0, keyArray.size() - keysUsed);
		keyText = keyArray[keyIndex];
		lastKeyIndex = keyArray.size() - 1 - keysUsed;

		// Do not allow az and sz within a policy, 90% of the time
		if ((!keyText.compare("az")) && (deterministicRandom()->random01() > .1) &&
		    (std::find(keyArray.begin() + lastKeyIndex + 1, keyArray.end(), "sz") != keyArray.end())) {
			skips++;
		} else if ((!keyText.compare("sz")) && (deterministicRandom()->random01() > .1) &&
		           (std::find(keyArray.begin() + lastKeyIndex + 1, keyArray.end(), "az") != keyArray.end())) {
			skips++;
		} else {
			if (g_replicationdebug > 3) {
				printf("  keys  index:%3d  value: %-10s used:%3d  total:%3d  size:%3lu\n",
				       keyIndex,
				       keyText.c_str(),
				       keysUsed,
				       usedKeyTotal,
				       keyArray.size());
			}
			indexKey = serverSet.keyIndex(keyText);
			valueSet.clear();
			// Determine all of the values for the key
			for (auto& entry : serverSet.getEntries()) {
				keyValue = serverSet.getValueViaEntry(entry, indexKey);
				if (keyValue.present()) {
					valueSet.insert(serverSet.valueText(keyValue.get()));
				}
			}
			valueTotal = deterministicRandom()->randomInt(1, valueSet.size() + 2);
			if ((valueTotal > maxValueTotal) && (deterministicRandom()->random01() > .25))
				valueTotal = maxValueTotal;
			policy = Reference<IReplicationPolicy>(new PolicyAcross(valueTotal, keyText, policy));
			if (g_replicationdebug > 1) {
				printf("  item%3d: (%3d =>%3d) %-10s  =>%4d\n",
				       keysUsed + 1,
				       keyIndex,
				       indexKey._id,
				       keyText.c_str(),
				       valueTotal);
			}
		}
		keysUsed++;
		// Move the used string to the end of the array
		if (keyIndex < lastKeyIndex) {
			if (g_replicationdebug > 2) {
				printf("  Copying%3d into %3d\n", lastKeyIndex, keyIndex);
			}
			keyArray[keyIndex] = keyArray[lastKeyIndex];
		} else if (g_replicationdebug > 2) {
			printf("  Skip   %3d into %3d\n", lastKeyIndex, keyIndex);
		}

		if (g_replicationdebug > 6) {
			keyIndex = 0;
			for (auto& key : keyArray) {
				keyIndex++;
				printf("%s  key: (%2d) %-10s\n", ((keyIndex > 1) ? "" : "\n"), keyIndex, key.c_str());
			}
		}
	}
	if (g_replicationdebug > 0)
		printf("Policy: %s\n", policy->info().c_str());
	return policy;
}

int testReplication() {
	const char* testTotalEnv = getenv("REPLICATION_TESTTOTAL");
	const char* debugLevelEnv = getenv("REPLICATION_DEBUGLEVEL");
	const char* policyTotalEnv = getenv("REPLICATION_POLICYTOTAL");
	const char* policyIndexEnv = getenv("REPLICATION_POLICYINDEX");
	const char* reportCacheEnv = getenv("REPLICATION_REPORTCACHE");
	const char* stopOnErrorEnv = getenv("REPLICATION_STOPONERROR");
	const char* skipTotalEnv = getenv("REPLICATION_SKIPTOTAL");
	const char* validateEnv = getenv("REPLICATION_VALIDATE");
	const char* findBestEnv = getenv("REPLICATION_FINDBEST");
	const char* rateSampleEnv = getenv("REPLICATION_RATESAMPLE");
	const char* policySampleEnv = getenv("REPLICATION_POLICYSAMPLE");
	const char* policyMinEnv = getenv("REPLICATION_POLICYEXTRA");
	int totalTests = testTotalEnv ? atoi(testTotalEnv) : 10000;
	int skipTotal = skipTotalEnv ? atoi(skipTotalEnv) : 0;
	int findBest = findBestEnv ? atoi(findBestEnv) : 0;
	int policyIndexStatic = policyIndexEnv ? atoi(policyIndexEnv) : -1;
	int policyTotal = policyTotalEnv ? atoi(policyTotalEnv) : 100;
	bool stopOnError = stopOnErrorEnv ? (atoi(stopOnErrorEnv) > 0) : false;
	bool validate = validateEnv ? (atoi(validateEnv) > 0) : true;
	int rateSample = rateSampleEnv ? atoi(rateSampleEnv) : 1000;
	int policySample = policySampleEnv ? atoi(policySampleEnv) : 100;
	int policyMin = policyMinEnv ? atoi(policyMinEnv) : 2;
	int policyIndex, testCounter, alsoSize, debugBackup, maxAlsoSize;
	std::vector<repTestType> serverIndexes;
	Reference<LocalitySet> testServers;
	std::vector<Reference<IReplicationPolicy>> policies;
	std::vector<LocalityEntry> alsoServers, bestSet;
	int totalErrors = 0;

	if (debugLevelEnv)
		g_replicationdebug = atoi(debugLevelEnv);
	debugBackup = g_replicationdebug;

	testServers = createTestLocalityMap(serverIndexes,
	                                    deterministicRandom()->randomInt(1, 5),
	                                    deterministicRandom()->randomInt(1, 6),
	                                    deterministicRandom()->randomInt(1, 10),
	                                    deterministicRandom()->randomInt(1, 10),
	                                    deterministicRandom()->randomInt(0, 4),
	                                    deterministicRandom()->randomInt(1, 5));
	maxAlsoSize = testServers->size() / deterministicRandom()->randomInt(2, 20);

	if (g_replicationdebug >= 0)
		printf("Running %d Replication test\n", totalTests);

	if ((!policyIndexEnv) || (policyIndexStatic >= 0)) {
		policies = getStaticPolicies();
	} else {
		if (g_replicationdebug > 0)
			printf("Creating %3d random policies.\n", policyTotal);
		policies.reserve(policyTotal);
		for (auto i = 0; i < policyTotal; i++) {
			if (g_replicationdebug > 0)
				printf(" (%3d) ", i + 1);
			policies.push_back(randomAcrossPolicy(*testServers));
		}
	}

	for (testCounter = 0; testCounter < totalTests; testCounter++) {
		if (!skipTotal) {
		} else if (testCounter < skipTotal) {
			g_replicationdebug = 1;
		} else {
			g_replicationdebug = debugBackup;
			skipTotal = 0;
		}
		alsoSize = deterministicRandom()->randomInt(0, testServers->size() + 1);
		if ((alsoSize > maxAlsoSize) && (deterministicRandom()->random01() > .2)) {
			alsoSize = maxAlsoSize;
		}

		if ((!alsoSize) && (alsoServers.size() > 0)) {
			alsoServers.clear();
		} else {
			alsoServers = testServers->getEntries();
			deterministicRandom()->randomShuffle(alsoServers);
			if (alsoSize < testServers->size()) {
				alsoServers.resize(alsoSize);
			}
		}

		policyIndex =
		    (policyIndexStatic >= 0) ? policyIndexStatic : deterministicRandom()->randomInt(0, policies.size());

		if (g_replicationdebug > 0)
			printf(" #%7d: (%3d) ", testCounter, policyIndex);

		if (findBest) {
			findBestPolicySet(bestSet, testServers, policies[policyIndex], policyMin, rateSample, policySample);

			if (g_replicationdebug > 1) {
				printf("BestSet:%4lu entries\n", bestSet.size());
				LocalitySet::staticDisplayEntries(testServers, bestSet, "best");
			}

			if (g_replicationdebug > 0)
				printf("%7lu   %s\n", bestSet.size(), policies[policyIndex]->info().c_str());
		}

		else if (!testPolicy(testServers, policies[policyIndex], alsoServers, validate)) {
			totalErrors++;
			if (stopOnError)
				break;
		}
	}
	if (g_replicationdebug >= 0)
		printf("Succeeded in completing %d of %d policies\n", testCounter - totalErrors, totalTests);
	if ((g_replicationdebug > 0) || ((reportCacheEnv) && (atoi(reportCacheEnv) > 0))) {
		testServers->cacheReport();
	}

	return totalErrors;
}

namespace {
void filterLocalityDataForPolicy(const std::set<std::string>& keys, LocalityData* ld) {
	for (auto iter = ld->_data.begin(); iter != ld->_data.end();) {
		auto prev = iter;
		iter++;
		if (keys.find(prev->first.toString()) == keys.end()) {
			ld->_data.erase(prev);
		}
	}
}
} // namespace

void filterLocalityDataForPolicyDcAndProcess(Reference<IReplicationPolicy> policy, LocalityData* ld) {
	if (!policy)
		return;
	std::set<std::string> keys = policy->attributeKeys();
	keys.insert(LocalityData::keyDcId.toString());
	keys.insert(LocalityData::keyProcessId.toString());
	filterLocalityDataForPolicy(policy->attributeKeys(), ld);
}

void filterLocalityDataForPolicy(Reference<IReplicationPolicy> policy, LocalityData* ld) {
	if (!policy)
		return;
	filterLocalityDataForPolicy(policy->attributeKeys(), ld);
}

void filterLocalityDataForPolicy(Reference<IReplicationPolicy> policy, std::vector<LocalityData>* vld) {
	if (!policy)
		return;
	for (LocalityData& ld : *vld) {
		filterLocalityDataForPolicy(policy, &ld);
	}
}

TEST_CASE("/fdbrpc/Replication/test") {
	printf("Running replication test\n");

	platform::setEnvironmentVar("REPLICATION_STOPONERROR", "1", 0);
	platform::setEnvironmentVar("REPLICATION_VALIDATE", "1", 0);

	ASSERT(testReplication() == 0);
	return Void();
}
