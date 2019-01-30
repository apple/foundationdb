/*
 * ReplicationPolicy.cpp
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

#include "fdbrpc/ReplicationPolicy.h"
#include "fdbrpc/Replication.h"
#include "flow/UnitTest.h"


bool IReplicationPolicy::selectReplicas(
	LocalitySetRef &										fromServers,
	std::vector<LocalityEntry>	&				results )
{
	return selectReplicas(fromServers, std::vector<LocalityEntry>(), results);
}

bool IReplicationPolicy::validate(
	LocalitySetRef const&								solutionSet ) const
{
	return validate(solutionSet->getEntries(), solutionSet);
}

bool IReplicationPolicy::validateFull(
	bool																solved,
	std::vector<LocalityEntry>	const&	solutionSet,
	std::vector<LocalityEntry> const&		alsoServers,
	LocalitySetRef const&				fromServers )
{
	bool	valid = true;
	std::vector<LocalityEntry>	totalSolution(solutionSet);

	// Append the also servers, if any
	if (alsoServers.size()) {
		totalSolution.reserve(totalSolution.size() + alsoServers.size());
		totalSolution.insert(totalSolution.end(), alsoServers.begin(), alsoServers.end());
	}

	if (!solved) {
		if (validate(totalSolution, fromServers)) {
			if (g_replicationdebug > 2) {
				printf("Error: Validate unsolved policy with%3lu also servers and%3lu solution servers\n", alsoServers.size(), solutionSet.size());
			}
			valid = false;
		}
		else if (validate(fromServers->getGroupEntries(), fromServers)) {
			if (g_replicationdebug > 2) {
				printf("Error: Validated unsolved policy with all%5d servers\n", fromServers->size());
			}
			valid = false;
		}
	}
	else if (!validate(totalSolution, fromServers)) {
		if (g_replicationdebug > 2) {
			printf("Error: Failed to validate solved policy with%3lu also servers and%3lu solution servers\n", alsoServers.size(), solutionSet.size());
		}
		valid = false;
	}
	else if (solutionSet.empty()) {
		if (!validate(alsoServers, fromServers)) {
			if (g_replicationdebug > 2) {
				printf("Error: Failed to validate policy with only%3lu also servers\n", alsoServers.size());
			}
			valid = false;
		}
	}
	else {
		auto	lastSolutionIndex = solutionSet.size()-1;
		auto	missingEntry = totalSolution[lastSolutionIndex];
		totalSolution[lastSolutionIndex] = totalSolution.back();
		totalSolution.pop_back();
		for (int index = 0; index < solutionSet.size() && index < totalSolution.size(); index ++) {
			if (g_replicationdebug > 3) {
				auto fromServer = fromServers->getRecordViaEntry(missingEntry);
				printf("Test remove entry:   %s   test:%3d of%3lu\n", fromServers->getEntryInfo(missingEntry).c_str(), index+1, solutionSet.size());
			}
			if (validate(totalSolution, fromServers)) {
				if (g_replicationdebug > 2) {
					printf("Invalid extra entry: %s\n", fromServers->getEntryInfo(missingEntry).c_str());
				}
				valid = false;
				break;
			}
			auto tempMissing = totalSolution[index];
			totalSolution[index] = missingEntry;
			missingEntry = tempMissing;
		}
	}
	return valid;
}

bool PolicyOne::selectReplicas(
	LocalitySetRef	&						fromServers,
	std::vector<LocalityEntry> const&		alsoServers,
	std::vector<LocalityEntry>	&				results )
{
	int		totalUsed = 0;
	int		itemsUsed = 0;
	if (alsoServers.size()) {
		totalUsed ++;
	}
	else if (fromServers->size()) {
		auto randomEntry = fromServers->random();
		results.push_back(randomEntry);
		itemsUsed ++;
		totalUsed ++;
		if (g_replicationdebug > 5) {
			printf("One    added:%4d %33s entry: %s\n", itemsUsed, "", fromServers->getEntryInfo(randomEntry).c_str());
		}
	}
	if (g_replicationdebug > 2) {
		printf("One    used:%5d results:%3d from %3d servers\n", totalUsed, itemsUsed, fromServers->size());
	}
	return (totalUsed > 0);
}

bool PolicyOne::validate(
	std::vector<LocalityEntry>	const&	solutionSet,
	LocalitySetRef const&				fromServers ) const
{
	return ((solutionSet.size() > 0) && (fromServers->size() > 0));
}

PolicyAcross::PolicyAcross(int count, std::string const& attribKey, IRepPolicyRef const policy):
	_count(count),_attribKey(attribKey),_policy(policy)
{
	return;
}

PolicyAcross::~PolicyAcross()
{
	return;
}

// Debug purpose only
// Trace all record entries to help debug
// fromServers is the servers locality to be printed out.
void IReplicationPolicy::traceLocalityRecords(LocalitySetRef const& fromServers) {
	std::vector<Reference<LocalityRecord>> const& recordArray = fromServers->getRecordArray();
	TraceEvent("LocalityRecordArray").detail("Size", recordArray.size());
	for (auto& record : recordArray) {
		traceOneLocalityRecord(record, fromServers);
	}
}

void IReplicationPolicy::traceOneLocalityRecord(Reference<LocalityRecord> record, LocalitySetRef const& fromServers) {
	int localityEntryIndex = record->_entryIndex._id;
	Reference<KeyValueMap> const& dataMap = record->_dataMap;
	std::vector<AttribRecord> const& keyValueArray = dataMap->_keyvaluearray;

	TraceEvent("LocalityRecordInfo")
	    .detail("EntryIndex", localityEntryIndex)
	    .detail("KeyValueArraySize", keyValueArray.size());
	for (int i = 0; i < keyValueArray.size(); ++i) {
		AttribRecord attribRecord = keyValueArray[i]; // first is key, second is value
		TraceEvent("LocalityRecordInfo")
		    .detail("EntryIndex", localityEntryIndex)
		    .detail("ArrayIndex", i)
		    .detail("Key", attribRecord.first._id)
		    .detail("Value", attribRecord.second._id)
		    .detail("KeyName", fromServers->keyText(attribRecord.first))
		    .detail("ValueName", fromServers->valueText(attribRecord.second));
	}
}

// Validate if the team satisfies the replication policy
// LocalitySet is the base class about the locality information
// solutionSet is the team to be validated
// fromServers is the location information of all servers
// return true if the team satisfies the policy; false otherwise
bool PolicyAcross::validate(
		std::vector<LocalityEntry>	const&	solutionSet,
		LocalitySetRef const&				fromServers ) const
{
	bool			valid = true;
	int				count = 0;
	// Get the indexKey from the policy name (e.g., zoneid) in _attribKey
	AttribKey indexKey = fromServers->keyIndex(_attribKey);
	auto			groupIndexKey = fromServers->getGroupKeyIndex(indexKey);
	std::map<AttribValue, std::vector<LocalityEntry>>	validMap;

	for (auto& item : solutionSet) {
		auto value = fromServers->getValueViaGroupKey(item, groupIndexKey);
		if (value.present()) {
			auto itValue = validMap.find(value.get());
			if (itValue != validMap.end()) {
				validMap[value.get()].push_back(item);
			}
			else {
				validMap[value.get()] = {item};
			}
		}
	}
	if (validMap.size() < _count) {
		if (g_replicationdebug > 3) {
			printf("Across too few values:%3lu <%2d key: %-7s policy: %-10s => %s\n", validMap.size(), _count, _attribKey.c_str(), _policy->name().c_str(), _policy->info().c_str());
		}
		valid = false;
	}
	else {
		if (g_replicationdebug > 3) {
			printf("Across check values:%9lu key: %-7s solutions:%2lu count:%2d policy: %-10s => %s\n", validMap.size(), _attribKey.c_str(), solutionSet.size(), _count, _policy->name().c_str(), _policy->info().c_str());
			for (auto& itValue : validMap) {
				printf("   value: (%3d) %-10s\n", itValue.first._id, fromServers->valueText(itValue.first).c_str());
			}
		}
		for (auto& itValid : validMap) {
			// itValid.second is the vector of LocalityEntries that belong to the same locality
			if (_policy->validate(itValid.second, fromServers)) {
				if (g_replicationdebug > 4) {
					printf("Across valid solution: %6lu key: %-7s count:%3d of%3d value: (%3d) %-10s policy: %-10s => "
					       "%s\n",
					       itValid.second.size(), _attribKey.c_str(), count + 1, _count, itValid.first._id,
					       fromServers->valueText(itValid.first).c_str(), _policy->name().c_str(),
					       _policy->info().c_str());
					if (g_replicationdebug > 5) {
						for (auto& entry : itValid.second) {
							printf("   entry: %s\n", fromServers->getEntryInfo(entry).c_str());
						}
					}
				}
				count ++;
			} else if (g_replicationdebug > 4) {
				printf("Across invalid solution:%5lu key: %-7s value: (%3d) %-10s policy: %-10s => %s\n", itValid.second.size(), _attribKey.c_str(), itValid.first._id, fromServers->valueText(itValid.first).c_str(), _policy->name().c_str(), _policy->info().c_str());
				if (g_replicationdebug > 5) {
					for (auto& entry : itValid.second) {
						printf("   entry: %s\n", fromServers->getEntryInfo(entry).c_str());
					}
				}
			}
		}
		if (count < _count) {
			if (g_replicationdebug > 3) {
				printf("Across failed solution: %3lu  key: %-7s values:%3lu count: %d=%d policy: %-10s => %s\n", solutionSet.size(), _attribKey.c_str(), validMap.size(),  count, _count, _policy->name().c_str(), _policy->info().c_str());
				for (auto& entry : solutionSet) {
					printf("   entry: %s\n", fromServers->getEntryInfo(entry).c_str());
				}
			}
			valid = false;
		}
	}
	return valid;
}

// Choose new servers from "least utilized" alsoServers and append the new servers to results
// fromserverse are the servers that have already been chosen and
// that should be excluded from being selected as replicas.
// FIXME: Simplify this function, such as removing unnecessary printf
bool PolicyAcross::selectReplicas(
	LocalitySetRef	&						fromServers,
	std::vector<LocalityEntry> const&		alsoServers,
	std::vector<LocalityEntry>	&				results )
{
	int					count = 0;
	AttribKey		indexKey = fromServers->keyIndex(_attribKey);
	auto				groupIndexKey = fromServers->getGroupKeyIndex(indexKey);
	int					resultsSize, resultsAdded;
	int					resultsInit = results.size();

	// Clear the member variables
	_usedValues.clear();
	_newResults.clear();
	_addedResults.resize(_arena, 0);

	if ((g_replicationdebug > 3) && (alsoServers.size())) {
		printf("Across !also:%4lu key: %-7s policy: %-10s => %s\n", alsoServers.size(), _attribKey.c_str(), _policy->name().c_str(), _policy->info().c_str());
	}
	for (auto& alsoServer : alsoServers) {
		auto value = fromServers->getValueViaGroupKey(alsoServer, groupIndexKey);
		if (value.present()) {
			auto lowerBound = std::lower_bound(_usedValues.begin(), _usedValues.end(), value.get());
			if ((lowerBound == _usedValues.end()) || (*lowerBound != value.get())) {
				//_selected is a set of processes that have the same indexKey and indexValue (value)
				_selected = fromServers->restrict(indexKey, value.get());
				if (_selected->size()) {
					// Pass only the also array item which are valid for the value
					if (g_replicationdebug > 5) {
						// entry is the locality entry info (entryValue) from the to-be-selected team member alsoServer
						printf("Across !select    key: %-7s value: (%3d) %-10s entry: %s\n", _attribKey.c_str(),
						       value.get()._id, fromServers->valueText(value.get()).c_str(),
						       fromServers->getEntryInfo(alsoServer).c_str());
					}
					resultsSize = _newResults.size();
					if (_policy->selectReplicas(_selected, alsoServers, _newResults))
					{
						resultsAdded = _newResults.size() - resultsSize;
						if (!resultsAdded) {
							count ++;
						}
						else {
							_addedResults.push_back(_arena, std::pair<int, int>(resultsAdded, resultsSize));
						}
						if (g_replicationdebug > 5) {
							printf("Across !added:%3d key: %-7s count:%3d of%3d value: (%3d) %-10s entry: %s\n",
							       resultsAdded, _attribKey.c_str(), count, _count, value.get()._id,
							       fromServers->valueText(value.get()).c_str(),
							       fromServers->getEntryInfo(alsoServer).c_str());
						}
						if (count >= _count) break;
						_usedValues.insert(lowerBound, value.get());
					}
					else if (g_replicationdebug > 5) {
						printf("Across !no answer key: %-7s value: (%3d) %-10s entry: %s\n", _attribKey.c_str(), value.get()._id, fromServers->valueText(value.get()).c_str(), fromServers->getEntryInfo(alsoServer).c_str());
					}
				}
				else if (g_replicationdebug > 5) {
					printf("Across !empty set key: %-7s value: (%3d) %-10s entry: %s\n", _attribKey.c_str(), value.get()._id, fromServers->valueText(value.get()).c_str(), fromServers->getEntryInfo(alsoServer).c_str());
				}

			}
			else if (g_replicationdebug > 5) {
				printf("Across !duplicate key: %-7s value: (%3d) %-10s entry: %s\n", _attribKey.c_str(), value.get()._id, fromServers->valueText(value.get()).c_str(), fromServers->getEntryInfo(alsoServer).c_str());
			}
		}
		else if (g_replicationdebug > 5) {
			printf("Across !no value  key: %-7s  %21s entry: %s\n", _attribKey.c_str(), "", fromServers->getEntryInfo(alsoServer).c_str());
		}
	}

	// Process the remaining results, if present
	if ((count < _count) && (_addedResults.size())) {
		// Sort the added results array
		std::sort(_addedResults.begin(), _addedResults.end(), PolicyAcross::compareAddedResults);

		if (g_replicationdebug > 2) {
			printf("Across !add sets  key: %-7s sets:%3d results:%3lu count:%3d of%3d\n", _attribKey.c_str(), _addedResults.size(), _newResults.size(), count, _count);
		}

		if (g_replicationdebug > 6) {
			LocalitySet::staticDisplayEntries(fromServers, alsoServers, "also");
			LocalitySet::staticDisplayEntries(fromServers, results, "results");
			LocalitySet::staticDisplayEntries(fromServers, _newResults, "add items");
		}

		for (auto& addedResult : _addedResults) {
			count ++;
			if (g_replicationdebug > 2) {
				printf("Across !add set   key: %-7s count:%3d of%3d  results:%3d index:%3d\n", _attribKey.c_str(), count, _count, addedResult.first, addedResult.second);
			}
			results.reserve(results.size() + addedResult.first);
			results.insert(results.end(), _newResults.begin()+addedResult.second, _newResults.begin()+addedResult.second+addedResult.first);
			if (count >= _count) break;
		}
		if (g_replicationdebug > 7) {
			LocalitySet::staticDisplayEntries(fromServers, results, "results");
		}
	}

	// Cannot find replica from the least used alsoServers, now try to find replicas from all servers
	// Process the remaining values
	if (count < _count) {
		if (g_replicationdebug > 3) {
			printf("Across items:%4d key: %-7s policy: %-10s => %s  count:%3d of%3d\n", fromServers->size(), _attribKey.c_str(), _policy->name().c_str(), _policy->info().c_str(), count, _count);
		}
		int recordIndex;
		// Use mutable array so that swaps does not affect actual element array
		auto& mutableArray = fromServers->getMutableEntries();
		for (int checksLeft = fromServers->size(); checksLeft > 0; checksLeft --) {
			if (g_replicationdebug > 6) {
				LocalitySet::staticDisplayEntries(fromServers, mutableArray, "mutable");
			}
			recordIndex = g_random->randomInt(0, checksLeft);
			auto& entry = mutableArray[recordIndex];
			auto value = fromServers->getValueViaGroupKey(entry, groupIndexKey);
			if (value.present()) {
				auto lowerBound = std::lower_bound(_usedValues.begin(), _usedValues.end(), value.get());
				if ((lowerBound == _usedValues.end()) || (*lowerBound != value.get())) {
					_selected = fromServers->restrict(indexKey, value.get());
					if (_selected->size()) {
						if (g_replicationdebug > 5) {
							printf("Across select:%3d key: %-7s value: (%3d) %-10s entry: %s  index:%4d\n",
							       fromServers->size() - checksLeft + 1, _attribKey.c_str(), value.get()._id,
							       fromServers->valueText(value.get()).c_str(),
							       fromServers->getEntryInfo(entry).c_str(), recordIndex);
						}
						if (_policy->selectReplicas(_selected, emptyEntryArray, results))
						{
							if (g_replicationdebug > 5) {
								printf("Across added:%4d key: %-7s value: (%3d) %-10s policy: %-10s => %s needed:%3d\n",
								       count + 1, _attribKey.c_str(), value.get()._id,
								       fromServers->valueText(value.get()).c_str(), _policy->name().c_str(),
								       _policy->info().c_str(), _count);
							}
							count ++;
							if (count >= _count) break;
							_usedValues.insert(lowerBound, value.get());
						}
						else if (g_replicationdebug > 5) {
							printf("Across no answer  key: %-7s value: (%3d) %-10s entry: %s\n", _attribKey.c_str(), value.get()._id, fromServers->valueText(value.get()).c_str(), fromServers->getEntryInfo(entry).c_str());
						}
					}
					else if (g_replicationdebug > 5) {
						printf("Across empty set:%3d    key: %-7s value: (%3d) %-10s  entry: %s  index:%4d\n", fromServers->size()-checksLeft+1, _attribKey.c_str(), value.get()._id, fromServers->valueText(value.get()).c_str(), fromServers->getEntryInfo(entry).c_str(), recordIndex);
					}
				}
				else if (g_replicationdebug > 5) {
					printf("Across duplicate  key: %-7s value: (%3d) %-10s entry: %s  attempt:%3d  index:%4d\n", _attribKey.c_str(), value.get()._id, fromServers->valueText(value.get()).c_str(), fromServers->getEntryInfo(entry).c_str(), fromServers->size()-checksLeft+1, recordIndex);
				}
			}
			else if (g_replicationdebug > 5) {
				printf("Across no value   key: %-7s  %21s entry: %s  attempt:%3d  index:%4d\n", _attribKey.c_str(), "", fromServers->getEntryInfo(entry).c_str(), fromServers->size()-checksLeft+1, recordIndex);
			}
			if (recordIndex != checksLeft-1) {
				if (g_replicationdebug > 5) {
					printf("Across swap       key: %-7s index:%4d  last:%4d  entry: %s\n", _attribKey.c_str(), recordIndex, checksLeft-1, fromServers->getEntryInfo(entry).c_str());
				}
				fromServers->swapMutableRecords(recordIndex, checksLeft-1);
			}
		}
	}
	// Clear the return array, if not satified
	if (count < _count) {
		if (g_replicationdebug > 4) printf("Across result count: %d < %d requested\n", count, _count);
		results.resize(resultsInit);
		count = 0;
	}
	if (g_replicationdebug > 2) {
		printf("Across used:%5lu results:%3d from %3d items  key: %-7s  policy: %-10s => %s\n", results.size()-resultsInit, count, fromServers->size(), _attribKey.c_str(), _policy->name().c_str(), _policy->info().c_str());
	}
	return (count >= _count);
}

bool PolicyAnd::validate(
	std::vector<LocalityEntry>	const&	solutionSet,
	LocalitySetRef const&				fromServers ) const
{
	bool valid = true;
	for (auto& policy : _policies) {
		if (!policy->validate(solutionSet, fromServers)) {
			valid = false;
			break;
		}
	}
	return valid;
}

bool PolicyAnd::selectReplicas(
	LocalitySetRef	&						fromServers,
	std::vector<LocalityEntry> const&		alsoServers,
	std::vector<LocalityEntry>	&				results )
{
	bool	passed = true;
	std::vector<LocalityEntry>	newResults(alsoServers);

	// Ensure that the results array is large enough
	if (newResults.capacity() < fromServers->size()) {
		newResults.reserve(fromServers->size());
	}

	for (auto& policy : _sortedPolicies) {
		if (g_replicationdebug > 3) {
			printf("And    also:%5lu used:  %4lu from %3d items  policy: %-10s => %s\n", newResults.size(), newResults.size()-alsoServers.size(), fromServers->size(), policy->name().c_str(), policy->info().c_str());
		}
		if (!policy->selectReplicas(fromServers, newResults, newResults))
		{
			if (g_replicationdebug > 3) {
				printf("And   failed  set:%4d policy: %-10s => %s\n", fromServers->size(), policy->name().c_str(), policy->info().c_str());
			}
			passed = false;
			break;
		}
	}
	if ((passed) && (newResults.size() > alsoServers.size())) {
		results.reserve(results.size() + newResults.size() - alsoServers.size());
		results.insert(results.end(), newResults.begin()+alsoServers.size(), newResults.end());
	}

	if (g_replicationdebug > 2) {
		printf("And    used:%5lu results:%3lu from %3d items\n", newResults.size()-alsoServers.size(), results.size(), fromServers->size());
	}
	return passed;
}

void testPolicySerialization(IRepPolicyRef& policy) {
	std::string	policyInfo = policy->info();

	BinaryWriter writer(IncludeVersion());
	serializeReplicationPolicy(writer, policy);

	BinaryReader reader(writer.getData(), writer.getLength(), IncludeVersion());
	IRepPolicyRef copy;
	serializeReplicationPolicy(reader, copy);

	ASSERT(policy->info() == copy->info());
}

void testReplicationPolicy(int nTests) {
	IRepPolicyRef policy = IRepPolicyRef(new PolicyAcross(1, "data_hall", IRepPolicyRef(new PolicyOne())));
	testPolicySerialization(policy);

	policy = IRepPolicyRef(new PolicyAnd({
		IRepPolicyRef(new PolicyAcross(2, "data_center", IRepPolicyRef(new PolicyAcross(3, "rack", IRepPolicyRef(new PolicyOne()))))),
		IRepPolicyRef(new PolicyAcross(2, "data_center", IRepPolicyRef(new PolicyAcross(2, "data_hall", IRepPolicyRef(new PolicyOne())))))
	}));

	testPolicySerialization(policy);
}

TEST_CASE("/ReplicationPolicy/Serialization") {
	testReplicationPolicy(1);
	return Void();
}
