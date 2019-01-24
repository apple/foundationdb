/*
 * ReplicationPolicy.h
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

#ifndef FLOW_REPLICATION_POLICY_H
#define FLOW_REPLICATION_POLICY_H
#pragma once

#include "flow/flow.h"
#include "fdbrpc/ReplicationTypes.h"

template <class Ar>
void serializeReplicationPolicy(Ar& ar, IRepPolicyRef& policy);
extern void testReplicationPolicy(int nTests);


struct IReplicationPolicy : public ReferenceCounted<IReplicationPolicy> {
		IReplicationPolicy() {}
		virtual ~IReplicationPolicy() {}
		virtual std::string name() const = 0;
		virtual std::string info() const = 0;
		virtual void addref() { ReferenceCounted<IReplicationPolicy>::addref(); }
		virtual void delref() { ReferenceCounted<IReplicationPolicy>::delref(); }
		virtual int maxResults() const = 0;
		virtual int depth() const = 0;
		virtual bool selectReplicas(
			LocalitySetRef &										fromServers,
			std::vector<LocalityEntry> const&		alsoServers,
			std::vector<LocalityEntry>	&				results ) = 0;
	    virtual void traceLocalityRecords(LocalitySetRef const& fromServers);
	    virtual void traceOneLocalityRecord(Reference<LocalityRecord> record, LocalitySetRef const& fromServers);
	    virtual bool validate(
			std::vector<LocalityEntry>	const&	solutionSet,
			LocalitySetRef const&								fromServers ) const = 0;

		bool operator == ( const IReplicationPolicy& r ) const { return info() == r.info(); }
		bool operator != ( const IReplicationPolicy& r ) const { return info() != r.info(); }

		template <class Ar>
		void serialize(Ar& ar) {
			IRepPolicyRef	refThis(this);
			serializeReplicationPolicy(ar, refThis);
			refThis->delref_no_destroy();
		}

		// Utility functions
		bool selectReplicas(
			LocalitySetRef &										fromServers,
			std::vector<LocalityEntry>	&				results );
		bool validate(
			LocalitySetRef const&								solutionSet ) const;
		bool validateFull(
			bool																solved,
			std::vector<LocalityEntry>	const&	solutionSet,
			std::vector<LocalityEntry> const&		alsoServers,
			LocalitySetRef const&								fromServers );

		// Returns a set of the attributes that this policy uses in selection and validation.
		std::set<std::string> attributeKeys() const
		{ std::set<std::string> keys; this->attributeKeys(&keys); return keys; }
		virtual void attributeKeys(std::set<std::string>*) const = 0;
};

template <class Archive>
inline void load( Archive& ar, IRepPolicyRef& value ) {
	bool	present = (value.getPtr());
	ar >> present;
	if (present) {
		serializeReplicationPolicy(ar, value);
	}
	else {
		value.clear();
	}
}

template <class Archive>
inline void save( Archive& ar, const IRepPolicyRef& value ) {
	bool	present = (value.getPtr());
	ar << present;
	if (present) {
		serializeReplicationPolicy(ar, (IRepPolicyRef&) value);
	}
}

struct PolicyOne : IReplicationPolicy, public ReferenceCounted<PolicyOne> {
	PolicyOne() {};
	virtual ~PolicyOne() {};
	virtual std::string name() const { return "One"; }
	virtual std::string info() const { return "1"; }
	virtual int maxResults() const { return 1; }
	virtual int depth() const { return 1; }
	virtual bool validate(
		std::vector<LocalityEntry>	const&	solutionSet,
		LocalitySetRef const&				fromServers ) const;
	virtual bool selectReplicas(
		LocalitySetRef	&						fromServers,
		std::vector<LocalityEntry> const&		alsoServers,
		std::vector<LocalityEntry>	&				results );
	template <class Ar>
	void serialize(Ar& ar) {
	}
	virtual void attributeKeys(std::set<std::string>* set) const override { return; }
};

struct PolicyAcross : IReplicationPolicy, public ReferenceCounted<PolicyAcross> {
	PolicyAcross(int count, std::string const& attribKey, IRepPolicyRef const policy);
	virtual ~PolicyAcross();
	virtual std::string name() const { return "Across"; }
	virtual std::string info() const
	{ return format("%s^%d x ", _attribKey.c_str(), _count) + _policy->info(); }
	virtual int maxResults() const { return _count * _policy->maxResults(); }
	virtual int depth() const  { return 1 + _policy->depth(); }
	virtual bool validate(
		std::vector<LocalityEntry>	const&	solutionSet,
		LocalitySetRef const&				fromServers ) const;
	virtual bool selectReplicas(
		LocalitySetRef	&						fromServers,
		std::vector<LocalityEntry> const&		alsoServers,
		std::vector<LocalityEntry>	&				results );

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, _attribKey, _count);
		serializeReplicationPolicy(ar, _policy);
	}

	static bool compareAddedResults(const std::pair<int, int>& rhs, const std::pair<int, int>& lhs)
	{ return (rhs.first < lhs.first) || (!(lhs.first < rhs.first) && (rhs.second < lhs.second)); }

	virtual void attributeKeys(std::set<std::string> *set) const override
	{ set->insert(_attribKey); _policy->attributeKeys(set); }

protected:
	int																_count;
	std::string												_attribKey;
	IRepPolicyRef								_policy;

	// Cache temporary members
	std::vector<AttribValue>					_usedValues;
	std::vector<LocalityEntry>				_newResults;
	LocalitySetRef										_selected;
	VectorRef<std::pair<int,int>>			_addedResults;
	Arena															_arena;
};

struct PolicyAnd : IReplicationPolicy, public ReferenceCounted<PolicyAnd> {
	PolicyAnd(std::vector<IRepPolicyRef> policies): _policies(policies), _sortedPolicies(policies)
	{
		// Sort the policy array
		std::sort(_sortedPolicies.begin(), _sortedPolicies.end(), PolicyAnd::comparePolicy);
	}
	virtual ~PolicyAnd() {}
	virtual std::string name() const { return "And"; }
	virtual std::string info() const {
		std::string	infoText;
		for (auto& policy : _policies) {
			infoText += ((infoText.length()) ? " & ("  : "(") + policy->info() + ")";
		}
		if (_policies.size()) infoText = "(" + infoText + ")";
		return infoText;
	}
	virtual int maxResults() const {
		int resultsMax = 0;
		for (auto& policy : _policies) {
			resultsMax += policy->maxResults();
		}
		return resultsMax;
	}
	virtual int depth() const {
		int policyDepth, depthMax = 0;
		for (auto& policy : _policies) {
			policyDepth = policy->depth();
			if (policyDepth > depthMax) {
				depthMax = policyDepth;
			}
		}
		return depthMax;
	}
	virtual bool validate(
		std::vector<LocalityEntry>	const&	solutionSet,
		LocalitySetRef const&				fromServers ) const;

	virtual bool selectReplicas(
		LocalitySetRef	&						fromServers,
		std::vector<LocalityEntry> const&		alsoServers,
		std::vector<LocalityEntry>	&				results );

	static bool comparePolicy(const IRepPolicyRef& rhs, const IRepPolicyRef& lhs)
	{ return (lhs->maxResults() < rhs->maxResults()) || (!(rhs->maxResults() < lhs->maxResults()) && (lhs->depth() < rhs->depth())); }

	template <class Ar>
	void serialize(Ar& ar) {
		int count = _policies.size();
		serializer(ar, count);
		_policies.resize(count);
		for(int i = 0; i < count; i++) {
			serializeReplicationPolicy(ar, _policies[i]);
		}
		if(Ar::isDeserializing) {
			_sortedPolicies = _policies;
			std::sort(_sortedPolicies.begin(), _sortedPolicies.end(), PolicyAnd::comparePolicy);
		}
	}

	virtual void attributeKeys(std::set<std::string> *set) const override
	{ for (const IRepPolicyRef& r : _policies) { r->attributeKeys(set); } }

protected:
	std::vector<IRepPolicyRef>			_policies;
	std::vector<IRepPolicyRef>			_sortedPolicies;
};

extern int testReplication();


template <class Ar>
void serializeReplicationPolicy(Ar& ar, IRepPolicyRef& policy) {
	if(Ar::isDeserializing) {
		StringRef name;
		serializer(ar, name);

		if(name == LiteralStringRef("One")) {
			PolicyOne* pointer = new PolicyOne();
			pointer->serialize(ar);
			policy = IRepPolicyRef(pointer);
		}
		else if(name == LiteralStringRef("Across")) {
			PolicyAcross* pointer = new PolicyAcross(0, "", IRepPolicyRef());
			pointer->serialize(ar);
			policy = IRepPolicyRef(pointer);
		}
		else if(name == LiteralStringRef("And")) {
			PolicyAnd* pointer = new PolicyAnd({});
			pointer->serialize(ar);
			policy = IRepPolicyRef(pointer);
		}
		else if(name == LiteralStringRef("None")) {
			policy = IRepPolicyRef();
		}
		else {
			TraceEvent(SevError, "SerializingInvalidPolicyType")
				.detailext("PolicyName", name);
		}
	}
	else {
		std::string name = policy ? policy->name() : "None";
		Standalone<StringRef> nameRef = StringRef(name);
		serializer(ar, nameRef);
		if(name == "One") {
			((PolicyOne*)policy.getPtr())->serialize(ar);
		}
		else if(name == "Across") {
			((PolicyAcross*)policy.getPtr())->serialize(ar);
		}
		else if(name == "And") {
			((PolicyAnd*)policy.getPtr())->serialize(ar);
		}
		else if(name == "None") {}
		else {
			TraceEvent(SevError, "SerializingInvalidPolicyType")
				.detail("PolicyName", name);
		}
	}
}

#endif
