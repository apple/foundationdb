/*
 * ReplicationPolicy.h
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

#ifndef FLOW_REPLICATION_POLICY_H
#define FLOW_REPLICATION_POLICY_H
#pragma once

#include "flow/flow.h"
#include "fdbrpc/ReplicationTypes.h"

template <class Ar>
void serializeReplicationPolicy(Ar& ar, Reference<IReplicationPolicy>& policy);
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
	virtual bool selectReplicas(Reference<LocalitySet>& fromServers,
	                            std::vector<LocalityEntry> const& alsoServers,
	                            std::vector<LocalityEntry>& results) = 0;
	virtual void traceLocalityRecords(Reference<LocalitySet> const& fromServers);
	virtual void traceOneLocalityRecord(Reference<LocalityRecord> record, Reference<LocalitySet> const& fromServers);
	virtual bool validate(std::vector<LocalityEntry> const& solutionSet,
	                      Reference<LocalitySet> const& fromServers) const = 0;

	bool operator==(const IReplicationPolicy& r) const { return info() == r.info(); }
	bool operator!=(const IReplicationPolicy& r) const { return info() != r.info(); }

	template <class Ar>
	void serialize(Ar& ar) {
		static_assert(!is_fb_function<Ar>);
		Reference<IReplicationPolicy> refThis(this);
		serializeReplicationPolicy(ar, refThis);
		refThis->delref_no_destroy();
	}
	virtual void deserializationDone() = 0;

	// Utility functions
	bool selectReplicas(Reference<LocalitySet>& fromServers, std::vector<LocalityEntry>& results);
	bool validate(Reference<LocalitySet> const& solutionSet) const;
	bool validateFull(bool solved,
	                  std::vector<LocalityEntry> const& solutionSet,
	                  std::vector<LocalityEntry> const& alsoServers,
	                  Reference<LocalitySet> const& fromServers);

	// Returns a set of the attributes that this policy uses in selection and validation.
	std::set<std::string> attributeKeys() const {
		std::set<std::string> keys;
		this->attributeKeys(&keys);
		return keys;
	}
	virtual void attributeKeys(std::set<std::string>*) const = 0;
};

template <class Archive>
inline void load(Archive& ar, Reference<IReplicationPolicy>& value) {
	bool present;
	ar >> present;
	if (present) {
		serializeReplicationPolicy(ar, value);
	} else {
		value.clear();
	}
}

template <class Archive>
inline void save(Archive& ar, const Reference<IReplicationPolicy>& value) {
	bool present = (value.getPtr() != nullptr);
	ar << present;
	if (present) {
		serializeReplicationPolicy(ar, (Reference<IReplicationPolicy>&)value);
	}
}

struct PolicyOne final : IReplicationPolicy, public ReferenceCounted<PolicyOne> {
	PolicyOne(){};
	explicit PolicyOne(const PolicyOne& o) {}
	std::string name() const override { return "One"; }
	std::string info() const override { return "1"; }
	int maxResults() const override { return 1; }
	int depth() const override { return 1; }
	bool validate(std::vector<LocalityEntry> const& solutionSet,
	              Reference<LocalitySet> const& fromServers) const override;
	bool selectReplicas(Reference<LocalitySet>& fromServers,
	                    std::vector<LocalityEntry> const& alsoServers,
	                    std::vector<LocalityEntry>& results) override;
	template <class Ar>
	void serialize(Ar& ar) {
		static_assert(!is_fb_function<Ar>);
	}
	void deserializationDone() override {}
	void attributeKeys(std::set<std::string>* set) const override { return; }
};

struct PolicyAcross final : IReplicationPolicy, public ReferenceCounted<PolicyAcross> {
	friend struct serializable_traits<PolicyAcross*>;
	PolicyAcross(int count, std::string const& attribKey, Reference<IReplicationPolicy> const policy);
	explicit PolicyAcross();
	explicit PolicyAcross(const PolicyAcross& other) : PolicyAcross(other._count, other._attribKey, other._policy) {}
	~PolicyAcross() override;
	std::string name() const override { return "Across"; }
	std::string embeddedPolicyName() const { return _policy->name(); }
	int getCount() const { return _count; }
	std::string info() const override { return format("%s^%d x ", _attribKey.c_str(), _count) + _policy->info(); }
	int maxResults() const override { return _count * _policy->maxResults(); }
	int depth() const override { return 1 + _policy->depth(); }
	bool validate(std::vector<LocalityEntry> const& solutionSet,
	              Reference<LocalitySet> const& fromServers) const override;
	bool selectReplicas(Reference<LocalitySet>& fromServers,
	                    std::vector<LocalityEntry> const& alsoServers,
	                    std::vector<LocalityEntry>& results) override;

	template <class Ar>
	void serialize(Ar& ar) {
		static_assert(!is_fb_function<Ar>);
		serializer(ar, _attribKey, _count);
		serializeReplicationPolicy(ar, _policy);
	}

	void deserializationDone() override {}

	static bool compareAddedResults(const std::pair<int, int>& rhs, const std::pair<int, int>& lhs) {
		return (rhs.first < lhs.first) || (!(lhs.first < rhs.first) && (rhs.second < lhs.second));
	}

	void attributeKeys(std::set<std::string>* set) const override {
		set->insert(_attribKey);
		_policy->attributeKeys(set);
	}

	Reference<IReplicationPolicy> embeddedPolicy() const { return _policy; }

	const std::string& attributeKey() const { return _attribKey; }

protected:
	int _count;
	std::string _attribKey;
	Reference<IReplicationPolicy> _policy;

	// Cache temporary members
	std::vector<AttribValue> _usedValues;
	std::vector<LocalityEntry> _newResults;
	Reference<LocalitySet> _selected;
	VectorRef<std::pair<int, int>> _addedResults;
	Arena _arena;
};

struct PolicyAnd final : IReplicationPolicy, public ReferenceCounted<PolicyAnd> {
	friend struct serializable_traits<PolicyAnd*>;
	PolicyAnd(std::vector<Reference<IReplicationPolicy>> policies) : _policies(policies), _sortedPolicies(policies) {
		// Sort the policy array
		std::sort(_sortedPolicies.begin(), _sortedPolicies.end(), PolicyAnd::comparePolicy);
	}
	explicit PolicyAnd(const PolicyAnd& other) : _policies(other._policies), _sortedPolicies(other._sortedPolicies) {}
	explicit PolicyAnd() {}
	std::string name() const override { return "And"; }
	std::string info() const override {
		std::string infoText;
		for (auto& policy : _policies) {
			infoText += ((infoText.length()) ? " & (" : "(") + policy->info() + ")";
		}
		if (_policies.size())
			infoText = "(" + infoText + ")";
		return infoText;
	}
	int maxResults() const override {
		int resultsMax = 0;
		for (auto& policy : _policies) {
			resultsMax += policy->maxResults();
		}
		return resultsMax;
	}
	int depth() const override {
		int policyDepth, depthMax = 0;
		for (auto& policy : _policies) {
			policyDepth = policy->depth();
			if (policyDepth > depthMax) {
				depthMax = policyDepth;
			}
		}
		return depthMax;
	}
	bool validate(std::vector<LocalityEntry> const& solutionSet,
	              Reference<LocalitySet> const& fromServers) const override;

	bool selectReplicas(Reference<LocalitySet>& fromServers,
	                    std::vector<LocalityEntry> const& alsoServers,
	                    std::vector<LocalityEntry>& results) override;

	static bool comparePolicy(const Reference<IReplicationPolicy>& rhs, const Reference<IReplicationPolicy>& lhs) {
		return (lhs->maxResults() < rhs->maxResults()) ||
		       (!(rhs->maxResults() < lhs->maxResults()) && (lhs->depth() < rhs->depth()));
	}

	template <class Ar>
	void serialize(Ar& ar) {
		static_assert(!is_fb_function<Ar>);
		int count = _policies.size();
		serializer(ar, count);
		_policies.resize(count);
		for (int i = 0; i < count; i++) {
			serializeReplicationPolicy(ar, _policies[i]);
		}
		if (Ar::isDeserializing) {
			_sortedPolicies = _policies;
			std::sort(_sortedPolicies.begin(), _sortedPolicies.end(), PolicyAnd::comparePolicy);
		}
	}

	void deserializationDone() override {
		_sortedPolicies = _policies;
		std::sort(_sortedPolicies.begin(), _sortedPolicies.end(), PolicyAnd::comparePolicy);
	}

	void attributeKeys(std::set<std::string>* set) const override {
		for (const Reference<IReplicationPolicy>& r : _policies) {
			r->attributeKeys(set);
		}
	}

protected:
	std::vector<Reference<IReplicationPolicy>> _policies;
	std::vector<Reference<IReplicationPolicy>> _sortedPolicies;
};

template <class Ar>
void serializeReplicationPolicy(Ar& ar, Reference<IReplicationPolicy>& policy) {
	// To change this serialization, ProtocolVersion::ReplicationPolicy must be updated, and downgrades need to be
	// considered
	if (Ar::isDeserializing) {
		StringRef name;
		serializer(ar, name);

		if (name == LiteralStringRef("One")) {
			PolicyOne* pointer = new PolicyOne();
			pointer->serialize(ar);
			policy = Reference<IReplicationPolicy>(pointer);
		} else if (name == LiteralStringRef("Across")) {
			PolicyAcross* pointer = new PolicyAcross(0, "", Reference<IReplicationPolicy>());
			pointer->serialize(ar);
			policy = Reference<IReplicationPolicy>(pointer);
		} else if (name == LiteralStringRef("And")) {
			PolicyAnd* pointer = new PolicyAnd{};
			pointer->serialize(ar);
			policy = Reference<IReplicationPolicy>(pointer);
		} else if (name == LiteralStringRef("None")) {
			policy = Reference<IReplicationPolicy>();
		} else {
			TraceEvent(SevError, "SerializingInvalidPolicyType").detail("PolicyName", name);
		}
	} else {
		std::string name = policy ? policy->name() : "None";
		Standalone<StringRef> nameRef = StringRef(name);
		serializer(ar, nameRef);
		if (name == "One") {
			((PolicyOne*)policy.getPtr())->serialize(ar);
		} else if (name == "Across") {
			((PolicyAcross*)policy.getPtr())->serialize(ar);
		} else if (name == "And") {
			((PolicyAnd*)policy.getPtr())->serialize(ar);
		} else if (name == "None") {
		} else {
			TraceEvent(SevError, "SerializingInvalidPolicyType").detail("PolicyName", name);
		}
	}
}

template <>
struct dynamic_size_traits<Reference<IReplicationPolicy>> : std::true_type {
private:
	using T = Reference<IReplicationPolicy>;

public:
	template <class Context>
	static size_t size(const T& value, Context& context) {
		// size gets called multiple times. If this becomes a performance problem, we can perform the
		// serialization once and cache the result as a mutable member of IReplicationPolicy
		BinaryWriter writer{ AssumeVersion(context.protocolVersion()) };
		::save(writer, value);
		return writer.getLength();
	}

	// Guaranteed to be called only once during serialization
	template <class Context>
	static void save(uint8_t* out, const T& value, Context& context) {
		BinaryWriter writer{ AssumeVersion(context.protocolVersion()) };
		::save(writer, value);
		memcpy(out, writer.getData(), writer.getLength());
	}

	// Context is an arbitrary type that is plumbed by reference throughout the
	// load call tree.
	template <class Context>
	static void load(const uint8_t* buf, size_t sz, Reference<IReplicationPolicy>& value, Context& context) {
		StringRef str(buf, sz);
		BinaryReader reader(str, AssumeVersion(context.protocolVersion()));
		::load(reader, value);
	}
};

#endif
