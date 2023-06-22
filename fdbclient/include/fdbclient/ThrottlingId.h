/**
 * ThrottlingId.h
 */

#pragma once

#include "fdbclient/TagThrottle.actor.h"
#include "fdbrpc/TenantName.h"
#include "flow/Arena.h"

// ThrottlingId is similar to std::variant<TransactionTag, TenantGroup>
class ThrottlingIdRef {
	bool _isTag;
	StringRef value;

public:
	constexpr static FileIdentifier file_identifier = 7913086;

	ThrottlingIdRef();
	ThrottlingIdRef(Arena& arena, ThrottlingIdRef other);
	static Standalone<ThrottlingIdRef> fromTag(TransactionTagRef const& tag);
	static Standalone<ThrottlingIdRef> fromTenantGroup(TenantGroupNameRef const& tenantGroup);
	bool isTag() const;
	TransactionTagRef const& getTag() const&;
	TenantGroupNameRef const& getTenantGroup() const&;
	bool operator==(ThrottlingIdRef const& rhs) const;
	std::string toString() const;
	size_t hash() const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, _isTag, value);
	}
};

namespace std {

template <>
struct hash<ThrottlingIdRef> {
	size_t operator()(ThrottlingIdRef const& throttlingId) const { return throttlingId.hash(); }
};

template <>
struct hash<Standalone<ThrottlingIdRef>> {
	size_t operator()(Standalone<ThrottlingIdRef> const throttlingId) const { return throttlingId.hash(); }
};

} // namespace std

struct HashThrottlingId {
	size_t operator()(ThrottlingIdRef const& throttlingId) const;
};

template <>
struct Traceable<ThrottlingIdRef> : std::true_type {
	static std::string toString(ThrottlingIdRef const& value) { return value.toString(); }
};

using ThrottlingId = Standalone<ThrottlingIdRef>;

template <class Value>
using ThrottlingIdMap = std::unordered_map<ThrottlingId, Value, HashThrottlingId>;

template <class Value>
using PrioritizedThrottlingIdMap = std::map<TransactionPriority, ThrottlingIdMap<Value>>;
