/**
 * ThrottlingId.cpp
 */

#include "fdbclient/ThrottlingId.h"

ThrottlingIdRef::ThrottlingIdRef() = default;

ThrottlingIdRef::ThrottlingIdRef(Arena& arena, ThrottlingIdRef other)
  : _isTag(other._isTag), value(arena, other.value) {}

ThrottlingId ThrottlingIdRef::fromTag(TransactionTagRef const& tag) {
	ThrottlingId result;
	result._isTag = true;
	result.value = StringRef(result.arena(), tag);
	return result;
}

ThrottlingId ThrottlingIdRef::fromTenantGroup(TenantGroupNameRef const& tenantGroup) {
	Standalone<ThrottlingIdRef> result;
	result._isTag = false;
	result.value = StringRef(result.arena(), tenantGroup);
	return result;
}

bool ThrottlingIdRef::isTag() const {
	return _isTag;
}

TransactionTagRef const& ThrottlingIdRef::getTag() const& {
	ASSERT(_isTag);
	return value;
}

TenantGroupNameRef const& ThrottlingIdRef::getTenantGroup() const& {
	ASSERT(!_isTag);
	return value;
}

bool ThrottlingIdRef::operator==(ThrottlingIdRef const& rhs) const {
	return (_isTag == rhs._isTag) && !value.compare(rhs.value);
}

std::string ThrottlingIdRef::toString() const {
	return printable(value.toString());
}

size_t ThrottlingIdRef::hash() const {
	return std::hash<StringRef>{}(value) ^ (_isTag ? 0x0 : 0x1);
}

size_t HashThrottlingId::operator()(ThrottlingIdRef const& throttlingId) const {
	return throttlingId.hash();
}
