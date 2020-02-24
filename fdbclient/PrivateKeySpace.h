#ifndef FDBCLIENT_PRIVATEKEYSPACE_H
#define FDBCLIENT_PRIVATEKEYSPACE_H
#pragma once

#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"

class ReadYourWritesTransaction;

class PrivateKeyRangeBaseImpl {
public:
	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin, KeySelector end, GetRangeLimits limits, bool snapshot = false, bool reverse = false) const = 0;

	KeyRangeRef getKeyRange() const {
		return range;
	}
protected:
	KeyRangeRef range;
};

// This class 
class PrivateKeyRangeSimpleImpl : public PrivateKeyRangeBaseImpl {
public:
	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const = 0;
	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin, KeySelector end, GetRangeLimits limits, bool snapshot = false, bool reverse = false) const;
};

// class PrivateKeyRangeGetAllImpl : public PrivateKeyRangeSimpleGetRangeImpl {
// public:
// 	virtual Future<Standalone<RangeResultRef>> getRange(const KeyRange& keys, ReadYourWritesTransaction* ryw) const;
// 	virtual Future<Standalone<RangeResultRef>> get(ReadYourWritesTransaction* ryw) const = 0;
// };
class PrivateKeySpace {
public:
	Future<Optional<Value>> get(ReadYourWritesTransaction* ryw, const Key& key, bool snapshot = false);

	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin, KeySelector end, GetRangeLimits limits, bool snapshot = false, bool reverse = false);

	void registerKeyRange(const KeyRange& kr, PrivateKeyRangeBaseImpl* impl) {
		impls.insert(kr, impl);
	}

	RangeMap<Key, PrivateKeyRangeBaseImpl*, KeyRangeRef>::Iterator getIteratorForKey(const Key& key) {
		return impls.rangeContaining(key);
	}

	KeyRangeMap<PrivateKeyRangeBaseImpl*>* getKeyRangeMap(){
		return &impls;
	}

private:
	KeyRangeMap<PrivateKeyRangeBaseImpl*> impls;
};

#endif