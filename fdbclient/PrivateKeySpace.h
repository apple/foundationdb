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
};

// This class 
class PrivateKeyRangeSimpleImpl : public PrivateKeyRangeBaseImpl {
public:
	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, const KeyRange& keys) const = 0;
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

	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin, KeySelector end, GetRangeLimits limits, bool snapshot = false, bool reverse = false) const;

	void registerKeyRange(const KeyRange& kr, PrivateKeyRangeBaseImpl* impl) {
		impls.insert(kr, impl);
	}

private:

	// ACTOR Future<Optional<Value>> getActor(ReadYourWritesTransaction* ryw, const Key& key, bool snapshot) {
	// 	// use getRange to workaround this
	// 	Standalone<RangeResultRef> result = wait(getRange(ryw, KeySelector( firstGreaterOrEqual(key), key.arena() ),
	// 			KeySelector( firstGreaterOrEqual(keyAfter(key)), key.arena() ), GetRangeLimits(1), snapshot));
	// 	if (result.size()) {
	// 		return Optional<Value>(result[0].value);
	// 	} else {
	// 		return Optional<Value>();
	// 	}
	// };
	KeyRangeMap<PrivateKeyRangeBaseImpl*> impls;
};

#endif