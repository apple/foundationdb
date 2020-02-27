#ifndef FDBCLIENT_PRIVATEKEYSPACE_H
#define FDBCLIENT_PRIVATEKEYSPACE_H
#pragma once

#include "flow/flow.h"
#include "flow/Arena.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"

class ReadYourWritesTransaction;

class PrivateKeyRangeBaseImpl {
public:
	// virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin, KeySelector end, GetRangeLimits limits, bool snapshot = false, bool reverse = false) const = 0;
	// TODO : My opinion is that having this interface is enough for underlying keyrange implemention
	// A key range doesn't have any knowledge about other key range, parameters like KeySelector, GetRangeLimits should be handled in PrivateKeySpace
	// Thus, it is no need to have them here
	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const = 0;

	explicit PrivateKeyRangeBaseImpl(KeyRef start, KeyRef end) {
		// TODO : checker: make sure it is in valid key range
		range = KeyRangeRef(start, end);
	}
	KeyRangeRef getKeyRange() const {
		return KeyRangeRef(range.begin, range.end);
	}
protected:
	KeyRange range; // underlying key range for this function
};


// class PrivateKeyRangeSimpleImpl : public PrivateKeyRangeBaseImpl {
// public:
// 	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const = 0;
// 	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin, KeySelector end, GetRangeLimits limits, bool snapshot = false, bool reverse = false) const;
// };

class PrivateKeySpace {
public:
	Future<Optional<Value>> get(ReadYourWritesTransaction* ryw, const Key& key, bool snapshot = false);

	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin, KeySelector end, GetRangeLimits limits, bool snapshot = false, bool reverse = false);

	PrivateKeySpace() {
		impls = KeyRangeMap<PrivateKeyRangeBaseImpl*>(NULL, LiteralStringRef("\xff\xff\xff"));
	}
	void registerKeyRange(const KeyRangeRef& kr, PrivateKeyRangeBaseImpl* impl) {
		// TODO : range checker
		impls.insert(kr, impl);
	}

	KeyRangeMap<PrivateKeyRangeBaseImpl*>* getKeyRangeMap(){
		return &impls;
	}

private:
	KeyRangeMap<PrivateKeyRangeBaseImpl*> impls;
};

#endif