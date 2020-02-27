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
	// TO DISCUSS : do we need this general getRange interface here?
	// Since a keyRange doesn't have any knowledge about other keyRanges, parameters like KeySelector,
	// GetRangeLimits should be handled together in PrivateKeySpace
	// Thus, having this general interface looks unnessary.
	// virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin, KeySelector end, GetRangeLimits limits, bool snapshot = false, bool reverse = false) const = 0;

	// Each derived class only needs to implement this simple version of getRange
	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const = 0;

	explicit PrivateKeyRangeBaseImpl(KeyRef start, KeyRef end) {
		range = KeyRangeRef(range.arena(), KeyRangeRef(start, end));
	}
	KeyRangeRef getKeyRange() const {
		return range;
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

	PrivateKeySpace(KeyRef rangeEndKey = allKeys.end) {
		// Default value is NULL
		impls = KeyRangeMap<PrivateKeyRangeBaseImpl*>(NULL, rangeEndKey);
	}
	void registerKeyRange(const KeyRangeRef& kr, PrivateKeyRangeBaseImpl* impl) {
		// TODO : range check
		impls.insert(kr, impl);
	}

	KeyRangeMap<PrivateKeyRangeBaseImpl*>* getKeyRangeMap(){
		return &impls;
	}

private:
	KeyRangeMap<PrivateKeyRangeBaseImpl*> impls;
};

#endif