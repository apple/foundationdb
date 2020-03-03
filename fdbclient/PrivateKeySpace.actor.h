#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_PRIVATEKEYSPACE_ACTOR_G_H)
#define FDBCLIENT_PRIVATEKEYSPACE_ACTOR_G_H
#include "fdbclient/PrivateKeySpace.actor.g.h"
#elif !defined(FDBCLIENT_PRIVATEKEYSPACE_ACTOR_H)
#define FDBCLIENT_PRIVATEKEYSPACE_ACTOR_H

#include "flow/flow.h"
#include "flow/Arena.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class PrivateKeyRangeBaseImpl {
public:
	// TO DISCUSS : do we need this general getRange interface here?
	// Since a keyRange doesn't have any knowledge about other keyRanges, parameters like KeySelector,
	// GetRangeLimits should be handled together in PrivateKeySpace
	// Thus, having this general interface looks unnessary.
	// virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin,
	// KeySelector end, GetRangeLimits limits, bool snapshot = false, bool reverse = false) const = 0;

	// Each derived class only needs to implement this simple version of getRange
	virtual Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                                    KeyRangeRef kr) const = 0;

	explicit PrivateKeyRangeBaseImpl(KeyRef start, KeyRef end) {
		range = KeyRangeRef(range.arena(), KeyRangeRef(start, end));
	}
	KeyRangeRef getKeyRange() const { return range; }
	ACTOR Future<Void> normalizeKeySelectorActor(const PrivateKeyRangeBaseImpl* pkrImpl,
                                             Reference<ReadYourWritesTransaction> ryw, KeySelector* ks);
protected:
	KeyRange range; // underlying key range for this function
};

// class PrivateKeyRangeSimpleImpl : public PrivateKeyRangeBaseImpl {
// public:
// 	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const = 0;
// 	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin, KeySelector
// end, GetRangeLimits limits, bool snapshot = false, bool reverse = false) const;
// };

class PrivateKeySpace {
public:
	Future<Optional<Value>> get(Reference<ReadYourWritesTransaction> ryw, const Key& key, bool snapshot = false);

	Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw, KeySelector begin,
	                                            KeySelector end, GetRangeLimits limits, bool snapshot = false,
	                                            bool reverse = false);

	PrivateKeySpace(KeyRef spaceStartKey = Key(), KeyRef spaceEndKey = allKeys.end) {
		// Default value is nullptr, begin of KeyRangeMap is Key()
		impls = KeyRangeMap<PrivateKeyRangeBaseImpl*>(nullptr, spaceEndKey);
		range = KeyRangeRef(spaceStartKey, spaceEndKey);
	}
	void registerKeyRange(const KeyRangeRef& kr, PrivateKeyRangeBaseImpl* impl) {
		// range check
		ASSERT(kr.begin >= range.begin && kr.end <= range.end);
		impls.insert(kr, impl);
	}

private:
	ACTOR Future<Optional<Value>> getActor(PrivateKeySpace* pks, Reference<ReadYourWritesTransaction> ryw, KeyRef key);

	ACTOR Future<Standalone<RangeResultRef>> getRangeAggregationActor(PrivateKeySpace* pks,
	                                                                  Reference<ReadYourWritesTransaction> ryw,
	                                                                  KeySelector begin, KeySelector end,
	                                                                  GetRangeLimits limits, bool reverse);

	KeyRangeMap<PrivateKeyRangeBaseImpl*> impls;
	KeyRange range;
};

#include "flow/unactorcompiler.h"
#endif
