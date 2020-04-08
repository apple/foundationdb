#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_SPECIALKEYSPACE_ACTOR_G_H)
#define FDBCLIENT_SPECIALKEYSPACE_ACTOR_G_H
#include "fdbclient/SpecialKeySpace.actor.g.h"
#elif !defined(FDBCLIENT_SPECIALKEYSPACE_ACTOR_H)
#define FDBCLIENT_SPECIALKEYSPACE_ACTOR_H

#include "flow/flow.h"
#include "flow/Arena.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class SpecialKeyRangeBaseImpl {
public:
	// Each derived class only needs to implement this simple version of getRange
	virtual Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                                    KeyRangeRef kr) const = 0;

	explicit SpecialKeyRangeBaseImpl(KeyRef start, KeyRef end) {
		range = KeyRangeRef(range.arena(), KeyRangeRef(start, end));
	}
	KeyRangeRef getKeyRange() const { return range; }
	ACTOR Future<Void> normalizeKeySelectorActor(const SpecialKeyRangeBaseImpl* pkrImpl,
	                                             Reference<ReadYourWritesTransaction> ryw, KeySelector* ks);

protected:
	KeyRange range; // underlying key range for this function
};

class SpecialKeySpace {
public:
	Future<Optional<Value>> get(Reference<ReadYourWritesTransaction> ryw, const Key& key);

	Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw, KeySelector begin,
	                                            KeySelector end, GetRangeLimits limits, bool reverse = false);

	SpecialKeySpace(KeyRef spaceStartKey = Key(), KeyRef spaceEndKey = normalKeys.end) {
		// Default value is nullptr, begin of KeyRangeMap is Key()
		impls = KeyRangeMap<SpecialKeyRangeBaseImpl*>(nullptr, spaceEndKey);
		range = KeyRangeRef(spaceStartKey, spaceEndKey);
	}
	void registerKeyRange(const KeyRangeRef& kr, SpecialKeyRangeBaseImpl* impl) {
		// range check
		// TODO: add range check not to be replaced by overlapped ones
		ASSERT(kr.begin >= range.begin && kr.end <= range.end);
		// make sure the registered range is not overlapping with existing ones
		// Note: kr.end should not be the same as another range's begin, although it should work even they are the same
		ASSERT(impls.rangeContaining(kr.begin) == impls.rangeContaining(kr.end) && impls[kr.begin] == nullptr);
		impls.insert(kr, impl);
	}

private:
	ACTOR Future<Optional<Value>> getActor(SpecialKeySpace* pks, Reference<ReadYourWritesTransaction> ryw, KeyRef key);

	ACTOR Future<Standalone<RangeResultRef>> getRangeAggregationActor(SpecialKeySpace* pks,
	                                                                  Reference<ReadYourWritesTransaction> ryw,
	                                                                  KeySelector begin, KeySelector end,
	                                                                  GetRangeLimits limits, bool reverse);

	KeyRangeMap<SpecialKeyRangeBaseImpl*> impls;
	KeyRange range;
};

// Use special key prefix "\xff\xff/transaction/conflicting_keys/<some_key>",
// to retrieve keys which caused latest not_committed(conflicting with another transaction) error.
// The returned key value pairs are interpretted as :
// prefix/<key1> : '1' - any keys equal or larger than this key are (probably) conflicting keys
// prefix/<key2> : '0' - any keys equal or larger than this key are (definitely) not conflicting keys
// Currently, the conflicting keyranges returned are original read_conflict_ranges or union of them.
class ConflictingKeysImpl : public SpecialKeyRangeBaseImpl {
public:
	explicit ConflictingKeysImpl(KeyRef start, KeyRef end);
	Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                            KeyRangeRef kr) const override;
};

#include "flow/unactorcompiler.h"
#endif
