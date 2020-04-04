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
	// TO DISCUSS : do we need this general getRange interface here?
	// Since a keyRange doesn't have any knowledge about other keyRanges, parameters like KeySelector,
	// GetRangeLimits should be handled together in SpecialKeySpace
	// Thus, having this general interface looks unnessary.
	// virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin,
	// KeySelector end, GetRangeLimits limits, bool snapshot = false, bool reverse = false) const = 0;

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
	// TODO : remove snapshot parameter
	Future<Optional<Value>> get(Reference<ReadYourWritesTransaction> ryw, const Key& key, bool snapshot = false);

	Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw, KeySelector begin,
	                                            KeySelector end, GetRangeLimits limits, bool snapshot = false,
	                                            bool reverse = false);

	SpecialKeySpace(KeyRef spaceStartKey = Key(), KeyRef spaceEndKey = normalKeys.end) {
		// Default value is nullptr, begin of KeyRangeMap is Key()
		impls = KeyRangeMap<SpecialKeyRangeBaseImpl*>(nullptr, spaceEndKey);
		range = KeyRangeRef(spaceStartKey, spaceEndKey);
	}
	void registerKeyRange(const KeyRangeRef& kr, SpecialKeyRangeBaseImpl* impl) {
		// range check
		ASSERT(kr.begin >= range.begin && kr.end <= range.end);
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

class ConflictingKeysImpl : public SpecialKeyRangeBaseImpl {
public:
	explicit ConflictingKeysImpl(KeyRef start, KeyRef end) : SpecialKeyRangeBaseImpl(start, end) {}
	virtual Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                                    KeyRangeRef kr) const {
		Standalone<RangeResultRef> result;
		if (ryw->getTransaction().info.conflictingKeys) {
			auto krMap = ryw->getTransaction().info.conflictingKeys.get();
			auto beginIter = krMap->rangeContaining(kr.begin);
			if (beginIter->begin() != kr.begin)
				++beginIter;
			auto endIter = krMap->rangeContaining(kr.end);
			for (auto it = beginIter; it != endIter; ++it) {
				// TODO : check push_back instead of push_back_deep
				result.push_back_deep(result.arena(), KeyValueRef(it->begin(), it->value()));
			}
			if (endIter->begin() != kr.end)
				result.push_back_deep(result.arena(), KeyValueRef(endIter->begin(), endIter->value()));
		}
		return result;
	}
};

#include "flow/unactorcompiler.h"
#endif
