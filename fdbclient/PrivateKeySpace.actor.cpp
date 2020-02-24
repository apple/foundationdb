#include "fdbclient/PrivateKeySpace.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

ACTOR Future<Optional<Value>> getActor(
    PrivateKeySpace* pks,
    ReadYourWritesTransaction* ryw,
    KeyRef key )
{
    // use getRange to workaround this
    Standalone<RangeResultRef> result = wait(pks->getRange(ryw, KeySelector( firstGreaterOrEqual(key) ),
			KeySelector( firstGreaterOrEqual(keyAfter(key)) ), GetRangeLimits()));
    ASSERT(result.size() <= 1);
    if (result.size()) {
        return Optional<Value>(result[0].value);
    } else {
        return Optional<Value>();
    }
}

ACTOR Future<Standalone<RangeResultRef>> getRangeAggregationActor(
    PrivateKeySpace* pks,
    ReadYourWritesTransaction* ryw,
    KeySelector begin,
    KeySelector end,
    GetRangeLimits limits,
    bool reverse )
{   
    // This function handles ranges lie over more than one underlying keyrane and aggregates all results
    // GetRangeLimits and reverse are also handled here

    // do parameter validation check stuff 
    if( limits.isReached() ) {
		TEST(true); // RYW range read limit 0
		return Standalone<RangeResultRef>();
	}

    // TODO: check the reason here
	// if( !limits.isValid() )
	// 	return range_limits_invalid();
    
    // erase equal here
    if( begin.orEqual )
		begin.removeOrEqual(begin.arena());

	if( end.orEqual )
		end.removeOrEqual(end.arena());
        
    if( begin.offset >= end.offset && begin.getKey() >= end.getKey() ) {
		TEST(true); //range inverted
		return Standalone<RangeResultRef>();
	}

    state std::deque<KeyValueRef> resultRef;
    state Standalone<RangeResultRef> result;
    // state RangeMap<Key, PrivateKeyRangeBaseImpl*, KeyRangeRef>::Iterator iter;
    state RangeMap<Key, PrivateKeyRangeBaseImpl*, KeyRangeRef>::Ranges ranges;


    // Handle the case where begin offset is zero or negative, which means at least one key before begin.key needs to be read
    state RangeMap<Key, PrivateKeyRangeBaseImpl*, KeyRangeRef>::Iterator iter = pks->getKeyRangeMap()->rangeContaining(begin.getKey());
    // state RangeMap<Key, PrivateKeyRangeBaseImpl*, KeyRangeRef>::Iterator prev = curr;
    // --prev;
    if (begin.offset <= 0) {
        state int remains = 1 - begin.offset;
        while (remains > 0) {
            if (iter.value() == NULL)  {
                if (iter == pks->getKeyRangeMap()->ranges().begin())
                    break;
                else
                    --iter;
                continue;
            }
            state Standalone<RangeResultRef> temp = wait(iter->value()->getRange(
                ryw, 
                KeySelector(firstGreaterOrEqual(iter.value()->getKeyRange().begin)),
                KeySelectorRef(firstGreaterOrEqual(begin.getKey())),
                GetRangeLimits()
            ));
            for (int i = temp.size() - 1; i >= 0 && remains > 0; --i) {
                resultRef.push_front(temp[i]);
                remains--;
            }
            if (iter == pks->getKeyRangeMap()->ranges().begin())
                break;
            else
                --iter;
        }
        if (remains > 0) {
            //throw error here
        }
    }
    // Check limits here

    // (begin.key, range.end)

    // contained range query

    // (range.begin, end.key)

    // end.offset > 0
    
    // The interesting range is in (begin.key, end.key)
    ranges = pks->getKeyRangeMap()->intersectingRanges(KeyRangeRef(begin.getKey(), end.getKey()));
    for (iter = ranges.begin(); iter != ranges.end(); ++iter) {
        if (iter->value() == NULL) continue;
        KeyRangeRef kr = iter->range();
        Standalone<RangeResultRef> pairs = wait(iter->value()->getRange(ryw,
            KeySelector( firstGreaterOrEqual(kr.begin) ),
			KeySelector( firstGreaterOrEqual(kr.end)),
            GetRangeLimits()
            ));
        result.append_deep(result.arena(), pairs.begin(), pairs.size());
    }
    if(begin.offset - end.offset <= result.size()){
        result.pop_front(begin.offset);
        for (int i =0; i<-end.offset; i++)
            result.pop_back();
    } else {
        return Standalone<RangeResultRef>();
    }
    return result;
}

ACTOR Future<Standalone<RangeResultRef>> getRangeActor(
    const PrivateKeyRangeSimpleImpl* pkrSimpleImpl,
    ReadYourWritesTransaction* ryw,
    KeySelector begin,
    KeySelector end )
{
    // If the start key or end key lies outside this keyrange, it cannot handle the case.
    // Thus, it is forced for the quired range lies the this keyrange
    // It assumes the begin of the range is never used as a key
    KeyRangeRef range = pkrSimpleImpl->getKeyRange();
    if (begin.orEqual)
        begin.removeOrEqual(begin.arena());
    ASSERT(begin.offset > 0 && begin.getKey() >= range.begin);
    if (end.orEqual)
        end.removeOrEqual(end.arena());
    ASSERT(end.offset <= 0 && end.getKey() <= range.end);

    KeyRangeRef kr(begin.getKey(), end.getKey());
    state Standalone<RangeResultRef> result = wait(pkrSimpleImpl->getRange(ryw, kr));
    if (begin.offset - end.offset >= result.size()) {
        // inverted select range
        return Standalone<RangeResultRef>();
    } else {
        // TODO : may need optimization
        // pop from head
        result.pop_front(begin.offset);
        // pop from end
        for (int i = 0; i < -end.offset; ++i) result.pop_back();
        // if (limits.reachedBy(result)) {
        //     int idx;
        //     for (idx = 0; idx < result.size(); ++ idx) {
        //         limits.decrement(result[idx]);
        //         if (limits.isReached()) break;
        //     }
        //     while (idx < result.size()) {
        //         result.pop_back();
        //         ++idx;
        //     }
        // }
        return result;
    }
}

Future<Standalone<RangeResultRef>> PrivateKeyRangeSimpleImpl::getRange(
    ReadYourWritesTransaction* ryw,
    KeySelector begin,
    KeySelector end,
    GetRangeLimits limits,
    bool snapshot,
    bool reverse ) const
{
    // ignore snapshot, which is invalid
    // ignore reverse and limits, which is handled by PrivateKeySpace when doing aggregation
    return getRangeActor(this, ryw, begin, end);
}

Future<Standalone<RangeResultRef>> PrivateKeySpace::getRange(
    ReadYourWritesTransaction* ryw,
    KeySelector begin,
    KeySelector end,
    GetRangeLimits limits,
    bool snapshot,
    bool reverse )
{
    // ignore snapshot, which is not used
    return getRangeAggregationActor(this, ryw, begin, end, limits, reverse);
}

Future<Optional<Value>> PrivateKeySpace::get(
    ReadYourWritesTransaction* ryw,
    const Key& key,
    bool snapshot)
{
    // ignore snapshot, which is not used
    return getActor(this, ryw, key);
}