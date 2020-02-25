#include "fdbclient/PrivateKeySpace.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

namespace {
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

// This function will move the given KeySelector to toward a standard KeySelector:
// orEqual == false && offset == 1
// If the corresponding key is not in this private key range, it will move as far as possible to adjust the offset to 1
// It looks like taking more time here since we query all keys twice in the worst case.
// However, moving the KeySelector while handling other parameters like limits makes the code much more complex and hard to maintain
// Seperate each part to make the code easy to understand and more compact
ACTOR Future<Void> normalizeKeySelectorActor(
    const PrivateKeyRangeBaseImpl* pkrImpl,
    ReadYourWritesTransaction* ryw,
    KeySelector* ks )
{
    ASSERT(!ks->orEqual); // should be removed before calling
    
    state KeyRangeRef range = pkrImpl->getKeyRange();
    state KeyRef startKey = range.begin;
    state KeyRef endKey = range.end;

    if (ks->offset < 1) {
        // less than the given key
        if (range.contains(ks->getKey()))
            endKey = keyAfter(ks->getKey());
    }
    else {
        // greater than the given key
        if (range.contains(ks->getKey()))
            startKey = ks->getKey();
    }

    Standalone<RangeResultRef> result = wait(pkrImpl->getRange(ryw, KeyRangeRef(startKey, endKey)));
    // TODO : KeySelector::setKey has bytes limit according to the knob, customize it if needed
    if (ks->offset < 1) {
        if (result.size() >= 1 - ks->offset) {
            ks->setKey(result[result.size()-(1-ks->offset)].key);
            ks->offset = 1;
        } else {
            ks->setKey(result[0].key);
            ks->offset += result.size();
        }
    } else {
        if (result.size() >= ks->offset - 1) {
            ks->setKey(result[ks->offset - 2].key);
            ks->offset = 1;
        } else {
            ks->setKey(result[result.size()-1].key);
            ks->offset -= result.size();
        }
    }
    return Void();
}

ACTOR Future<Standalone<RangeResultRef>> getRangeAggregationActor(
    PrivateKeySpace* pks,
    ReadYourWritesTransaction* ryw,
    KeySelector begin,
    KeySelector end,
    GetRangeLimits limits,
    bool reverse )
{   
    // This function handles ranges cover more than one keyrange and aggregates all results
    // KeySelector, GetRangeLimits and reverse are all handled here
    
    // make sure orEqual == false
    if(begin.orEqual)
		begin.removeOrEqual(begin.arena());
    if(end.orEqual)
        end.removeOrEqual(end.arena());

    // make sure offset == 1
    state RangeMap<Key, PrivateKeyRangeBaseImpl*, KeyRangeRef>::Iterator iter =
        pks->getKeyRangeMap()->rangeContaining(begin.getKey());
    while (begin.offset != 1 && iter != pks->getKeyRangeMap()->ranges().begin()) {
        wait(normalizeKeySelectorActor(iter->value(), ryw, &begin));
        --iter;
    }
    if (begin.offset != 1) {
        // The Key Selector points to key outside the whole private key space
        // TODO : Throw error here to indicate the case
        TEST(true);
    }
    iter = pks->getKeyRangeMap()->rangeContaining(end.getKey());
    while (end.offset != 1 && iter != pks->getKeyRangeMap()->ranges().end()) {
        wait(normalizeKeySelectorActor(iter->value(), ryw, &end));
        ++iter;
    }
    if (end.offset != 1) {
        // The Key Selector points to key outside the whole private key space
        // TODO : Throw error here to indicate the case
        TEST(true);
    }
    // return if range inverted
    if( begin.offset >= end.offset && begin.getKey() >= end.getKey() ) {
		TEST(true);
		return Standalone<RangeResultRef>();
	}
    
    state Standalone<RangeResultRef> result;
    state RangeMap<Key, PrivateKeyRangeBaseImpl*, KeyRangeRef>::Ranges ranges =
        pks->getKeyRangeMap()->intersectingRanges(KeyRangeRef(begin.getKey(), end.getKey()));
    // reverse handler
    // TODO : workaround to write this two together to make the code compact
    iter = reverse ? ranges.end() : ranges.begin();
    if (reverse) {
        while (iter != ranges.begin()) {
            --iter;
            if (iter->value() == NULL) 
                continue;
            KeyRangeRef kr = iter->range();
            KeyRef keyStart = kr.contains(begin.getKey()) ? begin.getKey() : kr.begin;
            KeyRef keyEnd =  kr.contains(end.getKey()) ? end.getKey() : kr.end;
            Standalone<RangeResultRef> pairs = wait(iter->value()->getRange(ryw, KeyRangeRef(keyStart, keyEnd)));
            // limits handler
            for (int i = pairs.size() - 1; i >= 0; --i) {
                result.push_back_deep(result.arena(), pairs[i]);
                limits.decrement(pairs[i]);
                if (limits.isReached())
                    return result;
            }
        }
    } else {
        for (iter = ranges.begin(); iter != ranges.end(); ++iter) {
            if (iter->value() == NULL)
                continue;
            KeyRangeRef kr = iter->range();
            KeyRef keyStart = kr.contains(begin.getKey()) ? begin.getKey() : kr.begin;
            KeyRef keyEnd =  kr.contains(end.getKey()) ? end.getKey() : kr.end;
            Standalone<RangeResultRef> pairs = wait(iter->value()->getRange(ryw, KeyRangeRef(keyStart, keyEnd)));
            // limits handler
            for (const KeyValueRef & kv : pairs) {
                result.push_back_deep(result.arena(), kv);
                limits.decrement(kv);
                if (limits.isReached())
                    return result;
            }
        }
    }
    return result;
}

// ACTOR Future<bool> moveKeySelectorActor(
//     const PrivateKeyRangeBaseImpl* pkrImpl,
//     ReadYourWritesTransaction* ryw,
//     KeySelector* begin,
//     KeySelector* end,
//     std::deque<KeyValueRef>& result )
// {
//     ASSERT(!begin->orEqual && !end->orEqual);
//     // If the start key or end key lies outside this keyrange, it cannot handle the case.
//     // Thus, it is forced for the quired range lies the this keyrange
//     // It assumes the begin of the range is never used as a key
//     state KeyRangeRef range = pkrImpl->getKeyRange();
//     state KeyRef startKey = range.begin;
//     state KeyRef endKey = range.end;
//     state int choice;
//     if (begin->offset != 1) {
//         if (begin->offset < 1) {
//             choice = 0;
//             if (range.contains(begin->getKey()))
//                 endKey = keyAfter(begin->getKey());
//         else {
//             choice = 1;
//             if (range.contains(begin->getKey()))
//                 startKey = begin->getKey();
//         }
//     } else {
//         if (end->offset <= 1) {
//             choice = 2;
//             if (range.contains(begin->getKey()))
//                 startKey = begin->getKey();
//             if (range.contains(end->getKey()))
//                 endKey = end->getKey();
//         } else {
//             choice = 3;
//             if (range.contains(end->getKey()))
//                 startKey = end->getKey();
//         }
//     }

//     state Standalone<RangeResultRef> temp = wait(pkrImpl->getRange(ryw, KeyRangeRef(startKey, endKey)));

//     if (choice == 0) {
//         for (int i = temp.size() - 1; i >= 0; --i) {
//             result.push_front(temp[i]);
//             ++begin->offset;
//             if (begin->offset == 1) return false; // TODO : add getLimits check here
//         }
//         return true;
//     } else if (choice == 1) {
//         if (begin->offset > 1) {
//             if (begin->offset - 1 <= temp.size()) {
//                 temp.pop_front(begin->offset - 1);
//                 begin->offset = 1;
//             } else {
//                 begin->offset -= temp.size();
//                 return true;
//             }
//         }
//         for (const KeyValueRef & kv : temp)
//             result.push_back(kv);
        
//         if ()
//     } else {
//         for (const KeyValueRef & kv : temp) {
//             result.push_back(kv);
//             --end->offset;
//             if (end->offset == 1) return false;
//         }
//         return true;
//     }
// }
} // namespace end
Future<Standalone<RangeResultRef>> PrivateKeySpace::getRange(
    ReadYourWritesTransaction* ryw,
    KeySelector begin,
    KeySelector end,
    GetRangeLimits limits,
    bool snapshot,
    bool reverse )
{
    // validate limits here
	if( !limits.isValid() )
		return range_limits_invalid();
    if( limits.isReached() ) {
		TEST(true); // read limit 0
		return Standalone<RangeResultRef>();
	}
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