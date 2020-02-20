#include "fdbclient/PrivateKeySpace.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

ACTOR Future<Optional<Value>> getActor(
    PrivateKeySpace* pks,
    ReadYourWritesTransaction* ryw,
    KeyRef key,
    bool snapshot )
{
    // use getRange to workaround this
    Standalone<RangeResultRef> result = wait(pks->getRange(ryw, KeySelector( firstGreaterOrEqual(key) ),
			KeySelector( firstGreaterOrEqual(keyAfter(key)) ), GetRangeLimits(1), snapshot));
    if (result.size()) {
        return Optional<Value>(result[0].value);
    } else {
        return Optional<Value>();
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
    // do the easiest stuff here, suppose we have no snapshot and reverse
    KeyRef startkey = begin.getKey();
    KeyRef endkey = end.getKey();
    KeyRange kr = KeyRangeRef(startkey, endkey);
    return getRange(ryw, kr);
}

Future<Standalone<RangeResultRef>> PrivateKeySpace::getRange(
    ReadYourWritesTransaction* ryw,
    KeySelector begin,
    KeySelector end,
    GetRangeLimits limits,
    bool snapshot,
    bool reverse ) const
{
    // do stuff here
    return Standalone<RangeResultRef>();
}

Future<Optional<Value>> PrivateKeySpace::get(
    ReadYourWritesTransaction* ryw,
    const Key& key,
    bool snapshot)
{
    return getActor(this, ryw, key, snapshot);
}