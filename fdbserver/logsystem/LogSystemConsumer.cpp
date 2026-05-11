#include "fdbserver/logsystem/LogSystemConsumer.h"

#include <utility>

Reference<IPeekCursor> LogSystemConsumer::peekAll(
    UID dbgid, Version begin, Version end, Tag tag, bool parallelGetMore) {
	return logSystem->peekAll(dbgid, begin, end, tag, parallelGetMore);
}

Reference<IPeekCursor> LogSystemConsumer::peekRemote(
    UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore) {
	return logSystem->peekRemote(dbgid, begin, end, tag, parallelGetMore);
}

Reference<IPeekCursor> LogSystemConsumer::peek(
    UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore) {
	return logSystem->peek(dbgid, begin, end, tag, parallelGetMore);
}

Reference<IPeekCursor> LogSystemConsumer::peek(
    UID dbgid, Version begin, Optional<Version> end, std::vector<Tag> tags, bool parallelGetMore) {
	return logSystem->peek(dbgid, begin, end, std::move(tags), parallelGetMore);
}

Reference<IPeekCursor> LogSystemConsumer::peekLocal(UID dbgid,
                                                    Tag tag,
                                                    Version begin,
                                                    Version end,
                                                    bool useMergePeekCursors,
                                                    int8_t peekLocality) {
	return logSystem->peekLocal(dbgid, tag, begin, end, useMergePeekCursors, peekLocality);
}

Reference<IPeekCursor> LogSystemConsumer::peekTxs(
    UID dbgid, Version begin, int8_t peekLocality, Version localEnd, bool canDiscardPopped) {
	return logSystem->peekTxs(dbgid, begin, peekLocality, localEnd, canDiscardPopped);
}

Reference<IPeekCursor> LogSystemConsumer::peekSingle(
    UID dbgid, Version begin, Tag tag, std::vector<std::pair<Version, Tag>> history) {
	return logSystem->peekSingle(dbgid, begin, tag, std::move(history));
}

Reference<IPeekCursor> LogSystemConsumer::peekLogRouter(
    UID dbgid,
    Version begin,
    Tag tag,
    bool useSatellite,
    Optional<Version> end,
    const Optional<std::map<uint8_t, std::vector<uint16_t>>>& knownStoppedTLogIds) {
	return logSystem->peekLogRouter(dbgid, begin, tag, useSatellite, end, knownStoppedTLogIds);
}

void LogSystemConsumer::popLogRouter(
    Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality) {
	logSystem->popLogRouter(upTo, tag, durableKnownCommittedVersion, popLocality);
}

void LogSystemConsumer::popTxs(Version upTo, int8_t popLocality) {
	logSystem->popTxs(upTo, popLocality);
}

void LogSystemConsumer::pop(
    Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality) {
	logSystem->pop(upTo, tag, durableKnownCommittedVersion, popLocality);
}

Future<Version> LogSystemConsumer::getTxsPoppedVersion() {
	return logSystem->getTxsPoppedVersion();
}

Version LogSystemConsumer::getEnd() const {
	return logSystem->getEnd();
}

Tag LogSystemConsumer::getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const {
	return logSystem->getPseudoPopTag(tag, type);
}
