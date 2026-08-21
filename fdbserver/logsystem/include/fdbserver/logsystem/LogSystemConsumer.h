/*
 * LogSystemConsumer.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FDBSERVER_LOGSYSTEM_LOGSYSTEMCONSUMER_H
#define FDBSERVER_LOGSYSTEM_LOGSYSTEMCONSUMER_H
#pragma once

#include "fdbserver/logsystem/LogSystem.h"

// Narrow consumer-facing view of the log system.  LogSystem retains ownership of
// epoch, push, and recovery state; LogSystemConsumer exposes only the read/pop
// behavior needed by storage servers and other log consumers.
struct LogSystemConsumer : ReferenceCounted<LogSystemConsumer> {
	explicit LogSystemConsumer(Reference<LogSystem> logSystem) : logSystem(logSystem) {}

	Reference<IReplayPeekCursor> peekAll(UID dbgid, Version begin, Version end, Tag tag, bool parallelGetMore);
	Reference<IPeekCursor> peekRemote(UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore);
	Reference<IPeekCursor> peek(UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore);
	Reference<IPeekCursor> peek(UID dbgid,
	                            Version begin,
	                            Optional<Version> end,
	                            std::vector<Tag> tags,
	                            bool parallelGetMore);
	Reference<IReplayPeekCursor> peekLocal(UID dbgid,
	                                       Tag tag,
	                                       Version begin,
	                                       Version end,
	                                       bool useMergePeekCursors,
	                                       int8_t peekLocality = tagLocalityInvalid);
	Reference<IPeekCursor> peekTxs(UID dbgid,
	                               Version begin,
	                               int8_t peekLocality,
	                               Version localEnd,
	                               bool canDiscardPopped);
	Reference<IReplayPeekCursor> peekSingle(
	    UID dbgid,
	    Version begin,
	    Tag tag,
	    std::vector<std::pair<Version, Tag>> history = std::vector<std::pair<Version, Tag>>());
	Reference<IReplayPeekCursor> peekLogRouter(
	    UID dbgid,
	    Version begin,
	    Tag tag,
	    bool useSatellite,
	    Optional<Version> end = Optional<Version>(),
	    const Optional<std::map<uint8_t, std::vector<uint16_t>>>& knownStoppedTLogIds =
	        Optional<std::map<uint8_t, std::vector<uint16_t>>>());

	void popLogRouter(Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality);
	void popTxs(Version upTo, int8_t popLocality = tagLocalityInvalid);
	void pop(Version upTo, Tag tag, Version durableKnownCommittedVersion = 0, int8_t popLocality = tagLocalityInvalid);
	// Waits until every currently targeted TLog reports that `tag` has been popped through `upTo`.
	Future<Void> waitForPopped(Version upTo, Tag tag, int8_t popLocality = tagLocalityInvalid);
	Future<Version> getTxsPoppedVersion();
	Version getEnd() const;
	Version getPeekEnd() const;
	Tag getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const;

private:
	Reference<LogSystem> logSystem;
};

#endif
