#include "fdbserver/logsystem/LogSystemConsumer.h"

#include <algorithm>
#include <utility>

#include "flow/genericactors.actor.h"

namespace {
bool shouldPopFromLogSet(Reference<LogSet> const& logSet, Tag tag, int8_t popLocality) {
	// CDC tags are replicated to each TLog set. Once a version is acknowledged, every copy can be discarded;
	// leaving remote copies unpopped can retain old log generations across failover.
	return logSet->locality == tagLocalitySpecial || logSet->locality == tag.locality ||
	       tag.locality == tagLocalityCDC ||
	       (tag.locality < 0 && ((popLocality == tagLocalityInvalid) == logSet->isLocal));
}
} // namespace

Reference<IReplayPeekCursor> LogSystemConsumer::peekAll(UID dbgid,
                                                        Version begin,
                                                        Version end,
                                                        Tag tag,
                                                        bool parallelGetMore) {
	auto& ls = *logSystem;
	int bestSet = 0;
	std::vector<Reference<LogSet>> localSets;
	Version lastBegin = 0;
	bool foundSpecial = false;
	int logIdx = 0;
	int bestSetIdx = 0;
	for (auto& log : ls.tLogs) {
		if (log->locality == tagLocalitySpecial) {
			foundSpecial = true;
		}
		if (log->isLocal && !log->logServers.empty() &&
		    (log->locality == tagLocalitySpecial || log->locality == tag.locality || tag.locality == tagLocalityTxs ||
		     tag.locality == tagLocalityLogRouter || tag.locality == tagLocalityCDC)) {
			lastBegin = std::max(lastBegin, log->startVersion);
			localSets.push_back(log);
			if (log->locality != tagLocalitySatellite) {
				bestSet = localSets.size() - 1;
				bestSetIdx = logIdx;
			}
		}
		logIdx++;
	}

	if (localSets.empty()) {
		lastBegin = end;
	}

	if (begin >= lastBegin && !localSets.empty()) {
		TraceEvent("TLogPeekAllCurrentOnly", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end)
		    .detail("BestLogs", localSets[bestSet]->logServerString());
		int bestServer = localSets[bestSet]->bestLocationFor(tag);
		Optional<std::vector<uint16_t>> bestKnownLockedTLogIds;
		if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
			ls.resetBestServerIfNotLocked(bestSetIdx, bestServer, end, ls.knownLockedTLogIds);
			ASSERT_WE_THINK(ls.knownLockedTLogIds.contains(bestSetIdx));
			bestKnownLockedTLogIds = ls.knownLockedTLogIds[bestSetIdx];
		}
		return makeReference<SetPeekCursor>(
		    localSets, bestSet, bestServer, tag, begin, end, parallelGetMore, bestKnownLockedTLogIds);
	} else {
		std::vector<Reference<IReplayPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;

		if (lastBegin < end && !localSets.empty()) {
			TraceEvent("TLogPeekAllAddingCurrent", dbgid)
			    .detail("Tag", tag.toString())
			    .detail("Begin", begin)
			    .detail("End", end)
			    .detail("BestLogs", localSets[bestSet]->logServerString());
			int bestServer = localSets[bestSet]->bestLocationFor(tag);
			Optional<std::vector<uint16_t>> bestKnownLockedTLogIds;
			if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
				ls.resetBestServerIfNotLocked(bestSetIdx, bestServer, end, ls.knownLockedTLogIds);
				ASSERT_WE_THINK(ls.knownLockedTLogIds.contains(bestSetIdx));
				bestKnownLockedTLogIds = ls.knownLockedTLogIds[bestSetIdx];
			}
			cursors.push_back(makeReference<SetPeekCursor>(
			    localSets, bestSet, bestServer, tag, lastBegin, end, parallelGetMore, bestKnownLockedTLogIds));
		}
		for (int i = 0; begin < lastBegin; i++) {
			if (i == ls.oldLogData.size()) {
				if (tag.locality == tagLocalityTxs) {
					break;
				}
				TraceEvent("TLogPeekAllDead", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end)
				    .detail("LastBegin", lastBegin)
				    .detail("OldLogDataSize", ls.oldLogData.size());
				return makeReference<ServerPeekCursor>(
				    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, ls.getPeekEnd(), false, false);
			}

			int bestOldSet = 0;
			std::vector<Reference<LogSet>> localOldSets;
			Version thisBegin = begin;
			bool thisSpecial = false;
			for (auto& log : ls.oldLogData[i].tLogs) {
				if (log->locality == tagLocalitySpecial) {
					thisSpecial = true;
				}
				if (log->isLocal && !log->logServers.empty() &&
				    (log->locality == tagLocalitySpecial || log->locality == tag.locality ||
				     tag.locality == tagLocalityTxs || tag.locality == tagLocalityLogRouter ||
				     tag.locality == tagLocalityCDC)) {
					thisBegin = std::max(thisBegin, log->startVersion);
					localOldSets.push_back(log);
					if (log->locality != tagLocalitySatellite) {
						bestOldSet = localOldSets.size() - 1;
					}
				}
			}

			if (localOldSets.empty()) {
				TraceEvent("TLogPeekAllNoLocalSets", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end)
				    .detail("LastBegin", lastBegin);
				if (cursors.empty() && !foundSpecial) {
					continue;
				}
				return makeReference<ServerPeekCursor>(
				    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, ls.getPeekEnd(), false, false);
			}
			if (thisSpecial) {
				foundSpecial = true;
			}

			if (thisBegin < lastBegin) {
				if (thisBegin < end) {
					TraceEvent("TLogPeekAllAddingOld", dbgid)
					    .detail("Tag", tag.toString())
					    .detail("Begin", begin)
					    .detail("End", end)
					    .detail("BestLogs", localOldSets[bestOldSet]->logServerString())
					    .detail("LastBegin", lastBegin)
					    .detail("ThisBegin", thisBegin);
					cursors.push_back(makeReference<SetPeekCursor>(localOldSets,
					                                               bestOldSet,
					                                               localOldSets[bestOldSet]->bestLocationFor(tag),
					                                               tag,
					                                               thisBegin,
					                                               std::min(lastBegin, end),
					                                               parallelGetMore));
					epochEnds.push_back(LogMessageVersion(std::min(lastBegin, end)));
				}
				lastBegin = thisBegin;
			}
		}

		return makeReference<ReplayMultiCursor>(cursors, epochEnds, tag.locality != tagLocalityCDC);
	}
}

Reference<IPeekCursor> LogSystemConsumer::peekRemote(UID dbgid,
                                                     Version begin,
                                                     Optional<Version> end,
                                                     Tag tag,
                                                     bool parallelGetMore) {
	auto& ls = *logSystem;
	int bestSet = -1;
	Version lastBegin = ls.recoveredAt.present() ? ls.recoveredAt.get() + 1 : 0;
	for (int t = 0; t < ls.tLogs.size(); t++) {
		if (ls.tLogs[t]->isLocal) {
			lastBegin = std::max(lastBegin, ls.tLogs[t]->startVersion);
		}

		if (!ls.tLogs[t]->logRouters.empty()) {
			ASSERT(bestSet == -1);
			bestSet = t;
		}
	}
	if (bestSet == -1) {
		TraceEvent("TLogPeekRemoteNoBestSet", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end.present() ? end.get() : ls.getPeekEnd());
		return makeReference<ServerPeekCursor>(Reference<AsyncVar<OptionalInterface<TLogInterface>>>(),
		                                       tag,
		                                       begin,
		                                       ls.getPeekEnd(),
		                                       false,
		                                       parallelGetMore);
	}
	if (begin >= lastBegin) {
		TraceEvent("TLogPeekRemoteBestOnly", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end.present() ? end.get() : ls.getPeekEnd())
		    .detail("BestSet", bestSet)
		    .detail("BestSetStart", lastBegin)
		    .detail("LogRouterIds", ls.tLogs[bestSet]->logRouterString());
		return makeReference<BufferedCursor>(ls.tLogs[bestSet]->logRouters,
		                                     tag,
		                                     begin,
		                                     end.present() ? end.get() + 1 : ls.getPeekEnd(),
		                                     parallelGetMore);
	} else {
		std::vector<Reference<IPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;
		TraceEvent("TLogPeekRemoteAddingBest", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end.present() ? end.get() : ls.getPeekEnd())
		    .detail("BestSet", bestSet)
		    .detail("BestSetStart", lastBegin)
		    .detail("LogRouterIds", ls.tLogs[bestSet]->logRouterString());
		cursors.push_back(makeReference<BufferedCursor>(ls.tLogs[bestSet]->logRouters,
		                                                tag,
		                                                lastBegin,
		                                                end.present() ? end.get() + 1 : ls.getPeekEnd(),
		                                                parallelGetMore));
		int i = 0;
		while (begin < lastBegin) {
			if (i == ls.oldLogData.size()) {
				TraceEvent("TLogPeekRemoteDead", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end.present() ? end.get() : ls.getPeekEnd())
				    .detail("LastBegin", lastBegin)
				    .detail("OldLogDataSize", ls.oldLogData.size());
				return makeReference<ServerPeekCursor>(Reference<AsyncVar<OptionalInterface<TLogInterface>>>(),
				                                       tag,
				                                       begin,
				                                       ls.getPeekEnd(),
				                                       false,
				                                       parallelGetMore);
			}

			int bestOldSet = -1;
			Version thisBegin = begin;
			for (int t = 0; t < ls.oldLogData[i].tLogs.size(); t++) {
				if (ls.oldLogData[i].tLogs[t]->isLocal) {
					thisBegin = std::max(thisBegin, ls.oldLogData[i].tLogs[t]->startVersion);
				}

				if (!ls.oldLogData[i].tLogs[t]->logRouters.empty()) {
					ASSERT(bestOldSet == -1);
					bestOldSet = t;
				}
			}
			if (bestOldSet == -1) {
				TraceEvent("TLogPeekRemoteNoOldBestSet", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end.present() ? end.get() : ls.getPeekEnd());
				return makeReference<ServerPeekCursor>(Reference<AsyncVar<OptionalInterface<TLogInterface>>>(),
				                                       tag,
				                                       begin,
				                                       ls.getPeekEnd(),
				                                       false,
				                                       parallelGetMore);
			}

			if (thisBegin < lastBegin) {
				TraceEvent("TLogPeekRemoteAddingOldBest", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end.present() ? end.get() : ls.getPeekEnd())
				    .detail("BestOldSet", bestOldSet)
				    .detail("LogRouterIds", ls.oldLogData[i].tLogs[bestOldSet]->logRouterString())
				    .detail("LastBegin", lastBegin)
				    .detail("ThisBegin", thisBegin)
				    .detail("BestStartVer", ls.oldLogData[i].tLogs[bestOldSet]->startVersion);
				cursors.push_back(makeReference<BufferedCursor>(
				    ls.oldLogData[i].tLogs[bestOldSet]->logRouters, tag, thisBegin, lastBegin, parallelGetMore));
				epochEnds.emplace_back(lastBegin);
				lastBegin = thisBegin;
			}
			i++;
		}

		return makeReference<MultiCursor>(cursors, epochEnds);
	}
}

Reference<IPeekCursor> LogSystemConsumer::peek(UID dbgid,
                                               Version begin,
                                               Optional<Version> end,
                                               Tag tag,
                                               bool parallelGetMore) {
	auto& ls = *logSystem;
	if (ls.tLogs.empty()) {
		TraceEvent("TLogPeekNoLogSets", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
		return makeReference<ServerPeekCursor>(
		    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, ls.getPeekEnd(), false, false);
	}

	if (tag.locality == tagLocalityRemoteLog) {
		return peekRemote(dbgid, begin, end, tag, parallelGetMore);
	} else {
		return peekAll(dbgid, begin, end.present() ? end.get() + 1 : ls.getPeekEnd(), tag, parallelGetMore);
	}
}

Reference<IPeekCursor> LogSystemConsumer::peek(UID dbgid,
                                               Version begin,
                                               Optional<Version> end,
                                               std::vector<Tag> tags,
                                               bool parallelGetMore) {
	auto& ls = *logSystem;
	if (tags.empty()) {
		TraceEvent("TLogPeekNoTags", dbgid).detail("Begin", begin);
		return makeReference<ServerPeekCursor>(
		    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), invalidTag, begin, ls.getPeekEnd(), false, false);
	}

	if (tags.size() == 1) {
		return peek(dbgid, begin, end, tags[0], parallelGetMore);
	}

	std::vector<Reference<IPeekCursor>> cursors;
	cursors.reserve(tags.size());
	for (auto tag : tags) {
		cursors.push_back(peek(dbgid, begin, end, tag, parallelGetMore));
	}
	return makeReference<BufferedCursor>(cursors, begin, end.present() ? end.get() + 1 : ls.getPeekEnd(), true, false);
}

Reference<IReplayPeekCursor> LogSystemConsumer::peekLocal(UID dbgid,
                                                          Tag tag,
                                                          Version begin,
                                                          Version end,
                                                          bool useMergePeekCursors,
                                                          int8_t peekLocality) {
	auto& ls = *logSystem;
	if (tag.locality >= 0 || tag.locality == tagLocalitySpecial) {
		peekLocality = tag.locality;
	}
	ASSERT(peekLocality >= 0 || tag.locality == tagLocalitySpecial);

	int bestSet = -1;
	bool foundSpecial = false;
	int logCount = 0;
	for (int t = 0; t < ls.tLogs.size(); t++) {
		if (!ls.tLogs[t]->logServers.empty() && ls.tLogs[t]->locality != tagLocalitySatellite) {
			logCount++;
		}
		if (!ls.tLogs[t]->logServers.empty() &&
		    (ls.tLogs[t]->locality == tagLocalitySpecial || ls.tLogs[t]->locality == peekLocality ||
		     peekLocality == tagLocalitySpecial)) {
			if (ls.tLogs[t]->locality == tagLocalitySpecial) {
				foundSpecial = true;
			}
			bestSet = t;
			break;
		}
	}
	if (bestSet == -1) {
		TraceEvent("TLogPeekLocalNoBestSet", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end)
		    .detail("LogCount", logCount);
		if (useMergePeekCursors || logCount > 1) {
			throw worker_removed();
		} else {
			return makeReference<ServerPeekCursor>(
			    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, ls.getPeekEnd(), false, false);
		}
	}

	if (begin >= ls.tLogs[bestSet]->startVersion) {
		TraceEvent("TLogPeekLocalBestOnly", dbgid)
		    .detail("Tag", tag.toString())
		    .detail("Begin", begin)
		    .detail("End", end)
		    .detail("BestSet", bestSet)
		    .detail("BestSetStart", ls.tLogs[bestSet]->startVersion)
		    .detail("LogId", ls.tLogs[bestSet]->logServers[ls.tLogs[bestSet]->bestLocationFor(tag)]->get().id());
		if (useMergePeekCursors) {
			int bestServer = ls.tLogs[bestSet]->bestLocationFor(tag);
			Optional<std::vector<uint16_t>> bestKnownLockedTLogIds;
			if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
				ls.resetBestServerIfNotLocked(bestSet, bestServer, end, ls.knownLockedTLogIds);
				ASSERT_WE_THINK(ls.knownLockedTLogIds.contains(bestSet));
				bestKnownLockedTLogIds = ls.knownLockedTLogIds[bestSet];
			}
			return makeReference<MergedPeekCursor>(ls.tLogs[bestSet]->logServers,
			                                       bestServer,
			                                       ls.tLogs[bestSet]->logServers.size() + 1 -
			                                           ls.tLogs[bestSet]->tLogReplicationFactor,
			                                       tag,
			                                       begin,
			                                       end,
			                                       true,
			                                       ls.tLogs[bestSet]->tLogLocalities,
			                                       ls.tLogs[bestSet]->tLogPolicy,
			                                       ls.tLogs[bestSet]->tLogReplicationFactor,
			                                       bestKnownLockedTLogIds);
		} else {
			return makeReference<ServerPeekCursor>(
			    ls.tLogs[bestSet]->logServers[ls.tLogs[bestSet]->bestLocationFor(tag)], tag, begin, end, false, false);
		}
	} else {
		std::vector<Reference<IReplayPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;

		if (ls.tLogs[bestSet]->startVersion < end) {
			TraceEvent("TLogPeekLocalAddingBest", dbgid)
			    .detail("Tag", tag.toString())
			    .detail("Begin", begin)
			    .detail("End", end)
			    .detail("BestSet", bestSet)
			    .detail("BestSetStart", ls.tLogs[bestSet]->startVersion)
			    .detail("LogId", ls.tLogs[bestSet]->logServers[ls.tLogs[bestSet]->bestLocationFor(tag)]->get().id());
			if (useMergePeekCursors) {
				int bestServer = ls.tLogs[bestSet]->bestLocationFor(tag);
				Optional<std::vector<uint16_t>> bestKnownLockedTLogIds;
				if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
					ls.resetBestServerIfNotLocked(bestSet, bestServer, end, ls.knownLockedTLogIds);
					ASSERT_WE_THINK(ls.knownLockedTLogIds.contains(bestSet));
					bestKnownLockedTLogIds = ls.knownLockedTLogIds[bestSet];
				}
				cursors.push_back(makeReference<MergedPeekCursor>(ls.tLogs[bestSet]->logServers,
				                                                  bestServer,
				                                                  ls.tLogs[bestSet]->logServers.size() + 1 -
				                                                      ls.tLogs[bestSet]->tLogReplicationFactor,
				                                                  tag,
				                                                  ls.tLogs[bestSet]->startVersion,
				                                                  end,
				                                                  true,
				                                                  ls.tLogs[bestSet]->tLogLocalities,
				                                                  ls.tLogs[bestSet]->tLogPolicy,
				                                                  ls.tLogs[bestSet]->tLogReplicationFactor,
				                                                  bestKnownLockedTLogIds));
			} else {
				cursors.push_back(makeReference<ServerPeekCursor>(
				    ls.tLogs[bestSet]->logServers[ls.tLogs[bestSet]->bestLocationFor(tag)],
				    tag,
				    ls.tLogs[bestSet]->startVersion,
				    end,
				    false,
				    false));
			}
		}
		Version lastBegin = ls.tLogs[bestSet]->startVersion;
		for (int i = 0; begin < lastBegin; i++) {
			if (i == ls.oldLogData.size()) {
				if (tag.locality == tagLocalityTxs && !cursors.empty()) {
					break;
				}
				TraceEvent("TLogPeekLocalDead", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end)
				    .detail("LastBegin", lastBegin)
				    .detail("OldLogDataSize", ls.oldLogData.size());
				throw worker_removed();
			}

			int bestOldSet = -1;
			logCount = 0;
			bool nextFoundSpecial = false;
			for (int t = 0; t < ls.oldLogData[i].tLogs.size(); t++) {
				if (!ls.oldLogData[i].tLogs[t]->logServers.empty() &&
				    ls.oldLogData[i].tLogs[t]->locality != tagLocalitySatellite) {
					logCount++;
				}
				if (!ls.oldLogData[i].tLogs[t]->logServers.empty() &&
				    (ls.oldLogData[i].tLogs[t]->locality == tagLocalitySpecial ||
				     ls.oldLogData[i].tLogs[t]->locality == peekLocality || peekLocality == tagLocalitySpecial)) {
					if (ls.oldLogData[i].tLogs[t]->locality == tagLocalitySpecial) {
						nextFoundSpecial = true;
					}
					if (foundSpecial && !ls.oldLogData[i].tLogs[t]->isLocal) {
						TraceEvent("TLogPeekLocalRemoteBeforeSpecial", dbgid)
						    .detail("Tag", tag.toString())
						    .detail("Begin", begin)
						    .detail("End", end)
						    .detail("LastBegin", lastBegin)
						    .detail("OldLogDataSize", ls.oldLogData.size())
						    .detail("Idx", i);
						throw worker_removed();
					}
					bestOldSet = t;
					break;
				}
			}

			if (bestOldSet == -1) {
				TraceEvent("TLogPeekLocalNoBestSet", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("End", end)
				    .detail("LastBegin", lastBegin)
				    .detail("OldLogDataSize", ls.oldLogData.size())
				    .detail("Idx", i)
				    .detail("LogRouterTags", ls.oldLogData[i].logRouterTags)
				    .detail("LogCount", logCount)
				    .detail("FoundSpecial", foundSpecial);
				if (ls.oldLogData[i].logRouterTags == 0 || logCount > 1 || foundSpecial) {
					throw worker_removed();
				}
				continue;
			}

			foundSpecial = nextFoundSpecial;

			Version thisBegin = std::max(ls.oldLogData[i].tLogs[bestOldSet]->startVersion, begin);
			if (thisBegin < lastBegin) {
				if (thisBegin < end) {
					TraceEvent("TLogPeekLocalAddingOldBest", dbgid)
					    .detail("Tag", tag.toString())
					    .detail("Begin", begin)
					    .detail("End", end)
					    .detail("BestOldSet", bestOldSet)
					    .detail("LogServers", ls.oldLogData[i].tLogs[bestOldSet]->logServerString())
					    .detail("ThisBegin", thisBegin)
					    .detail("LastBegin", lastBegin);
					// detail("LogId",
					// ls.oldLogData[i].tLogs[bestOldSet]->logServers[ls.tLogs[bestOldSet]->bestLocationFor( tag
					// )]->get().id());
					cursors.push_back(
					    makeReference<MergedPeekCursor>(ls.oldLogData[i].tLogs[bestOldSet]->logServers,
					                                    ls.oldLogData[i].tLogs[bestOldSet]->bestLocationFor(tag),
					                                    ls.oldLogData[i].tLogs[bestOldSet]->logServers.size() + 1 -
					                                        ls.oldLogData[i].tLogs[bestOldSet]->tLogReplicationFactor,
					                                    tag,
					                                    thisBegin,
					                                    std::min(lastBegin, end),
					                                    useMergePeekCursors,
					                                    ls.oldLogData[i].tLogs[bestOldSet]->tLogLocalities,
					                                    ls.oldLogData[i].tLogs[bestOldSet]->tLogPolicy,
					                                    ls.oldLogData[i].tLogs[bestOldSet]->tLogReplicationFactor));
					epochEnds.emplace_back(std::min(lastBegin, end));
				}
				lastBegin = thisBegin;
			}
		}

		return makeReference<ReplayMultiCursor>(cursors, epochEnds, tag.locality != tagLocalityCDC);
	}
}

Reference<IPeekCursor> LogSystemConsumer::peekTxs(UID dbgid,
                                                  Version begin,
                                                  int8_t peekLocality,
                                                  Version localEnd,
                                                  bool canDiscardPopped) {
	auto& ls = *logSystem;
	Version end = ls.getEnd();
	if (ls.tLogs.empty()) {
		TraceEvent("TLogPeekTxsNoLogs", dbgid).log();
		return makeReference<ServerPeekCursor>(
		    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), invalidTag, begin, end, false, false);
	}
	TraceEvent("TLogPeekTxs", dbgid)
	    .detail("Begin", begin)
	    .detail("End", end)
	    .detail("LocalEnd", localEnd)
	    .detail("PeekLocality", peekLocality)
	    .detail("CanDiscardPopped", canDiscardPopped);

	int maxTxsTags = ls.txsTags;
	for (auto& it : ls.oldLogData) {
		maxTxsTags = std::max<int>(maxTxsTags, it.txsTags);
	}

	if (peekLocality < 0 || localEnd == invalidVersion || localEnd <= begin) {
		std::vector<Reference<IReplayPeekCursor>> cursors;
		cursors.reserve(maxTxsTags);
		for (int i = 0; i < maxTxsTags; i++) {
			cursors.push_back(peekAll(dbgid, begin, end, Tag(tagLocalityTxs, i), true));
		}

		return makeReference<BufferedCursor>(cursors, begin, end, false, canDiscardPopped);
	}

	try {
		if (localEnd >= end) {
			std::vector<Reference<IReplayPeekCursor>> cursors;
			cursors.reserve(maxTxsTags);
			for (int i = 0; i < maxTxsTags; i++) {
				cursors.push_back(peekLocal(dbgid, Tag(tagLocalityTxs, i), begin, end, true, peekLocality));
			}

			return makeReference<BufferedCursor>(cursors, begin, end, false, canDiscardPopped);
		}

		std::vector<Reference<IPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;

		cursors.resize(2);

		std::vector<Reference<IReplayPeekCursor>> localCursors;
		std::vector<Reference<IReplayPeekCursor>> allCursors;
		for (int i = 0; i < maxTxsTags; i++) {
			localCursors.push_back(peekLocal(dbgid, Tag(tagLocalityTxs, i), begin, localEnd, true, peekLocality));
			allCursors.push_back(peekAll(dbgid, localEnd, end, Tag(tagLocalityTxs, i), true));
		}

		cursors[1] = makeReference<BufferedCursor>(localCursors, begin, localEnd, false, canDiscardPopped);
		cursors[0] = makeReference<BufferedCursor>(allCursors, localEnd, end, false, false);
		epochEnds.emplace_back(localEnd);

		return makeReference<MultiCursor>(cursors, epochEnds);
	} catch (Error& e) {
		if (e.code() == error_code_worker_removed) {
			std::vector<Reference<IReplayPeekCursor>> cursors;
			cursors.reserve(maxTxsTags);
			for (int i = 0; i < maxTxsTags; i++) {
				cursors.push_back(peekAll(dbgid, begin, end, Tag(tagLocalityTxs, i), true));
			}

			return makeReference<BufferedCursor>(cursors, begin, end, false, canDiscardPopped);
		}
		throw;
	}
}

Reference<IReplayPeekCursor> LogSystemConsumer::peekSingle(UID dbgid,
                                                           Version begin,
                                                           Tag tag,
                                                           std::vector<std::pair<Version, Tag>> history) {
	auto& ls = *logSystem;
	auto peekTag = [&](Tag readTag, Version readBegin, Version readEnd) -> Reference<IReplayPeekCursor> {
		if (readTag.locality == tagLocalityCDC) {
			return peekAll(dbgid, readBegin, readEnd, readTag, false);
		}
		return peekLocal(dbgid, readTag, readBegin, readEnd, false);
	};
	while (!history.empty() && begin >= history.back().first) {
		history.pop_back();
	}

	if (history.empty()) {
		TraceEvent("TLogPeekSingleNoHistory", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
		return peekTag(tag, begin, ls.getPeekEnd());
	} else {
		std::vector<Reference<IReplayPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;

		TraceEvent("TLogPeekSingleAddingLocal", dbgid).detail("Tag", tag.toString()).detail("Begin", history[0].first);
		cursors.push_back(peekTag(tag, history[0].first, ls.getPeekEnd()));

		for (int i = 0; i < history.size(); i++) {
			TraceEvent("TLogPeekSingleAddingOld", dbgid)
			    .detail("Tag", tag.toString())
			    .detail("HistoryTag", history[i].second.toString())
			    .detail("Begin", i + 1 == history.size() ? begin : std::max(history[i + 1].first, begin))
			    .detail("End", history[i].first);
			cursors.push_back(peekTag(history[i].second,
			                          i + 1 == history.size() ? begin : std::max(history[i + 1].first, begin),
			                          history[i].first));
			epochEnds.emplace_back(history[i].first);
		}

		return makeReference<ReplayMultiCursor>(cursors, epochEnds, tag.locality != tagLocalityCDC);
	}
}

Reference<IReplayPeekCursor> LogSystemConsumer::peekLogRouter(
    UID dbgid,
    Version begin,
    Tag tag,
    bool useSatellite,
    Optional<Version> end,
    const Optional<std::map<uint8_t, std::vector<uint16_t>>>& knownStoppedTLogIds) {
	auto& ls = *logSystem;
	bool found = false;
	if (!end.present()) {
		end = std::numeric_limits<Version>::max();
	} else {
		end = end.get() + 1; // The last version is exclusive to the cursor's desired range
	}

	for (const auto& log : ls.tLogs) {
		found = log->hasLogRouter(dbgid) || log->hasBackupWorker(dbgid);
		if (found) {
			break;
		}
	}
	if (found) {
		if (ls.stopped) {
			std::vector<Reference<LogSet>> localSets;
			// indexes into "localSets"
			int bestPrimarySet = 0;
			int bestSatelliteSet = -1;
			// indexes into "ls.tLogs"
			int logIdx = 0;
			int bestPrimarySetIdx = -1;
			int bestSatelliteSetIdx = -1;
			for (auto& log : ls.tLogs) {
				if (log->isLocal && !log->logServers.empty()) {
					TraceEvent("TLogPeekLogRouterLocalSet", dbgid)
					    .detail("Tag", tag.toString())
					    .detail("Begin", begin)
					    .detail("LogServers", log->logServerString());
					localSets.push_back(log);
					if (log->locality == tagLocalitySatellite) {
						bestSatelliteSet = localSets.size() - 1;
						bestSatelliteSetIdx = logIdx;
					} else {
						bestPrimarySet = localSets.size() - 1;
						bestPrimarySetIdx = logIdx;
					}
				}
				logIdx++;
			}
			int bestSet = bestPrimarySet;
			int bestSetIdx = bestPrimarySetIdx;
			if (useSatellite && bestSatelliteSet != -1 && ls.tLogs[bestSatelliteSet]->tLogVersion >= TLogVersion::V4) {
				bestSet = bestSatelliteSet;
				bestSetIdx = bestSatelliteSetIdx;
			}

			int bestServer = localSets[bestSet]->bestLocationFor(tag);
			Optional<std::vector<uint16_t>> bestKnownStoppedTLogIds;
			if (SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
				ls.resetBestServerIfNotLocked(bestSetIdx, bestServer, end, knownStoppedTLogIds);
				ASSERT_WE_THINK(!knownStoppedTLogIds.present() || knownStoppedTLogIds.get().contains(bestSetIdx));
				bestKnownStoppedTLogIds = knownStoppedTLogIds.get().at(bestSetIdx);
			}

			TraceEvent("TLogPeekLogRouterSets", dbgid).detail("Tag", tag.toString()).detail("Begin", begin);
			// FIXME: do this merge on one of the logs in the other data center to avoid sending multiple copies
			// across the WAN
			return makeReference<SetPeekCursor>(
			    localSets, bestSet, bestServer, tag, begin, end.get(), true, bestKnownStoppedTLogIds);
		} else {
			int bestPrimarySet = -1;
			int bestSatelliteSet = -1;
			for (int i = 0; i < ls.tLogs.size(); i++) {
				const auto& log = ls.tLogs[i];
				if (!log->logServers.empty() && log->isLocal) {
					if (log->locality == tagLocalitySatellite) {
						bestSatelliteSet = i;
						break;
					} else {
						if (bestPrimarySet == -1)
							bestPrimarySet = i;
					}
				}
			}
			int bestSet = bestPrimarySet;
			if (useSatellite && bestSatelliteSet != -1 && ls.tLogs[bestSatelliteSet]->tLogVersion >= TLogVersion::V4) {
				bestSet = bestSatelliteSet;
			}
			const auto& log = ls.tLogs[bestSet];
			TraceEvent("TLogPeekLogRouterBestOnly", dbgid)
			    .detail("Tag", tag.toString())
			    .detail("Begin", begin)
			    .detail("LogId", log->logServers[log->bestLocationFor(tag)]->get().id());
			return makeReference<ServerPeekCursor>(
			    log->logServers[log->bestLocationFor(tag)], tag, begin, end.get(), false, true);
		}
	}
	bool firstOld = true;
	for (const auto& old : ls.oldLogData) {
		found = false;
		for (const auto& log : old.tLogs) {
			found = log->hasLogRouter(dbgid) || log->hasBackupWorker(dbgid);
			if (found) {
				break;
			}
		}
		if (found) {
			Version oldEnd = firstOld && ls.recoveredAt.present() ? ls.recoveredAt.get() + 1 : old.epochEnd;
			if (begin >= oldEnd) {
				// The LogRouter has already advanced past this old epoch's range.
				// Skip it without creating a SetPeekCursor with begin >= end (which
				// would be born exhausted and hang).
				TraceEvent("TLogPeekLogRouterOldRangeEmpty", dbgid)
				    .detail("Tag", tag.toString())
				    .detail("Begin", begin)
				    .detail("OldEnd", oldEnd)
				    .detail("FirstOld", firstOld);
				firstOld = false;
				continue;
			}

			int bestPrimarySet = 0;
			int bestSatelliteSet = -1;
			std::vector<Reference<LogSet>> localSets;
			for (auto& log : old.tLogs) {
				if (log->isLocal && !log->logServers.empty()) {
					TraceEvent("TLogPeekLogRouterOldLocalSet", dbgid)
					    .detail("Tag", tag.toString())
					    .detail("Begin", begin)
					    .detail("LogServers", log->logServerString());
					localSets.push_back(log);
					if (log->locality == tagLocalitySatellite) {
						bestSatelliteSet = localSets.size() - 1;
					} else {
						bestPrimarySet = localSets.size() - 1;
					}
				}
			}
			int bestSet = bestPrimarySet;
			if (useSatellite && bestSatelliteSet != -1 && old.tLogs[bestSatelliteSet]->tLogVersion >= TLogVersion::V4) {
				bestSet = bestSatelliteSet;
			}

			TraceEvent("TLogPeekLogRouterOldSets", dbgid)
			    .detail("Tag", tag.toString())
			    .detail("Begin", begin)
			    .detail("OldEpoch", old.epochEnd)
			    .detail("RecoveredAt", ls.recoveredAt.present() ? ls.recoveredAt.get() : -1)
			    .detail("FirstOld", firstOld);
			// FIXME: do this merge on one of the logs in the other data center to avoid sending multiple copies
			// across the WAN
			return makeReference<SetPeekCursor>(
			    localSets, bestSet, localSets[bestSet]->bestLocationFor(tag), tag, begin, oldEnd, true);
		}
		firstOld = false;
	}
	// No current or historical log set can serve this router at the requested begin version.
	// Represent that state as an already-exhausted cursor so callers can wait for the
	// log system to change instead of repeatedly treating the placeholder like a stuck peek.
	return makeReference<ServerPeekCursor>(
	    Reference<AsyncVar<OptionalInterface<TLogInterface>>>(), tag, begin, begin, false, false);
}

void LogSystemConsumer::popLogRouter(Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality) {
	auto& ls = *logSystem;
	if (!upTo)
		return;

	Version lastGenerationStartVersion = LogSystem::getMaxLocalStartVersion(ls.tLogs);
	// Pop boundaries are exclusive. At a generation handoff, the remote TLog can durably reach
	// startVersion - 1 while the current log router still needs that pop to advance flow control.
	// Forward exactly that predecessor without making earlier generations eligible.
	if (upTo >= lastGenerationStartVersion ||
	    (lastGenerationStartVersion > 0 && upTo == lastGenerationStartVersion - 1)) {
		for (auto& t : ls.tLogs) {
			if (t->locality == popLocality) {
				for (auto& log : t->logRouters) {
					Version prev = ls.outstandingPops[std::make_pair(log->get().id(), tag)].first;
					if (prev < upTo) {
						ls.outstandingPops[std::make_pair(log->get().id(), tag)] =
						    std::make_pair(upTo, durableKnownCommittedVersion);
					}
					if (prev == 0) {
						ls.popActors.add(ls.popFromLog(log,
						                               tag,
						                               /*delayBeforePop=*/0.0,
						                               /*popLogRouter=*/true)); // Fast pop time because log routers can
						                                                        // only hold 5 seconds of data.
					}
				}
			}
		}
	}

	Version nextGenerationStartVersion = lastGenerationStartVersion;
	for (const auto& old : ls.oldLogData) {
		Version generationStartVersion = LogSystem::getMaxLocalStartVersion(old.tLogs);
		if (generationStartVersion <= upTo) {
			for (auto& t : old.tLogs) {
				if (t->locality == popLocality) {
					for (auto& log : t->logRouters) {
						auto logRouterIdTagPair = std::make_pair(log->get().id(), tag);

						// We pop the log router only if the popped version is within this generation's version range.
						// That is between the current generation's start version and the next generation's start
						// version.
						if (ls.logRouterLastPops.find(logRouterIdTagPair) == ls.logRouterLastPops.end() ||
						    ls.logRouterLastPops[logRouterIdTagPair] < nextGenerationStartVersion) {
							Version prev = ls.outstandingPops[logRouterIdTagPair].first;
							if (prev < upTo) {
								ls.outstandingPops[logRouterIdTagPair] =
								    std::make_pair(upTo, durableKnownCommittedVersion);
							}
							if (prev == 0) {
								ls.popActors.add(
								    ls.popFromLog(log, tag, /*delayBeforePop=*/0.0, /*popLogRouter=*/true));
							}
						}
					}
				}
			}
		}

		nextGenerationStartVersion = generationStartVersion;
	}
}

void LogSystemConsumer::popTxs(Version upTo, int8_t popLocality) {
	auto& ls = *logSystem;
	for (int i = 0; i < ls.txsTags; i++) {
		pop(upTo, Tag(tagLocalityTxs, i), 0, popLocality);
	}
}

void LogSystemConsumer::pop(Version upTo, Tag tag, Version durableKnownCommittedVersion, int8_t popLocality) {
	auto& ls = *logSystem;
	if (upTo <= 0)
		return;
	if (tag.locality == tagLocalityRemoteLog) {
		popLogRouter(upTo, tag, durableKnownCommittedVersion, popLocality);
		return;
	}
	for (auto& t : ls.tLogs) {
		if (shouldPopFromLogSet(t, tag, popLocality)) {
			for (auto& log : t->logServers) {
				Version prev = ls.outstandingPops[std::make_pair(log->get().id(), tag)].first;
				if (prev < upTo) {
					// update pop version for popFromLog actor
					ls.outstandingPops[std::make_pair(log->get().id(), tag)] =
					    std::make_pair(upTo, durableKnownCommittedVersion);
				}
				if (prev == 0) {
					// pop tag from log upto version defined in ls.outstandingPops[].first
					ls.popActors.add(ls.popFromLog(log, tag, SERVER_KNOBS->POP_FROM_LOG_DELAY, /*popLogRouter=*/false));
				}
			}
		}
	}
}

Future<Void> LogSystemConsumer::waitForPopped(Version upTo, Tag tag, int8_t popLocality) {
	while (true) {
		std::vector<Future<Version>> poppedFutures;
		for (auto& t : logSystem->tLogs) {
			if (shouldPopFromLogSet(t, tag, popLocality)) {
				for (auto& log : t->logServers) {
					poppedFutures.push_back(LogSystem::getPoppedFromTLog(log, tag));
				}
			}
		}
		if (poppedFutures.empty()) {
			co_return;
		}

		std::vector<Version> poppedVersions = co_await getAll(poppedFutures);
		if (std::all_of(poppedVersions.begin(), poppedVersions.end(), [upTo](Version poppedVersion) {
			    return poppedVersion >= upTo;
		    })) {
			co_return;
		}
		co_await delay(0.01, TaskPriority::TLogPop);
	}
}

Future<Version> LogSystemConsumer::getTxsPoppedVersion() {
	auto& ls = *logSystem;
	return ls.getPoppedTxs();
}

Version LogSystemConsumer::getEnd() const {
	return logSystem->getEnd();
}

Version LogSystemConsumer::getPeekEnd() const {
	return logSystem->getPeekEnd();
}

Tag LogSystemConsumer::getPseudoPopTag(Tag tag, ProcessClass::ClassType type) const {
	return logSystem->getPseudoPopTag(tag, type);
}
