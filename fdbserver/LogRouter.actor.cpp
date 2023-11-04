/*
 * LogRouter.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/Atomic.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/TLogInterface.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/CodeProbe.h"
#include "flow/Histogram.h"
#include "flow/Trace.h"
#include "flow/network.h"
#include "flow/DebugTrace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct LogRouterData {
	struct TagData : NonCopyable, public ReferenceCounted<TagData> {
		std::deque<std::pair<Version, LengthPrefixedStringRef>> version_messages;
		Version popped;
		Version durableKnownCommittedVersion;
		Tag tag;

		TagData(Tag tag, Version popped, Version durableKnownCommittedVersion)
		  : popped(popped), durableKnownCommittedVersion(durableKnownCommittedVersion), tag(tag) {}

		TagData(TagData&& r) noexcept
		  : version_messages(std::move(r.version_messages)), popped(r.popped),
		    durableKnownCommittedVersion(r.durableKnownCommittedVersion), tag(r.tag) {}
		void operator=(TagData&& r) noexcept {
			version_messages = std::move(r.version_messages);
			tag = r.tag;
			popped = r.popped;
			durableKnownCommittedVersion = r.durableKnownCommittedVersion;
		}

		// Erase messages not needed to update *from* versions >= before (thus, messages with toversion <= before)
		ACTOR Future<Void> eraseMessagesBefore(TagData* self,
		                                       Version before,
		                                       LogRouterData* tlogData,
		                                       TaskPriority taskID) {
			while (!self->version_messages.empty() && self->version_messages.front().first < before) {
				Version version = self->version_messages.front().first;

				while (!self->version_messages.empty() && self->version_messages.front().first == version) {
					self->version_messages.pop_front();
				}

				wait(yield(taskID));
			}

			return Void();
		}

		Future<Void> eraseMessagesBefore(Version before, LogRouterData* tlogData, TaskPriority taskID) {
			return eraseMessagesBefore(this, before, tlogData, taskID);
		}
	};

	const UID dbgid;
	Reference<AsyncVar<Reference<ILogSystem>>> logSystem;
	Future<Void> logSystemChanged = Void();
	Optional<UID> primaryPeekLocation;
	NotifiedVersion version; // The largest version at which the log router has peeked mutations
	                         // from satellite tLog or primary tLogs.
	NotifiedVersion minPopped; // The minimum version among all tags that has been popped by remote tLogs.
	const Version startVersion;
	Version minKnownCommittedVersion; // The minimum durable version among all LRs.
	                                  // A LR's durable version is the maximum version of mutations that have been
	                                  // popped by remote tLog.
	Version poppedVersion;
	Deque<std::pair<Version, Standalone<VectorRef<uint8_t>>>> messageBlocks;
	Tag routerTag;
	bool allowPops;
	LogSet logSet;
	bool foundEpochEnd; // Cluster is not fully recovered yet. LR has to handle recovery
	double waitForVersionTime = 0; // The total amount of time LR waits for remote tLog to peek and pop its data.
	double maxWaitForVersionTime = 0; // The max one-instance wait time when LR must wait for remote tLog to pop data.
	double getMoreTime = 0; // The total amount of time LR waits for satellite tLog's data to become available.
	double maxGetMoreTime = 0; // The max wait time LR spent in a pull-data-request to satellite tLog.
	int64_t generation = -1;
	Reference<Histogram> peekLatencyDist;

	struct PeekTrackerData {
		std::map<int, Promise<std::pair<Version, bool>>> sequence_version;
		double lastUpdate;
	};

	std::map<UID, PeekTrackerData> peekTracker;

	CounterCollection cc;
	Counter getMoreCount; // Increase by 1 when LR tries to pull data from satellite tLog.
	Counter
	    getMoreBlockedCount; // Increase by 1 if data is not available when LR tries to pull data from satellite tLog.
	Future<Void> logger;
	Reference<EventCacheHolder> eventCacheHolder;
	int activePeekStreams = 0;

	std::vector<Reference<TagData>> tag_data; // we only store data for the remote tag locality

	Reference<TagData> getTagData(Tag tag) {
		ASSERT(tag.locality == tagLocalityRemoteLog);
		if (tag.id >= tag_data.size()) {
			tag_data.resize(tag.id + 1);
		}
		return tag_data[tag.id];
	}

	// only callable after getTagData returns a null reference
	Reference<TagData> createTagData(Tag tag, Version popped, Version knownCommittedVersion) {
		auto newTagData = makeReference<TagData>(tag, popped, knownCommittedVersion);
		tag_data[tag.id] = newTagData;
		return newTagData;
	}

	LogRouterData(UID dbgid, const InitializeLogRouterRequest& req)
	  : dbgid(dbgid), logSystem(new AsyncVar<Reference<ILogSystem>>()), version(req.startVersion - 1), minPopped(0),
	    startVersion(req.startVersion), minKnownCommittedVersion(0), poppedVersion(0), routerTag(req.routerTag),
	    allowPops(false), foundEpochEnd(false), generation(req.recoveryCount),
	    peekLatencyDist(Histogram::getHistogram("LogRouter"_sr, "PeekTLogLatency"_sr, Histogram::Unit::milliseconds)),
	    cc("LogRouter", dbgid.toString()), getMoreCount("GetMoreCount", cc),
	    getMoreBlockedCount("GetMoreBlockedCount", cc) {
		// setup just enough of a logSet to be able to call getPushLocations
		logSet.logServers.resize(req.tLogLocalities.size());
		logSet.tLogPolicy = req.tLogPolicy;
		logSet.locality = req.locality;
		logSet.updateLocalitySet(req.tLogLocalities);

		for (int i = 0; i < req.tLogLocalities.size(); i++) {
			Tag tag(tagLocalityRemoteLog, i);
			auto tagData = getTagData(tag);
			if (!tagData) {
				tagData = createTagData(tag, 0, 0);
			}
		}

		eventCacheHolder = makeReference<EventCacheHolder>(dbgid.shortString() + ".PeekLocation");

		// FetchedVersions: How many version of mutations buffered at LR and have not been popped by remote tLogs
		specialCounter(cc, "Version", [this]() { return this->version.get(); });
		specialCounter(cc, "MinPopped", [this]() { return this->minPopped.get(); });
		// TODO: Add minPopped locality and minPoppedId, similar as tLog Metrics
		specialCounter(cc, "FetchedVersions", [this]() {
			return std::max<Version>(0,
			                         std::min<Version>(SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS,
			                                           this->version.get() - this->minPopped.get()));
		});
		specialCounter(cc, "MinKnownCommittedVersion", [this]() { return this->minKnownCommittedVersion; });
		specialCounter(cc, "PoppedVersion", [this]() { return this->poppedVersion; });
		specialCounter(cc, "FoundEpochEnd", [this]() { return this->foundEpochEnd; });
		specialCounter(cc, "WaitForVersionMS", [this]() {
			double val = this->waitForVersionTime;
			this->waitForVersionTime = 0;
			return int64_t(1000 * val);
		});
		specialCounter(cc, "WaitForVersionMaxMS", [this]() {
			double val = this->maxWaitForVersionTime;
			this->maxWaitForVersionTime = 0;
			return int64_t(1000 * val);
		});
		specialCounter(cc, "GetMoreMS", [this]() {
			double val = this->getMoreTime;
			this->getMoreTime = 0;
			return int64_t(1000 * val);
		});
		specialCounter(cc, "GetMoreMaxMS", [this]() {
			double val = this->maxGetMoreTime;
			this->maxGetMoreTime = 0;
			return int64_t(1000 * val);
		});
		specialCounter(cc, "Generation", [this]() { return this->generation; });
		specialCounter(cc, "ActivePeekStreams", [this]() { return this->activePeekStreams; });
		logger = cc.traceCounters("LogRouterMetrics",
		                          dbgid,
		                          SERVER_KNOBS->WORKER_LOGGING_INTERVAL,
		                          "LogRouterMetrics",
		                          [this](TraceEvent& te) {
			                          te.detail("PrimaryPeekLocation", this->primaryPeekLocation);
			                          te.detail("RouterTag", this->routerTag.toString());
		                          });
	}
};

void commitMessages(LogRouterData* self, Version version, const std::vector<TagsAndMessage>& taggedMessages) {
	if (!taggedMessages.size()) {
		return;
	}

	int msgSize = 0;
	for (const auto& i : taggedMessages) {
		msgSize += i.message.size();
	}

	// Grab the last block in the blocks list so we can share its arena
	// We pop all of the elements of it to create a "fresh" vector that starts at the end of the previous vector
	Standalone<VectorRef<uint8_t>> block;
	if (self->messageBlocks.empty()) {
		block = Standalone<VectorRef<uint8_t>>();
		block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, msgSize));
	} else {
		block = self->messageBlocks.back().second;
	}

	block.pop_front(block.size());

	for (const auto& msg : taggedMessages) {
		if (msg.message.size() > block.capacity() - block.size()) {
			self->messageBlocks.emplace_back(version, block);
			block = Standalone<VectorRef<uint8_t>>();
			block.reserve(block.arena(), std::max<int64_t>(SERVER_KNOBS->TLOG_MESSAGE_BLOCK_BYTES, msgSize));
		}

		block.append(block.arena(), msg.message.begin(), msg.message.size());
		for (const auto& tag : msg.tags) {
			auto tagData = self->getTagData(tag);
			if (!tagData) {
				tagData = self->createTagData(tag, 0, 0);
			}

			if (version >= tagData->popped) {
				tagData->version_messages.emplace_back(
				    version, LengthPrefixedStringRef((uint32_t*)(block.end() - msg.message.size())));
				if (tagData->version_messages.back().second.expectedSize() > SERVER_KNOBS->MAX_MESSAGE_SIZE) {
					TraceEvent(SevWarnAlways, "LargeMessage")
					    .detail("Size", tagData->version_messages.back().second.expectedSize());
				}
				TraceEvent("LogRouterPeekCommitMessage", self->dbgid)
				    .setMaxFieldLength(-1)
				    .setMaxEventLength(-1)
				    .detail("Tag", tag)
				    .detail("Tags", msg.tags)
				    .detail("Message",
				            LengthPrefixedStringRef((uint32_t*)(block.end() - msg.message.size())).toStringRef())
				    .detail("Version", version);
			}
		}

		msgSize -= msg.message.size();
	}
	self->messageBlocks.emplace_back(version, block);
}

ACTOR Future<Void> waitForVersion(LogRouterData* self, Version ver) {
	// The only time the log router should allow a gap in versions larger than MAX_READ_TRANSACTION_LIFE_VERSIONS is
	// when processing epoch end. Since one set of log routers is created per generation of transaction logs, the gap
	// caused by epoch end will be within MAX_VERSIONS_IN_FLIGHT of the log routers start version.
	state double startTime = now();
	if (self->version.get() < self->startVersion) {
		// Log router needs to wait for remote tLogs to process data, whose version is less than self->startVersion,
		// before the log router can pull more data (i.e., data after self->startVersion) from satellite tLog;
		// This prevents LR from getting OOM due to it pulls too much data from satellite tLog at once;
		// Note: each commit writes data to both primary tLog and satellite tLog. Satellite tLog can be viewed as
		//       a part of primary tLogs.
		if (ver > self->startVersion) {
			self->version.set(self->startVersion);
			// Wait for remote tLog to peek and pop from LR,
			// so that LR's minPopped version can increase to self->startVersion
			wait(self->minPopped.whenAtLeast(self->version.get()));
		}
		self->waitForVersionTime += now() - startTime;
		self->maxWaitForVersionTime = std::max(self->maxWaitForVersionTime, now() - startTime);
		return Void();
	}
	if (!self->foundEpochEnd) {
		// Similar to proxy that does not keep more than MAX_READ_TRANSACTION_LIFE_VERSIONS transactions oustanding;
		// Log router does not keep more than MAX_READ_TRANSACTION_LIFE_VERSIONS transactions outstanding because
		// remote SS cannot roll back to more than MAX_READ_TRANSACTION_LIFE_VERSIONS ago.
		wait(self->minPopped.whenAtLeast(
		    std::min(self->version.get(), ver - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS)));
	} else {
		while (self->minPopped.get() + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS < ver) {
			if (self->minPopped.get() + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS > self->version.get()) {
				self->version.set(self->minPopped.get() + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS);
				wait(yield(TaskPriority::TLogCommit));
			} else {
				wait(self->minPopped.whenAtLeast((self->minPopped.get() + 1)));
			}
		}
	}
	if (ver >= self->startVersion + SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT) {
		self->foundEpochEnd = true;
	}
	self->waitForVersionTime += now() - startTime;
	self->maxWaitForVersionTime = std::max(self->maxWaitForVersionTime, now() - startTime);
	return Void();
}

ACTOR Future<Reference<ILogSystem::IPeekCursor>> getPeekCursorData(LogRouterData* self,
                                                                   Reference<ILogSystem::IPeekCursor> r,
                                                                   Version startVersion) {
	state Version inputCursorVersion = 0;
	if (r) {
		inputCursorVersion = r->version().version;
	}
	state Reference<ILogSystem::IPeekCursor> result = r;
	state bool useSatellite = SERVER_KNOBS->LOG_ROUTER_PEEK_FROM_SATELLITES_PREFERRED;
	loop {
		Future<Void> getMoreF = Never();
		if (result) {
			TraceEvent("LogRouterPeekCursorDataBeforeGetMore", self->dbgid)
			    .detail("Version", self->version.get())
			    .detail("StartVersion", startVersion)
			    .detail("RouterTag", self->routerTag)
			    .detail("UseSatellite", useSatellite)
			    .detail("InputCursorVersion", inputCursorVersion)
			    .detail("CursorVersion", result->version().version);
			getMoreF = result->getMore(TaskPriority::TLogCommit);
			++self->getMoreCount;
			if (!getMoreF.isReady()) {
				++self->getMoreBlockedCount;
			}
			TraceEvent("LogRouterPeekCursorDataAfterGetMore", self->dbgid)
			    .detail("Version", self->version.get())
			    .detail("StartVersion", startVersion)
			    .detail("RouterTag", self->routerTag)
			    .detail("UseSatellite", useSatellite)
			    .detail("InputCursorVersion", inputCursorVersion)
			    .detail("CursorVersion", result->version().version);
		}
		state double startTime = now();
		choose {
			when(wait(getMoreF)) {
				double peekTime = now() - startTime;
				self->peekLatencyDist->sampleSeconds(peekTime);
				self->getMoreTime += peekTime;
				self->maxGetMoreTime = std::max(self->maxGetMoreTime, peekTime);
				TraceEvent("LogRouterPeekCursorDataReturnCursor", self->dbgid)
				    .detail("LogID", result->getPrimaryPeekLocation())
				    .detail("Version", self->version.get())
				    .detail("StartVersion", startVersion)
				    .detail("RouterTag", self->routerTag)
				    .detail("UseSatellite", useSatellite)
				    .detail("InputCursorVersion", inputCursorVersion)
				    .detail("CursorVersion", result->version().version);
				return result;
			}
			when(wait(self->logSystemChanged)) {
				if (self->logSystem->get()) {
					result =
					    self->logSystem->get()->peekLogRouter(self->dbgid, startVersion, self->routerTag, useSatellite);
					self->primaryPeekLocation = result->getPrimaryPeekLocation();
					TraceEvent("LogRouterPeekLocation1", self->dbgid)
					    .detail("LogID", result->getPrimaryPeekLocation())
					    .detail("Version", self->version.get())
					    .detail("StartVersion", startVersion)
					    .detail("RouterTag", self->routerTag)
					    .detail("UseSatellite", useSatellite)
					    .detail("InputCursorVersion", inputCursorVersion)
					    .detail("CursorVersion", result->version().version)
					    .trackLatest(self->eventCacheHolder->trackingKey);
				} else {
					result = Reference<ILogSystem::IPeekCursor>();
				}
				self->logSystemChanged = self->logSystem->onChange();
			}
			when(wait(result ? delay(SERVER_KNOBS->LOG_ROUTER_PEEK_SWITCH_DC_TIME) : Never())) {
				// Peek has become stuck for a while, trying switching between primary DC and satellite
				CODE_PROBE(true, "Detect log router slow peeks");
				TraceEvent(SevWarnAlways, "LogRouterSlowPeek", self->dbgid).detail("NextTrySatellite", !useSatellite);
				useSatellite = !useSatellite;
				result =
				    self->logSystem->get()->peekLogRouter(self->dbgid, startVersion, self->routerTag, useSatellite);
				self->primaryPeekLocation = result->getPrimaryPeekLocation();
				TraceEvent("LogRouterPeekLocation2", self->dbgid)
				    .detail("LogID", result->getPrimaryPeekLocation())
				    .detail("Version", self->version.get())
				    .detail("StartVersion", startVersion)
				    .trackLatest(self->eventCacheHolder->trackingKey);
			}
		}
	}
}

// Log router (LR) asynchronously pull data from satellite tLogs (preferred) or primary tLogs at tag (self->routerTag)
// for the version range from the LR's current version (exclusive) to its epoch's end version or recovery version.
ACTOR Future<Void> pullAsyncData(LogRouterData* self) {
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = self->version.get() + 1;
	state Version lastVer = 0;
	state std::vector<int> tags; // an optimization to avoid reallocating vector memory in every loop

	loop {
		Reference<ILogSystem::IPeekCursor> _r = wait(getPeekCursorData(self, r, tagAt));
		r = _r;

		self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, r->getMinKnownCommittedVersion());

		state Version verBegin = 0;
		state Version verEnd = 0;
		state Version ver = 0;
		state std::vector<TagsAndMessage> messages;
		state Arena arena;
		state Version maxVersion = self->version.get();
		while (true) {
			state bool foundMessage = r->hasMessage();
			/*TraceEvent("LogRouterPeekDecideTagCursorVersion", self->dbgid)
			    .detail("FoundMessage", foundMessage)
			    .detail("Ver", ver)
			    .detail("CursorVersion", r->version().version);*/
			if (!foundMessage || r->version().version != ver) {
				if (r->version().version > verEnd) {
					verEnd = r->version().version;
				}
				if (verBegin == 0 || r->version().version < verBegin) {
					verBegin = r->version().version;
				}
				ASSERT(r->version().version > lastVer);
				if (ver) {
					wait(waitForVersion(self, ver));

					commitMessages(self, ver, messages);
					self->version.set(ver);
					wait(yield(TaskPriority::TLogCommit));
					//TraceEvent("LogRouterVersion").detail("Ver",ver);
				}
				lastVer = ver;
				ver = r->version().version;
				messages.clear();
				arena = Arena();

				if (!foundMessage) {
					ver--; // ver is the next possible version we will get data for
					if (ver > self->version.get() && ver >= r->popped()) {
						wait(waitForVersion(self, ver));

						self->version.set(ver);
						wait(yield(TaskPriority::TLogCommit));
					}
					break;
				}
			}

			TagsAndMessage tagAndMsg;
			tagAndMsg.message = r->getMessageWithTags();
			tags.clear();
			self->logSet.getPushLocations(r->getTags(), tags, 0);
			TraceEvent("LogRouterPeekDecideTag", self->dbgid)
			    .setMaxFieldLength(-1)
			    .setMaxEventLength(-1)
			    .detail("TagAt", tagAt)
			    .detail("OldTags", r->getTags())
			    .detail("NewTags", tags)
			    .detail("Message", tagAndMsg.message)
			    .detail("VersionBegin", verBegin)
			    .detail("VersionEnd", verEnd)
			    .detail("MaxVersion", maxVersion)
			    .detail("CurrentMaxVersion", self->version.get())
			    .detail("MinKnownCommittedVersion", self->minKnownCommittedVersion)
			    .detail("CursorVersionRaw", r->version().toString())
			    .detail("CursorVersion", r->version().version);
			tagAndMsg.tags.reserve(arena, tags.size());
			for (const auto& t : tags) {
				tagAndMsg.tags.push_back(arena, Tag(tagLocalityRemoteLog, t));
			}
			messages.push_back(std::move(tagAndMsg));

			r->nextMessage();
		}

		tagAt = std::max(r->version().version, self->version.get() + 1);
	}
}

std::deque<std::pair<Version, LengthPrefixedStringRef>>& get_version_messages(LogRouterData* self, Tag tag) {
	auto tagData = self->getTagData(tag);
	if (!tagData) {
		static std::deque<std::pair<Version, LengthPrefixedStringRef>> empty;
		return empty;
	}
	return tagData->version_messages;
};

void peekMessagesFromMemory(LogRouterData* self, Tag tag, Version begin, BinaryWriter& messages, Version& endVersion) {
	ASSERT(!messages.getLength());

	auto& deque = get_version_messages(self, tag);
	//TraceEvent("TLogPeekMem", self->dbgid).detail("Tag", req.tag1).detail("PDS", self->persistentDataSequence).detail("PDDS", self->persistentDataDurableSequence).detail("Oldest", map1.empty() ? 0 : map1.begin()->key ).detail("OldestMsgCount", map1.empty() ? 0 : map1.begin()->value.size());

	auto it = std::lower_bound(deque.begin(),
	                           deque.end(),
	                           std::make_pair(begin, LengthPrefixedStringRef()),
	                           [](const auto& l, const auto& r) -> bool { return l.first < r.first; });

	Version currentVersion = -1;
	for (; it != deque.end(); ++it) {
		if (it->first != currentVersion) {
			if (messages.getLength() >= SERVER_KNOBS->DESIRED_TOTAL_BYTES) {
				endVersion = currentVersion + 1;
				//TraceEvent("TLogPeekMessagesReached2", self->dbgid);
				break;
			}

			currentVersion = it->first;
			messages << VERSION_HEADER << currentVersion;
		}
		TraceEvent("LogRouterPeekFromMemory", self->dbgid)
		    .setMaxFieldLength(-1)
		    .setMaxEventLength(-1)
		    .detail("Tag", tag)
		    .detail("Message", it->second.toStringRef())
		    .detail("Version", it->first);
		messages << it->second.toStringRef();
	}
}

Version poppedVersion(LogRouterData* self, Tag tag) {
	auto tagData = self->getTagData(tag);
	if (!tagData)
		return Version(0);
	return tagData->popped;
}

// Common logics to peek TLog and create TLogPeekReply that serves both streaming peek or normal peek request
ACTOR template <typename PromiseType>
Future<Void> logRouterPeekMessages(PromiseType replyPromise,
                                   LogRouterData* self,
                                   Version reqBegin,
                                   Tag reqTag,
                                   bool reqReturnIfBlocked = false,
                                   bool reqOnlySpilled = false,
                                   Optional<std::pair<UID, int>> reqSequence = Optional<std::pair<UID, int>>()) {
	state BinaryWriter messages(Unversioned());
	state int sequence = -1;
	state UID peekId;

	DebugLogTraceEvent("LogRouterPeek0", self->dbgid)
	    .detail("ReturnIfBlocked", reqReturnIfBlocked)
	    .detail("Tag", reqTag.toString())
	    .detail("Seq", reqSequence.present() ? reqSequence.get().second : -1)
	    .detail("SeqCursor", reqSequence.present() ? reqSequence.get().first : UID())
	    .detail("Ver", self->version.get())
	    .detail("Begin", reqBegin);

	if (reqSequence.present()) {
		try {
			peekId = reqSequence.get().first;
			sequence = reqSequence.get().second;
			if (sequence >= SERVER_KNOBS->PARALLEL_GET_MORE_REQUESTS &&
			    self->peekTracker.find(peekId) == self->peekTracker.end()) {
				throw operation_obsolete();
			}
			auto& trackerData = self->peekTracker[peekId];
			if (sequence == 0 && trackerData.sequence_version.find(0) == trackerData.sequence_version.end()) {
				trackerData.sequence_version[0].send(std::make_pair(reqBegin, reqOnlySpilled));
			}
			auto seqBegin = trackerData.sequence_version.begin();
			// The peek cursor and this comparison need to agree about the maximum number of in-flight requests.
			while (trackerData.sequence_version.size() &&
			       seqBegin->first <= sequence - SERVER_KNOBS->PARALLEL_GET_MORE_REQUESTS) {
				if (seqBegin->second.canBeSet()) {
					seqBegin->second.sendError(operation_obsolete());
				}
				trackerData.sequence_version.erase(seqBegin);
				seqBegin = trackerData.sequence_version.begin();
			}

			if (trackerData.sequence_version.size() && sequence < seqBegin->first) {
				throw operation_obsolete();
			}

			trackerData.lastUpdate = now();
			std::pair<Version, bool> prevPeekData = wait(trackerData.sequence_version[sequence].getFuture());
			reqBegin = prevPeekData.first;
			reqOnlySpilled = prevPeekData.second;
			wait(yield());
		} catch (Error& e) {
			DebugLogTraceEvent("LogRouterPeekError", self->dbgid)
			    .error(e)
			    .detail("Tag", reqTag.toString())
			    .detail("Seq", reqSequence.present() ? reqSequence.get().second : -1)
			    .detail("SeqCursor", reqSequence.present() ? reqSequence.get().first : UID())
			    .detail("Begin", reqBegin);

			if (e.code() == error_code_timed_out || e.code() == error_code_operation_obsolete) {
				replyPromise.sendError(e);
				return Void();
			} else {
				throw;
			}
		}
	}

	if (reqReturnIfBlocked && self->version.get() < reqBegin) {
		replyPromise.sendError(end_of_stream());
		if (reqSequence.present()) {
			auto& trackerData = self->peekTracker[peekId];
			auto& sequenceData = trackerData.sequence_version[sequence + 1];
			if (!sequenceData.isSet()) {
				sequenceData.send(std::make_pair(reqBegin, reqOnlySpilled));
			}
		}
		return Void();
	}

	if (self->version.get() < reqBegin) {
		wait(self->version.whenAtLeast(reqBegin));
		wait(delay(SERVER_KNOBS->TLOG_PEEK_DELAY, g_network->getCurrentTask()));
	}

	state double startTime = now();
	state Version poppedVer;
	state Version endVersion;
	// Run the peek logic in a loop to account for the case where there is no data to return to the caller, and we may
	// want to wait a little bit instead of just sending back an empty message. This feature is controlled by a knob.
	loop {

		poppedVer = poppedVersion(self, reqTag);

		if (poppedVer > reqBegin || reqBegin < self->startVersion) {
			// This should only happen if a packet is sent multiple times and the reply is not needed.
			// Since we are using popped differently, do not send a reply.
			TraceEvent(SevWarnAlways, "LogRouterPeekPopped", self->dbgid)
			    .detail("Begin", reqBegin)
			    .detail("Popped", poppedVer)
			    .detail("Tag", reqTag.toString())
			    .detail("Seq", reqSequence.present() ? reqSequence.get().second : -1)
			    .detail("SeqCursor", reqSequence.present() ? reqSequence.get().first : UID())
			    .detail("Start", self->startVersion);
			if (std::is_same<PromiseType, Promise<TLogPeekReply>>::value) {
				// kills logRouterPeekStream actor, otherwise that actor becomes stuck
				throw operation_obsolete();
			}
			if (std::is_same<PromiseType, ReplyPromise<TLogPeekReply>>::value) {
				// Send error to avoid a race condition that the peer is really retrying,
				// otherwise, the peer could be blocked forever.
				replyPromise.sendError(operation_obsolete());
			} else {
				replyPromise.send(Never());
			}

			return Void();
		}

		ASSERT(reqBegin >= poppedVersion(self, reqTag) && reqBegin >= self->startVersion);

		endVersion = self->version.get() + 1;
		peekMessagesFromMemory(self, reqTag, reqBegin, messages, endVersion);

		// Reply the peek request when
		//   - Have data return to the caller, or
		//   - Batching empty peek is disabled, or
		//   - Batching empty peek interval has been reached.
		if (messages.getLength() > 0 || !SERVER_KNOBS->PEEK_BATCHING_EMPTY_MSG ||
		    now() - startTime > SERVER_KNOBS->PEEK_BATCHING_EMPTY_MSG_INTERVAL) {
			break;
		}

		state Version waitUntilVersion = self->version.get() + 1;

		// Currently, from `reqBegin` to self->version are all empty peeks. Wait for more version, or the empty batching
		// interval has expired.
		wait(self->version.whenAtLeast(waitUntilVersion) ||
		     delay(SERVER_KNOBS->PEEK_BATCHING_EMPTY_MSG_INTERVAL - (now() - startTime)));
		if (self->version.get() < waitUntilVersion) {
			break; // We know that from `reqBegin` to self->version are all empty messages. Skip re-executing the peek
			       // logic.
		}
	}

	TLogPeekReply reply;
	reply.maxKnownVersion = self->version.get();
	reply.minKnownCommittedVersion = self->poppedVersion;
	auto messagesValue = messages.toValue();
	reply.arena.dependsOn(messagesValue.arena());
	reply.messages = messagesValue;
	reply.popped = self->minPopped.get() >= self->startVersion ? self->minPopped.get() : 0;
	reply.end = endVersion;
	reply.onlySpilled = false;

	if (reqSequence.present()) {
		auto& trackerData = self->peekTracker[peekId];
		trackerData.lastUpdate = now();
		auto& sequenceData = trackerData.sequence_version[sequence + 1];
		if (trackerData.sequence_version.size() && sequence + 1 < trackerData.sequence_version.begin()->first) {
			replyPromise.sendError(operation_obsolete());
			if (!sequenceData.isSet())
				sequenceData.sendError(operation_obsolete());
			return Void();
		}
		if (sequenceData.isSet()) {
			if (sequenceData.getFuture().get().first != reply.end) {
				CODE_PROBE(true, "tlog peek second attempt ended at a different version");
				replyPromise.sendError(operation_obsolete());
				return Void();
			}
		} else {
			sequenceData.send(std::make_pair(reply.end, reply.onlySpilled));
		}
		reply.begin = reqBegin;
	}

	replyPromise.send(reply);
	DebugLogTraceEvent("LogRouterPeek4", self->dbgid)
	    .setMaxFieldLength(-1)
	    .setMaxEventLength(-1)
	    .detail("Tag", reqTag.toString())
	    .detail("ReqBegin", reqBegin)
	    .detail("End", reply.end)
	    .detail("MessageSize", reply.messages.size())
	    .detail("Message", reply.messages)
	    .detail("PoppedVersion", self->poppedVersion);
	return Void();
}

// This actor keep pushing TLogPeekStreamReply until it's removed from the cluster or should recover
ACTOR Future<Void> logRouterPeekStream(LogRouterData* self, TLogPeekStreamRequest req) {
	self->activePeekStreams++;

	state Version begin = req.begin;
	state bool onlySpilled = false;
	req.reply.setByteLimit(std::min(SERVER_KNOBS->MAXIMUM_PEEK_BYTES, req.limitBytes));
	loop {
		state TLogPeekStreamReply reply;
		state Promise<TLogPeekReply> promise;
		state Future<TLogPeekReply> future(promise.getFuture());
		try {
			wait(req.reply.onReady() && store(reply.rep, future) &&
			     logRouterPeekMessages(promise, self, begin, req.tag, req.returnIfBlocked, onlySpilled));

			reply.rep.begin = begin;
			req.reply.send(reply);
			begin = reply.rep.end;
			onlySpilled = reply.rep.onlySpilled;
			if (reply.rep.end > self->version.get()) {
				wait(delay(SERVER_KNOBS->TLOG_PEEK_DELAY, g_network->getCurrentTask()));
			} else {
				wait(delay(0, g_network->getCurrentTask()));
			}
		} catch (Error& e) {
			self->activePeekStreams--;
			TraceEvent(SevDebug, "LogRouterPeekStreamEnd", self->dbgid)
			    .errorUnsuppressed(e)
			    .detail("Tag", req.tag)
			    .detail("PeerAddr", req.reply.getEndpoint().getPrimaryAddress());

			if (e.code() == error_code_end_of_stream || e.code() == error_code_operation_obsolete) {
				req.reply.sendError(e);
				return Void();
			} else {
				throw;
			}
		}
	}
}

ACTOR Future<Void> cleanupPeekTrackers(LogRouterData* self) {
	loop {
		double minTimeUntilExpiration = SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME;
		auto it = self->peekTracker.begin();
		while (it != self->peekTracker.end()) {
			double timeUntilExpiration = it->second.lastUpdate + SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME - now();
			if (timeUntilExpiration < 1.0e-6) {
				for (auto seq : it->second.sequence_version) {
					if (!seq.second.isSet()) {
						seq.second.sendError(timed_out());
					}
				}
				it = self->peekTracker.erase(it);
			} else {
				minTimeUntilExpiration = std::min(minTimeUntilExpiration, timeUntilExpiration);
				++it;
			}
		}

		wait(delay(minTimeUntilExpiration));
	}
}

ACTOR Future<Void> logRouterPop(LogRouterData* self, TLogPopRequest req) {
	auto tagData = self->getTagData(req.tag);
	if (!tagData) {
		tagData = self->createTagData(req.tag, req.to, req.durableKnownCommittedVersion);
	} else if (req.to > tagData->popped) {
		DebugLogTraceEvent("LogRouterPop", self->dbgid).detail("Tag", req.tag.toString()).detail("PopVersion", req.to);
		tagData->popped = req.to;
		tagData->durableKnownCommittedVersion = req.durableKnownCommittedVersion;
		wait(tagData->eraseMessagesBefore(req.to, self, TaskPriority::TLogPop));
	}

	state Version minPopped = std::numeric_limits<Version>::max();
	state Version minKnownCommittedVersion = std::numeric_limits<Version>::max();
	for (auto it : self->tag_data) {
		if (it) {
			minPopped = std::min(it->popped, minPopped);
			minKnownCommittedVersion = std::min(it->durableKnownCommittedVersion, minKnownCommittedVersion);
		}
	}

	while (!self->messageBlocks.empty() && self->messageBlocks.front().first < minPopped) {
		self->messageBlocks.pop_front();
		wait(yield(TaskPriority::TLogPop));
	}

	self->poppedVersion = std::min(minKnownCommittedVersion, self->minKnownCommittedVersion);
	if (self->logSystem->get() && self->allowPops) {
		const Tag popTag = self->logSystem->get()->getPseudoPopTag(self->routerTag, ProcessClass::LogRouterClass);
		self->logSystem->get()->pop(self->poppedVersion, popTag);
	}
	req.reply.send(Void());
	self->minPopped.set(std::max(minPopped, self->minPopped.get()));
	return Void();
}

ACTOR Future<Void> logRouterCore(TLogInterface interf,
                                 InitializeLogRouterRequest req,
                                 Reference<AsyncVar<ServerDBInfo> const> db) {
	state LogRouterData logRouterData(interf.id(), req);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection(addActor.getFuture());
	state Future<Void> dbInfoChange = Void();

	addActor.send(pullAsyncData(&logRouterData));
	addActor.send(cleanupPeekTrackers(&logRouterData));
	addActor.send(traceRole(Role::LOG_ROUTER, interf.id()));

	loop choose {
		when(wait(dbInfoChange)) {
			dbInfoChange = db->onChange();
			logRouterData.allowPops = db->get().recoveryState == RecoveryState::FULLY_RECOVERED &&
			                          db->get().recoveryCount >= req.recoveryCount;
			logRouterData.logSystem->set(ILogSystem::fromServerDBInfo(logRouterData.dbgid, db->get(), true));
		}
		when(TLogPeekRequest req = waitNext(interf.peekMessages.getFuture())) {
			addActor.send(logRouterPeekMessages(
			    req.reply, &logRouterData, req.begin, req.tag, req.returnIfBlocked, req.onlySpilled, req.sequence));
		}
		when(TLogPeekStreamRequest req = waitNext(interf.peekStreamMessages.getFuture())) {
			TraceEvent(SevDebug, "LogRouterPeekStream", logRouterData.dbgid)
			    .detail("Tag", req.tag)
			    .detail("Token", interf.peekStreamMessages.getEndpoint().token);
			addActor.send(logRouterPeekStream(&logRouterData, req));
		}
		when(TLogPopRequest req = waitNext(interf.popMessages.getFuture())) {
			// Request from remote tLog to pop data from LR
			addActor.send(logRouterPop(&logRouterData, req));
		}
		when(wait(error)) {}
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo> const> db,
                                uint64_t recoveryCount,
                                TLogInterface myInterface) {
	loop {
		bool isDisplaced =
		    ((db->get().recoveryCount > recoveryCount && db->get().recoveryState != RecoveryState::UNINITIALIZED) ||
		     (db->get().recoveryCount == recoveryCount && db->get().recoveryState == RecoveryState::FULLY_RECOVERED));
		isDisplaced = isDisplaced && !db->get().logSystemConfig.hasLogRouter(myInterface.id());
		if (isDisplaced) {
			throw worker_removed();
		}
		wait(db->onChange());
	}
}

ACTOR Future<Void> logRouter(TLogInterface interf,
                             InitializeLogRouterRequest req,
                             Reference<AsyncVar<ServerDBInfo> const> db) {
	try {
		TraceEvent("LogRouterStart", interf.id())
		    .detail("Start", req.startVersion)
		    .detail("Tag", req.routerTag.toString())
		    .detail("Localities", req.tLogLocalities.size())
		    .detail("Locality", req.locality);
		state Future<Void> core = logRouterCore(interf, req, db);
		loop choose {
			when(wait(core)) {
				return Void();
			}
			when(wait(checkRemoved(db, req.recoveryCount, interf))) {}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_worker_removed) {
			TraceEvent("LogRouterTerminated", interf.id()).errorUnsuppressed(e);
			return Void();
		}
		throw;
	}
}
