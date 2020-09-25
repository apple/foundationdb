/*
 * LogSystem.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_LOGSYSTEM_H
#define FDBSERVER_LOGSYSTEM_H

#include <set>
#include <vector>

#include "fdbserver/TLogInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbserver/MutationTracking.h"
#include "flow/IndexedSet.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Replication.h"

struct DBCoreState;
struct TLogSet;
struct CoreTLogSet;

struct ConnectionResetInfo : public ReferenceCounted<ConnectionResetInfo> {
	double lastReset;
	Future<Void> resetCheck;
	int slowReplies;
	int fastReplies;

	ConnectionResetInfo() : lastReset(now()), slowReplies(0), fastReplies(0), resetCheck(Void()) {}
};

// The set of tLog servers, logRouters and backupWorkers for a log tag
class LogSet : NonCopyable, public ReferenceCounted<LogSet> {
public:
	std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logServers;
	std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> logRouters;
	std::vector<Reference<AsyncVar<OptionalInterface<BackupInterface>>>> backupWorkers;
	std::vector<Reference<ConnectionResetInfo>> connectionResetTrackers;
	int32_t tLogWriteAntiQuorum;
	int32_t tLogReplicationFactor;
	std::vector< LocalityData > tLogLocalities; // Stores the localities of the log servers
	TLogVersion tLogVersion;
	Reference<IReplicationPolicy> tLogPolicy;
	Reference<LocalitySet> logServerSet;
	std::vector<int> logIndexArray;
	std::vector<LocalityEntry> logEntryArray;
	bool isLocal;
	int8_t locality;
	Version startVersion;
	std::vector<Future<TLogLockResult>> replies;
	std::vector<std::vector<int>> satelliteTagLocations;

	LogSet() : tLogWriteAntiQuorum(0), tLogReplicationFactor(0), isLocal(true), locality(tagLocalityInvalid), startVersion(invalidVersion) {}
	LogSet(const TLogSet& tlogSet);
	LogSet(const CoreTLogSet& coreSet);

	std::string logRouterString() {
		std::string result;
		for(int i = 0; i < logRouters.size(); i++) {
			if(i>0) {
				result += ", ";
			}
			result += logRouters[i]->get().id().toString();
		}
		return result;
	}

	bool hasLogRouter(UID id) const {
		for (const auto& router : logRouters) {
			if (router->get().id() == id) {
				return true;
			}
		}
		return false;
	}

	bool hasBackupWorker(UID id) const {
		for (const auto& worker : backupWorkers) {
			if (worker->get().id() == id) {
				return true;
			}
		}
		return false;
	}

	std::string logServerString() {
		std::string result;
		for(int i = 0; i < logServers.size(); i++) {
			if(i>0) {
				result += ", ";
			}
			result += logServers[i]->get().id().toString();
		}
		return result;
	}

	void populateSatelliteTagLocations(int logRouterTags, int oldLogRouterTags, int txsTags, int oldTxsTags) {
		satelliteTagLocations.clear();
		satelliteTagLocations.resize(std::max({logRouterTags,oldLogRouterTags,txsTags,oldTxsTags})+1);

		std::map<int,int> server_usedBest;
		std::set<std::pair<int,int>> used_servers;
		for(int i = 0; i < tLogLocalities.size(); i++) {
			used_servers.insert(std::make_pair(0,i));
		}

		Reference<LocalitySet> serverSet = Reference<LocalitySet>(new LocalityMap<std::pair<int,int>>());
		LocalityMap<std::pair<int,int>>* serverMap = (LocalityMap<std::pair<int,int>>*) serverSet.getPtr();
		std::vector<std::pair<int,int>> resultPairs;
		for(int loc = 0; loc < satelliteTagLocations.size(); loc++) {
			int team = loc;
			if(loc < logRouterTags) {
				team = loc + 1;
			} else if(loc == logRouterTags) {
				team = 0;
			}

			bool teamComplete = false;
			alsoServers.resize(1);
			serverMap->clear();
			resultPairs.clear();
			for(auto& used_idx : used_servers) {
				auto entry = serverMap->add(tLogLocalities[used_idx.second], &used_idx);
				if(!resultPairs.size()) {
					resultPairs.push_back(used_idx);
					alsoServers[0] = entry;
				}

				resultEntries.clear();
				if( serverSet->selectReplicas(tLogPolicy, alsoServers, resultEntries) ) {
					for(auto& entry : resultEntries) {
						resultPairs.push_back(*serverMap->getObject(entry));
					}
					int firstBestUsed = server_usedBest[resultPairs[0].second];
					for(int i = 1; i < resultPairs.size(); i++) {
						int thisBestUsed = server_usedBest[resultPairs[i].second];
						if(thisBestUsed < firstBestUsed) {
							std::swap(resultPairs[0], resultPairs[i]);
							firstBestUsed = thisBestUsed;
						}
					}
					server_usedBest[resultPairs[0].second]++;

					for(auto& res : resultPairs) {
						satelliteTagLocations[team].push_back(res.second);
						used_servers.erase(res);
						res.first++;
						used_servers.insert(res);
					}
					teamComplete = true;
					break;
				}
			}
			ASSERT(teamComplete);
		}

		checkSatelliteTagLocations();
	}

	void checkSatelliteTagLocations() {
		std::vector<int> usedBest;
		std::vector<int> used;
		usedBest.resize(tLogLocalities.size());
		used.resize(tLogLocalities.size());
		for(auto team : satelliteTagLocations) {
			usedBest[team[0]]++;
			for(auto loc : team) {
				used[loc]++;
			}
		}

		int minUsedBest = satelliteTagLocations.size();
		int maxUsedBest = 0;
		for(auto i : usedBest) {
			minUsedBest = std::min(minUsedBest, i);
			maxUsedBest = std::max(maxUsedBest, i);
		}

		int minUsed = satelliteTagLocations.size();
		int maxUsed = 0;
		for(auto i : used) {
			minUsed = std::min(minUsed, i);
			maxUsed = std::max(maxUsed, i);
		}

		bool foundDuplicate = false;
		std::set<Optional<Key>> zones;
		std::set<Optional<Key>> dcs;
		for(auto& loc : tLogLocalities) {
			if(zones.count(loc.zoneId())) {
				foundDuplicate = true;
				break;
			}
			zones.insert(loc.zoneId());
			dcs.insert(loc.dcId());
		}
		bool moreThanOneDC = dcs.size() > 1 ? true : false;

		TraceEvent(((maxUsed - minUsed > 1) || (maxUsedBest - minUsedBest > 1)) ? (g_network->isSimulated() && !foundDuplicate && !moreThanOneDC ? SevError : SevWarnAlways) : SevInfo, "CheckSatelliteTagLocations").detail("MinUsed", minUsed).detail("MaxUsed", maxUsed).detail("MinUsedBest", minUsedBest).detail("MaxUsedBest", maxUsedBest).detail("DuplicateZones", foundDuplicate).detail("NumOfDCs", dcs.size());
	}

	int bestLocationFor( Tag tag ) {
		if(locality == tagLocalitySatellite) {
			return satelliteTagLocations[tag == txsTag ? 0 : tag.id + 1][0];
		}

		//the following logic supports upgrades from 5.X
		if(tag == txsTag) return txsTagOld % logServers.size();
		return tag.id % logServers.size();
	}

	void updateLocalitySet( std::vector<LocalityData> const& localities ) {
		LocalityMap<int>* logServerMap;

		logServerSet = Reference<LocalitySet>(new LocalityMap<int>());
		logServerMap = (LocalityMap<int>*) logServerSet.getPtr();

		logEntryArray.clear();
		logEntryArray.reserve(localities.size());
		logIndexArray.clear();
		logIndexArray.reserve(localities.size());

		for( int i = 0; i < localities.size(); i++ ) {
			logIndexArray.push_back(i);
			logEntryArray.push_back(logServerMap->add(localities[i], &logIndexArray.back()));
		}
	}

	bool satisfiesPolicy( const std::vector<LocalityEntry>& locations ) {
		resultEntries.clear();

		// Run the policy, assert if unable to satify
		bool result = logServerSet->selectReplicas(tLogPolicy, locations, resultEntries);
		ASSERT(result);

		return resultEntries.size() == 0;
	}

	void getPushLocations(VectorRef<Tag> tags, std::vector<int>& locations, int locationOffset,
	                      bool allLocations = false) {
		if(locality == tagLocalitySatellite) {
			for(auto& t : tags) {
				if(t == txsTag || t.locality == tagLocalityTxs || t.locality == tagLocalityLogRouter) {
					for(int loc : satelliteTagLocations[t == txsTag ? 0 : t.id + 1]) {
						locations.push_back(locationOffset + loc);
					}
				}
			}
			uniquify(locations);
			return;
		}

		newLocations.clear();
		alsoServers.clear();
		resultEntries.clear();

		if (allLocations) {
			// special handling for allLocations
			TraceEvent("AllLocationsSet");
			for (int i = 0; i < logServers.size(); i++) {
				newLocations.push_back(i);
			}
		} else {
			for (auto& t : tags) {
				if (locality == tagLocalitySpecial || t.locality == locality || t.locality < 0) {
					newLocations.push_back(bestLocationFor(t));
				}
			}
		}

		uniquify( newLocations );

		if (newLocations.size())
			alsoServers.reserve(newLocations.size());

		// Convert locations to the also servers
		for (auto location : newLocations) {
			locations.push_back(locationOffset + location);
			alsoServers.push_back(logEntryArray[location]);
		}

		// Run the policy, assert if unable to satify
		bool result = logServerSet->selectReplicas(tLogPolicy, alsoServers, resultEntries);
		ASSERT(result);

		// Add the new servers to the location array
		LocalityMap<int>* logServerMap = (LocalityMap<int>*) logServerSet.getPtr();
		for (auto entry : resultEntries) {
			locations.push_back(locationOffset + *logServerMap->getObject(entry));
		}
		//TraceEvent("GetPushLocations").detail("Policy", tLogPolicy->info())
		//	.detail("Results", locations.size()).detail("Selection", logServerSet->size())
		//	.detail("Included", alsoServers.size()).detail("Duration", timer() - t);
	}

private:
	std::vector<LocalityEntry> alsoServers, resultEntries;
	std::vector<int> newLocations;
};

struct ILogSystem {
	// Represents a particular (possibly provisional) epoch of the log subsystem


	struct IPeekCursor {
		//clones the peek cursor, however you cannot call getMore() on the cloned cursor.
		virtual Reference<IPeekCursor> cloneNoMore() = 0;

		virtual void setProtocolVersion( ProtocolVersion version ) = 0;

		//if hasMessage() returns true, getMessage(), getMessageWithTags(), or reader() can be called.
		//does not modify the cursor
		virtual bool hasMessage() = 0;

		//pre: only callable if hasMessage() returns true
		//return the tags associated with the message for the current sequence
		virtual VectorRef<Tag> getTags() = 0;

		//pre: only callable if hasMessage() returns true
		//returns the arena containing the contents of getMessage(), getMessageWithTags(), and reader()
		virtual Arena& arena() = 0;

		//pre: only callable if hasMessage() returns true
		//returns an arena reader for the next message
		//caller cannot call getMessage(), getMessageWithTags(), and reader()
		//the caller must advance the reader before calling nextMessage()
		virtual ArenaReader* reader() = 0;

		//pre: only callable if hasMessage() returns true
		//caller cannot call getMessage(), getMessageWithTags(), and reader()
		//return the contents of the message for the current sequence
		virtual StringRef getMessage() = 0;

		//pre: only callable if hasMessage() returns true
		//caller cannot call getMessage(), getMessageWithTags(), and reader()
		//return the contents of the message for the current sequence
		virtual StringRef getMessageWithTags() = 0;

		//pre: only callable after getMessage(), getMessageWithTags(), or reader()
		//post: hasMessage() and version() have been updated
		//hasMessage() will never return false "in the middle" of a version (that is, if it does return false, version().subsequence will be zero)  < FIXME: Can we lose this property?
		virtual void nextMessage() = 0;

		//advances the cursor to the supplied LogMessageVersion, and updates hasMessage
		virtual void advanceTo(LogMessageVersion n) = 0;

		//returns immediately if hasMessage() returns true.
		//returns when either the result of hasMessage() or version() has changed, or a cursor has internally been exhausted.
		virtual Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply) = 0;

		//returns when the failure monitor detects that the servers associated with the cursor are failed
		virtual Future<Void> onFailed() = 0;

		//returns false if:
		// (1) the failure monitor detects that the servers associated with the cursor is failed
		// (2) the interface is not present
		// (3) the cursor cannot return any more results
		virtual bool isActive() = 0;

		//returns true if the cursor cannot return any more results
		virtual bool isExhausted() = 0;

		// Returns the smallest possible message version which the current message (if any) or a subsequent message might have
		// (If hasMessage(), this is therefore the message version of the current message)
		virtual const LogMessageVersion& version() = 0;

		//So far, the cursor has returned all messages which both satisfy the criteria passed to peek() to create the cursor AND have (popped(),0) <= message version number <= version()
		//Other messages might have been skipped
		virtual Version popped() = 0;

		// Returns the maximum version known to have been pushed (not necessarily durably) into the log system (0 is always a possible result!)
		virtual Version getMaxKnownVersion() { return 0; }

		virtual Version getMinKnownCommittedVersion() = 0;

		virtual Optional<UID> getPrimaryPeekLocation() = 0;

		virtual void addref() = 0;

		virtual void delref() = 0;
	};

	struct ServerPeekCursor : IPeekCursor, ReferenceCounted<ServerPeekCursor> {
		Reference<AsyncVar<OptionalInterface<TLogInterface>>> interf;
		const Tag tag;

		TLogPeekReply results;
		ArenaReader rd;
		LogMessageVersion messageVersion, end;
		Version poppedVersion;
		TagsAndMessage messageAndTags;
		bool hasMsg;
		Future<Void> more;
		UID randomID;
		bool returnIfBlocked;

		bool onlySpilled;
		bool parallelGetMore;
		int sequence;
		Deque<Future<TLogPeekReply>> futureResults;
		Future<Void> interfaceChanged;

		double lastReset;
		Future<Void> resetCheck;
		int slowReplies;
		int fastReplies;
		int unknownReplies;

		ServerPeekCursor( Reference<AsyncVar<OptionalInterface<TLogInterface>>> const& interf, Tag tag, Version begin, Version end, bool returnIfBlocked, bool parallelGetMore );
		ServerPeekCursor( TLogPeekReply const& results, LogMessageVersion const& messageVersion, LogMessageVersion const& end, TagsAndMessage const& message, bool hasMsg, Version poppedVersion, Tag tag );

		virtual Reference<IPeekCursor> cloneNoMore();
		virtual void setProtocolVersion( ProtocolVersion version );
		virtual Arena& arena();
		virtual ArenaReader* reader();
		virtual bool hasMessage();
		virtual void nextMessage();
		virtual StringRef getMessage();
		virtual StringRef getMessageWithTags();
		virtual VectorRef<Tag> getTags();
		virtual void advanceTo(LogMessageVersion n);
		virtual Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply);
		virtual Future<Void> onFailed();
		virtual bool isActive();
		virtual bool isExhausted();
		virtual const LogMessageVersion& version();
		virtual Version popped();
		virtual Version getMinKnownCommittedVersion();
		virtual Optional<UID> getPrimaryPeekLocation();

		virtual void addref() {
			ReferenceCounted<ServerPeekCursor>::addref();
		}

		virtual void delref() {
			ReferenceCounted<ServerPeekCursor>::delref();
		}

		virtual Version getMaxKnownVersion() { return results.maxKnownVersion; }
	};

	struct MergedPeekCursor : IPeekCursor, ReferenceCounted<MergedPeekCursor> {
		Reference<LogSet> logSet;
		std::vector< Reference<IPeekCursor> > serverCursors;
		std::vector<LocalityEntry> locations;
		std::vector< std::pair<LogMessageVersion, int> > sortedVersions;
		Tag tag;
		int bestServer, currentCursor, readQuorum;
		Optional<LogMessageVersion> nextVersion;
		LogMessageVersion messageVersion;
		bool hasNextMessage;
		UID randomID;
		int tLogReplicationFactor;
		Future<Void> more;

		MergedPeekCursor( std::vector< Reference<ILogSystem::IPeekCursor> > const& serverCursors, Version begin );
		MergedPeekCursor( std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> const& logServers, int bestServer, int readQuorum, Tag tag, Version begin, Version end, bool parallelGetMore, std::vector<LocalityData> const& tLogLocalities, Reference<IReplicationPolicy> const tLogPolicy, int tLogReplicationFactor );
		MergedPeekCursor( std::vector< Reference<IPeekCursor> > const& serverCursors, LogMessageVersion const& messageVersion, int bestServer, int readQuorum, Optional<LogMessageVersion> nextVersion, Reference<LogSet> logSet, int tLogReplicationFactor );

		virtual Reference<IPeekCursor> cloneNoMore();
		virtual void setProtocolVersion( ProtocolVersion version );
		virtual Arena& arena();
		virtual ArenaReader* reader();
		void calcHasMessage();
		void updateMessage(bool usePolicy);
		virtual bool hasMessage();
		virtual void nextMessage();
		virtual StringRef getMessage();
		virtual StringRef getMessageWithTags();
		virtual VectorRef<Tag> getTags();
		virtual void advanceTo(LogMessageVersion n);
		virtual Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply);
		virtual Future<Void> onFailed();
		virtual bool isActive();
		virtual bool isExhausted();
		virtual const LogMessageVersion& version();
		virtual Version popped();
		virtual Version getMinKnownCommittedVersion();
		virtual Optional<UID> getPrimaryPeekLocation();

		virtual void addref() {
			ReferenceCounted<MergedPeekCursor>::addref();
		}

		virtual void delref() {
			ReferenceCounted<MergedPeekCursor>::delref();
		}
	};

	struct SetPeekCursor : IPeekCursor, ReferenceCounted<SetPeekCursor> {
		std::vector<Reference<LogSet>> logSets;
		std::vector< std::vector< Reference<IPeekCursor> > > serverCursors;
		Tag tag;
		int bestSet, bestServer, currentSet, currentCursor;
		std::vector<LocalityEntry> locations;
		std::vector< std::pair<LogMessageVersion, int> > sortedVersions;
		Optional<LogMessageVersion> nextVersion;
		LogMessageVersion messageVersion;
		bool hasNextMessage;
		bool useBestSet;
		UID randomID;
		Future<Void> more;

		SetPeekCursor( std::vector<Reference<LogSet>> const& logSets, int bestSet, int bestServer, Tag tag, Version begin, Version end, bool parallelGetMore );
		SetPeekCursor( std::vector<Reference<LogSet>> const& logSets, std::vector< std::vector< Reference<IPeekCursor> > > const& serverCursors, LogMessageVersion const& messageVersion, int bestSet, int bestServer, Optional<LogMessageVersion> nextVersion, bool useBestSet );

		virtual Reference<IPeekCursor> cloneNoMore();
		virtual void setProtocolVersion( ProtocolVersion version );
		virtual Arena& arena();
		virtual ArenaReader* reader();
		void calcHasMessage();
		void updateMessage(int logIdx, bool usePolicy);
		virtual bool hasMessage();
		virtual void nextMessage();
		virtual StringRef getMessage();
		virtual StringRef getMessageWithTags();
		virtual VectorRef<Tag> getTags();
		virtual void advanceTo(LogMessageVersion n);
		virtual Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply);
		virtual Future<Void> onFailed();
		virtual bool isActive();
		virtual bool isExhausted();
		virtual const LogMessageVersion& version();
		virtual Version popped();
		virtual Version getMinKnownCommittedVersion();
		virtual Optional<UID> getPrimaryPeekLocation();

		virtual void addref() {
			ReferenceCounted<SetPeekCursor>::addref();
		}

		virtual void delref() {
			ReferenceCounted<SetPeekCursor>::delref();
		}
	};

	struct MultiCursor : IPeekCursor, ReferenceCounted<MultiCursor> {
		std::vector<Reference<IPeekCursor>> cursors;
		std::vector<LogMessageVersion> epochEnds;
		Version poppedVersion;

		MultiCursor( std::vector<Reference<IPeekCursor>> cursors, std::vector<LogMessageVersion> epochEnds );

		virtual Reference<IPeekCursor> cloneNoMore();
		virtual void setProtocolVersion( ProtocolVersion version );
		virtual Arena& arena();
		virtual ArenaReader* reader();
		virtual bool hasMessage();
		virtual void nextMessage();
		virtual StringRef getMessage();
		virtual StringRef getMessageWithTags();
		virtual VectorRef<Tag> getTags();
		virtual void advanceTo(LogMessageVersion n);
		virtual Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply);
		virtual Future<Void> onFailed();
		virtual bool isActive();
		virtual bool isExhausted();
		virtual const LogMessageVersion& version();
		virtual Version popped();
		virtual Version getMinKnownCommittedVersion();
		virtual Optional<UID> getPrimaryPeekLocation();

		virtual void addref() {
			ReferenceCounted<MultiCursor>::addref();
		}

		virtual void delref() {
			ReferenceCounted<MultiCursor>::delref();
		}
	};

	struct BufferedCursor : IPeekCursor, ReferenceCounted<BufferedCursor> {
		struct BufferedMessage {
			Arena arena;
			StringRef message;
			VectorRef<Tag> tags;
			LogMessageVersion version;

			BufferedMessage() {}
			explicit BufferedMessage( Version version ) : version(version) {}
			BufferedMessage( Arena arena, StringRef message, const VectorRef<Tag>& tags, const LogMessageVersion& version ) : arena(arena), message(message), tags(tags), version(version) {}

			bool operator < (BufferedMessage const& r) const {
				return version < r.version;
			}

			bool operator == (BufferedMessage const& r) const {
				return version == r.version;
			}
		};

		std::vector<Reference<IPeekCursor>> cursors;
		std::vector<Deque<BufferedMessage>> cursorMessages;
		std::vector<BufferedMessage> messages;
		int messageIndex;
		LogMessageVersion messageVersion;
		Version end;
		bool hasNextMessage;
		bool withTags;
		bool knownUnique;
		Version minKnownCommittedVersion;
		Version poppedVersion;
		Version initialPoppedVersion;
		bool canDiscardPopped;
		Future<Void> more;
		int targetQueueSize;
		UID randomID;

		//FIXME: collectTags is needed to support upgrades from 5.X to 6.0. Remove this code when we no longer support that upgrade.
		bool collectTags;
		void combineMessages();

		BufferedCursor( std::vector<Reference<IPeekCursor>> cursors, Version begin, Version end, bool withTags, bool collectTags, bool canDiscardPopped );
		BufferedCursor( std::vector<Reference<AsyncVar<OptionalInterface<TLogInterface>>>> const& logServers, Tag tag, Version begin, Version end, bool parallelGetMore );

		virtual Reference<IPeekCursor> cloneNoMore();
		virtual void setProtocolVersion( ProtocolVersion version );
		virtual Arena& arena();
		virtual ArenaReader* reader();
		virtual bool hasMessage();
		virtual void nextMessage();
		virtual StringRef getMessage();
		virtual StringRef getMessageWithTags();
		virtual VectorRef<Tag> getTags();
		virtual void advanceTo(LogMessageVersion n);
		virtual Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply);
		virtual Future<Void> onFailed();
		virtual bool isActive();
		virtual bool isExhausted();
		virtual const LogMessageVersion& version();
		virtual Version popped();
		virtual Version getMinKnownCommittedVersion();
		virtual Optional<UID> getPrimaryPeekLocation();

		virtual void addref() {
			ReferenceCounted<BufferedCursor>::addref();
		}

		virtual void delref() {
			ReferenceCounted<BufferedCursor>::delref();
		}
	};

	virtual void addref() = 0;
	virtual void delref() = 0;

	virtual std::string describe() = 0;
	virtual UID getDebugID() = 0;

	virtual void toCoreState( DBCoreState& ) = 0;

	virtual bool remoteStorageRecovered() = 0;

	virtual Future<Void> onCoreStateChanged() = 0;
		// Returns if and when the output of toCoreState() would change (for example, when older logs can be discarded from the state)

	virtual void coreStateWritten( DBCoreState const& newState ) = 0;
	    // Called when a core state has been written to the coordinators

	virtual Future<Void> onError() = 0;
		// Never returns normally, but throws an error if the subsystem stops working

	//Future<Void> push( UID bundle, int64_t seq, VectorRef<TaggedMessageRef> messages );
	virtual Future<Version> push( Version prevVersion, Version version, Version knownCommittedVersion, Version minKnownCommittedVersion, struct LogPushData& data, Optional<UID> debugID = Optional<UID>() ) = 0;
		// Waits for the version number of the bundle (in this epoch) to be prevVersion (i.e. for all pushes ordered earlier)
		// Puts the given messages into the bundle, each with the given tags, and with message versions (version, 0) - (version, N)
		// Changes the version number of the bundle to be version (unblocking the next push)
		// Returns when the preceding changes are durable.  (Later we will need multiple return signals for diffferent durability levels)
		// If the current epoch has ended, push will not return, and the pushed messages will not be visible in any subsequent epoch (but may become visible in this epoch)

	virtual Reference<IPeekCursor> peek( UID dbgid, Version begin, Optional<Version> end, Tag tag, bool parallelGetMore = false ) = 0;
		// Returns (via cursor interface) a stream of messages with the given tag and message versions >= (begin, 0), ordered by message version
		// If pop was previously or concurrently called with upTo > begin, the cursor may not return all such messages.  In that case cursor->popped() will
		// be greater than begin to reflect that.

	virtual Reference<IPeekCursor> peek( UID dbgid, Version begin, Optional<Version> end, std::vector<Tag> tags, bool parallelGetMore = false ) = 0;
		// Same contract as peek(), but for a set of tags

	virtual Reference<IPeekCursor> peekSingle( UID dbgid, Version begin, Tag tag, std::vector<std::pair<Version,Tag>> history = std::vector<std::pair<Version,Tag>>() ) = 0;
		// Same contract as peek(), but blocks until the preferred log server(s) for the given tag are available (and is correspondingly less expensive)

	virtual Reference<IPeekCursor> peekLogRouter( UID dbgid, Version begin, Tag tag ) = 0;
		// Same contract as peek(), but can only peek from the logs elected in the same generation.
		// If the preferred log server is down, a different log from the same generation will merge results locally before sending them to the log router.

	virtual Reference<IPeekCursor> peekTxs( UID dbgid, Version begin, int8_t peekLocality, Version localEnd, bool canDiscardPopped ) = 0;
		// Same contract as peek(), but only for peeking the txsLocality. It allows specifying a preferred peek locality.

	virtual Future<Version> getTxsPoppedVersion() = 0;

	virtual Version getKnownCommittedVersion() = 0;

	virtual Future<Void> onKnownCommittedVersionChange() = 0;

	virtual void popTxs( Version upTo, int8_t popLocality = tagLocalityInvalid ) = 0;

	virtual void pop( Version upTo, Tag tag, Version knownCommittedVersion = 0, int8_t popLocality = tagLocalityInvalid ) = 0;
		// Permits, but does not require, the log subsystem to strip `tag` from any or all messages with message versions < (upTo,0)
		// The popping of any given message may be arbitrarily delayed.

	virtual Future<Void> confirmEpochLive( Optional<UID> debugID = Optional<UID>() ) = 0;
		// Returns success after confirming that pushes in the current epoch are still possible

	virtual Future<Void> endEpoch() = 0;
		// Ends the current epoch without starting a new one

	static Reference<ILogSystem> fromServerDBInfo( UID const& dbgid, struct ServerDBInfo const& db, bool useRecoveredAt = false, Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>() );
	static Reference<ILogSystem> fromLogSystemConfig( UID const& dbgid, struct LocalityData const&, struct LogSystemConfig const&, bool excludeRemote = false, bool useRecoveredAt = false, Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>() );
		// Constructs a new ILogSystem implementation from the given ServerDBInfo/LogSystemConfig.  Might return a null reference if there isn't a fully recovered log system available.
		// The caller can peek() the returned log system and can push() if it has version numbers reserved for it and prevVersions

	static Reference<ILogSystem> fromOldLogSystemConfig( UID const& dbgid, struct LocalityData const&, struct LogSystemConfig const& );
		// Constructs a new ILogSystem implementation from the old log data within a ServerDBInfo/LogSystemConfig.  Might return a null reference if there isn't a fully recovered log system available.

	static Future<Void> recoverAndEndEpoch(Reference<AsyncVar<Reference<ILogSystem>>> const& outLogSystem, UID const& dbgid, DBCoreState const& oldState, FutureStream<TLogRejoinRequest> const& rejoins, LocalityData const& locality, bool* forceRecovery);
		// Constructs a new ILogSystem implementation based on the given oldState and rejoining log servers
		// Ensures that any calls to push or confirmEpochLive in the current epoch but strictly later than change_epoch will not return
		// Whenever changes in the set of available log servers require restarting recovery with a different end sequence, outLogSystem will be changed to a new ILogSystem

	virtual Version getEnd() = 0;
		// Call only on an ILogSystem obtained from recoverAndEndEpoch()
		// Returns the first unreadable version number of the recovered epoch (i.e. message version numbers < (get_end(), 0) will be readable)

	// Returns the start version of current epoch for backup workers.
	virtual Version getBackupStartVersion() const = 0;

	struct EpochTagsVersionsInfo {
		int32_t logRouterTags; // Number of log router tags.
		Version epochBegin, epochEnd;

		explicit EpochTagsVersionsInfo(int32_t n, Version begin, Version end)
		  : logRouterTags(n), epochBegin(begin), epochEnd(end) {}
	};

	// Returns EpochTagVersionsInfo for old epochs that this log system is aware of, excluding the current epoch.
	virtual std::map<LogEpoch, EpochTagsVersionsInfo> getOldEpochTagsVersionsInfo() const = 0;

	virtual Future<Reference<ILogSystem>> newEpoch( struct RecruitFromConfigurationReply const& recr, Future<struct RecruitRemoteFromConfigurationReply> const& fRemoteWorkers, DatabaseConfiguration const& config,
		LogEpoch recoveryCount, int8_t primaryLocality, int8_t remoteLocality, std::vector<Tag> const& allTags, Reference<AsyncVar<bool>> const& recruitmentStalled ) = 0;
		// Call only on an ILogSystem obtained from recoverAndEndEpoch()
		// Returns an ILogSystem representing a new epoch immediately following this one.  The new epoch is only provisional until the caller updates the coordinated DBCoreState

	virtual LogSystemConfig getLogSystemConfig() = 0;
		// Returns the physical configuration of this LogSystem, that could be used to construct an equivalent LogSystem using fromLogSystemConfig()

	virtual Standalone<StringRef> getLogsValue() = 0;

	virtual Future<Void> onLogSystemConfigChange() = 0;
		// Returns when the log system configuration has changed due to a tlog rejoin.

	virtual void getPushLocations(VectorRef<Tag> tags, std::vector<int>& locations, bool allLocations = false) = 0;

	void getPushLocations(std::vector<Tag> const& tags, std::vector<int>& locations, bool allLocations = false) {
		getPushLocations(VectorRef<Tag>((Tag*)&tags.front(), tags.size()), locations, allLocations);
	}

	virtual bool hasRemoteLogs() const = 0;

	virtual Tag getRandomRouterTag() const = 0;
	virtual int getLogRouterTags() const = 0; // Returns the number of router tags.

	virtual Tag getRandomTxsTag() const = 0;

	// Returns the TLogVersion of the current generation of TLogs.
	// (This only exists because getLogSystemConfig is a significantly more expensive call.)
	virtual TLogVersion getTLogVersion() const = 0;

	virtual void stopRejoins() = 0;

	// Returns the pseudo tag to be popped for the given process class. If the
	// process class doesn't use pseudo tag, return the same tag.
	virtual Tag getPseudoPopTag(Tag tag, ProcessClass::ClassType type) = 0;

	virtual bool hasPseudoLocality(int8_t locality) = 0;

	// Returns the actual version to be popped from the log router tag for the given pseudo tag.
	// For instance, a pseudo tag (-8, 2) means the actual popping tag is (-2, 2). Assuming there
	// are multiple pseudo tags, the returned version is the min(all pseudo tags' "upTo" versions).
	virtual Version popPseudoLocalityTag(Tag tag, Version upTo) = 0;

	virtual void setBackupWorkers(const std::vector<InitializeBackupReply>& replies) = 0;

	// Removes a finished backup worker from log system and returns true. Returns false
	// if the worker is not found.
	virtual bool removeBackupWorker(const BackupWorkerDoneRequest& req) = 0;

	virtual LogEpoch getOldestBackupEpoch() const = 0;
	virtual void setOldestBackupEpoch(LogEpoch epoch) = 0;
};

struct LengthPrefixedStringRef {
	// Represents a pointer to a string which is prefixed by a 4-byte length
	// A LengthPrefixedStringRef is only pointer-sized (8 bytes vs 12 bytes for StringRef), but the corresponding string is 4 bytes bigger, and
	// substring operations aren't efficient as they are with StringRef.  It's a good choice when there might be lots of references to the same
	// exact string.

	uint32_t* length;

	StringRef toStringRef() const { ASSERT(length); return StringRef( (uint8_t*)(length+1), *length ); }
	int expectedSize() const { ASSERT(length); return *length; }
	uint32_t* getLengthPtr() const { return length; }

	LengthPrefixedStringRef() : length(nullptr) {}
	LengthPrefixedStringRef(uint32_t* length) : length(length) {}
};

template<class T>
struct CompareFirst {
	bool operator() (T const& lhs, T const& rhs) const {
		return lhs.first < rhs.first;
	}
};

struct LogPushData : NonCopyable {
	// Log subsequences have to start at 1 (the MergedPeekCursor relies on this to make sure we never have !hasMessage() in the middle of data for a version

	explicit LogPushData(Reference<ILogSystem> logSystem) : logSystem(logSystem), subsequence(1) {
		for(auto& log : logSystem->getLogSystemConfig().tLogs) {
			if(log.isLocal) {
				for(int i = 0; i < log.tLogs.size(); i++) {
					messagesWriter.push_back( BinaryWriter( AssumeVersion(currentProtocolVersion) ) );
				}
			}
		}
	}

	void addTxsTag() {
		if ( logSystem->getTLogVersion() >= TLogVersion::V4 ) {
			next_message_tags.push_back( logSystem->getRandomTxsTag() );
		} else {
			next_message_tags.push_back( txsTag );
		}
	}

	// addTag() adds a tag for the *next* message to be added
	void addTag( Tag tag ) {
		next_message_tags.push_back( tag );
	}

	template<class T>
	void addTags(T tags) {
		next_message_tags.insert(next_message_tags.end(), tags.begin(), tags.end());
	}

	void addMessage( StringRef rawMessageWithoutLength, bool usePreviousLocations ) {
		if( !usePreviousLocations ) {
			prev_tags.clear();
			if(logSystem->hasRemoteLogs()) {
				prev_tags.push_back( logSystem->getRandomRouterTag() );
			}
			for(auto& tag : next_message_tags) {
				prev_tags.push_back(tag);
			}
			msg_locations.clear();
			logSystem->getPushLocations( prev_tags, msg_locations );
			next_message_tags.clear();
		}
		uint32_t subseq = this->subsequence++;
		uint32_t msgsize = rawMessageWithoutLength.size() + sizeof(subseq) + sizeof(uint16_t) + sizeof(Tag)*prev_tags.size();
		for(int loc : msg_locations) {
			messagesWriter[loc] << msgsize << subseq << uint16_t(prev_tags.size());
			for(auto& tag : prev_tags)
				messagesWriter[loc] << tag;
			messagesWriter[loc].serializeBytes(rawMessageWithoutLength);
		}
	}

	template <class T>
	void addTypedMessage(T const& item, bool allLocations = false) {
		prev_tags.clear();
		if(logSystem->hasRemoteLogs()) {
			prev_tags.push_back( logSystem->getRandomRouterTag() );
		}
		for(auto& tag : next_message_tags) {
			prev_tags.push_back(tag);
		}
		msg_locations.clear();
		logSystem->getPushLocations(prev_tags, msg_locations, allLocations);

		BinaryWriter bw(AssumeVersion(currentProtocolVersion));
		uint32_t subseq = this->subsequence++;
		bool first = true;
		int firstOffset=-1, firstLength=-1;
		for(int loc : msg_locations) {
			if (first) {
				BinaryWriter& wr = messagesWriter[loc];
				firstOffset = wr.getLength();
				wr << uint32_t(0) << subseq << uint16_t(prev_tags.size());
				for(auto& tag : prev_tags)
					wr << tag;
				wr << item;
				firstLength = wr.getLength() - firstOffset;
				*(uint32_t*)((uint8_t*)wr.getData() + firstOffset) = firstLength - sizeof(uint32_t);
				DEBUG_TAGS_AND_MESSAGE("ProxyPushLocations", invalidVersion, StringRef(((uint8_t*)wr.getData() + firstOffset), firstLength)).detail("PushLocations", msg_locations);
				first = false;
			} else {
				BinaryWriter& wr = messagesWriter[loc];
				BinaryWriter& from = messagesWriter[msg_locations[0]];
				wr.serializeBytes( (uint8_t*)from.getData() + firstOffset, firstLength );
			}
		}
		next_message_tags.clear();
	}

	Standalone<StringRef> getMessages(int loc) {
		return messagesWriter[loc].toValue();
	}

private:
	Reference<ILogSystem> logSystem;
	std::vector<Tag> next_message_tags;
	std::vector<Tag> prev_tags;
	std::vector<BinaryWriter> messagesWriter;
	std::vector<int> msg_locations;
	uint32_t subsequence;
};

#endif
