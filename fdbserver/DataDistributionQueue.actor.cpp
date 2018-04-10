/*
 * DataDistributionQueue.actor.cpp
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

#include "flow/actorcompiler.h"
#include "flow/ActorCollection.h"
#include "fdbrpc/sim_validation.h"
#include "fdbclient/SystemData.h"
#include "DataDistribution.h"
#include "fdbclient/DatabaseContext.h"
#include "MoveKeys.h"
#include "Knobs.h"
#include "fdbrpc/simulator.h"

#define WORK_FULL_UTILIZATION 10000   // This is not a knob; it is a fixed point scaling factor!

struct RelocateData {
	KeyRange keys;
	int priority;
	double startTime;
	UID randomId;
	int workFactor;
	std::vector<UID> src;
	bool wantsNewServers;
	TraceInterval interval;

	RelocateData() : startTime(-1), priority(-1), workFactor(0), wantsNewServers(false), interval("QueuedRelocation") {}
	RelocateData( RelocateShard const& rs ) : keys(rs.keys), priority(rs.priority), startTime(now()), randomId(g_random->randomUniqueID()), workFactor(0),
		wantsNewServers(
			rs.priority == PRIORITY_REBALANCE_SHARD ||
			rs.priority == PRIORITY_REBALANCE_OVERUTILIZED_TEAM ||
			rs.priority == PRIORITY_REBALANCE_UNDERUTILIZED_TEAM ||
			rs.priority == PRIORITY_SPLIT_SHARD ), interval("QueuedRelocation") {}

	bool operator> (const RelocateData& rhs) const {
		return priority != rhs.priority ? priority > rhs.priority : ( startTime != rhs.startTime ? startTime < rhs.startTime : randomId > rhs.randomId );
	}

	bool operator== (const RelocateData& rhs) const {
		return priority == rhs.priority && keys == rhs.keys && startTime == rhs.startTime && workFactor == rhs.workFactor && src == rhs.src && wantsNewServers == rhs.wantsNewServers && randomId == rhs.randomId;
	}

	bool changesBoundaries() {
		return priority == PRIORITY_MERGE_SHARD ||
			priority == PRIORITY_SPLIT_SHARD ||
			priority == PRIORITY_RECOVER_MOVE;
	}
};

struct Busyness {
	vector<int> ledger;

	Busyness() : ledger( 10, 0 ) {}

	bool canLaunch( int prio, int work ) {
		ASSERT( prio > 0 && prio < 1000 );
		return ledger[ prio / 100 ] <= WORK_FULL_UTILIZATION - work;  // allow for rounding errors in double division
	}
	void addWork( int prio, int work ) {
		ASSERT( prio > 0 && prio < 1000 );
		for( int i = 0; i <= (prio / 100); i++ )
			ledger[i] += work;
	}
	void removeWork( int prio, int work ) {
		addWork( prio, -work );
	}
	std::string toString() {
		std::string result;
		for(int i = 1; i < ledger.size();) {
			int j = i+1;
			while(j < ledger.size() && ledger[i] == ledger[j])
				j++;
			if(i != 1)
				result += ", ";
			result += i+1 == j ? format("%03d", i*100) : format("%03d/%03d", i*100, (j-1)*100);
			result += format("=%1.02f", (float)ledger[i] / WORK_FULL_UTILIZATION);
			i = j;
		}
		return result;
	}
};

// find the "workFactor" for this, were it launched now
int getWorkFactor( RelocateData const& relocation ) {
	// Avoid the divide by 0!
	ASSERT( relocation.src.size() );

	if( relocation.priority >= PRIORITY_TEAM_1_LEFT )
		return WORK_FULL_UTILIZATION / SERVER_KNOBS->RELOCATION_PARALLELISM_PER_SOURCE_SERVER;
	else if( relocation.priority >= PRIORITY_TEAM_2_LEFT )
		return WORK_FULL_UTILIZATION / 2 / SERVER_KNOBS->RELOCATION_PARALLELISM_PER_SOURCE_SERVER;
	else // for now we assume that any message at a lower priority can best be assumed to have a full team left for work
		return WORK_FULL_UTILIZATION / relocation.src.size() / SERVER_KNOBS->RELOCATION_PARALLELISM_PER_SOURCE_SERVER;
}

// return true if servers are not too busy to launch the relocation
bool canLaunch( RelocateData & relocation, int teamSize, std::map<UID, Busyness> & busymap,
		std::vector<RelocateData> cancellableRelocations ) {
	// assert this has not already been launched
	ASSERT( relocation.workFactor == 0 );
	ASSERT( relocation.src.size() != 0 );

	// find the "workFactor" for this, were it launched now
	int workFactor = getWorkFactor( relocation );
	int neededServers = std::max( 1, (int)relocation.src.size() - teamSize + 1 );
	// see if each of the SS can launch this task
	for( int i = 0; i < relocation.src.size(); i++ ) {
		// For each source server for this relocation, copy and modify its busyness to reflect work that WOULD be cancelled
		auto busyCopy = busymap[ relocation.src[i] ];
		for( int j = 0; j < cancellableRelocations.size(); j++ ) {
			auto& servers = cancellableRelocations[j].src;
			if( std::count( servers.begin(), servers.end(), relocation.src[i] ) )
				busyCopy.removeWork( cancellableRelocations[j].priority, cancellableRelocations[j].workFactor );
		}
		// Use this modified busyness to check if this relocation could be launched
		if( busyCopy.canLaunch( relocation.priority, workFactor ) ) {
			--neededServers;
			if( neededServers == 0 )
				return true;
		}
	}
	return false;
}

// update busyness for each server
void launch( RelocateData & relocation, std::map<UID, Busyness> & busymap ) {
	// if we are here this means that we can launch and should adjust all the work the servers can do
	relocation.workFactor = getWorkFactor( relocation );
	for( int i = 0; i < relocation.src.size(); i++ )
		busymap[ relocation.src[i] ].addWork( relocation.priority, relocation.workFactor );
}

void complete( RelocateData const& relocation, std::map<UID, Busyness> & busymap ) {
	ASSERT( relocation.workFactor > 0 );
	for( int i = 0; i < relocation.src.size(); i++ )
		busymap[ relocation.src[i] ].removeWork( relocation.priority, relocation.workFactor );
}

Future<Void> dataDistributionRelocator( struct DDQueueData* const& self, RelocateData const& rd );

struct DDQueueData {
	MasterInterface mi;
	MoveKeysLock lock;
	Database cx;

	TeamCollectionInterface teamCollection;
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
	PromiseStream<Promise<int64_t>> getAverageShardBytes;

	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;

	int activeRelocations;
	int queuedRelocations;
	int bytesWritten;
	std::map<int, int> priority_relocations;
	int teamSize;
	int durableStorageQuorum;

	std::map<UID, Busyness> busymap;

	KeyRangeMap< RelocateData > queueMap;
	std::set<RelocateData, std::greater<RelocateData>> fetchingSourcesQueue;
	std::set<RelocateData, std::greater<RelocateData>> fetchKeysComplete;
	KeyRangeActorMap getSourceActors;
	std::map<UID, std::set<RelocateData, std::greater<RelocateData>>> queue;

	KeyRangeMap< RelocateData > inFlight;
	KeyRangeActorMap inFlightActors;

	Promise<Void> error;
	PromiseStream<RelocateData> dataTransferComplete;
	PromiseStream<RelocateData> relocationComplete;
	PromiseStream<RelocateData> fetchSourceServersComplete;

	PromiseStream<RelocateShard> input;
	PromiseStream<GetMetricsRequest> getShardMetrics;

	double* lastLimited;
	double lastInterval;
	int suppressIntervals;

	DDQueueData( MasterInterface mi, MoveKeysLock lock, Database cx, TeamCollectionInterface teamCollection,
		Reference<ShardsAffectedByTeamFailure> sABTF, PromiseStream<Promise<int64_t>> getAverageShardBytes,
		int teamSize, int durableStorageQuorum, PromiseStream<RelocateShard> input,
		PromiseStream<GetMetricsRequest> getShardMetrics, double* lastLimited ) :
			activeRelocations( 0 ), queuedRelocations( 0 ), bytesWritten ( 0 ), teamCollection( teamCollection ),
			shardsAffectedByTeamFailure( sABTF ), getAverageShardBytes( getAverageShardBytes ), mi( mi ), lock( lock ),
			cx( cx ), teamSize( teamSize ), durableStorageQuorum( durableStorageQuorum ), input( input ),
			getShardMetrics( getShardMetrics ), startMoveKeysParallelismLock( SERVER_KNOBS->DD_MOVE_KEYS_PARALLELISM ),
			finishMoveKeysParallelismLock( SERVER_KNOBS->DD_MOVE_KEYS_PARALLELISM ), lastLimited(lastLimited), suppressIntervals(0), lastInterval(0) {}

	void validate() {
		if( EXPENSIVE_VALIDATION ) {
			for( auto it = fetchingSourcesQueue.begin(); it != fetchingSourcesQueue.end(); ++it ) {
				// relocates in the fetching queue do not have src servers yet.
				if( it->src.size() )
					TraceEvent(SevError, "DDQueueValidateError1").detail("Problem", "relocates in the fetching queue do not have src servers yet");

				// relocates in the fetching queue do not have a work factor yet.
				if( it->workFactor != 0.0 )
					TraceEvent(SevError, "DDQueueValidateError2").detail("Problem", "relocates in the fetching queue do not have a work factor yet");

				// relocates in the fetching queue are in the queueMap.
				auto range = queueMap.rangeContaining( it->keys.begin );
				if( range.value() != *it || range.range() != it->keys )
					TraceEvent(SevError, "DDQueueValidateError3").detail("Problem", "relocates in the fetching queue are in the queueMap");
			}

			/*
			for( auto it = queue.begin(); it != queue.end(); ++it ) {
				for( auto rdit = it->second.begin(); rdit != it->second.end(); ++rdit ) {
					// relocates in the queue are in the queueMap exactly.
					auto range = queueMap.rangeContaining( rdit->keys.begin );
					if( range.value() != *rdit || range.range() != rdit->keys )
						TraceEvent(SevError, "DDQueueValidateError4").detail("Problem", "relocates in the queue are in the queueMap exactly")
						.detail("RangeBegin", printable(range.range().begin))
						.detail("RangeEnd", printable(range.range().end))
						.detail("RelocateBegin2", printable(range.value().keys.begin))
						.detail("RelocateEnd2", printable(range.value().keys.end))
						.detail("RelocateStart", range.value().startTime)
						.detail("MapStart", rdit->startTime)
						.detail("RelocateWork", range.value().workFactor)
						.detail("MapWork", rdit->workFactor)
						.detail("RelocateSrc", range.value().src.size())
						.detail("MapSrc", rdit->src.size())
						.detail("RelocatePrio", range.value().priority)
						.detail("MapPrio", rdit->priority);

					// relocates in the queue have src servers
					if( !rdit->src.size() )
						TraceEvent(SevError, "DDQueueValidateError5").detail("Problem", "relocates in the queue have src servers");

					// relocates in the queue do not have a work factor yet.
					if( rdit->workFactor != 0.0 )
						TraceEvent(SevError, "DDQueueValidateError6").detail("Problem", "relocates in the queue do not have a work factor yet");

					bool contains = false;
					for( int i = 0; i < rdit->src.size(); i++ ) {
						if( rdit->src[i] == it->first ) {
							contains = true;
							break;
						}
					}
					if( !contains )
						TraceEvent(SevError, "DDQueueValidateError7").detail("Problem", "queued relocate data does not include ss under which its filed");
				}
			}*/

			auto inFlightRanges = inFlight.ranges();
			for( auto it = inFlightRanges.begin(); it != inFlightRanges.end(); ++it ) {
				for( int i = 0; i < it->value().src.size(); i++ ) {
					// each server in the inFlight map is in the busymap
					if( !busymap.count( it->value().src[i] ) )
						TraceEvent(SevError, "DDQueueValidateError8").detail("Problem", "each server in the inFlight map is in the busymap");

					// relocate data that is inFlight is not also in the queue
					if( queue[it->value().src[i]].count( it->value() ) )
						TraceEvent(SevError, "DDQueueValidateError9").detail("Problem", "relocate data that is inFlight is not also in the queue");
				}

				// in flight relocates have source servers
				if( it->value().startTime != -1 && !it->value().src.size() )
					TraceEvent(SevError, "DDQueueValidateError10").detail("Problem", "in flight relocates have source servers");

				if( inFlightActors.liveActorAt( it->range().begin ) ) {
					// the key range in the inFlight map matches the key range in the RelocateData message
					if( it->value().keys != it->range() )
						TraceEvent(SevError, "DDQueueValidateError11").detail("Problem", "the key range in the inFlight map matches the key range in the RelocateData message");
				}
			}

			for( auto it = busymap.begin(); it != busymap.end(); ++it ) {
				for( int i = 0; i < it->second.ledger.size() - 1; i++ ) {
					if( it->second.ledger[i] < it->second.ledger[i+1] )
						TraceEvent(SevError, "DDQueueValidateError12").detail("Problem", "ascending ledger problem")
						.detail("ledgerLevel", i).detail("ledgerValueA", it->second.ledger[i]).detail("ledgerValueB", it->second.ledger[i+1]);
					if( it->second.ledger[i] < 0.0 )
						TraceEvent(SevError, "DDQueueValidateError13").detail("Problem", "negative ascending problem")
						.detail("ledgerLevel", i).detail("ledgerValue", it->second.ledger[i]);
				}
			}

			std::set<RelocateData, std::greater<RelocateData>> queuedRelocationsMatch;
			for(auto it = queue.begin(); it != queue.end(); ++it)
				queuedRelocationsMatch.insert( it->second.begin(), it->second.end() );
			ASSERT( queuedRelocations == queuedRelocationsMatch.size() + fetchingSourcesQueue.size() );

			int testActive = 0;
			for(auto it = priority_relocations.begin(); it != priority_relocations.end(); ++it )
				testActive += it->second;
			ASSERT( activeRelocations + queuedRelocations == testActive );
		}
	}

	ACTOR Future<Void> getSourceServersForRange( Database cx, MasterInterface mi, RelocateData input, PromiseStream<RelocateData> output ) {
		state std::set<UID> servers;
		state Transaction tr(cx);

		// FIXME: is the merge case needed
		if( input.priority == PRIORITY_MERGE_SHARD ) {
			Void _ = wait( delay( 0.5, TaskDataDistribution - 2 ) );
		} else {
			Void _ = wait( delay( 0.0001, TaskDataDistributionLaunch ) );
		}

		loop {
			servers.clear();
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );
			try {
				Standalone<RangeResultRef> keyServersEntries  = wait(
					tr.getRange( lastLessOrEqual( keyServersKey( input.keys.begin ) ),
						firstGreaterOrEqual( keyServersKey( input.keys.end ) ), SERVER_KNOBS->DD_QUEUE_MAX_KEY_SERVERS ) );

				if(keyServersEntries.size() < SERVER_KNOBS->DD_QUEUE_MAX_KEY_SERVERS) {
					for( int shard = 0; shard < keyServersEntries.size(); shard++ ) {
						vector<UID> src, dest;
						decodeKeyServersValue( keyServersEntries[shard].value, src, dest );
						ASSERT( src.size() );
						for( int i = 0; i < src.size(); i++ )
							servers.insert( src[i] );
					}

					ASSERT(servers.size() > 0);
				}

				//If the size of keyServerEntries is large, then just assume we are using all storage servers
				else {
					Standalone<RangeResultRef> serverList = wait( tr.getRange( serverListKeys, CLIENT_KNOBS->TOO_MANY ) );
					ASSERT( !serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY );

					for(auto s = serverList.begin(); s != serverList.end(); ++s)
						servers.insert(decodeServerListValue( s->value ).id());

					ASSERT(servers.size() > 0);
				}

				break;
			} catch( Error& e ) {
				Void _ = wait( tr.onError(e) );
			}
		}

		input.src = std::vector<UID>( servers.begin(), servers.end() );
		output.send( input );
		return Void();
	}

	//This function cannot handle relocation requests which split a shard into three pieces
	void queueRelocation( RelocateData rd, std::set<UID> &serversToLaunchFrom ) {
		// Update sabtf for changes from DDTracker
		if( rd.changesBoundaries() )
			shardsAffectedByTeamFailure->defineShard( rd.keys );

		//TraceEvent("QueueRelocationBegin").detail("Begin", printable(rd.keys.begin)).detail("End", printable(rd.keys.end));

		// remove all items from both queues that are fully contained in the new relocation (i.e. will be overwritten)
		auto ranges = queueMap.intersectingRanges( rd.keys );
		for(auto r = ranges.begin(); r != ranges.end(); ++r ) {
			RelocateData& rrs = r->value();

			auto fetchingSourcesItr = fetchingSourcesQueue.find(rrs);
			bool foundActiveFetching = fetchingSourcesItr != fetchingSourcesQueue.end();
			std::set<RelocateData, std::greater<RelocateData>>* firstQueue;
			std::set<RelocateData, std::greater<RelocateData>>::iterator firstRelocationItr;
			bool foundActiveRelocation = false;

			if( !foundActiveFetching && rrs.src.size() ) {
				firstQueue = &queue[rrs.src[0]];
				firstRelocationItr = firstQueue->find( rrs );
				foundActiveRelocation = firstRelocationItr != firstQueue->end();
			}

			// If there is a queued job that wants data relocation which we are about to cancel/modify,
			//  make sure that we keep the relocation intent for the job that we queue up
			if( foundActiveFetching || foundActiveRelocation ) {
				rd.wantsNewServers |= rrs.wantsNewServers;
				rd.startTime = std::min( rd.startTime, rrs.startTime );
				if( rrs.priority >= PRIORITY_TEAM_UNHEALTHY && rd.changesBoundaries() )
					rd.priority = std::max( rd.priority, rrs.priority );
			}

			if( rd.keys.contains( rrs.keys ) ) {
				if(foundActiveFetching)
					fetchingSourcesQueue.erase( fetchingSourcesItr );
				else if(foundActiveRelocation) {
					firstQueue->erase( firstRelocationItr );
					for( int i = 1; i < rrs.src.size(); i++ )
						queue[rrs.src[i]].erase( rrs );
				}
			}

			if( foundActiveFetching || foundActiveRelocation ) {
				serversToLaunchFrom.insert( rrs.src.begin(), rrs.src.end() );
				/*TraceEvent(rrs.interval.end(), mi.id()).detail("Result","Cancelled")
					.detail("WasFetching", foundActiveFetching).detail("Contained", rd.keys.contains( rrs.keys ));*/
				queuedRelocations--;
				priority_relocations[ rrs.priority ]--;
			}
		}

		// determine the final state of the relocations map
		auto affectedQueuedItems = queueMap.getAffectedRangesAfterInsertion( rd.keys, rd );

		// put the new request into the global map of requests (modifies the ranges already present)
		queueMap.insert( rd.keys, rd );

		// cancel all the getSourceServers actors that intersect the new range that we will be getting
		getSourceActors.cancel( KeyRangeRef( affectedQueuedItems.front().begin, affectedQueuedItems.back().end ) );

		// update fetchingSourcesQueue and the per-server queue based on truncated ranges after insertion, (re-)launch getSourceServers
		auto queueMapItr = queueMap.rangeContaining(affectedQueuedItems[0].begin);
		for(int r = 0; r < affectedQueuedItems.size(); ++r, ++queueMapItr) {
			//ASSERT(queueMapItr->value() == queueMap.rangeContaining(affectedQueuedItems[r].begin)->value());
			RelocateData& rrs = queueMapItr->value();

			if( rrs.src.size() == 0 && ( rrs.keys == rd.keys || fetchingSourcesQueue.erase(rrs) > 0 ) ) {
				rrs.keys = affectedQueuedItems[r];

				rrs.interval = TraceInterval("QueuedRelocation");
				/*TraceEvent(rrs.interval.begin(), mi.id());
					.detail("KeyBegin", printable(rrs.keys.begin)).detail("KeyEnd", printable(rrs.keys.end))
					.detail("Priority", rrs.priority).detail("WantsNewServers", rrs.wantsNewServers);*/
				queuedRelocations++;
				priority_relocations[rrs.priority]++;

				fetchingSourcesQueue.insert( rrs );
				getSourceActors.insert( rrs.keys, getSourceServersForRange( cx, mi, rrs, fetchSourceServersComplete ) );
			} else {
				RelocateData newData( rrs );
				newData.keys = affectedQueuedItems[r];
				ASSERT( rrs.src.size() || rrs.startTime == -1 );

				bool foundActiveRelocation = false;
				for( int i = 0; i < rrs.src.size(); i++ ) {
					auto& serverQueue = queue[rrs.src[i]];

					if( serverQueue.erase(rrs) > 0 ) {
						if( !foundActiveRelocation ) {
							newData.interval = TraceInterval("QueuedRelocation");
							/*TraceEvent(newData.interval.begin(), mi.id());
								.detail("KeyBegin", printable(newData.keys.begin)).detail("KeyEnd", printable(newData.keys.end))
								.detail("Priority", newData.priority).detail("WantsNewServers", newData.wantsNewServers);*/
							queuedRelocations++;
							priority_relocations[newData.priority]++;
							foundActiveRelocation = true;
						}

						serverQueue.insert( newData );
					}
					else
						break;
				}

				// We update the keys of a relocation even if it is "dead" since it helps validate()
				rrs.keys = affectedQueuedItems[r];
				rrs.interval = newData.interval;
			}
		}

		/*TraceEvent("ReceivedRelocateShard", mi.id())
			.detail("KeyBegin", printable(rd.keys.begin))
			.detail("KeyEnd", printable(rd.keys.end))
			.detail("Priority", rd.priority)
			.detail("AffectedRanges", affectedQueuedItems.size()); */
	}

	void completeSourceFetch( RelocateData results ) {
		ASSERT( fetchingSourcesQueue.count( results ) );

		//logRelocation( results, "GotSourceServers" );

		fetchingSourcesQueue.erase( results );
		queueMap.insert( results.keys, results );
		for( int i = 0; i < results.src.size(); i++ ) {
			queue[results.src[i]].insert( results );
		}
	}

	void logRelocation( RelocateData rd, const char *title ) {
		std::string busyString;
		for(int i = 0; i < rd.src.size() && i < teamSize * 2; i++)
			busyString += describe(rd.src[i]) + " - (" + busymap[ rd.src[i] ].toString() + "); ";

		TraceEvent(title, mi.id())
			.detail("KeyBegin", printable(rd.keys.begin))
			.detail("KeyEnd", printable(rd.keys.end))
			.detail("Priority", rd.priority)
			.detail("WorkFactor", rd.workFactor)
			.detail("SourceServerCount", rd.src.size())
			.detail("SourceServers", describe(rd.src, teamSize * 2))
			.detail("SourceBusyness", busyString);
	}

	void launchQueuedWork( KeyRange keys ) {
		//combine all queued work in the key range and check to see if there is anything to launch
		std::set<RelocateData, std::greater<RelocateData>> combined;
		auto f = queueMap.intersectingRanges( keys );
		for(auto it = f.begin(); it != f.end(); ++it) {
			if( it->value().src.size() && queue[it->value().src[0]].count( it->value() ) )
				combined.insert( it->value() );
		}
		launchQueuedWork( combined );
	}

	void launchQueuedWork( std::set<UID> serversToLaunchFrom ) {
		//combine all work from the source servers to see if there is anything new to launch
		std::set<RelocateData, std::greater<RelocateData>> combined;
		for( auto id : serversToLaunchFrom ) {
			auto& queuedWork = queue[id];
			auto it = queuedWork.begin();
			for( int j = 0; j < teamSize && it != queuedWork.end(); j++) {
				combined.insert( *it );
				++it;
			}
		}
		launchQueuedWork( combined );
	}

	void launchQueuedWork( RelocateData launchData ) {
		//check a single RelocateData to see if it can be launched
		std::set<RelocateData, std::greater<RelocateData>> combined;
		combined.insert( launchData );
		launchQueuedWork( combined );
	}

	void launchQueuedWork( std::set<RelocateData, std::greater<RelocateData>> combined ) {
		int startedHere = 0;
		double startTime = now();
		// kick off relocators from items in the queue as need be
		std::set<RelocateData, std::greater<RelocateData>>::iterator it = combined.begin();
		for(; it != combined.end(); it++ ) {
			RelocateData rd( *it );

			bool overlappingInFlight = false;
			auto intersectingInFlight = inFlight.intersectingRanges( rd.keys );
			for(auto it = intersectingInFlight.begin(); it != intersectingInFlight.end(); ++it) {
				if( fetchKeysComplete.count( it->value() ) &&
					inFlightActors.liveActorAt( it->range().begin ) &&
						!rd.keys.contains( it->range() ) &&
						it->value().priority >= rd.priority &&
						rd.priority < PRIORITY_TEAM_UNHEALTHY ) {
					/*TraceEvent("OverlappingInFlight", mi.id())
						.detail("KeyBegin", printable(it->value().keys.begin))
						.detail("KeyEnd", printable(it->value().keys.end))
						.detail("Priority", it->value().priority); */
					overlappingInFlight = true;
					break;
				}
			}

			if( overlappingInFlight ) {
				//logRelocation( rd, "SkippingOverlappingInFlight" );
				continue;
			}

			// Because the busyness of a server is decreased when a superseding relocation is issued, we
			//  need to consider what the busyness of a server WOULD be if
			auto containedRanges = inFlight.containedRanges( rd.keys );
			std::vector<RelocateData> cancellableRelocations;
			for(auto it = containedRanges.begin(); it != containedRanges.end(); ++it) {
				if( inFlightActors.liveActorAt( it->range().begin ) ) {
					cancellableRelocations.push_back( it->value() );
				}
			}

			// SOMEDAY: the list of source servers may be outdated since they were fetched when the work was put in the queue
			// FIXME: we need spare capacity even when we're just going to be cancelling work via TEAM_HEALTHY
			if( !canLaunch( rd, teamSize, busymap, cancellableRelocations ) ) {
				//logRelocation( rd, "SkippingQueuedRelocation" );
				continue;
			}

			//logRelocation( rd, "LaunchingRelocation" );

			//TraceEvent(rd.interval.end(), mi.id()).detail("Result","Success");
			queuedRelocations--;
			priority_relocations[rd.priority]--;

			// now we are launching: remove this entry from the queue of all the src servers
			for( int i = 0; i < rd.src.size(); i++ ) {
				ASSERT( queue[rd.src[i]].erase(rd) );
			}

			// If there is a job in flight that wants data relocation which we are about to cancel/modify,
			//     make sure that we keep the relocation intent for the job that we launch
			auto f = inFlight.intersectingRanges( rd.keys );
			for(auto it = f.begin(); it != f.end(); ++it) {
				if( inFlightActors.liveActorAt( it->range().begin ) ) {
					rd.wantsNewServers |= it->value().wantsNewServers;
				}
			}
			startedHere++;

			// update both inFlightActors and inFlight key range maps, cancelling deleted RelocateShards
			vector<KeyRange> ranges;
			inFlightActors.getRangesAffectedByInsertion( rd.keys, ranges );
			inFlightActors.cancel( KeyRangeRef( ranges.front().begin, ranges.back().end ) );
			inFlight.insert( rd.keys, rd );
			for(int r=0; r<ranges.size(); r++) {
				RelocateData& rrs = inFlight.rangeContaining(ranges[r].begin)->value();
				rrs.keys = ranges[r];

				launch( rrs, busymap );
				activeRelocations++;
				priority_relocations[ rrs.priority ]++;
				inFlightActors.insert( rrs.keys, dataDistributionRelocator( this, rrs ) );
			}

			//logRelocation( rd, "LaunchedRelocation" );
		}
		if( now() - startTime > .001 && g_random->random01()<0.001 )
			TraceEvent(SevWarnAlways, "LaunchingQueueSlowx1000").detail("elapsed", now() - startTime );

		/*if( startedHere > 0 ) {
			TraceEvent("StartedDDRelocators", mi.id())
				.detail("QueueSize", queuedRelocations)
				.detail("StartedHere", startedHere)
				.detail("ActiveRelocations", activeRelocations);
		} */

		validate();
	}
};

extern bool noUnseed;

// This actor relocates the specified keys to a good place.
// These live in the inFlightActor key range map.
ACTOR Future<Void> dataDistributionRelocator( DDQueueData *self, RelocateData rd )
{
	state Promise<Void> errorOut( self->error );
	state TraceInterval relocateShardInterval("RelocateShard");
	state PromiseStream<RelocateData> dataTransferComplete( self->dataTransferComplete );
	state PromiseStream<RelocateData> relocationComplete( self->relocationComplete );
	state bool signalledTransferComplete = false;
	state UID masterId = self->mi.id();
	state Reference<IDataDistributionTeam> destination;

	try {
		if(now() - self->lastInterval < 1.0) {
			relocateShardInterval.severity = SevDebug;
			self->suppressIntervals++;
		}

		TraceEvent(relocateShardInterval.begin(), masterId)
			.detail("KeyBegin", printable(rd.keys.begin)).detail("KeyEnd", printable(rd.keys.end))
			.detail("Priority", rd.priority).detail("RelocationID", relocateShardInterval.pairID).detail("SuppressedEventCount", self->suppressIntervals);

		if(relocateShardInterval.severity != SevDebug) {
			self->lastInterval = now();
			self->suppressIntervals = 0;
		}

		state StorageMetrics metrics = wait( brokenPromiseToNever( self->getShardMetrics.getReply( GetMetricsRequest( rd.keys ) ) ) );

		ASSERT( rd.src.size() );
		loop {
			state int stuckCount = 0;
			loop {
				double inflightPenalty = SERVER_KNOBS->INFLIGHT_PENALTY_HEALTHY;
				if(rd.priority >= PRIORITY_TEAM_UNHEALTHY) inflightPenalty = SERVER_KNOBS->INFLIGHT_PENALTY_UNHEALTHY;
				if(rd.priority >= PRIORITY_TEAM_1_LEFT) inflightPenalty = SERVER_KNOBS->INFLIGHT_PENALTY_ONE_LEFT;

				auto req = GetTeamRequest( rd.wantsNewServers, rd.priority == PRIORITY_REBALANCE_UNDERUTILIZED_TEAM, true, inflightPenalty );
				req.sources = rd.src;
				Optional<Reference<IDataDistributionTeam>> bestTeam = wait( brokenPromiseToNever( self->teamCollection.getTeam.getReply( req ) ) );
				if( bestTeam.present() ) {
					destination = bestTeam.get();
					break;
				}
				TEST(true); //did not find a healthy destination team on the first attempt
				stuckCount++;
				TraceEvent(stuckCount > 50 ? SevWarnAlways : SevWarn, "BestTeamStuck", masterId).detail("Count", stuckCount).suppressFor(1.0);
				if(stuckCount > 50 && g_network->isSimulated()) { //FIXME: known bug in simulation we are supressing
					int unseed = noUnseed ? 0 : g_random->randomInt(0, 100001);
					TraceEvent("ElapsedTime").detail("SimTime", now()).detail("RealTime", 0)
						.detail("RandomUnseed", unseed);
					flushAndExit(0);
				}
				Void _ = wait( delay( SERVER_KNOBS->BEST_TEAM_STUCK_DELAY, TaskDataDistributionLaunch ) );
			}

			ASSERT(destination->isHealthy());   // team failure tracking is edge triggered, so must never put something on an unhealthy team!
			self->shardsAffectedByTeamFailure->moveShard( rd.keys, destination->getServerIDs() );

			destination->addDataInFlightToTeam( +metrics.bytes );

			TraceEvent(relocateShardInterval.severity, "RelocateShardHasDestination", masterId)
				.detail("PairId", relocateShardInterval.pairID)
				.detail("DestinationTeam", destination->getDesc());

			state Error error = success();
			state Promise<Void> dataMovementComplete;
			state Future<Void> doMoveKeys = moveKeys(
				self->cx, rd.keys, destination->getServerIDs(), self->lock,
				self->durableStorageQuorum, dataMovementComplete,
				&self->startMoveKeysParallelismLock,
				&self->finishMoveKeysParallelismLock,
				relocateShardInterval.pairID );
			state Future<Void> pollHealth = signalledTransferComplete ? Never() : delay( SERVER_KNOBS->HEALTH_POLL_TIME, TaskDataDistributionLaunch );
			try {
				loop {
					choose {
						when( Void _ = wait( doMoveKeys ) ) {
							self->fetchKeysComplete.insert( rd );
							break;
						}
						when( Void _ = wait( pollHealth ) ) {
							if( !destination->isHealthy() ) {
								if( !signalledTransferComplete ) {
									signalledTransferComplete = true;
									self->dataTransferComplete.send( rd );
								}
							}
							pollHealth = signalledTransferComplete ? Never() : delay( SERVER_KNOBS->HEALTH_POLL_TIME, TaskDataDistributionLaunch );
						}
						when( Void _ = wait( signalledTransferComplete ? Never() : dataMovementComplete.getFuture() ) ) {
							self->fetchKeysComplete.insert( rd );
							if( !signalledTransferComplete ) {
								signalledTransferComplete = true;
								self->dataTransferComplete.send( rd );
							}
						}
					}
				}
			} catch( Error& e ) {
				error = e;
			}

			//TraceEvent("RelocateShardFinished", masterId).detail("relocateId", relocateShardInterval.pairID);

			if( error.code() != error_code_move_to_removed_server ) {
				if( !error.code() ) {
					try {
						Void _ = wait( destination->updatePhysicalMetrics() ); //prevent a gap between the polling for an increase in physical metrics and decrementing data in flight
					} catch( Error& e ) {
						error = e;
					}
				}

				destination->addDataInFlightToTeam( -metrics.bytes );

				// onFinished.send( rs );
				if( !error.code() ) {
					TraceEvent(relocateShardInterval.end(), masterId).detail("Result","Success");
					if(rd.keys.begin == keyServersPrefix) {
						TraceEvent("MovedKeyServerKeys").detail("dest", destination->getDesc()).trackLatest("MovedKeyServers");
					}

					if( !signalledTransferComplete ) {
						signalledTransferComplete = true;
						dataTransferComplete.send( rd );
					}

					self->bytesWritten += metrics.bytes;
					relocationComplete.send( rd );
					return Void();
				} else {
					throw error;
				}
			} else {
				TEST(true);  // move to removed server
				destination->addDataInFlightToTeam( -metrics.bytes );
				Void _ = wait( delay( SERVER_KNOBS->RETRY_RELOCATESHARD_DELAY, TaskDataDistributionLaunch ) );
			}
		}
	} catch (Error& e) {
		TraceEvent(relocateShardInterval.end(), masterId).error(e, true);
		if( !signalledTransferComplete )
			dataTransferComplete.send( rd );

		relocationComplete.send( rd );

		if( e.code() != error_code_actor_cancelled )
			errorOut.sendError(e);
		throw;
	}
}

ACTOR Future<bool> rebalanceTeams( DDQueueData* self, int priority, Reference<IDataDistributionTeam> sourceTeam, Reference<IDataDistributionTeam> destTeam ) {
	if(g_network->isSimulated() && g_simulator.speedUpSimulation) {
		return false;
	}

	std::vector<KeyRange> shards = self->shardsAffectedByTeamFailure->getShardsFor( sourceTeam->getServerIDs() );

	if( !shards.size() )
		return false;

	state KeyRange moveShard = g_random->randomChoice( shards );
	StorageMetrics metrics = wait( brokenPromiseToNever( self->getShardMetrics.getReply(GetMetricsRequest(moveShard)) ) );

	int64_t sourceBytes = sourceTeam->getLoadBytes(false);
	int64_t destBytes = destTeam->getLoadBytes();
	if( sourceBytes - destBytes <= 3 * std::max<int64_t>( SERVER_KNOBS->MIN_SHARD_BYTES, metrics.bytes ) || metrics.bytes == 0 )
		return false;

	//verify the shard is still in sabtf
	std::vector<KeyRange> shards = self->shardsAffectedByTeamFailure->getShardsFor( sourceTeam->getServerIDs() );
	for( int i = 0; i < shards.size(); i++ ) {
		if( moveShard == shards[i] ) {
			TraceEvent(priority == PRIORITY_REBALANCE_OVERUTILIZED_TEAM ? "BgDDMountainChopper" : "BgDDValleyFiller", self->mi.id())
				.detail("sourceBytes", sourceBytes)
				.detail("destBytes", destBytes)
				.detail("shardBytes", metrics.bytes)
				.detail("sourceTeam", sourceTeam->getDesc())
				.detail("destTeam", destTeam->getDesc());

			self->input.send( RelocateShard( moveShard, priority ) );
			return true;
		}
	}

	return false;
}

ACTOR Future<Void> BgDDMountainChopper( DDQueueData* self ) {
	state double checkDelay = SERVER_KNOBS->BG_DD_POLLING_INTERVAL;
	state int resetCount = SERVER_KNOBS->DD_REBALANCE_RESET_AMOUNT;
	loop {
		Void _ = wait( delay(checkDelay, TaskDataDistributionLaunch) );
		if (self->priority_relocations[PRIORITY_REBALANCE_OVERUTILIZED_TEAM] < SERVER_KNOBS->DD_REBALANCE_PARALLELISM) {
			state Optional<Reference<IDataDistributionTeam>> randomTeam = wait( brokenPromiseToNever( self->teamCollection.getTeam.getReply( GetTeamRequest( true, false, true ) ) ) );
			if( randomTeam.present() ) {
				if( randomTeam.get()->getMinFreeSpaceRatio() > SERVER_KNOBS->FREE_SPACE_RATIO_DD_CUTOFF ) {
					state Optional<Reference<IDataDistributionTeam>> loadedTeam = wait( brokenPromiseToNever( self->teamCollection.getTeam.getReply( GetTeamRequest( true, true, false ) ) ) );
					if( loadedTeam.present() ) {
						bool moved = wait( rebalanceTeams( self, PRIORITY_REBALANCE_OVERUTILIZED_TEAM, loadedTeam.get(), randomTeam.get() ) );
						if(moved) {
							resetCount = 0;
						} else {
							resetCount++;
						}
					}
				}
			}
		}

		if( now() - (*self->lastLimited) < SERVER_KNOBS->BG_DD_SATURATION_DELAY ) {
			checkDelay = std::min(SERVER_KNOBS->BG_DD_MAX_WAIT, checkDelay * SERVER_KNOBS->BG_DD_INCREASE_RATE);
		} else {
			checkDelay = std::max(SERVER_KNOBS->BG_DD_MIN_WAIT, checkDelay / SERVER_KNOBS->BG_DD_DECREASE_RATE);
		}

		if(resetCount >= SERVER_KNOBS->DD_REBALANCE_RESET_AMOUNT && checkDelay < SERVER_KNOBS->BG_DD_POLLING_INTERVAL) {
			checkDelay = SERVER_KNOBS->BG_DD_POLLING_INTERVAL;
			resetCount = SERVER_KNOBS->DD_REBALANCE_RESET_AMOUNT;
		}
	}
}

ACTOR Future<Void> BgDDValleyFiller( DDQueueData* self ) {
	state double checkDelay = SERVER_KNOBS->BG_DD_POLLING_INTERVAL;
	state int resetCount = SERVER_KNOBS->DD_REBALANCE_RESET_AMOUNT;
	loop {
		Void _ = wait( delay(checkDelay, TaskDataDistributionLaunch) );
		if (self->priority_relocations[PRIORITY_REBALANCE_UNDERUTILIZED_TEAM] < SERVER_KNOBS->DD_REBALANCE_PARALLELISM) {
			state Optional<Reference<IDataDistributionTeam>> randomTeam = wait( brokenPromiseToNever( self->teamCollection.getTeam.getReply( GetTeamRequest( true, false, false ) ) ) );
			if( randomTeam.present() ) {
				state Optional<Reference<IDataDistributionTeam>> unloadedTeam = wait( brokenPromiseToNever( self->teamCollection.getTeam.getReply( GetTeamRequest( true, true, true ) ) ) );
				if( unloadedTeam.present() ) {
					if( unloadedTeam.get()->getMinFreeSpaceRatio() > SERVER_KNOBS->FREE_SPACE_RATIO_DD_CUTOFF ) {
						bool moved = wait( rebalanceTeams( self, PRIORITY_REBALANCE_UNDERUTILIZED_TEAM, randomTeam.get(), unloadedTeam.get() ) );
						if(moved) {
							resetCount = 0;
						} else {
							resetCount++;
						}
					}
				}
			}
		}

		if( now() - (*self->lastLimited) < SERVER_KNOBS->BG_DD_SATURATION_DELAY ) {
			checkDelay = std::min(SERVER_KNOBS->BG_DD_MAX_WAIT, checkDelay * SERVER_KNOBS->BG_DD_INCREASE_RATE);
		} else {
			checkDelay = std::max(SERVER_KNOBS->BG_DD_MIN_WAIT, checkDelay / SERVER_KNOBS->BG_DD_DECREASE_RATE);
		}

		if(resetCount >= SERVER_KNOBS->DD_REBALANCE_RESET_AMOUNT && checkDelay < SERVER_KNOBS->BG_DD_POLLING_INTERVAL) {
			checkDelay = SERVER_KNOBS->BG_DD_POLLING_INTERVAL;
			resetCount = SERVER_KNOBS->DD_REBALANCE_RESET_AMOUNT;
		}
	}
}

ACTOR Future<Void> dataDistributionQueue(
	Database cx,
	PromiseStream<RelocateShard> input,
	PromiseStream<GetMetricsRequest> getShardMetrics,
	TeamCollectionInterface teamCollection,
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure,
	MoveKeysLock lock,
	PromiseStream<Promise<int64_t>> getAverageShardBytes,
	MasterInterface mi,
	int teamSize,
	int durableStorageQuorum,
	double* lastLimited)
{
	state DDQueueData self( mi, lock, cx, teamCollection, shardsAffectedByTeamFailure, getAverageShardBytes, teamSize, durableStorageQuorum, input, getShardMetrics, lastLimited );
	state std::set<UID> serversToLaunchFrom;
	state KeyRange keysToLaunchFrom;
	state RelocateData launchData;
	state Future<Void> recordMetrics = delay(SERVER_KNOBS->DD_QUEUE_LOGGING_INTERVAL);
	state Future<Void> bgDDMountainChopper = BgDDMountainChopper( &self );
	state Future<Void> bgDDValleyFiller = BgDDValleyFiller( &self );

	state ActorCollectionNoErrors actors;
	state PromiseStream<KeyRange> rangesComplete;
	state Future<Void> launchQueuedWorkTimeout = Never();

	try {
		loop {
			self.validate();

			// For the given servers that caused us to go around the loop, find the next item(s) that can be launched.
			if( launchData.startTime != -1 ) {
				self.launchQueuedWork( launchData );
				launchData = RelocateData();
			}
			else if( !keysToLaunchFrom.empty() ) {
				self.launchQueuedWork( keysToLaunchFrom );
				keysToLaunchFrom = KeyRangeRef();
			}

			ASSERT( launchData.startTime == -1 && keysToLaunchFrom.empty() );

			choose {
				when ( RelocateShard rs = waitNext( self.input.getFuture() ) ) {
					bool wasEmpty = serversToLaunchFrom.empty();
					self.queueRelocation( rs, serversToLaunchFrom );
					if(wasEmpty && !serversToLaunchFrom.empty())
						launchQueuedWorkTimeout = delay(0, TaskDataDistributionLaunch);
				}
				when ( Void _ = wait(launchQueuedWorkTimeout) ) {
					self.launchQueuedWork( serversToLaunchFrom );
					serversToLaunchFrom = std::set<UID>();
					launchQueuedWorkTimeout = Never();
				}
				when ( RelocateData results = waitNext( self.fetchSourceServersComplete.getFuture() ) ) {
					self.completeSourceFetch( results );
					launchData = results;
				}
				when ( RelocateData done = waitNext( self.dataTransferComplete.getFuture() ) ) {
					complete( done, self.busymap );
					if(serversToLaunchFrom.empty() && !done.src.empty())
						launchQueuedWorkTimeout = delay(0, TaskDataDistributionLaunch);
					serversToLaunchFrom.insert(done.src.begin(), done.src.end());
				}
				when ( RelocateData done = waitNext( self.relocationComplete.getFuture() ) ) {
					self.activeRelocations--;
					self.priority_relocations[ done.priority ]--;
					self.fetchKeysComplete.erase( done );
					//self.logRelocation( done, "ShardRelocatorDone" );
					actors.add( tag( delay(0, TaskDataDistributionLaunch), done.keys, rangesComplete ) );
					if( g_network->isSimulated() && debug_isCheckRelocationDuration() && now() - done.startTime > 60 ) {
						TraceEvent(SevWarnAlways, "RelocationDurationTooLong").detail("Duration", now() - done.startTime);
						debug_setCheckRelocationDuration(false);
					}
				}
				when ( KeyRange done = waitNext( rangesComplete.getFuture() ) ) {
					keysToLaunchFrom = done;
				}
				when ( Void _ = wait( recordMetrics ) ) {
					Promise<int64_t> req;
					getAverageShardBytes.send( req );

					recordMetrics = delay(SERVER_KNOBS->DD_QUEUE_LOGGING_INTERVAL);

					int lowPriorityRelocations = 0, highPriorityRelocations = 0, highestPriorityRelocation = 0;
					for( auto it = self.priority_relocations.begin(); it != self.priority_relocations.end(); ++it ) {
						if (it->second)
							highestPriorityRelocation = std::max(highestPriorityRelocation, it->first);
						if( it->first < 200 )
							lowPriorityRelocations += it->second;
						else
							highPriorityRelocations += it->second;
					}

					TraceEvent("MovingData", mi.id())
						.detail( "InFlight", self.activeRelocations )
						.detail( "InQueue", self.queuedRelocations )
						.detail( "AverageShardSize", req.getFuture().isReady() ? req.getFuture().get() : -1 )
						.detail( "LowPriorityRelocations", lowPriorityRelocations )
						.detail( "HighPriorityRelocations", highPriorityRelocations )
						.detail( "HighestPriority", highestPriorityRelocation )
						.detail( "BytesWritten", self.bytesWritten )
						.trackLatest( format("%s/MovingData", printable(cx->dbName).c_str() ).c_str() );
				}
				when ( Void _ = wait( self.error.getFuture() ) ) {}  // Propagate errors from dataDistributionRelocator
				when ( Void _ = wait( bgDDMountainChopper ) ) {}
				when ( Void _ = wait( bgDDValleyFiller ) ) {}
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_broken_promise && // FIXME: Get rid of these broken_promise errors every time we are killed by the master dying
			e.code() != error_code_movekeys_conflict)
			TraceEvent(SevError, "dataDistributionQueueError", mi.id()).error(e);
		throw e;
	}
}
