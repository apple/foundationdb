/*
 * WriteDuringRead.actor.cpp
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
#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/ActorCollection.h"
#include "workloads.h"
#include "fdbclient/Atomic.h"

struct WriteDuringReadWorkload : TestWorkload {
	double testDuration, slowModeStart;
	int numOps;
	bool rarelyCommit, adjacentKeys;
	PerfIntCounter transactions, retries;
	std::map<Key, Value> memoryDatabase;
	std::map<Key, Value> lastCommittedDatabase;
	KeyRangeMap<int> changeCount;
	int minNode, nodes;
	double initialKeyDensity;
	AsyncTrigger finished;
	KeyRange conflictRange;
	std::pair<int,int> valueSizeRange;
	int maxClearSize;
	CoalescedKeyRangeMap<bool> addedConflicts;
	bool useSystemKeys;
	std::string keyPrefix;
	int64_t maximumTotalData;

	bool success;
	Database extraDB;
	bool useExtraDB;

	WriteDuringReadWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx), transactions("Transactions"), retries("Retries"), success(true) {
		testDuration = getOption( options, LiteralStringRef("testDuration"), 60.0 );
		slowModeStart = getOption( options, LiteralStringRef("slowModeStart"), 1000.0 );
		numOps = getOption( options, LiteralStringRef("numOps"), 21 );
		rarelyCommit = getOption( options, LiteralStringRef("rarelyCommit"), false );
		maximumTotalData = getOption( options, LiteralStringRef("maximumTotalData"), 7e6);
		minNode = getOption( options, LiteralStringRef("minNode"), 0);
		useSystemKeys = getOption( options, LiteralStringRef("useSystemKeys"), g_random->random01() < 0.5);
		adjacentKeys = g_random->random01() < 0.5;
		initialKeyDensity = g_random->random01(); // This fraction of keys are present before the first transaction (and after an unknown result)
		valueSizeRange = std::make_pair( 0, std::min<int>( g_random->randomInt(0, 4 << g_random->randomInt(0,16)), CLIENT_KNOBS->VALUE_SIZE_LIMIT * 1.2 ) );
		if( adjacentKeys ) {
			nodes = std::min<int64_t>( g_random->randomInt(1, 4 << g_random->randomInt(0,14)), CLIENT_KNOBS->KEY_SIZE_LIMIT * 1.2 );
		}
		else {
			nodes = g_random->randomInt(1, 4 << g_random->randomInt(0,20));
		}

		int newNodes = std::min<int>(nodes, maximumTotalData / (getKeyForIndex(nodes).size() + valueSizeRange.second));
		minNode = std::max(minNode, nodes - newNodes);
		nodes = newNodes;

		TEST(adjacentKeys && (nodes + minNode) > CLIENT_KNOBS->KEY_SIZE_LIMIT); //WriteDuringReadWorkload testing large keys
		
		useExtraDB = g_simulator.extraDB != NULL;
		if(useExtraDB) {
			Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
			Reference<Cluster> extraCluster = Cluster::createCluster(extraFile, -1);
			extraDB = extraCluster->createDatabase(LiteralStringRef("DB")).get();
			useSystemKeys = false;
		}

		if(useSystemKeys && g_random->random01() < 0.5) {
			keyPrefix = "\xff\x01";
		} else {
			keyPrefix = "\x02";
		}

		maxClearSize = 1<<g_random->randomInt(0, 20);
		conflictRange = KeyRangeRef( LiteralStringRef("\xfe"), LiteralStringRef("\xfe\x00") );
		if( clientId == 0 )
			TraceEvent("RYWConfiguration").detail("nodes", nodes).detail("initialKeyDensity", initialKeyDensity).detail("adjacentKeys", adjacentKeys).detail("valueSizeMin", valueSizeRange.first).detail("valueSizeMax", valueSizeRange.second).detail("maxClearSize", maxClearSize);
	}

	virtual std::string description() { return "WriteDuringRead"; }

	virtual Future<Void> setup( Database const& cx ) { 
		return Void();
	}

	virtual Future<Void> start( Database const& cx ) { 
		if( clientId == 0 )
			return loadAndRun( cx, this );
		return Void();
	}

	virtual Future<bool> check( Database const& cx ) {
		return success;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
		m.push_back( transactions.getMetric() );
		m.push_back( retries.getMetric() );
	}

	Key memoryGetKey( std::map<Key, Value> *db, KeySelector key ) {
		std::map<Key, Value>::iterator iter;
		if( key.orEqual )
			iter = db->upper_bound( key.getKey() );
		else
			iter = db->lower_bound( key.getKey() );
		
		int offset = key.offset - 1;
		while( offset > 0 ) {
			if( iter == db->end() ) return useSystemKeys ? allKeys.end : normalKeys.end;
			++iter;
			--offset;
		}
		while( offset < 0 ) {
			if( iter == db->begin() ) return allKeys.begin;
			--iter;
			++offset;
		}
		if( iter == db->end() ) return useSystemKeys ? allKeys.end : normalKeys.end;
		return iter->first;
	}

	ACTOR Future<Void> getKeyAndCompare( ReadYourWritesTransaction *tr, KeySelector key, bool snapshot, bool readYourWritesDisabled, bool snapshotRYWDisabled, WriteDuringReadWorkload* self, bool *doingCommit, int64_t* memLimit ) {
		state UID randomID = g_nondeterministic_random->randomUniqueID();
		//TraceEvent("WDRGetKey", randomID);
		try {
			state Key memRes = self->memoryGetKey( readYourWritesDisabled || (snapshot && snapshotRYWDisabled) ? &self->lastCommittedDatabase : &self->memoryDatabase, key );
			*memLimit -= memRes.expectedSize();
			Key _res = wait( tr->getKey( key, snapshot ) );
			Key res = _res;
			*memLimit += memRes.expectedSize();
			if( self->useSystemKeys && res > self->getKeyForIndex(self->nodes) )
				res = allKeys.end;
			if( res != memRes ) {
				TraceEvent(SevError, "WDRGetKeyWrongResult", randomID).detail("Key", printable(key.getKey())).detail("Offset", key.offset).detail("orEqual", key.orEqual)
					.detail("Snapshot", snapshot).detail("MemoryResult", printable(memRes)).detail("DbResult", printable(res));
				self->success = false;
			}
			return Void();
		} catch( Error &e ) {
			//TraceEvent("WDRGetKeyError", randomID).error(e,true);
			if( e.code() == error_code_used_during_commit ) {
				ASSERT( *doingCommit );
				return Void();
			} else if( e.code() == error_code_transaction_cancelled )
				return Void();
			throw;
		}
	}

	Standalone<VectorRef<KeyValueRef>> memoryGetRange( std::map<Key, Value> *db, KeySelector begin, KeySelector end, GetRangeLimits limit, bool reverse ) {
		Key beginKey = memoryGetKey( db, begin );
		Key endKey = memoryGetKey( db, end );
		//TraceEvent("WDRGetRange").detail("begin", printable(beginKey)).detail("end", printable(endKey));
		if( beginKey >= endKey )
			return Standalone<VectorRef<KeyValueRef>>();

		auto beginIter = db->lower_bound(beginKey);
		auto endIter = db->lower_bound(endKey);

		Standalone<VectorRef<KeyValueRef>> results;
		if( reverse ) {
			loop {
				if(beginIter == endIter || limit.reachedBy( results ))
					break;

				--endIter;
				results.push_back_deep(results.arena(), KeyValueRef(endIter->first, endIter->second) );
			}
		} else {
			for(; beginIter != endIter && !limit.reachedBy(results); ++beginIter )
				results.push_back_deep( results.arena(), KeyValueRef( beginIter->first, beginIter->second ) );
		}
		return results;
	}

	ACTOR Future<Void> getRangeAndCompare( ReadYourWritesTransaction *tr, KeySelector begin, KeySelector end, GetRangeLimits limit, bool snapshot, bool reverse, bool readYourWritesDisabled, bool snapshotRYWDisabled, WriteDuringReadWorkload* self, bool* doingCommit, int64_t* memLimit ) {
		state UID randomID = g_nondeterministic_random->randomUniqueID();
		/*TraceEvent("WDRGetRange", randomID).detail("BeginKey", printable(begin.getKey())).detail("BeginOffset", begin.offset).detail("BeginOrEqual", begin.orEqual)
			.detail("EndKey", printable(end.getKey())).detail("EndOffset", end.offset).detail("EndOrEqual", end.orEqual)
			.detail("Limit", limit.rows).detail("Snapshot", snapshot).detail("Reverse", reverse).detail("ReadYourWritesDisabled", readYourWritesDisabled);*/

		try {
			state Standalone<VectorRef<KeyValueRef>> memRes = self->memoryGetRange( readYourWritesDisabled || (snapshot && snapshotRYWDisabled) ? &self->lastCommittedDatabase : &self->memoryDatabase, begin, end, limit, reverse );
			*memLimit -= memRes.expectedSize();
			Standalone<RangeResultRef> _res = wait( tr->getRange( begin, end, limit, snapshot, reverse ) );
			Standalone<RangeResultRef> res = _res;
			*memLimit += memRes.expectedSize();
			
			int systemKeyCount = 0;
			bool resized = false;
			if( self->useSystemKeys ) {
				if( !reverse ) {
					int newSize = std::lower_bound( res.begin(), res.end(), self->getKeyForIndex(self->nodes), KeyValueRef::OrderByKey() ) - res.begin();
					if( newSize != res.size() ) {
						res.resize( res.arena(), newSize );
						resized = true;
					}
				} else {
					for(; systemKeyCount < res.size(); systemKeyCount++ )
						if( res[systemKeyCount].key < self->getKeyForIndex(self->nodes) )
							break;
					if( systemKeyCount > 0 ) {
						res = RangeResultRef( VectorRef<KeyValueRef>( &res[systemKeyCount], res.size() - systemKeyCount ), true );
						resized = true;
					}
				}
			}

			if( !limit.hasByteLimit() && systemKeyCount == 0 ) {
				if( res.size() != memRes.size() ) {
					TraceEvent(SevError, "WDRGetRangeWrongResult", randomID)
						.detail("BeginKey", printable(begin.getKey())).detail("BeginOffset", begin.offset).detail("BeginOrEqual", begin.orEqual)
						.detail("EndKey", printable(end.getKey())).detail("EndOffset", end.offset).detail("EndOrEqual", end.orEqual)
						.detail("LimitRows", limit.rows).detail("LimitBytes", limit.bytes).detail("Snapshot", snapshot).detail("Reverse", reverse).detail("MemorySize", memRes.size()).detail("DbSize", res.size())
						.detail("ReadYourWritesDisabled", readYourWritesDisabled);

					self->success = false;
					return Void();
				}

				for( int i = 0; i < res.size(); i++ ) {
					if( res[i] != memRes[i] ) {
						TraceEvent(SevError, "WDRGetRangeWrongResult", randomID)
							.detail("BeginKey", printable(begin.getKey())).detail("BeginOffset", begin.offset).detail("BeginOrEqual", begin.orEqual)
							.detail("EndKey", printable(end.getKey())).detail("EndOffset", end.offset).detail("EndOrEqual", end.orEqual)
							.detail("LimitRows", limit.rows).detail("LimitBytes", limit.bytes).detail("Snapshot", snapshot).detail("Reverse", reverse).detail("Size", memRes.size()).detail("WrongLocation", i)
							.detail("MemoryResultKey", printable(memRes[i].key)).detail("DbResultKey", printable(res[i].key))
							.detail("MemoryResultValueSize", memRes[i].value.size() ).detail("DbResultValueSize", res[i].value.size())
							.detail("ReadYourWritesDisabled", readYourWritesDisabled);
						self->success = false;
						return Void();
					}
				}
			} else {
				if( res.size() > memRes.size() || (res.size() < memRes.size() && !res.more) || (res.size() == 0 && res.more && !resized) ) {
					TraceEvent(SevError, "WDRGetRangeWrongResult", randomID)
						.detail("BeginKey", printable(begin.getKey())).detail("BeginOffset", begin.offset).detail("BeginOrEqual", begin.orEqual)
						.detail("EndKey", printable(end.getKey())).detail("EndOffset", end.offset).detail("EndOrEqual", end.orEqual)
						.detail("LimitRows", limit.rows).detail("LimitBytes", limit.bytes).detail("Snapshot", snapshot).detail("Reverse", reverse).detail("MemorySize", memRes.size()).detail("DbSize", res.size())
						.detail("ReadYourWritesDisabled", readYourWritesDisabled).detail("more", res.more).detail("systemKeyCount", systemKeyCount);

					self->success = false;
					return Void();
				}

				for( int i = 0; i < res.size(); i++ ) {
					if( res[i] != memRes[i] ) {
						TraceEvent(SevError, "WDRGetRangeWrongResult", randomID)
							.detail("BeginKey", printable(begin.getKey())).detail("BeginOffset", begin.offset).detail("BeginOrEqual", begin.orEqual)
							.detail("EndKey", printable(end.getKey())).detail("EndOffset", end.offset).detail("EndOrEqual", end.orEqual)
							.detail("LimitRows", limit.rows).detail("LimitBytes", limit.bytes).detail("Snapshot", snapshot).detail("Reverse", reverse).detail("Size", memRes.size()).detail("WrongLocation", i)
							.detail("MemoryResultKey", printable(memRes[i].key)).detail("DbResultKey", printable(res[i].key))
							.detail("MemoryResultValueSize", memRes[i].value.size() ).detail("DbResultValueSize", res[i].value.size())
							.detail("ReadYourWritesDisabled", readYourWritesDisabled).detail("more", res.more);
						self->success = false;
						return Void();
					}
				}
			}
			return Void();
		} catch( Error &e ) {
			//TraceEvent("WDRGetRangeError", randomID).error(e,true);
			if( e.code() == error_code_used_during_commit ) {
				ASSERT( *doingCommit );
				return Void();
			} else if( e.code() == error_code_transaction_cancelled )
				return Void();
			throw;
		}
	}

	Optional<Value> memoryGet( std::map<Key, Value> *db, Key key ) {
		auto iter = db->find( key );
		if( iter == db->end() )
			return Optional<Value>();
		else
			return iter->second;
	}

	ACTOR Future<Void> getAndCompare( ReadYourWritesTransaction *tr, Key key, bool snapshot, bool readYourWritesDisabled, bool snapshotRYWDisabled, WriteDuringReadWorkload* self, bool* doingCommit, int64_t* memLimit ) {
		state UID randomID = g_nondeterministic_random->randomUniqueID();
		//TraceEvent("WDRGet", randomID);
		try {
			state Optional<Value> memRes = self->memoryGet( readYourWritesDisabled || (snapshot && snapshotRYWDisabled) ? &self->lastCommittedDatabase : &self->memoryDatabase, key );
			*memLimit -= memRes.expectedSize();
			Optional<Value> res = wait( tr->get( key, snapshot ) );
			*memLimit += memRes.expectedSize();
			if( res != memRes ) {
				TraceEvent(SevError, "WDRGetWrongResult", randomID).detail("Key", printable(key)).detail("Snapshot", snapshot).detail("MemoryResult", memRes.present() ? memRes.get().size() : -1 ).detail("DbResult",  res.present() ? res.get().size() : -1 ).detail("rywDisable", readYourWritesDisabled);
				self->success = false;
			}
			return Void();
		} catch( Error &e ) {
			//TraceEvent("WDRGetError", randomID).error(e,true);
			if( e.code() == error_code_used_during_commit ) {
				ASSERT( *doingCommit );
				return Void();
			} else if( e.code() == error_code_transaction_cancelled )
				return Void();
			throw;
		}
	}

	ACTOR Future<Void> watchAndCompare( ReadYourWritesTransaction *tr, Key key, bool readYourWritesDisabled, WriteDuringReadWorkload* self, bool* doingCommit, int64_t* memLimit ) {
		state UID randomID = g_nondeterministic_random->randomUniqueID();
		//SOMEDAY: test setting a low outstanding watch limit
		if( readYourWritesDisabled ) //Only tests RYW activated watches
			return Void();
		
		//TraceEvent("WDRWatch", randomID).detail("Key", printable(key));
		try {
			state int changeNum = self->changeCount[key];
			state Optional<Value> memRes = self->memoryGet( &self->memoryDatabase, key );
			*memLimit -= memRes.expectedSize();
			choose {
				when( Void _ = wait( tr->watch( key ) ) ) {
					if( changeNum == self->changeCount[key] ) {
						TraceEvent(SevError, "WDRWatchWrongResult", randomID).detail("Reason", "Triggered without changing").detail("Key", printable(key)).detail("Value", changeNum).detail("duringCommit", *doingCommit);
					}
				}
				when( Void _ = wait( self->finished.onTrigger() ) ) {
					Optional<Value> memRes2 = self->memoryGet( &self->memoryDatabase, key );
					if( memRes != memRes2 ) {
						TraceEvent(SevError, "WDRWatchWrongResult", randomID).detail("Reason", "Changed without triggering").detail("Key", printable(key)).detail("Value1", printable(memRes)).detail("Value2", printable(memRes2));
					}
				}
			}
			*memLimit += memRes.expectedSize();
			
			return Void();
		} catch( Error &e ) {
			//check for transaction cancelled if the watch was not committed
			//TraceEvent("WDRWatchError", randomID).error(e,true);
			if( e.code() == error_code_used_during_commit ) {
				ASSERT( *doingCommit );
				return Void();
			} else if( e.code() == error_code_transaction_cancelled )
				return Void();
			throw;
		}
	}

	ACTOR Future<Void> commitAndUpdateMemory( ReadYourWritesTransaction *tr, WriteDuringReadWorkload* self, bool *cancelled, bool readYourWritesDisabled, bool snapshotRYWDisabled, bool readAheadDisabled, bool* doingCommit, double* startTime, Key timebombStr ) {
		state UID randomID = g_nondeterministic_random->randomUniqueID();
		//TraceEvent("WDRCommit", randomID);
		try {
			if( !readYourWritesDisabled && !*cancelled ) {
				KeyRangeMap<bool> transactionConflicts;
				tr->getWriteConflicts(&transactionConflicts);

				auto transactionRanges = transactionConflicts.ranges();
				auto addedRanges = self->addedConflicts.ranges();
				auto transactionIter = transactionRanges.begin();
				auto addedIter = addedRanges.begin();

				bool failed = false;
				while( transactionIter != transactionRanges.end() && addedIter != addedRanges.end() ) {
					if( transactionIter->begin() != addedIter->begin() || transactionIter->value() != addedIter->value() ) {
						TraceEvent(SevError, "WriteConflictError").detail("transactionKey", printable(transactionIter->begin())).detail("addedKey", printable(addedIter->begin())).detail("transactionVal", transactionIter->value()).detail("addedVal", addedIter->value());
						failed = true;
					}
					++transactionIter;
					++addedIter;
				}

				if( transactionIter != transactionRanges.end() || addedIter != addedRanges.end() ) {
					failed = true;
				}

				if( failed ) {
					TraceEvent(SevError, "WriteConflictRangeError");
					for(transactionIter = transactionRanges.begin(); transactionIter != transactionRanges.end(); ++transactionIter ) {
						TraceEvent("WCRTransaction").detail("range", printable(transactionIter.range())).detail("value", transactionIter.value());
					}
					for(addedIter = addedRanges.begin(); addedIter != addedRanges.end(); ++addedIter ) {
						TraceEvent("WCRAdded").detail("range", printable(addedIter.range())).detail("value", addedIter.value());
					}
				}
			}

			state std::map<Key, Value> committedDB = self->memoryDatabase;
			*doingCommit = true;
			Void _ = wait( tr->commit() );
			*doingCommit = false;
			self->finished.trigger();

			if(readYourWritesDisabled)
				tr->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
			if(snapshotRYWDisabled)
				tr->setOption(FDBTransactionOptions::SNAPSHOT_RYW_DISABLE);
			if(readAheadDisabled)
				tr->setOption(FDBTransactionOptions::READ_AHEAD_DISABLE);
			if(self->useSystemKeys)
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->addWriteConflictRange( self->conflictRange );
			self->addedConflicts.insert(allKeys, false);
			self->addedConflicts.insert( self->conflictRange, true );
			*startTime = now();
			tr->setOption( FDBTransactionOptions::TIMEOUT, timebombStr );

			//TraceEvent("WDRCommitSuccess", randomID).detail("CommittedVersion", tr->getCommittedVersion());
			self->lastCommittedDatabase = committedDB;
			
			return Void();
		} catch( Error &e ) {
			//TraceEvent("WDRCommitCancelled", randomID).error(e,true);
			if( e.code() == error_code_actor_cancelled || e.code() == error_code_transaction_cancelled || e.code() == error_code_used_during_commit )
				*cancelled = true;
			if( e.code() == error_code_actor_cancelled || e.code() == error_code_transaction_cancelled )
				throw commit_unknown_result();
			if (e.code() == error_code_transaction_too_old)
				throw not_committed();
			throw;
		}
	}

	Value getRandomValue() {
		return Value( std::string( g_random->randomInt(valueSizeRange.first,valueSizeRange.second+1), 'x' ) );
	}

	ACTOR Future<Void> loadAndRun( Database cx, WriteDuringReadWorkload* self ) {
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "WriteDuringRead");
		state double startTime = now();
		loop {
			state int i = 0;
			state int keysPerBatch = std::min<int64_t>(1000, 1 + CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT / 6 / (self->getKeyForIndex(self->nodes).size() + self->valueSizeRange.second));
			self->memoryDatabase = std::map<Key, Value>();
			for(; i < self->nodes; i+=keysPerBatch ) {
				state Transaction tr(cx);
				loop {
					if( now() - startTime > self->testDuration )
						return Void();
					try {
						if( i == 0 ) {
							tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
							tr.addWriteConflictRange(allKeys); // To prevent a write only transaction whose commit was previously cancelled from being reordered after this transaction
							tr.clear( normalKeys );
						}
						if( self->useSystemKeys )
							tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );

						int end = std::min(self->nodes, i+keysPerBatch );
						tr.clear( KeyRangeRef( self->getKeyForIndex(i), self->getKeyForIndex(end) ) );
						self->memoryDatabase.erase( self->memoryDatabase.lower_bound( self->getKeyForIndex(i) ), self->memoryDatabase.lower_bound( self->getKeyForIndex(end) ) );

						for( int j = i; j < end; j++ ) {
							if ( g_random->random01() < self->initialKeyDensity ) {
								Key key = self->getKeyForIndex( j );
								if( key.size() <= (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)) {
									Value value = self->getRandomValue();
									value = value.substr(0,std::min<int>(value.size(),CLIENT_KNOBS->VALUE_SIZE_LIMIT));
									self->memoryDatabase[ key ] = value;
									tr.set( key, value );
								}
							}
						}
						Void _ = wait( tr.commit() );
						//TraceEvent("WDRInitBatch").detail("i", i).detail("CommittedVersion", tr.getCommittedVersion());
						break;
					} catch( Error &e ) {
						Void _ = wait( tr.onError( e ) );
					}
				}
			}
			self->lastCommittedDatabase = self->memoryDatabase;
			self->addedConflicts.insert(allKeys, false);
			//TraceEvent("WDRInit");

			loop {
				Void _ = wait(delay( now() - startTime > self->slowModeStart || (g_network->isSimulated() && g_simulator.speedUpSimulation) ? 1.0 : 0.1 ));
				try {
					Void _ = wait( self->randomTransaction( ( self->useExtraDB && g_random->random01() < 0.5 ) ? self->extraDB : cx, self, startTime ) );
				} catch( Error &e ) {
					if( e.code() != error_code_not_committed )
						throw;
					break;
				}
				if( now() - startTime > self->testDuration )
					return Void();
			}
		}
	}

	Key getRandomKey() {
		return getKeyForIndex( g_random->randomInt(0, nodes ) );
	}

	Key getKeyForIndex( int idx ) {
		idx += minNode;
		if( adjacentKeys ) {
			return Key( idx ? keyPrefix + std::string( idx, '\x00' ) : "" );
		} else {
			return Key( keyPrefix + format( "%010d", idx ) );
		}
	}

	Key versionStampKeyForIndex( int idx ) {
		Key result = KeyRef( getKeyForIndex(idx).toString() + std::string(12,'\x00') );
		int16_t pos = g_random->randomInt(0, result.size() - 11);
		pos = littleEndian16(pos);
		uint8_t* data = mutateString(result);
		memcpy(data+result.size()-sizeof(int16_t), &pos, sizeof(int16_t));
		return result;
	}

	Key getRandomVersionStampKey() {
		return versionStampKeyForIndex( g_random->randomInt(0, nodes ) );
	}

	KeySelector getRandomKeySelector() {
		int scale = 1 << g_random->randomInt(0,14);
		return KeySelectorRef( getRandomKey(), g_random->random01() < 0.5, g_random->randomInt(-scale, scale) );
	}

	GetRangeLimits getRandomLimits() {
		int kind = g_random->randomInt(0,3);
		return GetRangeLimits(
			(kind&1) ? GetRangeLimits::ROW_LIMIT_UNLIMITED : g_random->randomInt(0, 1<<g_random->randomInt(1, 10)),
			(kind&2) ? GetRangeLimits::BYTE_LIMIT_UNLIMITED : g_random->randomInt(0, 1<<g_random->randomInt(1, 15)) );
	}

	KeyRange getRandomRange(int sizeLimit) {
		int startLocation = g_random->randomInt(0, nodes);
		int scale = g_random->randomInt(0, g_random->randomInt(2, 5) * g_random->randomInt(2, 5));
		int endLocation = startLocation + g_random->randomInt(0, 1+std::min(sizeLimit, std::min(nodes-startLocation, 1<<scale)));

		return KeyRangeRef( getKeyForIndex( startLocation ), getKeyForIndex( endLocation ) );
	}

	Value applyAtomicOp(Optional<StringRef> existingValue, Value value, MutationRef::Type type) {
		Arena arena;
		if (type == MutationRef::SetValue)
			return value;
		else if (type == MutationRef::AddValue)
			return doLittleEndianAdd(existingValue, value, arena);
		else if (type == MutationRef::AppendIfFits)
			return doAppendIfFits(existingValue, value, arena);
		else if (type == MutationRef::And)
			return doAndV2(existingValue, value, arena);
		else if (type == MutationRef::Or)
			return doOr(existingValue, value, arena);
		else if (type == MutationRef::Xor)
			return doXor(existingValue, value, arena);
		else if (type == MutationRef::Max)
			return doMax(existingValue, value, arena);
		else if (type == MutationRef::Min)
			return doMinV2(existingValue, value, arena);
		else if (type == MutationRef::ByteMin)
			return doByteMin(existingValue, value, arena);
		else if (type == MutationRef::ByteMax)
			return doByteMax(existingValue, value, arena);
		ASSERT(false);
		return Value();
	}

	ACTOR Future<Void> randomTransaction( Database cx, WriteDuringReadWorkload* self, double testStartTime ) { 
		state ReadYourWritesTransaction tr(cx);
		state bool readYourWritesDisabled = g_random->random01() < 0.5;
		state bool readAheadDisabled = g_random->random01() < 0.5;
		state bool snapshotRYWDisabled = g_random->random01() < 0.5;
		state int64_t timebomb = g_random->random01() < 0.01 ? g_random->randomInt64(1, 6000) : 0;
		state std::vector<Future<Void>> operations;
		state ActorCollection commits(false);
		state std::vector<Future<Void>> watches;
		state int changeNum = 1;
		state bool doingCommit = false;
		state int waitLocation = 0;
		state double startTime = now();

		state bool disableGetKey = BUGGIFY;
		state bool disableGetRange = BUGGIFY;
		state bool disableGet = BUGGIFY;
		state bool disableCommit = BUGGIFY;
		state bool disableClearRange = BUGGIFY;
		state bool disableClear = BUGGIFY;
		state bool disableWatch = BUGGIFY;
		state bool disableWriteConflictRange = BUGGIFY;
		state bool disableDelay = BUGGIFY;
		state bool disableReset = BUGGIFY;
		state bool disableReadConflictRange = BUGGIFY;
		state bool disableSet = BUGGIFY;
		state bool disableAtomicOp = BUGGIFY;
		
		state Key timebombStr = makeString( 8 );
		uint8_t* data = mutateString( timebombStr );
		memcpy(data, &timebomb, 8);

		loop {
			if(now() - testStartTime > self->testDuration) {
				return Void();
			}

			state int64_t memLimit = 1e8;
			state bool cancelled = false;
			if( readYourWritesDisabled )
				tr.setOption( FDBTransactionOptions::READ_YOUR_WRITES_DISABLE );
			if( snapshotRYWDisabled )
				tr.setOption( FDBTransactionOptions::SNAPSHOT_RYW_DISABLE );
			if( readAheadDisabled )
				tr.setOption( FDBTransactionOptions::READ_AHEAD_DISABLE );
			if( self->useSystemKeys )
				tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::TIMEOUT, timebombStr );
			tr.addWriteConflictRange( self->conflictRange );
			self->addedConflicts.insert( self->conflictRange, true );
			try {
				state int numWaits = g_random->randomInt( 1, 5 );
				state int i = 0;
				for(; i < numWaits && memLimit > 0; i++ ) {
					//TraceEvent("WDROps").detail("Count", i).detail("Max", numWaits).detail("readYourWritesDisabled",readYourWritesDisabled);
					state int numOps = g_random->randomInt( 1, self->numOps );
					state int j = 0;
					for(; j < numOps && memLimit > 0; j++ ) {
						if( commits.getResult().isError() )
							throw commits.getResult().getError();
						try {
							state int operationType = g_random->randomInt(0, 21);
							if( operationType == 0 && !disableGetKey ) {
								operations.push_back( self->getKeyAndCompare( &tr, self->getRandomKeySelector(), g_random->random01() < 0.5, readYourWritesDisabled, snapshotRYWDisabled, self, &doingCommit, &memLimit ) );
							} else if( operationType == 1 && !disableGetRange ) {
								operations.push_back( self->getRangeAndCompare( &tr, 
									self->getRandomKeySelector(),
									self->getRandomKeySelector(),
									self->getRandomLimits(), 
									g_random->random01() < 0.5, 
									g_random->random01() < 0.5, 
									readYourWritesDisabled, snapshotRYWDisabled, self, &doingCommit, &memLimit ) );
							} else if( operationType == 2 && !disableGet ) {
								operations.push_back( self->getAndCompare( &tr,
									self->getRandomKey(),
									g_random->random01() > 0.5, readYourWritesDisabled, snapshotRYWDisabled, self, &doingCommit, &memLimit ) );
							} else if( operationType == 3 && !disableCommit ) {
								if( !self->rarelyCommit || g_random->random01() < 1.0 / self->numOps ) {
									Future<Void> commit = self->commitAndUpdateMemory( &tr, self, &cancelled, readYourWritesDisabled, snapshotRYWDisabled, readAheadDisabled, &doingCommit, &startTime, timebombStr );
									operations.push_back( commit );
									commits.add( commit );
								}
							} else if( operationType == 4 && !disableClearRange ) {
								KeyRange range = self->getRandomRange( self->maxClearSize );
								self->changeCount.insert( range, changeNum++ );
								bool noConflict = g_random->random01() < 0.5;
								//TraceEvent("WDRClearRange").detail("Begin", printable(range)).detail("noConflict", noConflict);
								if( noConflict )
									tr.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
								tr.clear( range );
								if( !noConflict ) {
									KeyRangeRef conflict( range.begin.substr(0, std::min<int>(range.begin.size(), (range.begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1)), 
										range.end.substr(0, std::min<int>(range.end.size(), (range.end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1)));
									self->addedConflicts.insert(conflict, true);
								}
								self->memoryDatabase.erase( self->memoryDatabase.lower_bound( range.begin ), self->memoryDatabase.lower_bound( range.end ) );
							}  else if( operationType == 5 && !disableClear ) {
								Key key = self->getRandomKey();
								self->changeCount.insert( key, changeNum++ );
								bool noConflict = g_random->random01() < 0.5;
								//TraceEvent("WDRClear").detail("Key", printable(key)).detail("noConflict", noConflict);
								if( noConflict )
									tr.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
								tr.clear( key );
								if( !noConflict && key.size() <= (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT) ) {
									self->addedConflicts.insert(key, true);
								}
								self->memoryDatabase.erase( key );
							} else if( operationType == 6 && !disableWatch ) {
								watches.push_back( self->watchAndCompare( &tr,
									self->getRandomKey(),
									readYourWritesDisabled, self, &doingCommit, &memLimit ) );
							} else if( operationType == 7 && !disableWriteConflictRange ) {
								KeyRange range = self->getRandomRange( self->nodes );
								//TraceEvent("WDRAddWriteConflict").detail("range", printable(range));
								tr.addWriteConflictRange( range );
								KeyRangeRef conflict( range.begin.substr(0, std::min<int>(range.begin.size(), (range.begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1)), 
									range.end.substr(0, std::min<int>(range.end.size(), (range.end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1)));
								self->addedConflicts.insert(conflict, true);
							} else if( operationType == 8 && !disableDelay ) {
								double maxTime = 6.0;
								if( timebomb > 0 )
									maxTime = startTime + timebomb / 1000.0 - now();
								operations.push_back( delay( g_random->random01() * g_random->random01() * g_random->random01() * maxTime ) );
							} else if( operationType == 9 && !disableReset ) {
								if( g_random->random01() < 0.001 ) {
									//TraceEvent("WDRReset");
									tr.reset();
									self->memoryDatabase = self->lastCommittedDatabase;
									self->addedConflicts.insert(allKeys, false);
									if( readYourWritesDisabled )
										tr.setOption( FDBTransactionOptions::READ_YOUR_WRITES_DISABLE );
									if( snapshotRYWDisabled )
										tr.setOption( FDBTransactionOptions::SNAPSHOT_RYW_DISABLE );
									if( readAheadDisabled )
										tr.setOption( FDBTransactionOptions::READ_AHEAD_DISABLE );
									if( self->useSystemKeys )
										tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
									tr.addWriteConflictRange( self->conflictRange );
									self->addedConflicts.insert( self->conflictRange, true );
									startTime = now();
									tr.setOption( FDBTransactionOptions::TIMEOUT, timebombStr );
								}
							} else if( operationType == 10 && !disableReadConflictRange ) {
								KeyRange range = self->getRandomRange( self->maxClearSize );
								tr.addReadConflictRange( range );
							} else if( operationType == 11 && !disableAtomicOp ) {
								if(!self->useSystemKeys && g_random->random01() < 0.01) {
									Key versionStampKey = self->getRandomVersionStampKey();
									Value value = self->getRandomValue();
									KeyRangeRef range = getVersionstampKeyRange(versionStampKey.arena(), versionStampKey, normalKeys.end);
									self->changeCount.insert( range, changeNum++ );
									//TraceEvent("WDRVersionStamp").detail("versionStampKey", printable(versionStampKey)).detail("range", printable(range));
									tr.atomicOp( versionStampKey, value, MutationRef::SetVersionstampedKey );
									tr.clear( range );
									KeyRangeRef conflict( range.begin.substr(0, std::min<int>(range.begin.size(), (range.begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1)), 
										range.end.substr(0, std::min<int>(range.end.size(), (range.end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1)));
									self->addedConflicts.insert(conflict, true);
									self->memoryDatabase.erase( self->memoryDatabase.lower_bound( range.begin ), self->memoryDatabase.lower_bound( range.end ) );
								} else {
									Key key = self->getRandomKey();
									Value value = self->getRandomValue();
									MutationRef::Type opType;
									switch( g_random->randomInt(0,8) ) {
										case 0:
											opType = MutationRef::AddValue;
											break;
										case 1:
											opType = MutationRef::And;
											break;
										case 2:
											opType = MutationRef::Or;
											break;
										case 3:
											opType = MutationRef::Xor;
											break;
										case 4:
											opType = MutationRef::Max;
											break;
										case 5:
											opType = MutationRef::Min;
											break;
										case 6:
											opType = MutationRef::ByteMin;
											break;
										case 7:
											opType = MutationRef::ByteMax;
											break;
									}
									self->changeCount.insert( key, changeNum++ );
									bool noConflict = g_random->random01() < 0.5;
									//TraceEvent("WDRAtomicOp").detail("Key", printable(key)).detail("Value", value.size()).detail("noConflict", noConflict);
									if( noConflict )
										tr.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
									tr.atomicOp( key, value, opType );
									//TraceEvent("WDRAtomicOpSuccess").detail("Key", printable(key)).detail("Value", value.size());
									if( !noConflict && key.size() <= (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT) )
										self->addedConflicts.insert(key, true);
									Optional<Value> existing = self->memoryGet( &self->memoryDatabase, key );
									self->memoryDatabase[ key ] = self->applyAtomicOp( existing.present() ? Optional<StringRef>(existing.get()) : Optional<StringRef>(), value, opType );
								}
							} else if( operationType > 11 && !disableSet ) {
								Key key = self->getRandomKey();
								Value value = self->getRandomValue();
								self->changeCount.insert( key, changeNum++ );
								bool noConflict = g_random->random01() < 0.5;
								//TraceEvent("WDRSet").detail("Key", printable(key)).detail("Value", value.size()).detail("noConflict", noConflict);
								if( noConflict )
									tr.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
								tr.set( key, value );
								if( !noConflict && key.size() <= (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT) )
									self->addedConflicts.insert(key, true);
								//TraceEvent("WDRSetSuccess").detail("Key", printable(key)).detail("Value", value.size());
								self->memoryDatabase[ key ] = value;
							}
						} catch( Error &e ) {
							if( e.code() == error_code_used_during_commit )
								ASSERT( doingCommit );
							else if( e.code() != error_code_transaction_cancelled )
								throw;
						}
					}

					if( waitLocation < operations.size() ) {
						int waitOp = g_random->randomInt(waitLocation,operations.size());
						//TraceEvent("WDRWait").detail("Op", waitOp).detail("operations", operations.size()).detail("waitLocation", waitLocation);
						Void _ = wait( operations[waitOp] );
						Void _ = wait( delay(0.000001) ); //to ensure errors have propgated from reads to commits
						waitLocation = operations.size();
					}
				}
				Void _ = wait( waitForAll( operations ) );
				ASSERT( timebomb == 0 || 1000*(now() - startTime) <= timebomb + 1 );
				Void _ = wait( tr.debug_onIdle() );
				Void _ = wait( delay(0.000001) ); //to ensure triggered watches have a change to register
				self->finished.trigger();
				Void _ = wait( waitForAll( watches ) ); //only for errors, should have all returned
				self->changeCount.insert( allKeys, 0 );
				break;
			} catch( Error &e ) {
				operations.clear();
				commits.clear(false);
				waitLocation = 0;
				watches.clear();
				self->changeCount.insert( allKeys, 0 );
				doingCommit = false;
				//TraceEvent("WDRError").error(e, true);
				if(e.code() == error_code_database_locked) {
					self->memoryDatabase = self->lastCommittedDatabase;
					self->addedConflicts.insert(allKeys, false);
					return Void();
				}
				if( e.code() == error_code_not_committed || e.code() == error_code_commit_unknown_result || e.code() == error_code_transaction_too_large || e.code() == error_code_key_too_large || e.code() == error_code_value_too_large || cancelled )
					throw not_committed();
				try {
					Void _ = wait( tr.onError(e) );
				} catch( Error &e ) {
					if( e.code() == error_code_transaction_timed_out ) {
						ASSERT( timebomb != 0 && 1000*(now() - startTime) >= timebomb - 1 );
						throw not_committed();
					}
					throw e;
				}
				self->memoryDatabase = self->lastCommittedDatabase;
				self->addedConflicts.insert(allKeys, false);
			}
		}
		self->memoryDatabase = self->lastCommittedDatabase;
		self->addedConflicts.insert(allKeys, false);
		return Void();
	}
};

WorkloadFactory<WriteDuringReadWorkload> WriteDuringReadWorkloadFactory("WriteDuringRead");
