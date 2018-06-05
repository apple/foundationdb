/*
 * NativeAPI.actor.cpp
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

#include "DatabaseContext.h"
#include "NativeAPI.h"
#include "Atomic.h"
#include "flow/Platform.h"
#include "flow/actorcompiler.h"
#include "flow/ActorCollection.h"
#include "SystemData.h"
#include "fdbrpc/LoadBalance.h"
#include "StorageServerInterface.h"
#include "MasterProxyInterface.h"
#include "ClusterInterface.h"
#include "FailureMonitorClient.h"
#include "flow/DeterministicRandom.h"
#include "KeyRangeMap.h"
#include "flow/SystemMonitor.h"
#include "MutationList.h"
#include "CoordinationInterface.h"
#include "MonitorLeader.h"
#include "fdbrpc/TLSConnection.h"
#include "flow/Knobs.h"
#include "fdbclient/Knobs.h"
#include "fdbrpc/Net2FileSystem.h"

#include <iterator>

#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#undef min
#undef max
#else
#include <time.h>
#include "versions.h"
#endif

extern IRandom* trace_random;
extern const char* getHGVersion();

using std::min;
using std::max;
using std::make_pair;

NetworkOptions networkOptions;
Reference<TLSOptions> tlsOptions = Reference<TLSOptions>( new TLSOptions );

static const Key CLIENT_LATENCY_INFO_PREFIX = LiteralStringRef("client_latency/");
static const Key CLIENT_LATENCY_INFO_CTR_PREFIX = LiteralStringRef("client_latency_counter/");

Reference<LocationInfo> LocationInfo::getInterface( DatabaseContext *cx, std::vector<StorageServerInterface> const& alternatives, LocalityData const& clientLocality ) {
	std::vector<UID> handles;
	for( auto const& alternative : alternatives )
		handles.push_back( alternative.getVersion.getEndpoint().token ); // getVersion here was a random choice
	std::sort( handles.begin(), handles.end() );
	ASSERT( handles.size() );

	auto it = cx->ssid_locationInfo.find( handles );
	if( it != cx->ssid_locationInfo.end() ) {
		return Reference<LocationInfo>::addRef( it->second );
	}

	Reference<LocationInfo> loc( new LocationInfo(cx, alternatives, clientLocality) );
	cx->ssid_locationInfo[ handles ] = loc.getPtr();
	return loc;
}

void LocationInfo::notifyContextDestroyed() {
	cx = NULL;
}

LocationInfo::~LocationInfo() {
	if( cx ) {
		std::vector<UID> handles;
		for( auto const& alternative : getAlternatives() )
			handles.push_back( alternative.v.getVersion.getEndpoint().token ); // must match above choice of UID
		std::sort( handles.begin(), handles.end() );
		ASSERT_ABORT( handles.size() );

		auto it = cx->ssid_locationInfo.find( handles );
		if( it != cx->ssid_locationInfo.end() )
			cx->ssid_locationInfo.erase( it );
		cx = NULL;
	}
}

std::string printable( const VectorRef<KeyValueRef>& val ) {
	std::string s;
	for(int i=0; i<val.size(); i++)
		s = s + printable(val[i].key) + format(":%d ",val[i].value.size());
	return s;
}

std::string printable( const KeyValueRef& val ) {
	return printable(val.key) + format(":%d ",val.value.size());
}

std::string printable( const VectorRef<StringRef>& val ) {
	std::string s;
	for(int i=0; i<val.size(); i++)
		s = s + printable(val[i]) + " ";
	return s;
}

std::string printable( const StringRef& val ) {
	return val.printable();
}

std::string printable( const std::string& str ) {
	return StringRef(str).printable();
}

std::string printable( const Optional<StringRef>& val ) {
	if( val.present() )
		return printable( val.get() );
	return "[not set]";
}

std::string printable( const Optional<Standalone<StringRef>>& val ) {
	if( val.present() )
		return printable( val.get() );
	return "[not set]";
}

std::string printable( const KeyRangeRef& range ) {
	return printable(range.begin) + " - " + printable(range.end);
}

int unhex( char c ) {
	if (c >= '0' && c <= '9')
		return c-'0';
	if (c >= 'a' && c <= 'f')
		return c-'a'+10;
	if (c >= 'A' && c <= 'F')
		return c-'A'+10;
	UNREACHABLE();
}

std::string unprintable( std::string const& val ) {
	std::string s;
	for(int i=0; i<val.size(); i++) {
		char c = val[i];
		if ( c == '\\' ) {
			if (++i == val.size()) ASSERT(false);
			if (val[i] == '\\') {
				s += '\\';
			} else if (val[i] == 'x') {
				if (i+2 >= val.size()) ASSERT(false);
				s += char((unhex(val[i+1])<<4) + unhex(val[i+2]));
				i += 2;
			} else
				ASSERT(false);
		} else
			s += c;
	}
	return s;
}

void validateVersion(Version version) {
	// Version could be 0 if the INITIALIZE_NEW_DATABASE option is set. In that case, it is illegal to perform any reads.
	// We throw client_invalid_operation because the caller didn't directly set the version, so the version_invalid error
	// might be confusing.
	if(version == 0) {
		throw client_invalid_operation();
	}

	ASSERT(version > 0 || version == latestVersion);
}

void validateOptionValue(Optional<StringRef> value, bool shouldBePresent) {
	if(shouldBePresent && !value.present())
		throw invalid_option_value();
	if(!shouldBePresent && value.present() && value.get().size() > 0)
		throw invalid_option_value();
}

void dumpMutations( const MutationListRef& mutations ) {
	for(auto m=mutations.begin(); m; ++m) {
		switch (m->type) {
			case MutationRef::SetValue: printf("  '%s' := '%s'\n", printable(m->param1).c_str(), printable(m->param2).c_str()); break;
			case MutationRef::AddValue: printf("  '%s' += '%s'", printable(m->param1).c_str(), printable(m->param2).c_str()); break;
			case MutationRef::ClearRange: printf("  Clear ['%s','%s')\n", printable(m->param1).c_str(), printable(m->param2).c_str()); break;
			default: printf("  Unknown mutation %d('%s','%s')\n", m->type, printable(m->param1).c_str(), printable(m->param2).c_str()); break;
		}
	}
}

template <> void addref( DatabaseContext* ptr ) { ptr->addref(); }
template <> void delref( DatabaseContext* ptr ) { ptr->delref(); }

ACTOR Future<Void> databaseLogger( DatabaseContext *cx ) {
	loop {
		Void _ = wait( delay( CLIENT_KNOBS->SYSTEM_MONITOR_INTERVAL, cx->taskID ) );
		TraceEvent("TransactionMetrics")
			.detail("ReadVersions", cx->transactionReadVersions)
			.detail("LogicalUncachedReads", cx->transactionLogicalReads)
			.detail("PhysicalReadRequests", cx->transactionPhysicalReads)
			.detail("CommittedMutations", cx->transactionCommittedMutations)
			.detail("CommittedMutationBytes", cx->transactionCommittedMutationBytes)
			.detail("CommitStarted", cx->transactionsCommitStarted)
			.detail("CommitCompleted", cx->transactionsCommitCompleted)
			.detail("TooOld", cx->transactionsTooOld)
			.detail("FutureVersions", cx->transactionsFutureVersions)
			.detail("NotCommitted", cx->transactionsNotCommitted)
			.detail("MaybeCommitted", cx->transactionsMaybeCommitted)
			.detail("MeanLatency", 1000 * cx->latencies.mean())
			.detail("MedianLatency", 1000 * cx->latencies.median())
			.detail("Latency90", 1000 * cx->latencies.percentile(0.90))
			.detail("Latency98", 1000 * cx->latencies.percentile(0.98))
			.detail("MaxLatency", 1000 * cx->latencies.max())
			.detail("MeanRowReadLatency", 1000 * cx->readLatencies.mean())
			.detail("MedianRowReadLatency", 1000 * cx->readLatencies.median())
			.detail("MaxRowReadLatency", 1000 * cx->readLatencies.max())
			.detail("MeanGRVLatency", 1000 * cx->GRVLatencies.mean())
			.detail("MedianGRVLatency", 1000 * cx->GRVLatencies.median())
			.detail("MaxGRVLatency", 1000 * cx->GRVLatencies.max())
			.detail("MeanCommitLatency", 1000 * cx->commitLatencies.mean())
			.detail("MedianCommitLatency", 1000 * cx->commitLatencies.median())
			.detail("MaxCommitLatency", 1000 * cx->commitLatencies.max())
			.detail("MeanMutationsPerCommit", cx->mutationsPerCommit.mean())
			.detail("MedianMutationsPerCommit", cx->mutationsPerCommit.median())
			.detail("MaxMutationsPerCommit", cx->mutationsPerCommit.max())
			.detail("MeanBytesPerCommit", cx->bytesPerCommit.mean())
			.detail("MedianBytesPerCommit", cx->bytesPerCommit.median())
			.detail("MaxBytesPerCommit", cx->bytesPerCommit.max());
		cx->latencies.clear();
		cx->readLatencies.clear();
		cx->GRVLatencies.clear();
		cx->commitLatencies.clear();
		cx->mutationsPerCommit.clear();
		cx->bytesPerCommit.clear();
	}
}

ACTOR static Future<Standalone<StringRef> > getSampleVersionStamp(Transaction *tr) {
	loop{
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Optional<Value> _ = wait(tr->get(LiteralStringRef("\xff/StatusJsonTestKey62793")));
			state Future<Standalone<StringRef> > vstamp = tr->getVersionstamp();
			tr->makeSelfConflicting();
			Void _ = wait(tr->commit());
			Standalone<StringRef> val = wait(vstamp);
			return val;
		}
		catch (Error& e) {
			Void _ = wait(tr->onError(e));
		}
	}
}

struct TrInfoChunk {
	ValueRef value;
	Key key;
};

ACTOR static Future<Void> transactionInfoCommitActor(Transaction *tr, std::vector<TrInfoChunk> *chunks) {
	state const Key clientLatencyAtomicCtr = CLIENT_LATENCY_INFO_CTR_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin);
	state int retryCount = 0;
	loop{
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state Future<Standalone<StringRef> > vstamp = tr->getVersionstamp();
			int64_t numCommitBytes = 0;
			for (auto &chunk : *chunks) {
				tr->atomicOp(chunk.key, chunk.value, MutationRef::SetVersionstampedKey);
				numCommitBytes += chunk.key.size() + chunk.value.size() - 4; // subtract number of bytes of key that denotes verstion stamp index
			}
			tr->atomicOp(clientLatencyAtomicCtr, StringRef((uint8_t*)&numCommitBytes, 8), MutationRef::AddValue);
			Void _ = wait(tr->commit());
			return Void();
		}
		catch (Error& e) {
			retryCount++;
			if (retryCount == 10)
				throw;
			Void _ = wait(tr->onError(e));
		}
	}
}


ACTOR static Future<Void> delExcessClntTxnEntriesActor(Transaction *tr, int64_t clientTxInfoSizeLimit) {
	state const Key clientLatencyName = CLIENT_LATENCY_INFO_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin);
	state const Key clientLatencyAtomicCtr = CLIENT_LATENCY_INFO_CTR_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin);
	TraceEvent(SevInfo, "DelExcessClntTxnEntriesCalled");
	loop{
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> ctrValue = wait(tr->get(KeyRef(clientLatencyAtomicCtr), true));
			if (!ctrValue.present()) {
				TraceEvent(SevInfo, "NumClntTxnEntriesNotFound");
				return Void();
			}
			state int64_t txInfoSize = 0;
			ASSERT(ctrValue.get().size() == sizeof(int64_t));
			memcpy(&txInfoSize, ctrValue.get().begin(), ctrValue.get().size());
			if (txInfoSize < clientTxInfoSizeLimit)
				return Void();
			int getRangeByteLimit = (txInfoSize - clientTxInfoSizeLimit) < CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT ? (txInfoSize - clientTxInfoSizeLimit) : CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT;
			GetRangeLimits limit(CLIENT_KNOBS->ROW_LIMIT_UNLIMITED, getRangeByteLimit);
			Standalone<RangeResultRef> txEntries = wait(tr->getRange(KeyRangeRef(clientLatencyName, strinc(clientLatencyName)), limit));
			state int64_t numBytesToDel = 0;
			KeyRef endKey;
			for (auto &kv : txEntries) {
				endKey = kv.key;
				numBytesToDel += kv.key.size() + kv.value.size();
				if (txInfoSize - numBytesToDel <= clientTxInfoSizeLimit)
					break;
			}
			if (numBytesToDel) {
				tr->clear(KeyRangeRef(txEntries[0].key, strinc(endKey)));
				TraceEvent(SevInfo, "DeletingExcessCntTxnEntries").detail("BytesToBeDeleted", numBytesToDel);
				int64_t bytesDel = -numBytesToDel;
				tr->atomicOp(clientLatencyAtomicCtr, StringRef((uint8_t*)&bytesDel, 8), MutationRef::AddValue);
				Void _ = wait(tr->commit());
			}
			if (txInfoSize - numBytesToDel <= clientTxInfoSizeLimit)
				return Void();
		}
		catch (Error& e) {
			Void _ = wait(tr->onError(e));
		}
	}
}

// The reason for getting a pointer to DatabaseContext instead of a reference counted object is because reference counting will increment reference count for
// DatabaseContext which holds the future of this actor. This creates a cyclic reference and hence this actor and Database object will not be destroyed at all.
ACTOR static Future<Void> clientStatusUpdateActor(DatabaseContext *cx) {
	state const std::string clientLatencyName = CLIENT_LATENCY_INFO_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin).toString();
	state Transaction tr;
	state std::vector<TrInfoChunk> commitQ;
	state int txBytes = 0;

	loop {
		try {
			ASSERT(cx->clientStatusUpdater.outStatusQ.empty());
			cx->clientStatusUpdater.inStatusQ.swap(cx->clientStatusUpdater.outStatusQ);
			// Split Transaction Info into chunks
			state std::vector<TrInfoChunk> trChunksQ;
			for (auto &bw : cx->clientStatusUpdater.outStatusQ) {
				int64_t value_size_limit = BUGGIFY ? g_random->randomInt(1e3, CLIENT_KNOBS->VALUE_SIZE_LIMIT) : CLIENT_KNOBS->VALUE_SIZE_LIMIT;
				int num_chunks = (bw.getLength() + value_size_limit - 1) / value_size_limit;
				std::string random_id = g_random->randomAlphaNumeric(16);
				for (int i = 0; i < num_chunks; i++) {
					TrInfoChunk chunk;
					BinaryWriter chunkBW(Unversioned());
					chunkBW << bigEndian32(i+1) << bigEndian32(num_chunks);
					chunk.key = KeyRef(clientLatencyName + std::string(10, '\x00') + "/" + random_id + "/" + chunkBW.toStringRef().toString() + "/" + std::string(4, '\x00'));
					int32_t pos = littleEndian32(clientLatencyName.size());
					memcpy(mutateString(chunk.key) + chunk.key.size() - sizeof(int32_t), &pos, sizeof(int32_t));
					if (i == num_chunks - 1) {
						chunk.value = ValueRef(static_cast<uint8_t *>(bw.getData()) + (i * value_size_limit), bw.getLength() - (i * value_size_limit));
					}
					else {
						chunk.value = ValueRef(static_cast<uint8_t *>(bw.getData()) + (i * value_size_limit), value_size_limit);
					}
					trChunksQ.push_back(std::move(chunk));
				}
			}

			// Commit the chunks splitting into different transactions if needed
			state int64_t dataSizeLimit = BUGGIFY ? g_random->randomInt(200e3, 1.5 * CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT) : 0.8 * CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT;
			state std::vector<TrInfoChunk>::iterator tracking_iter = trChunksQ.begin();
			tr = Transaction(Database(Reference<DatabaseContext>::addRef(cx)));
			ASSERT(commitQ.empty() && (txBytes == 0));
			loop {
				state std::vector<TrInfoChunk>::iterator iter = tracking_iter;
				txBytes = 0;
				commitQ.clear();
				try {
					while (iter != trChunksQ.end()) {
						if (iter->value.size() + iter->key.size() + txBytes > dataSizeLimit) {
							Void _ = wait(transactionInfoCommitActor(&tr, &commitQ));
							tracking_iter = iter;
							commitQ.clear();
							txBytes = 0;
						}
						commitQ.push_back(*iter);
						txBytes += iter->value.size() + iter->key.size();
						++iter;
					}
					if (!commitQ.empty()) {
						Void _ = wait(transactionInfoCommitActor(&tr, &commitQ));
						commitQ.clear();
						txBytes = 0;
					}
					break;
				}
				catch (Error &e) {
					if (e.code() == error_code_transaction_too_large) {
						dataSizeLimit /= 2;
						ASSERT(dataSizeLimit >= CLIENT_KNOBS->VALUE_SIZE_LIMIT + CLIENT_KNOBS->KEY_SIZE_LIMIT);
					}
					else {
						TraceEvent(SevWarnAlways, "ClientTrInfoErrorCommit")
							.error(e)
							.detail("TxBytes", txBytes);
						commitQ.clear();
						txBytes = 0;
						throw;
					}
				}
			}
			cx->clientStatusUpdater.outStatusQ.clear();
			double clientSamplingProbability = std::isinf(cx->clientInfo->get().clientTxnInfoSampleRate) ? CLIENT_KNOBS->CSI_SAMPLING_PROBABILITY : cx->clientInfo->get().clientTxnInfoSampleRate;
			int64_t clientTxnInfoSizeLimit = cx->clientInfo->get().clientTxnInfoSizeLimit == -1 ? CLIENT_KNOBS->CSI_SIZE_LIMIT : cx->clientInfo->get().clientTxnInfoSizeLimit;
			if (!trChunksQ.empty() && g_random->random01() < clientSamplingProbability)
				Void _ = wait(delExcessClntTxnEntriesActor(&tr, clientTxnInfoSizeLimit));

			// tr is destructed because it hold a reference to DatabaseContext which creates a cycle mentioned above.
			// Hence destroy the transacation before sleeping to give a chance for the actor to be cleanedup if the Database is destroyed by the user.
			tr = Transaction();
			Void _ = wait(delay(CLIENT_KNOBS->CSI_STATUS_DELAY));
		}
		catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			cx->clientStatusUpdater.outStatusQ.clear();
			TraceEvent(SevWarnAlways, "UnableToWriteClientStatus").error(e);
			// tr is destructed because it hold a reference to DatabaseContext which creates a cycle mentioned above.
			// Hence destroy the transacation before sleeping to give a chance for the actor to be cleanedup if the Database is destroyed by the user.
			tr = Transaction();
			Void _ = wait(delay(10.0));
		}
	}
}

ACTOR static Future<Void> monitorMasterProxiesChange(Reference<AsyncVar<ClientDBInfo>> clientDBInfo, AsyncTrigger *triggerVar) {
	state vector< MasterProxyInterface > curProxies;
	curProxies = clientDBInfo->get().proxies;
	
	loop{
		Void _ = wait(clientDBInfo->onChange());
		if (clientDBInfo->get().proxies != curProxies) {
			curProxies = clientDBInfo->get().proxies;
			triggerVar->trigger();
		}
	}
}

DatabaseContext::DatabaseContext(
	Reference<AsyncVar<ClientDBInfo>> clientInfo,
	Reference<Cluster> cluster, Future<Void> clientInfoMonitor,
	Standalone<StringRef> dbName, Standalone<StringRef> dbId,
	int taskID, LocalityData clientLocality, bool enableLocalityLoadBalance, bool lockAware )
  : clientInfo(clientInfo), masterProxiesChangeTrigger(), cluster(cluster), clientInfoMonitor(clientInfoMonitor), dbName(dbName), dbId(dbId),
	transactionReadVersions(0), transactionLogicalReads(0), transactionPhysicalReads(0), transactionCommittedMutations(0), transactionCommittedMutationBytes(0), transactionsCommitStarted(0), 
	transactionsCommitCompleted(0), transactionsTooOld(0), transactionsFutureVersions(0), transactionsNotCommitted(0), transactionsMaybeCommitted(0), taskID(taskID),
	outstandingWatches(0), maxOutstandingWatches(CLIENT_KNOBS->DEFAULT_MAX_OUTSTANDING_WATCHES), clientLocality(clientLocality), enableLocalityLoadBalance(enableLocalityLoadBalance), lockAware(lockAware),
	latencies(1000), readLatencies(1000), commitLatencies(1000), GRVLatencies(1000), mutationsPerCommit(1000), bytesPerCommit(1000) 
{
	logger = databaseLogger( this );
	locationCacheSize = g_network->isSimulated() ?
			CLIENT_KNOBS->LOCATION_CACHE_EVICTION_SIZE_SIM :
			CLIENT_KNOBS->LOCATION_CACHE_EVICTION_SIZE;

	getValueSubmitted.init(LiteralStringRef("NativeAPI.GetValueSubmitted"));
	getValueCompleted.init(LiteralStringRef("NativeAPI.GetValueCompleted"));

	monitorMasterProxiesInfoChange = monitorMasterProxiesChange(clientInfo, &masterProxiesChangeTrigger);
	clientStatusUpdater.actor = clientStatusUpdateActor(this);
}

ACTOR static Future<Void> monitorClientInfo( Reference<AsyncVar<Optional<ClusterInterface>>> clusterInterface, Standalone<StringRef> dbName,
	Reference<ClusterConnectionFile> ccf, Reference<AsyncVar<ClientDBInfo>> outInfo ) 
{
	try {
		loop {
			OpenDatabaseRequest req;
			req.knownClientInfoID = outInfo->get().id;
			req.dbName = dbName;
			req.supportedVersions = VectorRef<ClientVersionRef>(req.arena, networkOptions.supportedVersions);
			req.traceLogGroup = StringRef(req.arena, networkOptions.traceLogGroup);

			ClusterConnectionString fileConnectionString;
			if (ccf && !ccf->fileContentsUpToDate(fileConnectionString)) {
				req.issues = LiteralStringRef("incorrect_cluster_file_contents");
				if(ccf->canGetFilename()) {
					TraceEvent(SevWarnAlways, "IncorrectClusterFileContents").detail("Filename", ccf->getFilename())
						.detail("ConnectionStringFromFile", fileConnectionString.toString())
						.detail("CurrentConnectionString", ccf->getConnectionString().toString());
				}
			}

			choose {
				when( ClientDBInfo ni = wait( clusterInterface->get().present() ? brokenPromiseToNever( clusterInterface->get().get().openDatabase.getReply( req ) ) : Never() ) ) {
					TraceEvent("ClientInfoChange").detail("ChangeID", ni.id);
					outInfo->set(ni);
				}
				when( Void _ = wait( clusterInterface->onChange() ) ) {
					if(clusterInterface->get().present())
						TraceEvent("ClientInfo_CCInterfaceChange").detail("CCID", clusterInterface->get().get().id());
				}
			}
		}
	} catch( Error& e ) {
		TraceEvent(SevError, "MonitorClientInfoError")
			.detail("DBName", printable(dbName))
			.detail("ConnectionFile", ccf && ccf->canGetFilename() ? ccf->getFilename() : "")
			.detail("ConnectionString", ccf ? ccf->getConnectionString().toString() : "")
			.error(e);

		throw;
	}
}

Future< Database > DatabaseContext::createDatabase( Reference<AsyncVar<Optional<ClusterInterface>>> clusterInterface, Reference<Cluster> cluster, Standalone<StringRef> dbName, LocalityData const& clientLocality ) {
	if (dbName != LiteralStringRef("DB")) {
		return invalid_database_name(); // we no longer offer multi-database support, so all databases *must* be named this
	}
	else {
		Reference<AsyncVar<ClientDBInfo>> info( new AsyncVar<ClientDBInfo> );
		Future<Void> monitor = monitorClientInfo( clusterInterface, dbName, cluster ? cluster->getConnectionFile() : Reference<ClusterConnectionFile>(), info );

		return std::move( Database( new DatabaseContext( info, cluster, monitor, dbName, LiteralStringRef(""), TaskDefaultEndpoint, clientLocality, true, false ) ) );
	}
}

Database DatabaseContext::create( Reference<AsyncVar<ClientDBInfo>> info, Future<Void> dependency, LocalityData clientLocality, bool enableLocalityLoadBalance, int taskID, bool lockAware ) {
	return Database( new DatabaseContext( info, Reference<Cluster>(), dependency, LiteralStringRef("DB"), LiteralStringRef(""), taskID, clientLocality, enableLocalityLoadBalance, lockAware ) );
}

DatabaseContext::~DatabaseContext() {
	monitorMasterProxiesInfoChange.cancel();
	for(auto it = ssid_locationInfo.begin(); it != ssid_locationInfo.end(); it = ssid_locationInfo.erase(it))
		it->second->notifyContextDestroyed();
	ASSERT_ABORT( ssid_locationInfo.empty() );
	locationCache.insert( allKeys, Reference<LocationInfo>() );
}

pair<KeyRange,Reference<LocationInfo>> DatabaseContext::getCachedLocation( const KeyRef& key, bool isBackward ) {
	if( isBackward ) {
		auto range = locationCache.rangeContainingKeyBefore(key);
		return std::make_pair(range->range(), range->value());
	}
	else {
		auto range = locationCache.rangeContaining(key);
		return std::make_pair(range->range(), range->value());
	}
}

bool DatabaseContext::getCachedLocations( const KeyRangeRef& range, vector<std::pair<KeyRange,Reference<LocationInfo>>>& result, int limit, bool reverse ) {
	result.clear();
	auto locRanges = locationCache.intersectingRanges(range);

	auto begin = locationCache.rangeContaining(range.begin);
	auto end = locationCache.rangeContainingKeyBefore(range.end);

	loop {
		auto r = reverse ? end : begin;
		if (!r->value()){
			TEST(result.size()); // had some but not all cached locations
			result.clear();
			return false;
		}
		result.push_back( make_pair(r->range() & range, r->value()) );
		if(result.size() == limit)
			break;

		if(begin == end)
			break;

		if(reverse)
			--end;
		else
			++begin;
	}

	return true;
}

Reference<LocationInfo> DatabaseContext::setCachedLocation( const KeyRangeRef& keys, const vector<StorageServerInterface>& servers ) {
	int maxEvictionAttempts = 100, attempts = 0;
	Reference<LocationInfo> loc = LocationInfo::getInterface( this, servers, clientLocality);
	while( locationCache.size() > locationCacheSize && attempts < maxEvictionAttempts) {
		TEST( true ); // NativeAPI storage server locationCache entry evicted
		attempts++;
		auto r = locationCache.randomRange();
		Key begin = r.begin(), end = r.end();  // insert invalidates r, so can't be passed a mere reference into it
		locationCache.insert( KeyRangeRef(begin, end), Reference<LocationInfo>() );
	}
	locationCache.insert( keys, loc );
	return std::move(loc);
}

void DatabaseContext::invalidateCache( const KeyRef& key, bool isBackward ) {
	if( isBackward )
		locationCache.rangeContainingKeyBefore(key)->value() = Reference<LocationInfo>();
	else
		locationCache.rangeContaining(key)->value() = Reference<LocationInfo>();
}

void DatabaseContext::invalidateCache( const KeyRangeRef& keys ) {
	auto rs = locationCache.intersectingRanges(keys);
	Key begin = rs.begin().begin(), end = rs.end().begin();  // insert invalidates rs, so can't be passed a mere reference into it
	locationCache.insert( KeyRangeRef(begin, end), Reference<LocationInfo>() );
}

Future<Void> DatabaseContext::onMasterProxiesChanged() {
	return this->masterProxiesChangeTrigger.onTrigger();
}

int64_t extractIntOption( Optional<StringRef> value, int64_t minValue, int64_t maxValue ) {
	validateOptionValue(value, true);
	if( value.get().size() != 8 ) {
		throw invalid_option_value();
	}

	int64_t passed = *((int64_t*)(value.get().begin()));
	if( passed > maxValue || passed < minValue ) {
		throw invalid_option_value();
	}

	return passed;
}

uint64_t extractHexOption( StringRef value ) {
	char* end;
	uint64_t id = strtoull( value.toString().c_str(), &end, 16 );
	if (*end)
		throw invalid_option_value();
	return id;
}

void DatabaseContext::setOption( FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	switch(option) {
		case FDBDatabaseOptions::LOCATION_CACHE_SIZE:
			locationCacheSize = (int)extractIntOption(value, 0, std::numeric_limits<int>::max());
			break;
		case FDBDatabaseOptions::MACHINE_ID:
			clientLocality = LocalityData( clientLocality.processId(), value.present() ? Standalone<StringRef>(value.get()) : Optional<Standalone<StringRef>>(), clientLocality.machineId(), clientLocality.dcId() );
			if( clientInfo->get().proxies.size() )
				masterProxies = Reference<ProxyInfo>( new ProxyInfo( clientInfo->get().proxies, clientLocality ));
			ssid_locationInfo.clear();
			locationCache.insert( allKeys, Reference<LocationInfo>() );
			break;
		case FDBDatabaseOptions::MAX_WATCHES:
			maxOutstandingWatches = (int)extractIntOption(value, 0, CLIENT_KNOBS->ABSOLUTE_MAX_WATCHES);
			break;
		case FDBDatabaseOptions::DATACENTER_ID:
			clientLocality = LocalityData(clientLocality.processId(), clientLocality.zoneId(), clientLocality.machineId(), value.present() ? Standalone<StringRef>(value.get()) : Optional<Standalone<StringRef>>());
			if( clientInfo->get().proxies.size() )
				masterProxies = Reference<ProxyInfo>( new ProxyInfo( clientInfo->get().proxies, clientLocality ));
			ssid_locationInfo.clear();
			locationCache.insert( allKeys, Reference<LocationInfo>() );
			break;
	}
}

void DatabaseContext::addWatch() {
	if(outstandingWatches >= maxOutstandingWatches)
		throw too_many_watches();

	++outstandingWatches;
}

void DatabaseContext::removeWatch() {
	--outstandingWatches;
	ASSERT(outstandingWatches >= 0);
}

extern uint32_t determinePublicIPAutomatically( ClusterConnectionString const& ccs );

Cluster::Cluster( Reference<ClusterConnectionFile> connFile, int apiVersion )
	: clusterInterface( new AsyncVar<Optional<ClusterInterface>> ), apiVersion(apiVersion), connectionFile( connFile )
{
	if(!g_network)
		throw network_not_setup();

	if(networkOptions.traceDirectory.present() && !traceFileIsOpen()) {
		g_network->initMetrics();
		FlowTransport::transport().initMetrics();
		initTraceEventMetrics();

		auto publicIP = determinePublicIPAutomatically( connFile->getConnectionString() );
		openTraceFile(NetworkAddress(publicIP, ::getpid()), networkOptions.traceRollSize, networkOptions.traceMaxLogsSize, networkOptions.traceDirectory.get(), "trace", networkOptions.traceLogGroup);

		TraceEvent("ClientStart")
			.detail("SourceVersion", getHGVersion())
			.detail("Version", FDB_VT_VERSION)
			.detail("PackageName", FDB_VT_PACKAGE_NAME)
			.detail("ClusterFile", connFile->getFilename().c_str())
			.detail("ConnectionString", connFile->getConnectionString().toString())
			.detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(NULL))
			.detail("ApiVersion", apiVersion)
			.detailf("ImageOffset", "%p", platform::getImageOffset())
			.trackLatest("ClientStart");

		initializeSystemMonitorMachineState(SystemMonitorMachineState(publicIP));

		systemMonitor();
		uncancellable( recurring( &systemMonitor, CLIENT_KNOBS->SYSTEM_MONITOR_INTERVAL, TaskFlushTrace ) );
	}

	leaderMon = monitorLeader( connectionFile, clusterInterface );
	failMon = failureMonitorClient( clusterInterface, false );
	connected = clusterInterface->onChange(); // SOMEDAY: This is a hack
}

Cluster::~Cluster() {}

Reference<Cluster> Cluster::createCluster( Reference<ClusterConnectionFile> connFile, int apiVersion ) {
	return Reference<Cluster>( new Cluster( connFile, apiVersion ) );
}

Reference<Cluster> Cluster::createCluster(std::string connFileName, int apiVersion) {
	Reference<ClusterConnectionFile> rccf = Reference<ClusterConnectionFile>(new ClusterConnectionFile(ClusterConnectionFile::lookupClusterFileName(connFileName).first));
	return Reference<Cluster>(new Cluster( rccf, apiVersion));
}

Future<Database> Cluster::createDatabase( Standalone<StringRef> dbName, LocalityData locality ) {
	return DatabaseContext::createDatabase( clusterInterface, Reference<Cluster>::addRef( this ), dbName, locality );
}

Future<Void> Cluster::onConnected() {
	return connected;
}

void Cluster::setOption(FDBClusterOptions::Option option, Optional<StringRef> value) { }

void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	switch(option) {
		// SOMEDAY: If the network is already started, should these three throw an error?
		case FDBNetworkOptions::TRACE_ENABLE:
			networkOptions.traceDirectory = value.present() ? value.get().toString() : "";
			break;
		case FDBNetworkOptions::TRACE_ROLL_SIZE:
			validateOptionValue(value, true);
			networkOptions.traceRollSize = extractIntOption(value, 0, std::numeric_limits<int64_t>::max());
			break;
		case FDBNetworkOptions::TRACE_MAX_LOGS_SIZE:
			validateOptionValue(value, true);
			networkOptions.traceMaxLogsSize = extractIntOption(value, 0, std::numeric_limits<int64_t>::max());
			break;
		case FDBNetworkOptions::TRACE_LOG_GROUP:
			if(value.present())
				networkOptions.traceLogGroup = value.get().toString();
			break;
		case FDBNetworkOptions::KNOB: {
			validateOptionValue(value, true);

			std::string optionValue = value.get().toString();
			size_t eq = optionValue.find_first_of('=');
			if(eq == optionValue.npos) {
				throw invalid_option_value();
			}

			std::string knob_name = optionValue.substr(0, eq);
			std::string knob_value = optionValue.substr(eq+1);
			if (!const_cast<FlowKnobs*>(FLOW_KNOBS)->setKnob( knob_name, knob_value ) &&
				!const_cast<ClientKnobs*>(CLIENT_KNOBS)->setKnob( knob_name, knob_value ))
			{
				fprintf(stderr, "FoundationDB client ignoring unrecognized knob option '%s'\n", knob_name.c_str());
			}
			break;
		}
		case FDBNetworkOptions::TLS_PLUGIN:
			validateOptionValue(value, true);
			tlsOptions->set_plugin_name_or_path( value.get().toString() );
			break;
		case FDBNetworkOptions::TLS_CERT_PATH:
			validateOptionValue(value, true);
			tlsOptions->set_cert_file( value.get().toString() );
			break;
		case FDBNetworkOptions::TLS_CERT_BYTES:
			tlsOptions->set_cert_data( value.get().toString() );
			break;
		case FDBNetworkOptions::TLS_CA_PATH:
			validateOptionValue(value, true);
			tlsOptions->set_ca_file( value.get().toString() );
			break;
		case FDBNetworkOptions::TLS_CA_BYTES:
			validateOptionValue(value, true);
			tlsOptions->set_ca_data(value.get().toString());
			break;
		case FDBNetworkOptions::TLS_PASSWORD:
			validateOptionValue(value, true);
			tlsOptions->set_key_password(value.get().toString());
			break;
		case FDBNetworkOptions::TLS_KEY_PATH:
			validateOptionValue(value, true);
			tlsOptions->set_key_file( value.get().toString() );
			break;
		case FDBNetworkOptions::TLS_KEY_BYTES:
			validateOptionValue(value, true);
			tlsOptions->set_key_data( value.get().toString() );
			break;
		case FDBNetworkOptions::TLS_VERIFY_PEERS:
			validateOptionValue(value, true);
			try {
				tlsOptions->set_verify_peers({ value.get().toString() });
			} catch( Error& e ) {
				TraceEvent(SevWarnAlways, "TLSValidationSetError")
					.detail("Input", value.get().toString() )
					.error( e );
				throw invalid_option_value();
			}
			break;
		case FDBNetworkOptions::DISABLE_CLIENT_STATISTICS_LOGGING:
			validateOptionValue(value, false);
			networkOptions.logClientInfo = false;
			break;
		case FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS:
		{
			// The multi-version API should be providing us these guarantees
			ASSERT(g_network);
			ASSERT(value.present());

			networkOptions.supportedVersions.resize(networkOptions.supportedVersions.arena(), 0);
			std::string versionString = value.get().toString();

			size_t index = 0;
			size_t nextIndex = 0;
			while(nextIndex != versionString.npos) {
				nextIndex = versionString.find(';', index);
				networkOptions.supportedVersions.push_back_deep(networkOptions.supportedVersions.arena(), ClientVersionRef(versionString.substr(index, nextIndex-index)));
				index = nextIndex + 1;
			}

			ASSERT(networkOptions.supportedVersions.size() > 0);

			break;
		}
		case FDBNetworkOptions::ENABLE_SLOW_TASK_PROFILING:
			validateOptionValue(value, false);
			networkOptions.slowTaskProfilingEnabled = true;
			break;
		default:
			break;
	}
}

void setupNetwork(uint64_t transportId, bool useMetrics) {
	if( g_network )
		throw network_already_setup();

	g_random = new DeterministicRandom( platform::getRandomSeed() );
	trace_random = new DeterministicRandom( platform::getRandomSeed() );
	g_nondeterministic_random = trace_random;
	g_debug_random = trace_random;
	if (!networkOptions.logClientInfo.present())
		networkOptions.logClientInfo = true;

	g_network = newNet2(NetworkAddress(), false, useMetrics || networkOptions.traceDirectory.present());
	FlowTransport::createInstance(transportId);
	Net2FileSystem::newFileSystem();

	tlsOptions->register_network();
}

void runNetwork() {
	if(!g_network)
		throw network_not_setup();

	if(networkOptions.traceDirectory.present() && networkOptions.slowTaskProfilingEnabled) {
		setupSlowTaskProfiler();
	}

	g_network->run();

	if(networkOptions.traceDirectory.present())
		systemMonitor();
}

void stopNetwork() {
	if(!g_network)
		throw network_not_setup();

	g_network->stop();
	closeTraceFile();
}

Reference<ProxyInfo> DatabaseContext::getMasterProxies() {
	if (masterProxiesLastChange != clientInfo->get().id) {
		masterProxiesLastChange = clientInfo->get().id;
		masterProxies.clear();
		if( clientInfo->get().proxies.size() )
			masterProxies = Reference<ProxyInfo>( new ProxyInfo( clientInfo->get().proxies, clientLocality ));
	}
	return masterProxies;
}

//Actor which will wait until the ProxyInfo returned by the DatabaseContext cx is not NULL
ACTOR Future<Reference<ProxyInfo>> getMasterProxiesFuture(DatabaseContext *cx) {
	loop{
		Reference<ProxyInfo> proxies = cx->getMasterProxies();
		if (proxies)
			return proxies;
		Void _ = wait( cx->onMasterProxiesChanged() );
	}
}

//Returns a future which will not be set until the ProxyInfo of this DatabaseContext is not NULL
Future<Reference<ProxyInfo>> DatabaseContext::getMasterProxiesFuture() {
	return ::getMasterProxiesFuture(this);
}

void GetRangeLimits::decrement( VectorRef<KeyValueRef> const& data ) {
	if( rows != CLIENT_KNOBS->ROW_LIMIT_UNLIMITED ) {
		ASSERT(data.size() <= rows);
		rows -= data.size();
	}

	minRows = std::max(0, minRows - data.size());

	if( bytes != CLIENT_KNOBS->BYTE_LIMIT_UNLIMITED )
		bytes = std::max( 0, bytes - (int)data.expectedSize() - (8-(int)sizeof(KeyValueRef))*data.size() );
}

void GetRangeLimits::decrement( KeyValueRef const& data ) {
	minRows = std::max(0, minRows - 1);
	if( rows != CLIENT_KNOBS->ROW_LIMIT_UNLIMITED )
		rows--;
	if( bytes != CLIENT_KNOBS->BYTE_LIMIT_UNLIMITED )
		bytes = std::max( 0, bytes - (int)8 - (int)data.expectedSize() );
}

// True if either the row or byte limit has been reached
bool GetRangeLimits::isReached() {
	return rows == 0 || (bytes == 0 && minRows == 0);
}

// True if data would cause the row or byte limit to be reached
bool GetRangeLimits::reachedBy( VectorRef<KeyValueRef> const& data ) {
	return ( rows != CLIENT_KNOBS->ROW_LIMIT_UNLIMITED && data.size() >= rows )
		|| ( bytes != CLIENT_KNOBS->BYTE_LIMIT_UNLIMITED && (int)data.expectedSize() + (8-(int)sizeof(KeyValueRef))*data.size() >= bytes && data.size() >= minRows );
}

bool GetRangeLimits::hasByteLimit() {
	return bytes != CLIENT_KNOBS->BYTE_LIMIT_UNLIMITED;
}

bool GetRangeLimits::hasRowLimit() {
	return rows != CLIENT_KNOBS->ROW_LIMIT_UNLIMITED;
}

bool GetRangeLimits::hasSatisfiedMinRows() {
	return hasByteLimit() && minRows == 0;
}


AddressExclusion AddressExclusion::parse( StringRef const& key ) {
	//Must not change: serialized to the database!
	std::string s = key.toString();
	int a,b,c,d,port,count=-1;
	if (sscanf(s.c_str(), "%d.%d.%d.%d%n", &a,&b,&c,&d, &count)<4) {
		TraceEvent(SevWarnAlways, "AddressExclusionParseError").detail("s", printable(key));
		return AddressExclusion();
	}
	s = s.substr(count);
	uint32_t ip = (a<<24)+(b<<16)+(c<<8)+d;
	if (!s.size())
		return AddressExclusion( ip );
	if (sscanf( s.c_str(), ":%d%n", &port, &count ) < 1 || count != s.size()) {
		TraceEvent(SevWarnAlways, "AddressExclusionParseError").detail("s", printable(key));
		return AddressExclusion();
	}
	return AddressExclusion( ip, port );
}

Future<Standalone<RangeResultRef>> getRange(
	Database const& cx,
	Future<Version> const& fVersion,
	KeySelector const& begin,
	KeySelector const& end,
	GetRangeLimits const& limits,
	bool const& reverse,
	TransactionInfo const& info);

Future<Optional<Value>> getValue( Future<Version> const& version, Key const& key, Database const& cx, TransactionInfo const& info, Reference<TransactionLogInfo> const& trLogInfo ) ;

ACTOR Future<Optional<StorageServerInterface>> fetchServerInterface( Database cx, TransactionInfo info, UID id, Future<Version> ver = latestVersion ) {
	Optional<Value> val = wait( getValue(ver, serverListKeyFor(id), cx, info, Reference<TransactionLogInfo>()) );
	if( !val.present() ) {
		// A storage server has been removed from serverList since we read keyServers
		return Optional<StorageServerInterface>();
	}

	return decodeServerListValue(val.get());
}

ACTOR Future<Optional<vector<StorageServerInterface>>> transactionalGetServerInterfaces( Future<Version> ver, Database cx, TransactionInfo info, vector<UID> ids ) {
	state vector< Future< Optional<StorageServerInterface> > > serverListEntries;
	for( int s = 0; s < ids.size(); s++ ) {
		serverListEntries.push_back( fetchServerInterface( cx, info, ids[s], ver ) );
	}

	vector<Optional<StorageServerInterface>> serverListValues = wait( getAll(serverListEntries) );
	vector<StorageServerInterface> serverInterfaces;
	for( int s = 0; s < serverListValues.size(); s++ ) {
		if( !serverListValues[s].present() ) {
			// A storage server has been removed from ServerList since we read keyServers
			return Optional<vector<StorageServerInterface>>();
		}
		serverInterfaces.push_back( serverListValues[s].get() );
	}
	return serverInterfaces;
}

//If isBackward == true, returns the shard containing the key before 'key' (an infinitely long, inexpressible key). Otherwise returns the shard containing key
ACTOR Future< pair<KeyRange,Reference<LocationInfo>> > getKeyLocation_internal( Database cx, Key key, TransactionInfo info, bool isBackward = false ) {
	if (isBackward) {
		ASSERT( key != allKeys.begin && key <= allKeys.end );
	} else {
		ASSERT( key < allKeys.end );
	}

	if( info.debugID.present() )
		g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getKeyLocation.Before");
	
	loop {
		choose {
			when ( Void _ = wait( cx->onMasterProxiesChanged() ) ) {}
			when ( GetKeyServerLocationsReply rep = wait( loadBalance( cx->getMasterProxies(), &MasterProxyInterface::getKeyServersLocations, GetKeyServerLocationsRequest(key, Optional<KeyRef>(), 100, isBackward, key.arena()), TaskDefaultPromiseEndpoint ) ) ) {
				if( info.debugID.present() )
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getKeyLocation.After");
				ASSERT( rep.results.size() == 1 );

				auto locationInfo = cx->setCachedLocation(rep.results[0].first, rep.results[0].second);
				return std::make_pair(KeyRange(rep.results[0].first, rep.arena), locationInfo);
			}
		}
	}
}

template <class F>
Future<pair<KeyRange, Reference<LocationInfo>>> getKeyLocation( Database const& cx, Key const& key, F StorageServerInterface::*member, TransactionInfo const& info, bool isBackward = false ) {
	auto ssi = cx->getCachedLocation( key, isBackward );
	if (!ssi.second) {
		return getKeyLocation_internal( cx, key, info, isBackward );
	}

	for(int i = 0; i < ssi.second->size(); i++) {
		if( IFailureMonitor::failureMonitor().onlyEndpointFailed(ssi.second->get(i, member).getEndpoint()) ) {
			cx->invalidateCache( key );
			ssi.second.clear();
			return getKeyLocation_internal( cx, key, info, isBackward );
		}
	}

	return ssi;
}

ACTOR Future< vector< pair<KeyRange,Reference<LocationInfo>> > > getKeyRangeLocations_internal( Database cx, KeyRange keys, int limit, bool reverse, TransactionInfo info ) {
	if( info.debugID.present() )
		g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getKeyLocations.Before");

	loop {
		choose {
			when ( Void _ = wait( cx->onMasterProxiesChanged() ) ) {}
			when ( GetKeyServerLocationsReply _rep = wait( loadBalance( cx->getMasterProxies(), &MasterProxyInterface::getKeyServersLocations, GetKeyServerLocationsRequest(keys.begin, keys.end, limit, reverse, keys.arena()), TaskDefaultPromiseEndpoint ) ) ) {
				state GetKeyServerLocationsReply rep = _rep;
				if( info.debugID.present() )
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getKeyLocations.After");
				ASSERT( rep.results.size() );

				state vector< pair<KeyRange,Reference<LocationInfo>> > results;
				state int shard = 0;
				for (; shard < rep.results.size(); shard++) {
					//FIXME: these shards are being inserted into the map sequentially, it would be much more CPU efficient to save the map pairs and insert them all at once.
					results.push_back( make_pair(rep.results[shard].first & keys, cx->setCachedLocation(rep.results[shard].first, rep.results[shard].second)) );
					Void _ = wait(yield());
				}

				return results;
			}
		}
	}
}

template <class F>
Future< vector< pair<KeyRange,Reference<LocationInfo>> > > getKeyRangeLocations( Database const& cx, KeyRange const& keys, int limit, bool reverse, F StorageServerInterface::*member, TransactionInfo const& info ) {
	ASSERT (!keys.empty());

	vector< pair<KeyRange,Reference<LocationInfo>> > locations;
	if (!cx->getCachedLocations(keys, locations, limit, reverse)) {
		return getKeyRangeLocations_internal( cx, keys, limit, reverse, info );
	}

	bool foundFailed = false;
	for(auto& it : locations) {
		bool onlyEndpointFailed = false;
		for(int i = 0; i < it.second->size(); i++) {
			if( IFailureMonitor::failureMonitor().onlyEndpointFailed(it.second->get(i, member).getEndpoint()) ) {
				onlyEndpointFailed = true;
				break;
			}
		}

		if( onlyEndpointFailed ) {
			cx->invalidateCache( it.first.begin );
			foundFailed = true;
		}
	}

	if(foundFailed) {
		return getKeyRangeLocations_internal( cx, keys, limit, reverse, info );
	}

	return locations;
}

ACTOR Future<Void> warmRange_impl( Transaction *self, Database cx, KeyRange keys ) {
	state int totalRanges = 0;
	state int totalRequests = 0;
	loop {
		vector<pair<KeyRange, Reference<LocationInfo>>> locations = wait(getKeyRangeLocations_internal(cx, keys, CLIENT_KNOBS->WARM_RANGE_SHARD_LIMIT, false, self->info));
		totalRanges += CLIENT_KNOBS->WARM_RANGE_SHARD_LIMIT;
		totalRequests++;
		if(locations.size() == 0 || totalRanges >= cx->locationCacheSize || locations[locations.size()-1].first.end >= keys.end)
			break;

		keys = KeyRangeRef(locations[locations.size()-1].first.end, keys.end);

		if(totalRequests%20 == 0) {
			//To avoid blocking the proxies from starting other transactions, occasionally get a read version.
			state Transaction tr(cx);
			loop {
				try {
					tr.setOption( FDBTransactionOptions::LOCK_AWARE );
					tr.setOption( FDBTransactionOptions::CAUSAL_READ_RISKY );
					Version _ = wait( tr.getReadVersion() );
					break;
				} catch( Error &e ) {
					Void _ = wait( tr.onError(e) );
				}
			}
		}
	}

	return Void();
}

Future<Void> Transaction::warmRange(Database cx, KeyRange keys) {
	return warmRange_impl(this, cx, keys);
}

ACTOR Future<Optional<Value>> getValue( Future<Version> version, Key key, Database cx, TransactionInfo info, Reference<TransactionLogInfo> trLogInfo )
{
	state Version ver = wait( version );
	validateVersion(ver);

	loop {
		state pair<KeyRange, Reference<LocationInfo>> ssi = wait( getKeyLocation(cx, key, &StorageServerInterface::getValue, info) );
		state Optional<UID> getValueID = Optional<UID>();
		state uint64_t startTime;
		state double startTimeD;
		try {
			//GetValueReply r = wait( g_random->randomChoice( ssi->get() ).getValue.getReply( GetValueRequest(key,ver) ) );
			//return r.value;
			if( info.debugID.present() ) {
				getValueID = g_nondeterministic_random->randomUniqueID();

				g_traceBatch.addAttach("GetValueAttachID", info.debugID.get().first(), getValueID.get().first());
				g_traceBatch.addEvent("GetValueDebug", getValueID.get().first(), "NativeAPI.getValue.Before"); //.detail("TaskID", g_network->getCurrentTask());
				/*TraceEvent("TransactionDebugGetValueInfo", getValueID.get())
					.detail("Key", printable(key))
					.detail("ReqVersion", ver)
					.detail("Servers", describe(ssi.second->get()));*/
			}

			++cx->getValueSubmitted;
			startTime = timer_int();
			startTimeD = now();
			++cx->transactionPhysicalReads;
			state GetValueReply reply = wait( loadBalance( ssi.second, &StorageServerInterface::getValue, GetValueRequest(key, ver, getValueID), TaskDefaultPromiseEndpoint, false, cx->enableLocalityLoadBalance ? &cx->queueModel : NULL ) );
			double latency = now() - startTimeD;
			cx->readLatencies.addSample(latency);
			if (trLogInfo) {
				int valueSize = reply.value.present() ? reply.value.get().size() : 0;
				trLogInfo->addLog(FdbClientLogEvents::EventGet(startTimeD, latency, valueSize, key));
			}
			cx->getValueCompleted->latency = timer_int() - startTime;
			cx->getValueCompleted->log();

			if( info.debugID.present() ) {
				g_traceBatch.addEvent("GetValueDebug", getValueID.get().first(), "NativeAPI.getValue.After"); //.detail("TaskID", g_network->getCurrentTask());
				/*TraceEvent("TransactionDebugGetValueDone", getValueID.get())
					.detail("Key", printable(key))
					.detail("ReqVersion", ver)
					.detail("ReplySize", reply.value.present() ? reply.value.get().size() : -1);*/
			}
			return reply.value;
		} catch (Error& e) {
			cx->getValueCompleted->latency = timer_int() - startTime;
			cx->getValueCompleted->log();
			if( info.debugID.present() ) {
				g_traceBatch.addEvent("GetValueDebug", getValueID.get().first(), "NativeAPI.getValue.Error"); //.detail("TaskID", g_network->getCurrentTask());
				/*TraceEvent("TransactionDebugGetValueDone", getValueID.get())
					.detail("Key", printable(key))
					.detail("ReqVersion", ver)
					.detail("ReplySize", reply.value.present() ? reply.value.get().size() : -1);*/
			}
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
				(e.code() == error_code_transaction_too_old && ver == latestVersion) ) {
				cx->invalidateCache( key );
				Void _ = wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, info.taskID));
			} else {
				if (trLogInfo)
					trLogInfo->addLog(FdbClientLogEvents::EventGetError(startTimeD, static_cast<int>(e.code()), key));
				throw e;
			}
		}
	}
}

ACTOR Future<Key> getKey( Database cx, KeySelector k, Future<Version> version, TransactionInfo info ) {
	Version _ = wait(version);

	if( info.debugID.present() )
		g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getKey.AfterVersion");

	loop {
		if (k.getKey() == allKeys.end) {
			if (k.offset > 0) return allKeys.end;
			k.orEqual = false;
		}
		else if (k.getKey() == allKeys.begin && k.offset <= 0) {
			return Key();
		}

		Key locationKey(k.getKey(), k.arena());
		state pair<KeyRange, Reference<LocationInfo>> ssi = wait( getKeyLocation(cx, locationKey, &StorageServerInterface::getKey, info, k.isBackward()) );
		
		try {
			if( info.debugID.present() )
				g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getKey.Before"); //.detail("StartKey", printable(k.getKey())).detail("offset",k.offset).detail("orEqual",k.orEqual);
			++cx->transactionPhysicalReads;
			GetKeyReply reply = wait( loadBalance( ssi.second, &StorageServerInterface::getKey, GetKeyRequest(k, version.get()), TaskDefaultPromiseEndpoint, false, cx->enableLocalityLoadBalance ? &cx->queueModel : NULL ) );
			if( info.debugID.present() )
				g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getKey.After"); //.detail("NextKey",printable(reply.sel.key)).detail("offset", reply.sel.offset).detail("orEqual", k.orEqual);
			k = reply.sel;
			if (!k.offset && k.orEqual) {
				return k.getKey();
			}
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
				cx->invalidateCache(k.getKey(), k.isBackward());

				Void _ = wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, info.taskID));
			} else {
				if(e.code() != error_code_actor_cancelled) {
					TraceEvent(SevInfo, "getKeyError")
						.error(e)
						.detail("AtKey", printable(k.getKey()))
						.detail("Offset", k.offset);
				}
				throw e;
			}
		}
	}
}

ACTOR Future<Version> waitForCommittedVersion( Database cx, Version version ) {
	try {
		loop {
			choose {
				when ( Void _ = wait( cx->onMasterProxiesChanged() ) ) {}
				when ( GetReadVersionReply v = wait( loadBalance( cx->getMasterProxies(), &MasterProxyInterface::getConsistentReadVersion, GetReadVersionRequest( 0, GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE ), cx->taskID ) ) ) {
					if (v.version >= version)
						return v.version;
					// SOMEDAY: Do the wait on the server side, possibly use less expensive source of committed version (causal consistency is not needed for this purpose)
					Void _ = wait( delay( CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, cx->taskID ) );
				}
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "waitForCommittedVersionError").error(e);
		throw;
	}
}

Future<Void> readVersionBatcher( DatabaseContext* const& cx, FutureStream< std::pair< Promise<GetReadVersionReply>, Optional<UID> > > const& versionStream, uint32_t const& flags );

ACTOR Future< Void > watchValue( Future<Version> version, Key key, Optional<Value> value, Database cx, int readVersionFlags, TransactionInfo info )
{
	state Version ver = wait( version );
	validateVersion(ver);
	ASSERT(ver != latestVersion);

	loop {
		state pair<KeyRange, Reference<LocationInfo>> ssi = wait( getKeyLocation(cx, key, &StorageServerInterface::watchValue, info ) );

		try {
			state Optional<UID> watchValueID = Optional<UID>();
			if( info.debugID.present() ) {
				watchValueID = g_nondeterministic_random->randomUniqueID();

				g_traceBatch.addAttach("WatchValueAttachID", info.debugID.get().first(), watchValueID.get().first());
				g_traceBatch.addEvent("WatchValueDebug", watchValueID.get().first(), "NativeAPI.watchValue.Before"); //.detail("TaskID", g_network->getCurrentTask());
			}
			state Version resp = wait( loadBalance( ssi.second, &StorageServerInterface::watchValue, WatchValueRequest(key, value, ver, watchValueID), TaskDefaultPromiseEndpoint ) );
			if( info.debugID.present() ) {
				g_traceBatch.addEvent("WatchValueDebug", watchValueID.get().first(), "NativeAPI.watchValue.After"); //.detail("TaskID", g_network->getCurrentTask());
			}

			//FIXME: wait for known committed version on the storage server before replying,
			//cannot do this until the storage server is notified on knownCommittedVersion changes from tlog (faster than the current update loop)
			Version v = wait( waitForCommittedVersion( cx, resp ) );

			//TraceEvent("watcherCommitted").detail("committedVersion", v).detail("watchVersion", resp).detail("key", printable( key )).detail("value", printable(value));

			if( v - resp < 50000000 ) // False if there is a master failure between getting the response and getting the committed version, Dependent on SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT
				return Void();
			ver = v;
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
				cx->invalidateCache( key );
				Void _ = wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, info.taskID));
			} else if( e.code() == error_code_watch_cancelled ) {
				TEST( true ); // Too many watches on the storage server, poll for changes instead
				Void _ = wait(delay(CLIENT_KNOBS->WATCH_POLLING_TIME, info.taskID));
			} else if ( e.code() == error_code_timed_out ) { //The storage server occasionally times out watches in case it was cancelled
				TEST( true ); // A watch timed out
				Void _ = wait(delay(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, info.taskID));
			} else {
				state Error err = e;
				Void _ = wait(delay(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, info.taskID));
				throw err;
			}
		}
	}
}

void transformRangeLimits(GetRangeLimits limits, bool reverse, GetKeyValuesRequest &req) {
	if(limits.bytes != 0) {
		if(!limits.hasRowLimit())
			req.limit = CLIENT_KNOBS->REPLY_BYTE_LIMIT; // Can't get more than this many rows anyway
		else
			req.limit = std::min( CLIENT_KNOBS->REPLY_BYTE_LIMIT, limits.rows );

		if(reverse)
			req.limit *= -1;

		if(!limits.hasByteLimit())
			req.limitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
		else
			req.limitBytes = std::min( CLIENT_KNOBS->REPLY_BYTE_LIMIT, limits.bytes );
	}
	else {
		req.limitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
		req.limit = reverse ? -limits.minRows : limits.minRows;
	}
}

ACTOR Future<Standalone<RangeResultRef>> getExactRange( Database cx, Version version,
	KeyRange keys, GetRangeLimits limits, bool reverse, TransactionInfo info )
{
	state Standalone<RangeResultRef> output;

	//printf("getExactRange( '%s', '%s' )\n", keys.begin.toString().c_str(), keys.end.toString().c_str());
	loop {
		state vector< pair<KeyRange, Reference<LocationInfo>> > locations = wait( getKeyRangeLocations( cx, keys, CLIENT_KNOBS->GET_RANGE_SHARD_LIMIT, reverse, &StorageServerInterface::getKeyValues, info ) );
		ASSERT( locations.size() );
		state int shard = 0;
		loop {
			const KeyRangeRef& range = locations[shard].first;

			GetKeyValuesRequest req;
			req.version = version;
			req.begin = firstGreaterOrEqual( range.begin );
			req.end = firstGreaterOrEqual( range.end );

			transformRangeLimits(limits, reverse, req);
			ASSERT(req.limitBytes > 0 && req.limit != 0 && req.limit < 0 == reverse);

			//FIXME: buggify byte limits on internal functions that use them, instead of globally
			req.debugID = info.debugID;

			try {
				if( info.debugID.present() ) {
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getExactRange.Before");
					/*TraceEvent("TransactionDebugGetExactRangeInfo", info.debugID.get())
					.detail("ReqBeginKey", printable(req.begin.getKey()))
					.detail("ReqEndKey", printable(req.end.getKey()))
					.detail("ReqLimit", req.limit)
					.detail("ReqLimitBytes", req.limitBytes)
					.detail("ReqVersion", req.version)
					.detail("Reverse", reverse)
					.detail("Servers", locations[shard].second->description());*/
				}
				++cx->transactionPhysicalReads;
				GetKeyValuesReply rep = wait( loadBalance( locations[shard].second, &StorageServerInterface::getKeyValues, req, TaskDefaultPromiseEndpoint, false, cx->enableLocalityLoadBalance ? &cx->queueModel : NULL ) );
				if( info.debugID.present() )
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getExactRange.After");
				output.arena().dependsOn( rep.arena );
				output.append( output.arena(), rep.data.begin(), rep.data.size() );

				if( limits.hasRowLimit() && rep.data.size() > limits.rows ) {
					TraceEvent(SevError, "GetExactRangeTooManyRows").detail("RowLimit", limits.rows).detail("DeliveredRows", output.size());
					ASSERT( false );
				}
				limits.decrement( rep.data );

				if (limits.isReached()) {
					output.more = true;
					return output;
				}

				bool more = rep.more;
				// If the reply says there is more but we know that we finished the shard, then fix rep.more
				if( reverse && more && rep.data.size() > 0 && output[output.size()-1].key == locations[shard].first.begin )
					more = false;

				if (more) {
					if( !rep.data.size() ) {
						TraceEvent(SevError, "GetExactRangeError").detail("Reason", "More data indicated but no rows present")
							.detail("LimitBytes", limits.bytes).detail("LimitRows", limits.rows)
							.detail("OutputSize", output.size()).detail("OutputBytes", output.expectedSize())
							.detail("BlockSize", rep.data.size()).detail("BlockBytes", rep.data.expectedSize());
						ASSERT( false );
					}
					TEST(true);   // GetKeyValuesReply.more in getExactRange
					// Make next request to the same shard with a beginning key just after the last key returned
					if( reverse )
						locations[shard].first = KeyRangeRef( locations[shard].first.begin, output[output.size()-1].key );
					else
						locations[shard].first = KeyRangeRef( keyAfter( output[output.size()-1].key ), locations[shard].first.end );
				}

				if (!more || locations[shard].first.empty()) {
					TEST(true);
					if(shard == locations.size()-1) {
						const KeyRangeRef& range = locations[shard].first;
						KeyRef begin = reverse ? keys.begin : range.end;
						KeyRef end = reverse ? range.begin : keys.end;

						if(begin >= end) {
							output.more = false;
							return output;
						}
						TEST(true); //Multiple requests of key locations

						keys = KeyRangeRef(begin, end);
						break;
					}

					++shard;
				}

				// Soft byte limit - return results early if the user specified a byte limit and we got results
				// This can prevent problems where the desired range spans many shards and would be too slow to
				// fetch entirely.
				if(limits.hasSatisfiedMinRows() && output.size() > 0) {
					output.more = true;
					return output;
				}

			} catch (Error& e) {
				if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
					const KeyRangeRef& range = locations[shard].first;

					if( reverse )
						keys = KeyRangeRef( keys.begin, range.end );
					else
						keys = KeyRangeRef( range.begin, keys.end );

					cx->invalidateCache( keys );
					Void _ = wait( delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, info.taskID ));
					break;
				} else {
					TraceEvent(SevInfo, "getExactRangeError")
						.error(e)
						.detail("ShardBegin", printable(locations[shard].first.begin))
						.detail("ShardEnd", printable(locations[shard].first.end));
					throw;
				}
			}
		}
	}
}

Future<Key> resolveKey( Database const& cx, KeySelector const& key, Version const& version, TransactionInfo const& info ) {
	if( key.isFirstGreaterOrEqual() )
		return Future<Key>( key.getKey() );

	if( key.isFirstGreaterThan() )
		return Future<Key>( keyAfter( key.getKey() ) );

	return getKey( cx, key, version, info );
}

ACTOR Future<Standalone<RangeResultRef>> getRangeFallback( Database cx, Version version,
	KeySelector begin, KeySelector end, GetRangeLimits limits, bool reverse, TransactionInfo info )
{
	if(version == latestVersion) {
		state Transaction transaction(cx);
		transaction.setOption(FDBTransactionOptions::CAUSAL_READ_RISKY);
		transaction.setOption(FDBTransactionOptions::LOCK_AWARE);
		transaction.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		Version ver = wait( transaction.getReadVersion() );
		version = ver;
	}

	Future<Key> fb = resolveKey(cx, begin, version, info);
	state Future<Key> fe = resolveKey(cx, end, version, info);

	state Key b = wait(fb);
	state Key e = wait(fe);
	if (b >= e) {
		return Standalone<RangeResultRef>();
	}

	//if e is allKeys.end, we have read through the end of the database
	//if b is allKeys.begin, we have either read through the beginning of the database,
	//or allKeys.begin exists in the database and will be part of the conflict range anyways

	Standalone<RangeResultRef> _r = wait( getExactRange(cx, version, KeyRangeRef(b, e), limits, reverse, info) );
	Standalone<RangeResultRef> r = _r;

	if(b == allKeys.begin && ((reverse && !r.more) || !reverse))
		r.readToBegin = true;
	if(e == allKeys.end && ((!reverse && !r.more) || reverse))
		r.readThroughEnd = true;


	ASSERT( !limits.hasRowLimit() || r.size() <= limits.rows );

	// If we were limiting bytes and the returned range is twice the request (plus 10K) log a warning
	if( limits.hasByteLimit() && r.expectedSize() > size_t(limits.bytes + CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT + CLIENT_KNOBS->VALUE_SIZE_LIMIT + 1) && limits.minRows == 0 ) {
		TraceEvent(SevWarnAlways, "GetRangeFallbackTooMuchData")
			.detail("LimitBytes", limits.bytes)
			.detail("DeliveredBytes", r.expectedSize())
			.detail("LimitRows", limits.rows)
			.detail("DeliveredRows", r.size());
	}

	return r;
}

void getRangeFinished(Reference<TransactionLogInfo> trLogInfo, double startTime, KeySelector begin, KeySelector end, bool snapshot, 
	Promise<std::pair<Key, Key>> conflictRange, bool reverse, Standalone<RangeResultRef> result) 
{
	if( trLogInfo ) {
		int rangeSize = 0;
		for (const KeyValueRef &kv : result.contents())
			rangeSize += kv.key.size() + kv.value.size();
		trLogInfo->addLog(FdbClientLogEvents::EventGetRange(startTime, now()-startTime, rangeSize, begin.getKey(), end.getKey()));
	}

	if( !snapshot ) {
		Key rangeBegin;
		Key rangeEnd;

		if(result.readToBegin) {
			rangeBegin = allKeys.begin;
		}
		else if(((!reverse || !result.more || begin.offset > 1) && begin.offset > 0) || result.size() == 0) {
			rangeBegin = Key(begin.getKey(), begin.arena());
		}
		else {
			rangeBegin = reverse ? result.end()[-1].key : result[0].key;
		}

		if(end.offset > begin.offset && end.getKey() < rangeBegin) {
			rangeBegin = Key(end.getKey(), end.arena());
		}

		if(result.readThroughEnd) {
			rangeEnd = allKeys.end;
		}
		else if(((reverse || !result.more || end.offset <= 0) && end.offset <= 1) || result.size() == 0) {
			rangeEnd = Key(end.getKey(), end.arena());
		}
		else {
			rangeEnd = keyAfter(reverse ? result[0].key : result.end()[-1].key);
		}

		if(begin.offset < end.offset && begin.getKey() > rangeEnd) {
			rangeEnd = Key(begin.getKey(), begin.arena());
		}

		conflictRange.send(std::make_pair(rangeBegin, rangeEnd));
	}
}

ACTOR Future<Standalone<RangeResultRef>> getRange( Database cx, Reference<TransactionLogInfo> trLogInfo, Future<Version> fVersion,
	KeySelector begin, KeySelector end, GetRangeLimits limits, Promise<std::pair<Key, Key>> conflictRange, bool snapshot, bool reverse, 
	TransactionInfo info )
{
	state GetRangeLimits originalLimits( limits );
	state KeySelector originalBegin = begin;
	state KeySelector originalEnd = end;
	state Standalone<RangeResultRef> output;

	try {
		state Version version = wait( fVersion );
		validateVersion(version);

		state double startTime = now();
		state Version readVersion = version; // Needed for latestVersion requests; if more, make future requests at the version that the first one completed
											 // FIXME: Is this really right?  Weaken this and see if there is a problem; if so maybe there is a much subtler problem even with this.

		if( begin.getKey() == allKeys.begin && begin.offset < 1 ) {
			output.readToBegin = true;
			begin = KeySelector(firstGreaterOrEqual( begin.getKey() ), begin.arena());
		}

		ASSERT( !limits.isReached() );
		ASSERT( (!limits.hasRowLimit() || limits.rows >= limits.minRows) && limits.minRows >= 0 );

		loop {
			if( end.getKey() == allKeys.begin && (end.offset < 1 || end.isFirstGreaterOrEqual()) ) {
				getRangeFinished(trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
				return output;
			}

			Key locationKey = reverse ? Key(end.getKey(), end.arena()) : Key(begin.getKey(), begin.arena());
			bool locationBackward = reverse ? (end-1).isBackward() : begin.isBackward();
			state pair<KeyRange, Reference<LocationInfo>> beginServer = wait( getKeyLocation( cx, locationKey, &StorageServerInterface::getKeyValues, info, locationBackward ) );
			state KeyRange shard = beginServer.first;
			state bool modifiedSelectors = false;
			state GetKeyValuesRequest req;

			req.version = readVersion;

			if( reverse && (begin-1).isDefinitelyLess(shard.begin) &&
				( !begin.isFirstGreaterOrEqual() || begin.getKey() != shard.begin ) ) { //In this case we would be setting modifiedSelectors to true, but not modifying anything

				req.begin = firstGreaterOrEqual( shard.begin );
				modifiedSelectors = true;
			}
			else req.begin = begin;

			if( !reverse && end.isDefinitelyGreater(shard.end) ) {
				req.end = firstGreaterOrEqual( shard.end );
				modifiedSelectors = true;
			}
			else req.end = end;

			transformRangeLimits(limits, reverse, req);
			ASSERT(req.limitBytes > 0 && req.limit != 0 && req.limit < 0 == reverse);

			req.debugID = info.debugID;
			try {
				if( info.debugID.present() ) {
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getRange.Before");
					/*TraceEvent("TransactionDebugGetRangeInfo", info.debugID.get())
						.detail("ReqBeginKey", printable(req.begin.getKey()))
						.detail("ReqEndKey", printable(req.end.getKey()))
						.detail("originalBegin", originalBegin.toString())
						.detail("originalEnd", originalEnd.toString())
						.detail("Begin", begin.toString())
						.detail("End", end.toString())
						.detail("shard", printable(shard))
						.detail("ReqLimit", req.limit)
						.detail("ReqLimitBytes", req.limitBytes)
						.detail("ReqVersion", req.version)
						.detail("Reverse", reverse)
						.detail("ModifiedSelectors", modifiedSelectors)
						.detail("Servers", beginServer.second->description());*/
				}

				++cx->transactionPhysicalReads;
				GetKeyValuesReply rep = wait( loadBalance(beginServer.second, &StorageServerInterface::getKeyValues, req, TaskDefaultPromiseEndpoint, false, cx->enableLocalityLoadBalance ? &cx->queueModel : NULL ) );

				if( info.debugID.present() ) {
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getRange.After");//.detail("SizeOf", rep.data.size());
					/*TraceEvent("TransactionDebugGetRangeDone", info.debugID.get())
						.detail("ReqBeginKey", printable(req.begin.getKey()))
						.detail("ReqEndKey", printable(req.end.getKey()))
						.detail("RepIsMore", rep.more)
						.detail("VersionReturned", rep.version)
						.detail("RowsReturned", rep.data.size());*/
				}

				ASSERT( !rep.more || rep.data.size() );
				ASSERT( !limits.hasRowLimit() || rep.data.size() <= limits.rows );

				limits.decrement( rep.data );

				if(reverse && begin.isLastLessOrEqual() && rep.data.size() && rep.data.end()[-1].key == begin.getKey()) {
					modifiedSelectors = false;
				}

				bool finished = limits.isReached() || ( !modifiedSelectors && !rep.more ) || limits.hasSatisfiedMinRows();
				bool readThrough = modifiedSelectors && !rep.more;

				// optimization: first request got all data--just return it
				if( finished && !output.size() ) {
					bool readToBegin = output.readToBegin;
					bool readThroughEnd = output.readThroughEnd;

					output = Standalone<RangeResultRef>( RangeResultRef( rep.data, modifiedSelectors || limits.isReached() || rep.more ), rep.arena );
					output.readToBegin = readToBegin;
					output.readThroughEnd = readThroughEnd;

					if( BUGGIFY && limits.hasByteLimit() && output.size() > std::max(1, originalLimits.minRows) ) {
						output.more = true;
						output.resize(output.arena(), g_random->randomInt(std::max(1,originalLimits.minRows),output.size()));
						getRangeFinished(trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
						return output;
					}

					if( readThrough ) {
						output.arena().dependsOn( shard.arena() );
						output.readThrough = reverse ? shard.begin : shard.end;
					}

					getRangeFinished(trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
					return output;
				}

				output.arena().dependsOn( rep.arena );
				output.append(output.arena(), rep.data.begin(), rep.data.size());

				if( finished ) {
					if( readThrough ) {
						output.arena().dependsOn( shard.arena() );
						output.readThrough = reverse ? shard.begin : shard.end;
					}
					output.more = modifiedSelectors || limits.isReached() || rep.more;

					getRangeFinished(trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
					return output;
				}

				readVersion = rep.version; // see above comment

				if( !rep.more ) {
					ASSERT( modifiedSelectors );
					TEST(true);  // !GetKeyValuesReply.more and modifiedSelectors in getRange

					if( !rep.data.size() ) {
						Standalone<RangeResultRef> result = wait( getRangeFallback(cx, version, originalBegin, originalEnd, originalLimits, reverse, info ) );
						getRangeFinished(trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, result);
						return result;
					}

					if( reverse )
						end = firstGreaterOrEqual( shard.begin );
					else
						begin = firstGreaterOrEqual( shard.end );
				} else {
					TEST(true);  // GetKeyValuesReply.more in getRange
					if( reverse )
						end = firstGreaterOrEqual( output[output.size()-1].key );
					else
						begin = firstGreaterThan( output[output.size()-1].key );
				}


			} catch ( Error& e ) {
				if( info.debugID.present() ) {
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getRange.Error");
					TraceEvent("TransactionDebugError", info.debugID.get()).error(e);
				}
				if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
					(e.code() == error_code_transaction_too_old && readVersion == latestVersion))
				{
					cx->invalidateCache( reverse ? end.getKey() : begin.getKey(), reverse ? (end-1).isBackward() : begin.isBackward() );

					if (e.code() == error_code_wrong_shard_server) {
						Standalone<RangeResultRef> result = wait( getRangeFallback(cx, version, originalBegin, originalEnd, originalLimits, reverse, info ) );
						getRangeFinished(trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, result);
						return result;
					}

					Void _ = wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, info.taskID));
				} else {
					if (trLogInfo)
						trLogInfo->addLog(FdbClientLogEvents::EventGetRangeError(startTime, static_cast<int>(e.code()), begin.getKey(), end.getKey()));

					throw e;
				}
			}
		}
	}
	catch(Error &e) {
		if(conflictRange.canBeSet()) {
			conflictRange.send(std::make_pair(Key(), Key()));
		}

		throw;
	}
}

Future<Standalone<RangeResultRef>> getRange( Database const& cx, Future<Version> const& fVersion, KeySelector const& begin, KeySelector const& end, 
	GetRangeLimits const& limits, bool const& reverse, TransactionInfo const& info ) 
{
	return getRange(cx, Reference<TransactionLogInfo>(), fVersion, begin, end, limits, Promise<std::pair<Key, Key>>(), true, reverse, info);
}

Transaction::Transaction( Database const& cx )
	: cx(cx), info(cx->taskID), backoff(CLIENT_KNOBS->DEFAULT_BACKOFF), committedVersion(invalidVersion), versionstampPromise(Promise<Standalone<StringRef>>()), numErrors(0), trLogInfo(createTrLogInfoProbabilistically(cx))
{
	setPriority(GetReadVersionRequest::PRIORITY_DEFAULT);
	if(cx->lockAware)
		options.lockAware = true;
}

Transaction::~Transaction() {
	flushTrLogsIfEnabled();
	cancelWatches();
}

void Transaction::operator=(Transaction&& r) noexcept(true) {
	flushTrLogsIfEnabled();
	cx = std::move(r.cx);
	tr = std::move(r.tr);
	readVersion = std::move(r.readVersion);
	extraConflictRanges = std::move(r.extraConflictRanges);
	commitResult = std::move(r.commitResult);
	committing = std::move(r.committing);
	options = std::move(r.options);
	info = r.info;
	backoff = r.backoff;
	numErrors = r.numErrors;
	committedVersion = r.committedVersion;
	versionstampPromise = std::move(r.versionstampPromise);
	watches = r.watches;
	trLogInfo = std::move(r.trLogInfo);
}

void Transaction::flushTrLogsIfEnabled() {
	if (trLogInfo && trLogInfo->logsAdded && trLogInfo->trLogWriter.getData()) {
		ASSERT(trLogInfo->flushed == false);
		cx->clientStatusUpdater.inStatusQ.push_back(std::move(trLogInfo->trLogWriter));
		trLogInfo->flushed = true;
	}
}

void Transaction::setVersion( Version v ) {
	startTime = now();
	if (readVersion.isValid())
		throw read_version_already_set();
	if (v <= 0)
		throw version_invalid();
	readVersion = v;
}

Future<Optional<Value>> Transaction::get( const Key& key, bool snapshot ) {
	++cx->transactionLogicalReads;
	//ASSERT (key < allKeys.end);

	//There are no keys in the database with size greater than KEY_SIZE_LIMIT
	if(key.size() > (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		return Optional<Value>();

	auto ver = getReadVersion();

/*	if (!systemKeys.contains(key))
		return Optional<Value>(Value()); */

	if( !snapshot )
		tr.transaction.read_conflict_ranges.push_back(tr.arena, singleKeyRange(key, tr.arena));

	return getValue( ver, key, cx, info, trLogInfo );
}

void Watch::setWatch(Future<Void> watchFuture) {
	this->watchFuture = watchFuture;

	//Cause the watch loop to go around and start waiting on watchFuture
	onSetWatchTrigger.send(Void());
}

//FIXME: This seems pretty horrible. Now a Database can't die until all of its watches do...
ACTOR Future<Void> watch( Reference<Watch> watch, Database cx, Transaction *self ) {
	cx->addWatch();
	try {
		self->watches.push_back(watch);

		choose {
			// RYOW write to value that is being watched (if applicable)
			// Errors
			when(Void _ = wait(watch->onChangeTrigger.getFuture())) { }

			// NativeAPI finished commit and updated watchFuture
			when(Void _ = wait(watch->onSetWatchTrigger.getFuture())) {

				// NativeAPI watchValue future finishes or errors
				Void _ = wait(watch->watchFuture);
			}
		}
	}
	catch(Error &e) {
		cx->removeWatch();
		throw;
	}

	cx->removeWatch();
	return Void();
}

Future< Void > Transaction::watch( Reference<Watch> watch ) {
	return ::watch(watch, cx, this);
}

ACTOR Future< Standalone< VectorRef< const char*>>> getAddressesForKeyActor( Key key, Future<Version> ver, Database cx, TransactionInfo info ) {
	state vector<StorageServerInterface> ssi;

	// If key >= allKeys.end, then getRange will return a kv-pair with an empty value. This will result in our serverInterfaces vector being empty, which will cause us to return an empty addresses list.

	state Key ksKey = keyServersKey(key);
	Future<Standalone<RangeResultRef>> futureServerUids = getRange(cx, ver, lastLessOrEqual(ksKey), firstGreaterThan(ksKey), GetRangeLimits(1), false, info);
	Standalone<RangeResultRef> serverUids = wait( futureServerUids );

	ASSERT( serverUids.size() ); // every shard needs to have a team

	vector<UID> src;
	vector<UID> ignore; // 'ignore' is so named because it is the vector into which we decode the 'dest' servers in the case where this key is being relocated. But 'src' is the canonical location until the move is finished, because it could be cancelled at any time.
	decodeKeyServersValue(serverUids[0].value, src, ignore);
	Optional<vector<StorageServerInterface>> serverInterfaces = wait( transactionalGetServerInterfaces(ver, cx, info, src) );

	ASSERT( serverInterfaces.present() );  // since this is happening transactionally, /FF/keyServers and /FF/serverList need to be consistent with one another
	ssi = serverInterfaces.get();

	Standalone<VectorRef<const char*>> addresses;
	for (auto i : ssi) {
		std::string ipString = toIPString(i.address().ip);
		char* c_string = new (addresses.arena()) char[ipString.length()+1];
		strcpy(c_string, ipString.c_str());
		addresses.push_back(addresses.arena(), c_string);
	}
	return addresses;
}

Future< Standalone< VectorRef< const char*>>> Transaction::getAddressesForKey( const Key& key ) {
	++cx->transactionLogicalReads;
	auto ver = getReadVersion();

	return getAddressesForKeyActor(key, ver, cx, info);
}

ACTOR Future< Key > getKeyAndConflictRange(
	Database cx, KeySelector k, Future<Version> version, Promise<std::pair<Key, Key>> conflictRange, TransactionInfo info)
{
	try {
		Key rep = wait( getKey(cx, k, version, info) );
		if( k.offset <= 0 )
			conflictRange.send( std::make_pair( rep, k.orEqual ? keyAfter( k.getKey() ) : Key(k.getKey(), k.arena()) ) );
		else
			conflictRange.send( std::make_pair( k.orEqual ? keyAfter( k.getKey() ) : Key(k.getKey(), k.arena()), keyAfter( rep ) ) );
		return std::move(rep);
	} catch( Error&e ) {
		conflictRange.send(std::make_pair(Key(), Key()));
		throw;
	}
}

Future< Key > Transaction::getKey( const KeySelector& key, bool snapshot ) {
	++cx->transactionLogicalReads;
	if( snapshot )
		return ::getKey(cx, key, getReadVersion(), info);

	Promise<std::pair<Key, Key>> conflictRange;
	extraConflictRanges.push_back( conflictRange.getFuture() );
	return getKeyAndConflictRange( cx, key, getReadVersion(), conflictRange, info );
}

Future< Standalone<RangeResultRef> > Transaction::getRange(
	const KeySelector& begin,
	const KeySelector& end,
	GetRangeLimits limits,
	bool snapshot,
	bool reverse )
{
	++cx->transactionLogicalReads;

	if( limits.isReached() )
		return Standalone<RangeResultRef>();

	if( !limits.isValid() )
		return range_limits_invalid();

	ASSERT(limits.rows != 0);

	KeySelector b = begin;
	if( b.orEqual ) {
		TEST(true); // Native begin orEqual==true
		b.removeOrEqual(b.arena());
	}

	KeySelector e = end;
	if( e.orEqual ) {
		TEST(true); // Native end orEqual==true
		e.removeOrEqual(e.arena());
	}

	if( b.offset >= e.offset && b.getKey() >= e.getKey() ) {
		TEST(true); // Native range inverted
		return Standalone<RangeResultRef>();
	}

	Promise<std::pair<Key, Key>> conflictRange;
	if(!snapshot) {
		extraConflictRanges.push_back( conflictRange.getFuture() );
	}

	return ::getRange(cx, trLogInfo, getReadVersion(), b, e, limits, conflictRange, snapshot, reverse, info);
}

Future< Standalone<RangeResultRef> > Transaction::getRange(
	const KeySelector& begin,
	const KeySelector& end,
	int limit,
	bool snapshot,
	bool reverse )
{
	return getRange( begin, end, GetRangeLimits( limit ), snapshot, reverse );
}

void Transaction::addReadConflictRange( KeyRangeRef const& keys ) {
	ASSERT( !keys.empty() );

	//There aren't any keys in the database with size larger than KEY_SIZE_LIMIT, so if range contains large keys
	//we can translate it to an equivalent one with smaller keys
	KeyRef begin = keys.begin;
	KeyRef end = keys.end;

	if(begin.size() > (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		begin = begin.substr(0, (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1);
	if(end.size() > (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		end = end.substr(0, (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1);

	KeyRangeRef r = KeyRangeRef(begin, end);

	if(r.empty()) {
		return;
	}

	tr.transaction.read_conflict_ranges.push_back_deep( tr.arena, r );
}

void Transaction::makeSelfConflicting() {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(LiteralStringRef("\xFF/SC/"));
	wr << g_random->randomUniqueID();
	auto r = singleKeyRange( wr.toStringRef(), tr.arena );
	tr.transaction.read_conflict_ranges.push_back( tr.arena, r );
	tr.transaction.write_conflict_ranges.push_back( tr.arena, r );
}

void Transaction::set( const KeyRef& key, const ValueRef& value, bool addConflictRange ) {

	if(key.size() > (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		throw key_too_large();
	if(value.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT)
		throw value_too_large();

	auto &req = tr;
	auto &t = req.transaction;
	auto r = singleKeyRange( key, req.arena );
	auto v = ValueRef( req.arena, value );
	t.mutations.push_back( req.arena, MutationRef( MutationRef::SetValue, r.begin, v ) );

	if( addConflictRange ) {
		t.write_conflict_ranges.push_back( req.arena, r );
	}
}

void Transaction::atomicOp(const KeyRef& key, const ValueRef& operand, MutationRef::Type operationType, bool addConflictRange) {
	if(key.size() > (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		throw key_too_large();
	if(operand.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT)
		throw value_too_large();

	if (apiVersionAtLeast(510)) {
		if (operationType == MutationRef::Min)
			operationType = MutationRef::MinV2;
		else if (operationType == MutationRef::And)
			operationType = MutationRef::AndV2;
	}

	auto &req = tr;
	auto &t = req.transaction;
	auto r = singleKeyRange( key, req.arena );
	auto v = ValueRef( req.arena, operand );

	t.mutations.push_back( req.arena, MutationRef( operationType, r.begin, v ) );

	if( addConflictRange )
		t.write_conflict_ranges.push_back( req.arena, r );

	TEST(true); //NativeAPI atomic operation
}

void Transaction::clear( const KeyRangeRef& range, bool addConflictRange ) {
	auto &req = tr;
	auto &t = req.transaction;

	KeyRef begin = range.begin;
	KeyRef end = range.end;

	//There aren't any keys in the database with size larger than KEY_SIZE_LIMIT, so if range contains large keys
	//we can translate it to an equivalent one with smaller keys
	if(begin.size() > (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		begin = begin.substr(0, (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1);
	if(end.size() > (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		end = end.substr(0, (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1);

	auto r = KeyRangeRef( req.arena, KeyRangeRef(begin, end) );
	if (r.empty()) return;

	t.mutations.push_back( req.arena, MutationRef( MutationRef::ClearRange, r.begin, r.end ) );

	if(addConflictRange)
		t.write_conflict_ranges.push_back( req.arena, r );
}
void Transaction::clear( const KeyRef& key, bool addConflictRange ) {

	//There aren't any keys in the database with size larger than KEY_SIZE_LIMIT
	if(key.size() > (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		return;

	auto &req = tr;
	auto &t = req.transaction;

	//efficient single key range clear range mutation, see singleKeyRange
	uint8_t* data = new ( req.arena ) uint8_t[ key.size()+1 ];
	memcpy(data, key.begin(), key.size() );
	data[key.size()] = 0;
	t.mutations.push_back( req.arena, MutationRef( MutationRef::ClearRange, KeyRef(data,key.size()), KeyRef(data, key.size()+1)) );

	if(addConflictRange)
		t.write_conflict_ranges.push_back( req.arena, KeyRangeRef( KeyRef(data,key.size()), KeyRef(data, key.size()+1) ) );
}
void Transaction::addWriteConflictRange( const KeyRangeRef& keys ) {
	ASSERT( !keys.empty() );
	auto &req = tr;
	auto &t = req.transaction;

	//There aren't any keys in the database with size larger than KEY_SIZE_LIMIT, so if range contains large keys
	//we can translate it to an equivalent one with smaller keys
	KeyRef begin = keys.begin;
	KeyRef end = keys.end;

	if (begin.size() > (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		begin = begin.substr(0, (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT) + 1);
	if (end.size() > (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		end = end.substr(0, (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT) + 1);

	KeyRangeRef r = KeyRangeRef(begin, end);

	if (r.empty()) {
		return;
	}

	t.write_conflict_ranges.push_back_deep( req.arena, r );
}

double Transaction::getBackoff() {
	double b = backoff * g_random->random01();
	backoff = std::min(backoff * CLIENT_KNOBS->BACKOFF_GROWTH_RATE, options.maxBackoff);
	return b;
}

void Transaction::reset() {
	tr = CommitTransactionRequest();
	readVersion = Future<Version>();
	extraConflictRanges.clear();
	versionstampPromise = Promise<Standalone<StringRef>>();
	commitResult = Promise<Void>();
	committing = Future<Void>();
	info.taskID = cx->taskID;
	info.debugID = Optional<UID>();
	flushTrLogsIfEnabled();
	trLogInfo = Reference<TransactionLogInfo>(createTrLogInfoProbabilistically(cx));
	cancelWatches();

	if(apiVersionAtLeast(16)) {
		options.reset();
		setPriority(GetReadVersionRequest::PRIORITY_DEFAULT);
		if(cx->lockAware)
			options.lockAware = true;
	}
}

void Transaction::fullReset() {
	reset();
	backoff = CLIENT_KNOBS->DEFAULT_BACKOFF;
}

int Transaction::apiVersionAtLeast(int minVersion) const {
	if(cx->cluster)
		return cx->cluster->apiVersionAtLeast(minVersion);

	return true;
}



class MutationBlock {
public:
	bool mutated;
	bool cleared;
	ValueRef setValue;

	MutationBlock() : mutated(false) {}
	MutationBlock(bool _cleared) : mutated(true), cleared(_cleared) {}
	MutationBlock(ValueRef value) : mutated(true), cleared(false), setValue(value) {}
};

bool compareBegin( KeyRangeRef lhs, KeyRangeRef rhs ) { return lhs.begin < rhs.begin; }

// If there is any intersection between the two given sets of ranges, returns a range that
//   falls within the intersection
Optional<KeyRangeRef> intersects(VectorRef<KeyRangeRef> lhs, VectorRef<KeyRangeRef> rhs) {
	if( lhs.size() && rhs.size() ) {
		std::sort( lhs.begin(), lhs.end(), compareBegin );
		std::sort( rhs.begin(), rhs.end(), compareBegin );

		int l = 0, r = 0;
		while(l < lhs.size() && r < rhs.size()) {
			if( lhs[l].end <= rhs[r].begin )
				l++;
			else if( rhs[r].end <= lhs[l].begin )
				r++;
			else
				return lhs[l] & rhs[r];
		}
	}

	return Optional<KeyRangeRef>();
}

ACTOR void checkWrites( Database cx, Future<Void> committed, Promise<Void> outCommitted, CommitTransactionRequest req, Transaction* checkTr )
{
	state Version version;
	try {
		Void _ = wait( committed );
		// If the commit is successful, by definition the transaction still exists for now.  Grab the version, and don't use it again.
		version = checkTr->getCommittedVersion();
		outCommitted.send(Void());
	} catch (Error& e) {
		outCommitted.sendError(e);
		return;
	}

	Void _ = wait( delay( g_random->random01() ) ); // delay between 0 and 1 seconds

	//Future<Optional<Version>> version, Database cx, CommitTransactionRequest req ) {
	state KeyRangeMap<MutationBlock> expectedValues;

	auto &mutations = req.transaction.mutations;
	state int mCount = mutations.size(); // debugging info for traceEvent

	for( int idx = 0; idx < mutations.size(); idx++) {
		if( mutations[idx].type == MutationRef::SetValue )
			expectedValues.insert( singleKeyRange( mutations[idx].param1 ),
				MutationBlock( mutations[idx].param2 ) );
		else if( mutations[idx].type == MutationRef::ClearRange )
			expectedValues.insert( KeyRangeRef( mutations[idx].param1, mutations[idx].param2 ),
				MutationBlock( true ) );
	}

	try {
		state Transaction tr(cx);
		tr.setVersion( version );
		state int checkedRanges = 0;
		state KeyRangeMap<MutationBlock>::Ranges ranges = expectedValues.ranges();
		state KeyRangeMap<MutationBlock>::Iterator it = ranges.begin();
		for(; it != ranges.end(); ++it) {
			state MutationBlock m = it->value();
			if( m.mutated ) {
				checkedRanges++;
				if( m.cleared ) {
					Standalone<RangeResultRef> shouldBeEmpty = wait(
						tr.getRange( it->range(), 1 ) );
					if( shouldBeEmpty.size() ) {
						TraceEvent(SevError, "CheckWritesFailed").detail("Class", "Clear").detail("KeyBegin", printable(it->range().begin).c_str())
							.detail("KeyEnd", printable(it->range().end).c_str());
						return;
					}
				} else {
					Optional<Value> val = wait( tr.get( it->range().begin ) );
					if( !val.present() || val.get() != m.setValue ) {
						TraceEvent evt = TraceEvent(SevError, "CheckWritesFailed").detail("Class", "Set").detail("Key", printable(it->range().begin).c_str())
							.detail("Expected", printable(m.setValue).c_str());
						if( !val.present() )
							evt.detail("Actual", "_Value Missing_");
						else
							evt.detail("Actual", printable(val.get()).c_str());
						return;
					}
				}
			}
		}
		TraceEvent("CheckWritesSuccess").detail("Version", version).detail("MutationCount", mCount).detail("CheckedRanges", checkedRanges);
	} catch( Error& e ) {
		bool ok = e.code() == error_code_transaction_too_old || e.code() == error_code_future_version;
		TraceEvent( ok ? SevWarn : SevError, "CheckWritesFailed" ).error(e);
		throw;
	}
}

ACTOR static Future<Void> commitDummyTransaction( Database cx, KeyRange range, TransactionInfo info, TransactionOptions options ) {
	state Transaction tr(cx);
	state int retries = 0;
	loop {
		try {
			TraceEvent("CommitDummyTransaction").detail("Key", printable(range.begin)).detail("Retries", retries);
			tr.options = options;
			tr.info.taskID = info.taskID;
			tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::CAUSAL_WRITE_RISKY );
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );
			tr.addReadConflictRange(range);
			tr.addWriteConflictRange(range);
			Void _ = wait( tr.commit() );
			return Void();
		} catch (Error& e) {
			TraceEvent("CommitDummyTransactionError").error(e,true).detail("Key", printable(range.begin)).detail("Retries", retries);
			Void _ = wait( tr.onError(e) );
		}
		++retries;
	}
}

void Transaction::cancelWatches(Error const& e) {
	for(int i = 0; i < watches.size(); ++i)
		if(!watches[i]->onChangeTrigger.isSet())
			watches[i]->onChangeTrigger.sendError(e);

	watches.clear();
}

void Transaction::setupWatches() {
	try {
		Future<Version> watchVersion = getCommittedVersion() > 0 ? getCommittedVersion() : getReadVersion();

		for(int i = 0; i < watches.size(); ++i)
			watches[i]->setWatch(watchValue( watchVersion, watches[i]->key, watches[i]->value, cx, options.getReadVersionFlags, info ));

		watches.clear();
	}
	catch(Error &e) {
		ASSERT(false); // The above code must NOT throw because commit has already occured.
		throw internal_error();
	}
}

ACTOR static Future<Void> tryCommit( Database cx, Reference<TransactionLogInfo> trLogInfo, CommitTransactionRequest req, Future<Version> readVersion, TransactionInfo info, Version* pCommittedVersion, Transaction* tr, TransactionOptions options) {
	state TraceInterval interval( "TransactionCommit" );
	state double startTime;
	if (info.debugID.present())
		TraceEvent(interval.begin()).detail( "Parent", info.debugID.get() );

	try {
		Version v = wait( readVersion );
		req.transaction.read_snapshot = v;

		startTime = now();
		state Optional<UID> commitID = Optional<UID>();
		if(info.debugID.present()) {
			commitID = g_nondeterministic_random->randomUniqueID();
			g_traceBatch.addAttach("CommitAttachID", info.debugID.get().first(), commitID.get().first());
			g_traceBatch.addEvent("CommitDebug", commitID.get().first(), "NativeAPI.commit.Before");
		}

		req.debugID = commitID;
		state Future<CommitID> reply;
		if (options.commitOnFirstProxy) {
			const std::vector<MasterProxyInterface>& proxies = cx->clientInfo->get().proxies;
			reply = proxies.size() ? throwErrorOr ( brokenPromiseToMaybeDelivered ( proxies[0].commit.tryGetReply(req) ) ) : Never();
		} else {
			reply = loadBalance( cx->getMasterProxies(), &MasterProxyInterface::commit, req, TaskDefaultPromiseEndpoint, true );
		}

		choose {
			when ( Void _ = wait( cx->onMasterProxiesChanged() ) ) {
				reply.cancel();
				throw request_maybe_delivered();
			}
			when (CommitID ci = wait( reply )) {
				Version v = ci.version;
				if (v != invalidVersion) {
					if (info.debugID.present())
						TraceEvent(interval.end()).detail("CommittedVersion", v);
					*pCommittedVersion = v;

					Standalone<StringRef> ret = makeString(10);
					placeVersionstamp(mutateString(ret), v, ci.txnBatchId);
					tr->versionstampPromise.send(ret);

					tr->numErrors = 0;
					cx->transactionsCommitCompleted++;
					cx->transactionCommittedMutations += req.transaction.mutations.size();
					cx->transactionCommittedMutationBytes += req.transaction.mutations.expectedSize();

					if(info.debugID.present())
						g_traceBatch.addEvent("CommitDebug", commitID.get().first(), "NativeAPI.commit.After");

					double latency = now() - startTime;
					cx->commitLatencies.addSample(latency);
					cx->latencies.addSample(now() - tr->startTime);
					if (trLogInfo)
						trLogInfo->addLog(FdbClientLogEvents::EventCommit(startTime, latency, req.transaction.mutations.size(), req.transaction.mutations.expectedSize(), req));
					return Void();
				} else {
					if (info.debugID.present())
						TraceEvent(interval.end()).detail("Conflict", 1);

					if(info.debugID.present())
						g_traceBatch.addEvent("CommitDebug", commitID.get().first(), "NativeAPI.commit.After");

					throw not_committed();
				}
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_request_maybe_delivered || e.code() == error_code_commit_unknown_result) {
			// We don't know if the commit happened, and it might even still be in flight.

			if (!options.causalWriteRisky) {
				// Make sure it's not still in flight, either by ensuring the master we submitted to is dead, or the version we submitted with is dead, or by committing a conflicting transaction successfully
				//if ( cx->getMasterProxies()->masterGeneration <= originalMasterGeneration )

				// To ensure the original request is not in flight, we need a key range which intersects its read conflict ranges
				// We pick a key range which also intersects its write conflict ranges, since that avoids potentially creating conflicts where there otherwise would be none
				// We make the range as small as possible (a single key range) to minimize conflicts
				// The intersection will never be empty, because if it were (since !causalWriteRisky) makeSelfConflicting would have been applied automatically to req
				KeyRangeRef selfConflictingRange = intersects( req.transaction.write_conflict_ranges, req.transaction.read_conflict_ranges ).get();

				TEST(true);  // Waiting for dummy transaction to report commit_unknown_result

				Void _ = wait( commitDummyTransaction( cx, singleKeyRange(selfConflictingRange.begin), info, tr->options ) );
			}

			// The user needs to be informed that we aren't sure whether the commit happened.  Standard retry loops retry it anyway (relying on transaction idempotence) but a client might do something else.
			throw commit_unknown_result();
		} else {
			if (e.code() != error_code_transaction_too_old && e.code() != error_code_not_committed && e.code() != error_code_database_locked)
				TraceEvent(SevError, "tryCommitError").error(e);
			if (trLogInfo)
				trLogInfo->addLog(FdbClientLogEvents::EventCommitError(startTime, static_cast<int>(e.code()), req));
			throw;
		}
	}
}

Future<Void> Transaction::commitMutations() {
	try {
		//if this is a read-only transaction return immediately
		if( !tr.transaction.write_conflict_ranges.size() && !tr.transaction.mutations.size() ) {
			numErrors = 0;

			committedVersion = invalidVersion;
			versionstampPromise.sendError(no_commit_version());
			return Void();
		}

		cx->transactionsCommitStarted++;

		if(options.readOnly)
			return transaction_read_only();

		cx->mutationsPerCommit.addSample(tr.transaction.mutations.size());
		cx->bytesPerCommit.addSample(tr.transaction.mutations.expectedSize());

		size_t transactionSize = tr.transaction.mutations.expectedSize() + tr.transaction.read_conflict_ranges.expectedSize() + tr.transaction.write_conflict_ranges.expectedSize();
		if (transactionSize > (uint64_t)FLOW_KNOBS->PACKET_WARNING) {
			TraceEvent(!g_network->isSimulated() ? SevWarnAlways : SevWarn, "LargeTransaction")
				.detail("Size", transactionSize)
				.detail("NumMutations", tr.transaction.mutations.size())
				.detail("readConflictSize", tr.transaction.read_conflict_ranges.expectedSize())
				.detail("writeConflictSize", tr.transaction.write_conflict_ranges.expectedSize())
				.suppressFor(1.0);
		}

		if(!apiVersionAtLeast(300)) {
			transactionSize = tr.transaction.mutations.expectedSize(); // Old API versions didn't account for conflict ranges when determining whether to throw transaction_too_large
		}

		if (transactionSize > (options.customTransactionSizeLimit == 0 ? (uint64_t)CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT : (uint64_t)options.customTransactionSizeLimit))
			return transaction_too_large();

		if( !readVersion.isValid() )
			getReadVersion( GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY ); // sets up readVersion field.  We had no reads, so no need for (expensive) full causal consistency.

		bool isCheckingWrites = options.checkWritesEnabled && g_random->random01() < 0.01;
		for(int i=0; i<extraConflictRanges.size(); i++)
			if (extraConflictRanges[i].isReady() && extraConflictRanges[i].get().first < extraConflictRanges[i].get().second )
				tr.transaction.read_conflict_ranges.push_back( tr.arena, KeyRangeRef(extraConflictRanges[i].get().first, extraConflictRanges[i].get().second) );

		if( !options.causalWriteRisky && !intersects( tr.transaction.write_conflict_ranges, tr.transaction.read_conflict_ranges ).present() )
			makeSelfConflicting();

		if (isCheckingWrites) {
			// add all writes into the read conflict range...
			tr.transaction.read_conflict_ranges.append( tr.arena, tr.transaction.write_conflict_ranges.begin(), tr.transaction.write_conflict_ranges.size() );
		}

		if ( options.debugDump ) {
			UID u = g_nondeterministic_random->randomUniqueID();
			TraceEvent("TransactionDump", u);
			for(auto i=tr.transaction.mutations.begin(); i!=tr.transaction.mutations.end(); ++i)
				TraceEvent("TransactionMutation", u).detail("T", i->type).detail("P1", printable(i->param1)).detail("P2", printable(i->param2));
		}

		tr.isLockAware = options.lockAware;

		Future<Void> commitResult = tryCommit( cx, trLogInfo, tr, readVersion, info, &this->committedVersion, this, options );

		if (isCheckingWrites) {
			Promise<Void> committed;
			checkWrites( cx, commitResult, committed, tr, this );
			return committed.getFuture();
		}
		return commitResult;
	} catch( Error& e ) {
		TraceEvent("ClientCommitError").error(e);
		return Future<Void>( e );
	} catch( ... ) {
		Error e( error_code_unknown_error );
		TraceEvent("ClientCommitError").error(e);
		return Future<Void>( e );
	}
}

ACTOR Future<Void> commitAndWatch(Transaction *self) {
	try {
		Void _ = wait(self->commitMutations());

		if(!self->watches.empty()) {
			self->setupWatches();
		}

		self->reset();
		return Void();
	}
	catch(Error &e) {
		if(e.code() != error_code_actor_cancelled) {
			if(!self->watches.empty()) {
				self->cancelWatches(e);
			}

			self->versionstampPromise.sendError(transaction_invalid_version());
			self->reset();
		}

		throw;
	}
}

Future<Void> Transaction::commit() {
	ASSERT(!committing.isValid());
	committing = commitAndWatch(this);
	return committing;
}

void Transaction::setPriority( uint32_t priorityFlag ) {
	options.getReadVersionFlags = (options.getReadVersionFlags & ~GetReadVersionRequest::FLAG_PRIORITY_MASK) | priorityFlag;
}

void Transaction::setOption( FDBTransactionOptions::Option option, Optional<StringRef> value ) {
	switch(option) {
		case FDBTransactionOptions::INITIALIZE_NEW_DATABASE:
			validateOptionValue(value, false);
			if(readVersion.isValid())
				throw read_version_already_set();
			readVersion = Version(0);
			options.causalWriteRisky = true;
			break;

		case FDBTransactionOptions::CAUSAL_READ_RISKY:
			validateOptionValue(value, false);
			options.getReadVersionFlags |= GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY;
			break;

		case FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE:
			validateOptionValue(value, false);
			setPriority(GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE);
			break;

		case FDBTransactionOptions::PRIORITY_BATCH:
			validateOptionValue(value, false);
			setPriority(GetReadVersionRequest::PRIORITY_BATCH);
			break;

		case FDBTransactionOptions::CAUSAL_WRITE_RISKY:
			validateOptionValue(value, false);
			options.causalWriteRisky = true;
			break;

		case FDBTransactionOptions::COMMIT_ON_FIRST_PROXY:
			validateOptionValue(value, false);
			options.commitOnFirstProxy = true;
			break;

		case FDBTransactionOptions::CHECK_WRITES_ENABLE:
			validateOptionValue(value, false);
			options.checkWritesEnabled = true;
			break;

		case FDBTransactionOptions::DEBUG_DUMP:
			validateOptionValue(value, false);
			options.debugDump = true;
			break;

		case FDBTransactionOptions::TRANSACTION_LOGGING_ENABLE:
			validateOptionValue(value, true);

			if(value.get().size() > 100) {
				throw invalid_option_value();
			}

			if(trLogInfo) {
				if(!trLogInfo->identifier.present()) {
					trLogInfo->identifier = printable(value.get());
				}
				else if(trLogInfo->identifier.get() != printable(value.get())) {
					TraceEvent(SevWarn, "CannotChangeTransactionLoggingIdentifier").detail("PreviousIdentifier", trLogInfo->identifier.get()).detail("NewIdentifier", printable(value.get()));
					throw client_invalid_operation();
				}
			}
			else {
				trLogInfo = Reference<TransactionLogInfo>(new TransactionLogInfo(printable(value.get())));
			}

			break;

		case FDBTransactionOptions::MAX_RETRY_DELAY:
			validateOptionValue(value, true);
			options.maxBackoff = extractIntOption(value, 0, std::numeric_limits<int32_t>::max()) / 1000.0;
			break;

		case FDBTransactionOptions::LOCK_AWARE:
			validateOptionValue(value, false);
			options.lockAware = true;
			options.readOnly = false;
			break;

		case FDBTransactionOptions::READ_LOCK_AWARE:
			validateOptionValue(value, false);
			if(!options.lockAware) {
				options.lockAware = true;
				options.readOnly = true;
			}
			break;

		default:
			break;
	}
}

ACTOR Future<GetReadVersionReply> getConsistentReadVersion( DatabaseContext *cx, uint32_t transactionCount, uint32_t flags, Optional<UID> debugID ) {
	try {
		if( debugID.present() )
			g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "NativeAPI.getConsistentReadVersion.Before");
		loop {
			state GetReadVersionRequest req( transactionCount, flags, debugID );
			choose {
				when ( Void _ = wait( cx->onMasterProxiesChanged() ) ) {}
				when ( GetReadVersionReply v = wait( loadBalance( cx->getMasterProxies(), &MasterProxyInterface::getConsistentReadVersion, req, cx->taskID ) ) ) {
					if( debugID.present() )
						g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "NativeAPI.getConsistentReadVersion.After");
					ASSERT( v.version > 0 );
					return v;
				}
			}
		}
	} catch (Error& e) {
		if( e.code() != error_code_broken_promise && e.code() != error_code_actor_cancelled )
			TraceEvent(SevError, "getConsistentReadVersionError").error(e);
		throw;
	}
}

ACTOR Future<Void> readVersionBatcher( DatabaseContext *cx, FutureStream< std::pair< Promise<GetReadVersionReply>, Optional<UID> > > versionStream, uint32_t flags ) {
	state std::vector< Promise<GetReadVersionReply> > requests;
	state PromiseStream< Future<Void> > addActor;
	state Future<Void> collection = actorCollection( addActor.getFuture() );
	state Future<Void> timeout;
	state Optional<UID> debugID;
	state bool send_batch;

	// dynamic batching
	state PromiseStream<double> replyTimes;
	state PromiseStream<Error> _errorStream;
	state double batchTime = 0;
	state double lastRequestTime = now();

	loop {
		send_batch = false;
		choose {
			when(std::pair< Promise<GetReadVersionReply>, Optional<UID> > req = waitNext(versionStream)) {
				if (req.second.present()) {
					if (!debugID.present())
						debugID = g_nondeterministic_random->randomUniqueID();
					g_traceBatch.addAttach("TransactionAttachID", req.second.get().first(), debugID.get().first());
				}
				requests.push_back(req.first);
				if (requests.size() == CLIENT_KNOBS->MAX_BATCH_SIZE)
					send_batch = true;
				else if (!timeout.isValid())
					timeout = delay(batchTime, cx->taskID);
			}
			when(Void _ = wait(timeout.isValid() ? timeout : Never())) {
				send_batch = true;
			}
			// dynamic batching monitors reply latencies
			when(double reply_latency = waitNext(replyTimes.getFuture())){
				double target_latency = reply_latency * 0.5;
				batchTime = min(0.1 * target_latency + 0.9 * batchTime, CLIENT_KNOBS->GRV_BATCH_TIMEOUT);
			}
			when(Void _ = wait(collection)){} // for errors
		}
		if (send_batch) {
			int count = requests.size();
			ASSERT(count);

			// dynamic batching
			Promise<GetReadVersionReply> GRVReply;
			requests.push_back(GRVReply);
			addActor.send(timeReply(GRVReply.getFuture(), replyTimes));

			Future<Void> batch =
				broadcast(
					getConsistentReadVersion(cx, count, flags, std::move(debugID)),
					std::vector< Promise<GetReadVersionReply> >(std::move(requests)));
			debugID = Optional<UID>();
			requests = std::vector< Promise<GetReadVersionReply> >();
			addActor.send(batch);
			timeout = Future<Void>();
		}
	}
}

ACTOR Future<Version> extractReadVersion(DatabaseContext* cx, Reference<TransactionLogInfo> trLogInfo, Future<GetReadVersionReply> f, bool lockAware, double startTime) {
	GetReadVersionReply rep = wait(f);
	double latency = now() - startTime;
	cx->GRVLatencies.addSample(latency);
	if (trLogInfo)
		trLogInfo->addLog(FdbClientLogEvents::EventGetVersion(startTime, latency));
	if(rep.locked && !lockAware)
		throw database_locked();

	return rep.version;
}

Future<Version> Transaction::getReadVersion(uint32_t flags) {
	cx->transactionReadVersions++;
	flags |= options.getReadVersionFlags;

	auto& batcher = cx->versionBatcher[ flags ];
	if (!batcher.actor.isValid()) {
		batcher.actor = readVersionBatcher( cx.getPtr(), batcher.stream.getFuture(), flags );
	}
	if (!readVersion.isValid()) {
		Promise<GetReadVersionReply> p;
		batcher.stream.send( std::make_pair( p, info.debugID ) );
		startTime = now();
		readVersion = extractReadVersion( cx.getPtr(), trLogInfo, p.getFuture(), options.lockAware, startTime);
	}
	return readVersion;
}

Future<Standalone<StringRef>> Transaction::getVersionstamp() {
	if(committing.isValid()) {
		return transaction_invalid_version();
	}
	return versionstampPromise.getFuture();
}

Future<Void> Transaction::onError( Error const& e ) {
	if (e.code() == error_code_success)
	{
		return client_invalid_operation();
	}
	if (e.code() == error_code_not_committed ||
		e.code() == error_code_commit_unknown_result ||
		e.code() == error_code_database_locked)
	{
		if(e.code() == error_code_not_committed)
			cx->transactionsNotCommitted++;
		if(e.code() == error_code_commit_unknown_result)
			cx->transactionsMaybeCommitted++;

		double backoff = getBackoff();
		reset();
		return delay( backoff, info.taskID );
	}
	if (e.code() == error_code_transaction_too_old ||
		e.code() == error_code_future_version)
	{
		if( e.code() == error_code_transaction_too_old )
			cx->transactionsTooOld++;
		else if( e.code() == error_code_future_version )
			cx->transactionsFutureVersions++;

		double maxBackoff = options.maxBackoff;
		reset();
		return delay( std::min(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, maxBackoff), info.taskID );
	}

	if(g_network->isSimulated() && ++numErrors % 10 == 0)
		TraceEvent(SevWarnAlways, "TransactionTooManyRetries").detail("NumRetries", numErrors);

	return e;
}

ACTOR Future<Void> trackBoundedStorageMetrics(
	KeyRange keys,
	Reference<LocationInfo> location,
	StorageMetrics x,
	StorageMetrics halfError,
	PromiseStream<StorageMetrics> deltaStream)
{
	try {
		loop {
			WaitMetricsRequest req( keys, x - halfError, x + halfError );
			StorageMetrics nextX = wait( loadBalance( location, &StorageServerInterface::waitMetrics, req ) );
			deltaStream.send( nextX - x );
			x = nextX;
		}
	} catch (Error& e) {
		deltaStream.sendError(e);
		throw e;
	}
}

ACTOR Future< StorageMetrics > waitStorageMetricsMultipleLocations(
	vector< pair<KeyRange,Reference<LocationInfo>> > locations,
	StorageMetrics min,
	StorageMetrics max,
	StorageMetrics permittedError)
{
	state int nLocs = locations.size();
	state vector<Future<StorageMetrics>> fx( nLocs );
	state StorageMetrics total;
	state PromiseStream<StorageMetrics> deltas;
	state vector<Future<Void>> wx( fx.size() );
	state StorageMetrics halfErrorPerMachine = permittedError * (0.5 / nLocs);
	state StorageMetrics maxPlus = max + halfErrorPerMachine * (nLocs-1);
	state StorageMetrics minMinus = min - halfErrorPerMachine * (nLocs-1);

	for(int i=0; i<nLocs; i++) {
		WaitMetricsRequest req(locations[i].first, StorageMetrics(), StorageMetrics());
		req.min.bytes = 0;
		req.max.bytes = -1;
		fx[i] = loadBalance( locations[i].second, &StorageServerInterface::waitMetrics, req, TaskDataDistribution );
	}
	Void _ = wait( waitForAll(fx) );

	// invariant: true total is between (total-permittedError/2, total+permittedError/2)
	for(int i=0; i<nLocs; i++)
		total += fx[i].get();

	if (!total.allLessOrEqual( maxPlus )) return total;
	if (!minMinus.allLessOrEqual( total )) return total;

	for(int i=0; i<nLocs; i++)
		wx[i] = trackBoundedStorageMetrics( locations[i].first, locations[i].second, fx[i].get(), halfErrorPerMachine, deltas );

	loop {
		StorageMetrics delta = waitNext(deltas.getFuture());
		total += delta;
		if (!total.allLessOrEqual( maxPlus )) return total;
		if (!minMinus.allLessOrEqual( total )) return total;
	}
}

ACTOR Future< StorageMetrics > waitStorageMetrics(
	Database cx,
	KeyRange keys,
	StorageMetrics min,
	StorageMetrics max,
	StorageMetrics permittedError,
	int shardLimit )
{
	state int tooManyShardsCount = 0;
	loop {
		state vector< pair<KeyRange, Reference<LocationInfo>> > locations = wait( getKeyRangeLocations( cx, keys, shardLimit, false, &StorageServerInterface::waitMetrics, TransactionInfo(TaskDataDistribution) ) );
		
		if( locations.size() == shardLimit ) {
			TraceEvent(!g_network->isSimulated() && ++tooManyShardsCount >= 15 ? SevWarnAlways : SevWarn, "WaitStorageMetricsPenalty")
				.detail("Keys", printable(keys))
				.detail("Locations", locations.size())
				.detail("Limit", CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT)
				.detail("JitteredSecondsOfPenitence", CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY);
			Void _ = wait(delayJittered(CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY, TaskDataDistribution));
			// make sure that the next getKeyRangeLocations() call will actually re-fetch the range
			cx->invalidateCache( keys );
		} else {
			tooManyShardsCount = 0;
			//SOMEDAY: Right now, if there are too many shards we delay and check again later. There may be a better solution to this.
			try {
				if (locations.size() > 1) {
					StorageMetrics x = wait( waitStorageMetricsMultipleLocations( locations, min, max, permittedError ) );
					return x;
				} else {
					WaitMetricsRequest req( keys, min, max );
					StorageMetrics x = wait( loadBalance( locations[0].second, &StorageServerInterface::waitMetrics, req, TaskDataDistribution ) );
					return x;
				}
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
					TraceEvent(SevError, "waitStorageMetricsError").error(e);
					throw;
				}
				cx->invalidateCache(keys);
				Void _ = wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskDataDistribution));
			}
		}
	}
}

Future< StorageMetrics > Transaction::waitStorageMetrics(
	KeyRange const& keys,
	StorageMetrics const& min,
	StorageMetrics const& max,
	StorageMetrics const& permittedError,
	int shardLimit )
{
	return ::waitStorageMetrics( cx, keys, min, max, permittedError, shardLimit );
}

Future< StorageMetrics > Transaction::getStorageMetrics( KeyRange const& keys, int shardLimit ) {
	StorageMetrics m;
	m.bytes = -1;
	return ::waitStorageMetrics( cx, keys, StorageMetrics(), m, StorageMetrics(), shardLimit );
}

ACTOR Future< Standalone<VectorRef<KeyRef>> > splitStorageMetrics( Database cx, KeyRange keys, StorageMetrics limit, StorageMetrics estimated )
{
	loop {
		state vector< pair<KeyRange, Reference<LocationInfo>> > locations = wait( getKeyRangeLocations( cx, keys, CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT, false, &StorageServerInterface::splitMetrics, TransactionInfo(TaskDataDistribution) ) );
		state StorageMetrics used;
		state Standalone<VectorRef<KeyRef>> results;

		//SOMEDAY: Right now, if there are too many shards we delay and check again later. There may be a better solution to this.
		if(locations.size() == CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT) {
			Void _ = wait(delay(CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY, TaskDataDistribution));
			cx->invalidateCache(keys);
		}
		else {
			results.push_back_deep( results.arena(), keys.begin );
			try {
				//TraceEvent("SplitStorageMetrics").detail("locations", locations.size());

				state int i = 0;
				for(; i<locations.size(); i++) {
					SplitMetricsRequest req( locations[i].first, limit, used, estimated, i == locations.size() - 1 );
					SplitMetricsReply res = wait( loadBalance( locations[i].second, &StorageServerInterface::splitMetrics, req, TaskDataDistribution ) );
					if( res.splits.size() && res.splits[0] <= results.back() ) { // split points are out of order, possibly because of moving data, throw error to retry
						ASSERT_WE_THINK(false);   // FIXME: This seems impossible and doesn't seem to be covered by testing
						throw all_alternatives_failed();
					}
					if( res.splits.size() ) {
						results.append( results.arena(), res.splits.begin(), res.splits.size() );
						results.arena().dependsOn( res.splits.arena() );
					}
					used = res.used;

					//TraceEvent("SplitStorageMetricsResult").detail("used", used.bytes).detail("location", i).detail("size", res.splits.size());
				}

				if( used.allLessOrEqual( limit * CLIENT_KNOBS->STORAGE_METRICS_UNFAIR_SPLIT_LIMIT ) ) {
					results.resize(results.arena(), results.size() - 1);
				}

				results.push_back_deep( results.arena(), keys.end );
				return results;
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
					TraceEvent(SevError, "splitStorageMetricsError").error(e);
					throw;
				}
				cx->invalidateCache( keys );
				Void _ = wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskDataDistribution));
			}
		}
	}
}

Future< Standalone<VectorRef<KeyRef>> > Transaction::splitStorageMetrics( KeyRange const& keys, StorageMetrics const& limit, StorageMetrics const& estimated ) {
	return ::splitStorageMetrics( cx, keys, limit, estimated );
}

void Transaction::checkDeferredError() { cx->checkDeferredError(); }

Reference<TransactionLogInfo> Transaction::createTrLogInfoProbabilistically(const Database &cx) {
	double clientSamplingProbability = std::isinf(cx->clientInfo->get().clientTxnInfoSampleRate) ? CLIENT_KNOBS->CSI_SAMPLING_PROBABILITY : cx->clientInfo->get().clientTxnInfoSampleRate;
	if (((networkOptions.logClientInfo.present() && networkOptions.logClientInfo.get()) || BUGGIFY) && g_random->random01() < clientSamplingProbability)
		return Reference<TransactionLogInfo>(new TransactionLogInfo());
	else
		return Reference<TransactionLogInfo>();
}

void enableClientInfoLogging() {
	ASSERT(networkOptions.logClientInfo.present() == false);
	networkOptions.logClientInfo = true;
	TraceEvent(SevInfo, "ClientInfoLoggingEnabled");
}
