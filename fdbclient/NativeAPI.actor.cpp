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

#include "fdbclient/NativeAPI.actor.h"

#include <algorithm>
#include <iterator>
#include <regex>
#include <unordered_set>
#include <tuple>
#include <utility>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/MultiInterface.h"

#include "fdbclient/Atomic.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MasterProxyInterface.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/LoadBalance.h"
#include "fdbrpc/Net2FileSystem.h"
#include "fdbrpc/simulator.h"
#include "flow/Arena.h"
#include "flow/ActorCollection.h"
#include "flow/DeterministicRandom.h"
#include "flow/Error.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/SystemMonitor.h"
#include "flow/TLSConfig.actor.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/serialize.h"

#include "fdbclient/versions.h"

#ifdef WIN32
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#undef min
#undef max
#else
#include <time.h>
#endif
#include "flow/actorcompiler.h" // This must be the last #include.

extern const char* getSourceVersion();

using std::max;
using std::min;
using std::pair;

namespace {

ACTOR template <class T, class Fun>
Future<T> runAfter(Future<T> in, Fun func) {
	T res = wait(in);
	return func(res);
}

template <class Interface, class Request>
Future<REPLY_TYPE(Request)> loadBalance(
	DatabaseContext* ctx, const Reference<LocationInfo> alternatives, RequestStream<Request> Interface::*channel,
	const Request& request = Request(), TaskPriority taskID = TaskPriority::DefaultPromiseEndpoint,
	bool atMostOnce = false, // if true, throws request_maybe_delivered() instead of retrying automatically
	QueueModel* model = NULL) {
	if (alternatives->hasCaches) {
		return loadBalance(alternatives->locations(), channel, request, taskID, atMostOnce, model);
	}
	return runAfter(loadBalance(alternatives->locations(), channel, request, taskID, atMostOnce, model),
					[ctx](auto res) {
						if (res.cached) {
							ctx->updateCache.trigger();
						}
						return res;
	                });
}
} // namespace

NetworkOptions networkOptions;
TLSConfig tlsConfig(TLSEndpointType::CLIENT);

// The default values, TRACE_DEFAULT_ROLL_SIZE and TRACE_DEFAULT_MAX_LOGS_SIZE are located in Trace.h.
NetworkOptions::NetworkOptions()
	: localAddress(""), clusterFile(""), traceDirectory(Optional<std::string>()),
	  traceRollSize(TRACE_DEFAULT_ROLL_SIZE), traceMaxLogsSize(TRACE_DEFAULT_MAX_LOGS_SIZE), traceLogGroup("default"),
	  traceFormat("xml"), traceClockSource("now"), runLoopProfilingEnabled(false), supportedVersions(new ReferencedObject<Standalone<VectorRef<ClientVersionRef>>>()) {}

static const Key CLIENT_LATENCY_INFO_PREFIX = LiteralStringRef("client_latency/");
static const Key CLIENT_LATENCY_INFO_CTR_PREFIX = LiteralStringRef("client_latency_counter/");

Reference<StorageServerInfo> StorageServerInfo::getInterface( DatabaseContext *cx, StorageServerInterface const& ssi, LocalityData const& locality ) {
	auto it = cx->server_interf.find( ssi.id() );
	if( it != cx->server_interf.end() ) {
		if(it->second->interf.getValue.getEndpoint().token != ssi.getValue.getEndpoint().token) {
			if(it->second->interf.locality == ssi.locality) {
				//FIXME: load balance holds pointers to individual members of the interface, and this assignment will swap out the object they are
				//       pointing to. This is technically correct, but is very unnatural. We may want to refactor load balance to take an AsyncVar<Reference<Interface>>
				//       so that it is notified when the interface changes.
				it->second->interf = ssi;
			} else {
				it->second->notifyContextDestroyed();
				Reference<StorageServerInfo> loc( new StorageServerInfo(cx, ssi, locality) );
				cx->server_interf[ ssi.id() ] = loc.getPtr();
				return loc;
			}
		}

		return Reference<StorageServerInfo>::addRef( it->second );
	}

	Reference<StorageServerInfo> loc( new StorageServerInfo(cx, ssi, locality) );
	cx->server_interf[ ssi.id() ] = loc.getPtr();
	return loc;
}

void StorageServerInfo::notifyContextDestroyed() {
	cx = NULL;
}

StorageServerInfo::~StorageServerInfo() {
	if( cx ) {
		auto it = cx->server_interf.find( interf.id() );
		if( it != cx->server_interf.end() )
			cx->server_interf.erase( it );
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

void DatabaseContext::validateVersion(Version version) {
	// Version could be 0 if the INITIALIZE_NEW_DATABASE option is set. In that case, it is illegal to perform any
	// reads. We throw client_invalid_operation because the caller didn't directly set the version, so the
	// version_invalid error might be confusing.
	if (version == 0) {
		throw client_invalid_operation();
	}
	if (switchable && version < minAcceptableReadVersion) {
		TEST(true); // Attempted to read a version lower than any this client has seen from the current cluster
		throw transaction_too_old();
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
	state double lastLogged = 0;
	loop {
		wait(delay(CLIENT_KNOBS->SYSTEM_MONITOR_INTERVAL, TaskPriority::FlushTrace));
		TraceEvent ev("TransactionMetrics", cx->dbId);

		ev.detail("Elapsed", (lastLogged == 0) ? 0 : now() - lastLogged)
			.detail("Cluster", cx->getConnectionFile() ? cx->getConnectionFile()->getConnectionString().clusterKeyName().toString() : "")
			.detail("Internal", cx->internal);

		cx->cc.logToTraceEvent(ev);

		ev.detail("MeanLatency", cx->latencies.mean())
			.detail("MedianLatency", cx->latencies.median())
			.detail("Latency90", cx->latencies.percentile(0.90))
			.detail("Latency98", cx->latencies.percentile(0.98))
			.detail("MaxLatency", cx->latencies.max())
			.detail("MeanRowReadLatency", cx->readLatencies.mean())
			.detail("MedianRowReadLatency", cx->readLatencies.median())
			.detail("MaxRowReadLatency", cx->readLatencies.max())
			.detail("MeanGRVLatency", cx->GRVLatencies.mean())
			.detail("MedianGRVLatency", cx->GRVLatencies.median())
			.detail("MaxGRVLatency", cx->GRVLatencies.max())
			.detail("MeanCommitLatency", cx->commitLatencies.mean())
			.detail("MedianCommitLatency", cx->commitLatencies.median())
			.detail("MaxCommitLatency", cx->commitLatencies.max())
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

		lastLogged = now();
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
			wait(tr->commit());
			return Void();
		}
		catch (Error& e) {
			retryCount++;
			if (retryCount == 10)
				throw;
			wait(tr->onError(e));
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
			GetRangeLimits limit(GetRangeLimits::ROW_LIMIT_UNLIMITED, getRangeByteLimit);
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
				wait(tr->commit());
			}
			if (txInfoSize - numBytesToDel <= clientTxInfoSizeLimit)
				return Void();
		}
		catch (Error& e) {
			wait(tr->onError(e));
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
			for (auto &entry : cx->clientStatusUpdater.outStatusQ) {
				auto &bw = entry.second;
				int64_t value_size_limit = BUGGIFY ? deterministicRandom()->randomInt(1e3, CLIENT_KNOBS->VALUE_SIZE_LIMIT) : CLIENT_KNOBS->VALUE_SIZE_LIMIT;
				int num_chunks = (bw.getLength() + value_size_limit - 1) / value_size_limit;
				std::string random_id = deterministicRandom()->randomAlphaNumeric(16);
				std::string user_provided_id = entry.first.size() ? entry.first + "/" : "";
				for (int i = 0; i < num_chunks; i++) {
					TrInfoChunk chunk;
					BinaryWriter chunkBW(Unversioned());
					chunkBW << bigEndian32(i+1) << bigEndian32(num_chunks);
					chunk.key = KeyRef(clientLatencyName + std::string(10, '\x00') + "/" + random_id + "/" + chunkBW.toValue().toString() + "/" + user_provided_id + std::string(4, '\x00'));
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
			state int64_t dataSizeLimit = BUGGIFY ? deterministicRandom()->randomInt(200e3, 1.5 * CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT) : 0.8 * CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT;
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
							wait(transactionInfoCommitActor(&tr, &commitQ));
							tracking_iter = iter;
							commitQ.clear();
							txBytes = 0;
						}
						commitQ.push_back(*iter);
						txBytes += iter->value.size() + iter->key.size();
						++iter;
					}
					if (!commitQ.empty()) {
						wait(transactionInfoCommitActor(&tr, &commitQ));
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
			if (!trChunksQ.empty() && deterministicRandom()->random01() < clientSamplingProbability)
				wait(delExcessClntTxnEntriesActor(&tr, clientTxnInfoSizeLimit));

			// tr is destructed because it hold a reference to DatabaseContext which creates a cycle mentioned above.
			// Hence destroy the transacation before sleeping to give a chance for the actor to be cleanedup if the Database is destroyed by the user.
			tr = Transaction();
			wait(delay(CLIENT_KNOBS->CSI_STATUS_DELAY));
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
			wait(delay(10.0));
		}
	}
}

ACTOR static Future<Void> monitorMasterProxiesChange(Reference<AsyncVar<ClientDBInfo>> clientDBInfo, AsyncTrigger *triggerVar) {
	state vector< MasterProxyInterface > curProxies;
	curProxies = clientDBInfo->get().proxies;

	loop{
		wait(clientDBInfo->onChange());
		if (clientDBInfo->get().proxies != curProxies) {
			curProxies = clientDBInfo->get().proxies;
			triggerVar->trigger();
		}
	}
}

void updateLocationCacheWithCaches(DatabaseContext* self, const std::map<UID, StorageServerInterface>& removed,
								   const std::map<UID, StorageServerInterface>& added) {
	// TODO: this needs to be more clever in the future
	auto ranges = self->locationCache.ranges();
	for (auto iter = ranges.begin(); iter != ranges.end(); ++iter) {
		if (iter->value() && iter->value()->hasCaches) {
			auto& val = iter->value();
			std::vector<Reference<ReferencedInterface<StorageServerInterface>>> interfaces;
			interfaces.reserve(val->size() - removed.size() + added.size());
			for (int i = 0; i < val->size(); ++i) {
				const auto& interf = (*val)[i];
				if (removed.count(interf->interf.id()) == 0) {
					interfaces.emplace_back(interf);
				}
			}
			for (const auto& p : added) {
				interfaces.emplace_back(Reference<ReferencedInterface<StorageServerInterface>>{new ReferencedInterface<StorageServerInterface>{p.second}});
			}
			iter->value() = Reference<LocationInfo>{ new LocationInfo(interfaces, true) };
		}
	}
}

Reference<LocationInfo> addCaches(const Reference<LocationInfo>& loc,
								  const std::vector<Reference<ReferencedInterface<StorageServerInterface>>>& other) {
	std::vector<Reference<ReferencedInterface<StorageServerInterface>>> interfaces;
	interfaces.reserve(loc->size() + other.size());
	for (int i = 0; i < loc->size(); ++i) {
		interfaces.emplace_back((*loc)[i]);
	}
	interfaces.insert(interfaces.end(), other.begin(), other.end());
	return Reference<LocationInfo>{ new LocationInfo{ interfaces, true } };
}

ACTOR Future<Void> updateCachedRanges(DatabaseContext* self, std::map<UID, StorageServerInterface>* cacheServers) {
	state Database db(self);
	state ReadYourWritesTransaction tr(db);
	state Value trueValue = storageCacheValue(std::vector<uint16_t>{ 0 });
	state Value falseValue = storageCacheValue(std::vector<uint16_t>{});
	try {
		loop {
			wait(self->updateCache.onTrigger());
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			try {
				Standalone<RangeResultRef> range = wait(tr.getRange(storageCacheKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!range.more);
				std::vector<Reference<ReferencedInterface<StorageServerInterface>>> cacheInterfaces;
				cacheInterfaces.reserve(cacheServers->size());
				for (const auto& p : *cacheServers) {
					cacheInterfaces.emplace_back(Reference<ReferencedInterface<StorageServerInterface>>{
					    new ReferencedInterface<StorageServerInterface>{ p.second } });
				}
				bool currCached = false;
				KeyRef begin, end;
				for (const auto& kv : range) {
					// These booleans have to flip consistently
					ASSERT(currCached == (kv.value == falseValue));
					if (kv.value == trueValue) {
						begin = kv.key.substr(storageCacheKeys.begin.size());
						currCached = true;
					} else {
						currCached = false;
						end = kv.key.substr(storageCacheKeys.begin.size());
						KeyRangeRef cachedRange{begin, end};
						auto ranges = self->locationCache.containedRanges(cachedRange);
						KeyRef containedRangesBegin, containedRangesEnd, prevKey;
						if (!ranges.empty()) {
							containedRangesBegin = ranges.begin().range().begin;
						}
						for (auto iter = ranges.begin(); iter != ranges.end(); ++iter) {
							// We probably don't want to do the code below? Otherwise we would never
							// fetch the corresponding storages - which would give us a different semantics
							//if (containedRangesEnd > iter->range().begin) {
							//	self->locationCache.insert(
							//	    KeyRangeRef{ containedRangesEnd, iter->range().begin },
							//	    Reference<LocationInfo>{ new LocationInfo{ cacheInterfaces, true } });
							//}
							containedRangesEnd = iter->range().end;
							if (iter->value() && !iter->value()->hasCaches) {
								iter->value() = addCaches(iter->value(), cacheInterfaces);
							}
						}
						auto iter = self->locationCache.rangeContaining(begin);
						if (iter->value() && !iter->value()->hasCaches) {
							if (end>=iter->range().end) {
								self->locationCache.insert(KeyRangeRef{ begin, iter->range().end },
														   addCaches(iter->value(), cacheInterfaces));
							} else {
								self->locationCache.insert(KeyRangeRef{ begin, end },
														   addCaches(iter->value(), cacheInterfaces));
							}
						}
						iter = self->locationCache.rangeContainingKeyBefore(end);
						if (iter->value() && !iter->value()->hasCaches) {
							self->locationCache.insert(KeyRangeRef{iter->range().begin, end}, addCaches(iter->value(), cacheInterfaces));
						}
					}
				}
				wait(delay(2.0)); // we want to wait at least some small amount of time before
				// updating this list again
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "UpdateCachedRangesFailed")
			.error(e);
		throw;
	}
}

ACTOR Future<Void> monitorCacheList(DatabaseContext* self) {
	state Database db(self);
	state Transaction tr(db);
	state std::map<UID, StorageServerInterface> cacheServerMap;
	state Future<Void> updateRanges = updateCachedRanges(self, &cacheServerMap);
	// if no caches are configured, we don't want to run this actor at all
	// so we just wait for the first trigger from a storage server
	wait(self->updateCache.onTrigger());
	try {
		loop {
			tr.reset();
			try {
				Standalone<RangeResultRef> cacheList =
				    wait(tr.getRange(storageCacheServerKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!cacheList.more);
				bool hasChanges = false;
				std::map<UID, StorageServerInterface> allCacheServers;
				for (auto kv : cacheList) {
					auto ssi = BinaryReader::fromStringRef<StorageServerInterface>(kv.value, IncludeVersion());
					allCacheServers.emplace(ssi.id(), ssi);
				}
				std::map<UID, StorageServerInterface> newCacheServers;
				std::map<UID, StorageServerInterface> deletedCacheServers;
				std::set_difference(allCacheServers.begin(), allCacheServers.end(), cacheServerMap.begin(),
									cacheServerMap.end(),
									std::insert_iterator<std::map<UID, StorageServerInterface>>(
										newCacheServers, newCacheServers.begin()));
				std::set_difference(cacheServerMap.begin(), cacheServerMap.end(), allCacheServers.begin(),
									allCacheServers.end(),
									std::insert_iterator<std::map<UID, StorageServerInterface>>(
										deletedCacheServers, deletedCacheServers.begin()));
				hasChanges = !(newCacheServers.empty() && deletedCacheServers.empty());
				if (hasChanges) {
					updateLocationCacheWithCaches(self, deletedCacheServers, newCacheServers);
				}
				cacheServerMap = std::move(allCacheServers);
				wait(delay(5.0));
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "MonitorCacheListFailed").error(e);
		throw;
	}
}

ACTOR static Future<HealthMetrics> getHealthMetricsActor(DatabaseContext *cx, bool detailed) {
	if (now() - cx->healthMetricsLastUpdated < CLIENT_KNOBS->AGGREGATE_HEALTH_METRICS_MAX_STALENESS) {
		if (detailed) {
			return cx->healthMetrics;
		}
		else {
			HealthMetrics result;
			result.update(cx->healthMetrics, false, false);
			return result;
		}
	}
	state bool sendDetailedRequest = detailed && now() - cx->detailedHealthMetricsLastUpdated >
		CLIENT_KNOBS->DETAILED_HEALTH_METRICS_MAX_STALENESS;
	loop {
		choose {
			when(wait(cx->onMasterProxiesChanged())) {}
			when(GetHealthMetricsReply rep =
				 wait(basicLoadBalance(cx->getMasterProxies(false), &MasterProxyInterface::getHealthMetrics,
							 GetHealthMetricsRequest(sendDetailedRequest)))) {
				cx->healthMetrics.update(rep.healthMetrics, detailed, true);
				if (detailed) {
					cx->healthMetricsLastUpdated = now();
					cx->detailedHealthMetricsLastUpdated = now();
					return cx->healthMetrics;
				}
				else {
					cx->healthMetricsLastUpdated = now();
					HealthMetrics result;
					result.update(cx->healthMetrics, false, false);
					return result;
				}
			}
		}
	}
}

Future<HealthMetrics> DatabaseContext::getHealthMetrics(bool detailed = false) {
	return getHealthMetricsActor(this, detailed);
}

void DatabaseContext::registerSpecialKeySpaceModule(SpecialKeySpace::MODULE module, std::unique_ptr<SpecialKeyRangeBaseImpl> impl) {
	specialKeySpace->registerKeyRange(module, impl->getKeyRange(), impl.get());
	specialKeySpaceModules.push_back(std::move(impl));
}

ACTOR Future<Standalone<RangeResultRef>> getWorkerInterfaces(Reference<ClusterConnectionFile> clusterFile);
ACTOR Future<Optional<Value>> getJSON(Database db);

struct WorkerInterfacesSpecialKeyImpl : SpecialKeyRangeBaseImpl {
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override {
		if (ryw->getDatabase().getPtr() && ryw->getDatabase()->getConnectionFile()) {
			Key prefix = Key(getKeyRange().begin);
			return map(getWorkerInterfaces(ryw->getDatabase()->getConnectionFile()),
			           [prefix = prefix, kr = KeyRange(kr)](const Standalone<RangeResultRef>& in) {
				           Standalone<RangeResultRef> result;
				           for (const auto& [k_, v] : in) {
					           auto k = k_.withPrefix(prefix);
					           if (kr.contains(k)) result.push_back_deep(result.arena(), KeyValueRef(k, v));
				           }

				           std::sort(result.begin(), result.end(), KeyValueRef::OrderByKey{});
				           return result;
			           });
		} else {
			return Standalone<RangeResultRef>();
		}
	}

	explicit WorkerInterfacesSpecialKeyImpl(KeyRangeRef kr) : SpecialKeyRangeBaseImpl(kr) {}
};

struct SingleSpecialKeyImpl : SpecialKeyRangeBaseImpl {
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override {
		ASSERT(kr.contains(k));
		return map(f(ryw), [k = k](Optional<Value> v) {
			Standalone<RangeResultRef> result;
			if (v.present()) {
				result.push_back_deep(result.arena(), KeyValueRef(k, v.get()));
			}
			return result;
		});
	}

	SingleSpecialKeyImpl(KeyRef k, const std::function<Future<Optional<Value>>(ReadYourWritesTransaction*)>& f)
	  : SpecialKeyRangeBaseImpl(singleKeyRange(k)), k(k), f(f) {}

private:
	Key k;
	std::function<Future<Optional<Value>>(ReadYourWritesTransaction*)> f;
};

DatabaseContext::DatabaseContext(Reference<AsyncVar<Reference<ClusterConnectionFile>>> connectionFile,
                                 Reference<AsyncVar<ClientDBInfo>> clientInfo, Future<Void> clientInfoMonitor,
                                 TaskPriority taskID, LocalityData const& clientLocality,
                                 bool enableLocalityLoadBalance, bool lockAware, bool internal, int apiVersion,
                                 bool switchable)
  : connectionFile(connectionFile), clientInfo(clientInfo), clientInfoMonitor(clientInfoMonitor), taskID(taskID),
    clientLocality(clientLocality), enableLocalityLoadBalance(enableLocalityLoadBalance), lockAware(lockAware),
    apiVersion(apiVersion), switchable(switchable), provisional(false), cc("TransactionMetrics"),
    transactionReadVersions("ReadVersions", cc), 
    transactionReadVersionsThrottled("ReadVersionsThrottled", cc),
    transactionReadVersionsCompleted("ReadVersionsCompleted", cc),
    transactionReadVersionBatches("ReadVersionBatches", cc),
    transactionBatchReadVersions("BatchPriorityReadVersions", cc),
    transactionDefaultReadVersions("DefaultPriorityReadVersions", cc),
    transactionImmediateReadVersions("ImmediatePriorityReadVersions", cc),
    transactionBatchReadVersionsCompleted("BatchPriorityReadVersionsCompleted", cc),
    transactionDefaultReadVersionsCompleted("DefaultPriorityReadVersionsCompleted", cc),
    transactionImmediateReadVersionsCompleted("ImmediatePriorityReadVersionsCompleted", cc),
    transactionLogicalReads("LogicalUncachedReads", cc), transactionPhysicalReads("PhysicalReadRequests", cc),
    transactionPhysicalReadsCompleted("PhysicalReadRequestsCompleted", cc),
    transactionGetKeyRequests("GetKeyRequests", cc), transactionGetValueRequests("GetValueRequests", cc),
    transactionGetRangeRequests("GetRangeRequests", cc), transactionWatchRequests("WatchRequests", cc),
    transactionGetAddressesForKeyRequests("GetAddressesForKeyRequests", cc), transactionBytesRead("BytesRead", cc),
    transactionKeysRead("KeysRead", cc), transactionMetadataVersionReads("MetadataVersionReads", cc),
    transactionCommittedMutations("CommittedMutations", cc),
    transactionCommittedMutationBytes("CommittedMutationBytes", cc), transactionSetMutations("SetMutations", cc),
    transactionClearMutations("ClearMutations", cc), transactionAtomicMutations("AtomicMutations", cc),
    transactionsCommitStarted("CommitStarted", cc), transactionsCommitCompleted("CommitCompleted", cc),
    transactionKeyServerLocationRequests("KeyServerLocationRequests", cc),
    transactionKeyServerLocationRequestsCompleted("KeyServerLocationRequestsCompleted", cc),
    transactionsTooOld("TooOld", cc), transactionsFutureVersions("FutureVersions", cc),
    transactionsNotCommitted("NotCommitted", cc), transactionsMaybeCommitted("MaybeCommitted", cc),
    transactionsResourceConstrained("ResourceConstrained", cc), transactionsThrottled("Throttled", cc),
    transactionsProcessBehind("ProcessBehind", cc), outstandingWatches(0), latencies(1000), readLatencies(1000),
    commitLatencies(1000), GRVLatencies(1000), mutationsPerCommit(1000), bytesPerCommit(1000), mvCacheInsertLocation(0),
    healthMetricsLastUpdated(0), detailedHealthMetricsLastUpdated(0), internal(internal),
    specialKeySpace(std::make_unique<SpecialKeySpace>(specialKeys.begin, specialKeys.end, /* test */ false)) {
	dbId = deterministicRandom()->randomUniqueID();
	connected = clientInfo->get().proxies.size() ? Void() : clientInfo->onChange();

	metadataVersionCache.resize(CLIENT_KNOBS->METADATA_VERSION_CACHE_SIZE);
	maxOutstandingWatches = CLIENT_KNOBS->DEFAULT_MAX_OUTSTANDING_WATCHES;

	snapshotRywEnabled = apiVersionAtLeast(300) ? 1 : 0;

	logger = databaseLogger( this );
	locationCacheSize = g_network->isSimulated() ?
			CLIENT_KNOBS->LOCATION_CACHE_EVICTION_SIZE_SIM :
			CLIENT_KNOBS->LOCATION_CACHE_EVICTION_SIZE;

	getValueSubmitted.init(LiteralStringRef("NativeAPI.GetValueSubmitted"));
	getValueCompleted.init(LiteralStringRef("NativeAPI.GetValueCompleted"));

	monitorMasterProxiesInfoChange = monitorMasterProxiesChange(clientInfo, &masterProxiesChangeTrigger);
	clientStatusUpdater.actor = clientStatusUpdateActor(this);
	cacheListMonitor = monitorCacheList(this);
	if (apiVersionAtLeast(630)) {
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::TRANSACTION, std::make_unique<ConflictingKeysImpl>(conflictingKeysRange));
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::TRANSACTION, std::make_unique<ReadConflictRangeImpl>(readConflictRangeKeysRange));
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::TRANSACTION, std::make_unique<WriteConflictRangeImpl>(writeConflictRangeKeysRange));
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::METRICS,
		                              std::make_unique<DDStatsRangeImpl>(ddStatsRange));
		registerSpecialKeySpaceModule(SpecialKeySpace::MODULE::WORKERINTERFACE, std::make_unique<WorkerInterfacesSpecialKeyImpl>(KeyRangeRef(
		    LiteralStringRef("\xff\xff/worker_interfaces/"), LiteralStringRef("\xff\xff/worker_interfaces0"))));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::STATUSJSON,
		    std::make_unique<SingleSpecialKeyImpl>(LiteralStringRef("\xff\xff/status/json"),
		                                           [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			                                           if (ryw->getDatabase().getPtr() &&
			                                               ryw->getDatabase()->getConnectionFile()) {
				                                           return getJSON(ryw->getDatabase());
			                                           } else {
				                                           return Optional<Value>();
			                                           }
		                                           }));
		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::CLUSTERFILEPATH,
		    std::make_unique<SingleSpecialKeyImpl>(
		        LiteralStringRef("\xff\xff/cluster_file_path"),
		        [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			        try {
				        if (ryw->getDatabase().getPtr() && ryw->getDatabase()->getConnectionFile()) {
					        Optional<Value> output = StringRef(ryw->getDatabase()->getConnectionFile()->getFilename());
					        return output;
				        }
			        } catch (Error& e) {
				        return e;
			        }
			        return Optional<Value>();
		        }));

		registerSpecialKeySpaceModule(
		    SpecialKeySpace::MODULE::CONNECTIONSTRING,
		    std::make_unique<SingleSpecialKeyImpl>(
		        LiteralStringRef("\xff\xff/connection_string"),
		        [](ReadYourWritesTransaction* ryw) -> Future<Optional<Value>> {
			        try {
				        if (ryw->getDatabase().getPtr() && ryw->getDatabase()->getConnectionFile()) {
					        Reference<ClusterConnectionFile> f = ryw->getDatabase()->getConnectionFile();
					        Optional<Value> output = StringRef(f->getConnectionString().toString());
					        return output;
				        }
			        } catch (Error& e) {
				        return e;
			        }
			        return Optional<Value>();
		        }));
	}
	throttleExpirer = recurring([this](){ expireThrottles(); }, CLIENT_KNOBS->TAG_THROTTLE_EXPIRATION_INTERVAL);

	if(BUGGIFY) {
		DatabaseContext::debugUseTags = true;
	}
}

DatabaseContext::DatabaseContext( const Error &err ) : deferredError(err), cc("TransactionMetrics"), transactionReadVersions("ReadVersions", cc), transactionReadVersionsThrottled("ReadVersionsThrottled", cc),
	transactionReadVersionsCompleted("ReadVersionsCompleted", cc), transactionReadVersionBatches("ReadVersionBatches", cc), transactionBatchReadVersions("BatchPriorityReadVersions", cc), 
	transactionDefaultReadVersions("DefaultPriorityReadVersions", cc), transactionImmediateReadVersions("ImmediatePriorityReadVersions", cc), 
	transactionBatchReadVersionsCompleted("BatchPriorityReadVersionsCompleted", cc), transactionDefaultReadVersionsCompleted("DefaultPriorityReadVersionsCompleted", cc), 
	transactionImmediateReadVersionsCompleted("ImmediatePriorityReadVersionsCompleted", cc), transactionLogicalReads("LogicalUncachedReads", cc), transactionPhysicalReads("PhysicalReadRequests", cc), 
	transactionPhysicalReadsCompleted("PhysicalReadRequestsCompleted", cc), transactionGetKeyRequests("GetKeyRequests", cc), transactionGetValueRequests("GetValueRequests", cc), 
	transactionGetRangeRequests("GetRangeRequests", cc), transactionWatchRequests("WatchRequests", cc), transactionGetAddressesForKeyRequests("GetAddressesForKeyRequests", cc), 
	transactionBytesRead("BytesRead", cc), transactionKeysRead("KeysRead", cc), transactionMetadataVersionReads("MetadataVersionReads", cc), transactionCommittedMutations("CommittedMutations", cc), 
	transactionCommittedMutationBytes("CommittedMutationBytes", cc), transactionSetMutations("SetMutations", cc), transactionClearMutations("ClearMutations", cc), 
	transactionAtomicMutations("AtomicMutations", cc), transactionsCommitStarted("CommitStarted", cc), transactionsCommitCompleted("CommitCompleted", cc), 
	transactionKeyServerLocationRequests("KeyServerLocationRequests", cc), transactionKeyServerLocationRequestsCompleted("KeyServerLocationRequestsCompleted", cc), transactionsTooOld("TooOld", cc), 
	transactionsFutureVersions("FutureVersions", cc), transactionsNotCommitted("NotCommitted", cc), transactionsMaybeCommitted("MaybeCommitted", cc), 
	transactionsResourceConstrained("ResourceConstrained", cc), transactionsThrottled("Throttled", cc), transactionsProcessBehind("ProcessBehind", cc), latencies(1000), readLatencies(1000), commitLatencies(1000),
	GRVLatencies(1000), mutationsPerCommit(1000), bytesPerCommit(1000), 
	internal(false) {}


Database DatabaseContext::create(Reference<AsyncVar<ClientDBInfo>> clientInfo, Future<Void> clientInfoMonitor, LocalityData clientLocality, bool enableLocalityLoadBalance, TaskPriority taskID, bool lockAware, int apiVersion, bool switchable) {
	return Database( new DatabaseContext( Reference<AsyncVar<Reference<ClusterConnectionFile>>>(), clientInfo, clientInfoMonitor, taskID, clientLocality, enableLocalityLoadBalance, lockAware, true, apiVersion, switchable ) );
}

DatabaseContext::~DatabaseContext() {
	cacheListMonitor.cancel();
	monitorMasterProxiesInfoChange.cancel();
	for(auto it = server_interf.begin(); it != server_interf.end(); it = server_interf.erase(it))
		it->second->notifyContextDestroyed();
	ASSERT_ABORT( server_interf.empty() );
	locationCache.insert(allKeys, Reference<LocationInfo>());
}

pair<KeyRange, Reference<LocationInfo>> DatabaseContext::getCachedLocation( const KeyRef& key, bool isBackward ) {
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

	auto begin = locationCache.rangeContaining(range.begin);
	auto end = locationCache.rangeContainingKeyBefore(range.end);

	loop {
		auto r = reverse ? end : begin;
		if (!r->value()){
			TEST(result.size()); // had some but not all cached locations
			result.clear();
			return false;
		}
		result.emplace_back(r->range() & range, r->value());
		if (result.size() == limit || begin == end) {
			break;
		}

		if(reverse)
			--end;
		else
			++begin;
	}

	return true;
}

Reference<LocationInfo> DatabaseContext::setCachedLocation( const KeyRangeRef& keys, const vector<StorageServerInterface>& servers ) {
	vector<Reference<ReferencedInterface<StorageServerInterface>>> serverRefs;
	serverRefs.reserve(servers.size());
	for(auto& interf : servers) {
		serverRefs.push_back( StorageServerInfo::getInterface( this, interf, clientLocality ) );
	}

	int maxEvictionAttempts = 100, attempts = 0;
	Reference<LocationInfo> loc = Reference<LocationInfo>( new LocationInfo(serverRefs) );
	while( locationCache.size() > locationCacheSize && attempts < maxEvictionAttempts) {
		TEST( true ); // NativeAPI storage server locationCache entry evicted
		attempts++;
		auto r = locationCache.randomRange();
		Key begin = r.begin(), end = r.end();  // insert invalidates r, so can't be passed a mere reference into it
		locationCache.insert(KeyRangeRef(begin, end), Reference<LocationInfo>());
	}
	locationCache.insert( keys, loc );
	return loc;
}

void DatabaseContext::invalidateCache( const KeyRef& key, bool isBackward ) {
	if( isBackward ) {
		locationCache.rangeContainingKeyBefore(key)->value() = Reference<LocationInfo>();
	} else {
		locationCache.rangeContaining(key)->value() = Reference<LocationInfo>();
	}
}

void DatabaseContext::invalidateCache( const KeyRangeRef& keys ) {
	auto rs = locationCache.intersectingRanges(keys);
	Key begin = rs.begin().begin(), end = rs.end().begin();  // insert invalidates rs, so can't be passed a mere reference into it
	locationCache.insert(KeyRangeRef(begin, end), Reference<LocationInfo>());
}

Future<Void> DatabaseContext::onMasterProxiesChanged() {
	return this->masterProxiesChangeTrigger.onTrigger();
}

bool DatabaseContext::sampleReadTags() {
	return clientInfo->get().transactionTagSampleRate > 0 && deterministicRandom()->random01() <= clientInfo->get().transactionTagSampleRate;
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
	int defaultFor = FDBDatabaseOptions::optionInfo.getMustExist(option).defaultFor;
	if (defaultFor >= 0) {
		ASSERT(FDBTransactionOptions::optionInfo.find((FDBTransactionOptions::Option)defaultFor) !=
		       FDBTransactionOptions::optionInfo.end());
		transactionDefaults.addOption((FDBTransactionOptions::Option)defaultFor, value.castTo<Standalone<StringRef>>());
	}
	else {
		switch(option) {
			case FDBDatabaseOptions::LOCATION_CACHE_SIZE:
				locationCacheSize = (int)extractIntOption(value, 0, std::numeric_limits<int>::max());
				break;
			case FDBDatabaseOptions::MACHINE_ID:
				clientLocality = LocalityData( clientLocality.processId(), value.present() ? Standalone<StringRef>(value.get()) : Optional<Standalone<StringRef>>(), clientLocality.machineId(), clientLocality.dcId() );
				if( clientInfo->get().proxies.size() )
					masterProxies = Reference<ProxyInfo>( new ProxyInfo( clientInfo->get().proxies ) );
				server_interf.clear();
				locationCache.insert( allKeys, Reference<LocationInfo>() );
				break;
			case FDBDatabaseOptions::MAX_WATCHES:
				maxOutstandingWatches = (int)extractIntOption(value, 0, CLIENT_KNOBS->ABSOLUTE_MAX_WATCHES);
				break;
			case FDBDatabaseOptions::DATACENTER_ID:
				clientLocality = LocalityData(clientLocality.processId(), clientLocality.zoneId(), clientLocality.machineId(), value.present() ? Standalone<StringRef>(value.get()) : Optional<Standalone<StringRef>>());
				if( clientInfo->get().proxies.size() )
					masterProxies = Reference<ProxyInfo>( new ProxyInfo( clientInfo->get().proxies ));
				server_interf.clear();
				locationCache.insert( allKeys, Reference<LocationInfo>() );
				break;
			case FDBDatabaseOptions::SNAPSHOT_RYW_ENABLE:
				validateOptionValue(value, false);
				snapshotRywEnabled++;
				break;
			case FDBDatabaseOptions::SNAPSHOT_RYW_DISABLE:
				validateOptionValue(value, false);
				snapshotRywEnabled--;
				break;
			default:
				break;
		}
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

Future<Void> DatabaseContext::onConnected() {
	return connected;
}

ACTOR static Future<Void> switchConnectionFileImpl(Reference<ClusterConnectionFile> connFile, DatabaseContext* self) {
	TEST(true); // Switch connection file
	TraceEvent("SwitchConnectionFile")
	    .detail("ConnectionFile", connFile->canGetFilename() ? connFile->getFilename() : "")
	    .detail("ConnectionString", connFile->getConnectionString().toString());

	// Reset state from former cluster.
	self->masterProxies.clear();
	self->minAcceptableReadVersion = std::numeric_limits<Version>::max();
	self->invalidateCache(allKeys);

	auto clearedClientInfo = self->clientInfo->get();
	clearedClientInfo.proxies.clear();
	clearedClientInfo.id = deterministicRandom()->randomUniqueID();
	self->clientInfo->set(clearedClientInfo);
	self->connectionFile->set(connFile);

	state Database db(Reference<DatabaseContext>::addRef(self));
	state Transaction tr(db);
	loop {
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		try {
			TraceEvent("SwitchConnectionFileAttemptingGRV");
			Version v = wait(tr.getReadVersion());
			TraceEvent("SwitchConnectionFileGotRV")
			    .detail("ReadVersion", v)
			    .detail("MinAcceptableReadVersion", self->minAcceptableReadVersion);
			ASSERT(self->minAcceptableReadVersion != std::numeric_limits<Version>::max());
			self->connectionFileChangedTrigger.trigger();
			return Void();
		} catch (Error& e) {
			TraceEvent("SwitchConnectionFileError").detail("Error", e.what());
			wait(tr.onError(e));
		}
	}
}

Reference<ClusterConnectionFile> DatabaseContext::getConnectionFile() {
	if(connectionFile) {
		return connectionFile->get();
	}
	return Reference<ClusterConnectionFile>();
}

Future<Void> DatabaseContext::switchConnectionFile(Reference<ClusterConnectionFile> standby) {
	ASSERT(switchable);
	return switchConnectionFileImpl(standby, this);
}

Future<Void> DatabaseContext::connectionFileChanged() {
	return connectionFileChangedTrigger.onTrigger();
}

void DatabaseContext::expireThrottles() {
	for(auto &priorityItr : throttledTags) {
		for(auto tagItr = priorityItr.second.begin(); tagItr != priorityItr.second.end();) {
			if(tagItr->second.expired()) {
				TEST(true); // Expiring client throttle
				tagItr = priorityItr.second.erase(tagItr);
			}
			else {
				++tagItr;
			}
		}
	}
}

extern IPAddress determinePublicIPAutomatically(ClusterConnectionString const& ccs);

Database Database::createDatabase( Reference<ClusterConnectionFile> connFile, int apiVersion, bool internal, LocalityData const& clientLocality, DatabaseContext *preallocatedDb ) {
	if(!g_network)
		throw network_not_setup();

	if(connFile) {
		if(networkOptions.traceDirectory.present() && !traceFileIsOpen()) {
			g_network->initMetrics();
			FlowTransport::transport().initMetrics();
			initTraceEventMetrics();

			auto publicIP = determinePublicIPAutomatically( connFile->getConnectionString() );
			selectTraceFormatter(networkOptions.traceFormat);
			selectTraceClockSource(networkOptions.traceClockSource);
			openTraceFile(NetworkAddress(publicIP, ::getpid()), networkOptions.traceRollSize,
			              networkOptions.traceMaxLogsSize, networkOptions.traceDirectory.get(), "trace",
			              networkOptions.traceLogGroup, networkOptions.traceFileIdentifier);

			TraceEvent("ClientStart")
				.detail("SourceVersion", getSourceVersion())
				.detail("Version", FDB_VT_VERSION)
				.detail("PackageName", FDB_VT_PACKAGE_NAME)
				.detail("ClusterFile", connFile->getFilename().c_str())
				.detail("ConnectionString", connFile->getConnectionString().toString())
				.detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(NULL))
				.detail("ApiVersion", apiVersion)
				.detailf("ImageOffset", "%p", platform::getImageOffset())
				.trackLatest("ClientStart");

			initializeSystemMonitorMachineState(SystemMonitorMachineState(IPAddress(publicIP)));

			systemMonitor();
			uncancellable( recurring( &systemMonitor, CLIENT_KNOBS->SYSTEM_MONITOR_INTERVAL, TaskPriority::FlushTrace ) );
		}
	}

	g_network->initTLS();

	Reference<AsyncVar<ClientDBInfo>> clientInfo(new AsyncVar<ClientDBInfo>());
	Reference<AsyncVar<Reference<ClusterConnectionFile>>> connectionFile(new AsyncVar<Reference<ClusterConnectionFile>>());
	connectionFile->set(connFile);
	Future<Void> clientInfoMonitor = monitorProxies(connectionFile, clientInfo, networkOptions.supportedVersions, StringRef(networkOptions.traceLogGroup));

	DatabaseContext *db;
	if(preallocatedDb) {
		db = new (preallocatedDb) DatabaseContext(connectionFile, clientInfo, clientInfoMonitor, TaskPriority::DefaultEndpoint, clientLocality, true, false, internal, apiVersion, /*switchable*/ true);
	}
	else {
		db = new DatabaseContext(connectionFile, clientInfo, clientInfoMonitor, TaskPriority::DefaultEndpoint, clientLocality, true, false, internal, apiVersion, /*switchable*/ true);
	}

	return Database(db);
}

Database Database::createDatabase( std::string connFileName, int apiVersion, bool internal, LocalityData const& clientLocality ) {
	Reference<ClusterConnectionFile> rccf = Reference<ClusterConnectionFile>(new ClusterConnectionFile(ClusterConnectionFile::lookupClusterFileName(connFileName).first));
	return Database::createDatabase(rccf, apiVersion, internal, clientLocality);
}

const UniqueOrderedOptionList<FDBTransactionOptions>& Database::getTransactionDefaults() const {
	ASSERT(db);
	return db->transactionDefaults;
}

void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	std::regex identifierRegex("^[a-zA-Z0-9_]*$");
	switch(option) {
		// SOMEDAY: If the network is already started, should these five throw an error?
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
		case FDBNetworkOptions::TRACE_FORMAT:
			validateOptionValue(value, true);
			networkOptions.traceFormat = value.get().toString();
			if (!validateTraceFormat(networkOptions.traceFormat)) {
				fprintf(stderr, "Unrecognized trace format: `%s'\n", networkOptions.traceFormat.c_str());
				throw invalid_option_value();
			}
			break;
		case FDBNetworkOptions::TRACE_FILE_IDENTIFIER:
			validateOptionValue(value, true);
			networkOptions.traceFileIdentifier = value.get().toString();
			if (networkOptions.traceFileIdentifier.length() > CLIENT_KNOBS->TRACE_LOG_FILE_IDENTIFIER_MAX_LENGTH) {
				fprintf(stderr, "Trace file identifier provided is too long.\n");
				throw invalid_option_value();
			} else if (!std::regex_match(networkOptions.traceFileIdentifier, identifierRegex)) {
				fprintf(stderr, "Trace file identifier should only contain alphanumerics and underscores.\n");
				throw invalid_option_value();
			}
			break;

		case FDBNetworkOptions::TRACE_LOG_GROUP:
			if(value.present()) {
				if (traceFileIsOpen()) {
					setTraceLogGroup(value.get().toString());
				}
				else {
					networkOptions.traceLogGroup = value.get().toString();
				}
			}
			break;
		case FDBNetworkOptions::TRACE_CLOCK_SOURCE:
			validateOptionValue(value, true);
			networkOptions.traceClockSource = value.get().toString();
			if (!validateTraceClockSource(networkOptions.traceClockSource)) {
				fprintf(stderr, "Unrecognized trace clock source: `%s'\n", networkOptions.traceClockSource.c_str());
				throw invalid_option_value();
			}
			break;
		case FDBNetworkOptions::KNOB: {
			validateOptionValue(value, true);

			std::string optionValue = value.get().toString();
			TraceEvent("SetKnob").detail("KnobString", optionValue);

			size_t eq = optionValue.find_first_of('=');
			if(eq == optionValue.npos) {
				TraceEvent(SevWarnAlways, "InvalidKnobString").detail("KnobString", optionValue);
				throw invalid_option_value();
			}

			std::string knobName = optionValue.substr(0, eq);
			std::string knobValue = optionValue.substr(eq+1);
			if (const_cast<FlowKnobs*>(FLOW_KNOBS)->setKnob(knobName, knobValue))
			{
				// update dependent knobs
				const_cast<FlowKnobs*>(FLOW_KNOBS)->initialize();
			}
			else if (const_cast<ClientKnobs*>(CLIENT_KNOBS)->setKnob(knobName, knobValue))
			{
				// update dependent knobs
				const_cast<ClientKnobs*>(CLIENT_KNOBS)->initialize();
			}
			else
			{
				TraceEvent(SevWarnAlways, "UnrecognizedKnob").detail("Knob", knobName.c_str());
				fprintf(stderr, "FoundationDB client ignoring unrecognized knob option '%s'\n", knobName.c_str());
			}
			break;
		}
		case FDBNetworkOptions::TLS_PLUGIN:
			validateOptionValue(value, true);
			break;
		case FDBNetworkOptions::TLS_CERT_PATH:
			validateOptionValue(value, true);
			tlsConfig.setCertificatePath(value.get().toString());
			break;
		case FDBNetworkOptions::TLS_CERT_BYTES: {
			validateOptionValue(value, true);
			tlsConfig.setCertificateBytes(value.get().toString());
			break;
		}
		case FDBNetworkOptions::TLS_CA_PATH: {
			validateOptionValue(value, true);
			tlsConfig.setCAPath(value.get().toString());
			break;
		}
		case FDBNetworkOptions::TLS_CA_BYTES: {
			validateOptionValue(value, true);
			tlsConfig.setCABytes(value.get().toString());
			break;
		}
		case FDBNetworkOptions::TLS_PASSWORD:
			validateOptionValue(value, true);
			tlsConfig.setPassword(value.get().toString());
			break;
		case FDBNetworkOptions::TLS_KEY_PATH:
			validateOptionValue(value, true);
			tlsConfig.setKeyPath(value.get().toString());
			break;
		case FDBNetworkOptions::TLS_KEY_BYTES: {
			validateOptionValue(value, true);
			tlsConfig.setKeyBytes(value.get().toString());
			break;
		}
		case FDBNetworkOptions::TLS_VERIFY_PEERS:
			validateOptionValue(value, true);
			tlsConfig.clearVerifyPeers();
			tlsConfig.addVerifyPeers( value.get().toString() );
			break;
		case FDBNetworkOptions::CLIENT_BUGGIFY_ENABLE:
			enableBuggify(true, BuggifyType::Client);
			break;
		case FDBNetworkOptions::CLIENT_BUGGIFY_DISABLE:
			enableBuggify(false, BuggifyType::Client);
			break;
		case FDBNetworkOptions::CLIENT_BUGGIFY_SECTION_ACTIVATED_PROBABILITY:
			validateOptionValue(value, true);
			clearBuggifySections(BuggifyType::Client);
			P_BUGGIFIED_SECTION_ACTIVATED[int(BuggifyType::Client)] = double(extractIntOption(value, 0, 100))/100.0;
			break;
		case FDBNetworkOptions::CLIENT_BUGGIFY_SECTION_FIRED_PROBABILITY:
			validateOptionValue(value, true);
			P_BUGGIFIED_SECTION_FIRES[int(BuggifyType::Client)] = double(extractIntOption(value, 0, 100))/100.0;
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

			Standalone<VectorRef<ClientVersionRef>> supportedVersions;
			std::vector<StringRef> supportedVersionsStrings = value.get().splitAny(LiteralStringRef(";"));
			for (StringRef versionString: supportedVersionsStrings) {
				supportedVersions.push_back_deep(supportedVersions.arena(), ClientVersionRef(versionString));
			}

			ASSERT(supportedVersions.size() > 0);
			networkOptions.supportedVersions->set(supportedVersions);

			break;
		}
		case FDBNetworkOptions::ENABLE_RUN_LOOP_PROFILING: // Same as ENABLE_SLOW_TASK_PROFILING
			validateOptionValue(value, false);
			networkOptions.runLoopProfilingEnabled = true;
			break;
		default:
			break;
	}
}

void setupNetwork(uint64_t transportId, bool useMetrics) {
	if( g_network )
		throw network_already_setup();

	if (!networkOptions.logClientInfo.present())
		networkOptions.logClientInfo = true;

	TLS::DisableOpenSSLAtExitHandler();
	g_network = newNet2(tlsConfig, false, useMetrics || networkOptions.traceDirectory.present());
	g_network->addStopCallback( Net2FileSystem::stop );
	g_network->addStopCallback( TLS::DestroyOpenSSLGlobalState );
	FlowTransport::createInstance(true, transportId);
	Net2FileSystem::newFileSystem();
}

void runNetwork() {
	if(!g_network) {
		throw network_not_setup();
	}

	if(!g_network->checkRunnable()) {
		throw network_cannot_be_restarted();
	}

	if(networkOptions.traceDirectory.present() && networkOptions.runLoopProfilingEnabled) {
		setupRunLoopProfiler();
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

Reference<ProxyInfo> DatabaseContext::getMasterProxies(bool useProvisionalProxies) {
	if (masterProxiesLastChange != clientInfo->get().id) {
		masterProxiesLastChange = clientInfo->get().id;
		masterProxies.clear();
		if( clientInfo->get().proxies.size() ) {
			masterProxies = Reference<ProxyInfo>( new ProxyInfo( clientInfo->get().proxies ));
			provisional = clientInfo->get().proxies[0].provisional;
		}
	}
	if(provisional && !useProvisionalProxies) {
		return Reference<ProxyInfo>();
	}
	return masterProxies;
}

//Actor which will wait until the MultiInterface<MasterProxyInterface> returned by the DatabaseContext cx is not NULL
ACTOR Future<Reference<ProxyInfo>> getMasterProxiesFuture(DatabaseContext *cx, bool useProvisionalProxies) {
	loop{
		Reference<ProxyInfo> proxies = cx->getMasterProxies(useProvisionalProxies);
		if (proxies)
			return proxies;
		wait( cx->onMasterProxiesChanged() );
	}
}

//Returns a future which will not be set until the ProxyInfo of this DatabaseContext is not NULL
Future<Reference<ProxyInfo>> DatabaseContext::getMasterProxiesFuture(bool useProvisionalProxies) {
	return ::getMasterProxiesFuture(this, useProvisionalProxies);
}

void GetRangeLimits::decrement( VectorRef<KeyValueRef> const& data ) {
	if (rows != GetRangeLimits::ROW_LIMIT_UNLIMITED) {
		ASSERT(data.size() <= rows);
		rows -= data.size();
	}

	minRows = std::max(0, minRows - data.size());

	if (bytes != GetRangeLimits::BYTE_LIMIT_UNLIMITED)
		bytes = std::max( 0, bytes - (int)data.expectedSize() - (8-(int)sizeof(KeyValueRef))*data.size() );
}

void GetRangeLimits::decrement( KeyValueRef const& data ) {
	minRows = std::max(0, minRows - 1);
	if (rows != GetRangeLimits::ROW_LIMIT_UNLIMITED) rows--;
	if (bytes != GetRangeLimits::BYTE_LIMIT_UNLIMITED) bytes = std::max(0, bytes - (int)8 - (int)data.expectedSize());
}

// True if either the row or byte limit has been reached
bool GetRangeLimits::isReached() {
	return rows == 0 || (bytes == 0 && minRows == 0);
}

// True if data would cause the row or byte limit to be reached
bool GetRangeLimits::reachedBy( VectorRef<KeyValueRef> const& data ) {
	return (rows != GetRangeLimits::ROW_LIMIT_UNLIMITED && data.size() >= rows) ||
	       (bytes != GetRangeLimits::BYTE_LIMIT_UNLIMITED &&
	        (int)data.expectedSize() + (8 - (int)sizeof(KeyValueRef)) * data.size() >= bytes && data.size() >= minRows);
}

bool GetRangeLimits::hasByteLimit() {
	return bytes != GetRangeLimits::BYTE_LIMIT_UNLIMITED;
}

bool GetRangeLimits::hasRowLimit() {
	return rows != GetRangeLimits::ROW_LIMIT_UNLIMITED;
}

bool GetRangeLimits::hasSatisfiedMinRows() {
	return hasByteLimit() && minRows == 0;
}

AddressExclusion AddressExclusion::parse( StringRef const& key ) {
	//Must not change: serialized to the database!
	auto parsedIp = IPAddress::parse(key.toString());
	if (parsedIp.present()) {
		return AddressExclusion(parsedIp.get());
	}

	// Not a whole machine, includes `port'.
	try {
		auto addr = NetworkAddress::parse(key.toString());
		if (addr.isTLS()) {
			TraceEvent(SevWarnAlways, "AddressExclusionParseError")
				.detail("String", key)
				.detail("Description", "Address inclusion string should not include `:tls' suffix.");
			return AddressExclusion();
		}
		return AddressExclusion(addr.ip, addr.port);
	} catch (Error& ) {
		TraceEvent(SevWarnAlways, "AddressExclusionParseError").detail("String", key);
		return AddressExclusion();
	}
}

Future<Standalone<RangeResultRef>> getRange(
	Database const& cx,
	Future<Version> const& fVersion,
	KeySelector const& begin,
	KeySelector const& end,
	GetRangeLimits const& limits,
	bool const& reverse,
	TransactionInfo const& info,
	TagSet const& tags);

ACTOR Future<Optional<Value>> getValue(Future<Version> version, Key key, Database cx, TransactionInfo info,
                                       Reference<TransactionLogInfo> trLogInfo, TagSet tags);

ACTOR Future<Optional<StorageServerInterface>> fetchServerInterface( Database cx, TransactionInfo info, UID id, TagSet tags, Future<Version> ver = latestVersion ) {
	Optional<Value> val = wait( getValue(ver, serverListKeyFor(id), cx, info, Reference<TransactionLogInfo>(), tags) );
	if( !val.present() ) {
		// A storage server has been removed from serverList since we read keyServers
		return Optional<StorageServerInterface>();
	}

	return decodeServerListValue(val.get());
}

ACTOR Future<Optional<vector<StorageServerInterface>>> transactionalGetServerInterfaces( Future<Version> ver, Database cx, TransactionInfo info, vector<UID> ids, TagSet tags ) {
	state vector< Future< Optional<StorageServerInterface> > > serverListEntries;
	for( int s = 0; s < ids.size(); s++ ) {
		serverListEntries.push_back( fetchServerInterface( cx, info, ids[s], tags, ver ) );
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
		++cx->transactionKeyServerLocationRequests;
		choose {
			when ( wait( cx->onMasterProxiesChanged() ) ) {}
			when ( GetKeyServerLocationsReply rep = wait( basicLoadBalance( cx->getMasterProxies(info.useProvisionalProxies), &MasterProxyInterface::getKeyServersLocations, GetKeyServerLocationsRequest(key, Optional<KeyRef>(), 100, isBackward, key.arena()), TaskPriority::DefaultPromiseEndpoint ) ) ) {
				++cx->transactionKeyServerLocationRequestsCompleted;
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
Future<pair<KeyRange, Reference<LocationInfo>>> getKeyLocation(Database const& cx, Key const& key,
															   F StorageServerInterface::*member,
															   TransactionInfo const& info,
															   bool isBackward = false) {
	// we first check whether this range is cached
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
		++cx->transactionKeyServerLocationRequests;
		choose {
			when ( wait( cx->onMasterProxiesChanged() ) ) {}
			when ( GetKeyServerLocationsReply _rep = wait( basicLoadBalance( cx->getMasterProxies(info.useProvisionalProxies), &MasterProxyInterface::getKeyServersLocations, GetKeyServerLocationsRequest(keys.begin, keys.end, limit, reverse, keys.arena()), TaskPriority::DefaultPromiseEndpoint ) ) ) {
				++cx->transactionKeyServerLocationRequestsCompleted;
				state GetKeyServerLocationsReply rep = _rep;
				if( info.debugID.present() )
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getKeyLocations.After");
				ASSERT( rep.results.size() );

				state vector< pair<KeyRange,Reference<LocationInfo>> > results;
				state int shard = 0;
				for (; shard < rep.results.size(); shard++) {
					//FIXME: these shards are being inserted into the map sequentially, it would be much more CPU efficient to save the map pairs and insert them all at once.
					results.emplace_back(rep.results[shard].first & keys, cx->setCachedLocation(rep.results[shard].first, rep.results[shard].second));
					wait(yield());
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
					wait(success( tr.getReadVersion() ));
					break;
				} catch( Error &e ) {
					wait( tr.onError(e) );
				}
			}
		}
	}

	return Void();
}

Future<Void> Transaction::warmRange(Database cx, KeyRange keys) {
	return warmRange_impl(this, cx, keys);
}

ACTOR Future<Optional<Value>> getValue( Future<Version> version, Key key, Database cx, TransactionInfo info, Reference<TransactionLogInfo> trLogInfo, TagSet tags )
{
	state Version ver = wait( version );
	cx->validateVersion(ver);

	loop {
		state pair<KeyRange, Reference<LocationInfo>> ssi = wait( getKeyLocation(cx, key, &StorageServerInterface::getValue, info) );
		state Optional<UID> getValueID = Optional<UID>();
		state uint64_t startTime;
		state double startTimeD;
		try {
			if( info.debugID.present() ) {
				getValueID = nondeterministicRandom()->randomUniqueID();

				g_traceBatch.addAttach("GetValueAttachID", info.debugID.get().first(), getValueID.get().first());
				g_traceBatch.addEvent("GetValueDebug", getValueID.get().first(), "NativeAPI.getValue.Before"); //.detail("TaskID", g_network->getCurrentTask());
				/*TraceEvent("TransactionDebugGetValueInfo", getValueID.get())
					.detail("Key", key)
					.detail("ReqVersion", ver)
					.detail("Servers", describe(ssi.second->get()));*/
			}

			++cx->getValueSubmitted;
			startTime = timer_int();
			startTimeD = now();
			++cx->transactionPhysicalReads;

			state GetValueReply reply;
			try {
				if (CLIENT_BUGGIFY) {
					throw deterministicRandom()->randomChoice(
						std::vector<Error>{ transaction_too_old(), future_version() });
				}
				choose {
					when(wait(cx->connectionFileChanged())) { throw transaction_too_old(); }
					when(GetValueReply _reply =
							wait(loadBalance(cx.getPtr(), ssi.second, &StorageServerInterface::getValue,
											GetValueRequest(key, ver, cx->sampleReadTags() ? tags : Optional<TagSet>(), getValueID), TaskPriority::DefaultPromiseEndpoint, false,
											cx->enableLocalityLoadBalance ? &cx->queueModel : nullptr))) {
						reply = _reply;
					}
				}
				++cx->transactionPhysicalReadsCompleted;
			}
			catch(Error&) {
				++cx->transactionPhysicalReadsCompleted;
				throw;
			}

			double latency = now() - startTimeD;
			cx->readLatencies.addSample(latency);
			if (trLogInfo) {
				int valueSize = reply.value.present() ? reply.value.get().size() : 0;
				trLogInfo->addLog(FdbClientLogEvents::EventGet(startTimeD, cx->clientLocality.dcId(), latency, valueSize, key));
			}
			cx->getValueCompleted->latency = timer_int() - startTime;
			cx->getValueCompleted->log();

			if( info.debugID.present() ) {
				g_traceBatch.addEvent("GetValueDebug", getValueID.get().first(), "NativeAPI.getValue.After"); //.detail("TaskID", g_network->getCurrentTask());
				/*TraceEvent("TransactionDebugGetValueDone", getValueID.get())
					.detail("Key", key)
					.detail("ReqVersion", ver)
					.detail("ReplySize", reply.value.present() ? reply.value.get().size() : -1);*/
			}

			cx->transactionBytesRead += reply.value.present() ? reply.value.get().size() : 0;
			++cx->transactionKeysRead;
			return reply.value;
		} catch (Error& e) {
			cx->getValueCompleted->latency = timer_int() - startTime;
			cx->getValueCompleted->log();
			if( info.debugID.present() ) {
				g_traceBatch.addEvent("GetValueDebug", getValueID.get().first(), "NativeAPI.getValue.Error"); //.detail("TaskID", g_network->getCurrentTask());
				/*TraceEvent("TransactionDebugGetValueDone", getValueID.get())
					.detail("Key", key)
					.detail("ReqVersion", ver)
					.detail("ReplySize", reply.value.present() ? reply.value.get().size() : -1);*/
			}
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
				(e.code() == error_code_transaction_too_old && ver == latestVersion) ) {
				cx->invalidateCache( key );
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, info.taskID));
			} else {
				if (trLogInfo)
					trLogInfo->addLog(FdbClientLogEvents::EventGetError(startTimeD, cx->clientLocality.dcId(), static_cast<int>(e.code()), key));
				throw e;
			}
		}
	}
}

ACTOR Future<Key> getKey( Database cx, KeySelector k, Future<Version> version, TransactionInfo info, TagSet tags ) {
	wait(success(version));

	state Optional<UID> getKeyID = Optional<UID>();
	if( info.debugID.present() ) {
		getKeyID = nondeterministicRandom()->randomUniqueID();

		g_traceBatch.addAttach("GetKeyAttachID", info.debugID.get().first(), getKeyID.get().first());
		g_traceBatch.addEvent("GetKeyDebug", getKeyID.get().first(), "NativeAPI.getKey.AfterVersion"); //.detail("StartKey", k.getKey()).detail("Offset",k.offset).detail("OrEqual",k.orEqual);
	}

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
				g_traceBatch.addEvent("GetKeyDebug", getKeyID.get().first(), "NativeAPI.getKey.Before"); //.detail("StartKey", k.getKey()).detail("Offset",k.offset).detail("OrEqual",k.orEqual);
			++cx->transactionPhysicalReads;
			state GetKeyReply reply;
			try {
				choose {
					when(wait(cx->connectionFileChanged())) { throw transaction_too_old(); }
					when(GetKeyReply _reply =
							wait(loadBalance(cx.getPtr(), ssi.second, &StorageServerInterface::getKey, GetKeyRequest(k, version.get(), cx->sampleReadTags() ? tags : Optional<TagSet>(), getKeyID),
											TaskPriority::DefaultPromiseEndpoint, false,
											cx->enableLocalityLoadBalance ? &cx->queueModel : nullptr))) {
						reply = _reply;
					}
				}
				++cx->transactionPhysicalReadsCompleted;
			} catch(Error&) {
				++cx->transactionPhysicalReadsCompleted;
				throw;
			}
			if( info.debugID.present() )
				g_traceBatch.addEvent("GetKeyDebug", getKeyID.get().first(), "NativeAPI.getKey.After"); //.detail("NextKey",reply.sel.key).detail("Offset", reply.sel.offset).detail("OrEqual", k.orEqual);
			k = reply.sel;
			if (!k.offset && k.orEqual) {
				return k.getKey();
			}
		} catch (Error& e) {
			if( info.debugID.present() )
				g_traceBatch.addEvent("GetKeyDebug", getKeyID.get().first(), "NativeAPI.getKey.Error");
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
				cx->invalidateCache(k.getKey(), k.isBackward());

				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, info.taskID));
			} else {
				TraceEvent(SevInfo, "GetKeyError")
					.error(e)
					.detail("AtKey", k.getKey())
					.detail("Offset", k.offset);
				throw e;
			}
		}
	}
}

ACTOR Future<Version> waitForCommittedVersion( Database cx, Version version ) {
	try {
		loop {
			choose {
				when ( wait( cx->onMasterProxiesChanged() ) ) {}
				when ( GetReadVersionReply v = wait( basicLoadBalance( cx->getMasterProxies(false), &MasterProxyInterface::getConsistentReadVersion, GetReadVersionRequest( 0, TransactionPriority::IMMEDIATE ), cx->taskID ) ) ) {
					cx->minAcceptableReadVersion = std::min(cx->minAcceptableReadVersion, v.version);

					if (v.version >= version)
						return v.version;
					// SOMEDAY: Do the wait on the server side, possibly use less expensive source of committed version (causal consistency is not needed for this purpose)
					wait( delay( CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, cx->taskID ) );
				}
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "WaitForCommittedVersionError").error(e);
		throw;
	}
}

ACTOR Future<Version> getRawVersion( Database cx ) {
	loop {
		choose {
			when ( wait( cx->onMasterProxiesChanged() ) ) {}
			when ( GetReadVersionReply v = wait( basicLoadBalance( cx->getMasterProxies(false), &MasterProxyInterface::getConsistentReadVersion, GetReadVersionRequest( 0, TransactionPriority::IMMEDIATE ), cx->taskID ) ) ) {
				return v.version;
			}
		}
	}
}

ACTOR Future<Void> readVersionBatcher(
	DatabaseContext* cx, FutureStream<std::pair<Promise<GetReadVersionReply>, Optional<UID>>> versionStream,
	uint32_t flags);

ACTOR Future<Void> watchValue(Future<Version> version, Key key, Optional<Value> value, Database cx,
                              TransactionInfo info, TagSet tags) {
	state Version ver = wait( version );
	cx->validateVersion(ver);
	ASSERT(ver != latestVersion);

	loop {
		state pair<KeyRange, Reference<LocationInfo>> ssi = wait( getKeyLocation(cx, key, &StorageServerInterface::watchValue, info ) );

		try {
			state Optional<UID> watchValueID = Optional<UID>();
			if( info.debugID.present() ) {
				watchValueID = nondeterministicRandom()->randomUniqueID();

				g_traceBatch.addAttach("WatchValueAttachID", info.debugID.get().first(), watchValueID.get().first());
				g_traceBatch.addEvent("WatchValueDebug", watchValueID.get().first(), "NativeAPI.watchValue.Before"); //.detail("TaskID", g_network->getCurrentTask());
			}
			state WatchValueReply resp;
			choose {
				when(WatchValueReply r = wait(loadBalance(cx.getPtr(), ssi.second, &StorageServerInterface::watchValue,
				                                          WatchValueRequest(key, value, ver, cx->sampleReadTags() ? tags : Optional<TagSet>(), watchValueID),
				                                          TaskPriority::DefaultPromiseEndpoint))) {
					resp = r;
				}
				when(wait(cx->connectionFile ? cx->connectionFile->onChange() : Never())) { wait(Never()); }
			}
			if( info.debugID.present() ) {
				g_traceBatch.addEvent("WatchValueDebug", watchValueID.get().first(), "NativeAPI.watchValue.After"); //.detail("TaskID", g_network->getCurrentTask());
			}

			//FIXME: wait for known committed version on the storage server before replying,
			//cannot do this until the storage server is notified on knownCommittedVersion changes from tlog (faster than the current update loop)
			Version v = wait(waitForCommittedVersion(cx, resp.version));

			//TraceEvent("WatcherCommitted").detail("CommittedVersion", v).detail("WatchVersion", resp.version).detail("Key",  key ).detail("Value", value);

			// False if there is a master failure between getting the response and getting the committed version,
			// Dependent on SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT
			if (v - resp.version < 50000000) return Void();
			ver = v;
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
				cx->invalidateCache( key );
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, info.taskID));
			} else if( e.code() == error_code_watch_cancelled || e.code() == error_code_process_behind ) {
				TEST( e.code() == error_code_watch_cancelled ); // Too many watches on the storage server, poll for changes instead
				TEST( e.code() == error_code_process_behind ); // The storage servers are all behind
				wait(delay(CLIENT_KNOBS->WATCH_POLLING_TIME, info.taskID));
			} else if ( e.code() == error_code_timed_out ) { //The storage server occasionally times out watches in case it was cancelled
				TEST( true ); // A watch timed out
				wait(delay(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, info.taskID));
			} else {
				state Error err = e;
				wait(delay(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, info.taskID));
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
	KeyRange keys, GetRangeLimits limits, bool reverse, TransactionInfo info, TagSet tags )
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
			req.tags = cx->sampleReadTags() ? tags : Optional<TagSet>();
			req.debugID = info.debugID;

			try {
				if( info.debugID.present() ) {
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getExactRange.Before");
					/*TraceEvent("TransactionDebugGetExactRangeInfo", info.debugID.get())
						.detail("ReqBeginKey", req.begin.getKey())
						.detail("ReqEndKey", req.end.getKey())
						.detail("ReqLimit", req.limit)
						.detail("ReqLimitBytes", req.limitBytes)
						.detail("ReqVersion", req.version)
						.detail("Reverse", reverse)
						.detail("Servers", locations[shard].second->description());*/
				}
				++cx->transactionPhysicalReads;
				state GetKeyValuesReply rep;
				try {
					choose {
						when(wait(cx->connectionFileChanged())) { throw transaction_too_old(); }
						when(GetKeyValuesReply _rep = wait(
								 loadBalance(cx.getPtr(), locations[shard].second, &StorageServerInterface::getKeyValues,
											 req, TaskPriority::DefaultPromiseEndpoint, false,
											 cx->enableLocalityLoadBalance ? &cx->queueModel : nullptr))) {
							rep = _rep;
						}
					}
					++cx->transactionPhysicalReadsCompleted;
				} catch(Error&) {
					++cx->transactionPhysicalReadsCompleted;
					throw;
				}
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
					wait( delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, info.taskID ));
					break;
				} else {
					TraceEvent(SevInfo, "GetExactRangeError")
						.error(e)
						.detail("ShardBegin", locations[shard].first.begin)
						.detail("ShardEnd", locations[shard].first.end);
					throw;
				}
			}
		}
	}
}

Future<Key> resolveKey( Database const& cx, KeySelector const& key, Version const& version, TransactionInfo const& info, TagSet const& tags ) {
	if( key.isFirstGreaterOrEqual() )
		return Future<Key>( key.getKey() );

	if( key.isFirstGreaterThan() )
		return Future<Key>( keyAfter( key.getKey() ) );

	return getKey( cx, key, version, info, tags );
}

ACTOR Future<Standalone<RangeResultRef>> getRangeFallback( Database cx, Version version,
	KeySelector begin, KeySelector end, GetRangeLimits limits, bool reverse, TransactionInfo info,
	TagSet tags )
{
	if(version == latestVersion) {
		state Transaction transaction(cx);
		transaction.setOption(FDBTransactionOptions::CAUSAL_READ_RISKY);
		transaction.setOption(FDBTransactionOptions::LOCK_AWARE);
		transaction.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		Version ver = wait( transaction.getReadVersion() );
		version = ver;
	}

	Future<Key> fb = resolveKey(cx, begin, version, info, tags);
	state Future<Key> fe = resolveKey(cx, end, version, info, tags);

	state Key b = wait(fb);
	state Key e = wait(fe);
	if (b >= e) {
		return Standalone<RangeResultRef>();
	}

	//if e is allKeys.end, we have read through the end of the database
	//if b is allKeys.begin, we have either read through the beginning of the database,
	//or allKeys.begin exists in the database and will be part of the conflict range anyways

	Standalone<RangeResultRef> _r = wait( getExactRange(cx, version, KeyRangeRef(b, e), limits, reverse, info, tags) );
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

void getRangeFinished(Database cx, Reference<TransactionLogInfo> trLogInfo, double startTime, KeySelector begin, KeySelector end, bool snapshot,
	Promise<std::pair<Key, Key>> conflictRange, bool reverse, Standalone<RangeResultRef> result)
{
	int64_t bytes = 0;
	for(const KeyValueRef &kv : result) {
		bytes += kv.key.size() + kv.value.size();
	}

	cx->transactionBytesRead += bytes;
	cx->transactionKeysRead += result.size();
	
	if( trLogInfo ) {
		trLogInfo->addLog(FdbClientLogEvents::EventGetRange(startTime, cx->clientLocality.dcId(), now()-startTime, bytes, begin.getKey(), end.getKey()));
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
	TransactionInfo info, TagSet tags )
{
	state GetRangeLimits originalLimits( limits );
	state KeySelector originalBegin = begin;
	state KeySelector originalEnd = end;
	state Standalone<RangeResultRef> output;

	try {
		state Version version = wait( fVersion );
		cx->validateVersion(version);

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
				getRangeFinished(cx, trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
				return output;
			}

			Key locationKey = reverse ? Key(end.getKey(), end.arena()) : Key(begin.getKey(), begin.arena());
			bool locationBackward = reverse ? (end-1).isBackward() : begin.isBackward();
			state pair<KeyRange, Reference<LocationInfo>> beginServer = wait( getKeyLocation( cx, locationKey, &StorageServerInterface::getKeyValues, info, locationBackward ) );
			state KeyRange shard = beginServer.first;
			state bool modifiedSelectors = false;
			state GetKeyValuesRequest req;

			req.isFetchKeys = (info.taskID == TaskPriority::FetchKeys);
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

			req.tags = cx->sampleReadTags() ? tags : Optional<TagSet>();
			req.debugID = info.debugID;
			try {
				if( info.debugID.present() ) {
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getRange.Before");
					/*TraceEvent("TransactionDebugGetRangeInfo", info.debugID.get())
						.detail("ReqBeginKey", req.begin.getKey())
						.detail("ReqEndKey", req.end.getKey())
						.detail("OriginalBegin", originalBegin.toString())
						.detail("OriginalEnd", originalEnd.toString())
						.detail("Begin", begin.toString())
						.detail("End", end.toString())
						.detail("Shard", shard)
						.detail("ReqLimit", req.limit)
						.detail("ReqLimitBytes", req.limitBytes)
						.detail("ReqVersion", req.version)
						.detail("Reverse", reverse)
						.detail("ModifiedSelectors", modifiedSelectors)
						.detail("Servers", beginServer.second->description());*/
				}

				++cx->transactionPhysicalReads;
				++cx->transactionGetRangeRequests;
				state GetKeyValuesReply rep;
				try {
					if (CLIENT_BUGGIFY) {
						throw deterministicRandom()->randomChoice(std::vector<Error>{
								transaction_too_old(), future_version()
									});
					}
					GetKeyValuesReply _rep =
						wait(loadBalance(cx.getPtr(), beginServer.second, &StorageServerInterface::getKeyValues, req,
										 TaskPriority::DefaultPromiseEndpoint, false,
										 cx->enableLocalityLoadBalance ? &cx->queueModel : NULL));
					rep = _rep;
					++cx->transactionPhysicalReadsCompleted;
				} catch(Error&) {
					++cx->transactionPhysicalReadsCompleted;
					throw;
				}

				if( info.debugID.present() ) {
					g_traceBatch.addEvent("TransactionDebug", info.debugID.get().first(), "NativeAPI.getRange.After");//.detail("SizeOf", rep.data.size());
					/*TraceEvent("TransactionDebugGetRangeDone", info.debugID.get())
						.detail("ReqBeginKey", req.begin.getKey())
						.detail("ReqEndKey", req.end.getKey())
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
						output.resize(output.arena(), deterministicRandom()->randomInt(std::max(1,originalLimits.minRows),output.size()));
						getRangeFinished(cx, trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
						return output;
					}

					if( readThrough ) {
						output.arena().dependsOn( shard.arena() );
						output.readThrough = reverse ? shard.begin : shard.end;
					}

					getRangeFinished(cx, trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
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

					getRangeFinished(cx, trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, output);
					return output;
				}

				readVersion = rep.version; // see above comment

				if( !rep.more ) {
					ASSERT( modifiedSelectors );
					TEST(true);  // !GetKeyValuesReply.more and modifiedSelectors in getRange

					if( !rep.data.size() ) {
						Standalone<RangeResultRef> result = wait( getRangeFallback(cx, version, originalBegin, originalEnd, originalLimits, reverse, info, tags ) );
						getRangeFinished(cx, trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, result);
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
						Standalone<RangeResultRef> result = wait( getRangeFallback(cx, version, originalBegin, originalEnd, originalLimits, reverse, info, tags ) );
						getRangeFinished(cx, trLogInfo, startTime, originalBegin, originalEnd, snapshot, conflictRange, reverse, result);
						return result;
					}

					wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, info.taskID));
				} else {
					if (trLogInfo)
						trLogInfo->addLog(FdbClientLogEvents::EventGetRangeError(startTime, cx->clientLocality.dcId(), static_cast<int>(e.code()), begin.getKey(), end.getKey()));

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
	GetRangeLimits const& limits, bool const& reverse, TransactionInfo const& info, TagSet const& tags )
{
	return getRange(cx, Reference<TransactionLogInfo>(), fVersion, begin, end, limits, Promise<std::pair<Key, Key>>(), true, reverse, info, tags);
}

bool DatabaseContext::debugUseTags = false;
const std::vector<std::string> DatabaseContext::debugTransactionTagChoices = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t" }; 

void debugAddTags(Transaction *tr) {
	int numTags = deterministicRandom()->randomInt(0, CLIENT_KNOBS->MAX_TAGS_PER_TRANSACTION+1);
	for(int i = 0; i < numTags; ++i) { 
		TransactionTag tag;
		if(deterministicRandom()->random01() < 0.7) {
			tag = TransactionTagRef(deterministicRandom()->randomChoice(DatabaseContext::debugTransactionTagChoices));
		}
		else {
			int length = deterministicRandom()->randomInt(1, CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH+1);
			uint8_t* s = new (tag.arena()) uint8_t[length];
			for(int j = 0; j < length; ++j) {
				s[j] = (uint8_t)deterministicRandom()->randomInt(0, 256);
			}

			tag.contents() = TransactionTagRef(s, length);
		}

		if(deterministicRandom()->coinflip()) {
			tr->options.readTags.addTag(tag);
		}
		tr->options.tags.addTag(tag);
	}

}

Transaction::Transaction( Database const& cx )
	: cx(cx), info(cx->taskID), backoff(CLIENT_KNOBS->DEFAULT_BACKOFF), committedVersion(invalidVersion), versionstampPromise(Promise<Standalone<StringRef>>()), options(cx), numErrors(0), trLogInfo(createTrLogInfoProbabilistically(cx))
{
	if(DatabaseContext::debugUseTags) {
		debugAddTags(this);
	}
}

Transaction::~Transaction() {
	flushTrLogsIfEnabled();
	cancelWatches();
}

void Transaction::operator=(Transaction&& r) noexcept {
	flushTrLogsIfEnabled();
	cx = std::move(r.cx);
	tr = std::move(r.tr);
	readVersion = std::move(r.readVersion);
	metadataVersion = std::move(r.metadataVersion);
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
		cx->clientStatusUpdater.inStatusQ.push_back({ trLogInfo->identifier, std::move(trLogInfo->trLogWriter) });
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
	++cx->transactionGetValueRequests;
	//ASSERT (key < allKeys.end);

	//There are no keys in the database with size greater than KEY_SIZE_LIMIT
	if(key.size() > (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		return Optional<Value>();

	auto ver = getReadVersion();

/*	if (!systemKeys.contains(key))
		return Optional<Value>(Value()); */

	if( !snapshot )
		tr.transaction.read_conflict_ranges.push_back(tr.arena, singleKeyRange(key, tr.arena));

	if(key == metadataVersionKey) {
		++cx->transactionMetadataVersionReads;
		if(!ver.isReady() || metadataVersion.isSet()) {
			return metadataVersion.getFuture();
		} else {
			if(ver.isError()) return ver.getError();
			if(ver.get() == cx->metadataVersionCache[cx->mvCacheInsertLocation].first) {
				return cx->metadataVersionCache[cx->mvCacheInsertLocation].second;
			}

			Version v = ver.get();
			int hi = cx->mvCacheInsertLocation;
			int lo = (cx->mvCacheInsertLocation+1)%cx->metadataVersionCache.size();

			while(hi!=lo) {
				int cu = hi > lo ? (hi + lo)/2 : ((hi + cx->metadataVersionCache.size() + lo)/2)%cx->metadataVersionCache.size();
				if(v == cx->metadataVersionCache[cu].first) {
					return cx->metadataVersionCache[cu].second;
				}
				if(cu == lo) {
					break;
				}
				if(v < cx->metadataVersionCache[cu].first) {
					hi = cu;
				} else {
					lo = (cu+1)%cx->metadataVersionCache.size();
				}
			}
		}
	}

	return getValue( ver, key, cx, info, trLogInfo, options.readTags );
}

void Watch::setWatch(Future<Void> watchFuture) {
	this->watchFuture = watchFuture;

	//Cause the watch loop to go around and start waiting on watchFuture
	onSetWatchTrigger.send(Void());
}

//FIXME: This seems pretty horrible. Now a Database can't die until all of its watches do...
ACTOR Future<Void> watch(Reference<Watch> watch, Database cx, TagSet tags, TransactionInfo info) {
	try {
		choose {
			// RYOW write to value that is being watched (if applicable)
			// Errors
			when(wait(watch->onChangeTrigger.getFuture())) { }

			// NativeAPI finished commit and updated watchFuture
			when(wait(watch->onSetWatchTrigger.getFuture())) {

				loop {
					choose {
						// NativeAPI watchValue future finishes or errors
						when(wait(watch->watchFuture)) { break; }

						when(wait(cx->connectionFileChanged())) {
							TEST(true); // Recreated a watch after switch
							watch->watchFuture =
							    watchValue(cx->minAcceptableReadVersion, watch->key, watch->value, cx, info, tags);
						}
					}
				}
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

Future<Version> Transaction::getRawReadVersion() {
	return ::getRawVersion(cx);
}

Future< Void > Transaction::watch( Reference<Watch> watch ) {
	++cx->transactionWatchRequests;
	cx->addWatch();
	watches.push_back(watch);
	return ::watch(watch, cx, options.readTags, info);
}

ACTOR Future<Standalone<VectorRef<const char*>>> getAddressesForKeyActor(Key key, Future<Version> ver, Database cx,
                                                                         TransactionInfo info,
                                                                         TransactionOptions options) {
	state vector<StorageServerInterface> ssi;

	// If key >= allKeys.end, then getRange will return a kv-pair with an empty value. This will result in our serverInterfaces vector being empty, which will cause us to return an empty addresses list.

	state Key ksKey = keyServersKey(key);
	state Standalone<RangeResultRef> serverTagResult = wait( getRange(cx, ver, lastLessOrEqual(serverTagKeys.begin), firstGreaterThan(serverTagKeys.end), GetRangeLimits(CLIENT_KNOBS->TOO_MANY), false, info, options.readTags) );
	ASSERT( !serverTagResult.more && serverTagResult.size() < CLIENT_KNOBS->TOO_MANY );
	Future<Standalone<RangeResultRef>> futureServerUids = getRange(cx, ver, lastLessOrEqual(ksKey), firstGreaterThan(ksKey), GetRangeLimits(1), false, info, options.readTags);
	Standalone<RangeResultRef> serverUids = wait( futureServerUids );

	ASSERT( serverUids.size() ); // every shard needs to have a team

	vector<UID> src;
	vector<UID> ignore; // 'ignore' is so named because it is the vector into which we decode the 'dest' servers in the case where this key is being relocated. But 'src' is the canonical location until the move is finished, because it could be cancelled at any time.
	decodeKeyServersValue(serverTagResult, serverUids[0].value, src, ignore);
	Optional<vector<StorageServerInterface>> serverInterfaces = wait( transactionalGetServerInterfaces(ver, cx, info, src, options.readTags) );

	ASSERT( serverInterfaces.present() );  // since this is happening transactionally, /FF/keyServers and /FF/serverList need to be consistent with one another
	ssi = serverInterfaces.get();

	Standalone<VectorRef<const char*>> addresses;
	for (auto i : ssi) {
		std::string ipString = options.includePort ? i.address().toString() : i.address().ip.toString();
		char* c_string = new (addresses.arena()) char[ipString.length()+1];
		strcpy(c_string, ipString.c_str());
		addresses.push_back(addresses.arena(), c_string);
	}
	return addresses;
}

Future< Standalone< VectorRef< const char*>>> Transaction::getAddressesForKey( const Key& key ) {
	++cx->transactionLogicalReads;
	++cx->transactionGetAddressesForKeyRequests;
	auto ver = getReadVersion();

	return getAddressesForKeyActor(key, ver, cx, info, options);
}

ACTOR Future< Key > getKeyAndConflictRange(
	Database cx, KeySelector k, Future<Version> version, Promise<std::pair<Key, Key>> conflictRange, TransactionInfo info, TagSet tags)
{
	try {
		Key rep = wait( getKey(cx, k, version, info, tags) );
		if( k.offset <= 0 )
			conflictRange.send( std::make_pair( rep, k.orEqual ? keyAfter( k.getKey() ) : Key(k.getKey(), k.arena()) ) );
		else
			conflictRange.send( std::make_pair( k.orEqual ? keyAfter( k.getKey() ) : Key(k.getKey(), k.arena()), keyAfter( rep ) ) );
		return rep;
	} catch( Error&e ) {
		conflictRange.send(std::make_pair(Key(), Key()));
		throw;
	}
}

Future< Key > Transaction::getKey( const KeySelector& key, bool snapshot ) {
	++cx->transactionLogicalReads;
	++cx->transactionGetKeyRequests;
	if( snapshot )
		return ::getKey(cx, key, getReadVersion(), info, options.readTags);

	Promise<std::pair<Key, Key>> conflictRange;
	extraConflictRanges.push_back( conflictRange.getFuture() );
	return getKeyAndConflictRange( cx, key, getReadVersion(), conflictRange, info, options.readTags );
}

Future< Standalone<RangeResultRef> > Transaction::getRange(
	const KeySelector& begin,
	const KeySelector& end,
	GetRangeLimits limits,
	bool snapshot,
	bool reverse )
{
	++cx->transactionLogicalReads;
	++cx->transactionGetRangeRequests;

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

	return ::getRange(cx, trLogInfo, getReadVersion(), b, e, limits, conflictRange, snapshot, reverse, info, options.readTags);
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
	wr << deterministicRandom()->randomUniqueID();
	auto r = singleKeyRange( wr.toValue(), tr.arena );
	tr.transaction.read_conflict_ranges.push_back( tr.arena, r );
	tr.transaction.write_conflict_ranges.push_back( tr.arena, r );
}

void Transaction::set( const KeyRef& key, const ValueRef& value, bool addConflictRange ) {
	++cx->transactionSetMutations;
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
	++cx->transactionAtomicMutations;
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

	if (addConflictRange && operationType != MutationRef::SetVersionstampedKey)
		t.write_conflict_ranges.push_back( req.arena, r );

	TEST(true); //NativeAPI atomic operation
}

void Transaction::clear( const KeyRangeRef& range, bool addConflictRange ) {
	++cx->transactionClearMutations;
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
	++cx->transactionClearMutations;
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

double Transaction::getBackoff(int errCode) {
	double returnedBackoff = backoff;

	if(errCode == error_code_tag_throttled) {
		auto priorityItr = cx->throttledTags.find(options.priority);
		for(auto &tag : options.tags) {
			if(priorityItr != cx->throttledTags.end()) {
				auto tagItr = priorityItr->second.find(tag);
				if(tagItr != priorityItr->second.end()) {
					TEST(true); // Returning throttle backoff
					returnedBackoff = std::min(CLIENT_KNOBS->TAG_THROTTLE_RECHECK_INTERVAL, std::max(returnedBackoff, tagItr->second.throttleDuration()));
					if(returnedBackoff == CLIENT_KNOBS->TAG_THROTTLE_RECHECK_INTERVAL) {
						break;
					}
				}
			}
		}
	}

	returnedBackoff *= deterministicRandom()->random01();

	// Set backoff for next time
	if(errCode == error_code_proxy_memory_limit_exceeded) {
		backoff = std::min(backoff * CLIENT_KNOBS->BACKOFF_GROWTH_RATE, CLIENT_KNOBS->RESOURCE_CONSTRAINED_MAX_BACKOFF);
	}
	else {
		backoff = std::min(backoff * CLIENT_KNOBS->BACKOFF_GROWTH_RATE, options.maxBackoff);
	}

	return returnedBackoff;
}

TransactionOptions::TransactionOptions(Database const& cx) {
	reset(cx);
	if (BUGGIFY) {
		commitOnFirstProxy = true;
	}
}

void TransactionOptions::clear() {
	maxBackoff = CLIENT_KNOBS->DEFAULT_MAX_BACKOFF;
	getReadVersionFlags = 0;
	sizeLimit = CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT;
	maxTransactionLoggingFieldLength = 0;
	checkWritesEnabled = false;
	causalWriteRisky = false;
	commitOnFirstProxy = false;
	debugDump = false;
	lockAware = false;
	readOnly = false;
	firstInBatch = false;
	includePort = false;
	reportConflictingKeys = false;
	tags = TagSet{};
	readTags = TagSet{};
	priority = TransactionPriority::DEFAULT;
}

TransactionOptions::TransactionOptions() {
	clear();
}

void TransactionOptions::reset(Database const& cx) {
	clear();
	lockAware = cx->lockAware;
	if (cx->apiVersionAtLeast(630)) {
		includePort = true;
	}
}

void Transaction::reset() {
	tr = CommitTransactionRequest();
	readVersion = Future<Version>();
	metadataVersion = Promise<Optional<Key>>();
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
		options.reset(cx);
	}
}

void Transaction::fullReset() {
	reset();
	backoff = CLIENT_KNOBS->DEFAULT_BACKOFF;
}

int Transaction::apiVersionAtLeast(int minVersion) const {
	return cx->apiVersionAtLeast(minVersion);
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
		wait( committed );
		// If the commit is successful, by definition the transaction still exists for now.  Grab the version, and don't use it again.
		version = checkTr->getCommittedVersion();
		outCommitted.send(Void());
	} catch (Error& e) {
		outCommitted.sendError(e);
		return;
	}

	wait( delay( deterministicRandom()->random01() ) ); // delay between 0 and 1 seconds

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
						TraceEvent(SevError, "CheckWritesFailed").detail("Class", "Clear").detail("KeyBegin", it->range().begin)
							.detail("KeyEnd", it->range().end);
						return;
					}
				} else {
					Optional<Value> val = wait( tr.get( it->range().begin ) );
					if( !val.present() || val.get() != m.setValue ) {
						TraceEvent evt(SevError, "CheckWritesFailed");
						evt.detail("Class", "Set")
							.detail("Key", it->range().begin)
							.detail("Expected", m.setValue);
						if( !val.present() )
							evt.detail("Actual", "_Value Missing_");
						else
							evt.detail("Actual", val.get());
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
			TraceEvent("CommitDummyTransaction").detail("Key", range.begin).detail("Retries", retries);
			tr.options = options;
			tr.info.taskID = info.taskID;
			tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
			tr.setOption( FDBTransactionOptions::CAUSAL_WRITE_RISKY );
			tr.setOption( FDBTransactionOptions::LOCK_AWARE );
			tr.addReadConflictRange(range);
			tr.addWriteConflictRange(range);
			wait( tr.commit() );
			return Void();
		} catch (Error& e) {
			TraceEvent("CommitDummyTransactionError").error(e,true).detail("Key", range.begin).detail("Retries", retries);
			wait( tr.onError(e) );
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
			watches[i]->setWatch(watchValue(watchVersion, watches[i]->key, watches[i]->value, cx, info, options.readTags));

		watches.clear();
	}
	catch(Error&) {
		ASSERT(false); // The above code must NOT throw because commit has already occured.
		throw internal_error();
	}
}

ACTOR static Future<Void> tryCommit( Database cx, Reference<TransactionLogInfo> trLogInfo, CommitTransactionRequest req, Future<Version> readVersion, TransactionInfo info, Version* pCommittedVersion, Transaction* tr, TransactionOptions options) {
	state TraceInterval interval( "TransactionCommit" );
	state double startTime = now();
	if (info.debugID.present())
		TraceEvent(interval.begin()).detail( "Parent", info.debugID.get() );
	try {
		if(CLIENT_BUGGIFY) {
			throw deterministicRandom()->randomChoice(std::vector<Error>{
					not_committed(),
					transaction_too_old(),
					proxy_memory_limit_exceeded(),
					commit_unknown_result()});
		}

		Version v = wait( readVersion );
		req.transaction.read_snapshot = v;

		startTime = now();
		state Optional<UID> commitID = Optional<UID>();
		if(info.debugID.present()) {
			commitID = nondeterministicRandom()->randomUniqueID();
			g_traceBatch.addAttach("CommitAttachID", info.debugID.get().first(), commitID.get().first());
			g_traceBatch.addEvent("CommitDebug", commitID.get().first(), "NativeAPI.commit.Before");
		}

		req.debugID = commitID;
		state Future<CommitID> reply;
		if (options.commitOnFirstProxy) {
			if(cx->clientInfo->get().firstProxy.present()) {
				reply = throwErrorOr ( brokenPromiseToMaybeDelivered ( cx->clientInfo->get().firstProxy.get().commit.tryGetReply(req) ) );
			} else {
				const std::vector<MasterProxyInterface>& proxies = cx->clientInfo->get().proxies;
				reply = proxies.size() ? throwErrorOr ( brokenPromiseToMaybeDelivered ( proxies[0].commit.tryGetReply(req) ) ) : Never();
			}
		} else {
			reply = basicLoadBalance( cx->getMasterProxies(info.useProvisionalProxies), &MasterProxyInterface::commit, req, TaskPriority::DefaultPromiseEndpoint, true );
		}

		choose {
			when ( wait( cx->onMasterProxiesChanged() ) ) {
				reply.cancel();
				throw request_maybe_delivered();
			}
			when (CommitID ci = wait( reply )) {
				Version v = ci.version;
				if (v != invalidVersion) {
					if (CLIENT_BUGGIFY) {
						throw commit_unknown_result();
					}
					if (info.debugID.present())
						TraceEvent(interval.end()).detail("CommittedVersion", v);
					*pCommittedVersion = v;
					if(v > cx->metadataVersionCache[cx->mvCacheInsertLocation].first) {
						cx->mvCacheInsertLocation = (cx->mvCacheInsertLocation + 1)%cx->metadataVersionCache.size();
						cx->metadataVersionCache[cx->mvCacheInsertLocation] = std::make_pair(v, ci.metadataVersion);
					}

					Standalone<StringRef> ret = makeString(10);
					placeVersionstamp(mutateString(ret), v, ci.txnBatchId);
					tr->versionstampPromise.send(ret);

					tr->numErrors = 0;
					++cx->transactionsCommitCompleted;
					cx->transactionCommittedMutations += req.transaction.mutations.size();
					cx->transactionCommittedMutationBytes += req.transaction.mutations.expectedSize();

					if(info.debugID.present())
						g_traceBatch.addEvent("CommitDebug", commitID.get().first(), "NativeAPI.commit.After");

					double latency = now() - startTime;
					cx->commitLatencies.addSample(latency);
					cx->latencies.addSample(now() - tr->startTime);
					if (trLogInfo)
						trLogInfo->addLog(FdbClientLogEvents::EventCommit_V2(startTime, cx->clientLocality.dcId(), latency, req.transaction.mutations.size(), req.transaction.mutations.expectedSize(), ci.version, req));
					return Void();
				} else {
					// clear the RYW transaction which contains previous conflicting keys
					tr->info.conflictingKeys.reset();
					if (ci.conflictingKRIndices.present()) {
						tr->info.conflictingKeys =
						    std::make_shared<CoalescedKeyRangeMap<Value>>(conflictingKeysFalse, specialKeys.end);
						state Standalone<VectorRef<int>> conflictingKRIndices = ci.conflictingKRIndices.get();
						// drop duplicate indices and merge overlapped ranges
						// Note: addReadConflictRange in native transaction object does not merge overlapped ranges
						state std::unordered_set<int> mergedIds(conflictingKRIndices.begin(),
																conflictingKRIndices.end());
						for (auto const& rCRIndex : mergedIds) {
							const KeyRangeRef kr = req.transaction.read_conflict_ranges[rCRIndex];
							const KeyRange krWithPrefix = KeyRangeRef(kr.begin.withPrefix(conflictingKeysRange.begin),
							                                          kr.end.withPrefix(conflictingKeysRange.begin));
							tr->info.conflictingKeys->insert(krWithPrefix, conflictingKeysTrue);
						}
					}

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

				wait( commitDummyTransaction( cx, singleKeyRange(selfConflictingRange.begin), info, tr->options ) );
			}

			// The user needs to be informed that we aren't sure whether the commit happened.  Standard retry loops retry it anyway (relying on transaction idempotence) but a client might do something else.
			throw commit_unknown_result();
		} else {
			if (e.code() != error_code_transaction_too_old
				&& e.code() != error_code_not_committed
				&& e.code() != error_code_database_locked
				&& e.code() != error_code_proxy_memory_limit_exceeded
				&& e.code() != error_code_batch_transaction_throttled
				&& e.code() != error_code_tag_throttled)
			{
				TraceEvent(SevError, "TryCommitError").error(e);
			}
			if (trLogInfo)
				trLogInfo->addLog(FdbClientLogEvents::EventCommitError(startTime, cx->clientLocality.dcId(), static_cast<int>(e.code()), req));
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

		++cx->transactionsCommitStarted;

		if(options.readOnly)
			return transaction_read_only();

		cx->mutationsPerCommit.addSample(tr.transaction.mutations.size());
		cx->bytesPerCommit.addSample(tr.transaction.mutations.expectedSize());

		size_t transactionSize = getSize();
		if (transactionSize > (uint64_t)FLOW_KNOBS->PACKET_WARNING) {
			TraceEvent(!g_network->isSimulated() ? SevWarnAlways : SevWarn, "LargeTransaction")
				.suppressFor(1.0)
				.detail("Size", transactionSize)
				.detail("NumMutations", tr.transaction.mutations.size())
				.detail("ReadConflictSize", tr.transaction.read_conflict_ranges.expectedSize())
				.detail("WriteConflictSize", tr.transaction.write_conflict_ranges.expectedSize())
				.detail("DebugIdentifier", trLogInfo ? trLogInfo->identifier : "");
		}

		if(!apiVersionAtLeast(300)) {
			transactionSize = tr.transaction.mutations.expectedSize(); // Old API versions didn't account for conflict ranges when determining whether to throw transaction_too_large
		}

		if (transactionSize > options.sizeLimit) {
			return transaction_too_large();
		}

		if( !readVersion.isValid() )
			getReadVersion( GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY ); // sets up readVersion field.  We had no reads, so no need for (expensive) full causal consistency.

		bool isCheckingWrites = options.checkWritesEnabled && deterministicRandom()->random01() < 0.01;
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
			UID u = nondeterministicRandom()->randomUniqueID();
			TraceEvent("TransactionDump", u);
			for(auto i=tr.transaction.mutations.begin(); i!=tr.transaction.mutations.end(); ++i)
				TraceEvent("TransactionMutation", u).detail("T", i->type).detail("P1", i->param1).detail("P2", i->param2);
		}

		if(options.lockAware) {
			tr.flags = tr.flags | CommitTransactionRequest::FLAG_IS_LOCK_AWARE;
		}
		if(options.firstInBatch) {
			tr.flags = tr.flags | CommitTransactionRequest::FLAG_FIRST_IN_BATCH;
		}
		if (options.reportConflictingKeys) {
			tr.transaction.report_conflicting_keys = true;
		}

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
		wait(self->commitMutations());

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
			options.priority = TransactionPriority::IMMEDIATE;
			break;

		case FDBTransactionOptions::PRIORITY_BATCH:
			validateOptionValue(value, false);
			options.priority = TransactionPriority::BATCH;
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
			setOption(FDBTransactionOptions::DEBUG_TRANSACTION_IDENTIFIER, value);
			setOption(FDBTransactionOptions::LOG_TRANSACTION);
			break;

		case FDBTransactionOptions::DEBUG_TRANSACTION_IDENTIFIER:
			validateOptionValue(value, true);

			if (value.get().size() > 100 || value.get().size() == 0) {
				throw invalid_option_value();
			}

			if (trLogInfo) {
				if (trLogInfo->identifier.empty()) {
					trLogInfo->identifier = value.get().printable();
				}
				else if (trLogInfo->identifier != value.get().printable()) {
					TraceEvent(SevWarn, "CannotChangeDebugTransactionIdentifier").detail("PreviousIdentifier", trLogInfo->identifier).detail("NewIdentifier", value.get());
					throw client_invalid_operation();
				}
			}
			else {
				trLogInfo = Reference<TransactionLogInfo>(new TransactionLogInfo(value.get().printable(), TransactionLogInfo::DONT_LOG));
				trLogInfo->maxFieldLength = options.maxTransactionLoggingFieldLength;
			}
			if (info.debugID.present()) {
				TraceEvent(SevInfo, "TransactionBeingTraced")
					.detail("DebugTransactionID", trLogInfo->identifier)
					.detail("ServerTraceID", info.debugID.get().toString());

			}
			break;

		case FDBTransactionOptions::LOG_TRANSACTION:
			validateOptionValue(value, false);
			if (trLogInfo && !trLogInfo->identifier.empty()) {
				trLogInfo->logTo(TransactionLogInfo::TRACE_LOG);
			}
			else {
				TraceEvent(SevWarn, "DebugTransactionIdentifierNotSet").detail("Error", "Debug Transaction Identifier option must be set before logging the transaction");
				throw client_invalid_operation();
			}
			break;

		case FDBTransactionOptions::TRANSACTION_LOGGING_MAX_FIELD_LENGTH:
			validateOptionValue(value, true);
			{
				int maxFieldLength = extractIntOption(value, -1, std::numeric_limits<int32_t>::max());
				if(maxFieldLength == 0) {
					throw invalid_option_value();
				}
				options.maxTransactionLoggingFieldLength = maxFieldLength;
			}
			if(trLogInfo) {
				trLogInfo->maxFieldLength = options.maxTransactionLoggingFieldLength;
			}
			break;

		case FDBTransactionOptions::SERVER_REQUEST_TRACING:
			validateOptionValue(value, false);
			debugTransaction(deterministicRandom()->randomUniqueID());
			if (trLogInfo && !trLogInfo->identifier.empty()) {
				TraceEvent(SevInfo, "TransactionBeingTraced")
					.detail("DebugTransactionID", trLogInfo->identifier)
					.detail("ServerTraceID", info.debugID.get().toString());
			}
			break;

		case FDBTransactionOptions::MAX_RETRY_DELAY:
			validateOptionValue(value, true);
			options.maxBackoff = extractIntOption(value, 0, std::numeric_limits<int32_t>::max()) / 1000.0;
			break;

		case FDBTransactionOptions::SIZE_LIMIT:
			validateOptionValue(value, true);
			options.sizeLimit = extractIntOption(value, 32, CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT);
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

		case FDBTransactionOptions::FIRST_IN_BATCH:
			validateOptionValue(value, false);
			options.firstInBatch = true;
			break;

		case FDBTransactionOptions::USE_PROVISIONAL_PROXIES:
			validateOptionValue(value, false);
			options.getReadVersionFlags |= GetReadVersionRequest::FLAG_USE_PROVISIONAL_PROXIES;
			info.useProvisionalProxies = true;
			break;

		case FDBTransactionOptions::INCLUDE_PORT_IN_ADDRESS:
			validateOptionValue(value, false);
			options.includePort = true;
			break;

		case FDBTransactionOptions::TAG:
			validateOptionValue(value, true);
			options.tags.addTag(value.get());
			break;

		case FDBTransactionOptions::AUTO_THROTTLE_TAG:
			validateOptionValue(value, true);
			options.tags.addTag(value.get());
			options.readTags.addTag(value.get());
			break;

	    case FDBTransactionOptions::REPORT_CONFLICTING_KEYS:
		    validateOptionValue(value, false);
		    options.reportConflictingKeys = true;
		    break;

	    default:
			break;
	}
}

ACTOR Future<GetReadVersionReply> getConsistentReadVersion( DatabaseContext *cx, uint32_t transactionCount, TransactionPriority priority, uint32_t flags, TransactionTagMap<uint32_t> tags, Optional<UID> debugID ) {
	try {
		++cx->transactionReadVersionBatches;
		if( debugID.present() )
			g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "NativeAPI.getConsistentReadVersion.Before");
		loop {
			state GetReadVersionRequest req( transactionCount, priority, flags, tags, debugID );
			choose {
				when ( wait( cx->onMasterProxiesChanged() ) ) {}
				when ( GetReadVersionReply v = wait( basicLoadBalance( cx->getMasterProxies(flags & GetReadVersionRequest::FLAG_USE_PROVISIONAL_PROXIES), &MasterProxyInterface::getConsistentReadVersion, req, cx->taskID ) ) ) {
					if(tags.size() != 0) {
						auto &priorityThrottledTags = cx->throttledTags[priority];
						for(auto& tag : tags) {
							auto itr = v.tagThrottleInfo.find(tag.first);
							if(itr == v.tagThrottleInfo.end()) {
								TEST(true); // Removing client throttle
								priorityThrottledTags.erase(tag.first);
							}
							else {
								TEST(true); // Setting client throttle
								auto result = priorityThrottledTags.try_emplace(tag.first, itr->second);
								if(!result.second) {
									result.first->second.update(itr->second);
								}
							}
						}
					}

					if( debugID.present() )
						g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "NativeAPI.getConsistentReadVersion.After");
					ASSERT( v.version > 0 );
					cx->minAcceptableReadVersion = std::min(cx->minAcceptableReadVersion, v.version);
					return v;
				}
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_broken_promise && e.code() != error_code_batch_transaction_throttled)
			TraceEvent(SevError, "GetConsistentReadVersionError").error(e);
		throw;
	}
}

ACTOR Future<Void> readVersionBatcher( DatabaseContext *cx, FutureStream<DatabaseContext::VersionRequest> versionStream, TransactionPriority priority, uint32_t flags ) {
	state std::vector< Promise<GetReadVersionReply> > requests;
	state PromiseStream< Future<Void> > addActor;
	state Future<Void> collection = actorCollection( addActor.getFuture() );
	state Future<Void> timeout;
	state Optional<UID> debugID;
	state bool send_batch;

	state TransactionTagMap<uint32_t> tags;

	// dynamic batching
	state PromiseStream<double> replyTimes;
	state PromiseStream<Error> _errorStream;
	state double batchTime = 0;
	loop {
		send_batch = false;
		choose {
			when(DatabaseContext::VersionRequest req = waitNext(versionStream)) {
				if (req.debugID.present()) {
					if (!debugID.present()) {
						debugID = nondeterministicRandom()->randomUniqueID();
					}
					g_traceBatch.addAttach("TransactionAttachID", req.debugID.get().first(), debugID.get().first());
				}
				requests.push_back(req.reply);
				for(auto tag : req.tags) {
					++tags[tag];
				}

				if (requests.size() == CLIENT_KNOBS->MAX_BATCH_SIZE)
					send_batch = true;
				else if (!timeout.isValid())
					timeout = delay(batchTime, TaskPriority::GetConsistentReadVersion);
			}
			when(wait(timeout.isValid() ? timeout : Never())) { send_batch = true; }
			// dynamic batching monitors reply latencies
			when(double reply_latency = waitNext(replyTimes.getFuture())) {
				double target_latency = reply_latency * 0.5;
				batchTime = min(0.1 * target_latency + 0.9 * batchTime, CLIENT_KNOBS->GRV_BATCH_TIMEOUT);
			}
			when(wait(collection)) {} // for errors
		}
		if (send_batch) {
			int count = requests.size();
			ASSERT(count);
			// dynamic batching
			Promise<GetReadVersionReply> GRVReply;
			requests.push_back(GRVReply);
			addActor.send(ready(timeReply(GRVReply.getFuture(), replyTimes)));

			Future<Void> batch = incrementalBroadcastWithError(
			    getConsistentReadVersion(cx, count, priority, flags, std::move(tags), std::move(debugID)),
			    std::move(requests), CLIENT_KNOBS->BROADCAST_BATCH_SIZE);

			tags.clear();
			debugID = Optional<UID>();
			requests.clear();
			addActor.send(batch);
			timeout = Future<Void>();
		}
	}
}

ACTOR Future<Version> extractReadVersion(DatabaseContext* cx, TransactionPriority priority, Reference<TransactionLogInfo> trLogInfo, Future<GetReadVersionReply> f, bool lockAware, double startTime, Promise<Optional<Value>> metadataVersion, TagSet tags) {
	GetReadVersionReply rep = wait(f);
	double latency = now() - startTime;
	cx->GRVLatencies.addSample(latency);
	if (trLogInfo)
		trLogInfo->addLog(FdbClientLogEvents::EventGetVersion_V3(startTime, cx->clientLocality.dcId(), latency, priority, rep.version));
	if (rep.version == 1 && rep.locked) {
		throw proxy_memory_limit_exceeded();
	}
	if(rep.locked && !lockAware)
		throw database_locked();

	++cx->transactionReadVersionsCompleted;
	switch(priority) {
		case TransactionPriority::IMMEDIATE:
			++cx->transactionImmediateReadVersionsCompleted;
			break;
		case TransactionPriority::DEFAULT:
			++cx->transactionDefaultReadVersionsCompleted;
			break;
		case TransactionPriority::BATCH:
			++cx->transactionBatchReadVersionsCompleted;
			break;
		default:
			ASSERT(false);
	}

	if(tags.size() != 0) {
		auto &priorityThrottledTags = cx->throttledTags[priority];
		for(auto &tag : tags) {
			auto itr = priorityThrottledTags.find(tag);
			if(itr != priorityThrottledTags.end()) {
				if(itr->second.expired()) {
					priorityThrottledTags.erase(itr);
				}
				else if(itr->second.throttleDuration() > 0) {
					TEST(true); // throttling transaction after getting read version
					++cx->transactionReadVersionsThrottled;
					throw tag_throttled();
				}
			}
		}

		for(auto &tag : tags) {
			auto itr = priorityThrottledTags.find(tag);
			if(itr != priorityThrottledTags.end()) {
				itr->second.addReleased(1);
			}
		}
	}

	if(rep.version > cx->metadataVersionCache[cx->mvCacheInsertLocation].first) {
		cx->mvCacheInsertLocation = (cx->mvCacheInsertLocation + 1) % cx->metadataVersionCache.size();
		cx->metadataVersionCache[cx->mvCacheInsertLocation] = std::make_pair(rep.version, rep.metadataVersion);
	}

	metadataVersion.send(rep.metadataVersion);
	return rep.version;
}

Future<Version> Transaction::getReadVersion(uint32_t flags) {
	if (!readVersion.isValid()) {
		++cx->transactionReadVersions;
		flags |= options.getReadVersionFlags;
		switch(options.priority) {
			case TransactionPriority::IMMEDIATE:
				flags |= GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE;
				++cx->transactionImmediateReadVersions;
				break;
			case TransactionPriority::DEFAULT:
				flags |= GetReadVersionRequest::PRIORITY_DEFAULT;
				++cx->transactionDefaultReadVersions;
				break;	
			case TransactionPriority::BATCH:
				flags |= GetReadVersionRequest::PRIORITY_BATCH;
				++cx->transactionBatchReadVersions;
				break;
			default:
				ASSERT(false);
		}

		if(options.tags.size() != 0) {
			double maxThrottleDelay = 0.0;
			bool canRecheck = false;

			auto &priorityThrottledTags = cx->throttledTags[options.priority];
			for(auto &tag : options.tags) {
				auto itr = priorityThrottledTags.find(tag);
				if(itr != priorityThrottledTags.end()) {
					if(!itr->second.expired()) {
						maxThrottleDelay = std::max(maxThrottleDelay, itr->second.throttleDuration());
						canRecheck = itr->second.canRecheck();
					}
					else {
						priorityThrottledTags.erase(itr);
					}
				}
			}

			if(maxThrottleDelay > 0.0 && !canRecheck) { // TODO: allow delaying?
				TEST(true); // Throttling tag before GRV request
				++cx->transactionReadVersionsThrottled;
				readVersion = tag_throttled();
				return readVersion;
			}
			else {
				TEST(maxThrottleDelay > 0.0); // Rechecking throttle
			}

			for(auto &tag : options.tags) {
				auto itr = priorityThrottledTags.find(tag);
				if(itr != priorityThrottledTags.end()) {
					itr->second.updateChecked();
				}
			}
		}

		auto& batcher = cx->versionBatcher[ flags ];
		if (!batcher.actor.isValid()) {
			batcher.actor = readVersionBatcher( cx.getPtr(), batcher.stream.getFuture(), options.priority, flags );
		}

		auto const req = DatabaseContext::VersionRequest(options.tags, info.debugID);
		batcher.stream.send(req);
		startTime = now();
		readVersion = extractReadVersion( cx.getPtr(), options.priority, trLogInfo, req.reply.getFuture(), options.lockAware, startTime, metadataVersion, options.tags);
	}
	return readVersion;
}

Optional<Version> Transaction::getCachedReadVersion() {
	if (readVersion.isValid() && readVersion.isReady() && !readVersion.isError()) {
		return readVersion.get();
	} else {
		return Optional<Version>();
	}
}

Future<Standalone<StringRef>> Transaction::getVersionstamp() {
	if(committing.isValid()) {
		return transaction_invalid_version();
	}
	return versionstampPromise.getFuture();
}

uint32_t Transaction::getSize() {
	auto s = tr.transaction.mutations.expectedSize() + tr.transaction.read_conflict_ranges.expectedSize() +
	       tr.transaction.write_conflict_ranges.expectedSize();
	return s;
}

Future<Void> Transaction::onError( Error const& e ) {
	if (e.code() == error_code_success) {
		return client_invalid_operation();
	}
	if (e.code() == error_code_not_committed ||
		e.code() == error_code_commit_unknown_result ||
		e.code() == error_code_database_locked ||
		e.code() == error_code_proxy_memory_limit_exceeded ||
		e.code() == error_code_process_behind ||
		e.code() == error_code_batch_transaction_throttled ||
		e.code() == error_code_tag_throttled)
	{
		if(e.code() == error_code_not_committed)
			++cx->transactionsNotCommitted;
		else if (e.code() == error_code_commit_unknown_result)
			++cx->transactionsMaybeCommitted;
		else if (e.code() == error_code_proxy_memory_limit_exceeded)
			++cx->transactionsResourceConstrained;
		else if (e.code() == error_code_process_behind)
			++cx->transactionsProcessBehind;
		else if (e.code() == error_code_batch_transaction_throttled || e.code() == error_code_tag_throttled) {
			++cx->transactionsThrottled;
		}

		double backoff = getBackoff(e.code());
		reset();
		return delay(backoff, info.taskID);
	}
	if (e.code() == error_code_transaction_too_old ||
		e.code() == error_code_future_version)
	{
		if( e.code() == error_code_transaction_too_old )
			++cx->transactionsTooOld;
		else if( e.code() == error_code_future_version )
			++cx->transactionsFutureVersions;

		double maxBackoff = options.maxBackoff;
		reset();
		return delay(std::min(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY, maxBackoff), info.taskID);
	}

	if(g_network->isSimulated() && ++numErrors % 10 == 0)
		TraceEvent(SevWarnAlways, "TransactionTooManyRetries").detail("NumRetries", numErrors);

	return e;
}
ACTOR Future<StorageMetrics> getStorageMetricsLargeKeyRange(Database cx, KeyRangeRef keys);

ACTOR Future<StorageMetrics> doGetStorageMetrics(Database cx, KeyRangeRef keys, Reference<LocationInfo> locationInfo) {
	loop {
		try {
			WaitMetricsRequest req(keys, StorageMetrics(), StorageMetrics());
			req.min.bytes = 0;
			req.max.bytes = -1;
			StorageMetrics m = wait(
			    loadBalance(locationInfo->locations(), &StorageServerInterface::waitMetrics, req, TaskPriority::DataDistribution));
			return m;
		} catch (Error& e) {
			if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
				TraceEvent(SevError, "WaitStorageMetricsError").error(e);
				throw;
			}
			wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
			cx->invalidateCache(keys);
			StorageMetrics m = wait(getStorageMetricsLargeKeyRange(cx, keys));
			return m;
		}
	}
}

ACTOR Future<StorageMetrics> getStorageMetricsLargeKeyRange(Database cx, KeyRangeRef keys) {

	vector<pair<KeyRange, Reference<LocationInfo>>> locations = wait(getKeyRangeLocations(
	    cx, keys, std::numeric_limits<int>::max(), false, &StorageServerInterface::waitMetrics, TransactionInfo(TaskPriority::DataDistribution)));
	state int nLocs = locations.size();
	state vector<Future<StorageMetrics>> fx(nLocs);
	state StorageMetrics total;
	for (int i = 0; i < nLocs; i++) {
		fx[i] = doGetStorageMetrics(cx, locations[i].first, locations[i].second);
	}
	wait(waitForAll(fx));
	for (int i = 0; i < nLocs; i++) {
		total += fx[i].get();
	}
	return total;
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
			StorageMetrics nextX = wait(loadBalance(location->locations(), &StorageServerInterface::waitMetrics, req));
			deltaStream.send(nextX - x);
			x = nextX;
		}
	} catch (Error& e) {
		deltaStream.sendError(e);
		throw e;
	}
}

ACTOR Future<StorageMetrics> waitStorageMetricsMultipleLocations(
    vector<pair<KeyRange, Reference<LocationInfo>>> locations, StorageMetrics min, StorageMetrics max,
    StorageMetrics permittedError) {
	state int nLocs = locations.size();
	state vector<Future<StorageMetrics>> fx(nLocs);
	state StorageMetrics total;
	state PromiseStream<StorageMetrics> deltas;
	state vector<Future<Void>> wx( fx.size() );
	state StorageMetrics halfErrorPerMachine = permittedError * (0.5 / nLocs);
	state StorageMetrics maxPlus = max + halfErrorPerMachine * (nLocs-1);
	state StorageMetrics minMinus = min - halfErrorPerMachine * (nLocs-1);

	for (int i = 0; i < nLocs; i++) {
		WaitMetricsRequest req(locations[i].first, StorageMetrics(), StorageMetrics());
		req.min.bytes = 0;
		req.max.bytes = -1;
		fx[i] = loadBalance(locations[i].second->locations(), &StorageServerInterface::waitMetrics, req,
		                    TaskPriority::DataDistribution);
	}
	wait(waitForAll(fx));

	// invariant: true total is between (total-permittedError/2, total+permittedError/2)
	for (int i = 0; i < nLocs; i++) total += fx[i].get();

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

ACTOR Future< StorageMetrics > extractMetrics( Future<std::pair<Optional<StorageMetrics>, int>> fMetrics ) {
	std::pair<Optional<StorageMetrics>, int> x = wait(fMetrics);
	return x.first.get();
}

ACTOR Future<Standalone<VectorRef<KeyRangeRef>>> getReadHotRanges(Database cx, KeyRange keys) {
	loop {
		int64_t shardLimit = 100; // Shard limit here does not really matter since this function is currently only used
		                          // to find the read-hot sub ranges within a read-hot shard.
		vector<pair<KeyRange, Reference<LocationInfo>>> locations =
		    wait(getKeyRangeLocations(cx, keys, shardLimit, false, &StorageServerInterface::getReadHotRanges,
		                              TransactionInfo(TaskPriority::DataDistribution)));
		try {
			// TODO: how to handle this?
			// This function is called whenever a shard becomes read-hot. But somehow the shard was splitted across more
			// than one storage server after become read-hot and before this function is called, i.e. a race condition.
			// Should we abort and wait the newly splitted shards to be hot again?
			state int nLocs = locations.size();
			// if (nLocs > 1) {
			// 	TraceEvent("RHDDebug")
			// 	    .detail("NumSSIs", nLocs)
			// 	    .detail("KeysBegin", keys.begin.printable().c_str())
			// 	    .detail("KeysEnd", keys.end.printable().c_str());
			// }
			state vector<Future<ReadHotSubRangeReply>> fReplies(nLocs);
			for (int i = 0; i < nLocs; i++) {
				ReadHotSubRangeRequest req(locations[i].first);
				fReplies[i] = loadBalance(locations[i].second->locations(), &StorageServerInterface::getReadHotRanges, req,
				                          TaskPriority::DataDistribution);
			}

			wait(waitForAll(fReplies));
			Standalone<VectorRef<KeyRangeRef>> results;

			for (int i = 0; i < nLocs; i++)
				results.append(results.arena(), fReplies[i].get().readHotRanges.begin(),
				               fReplies[i].get().readHotRanges.size());

			return results;
		} catch (Error& e) {
			if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
				TraceEvent(SevError, "GetReadHotSubRangesError").error(e);
				throw;
			}
			cx->invalidateCache(keys);
			wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
		}
	}
}
	
ACTOR Future< std::pair<Optional<StorageMetrics>, int> > waitStorageMetrics(
	Database cx,
	KeyRange keys,
	StorageMetrics min,
	StorageMetrics max,
	StorageMetrics permittedError,
	int shardLimit,
	int expectedShardCount )
{
	loop {
		vector< pair<KeyRange, Reference<LocationInfo>> > locations = wait( getKeyRangeLocations( cx, keys, shardLimit, false, &StorageServerInterface::waitMetrics, TransactionInfo(TaskPriority::DataDistribution) ) );
		if(expectedShardCount >= 0 && locations.size() != expectedShardCount) {
			return std::make_pair(Optional<StorageMetrics>(), locations.size());
		}

		//SOMEDAY: Right now, if there are too many shards we delay and check again later. There may be a better solution to this.
		if(locations.size() < shardLimit) {
			try {
				Future<StorageMetrics> fx;
				if (locations.size() > 1) {
					fx = waitStorageMetricsMultipleLocations(locations, min, max, permittedError);
				} else {
					WaitMetricsRequest req( keys, min, max );
					fx = loadBalance(locations[0].second->locations(), &StorageServerInterface::waitMetrics, req,
					                 TaskPriority::DataDistribution);
				}
				StorageMetrics x = wait(fx);
				return std::make_pair(x,-1);
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
					TraceEvent(SevError, "WaitStorageMetricsError").error(e);
					throw;
				}
				cx->invalidateCache(keys);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
			}
		} else {
			TraceEvent(SevWarn, "WaitStorageMetricsPenalty")
				.detail("Keys", keys)
				.detail("Limit", CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT)
				.detail("JitteredSecondsOfPenitence", CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY);
			wait(delayJittered(CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY, TaskPriority::DataDistribution));
			// make sure that the next getKeyRangeLocations() call will actually re-fetch the range
			cx->invalidateCache( keys );
		}
	}
}

Future< std::pair<Optional<StorageMetrics>, int> > Transaction::waitStorageMetrics(
	KeyRange const& keys,
	StorageMetrics const& min,
	StorageMetrics const& max,
	StorageMetrics const& permittedError,
	int shardLimit,
	int expectedShardCount )
{
	return ::waitStorageMetrics( cx, keys, min, max, permittedError, shardLimit, expectedShardCount );
}

Future< StorageMetrics > Transaction::getStorageMetrics( KeyRange const& keys, int shardLimit ) {
	if (shardLimit > 0) {
		StorageMetrics m;
		m.bytes = -1;
		return extractMetrics(::waitStorageMetrics(cx, keys, StorageMetrics(), m, StorageMetrics(), shardLimit, -1));
	} else {
		return ::getStorageMetricsLargeKeyRange(cx, keys);
	}
}

ACTOR Future<Standalone<VectorRef<DDMetricsRef>>> waitDataDistributionMetricsList(Database cx, KeyRange keys,
                                                                               int shardLimit) {
	state Future<Void> clientTimeout = delay(5.0);
	loop {
		choose {
			when(wait(cx->onMasterProxiesChanged())) {}
			when(ErrorOr<GetDDMetricsReply> rep =
			         wait(errorOr(basicLoadBalance(cx->getMasterProxies(false), &MasterProxyInterface::getDDMetrics,
			                                  GetDDMetricsRequest(keys, shardLimit))))) {
				if (rep.isError()) {
					throw rep.getError();
				}
				return rep.get().storageMetricsList;
			}
			when(wait(clientTimeout)) { throw timed_out(); }
		}
	}
}

Future<Standalone<VectorRef<KeyRangeRef>>> Transaction::getReadHotRanges(KeyRange const& keys) {
	return ::getReadHotRanges(cx, keys);
}

ACTOR Future< Standalone<VectorRef<KeyRef>> > splitStorageMetrics( Database cx, KeyRange keys, StorageMetrics limit, StorageMetrics estimated )
{
	loop {
		state vector< pair<KeyRange, Reference<LocationInfo>> > locations = wait( getKeyRangeLocations( cx, keys, CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT, false, &StorageServerInterface::splitMetrics, TransactionInfo(TaskPriority::DataDistribution) ) );
		state StorageMetrics used;
		state Standalone<VectorRef<KeyRef>> results;

		//SOMEDAY: Right now, if there are too many shards we delay and check again later. There may be a better solution to this.
		if(locations.size() == CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT) {
			wait(delay(CLIENT_KNOBS->STORAGE_METRICS_TOO_MANY_SHARDS_DELAY, TaskPriority::DataDistribution));
			cx->invalidateCache(keys);
		}
		else {
			results.push_back_deep( results.arena(), keys.begin );
			try {
				//TraceEvent("SplitStorageMetrics").detail("Locations", locations.size());

				state int i = 0;
				for(; i<locations.size(); i++) {
					SplitMetricsRequest req( locations[i].first, limit, used, estimated, i == locations.size() - 1 );
					SplitMetricsReply res =
					    wait(loadBalance(locations[i].second->locations(), &StorageServerInterface::splitMetrics, req,
					                     TaskPriority::DataDistribution));
					if (res.splits.size() &&
					    res.splits[0] <= results.back()) { // split points are out of order, possibly because of moving
						                                   // data, throw error to retry
						ASSERT_WE_THINK(false);   // FIXME: This seems impossible and doesn't seem to be covered by testing
						throw all_alternatives_failed();
					}
					if( res.splits.size() ) {
						results.append( results.arena(), res.splits.begin(), res.splits.size() );
						results.arena().dependsOn( res.splits.arena() );
					}
					used = res.used;

					//TraceEvent("SplitStorageMetricsResult").detail("Used", used.bytes).detail("Location", i).detail("Size", res.splits.size());
				}

				if( used.allLessOrEqual( limit * CLIENT_KNOBS->STORAGE_METRICS_UNFAIR_SPLIT_LIMIT ) ) {
					results.resize(results.arena(), results.size() - 1);
				}

				results.push_back_deep( results.arena(), keys.end );
				return results;
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server && e.code() != error_code_all_alternatives_failed) {
					TraceEvent(SevError, "SplitStorageMetricsError").error(e);
					throw;
				}
				cx->invalidateCache( keys );
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DataDistribution));
			}
		}
	}
}

Future< Standalone<VectorRef<KeyRef>> > Transaction::splitStorageMetrics( KeyRange const& keys, StorageMetrics const& limit, StorageMetrics const& estimated ) {
	return ::splitStorageMetrics( cx, keys, limit, estimated );
}

void Transaction::checkDeferredError() { cx->checkDeferredError(); }

Reference<TransactionLogInfo> Transaction::createTrLogInfoProbabilistically(const Database &cx) {
	if(!cx->isError()) {
		double clientSamplingProbability = std::isinf(cx->clientInfo->get().clientTxnInfoSampleRate) ? CLIENT_KNOBS->CSI_SAMPLING_PROBABILITY : cx->clientInfo->get().clientTxnInfoSampleRate;
		if (((networkOptions.logClientInfo.present() && networkOptions.logClientInfo.get()) || BUGGIFY) && deterministicRandom()->random01() < clientSamplingProbability && (!g_network->isSimulated() || !g_simulator.speedUpSimulation)) {
			return Reference<TransactionLogInfo>(new TransactionLogInfo(TransactionLogInfo::DATABASE));
		}
	}

	return Reference<TransactionLogInfo>();
}

void enableClientInfoLogging() {
	ASSERT(networkOptions.logClientInfo.present() == false);
	networkOptions.logClientInfo = true;
	TraceEvent(SevInfo, "ClientInfoLoggingEnabled");
}

ACTOR Future<Void> snapCreate(Database cx, Standalone<StringRef> snapCmd, UID snapUID) {
	TraceEvent("SnapCreateEnter")
	    .detail("SnapCmd", snapCmd.toString())
	    .detail("UID", snapUID);
	try {
		loop {
			choose {
				when(wait(cx->onMasterProxiesChanged())) {}
				when(wait(basicLoadBalance(cx->getMasterProxies(false), &MasterProxyInterface::proxySnapReq, ProxySnapRequest(snapCmd, snapUID, snapUID), cx->taskID, true /*atmostOnce*/ ))) {
					TraceEvent("SnapCreateExit")
						.detail("SnapCmd", snapCmd.toString())
						.detail("UID", snapUID);
					return Void();
				}
			}
		}
	} catch (Error& e) {
		TraceEvent("SnapCreateError")
			.detail("SnapCmd", snapCmd.toString())
			.detail("UID", snapUID)
			.error(e);
		throw;
	}
}

ACTOR Future<bool> checkSafeExclusions(Database cx, vector<AddressExclusion> exclusions) {
	TraceEvent("ExclusionSafetyCheckBegin")
	    .detail("NumExclusion", exclusions.size())
	    .detail("Exclusions", describe(exclusions));
	state ExclusionSafetyCheckRequest req(exclusions);
	state bool ddCheck;
	try {
		loop {
			choose {
				when(wait(cx->onMasterProxiesChanged())) {}
				when(ExclusionSafetyCheckReply _ddCheck =
				         wait(basicLoadBalance(cx->getMasterProxies(false), &MasterProxyInterface::exclusionSafetyCheckReq,
				                          req, cx->taskID))) {
					ddCheck = _ddCheck.safe;
					break;
				}
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			TraceEvent("ExclusionSafetyCheckError")
			    .detail("NumExclusion", exclusions.size())
			    .detail("Exclusions", describe(exclusions))
			    .error(e);
		}
		throw;
	}
	TraceEvent("ExclusionSafetyCheckCoordinators");
	state ClientCoordinators coordinatorList(cx->getConnectionFile());
	state vector<Future<Optional<LeaderInfo>>> leaderServers;
	for (int i = 0; i < coordinatorList.clientLeaderServers.size(); i++) {
		leaderServers.push_back(retryBrokenPromise(coordinatorList.clientLeaderServers[i].getLeader,
		                                           GetLeaderRequest(coordinatorList.clusterKey, UID()),
		                                           TaskPriority::CoordinationReply));
	}
	// Wait for quorum so we don't dismiss live coordinators as unreachable by acting too fast
	choose {
		when(wait(smartQuorum(leaderServers, leaderServers.size() / 2 + 1, 1.0))) {}
		when(wait(delay(3.0))) {
			TraceEvent("ExclusionSafetyCheckNoCoordinatorQuorum");
			return false;
		}
	}
	int attemptCoordinatorExclude = 0;
	int coordinatorsUnavailable = 0;
	for (int i = 0; i < leaderServers.size(); i++) {
		NetworkAddress leaderAddress =
		    coordinatorList.clientLeaderServers[i].getLeader.getEndpoint().getPrimaryAddress();
		if (leaderServers[i].isReady()) {
			if ((std::count(exclusions.begin(), exclusions.end(),
			                AddressExclusion(leaderAddress.ip, leaderAddress.port)) ||
			     std::count(exclusions.begin(), exclusions.end(), AddressExclusion(leaderAddress.ip)))) {
				attemptCoordinatorExclude++;
			}
		} else {
			coordinatorsUnavailable++;
		}
	}
	int faultTolerance = (leaderServers.size() - 1) / 2 - coordinatorsUnavailable;
	bool coordinatorCheck = (attemptCoordinatorExclude <= faultTolerance);
	TraceEvent("ExclusionSafetyCheckFinish")
	    .detail("CoordinatorListSize", leaderServers.size())
	    .detail("NumExclusions", exclusions.size())
	    .detail("FaultTolerance", faultTolerance)
	    .detail("AttemptCoordinatorExclude", attemptCoordinatorExclude)
	    .detail("CoordinatorCheck", coordinatorCheck)
	    .detail("DataDistributorCheck", ddCheck);

	return (ddCheck && coordinatorCheck);
}
