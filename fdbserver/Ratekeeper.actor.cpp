/*
 * Ratekeeper.actor.cpp
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
#include "flow/IndexedSet.h"
#include "Ratekeeper.h"
#include "fdbrpc/FailureMonitor.h"
#include "Knobs.h"
#include "fdbrpc/Smoother.h"
#include "ServerDBInfo.h"
#include "fdbrpc/simulator.h"

enum limitReason_t {
	unlimited,  // TODO: rename to workload?
	storage_server_write_queue_size,
	storage_server_write_bandwidth_mvcc,
	storage_server_readable_behind,
	log_server_mvcc_write_bandwidth,
	log_server_write_queue,
	storage_server_min_free_space,  // a storage server's normal limits are being reduced by low free space
	storage_server_min_free_space_ratio,  // a storage server's normal limits are being reduced by a low free space ratio
	log_server_min_free_space,
	log_server_min_free_space_ratio,
	limitReason_t_end
};

int limitReasonEnd = limitReason_t_end;

const char* limitReasonName[] = {
	"workload",
	"storage_server_write_queue_size",
	"storage_server_write_bandwidth_mvcc",
	"storage_server_readable_behind",
	"log_server_mvcc_write_bandwidth",
	"log_server_write_queue",
	"storage_server_min_free_space",
	"storage_server_min_free_space_ratio",
	"log_server_min_free_space",
	"log_server_min_free_space_ratio"
};
static_assert(sizeof(limitReasonName) / sizeof(limitReasonName[0]) == limitReason_t_end, "limitReasonDesc table size");

// NOTE: This has a corresponding table in Script.cs (see RatekeeperReason graph)
// IF UPDATING THIS ARRAY, UPDATE SCRIPT.CS!
const char* limitReasonDesc[] = {
	"Workload or read performance.",
	"Storage server performance (storage queue).",
	"Storage server MVCC memory.",
	"Storage server version falling behind.",
	"Log server MVCC memory.",
	"Storage server performance (log queue).",
	"Storage server running out of space (approaching 100MB limit).",
	"Storage server running out of space (approaching 5% limit).",
	"Log server running out of space (approaching 100MB limit).",
	"Log server running out of space (approaching 5% limit).",
};

static_assert(sizeof(limitReasonDesc) / sizeof(limitReasonDesc[0]) == limitReason_t_end, "limitReasonDesc table size");

struct StorageQueueInfo {
	bool valid;
	UID id;
	LocalityData locality;
	StorageQueuingMetricsReply lastReply;
	StorageQueuingMetricsReply prevReply;
	Smoother smoothDurableBytes, smoothInputBytes, verySmoothDurableBytes;
	Smoother smoothDurableVersion, smoothLatestVersion;
	Smoother smoothFreeSpace;
	Smoother smoothTotalSpace;
	double readReplyRate;
	limitReason_t limitReason;
	StorageQueueInfo(UID id, LocalityData locality) : valid(false), id(id), locality(locality), smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
		smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
		smoothDurableVersion(1.), smoothLatestVersion(1.), smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT),
		smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT), readReplyRate(0.0), limitReason(limitReason_t::unlimited)
	{
		// FIXME: this is a tacky workaround for a potential unitialized use in trackStorageServerQueueInfo
		lastReply.instanceID = -1;
	}
};

struct TLogQueueInfo {
	bool valid;
	UID id;
	TLogQueuingMetricsReply lastReply;
	TLogQueuingMetricsReply prevReply;
	Smoother smoothDurableBytes, smoothInputBytes, verySmoothDurableBytes;
	Smoother smoothFreeSpace;
	Smoother smoothTotalSpace;
	TLogQueueInfo(UID id) : valid(false), id(id), smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
		verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT), smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT),
		smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT) {
		// FIXME: this is a tacky workaround for a potential unitialized use in trackTLogQueueInfo (copied from storageQueueInfO)
		lastReply.instanceID = -1;
	}
};

struct Ratekeeper {
	Map<UID, StorageQueueInfo> storageQueueInfo;
	Map<UID, TLogQueueInfo> tlogQueueInfo;
	std::map<UID, std::pair<int64_t, double> > proxy_transactionCountAndTime;
	Smoother smoothReleasedTransactions, smoothTotalDurableBytes;
	double TPSLimit;
	Standalone<StringRef> dbName;
	DatabaseConfiguration configuration;

	Int64MetricHandle tpsLimitMetric;
	Int64MetricHandle actualTpsMetric;
	Int64MetricHandle reasonMetric;
	double lastWarning;
	double* lastLimited;

	Ratekeeper() : smoothReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothTotalDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT), TPSLimit(std::numeric_limits<double>::infinity()),
		tpsLimitMetric(LiteralStringRef("Ratekeeper.TPSLimit")),
		actualTpsMetric(LiteralStringRef("Ratekeeper.ActualTPS")),
		reasonMetric(LiteralStringRef("Ratekeeper.Reason")),
		lastWarning(0)
	{}
};

//SOMEDAY: template trackStorageServerQueueInfo and trackTLogQueueInfo into one function
ACTOR Future<Void> trackStorageServerQueueInfo( Ratekeeper* self, StorageServerInterface ssi ) {
	self->storageQueueInfo.insert( mapPair(ssi.id(), StorageQueueInfo(ssi.id(), ssi.locality) ) );
	state Map<UID, StorageQueueInfo>::iterator myQueueInfo = self->storageQueueInfo.find(ssi.id());
	TraceEvent("RkTracking", ssi.id());
	try {
		loop {
			ErrorOr<StorageQueuingMetricsReply> reply = wait( ssi.getQueuingMetrics.getReplyUnlessFailedFor( StorageQueuingMetricsRequest(), 0, 0 ) ); // SOMEDAY: or tryGetReply?
			if (reply.present()) {
				myQueueInfo->value.valid = true;
				myQueueInfo->value.prevReply = myQueueInfo->value.lastReply;
				myQueueInfo->value.lastReply = reply.get();
				myQueueInfo->value.readReplyRate = reply.get().readReplyRate;
				if (myQueueInfo->value.prevReply.instanceID != reply.get().instanceID) {
					myQueueInfo->value.smoothDurableBytes.reset(reply.get().bytesDurable);
					myQueueInfo->value.verySmoothDurableBytes.reset(reply.get().bytesDurable);
					myQueueInfo->value.smoothInputBytes.reset(reply.get().bytesInput);
					myQueueInfo->value.smoothFreeSpace.reset(reply.get().storageBytes.available);
					myQueueInfo->value.smoothTotalSpace.reset(reply.get().storageBytes.total);
				} else {
					self->smoothTotalDurableBytes.addDelta( reply.get().bytesDurable - myQueueInfo->value.prevReply.bytesDurable );
					myQueueInfo->value.smoothDurableBytes.setTotal( reply.get().bytesDurable );
					myQueueInfo->value.verySmoothDurableBytes.setTotal( reply.get().bytesDurable );
					myQueueInfo->value.smoothInputBytes.setTotal( reply.get().bytesInput );
					myQueueInfo->value.smoothFreeSpace.setTotal( reply.get().storageBytes.available );
					myQueueInfo->value.smoothTotalSpace.setTotal( reply.get().storageBytes.total );
				}
			} else {
				if(myQueueInfo->value.valid) {
					TraceEvent("RkStorageServerDidNotRespond", ssi.id());
				}
				myQueueInfo->value.valid = false;
			}

			Void _ = wait(delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) && IFailureMonitor::failureMonitor().onStateEqual(ssi.getQueuingMetrics.getEndpoint(), FailureStatus(false)));
		}
	} catch (...) {
		// including cancellation
		self->storageQueueInfo.erase( myQueueInfo );
		throw;
	}
}

ACTOR Future<Void> trackTLogQueueInfo( Ratekeeper* self, TLogInterface tli ) {
	self->tlogQueueInfo.insert( mapPair(tli.id(), TLogQueueInfo(tli.id()) ) );
	state Map<UID, TLogQueueInfo>::iterator myQueueInfo = self->tlogQueueInfo.find(tli.id());
	TraceEvent("RkTracking", tli.id());
	try {
		loop {
			ErrorOr<TLogQueuingMetricsReply> reply = wait( tli.getQueuingMetrics.getReplyUnlessFailedFor( TLogQueuingMetricsRequest(), 0, 0 ) );  // SOMEDAY: or tryGetReply?
			if (reply.present()) {
				myQueueInfo->value.valid = true;
				myQueueInfo->value.prevReply = myQueueInfo->value.lastReply;
				myQueueInfo->value.lastReply = reply.get();
				if (myQueueInfo->value.prevReply.instanceID != reply.get().instanceID) {
					myQueueInfo->value.smoothDurableBytes.reset(reply.get().bytesDurable);
					myQueueInfo->value.verySmoothDurableBytes.reset(reply.get().bytesDurable);
					myQueueInfo->value.smoothInputBytes.reset(reply.get().bytesInput);
					myQueueInfo->value.smoothFreeSpace.reset(reply.get().storageBytes.available);
					myQueueInfo->value.smoothTotalSpace.reset(reply.get().storageBytes.total);
				} else {
					self->smoothTotalDurableBytes.addDelta( reply.get().bytesDurable - myQueueInfo->value.prevReply.bytesDurable );
					myQueueInfo->value.smoothDurableBytes.setTotal(reply.get().bytesDurable);
					myQueueInfo->value.verySmoothDurableBytes.setTotal(reply.get().bytesDurable);
					myQueueInfo->value.smoothInputBytes.setTotal(reply.get().bytesInput);
					myQueueInfo->value.smoothFreeSpace.setTotal(reply.get().storageBytes.available);
					myQueueInfo->value.smoothTotalSpace.setTotal(reply.get().storageBytes.total);
				}
			} else {
				if(myQueueInfo->value.valid) {
					TraceEvent("RkTLogDidNotRespond", tli.id());
				}
				myQueueInfo->value.valid = false;
			}

			Void _ = wait(delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) && IFailureMonitor::failureMonitor().onStateEqual(tli.getQueuingMetrics.getEndpoint(), FailureStatus(false)));
		}
	} catch (...) {
		// including cancellation
		self->tlogQueueInfo.erase( myQueueInfo );
		throw;
	}
}

ACTOR Future<Void> splitError( Future<Void> in, Promise<Void> errOut ) {
	try {
		Void _ = wait( in );
		return Void();
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && !errOut.isSet())
			errOut.sendError(e);
		throw;
	}
}

ACTOR Future<Void> trackEachStorageServer(
	Ratekeeper* self,
	FutureStream< std::pair<UID, Optional<StorageServerInterface>> > serverChanges )
{
	state Map<UID, Future<Void>> actors;
	state Promise<Void> err;
	loop choose {
		when (state std::pair< UID, Optional<StorageServerInterface> > change = waitNext(serverChanges) ) {
			Void _ = wait(delay(0)); // prevent storageServerTracker from getting cancelled while on the call stack
			if (change.second.present()) {
				auto& a = actors[ change.first ];
				a = Future<Void>();
				a = splitError( trackStorageServerQueueInfo(self, change.second.get()), err );
			} else
				actors.erase( change.first );
		}
		when (Void _ = wait(err.getFuture())) {}
	}
}

void updateRate( Ratekeeper* self ) {
	//double controlFactor = ;  // dt / eFoldingTime

	double actualTPS = self->smoothReleasedTransactions.smoothRate();
	self->actualTpsMetric = (int64_t)actualTPS;
	// SOMEDAY: Remove the max( 1.0, ... ) since the below calculations _should_ be able to recover back up from this value
	actualTPS = std::max( std::max( 1.0, actualTPS ), self->smoothTotalDurableBytes.smoothRate() / CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT );

	self->TPSLimit = std::numeric_limits<double>::infinity();
	UID reasonID = UID();
	limitReason_t limitReason = limitReason_t::unlimited;

	int sscount = 0;
	double readReplyRateSum=0.0;

	int64_t worstFreeSpaceStorageServer = std::numeric_limits<int64_t>::max();
	int64_t worstStorageQueueStorageServer = 0;
	int64_t limitingStorageQueueStorageServer = 0;

	std::multimap<double, StorageQueueInfo*> storageTPSLimitReverseIndex;

	// Look at each storage server's write queue, compute and store the desired rate ratio
	for(auto i = self->storageQueueInfo.begin(); i != self->storageQueueInfo.end(); ++i) {
		auto& ss = i->value;
		if (!ss.valid) continue;
		++sscount;

		ss.limitReason = limitReason_t::unlimited;

		readReplyRateSum += ss.readReplyRate;

		int64_t minFreeSpace = std::max(SERVER_KNOBS->MIN_FREE_SPACE, (int64_t)(SERVER_KNOBS->MIN_FREE_SPACE_RATIO * ss.smoothTotalSpace.smoothTotal()));

		worstFreeSpaceStorageServer = std::min(worstFreeSpaceStorageServer, (int64_t)ss.smoothFreeSpace.smoothTotal() - minFreeSpace);

		int64_t springBytes = std::max<int64_t>(1, std::min(SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER, (ss.smoothFreeSpace.smoothTotal() - minFreeSpace) * 0.2));
		int64_t targetBytes = std::max<int64_t>(1, std::min(SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER, (int64_t)ss.smoothFreeSpace.smoothTotal() - minFreeSpace));
		if (targetBytes != SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER) {
			if (minFreeSpace == SERVER_KNOBS->MIN_FREE_SPACE) {
				ss.limitReason = limitReason_t::storage_server_min_free_space;
			} else {
				ss.limitReason = limitReason_t::storage_server_min_free_space_ratio;
			}
		}

		int64_t storageQueue = ss.lastReply.bytesInput - ss.smoothDurableBytes.smoothTotal();
		worstStorageQueueStorageServer = std::max(worstStorageQueueStorageServer, storageQueue);
		int64_t b = storageQueue - targetBytes;
		double targetRateRatio = std::min(( b + springBytes ) / (double)springBytes, 2.0);

		double inputRate = ss.smoothInputBytes.smoothRate();
		//inputRate = std::max( inputRate, actualTPS / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE );

		/*if( g_random->random01() < 0.1 ) {
			TraceEvent("RateKeeperUpdateRate", ss.id)
				.detail("MinFreeSpace", minFreeSpace)
				.detail("SpringBytes", springBytes)
				.detail("TargetBytes", targetBytes)
				.detail("SmoothTotalSpaceTotal", ss.smoothTotalSpace.smoothTotal())
				.detail("SmoothFreeSpaceTotal", ss.smoothFreeSpace.smoothTotal())
				.detail("LastReplyBytesInput", ss.lastReply.bytesInput)
				.detail("SmoothDurableBytesTotal", ss.smoothDurableBytes.smoothTotal())
				.detail("TargetRateRatio", targetRateRatio)
				.detail("SmoothInputBytesRate", ss.smoothInputBytes.smoothRate())
				.detail("ActualTPS", actualTPS)
				.detail("InputRate", inputRate)
				.detail("VerySmoothDurableBytesRate", ss.verySmoothDurableBytes.smoothRate())
				.detail("b", b);
		}*/

		// Don't let any storage server use up its target bytes faster than its MVCC window!
		double maxBytesPerSecond = (targetBytes - springBytes) / ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS)/SERVER_KNOBS->VERSIONS_PER_SECOND) + 2.0);
		double limitTPS = std::min(actualTPS * maxBytesPerSecond / std::max(1.0e-8, inputRate), maxBytesPerSecond * SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE);
		if (ss.limitReason == limitReason_t::unlimited)
			ss.limitReason = limitReason_t::storage_server_write_bandwidth_mvcc;

		if (targetRateRatio > 0 && inputRate > 0) {
			ASSERT(inputRate != 0);
			double smoothedRate = std::max( ss.verySmoothDurableBytes.smoothRate(), actualTPS / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE );
			double x =  smoothedRate / (inputRate * targetRateRatio);
			double lim = actualTPS * x;
			if (lim < limitTPS) {
				limitTPS = lim;
				if (ss.limitReason == limitReason_t::unlimited || ss.limitReason == limitReason_t::storage_server_write_bandwidth_mvcc)
					ss.limitReason = limitReason_t::storage_server_write_queue_size;
			}
		}

		storageTPSLimitReverseIndex.insert(std::make_pair(limitTPS, &ss));

		if(limitTPS < self->TPSLimit && (ss.limitReason == limitReason_t::storage_server_min_free_space || ss.limitReason == limitReason_t::storage_server_min_free_space_ratio)) {
			reasonID = ss.id;
			self->TPSLimit = limitTPS;
			limitReason = ss.limitReason;
		}
	}

	std::set<Optional<Standalone<StringRef>>> ignoredMachines;
	for(auto ss = storageTPSLimitReverseIndex.begin(); ss != storageTPSLimitReverseIndex.end() && ss->first < self->TPSLimit; ++ss) {
		if(ignoredMachines.size() < std::min(self->configuration.storageTeamSize - 1, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND)) {
			ignoredMachines.insert(ss->second->locality.zoneId());
			continue;
		}
		if(ignoredMachines.count(ss->second->locality.zoneId()) > 0) {
			continue;
		}

		limitingStorageQueueStorageServer = ss->second->lastReply.bytesInput - ss->second->smoothDurableBytes.smoothTotal();
		self->TPSLimit = ss->first;
		limitReason = storageTPSLimitReverseIndex.begin()->second->limitReason;
		reasonID = storageTPSLimitReverseIndex.begin()->second->id; // Although we aren't controlling based on the worst SS, we still report it as the limiting process

		break;
	}

	double writeToReadLatencyLimit = 0;
	Version worstVersionLag = 0;
	Version limitingVersionLag = 0;

	{
		Version minSSVer = std::numeric_limits<Version>::max();
		Version minLimitingSSVer = std::numeric_limits<Version>::max();
		for(auto i = self->storageQueueInfo.begin(); i != self->storageQueueInfo.end(); ++i) {
			auto& ss = i->value;
			if (!ss.valid) continue;

			minSSVer = std::min(minSSVer, ss.lastReply.v);

			// Machines that ratekeeper isn't controlling can fall arbitrarily far behind
			if(ignoredMachines.count(i->value.locality.zoneId()) == 0) {
				minLimitingSSVer = std::min(minLimitingSSVer, ss.lastReply.v);
			}
		}

		Version maxTLVer = std::numeric_limits<Version>::min();
		for(auto i = self->tlogQueueInfo.begin(); i != self->tlogQueueInfo.end(); ++i) {
			auto& tl = i->value;
			if (!tl.valid) continue;
			maxTLVer = std::max(maxTLVer, tl.lastReply.v);
		}

		// writeToReadLatencyLimit: 0 = infinte speed; 1 = TL durable speed ; 2 = half TL durable speed
		writeToReadLatencyLimit = ((maxTLVer - minLimitingSSVer) - SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE/2) / (SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE/4);
		worstVersionLag = std::max((Version)0, maxTLVer - minSSVer);
		limitingVersionLag = std::max((Version)0, maxTLVer - minLimitingSSVer);
	}

	int64_t worstFreeSpaceTLog = std::numeric_limits<int64_t>::max();
	int64_t worstStorageQueueTLog = 0;
	int tlcount = 0;
	for(auto i = self->tlogQueueInfo.begin(); i != self->tlogQueueInfo.end(); ++i) {
		auto& tl = i->value;
		if (!tl.valid) continue;
		++tlcount;

		limitReason_t tlogLimitReason = limitReason_t::log_server_write_queue;

		int64_t minFreeSpace = std::max( SERVER_KNOBS->MIN_FREE_SPACE, (int64_t)(SERVER_KNOBS->MIN_FREE_SPACE_RATIO * tl.smoothTotalSpace.smoothTotal()));

		worstFreeSpaceTLog = std::min(worstFreeSpaceTLog, (int64_t)tl.smoothFreeSpace.smoothTotal() - minFreeSpace);

		int64_t springBytes = std::max<int64_t>(1, std::min(SERVER_KNOBS->SPRING_BYTES_TLOG, (tl.smoothFreeSpace.smoothTotal() - minFreeSpace) * 0.2));
		int64_t targetBytes = std::max<int64_t>(1, std::min(SERVER_KNOBS->TARGET_BYTES_PER_TLOG, (int64_t)tl.smoothFreeSpace.smoothTotal() - minFreeSpace));
		if (targetBytes != SERVER_KNOBS->TARGET_BYTES_PER_TLOG) {
			if (minFreeSpace == SERVER_KNOBS->MIN_FREE_SPACE) {
				tlogLimitReason = limitReason_t::log_server_min_free_space;
			} else {
				tlogLimitReason = limitReason_t::log_server_min_free_space_ratio;
			}
		}

		int64_t queue = tl.lastReply.bytesInput - tl.smoothDurableBytes.smoothTotal();
		int64_t b = queue - targetBytes;
		worstStorageQueueTLog = std::max(worstStorageQueueTLog, queue);

		if( tl.lastReply.bytesInput - tl.lastReply.bytesDurable > tl.lastReply.storageBytes.free - minFreeSpace / 2 ) {
			if(now() - self->lastWarning > 5.0) {
				self->lastWarning = now();
				TraceEvent(SevWarnAlways, "RkTlogMinFreeSpaceZero").detail("reasonId", tl.id);
			}
			reasonID = tl.id;
			limitReason = limitReason_t::log_server_min_free_space;
			self->TPSLimit = 0.0;
		}

		double targetRateRatio = std::min( ( b + springBytes ) / (double)springBytes, 2.0 );

		if (writeToReadLatencyLimit > targetRateRatio){
			targetRateRatio = writeToReadLatencyLimit;
			tlogLimitReason = limitReason_t::storage_server_readable_behind;
		}

		double inputRate = tl.smoothInputBytes.smoothRate();

		if (targetRateRatio > 0) {
			double smoothedRate = std::max( tl.verySmoothDurableBytes.smoothRate(), actualTPS / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE );
			double x = smoothedRate / (inputRate * targetRateRatio);
			if (targetRateRatio < .75)  //< FIXME: KNOB for 2.0
				x = std::max(x, 0.95);
			double lim = actualTPS * x;
			if (lim < self->TPSLimit){
				self->TPSLimit = lim;
				reasonID = tl.id;
				limitReason = tlogLimitReason;
			}
		}
		if (inputRate > 0) {
			// Don't let any tlogs use up its target bytes faster than its MVCC window!
			double x = ((targetBytes - springBytes) / ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS)/SERVER_KNOBS->VERSIONS_PER_SECOND) + 2.0)) / inputRate;
			double lim = actualTPS * x;
			if (lim < self->TPSLimit){
				self->TPSLimit = lim;
				reasonID = tl.id;
				limitReason = limitReason_t::log_server_mvcc_write_bandwidth;
			}
		}
	}

	self->TPSLimit = std::max(self->TPSLimit, 0.0);

	if(g_network->isSimulated() && g_simulator.speedUpSimulation) {
		self->TPSLimit = std::max(self->TPSLimit, 100.0);
	}

	int64_t totalDiskUsageBytes = 0;
	for(auto & t : self->tlogQueueInfo)
		if (t.value.valid)
			totalDiskUsageBytes += t.value.lastReply.storageBytes.used;
	for(auto & s : self->storageQueueInfo)
		if (s.value.valid)
			totalDiskUsageBytes += s.value.lastReply.storageBytes.used;

	self->tpsLimitMetric = std::min(self->TPSLimit, 1e6);
	self->reasonMetric = limitReason;

	if( self->smoothReleasedTransactions.smoothRate() > SERVER_KNOBS->LAST_LIMITED_RATIO * self->TPSLimit ) {
		(*self->lastLimited) = now();
	}

	if (g_random->random01() < 0.1){
		TraceEvent("RkUpdate")
			.detail("TPSLimit", self->TPSLimit)
			.detail("Reason", limitReason)
			.detail("ReasonServerID", reasonID)
			.detail("ReleasedTPS", self->smoothReleasedTransactions.smoothRate())
			.detail("TPSBasis", actualTPS)
			.detail("StorageServers", sscount)
			.detail("Proxies", self->proxy_transactionCountAndTime.size())
			.detail("TLogs", tlcount)
			.detail("ReadReplyRate", readReplyRateSum)
			.detail("WorstFreeSpaceStorageServer", worstFreeSpaceStorageServer)
			.detail("WorstFreeSpaceTLog", worstFreeSpaceTLog)
			.detail("WorstStorageServerQueue", worstStorageQueueStorageServer)
			.detail("LimitingStorageServerQueue", limitingStorageQueueStorageServer)
			.detail("WorstTLogQueue", worstStorageQueueTLog)
			.detail("TotalDiskUsageBytes", totalDiskUsageBytes)
			.detail("WorstStorageServerVersionLag", worstVersionLag)
			.detail("LimitingStorageServerVersionLag", limitingVersionLag)
			.trackLatest(format("%s/RkUpdate", printable(self->dbName).c_str() ).c_str());
	}
}

ACTOR Future<Void> rateKeeper(
	Reference<AsyncVar<ServerDBInfo>> dbInfo,
	PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > serverChanges,
	FutureStream< struct GetRateInfoRequest > getRateInfo,
	Standalone<StringRef> dbName,
	DatabaseConfiguration configuration,
	double* lastLimited)
{
	state Ratekeeper self;
	state Future<Void> track = trackEachStorageServer( &self, serverChanges.getFuture() );
	state Future<Void> timeout = Void();
	state std::vector<Future<Void>> actors;
	state std::vector<Future<Void>> tlogTrackers;
	state std::vector<TLogInterface> tlogInterfs;
	state Promise<Void> err;
	self.dbName = dbName;
	self.configuration = configuration;
	self.lastLimited = lastLimited;

	TraceEvent("RkTLogQueueSizeParameters").detail("Target", SERVER_KNOBS->TARGET_BYTES_PER_TLOG).detail("Spring", SERVER_KNOBS->SPRING_BYTES_TLOG)
		.detail("Rate", (SERVER_KNOBS->TARGET_BYTES_PER_TLOG - SERVER_KNOBS->SPRING_BYTES_TLOG) / ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) + 2.0));

	TraceEvent("RkStorageServerQueueSizeParameters").detail("Target", SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER).detail("Spring", SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER).detail("EBrake", SERVER_KNOBS->STORAGE_HARD_LIMIT_BYTES)
		.detail("Rate", (SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER - SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER) / ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) + 2.0));

	tlogInterfs = dbInfo->get().logSystemConfig.allPresentLogs();
	for( int i = 0; i < tlogInterfs.size(); i++ )
		tlogTrackers.push_back( splitError( trackTLogQueueInfo(&self, tlogInterfs[i]), err ) );

	loop{
		choose {
			when (Void _ = wait( track )) { break; }
			when (Void _ = wait( timeout )) {
				updateRate( &self );
				double tooOld = now() - 1.0;
				for(auto p=self.proxy_transactionCountAndTime.begin(); p!=self.proxy_transactionCountAndTime.end(); ) {
					if (p->second.second < tooOld)
						p = self.proxy_transactionCountAndTime.erase(p);
					else
						++p;
				}
				timeout = delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE);
			}
			when (GetRateInfoRequest req = waitNext(getRateInfo)) {
				GetRateInfoReply reply;

				auto& p = self.proxy_transactionCountAndTime[ req.requesterID ];
				//TraceEvent("RKMPU", req.requesterID).detail("TRT", req.totalReleasedTransactions).detail("Last", p.first).detail("Delta", req.totalReleasedTransactions - p.first);
				if (p.first > 0)
					self.smoothReleasedTransactions.addDelta( req.totalReleasedTransactions - p.first );

				p.first = req.totalReleasedTransactions;
				p.second = now();

				reply.transactionRate = self.TPSLimit / self.proxy_transactionCountAndTime.size();
				reply.leaseDuration = SERVER_KNOBS->METRIC_UPDATE_RATE;
				req.reply.send( reply );
			}
			when (Void _ = wait(err.getFuture())) {}
			when (Void _ = wait(dbInfo->onChange())) {
				if( tlogInterfs != dbInfo->get().logSystemConfig.allPresentLogs() ) {
					tlogInterfs = dbInfo->get().logSystemConfig.allPresentLogs();
					tlogTrackers = std::vector<Future<Void>>();
					for( int i = 0; i < tlogInterfs.size(); i++ )
						tlogTrackers.push_back( splitError( trackTLogQueueInfo(&self, tlogInterfs[i]), err ) );
				}
			}
		}
	}
	return Void();
}
