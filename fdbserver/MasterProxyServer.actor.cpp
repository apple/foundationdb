/*
 * MasterProxyServer.actor.cpp
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
#include "fdbclient/MasterProxyInterface.h"
#include "fdbclient/NativeAPI.h"
#include "MasterInterface.h"
#include "WorkerInterface.h"
#include "WaitFailure.h"
#include "Knobs.h"
#include "ServerDBInfo.h"
#include "LogSystem.h"
#include "LogSystemDiskQueueAdapter.h"
#include "IKeyValueStore.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/sim_validation.h"
#include "fdbrpc/batcher.actor.h"
#include "fdbclient/Notified.h"
#include "fdbclient/KeyRangeMap.h"
#include "ConflictSet.h"
#include "flow/Stats.h"
#include "ApplyMetadataMutation.h"
#include "RecoveryState.h"
#include "fdbclient/Atomic.h"
#include "flow/TDMetric.actor.h"

struct ProxyStats {
	CounterCollection cc;
	Counter txnStartIn, txnStartOut, txnStartBatch;
	Counter txnSystemPriorityStartIn, txnSystemPriorityStartOut;
	Counter txnBatchPriorityStartIn, txnBatchPriorityStartOut;
	Counter txnDefaultPriorityStartIn, txnDefaultPriorityStartOut;
	Counter txnCommitIn, txnCommitVersionAssigned, txnCommitResolving, txnCommitResolved, txnCommitOut, txnCommitOutSuccess;
	Counter txnConflicts;
	Counter commitBatchIn, commitBatchOut;
	Counter mutationBytes;
	Counter mutations;
	Counter conflictRanges;
	Version lastCommitVersionAssigned;

	Future<Void> logger;

	explicit ProxyStats(UID id, Version* pVersion, NotifiedVersion* pCommittedVersion)
	  : cc("ProxyStats", id.toString()),
		txnStartIn("txnStartIn", cc), txnStartOut("txnStartOut", cc), txnStartBatch("txnStartBatch", cc), txnSystemPriorityStartIn("txnSystemPriorityStartIn", cc), txnSystemPriorityStartOut("txnSystemPriorityStartOut", cc), txnBatchPriorityStartIn("txnBatchPriorityStartIn", cc), txnBatchPriorityStartOut("txnBatchPriorityStartOut", cc),
		txnDefaultPriorityStartIn("txnDefaultPriorityStartIn", cc), txnDefaultPriorityStartOut("txnDefaultPriorityStartOut", cc), txnCommitIn("txnCommitIn", cc),	txnCommitVersionAssigned("txnCommitVersionAssigned", cc), txnCommitResolving("txnCommitResolving", cc), txnCommitResolved("txnCommitResolved", cc), txnCommitOut("txnCommitOut", cc),
		txnCommitOutSuccess("txnCommitOutSuccess", cc), txnConflicts("txnConflicts", cc), commitBatchIn("commitBatchIn", cc), commitBatchOut("commitBatchOut", cc), mutationBytes("mutationBytes", cc), mutations("mutations", cc), conflictRanges("conflictRanges", cc), lastCommitVersionAssigned(0)
	{
		specialCounter(cc, "lastAssignedCommitVersion", [this](){return this->lastCommitVersionAssigned;});
		specialCounter(cc, "version", [pVersion](){return *pVersion; });
		specialCounter(cc, "committedVersion", [pCommittedVersion](){ return pCommittedVersion->get(); });
		logger = traceCounters("ProxyMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ProxyMetrics");
	}
};

ACTOR template <class T>
Future<Void> forwardValue(Promise<T> out, Future<T> in)
{
	// Like forwardPromise, but throws on error
	T t = wait(in);
	out.send(t);
	return Void();
}

int getBytes(Promise<Version> const& r) { return 0; }

ACTOR Future<Void> getRate(UID myID, MasterInterface master, int64_t* inTransactionCount, double* outTransactionRate) {
	state Future<Void> nextRequestTimer = Void();
	state Future<Void> leaseTimeout = Never();
	state Future<GetRateInfoReply> reply;
	state int64_t lastTC = 0;

	loop choose{
		when(Void _ = wait(nextRequestTimer)) {
			nextRequestTimer = Never();
			reply = brokenPromiseToNever(master.getRateInfo.getReply(GetRateInfoRequest(myID, *inTransactionCount)));
		}
		when(GetRateInfoReply rep = wait(reply)) {
			reply = Never();
			*outTransactionRate = rep.transactionRate;
			TraceEvent("MasterProxyRate", myID).detail("Rate", rep.transactionRate).detail("Lease", rep.leaseDuration).detail("ReleasedTransactions", *inTransactionCount - lastTC);
			lastTC = *inTransactionCount;
			leaseTimeout = delay(rep.leaseDuration);
			nextRequestTimer = delayJittered(rep.leaseDuration / 2);
		}
		when(Void _ = wait(leaseTimeout)) {
			*outTransactionRate = 0;
			TraceEvent("MasterProxyRate", myID).detail("Rate", 0).detail("Lease", "Expired");
			leaseTimeout = Never();
		}
	}
}

ACTOR Future<Void> queueTransactionStartRequests(std::priority_queue< std::pair<GetReadVersionRequest, int64_t>,
	std::vector< std::pair<GetReadVersionRequest, int64_t> > > *transactionQueue,
	FutureStream<GetReadVersionRequest> readVersionRequests,
	PromiseStream<Void> GRVTimer, double *lastGRVTime,
	double *GRVBatchTime, FutureStream<double> replyTimes,
	ProxyStats* stats) {
	state int64_t counter = 0;
	loop choose{
		when(GetReadVersionRequest req = waitNext(readVersionRequests)) {
			if (req.debugID.present())
				g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "MasterProxyServer.queueTransactionStartRequests.Before");

			stats->txnStartIn += req.transactionCount;
			if (req.priority() >= GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE)
				stats->txnSystemPriorityStartIn += req.transactionCount;
			else if (req.priority() >= GetReadVersionRequest::PRIORITY_DEFAULT)
				stats->txnDefaultPriorityStartIn += req.transactionCount;
			else
				stats->txnBatchPriorityStartIn += req.transactionCount;

			if (transactionQueue->empty()) {
				if (now() - *lastGRVTime > *GRVBatchTime)
					*lastGRVTime = now() - *GRVBatchTime;

				forwardPromise(GRVTimer, delayJittered(*GRVBatchTime - (now() - *lastGRVTime), TaskProxyGRVTimer));
			}

			transactionQueue->push(std::make_pair(req, counter--));
		}
		// dynamic batching monitors reply latencies
		when(double reply_latency = waitNext(replyTimes)) {
			double target_latency = reply_latency * SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
			*GRVBatchTime = 
				std::max(SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MIN, 
					std::min(SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MAX, 
						target_latency * SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA + *GRVBatchTime * (1-SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA)));
		}
	}
}

ACTOR void discardCommit(UID id, Future<LogSystemDiskQueueAdapter::CommitMessage> fcm, Future<Void> dummyCommitState) {
	ASSERT(!dummyCommitState.isReady());
	LogSystemDiskQueueAdapter::CommitMessage cm = wait(fcm);
	TraceEvent("Discarding", id).detail("count", cm.messages.size());
	cm.acknowledge.send(Void());
	ASSERT(dummyCommitState.isReady());
}

DESCR struct SingleKeyMutation {
	Standalone<StringRef> shardBegin;
	Standalone<StringRef> shardEnd;
	int64_t tag1;
	int64_t tag2;
	int64_t tag3;
};

struct ProxyCommitData {
	UID dbgid;
	ProxyStats stats;
	MasterInterface master;
	vector<ResolverInterface> resolvers;
	LogSystemDiskQueueAdapter* logAdapter;
	Reference<ILogSystem> logSystem;
	IKeyValueStore* txnStateStore;
	NotifiedVersion committedVersion;   // Provided that this recovery has succeeded or will succeed, this version is fully committed (durable)
	Version version;  // The version at which txnStateStore is up to date
	Promise<Void> validState;  // Set once txnStateStore and version are valid
	double lastVersionTime;
	KeyRangeMap<std::set<Key>> vecBackupKeys;
	uint64_t commitVersionRequestNumber;
	uint64_t mostRecentProcessedRequestNumber;
	KeyRangeMap<Deque<std::pair<Version,int>>> keyResolvers;
	KeyRangeMap<ServerCacheInfo> keyInfo;
	std::map<Key, applyMutationsData> uid_applyMutationsData;
	bool firstProxy;
	double lastCoalesceTime;
	bool locked;

	int64_t localCommitBatchesStarted;
	NotifiedVersion latestLocalCommitBatchResolving;
	NotifiedVersion latestLocalCommitBatchLogging;

	PromiseStream<Void> commitBatchStartNotifications;
	PromiseStream<Future<GetCommitVersionReply>> commitBatchVersions;  // 1:1 with commitBatchStartNotifications
	RequestStream<GetReadVersionRequest> getConsistentReadVersion;
	RequestStream<CommitTransactionRequest> commit;
	Database cx;
	EventMetricHandle<SingleKeyMutation> singleKeyMutationEvent;

	std::map<UID, Reference<StorageInfo>> storageCache;

	ProxyCommitData(UID dbgid, MasterInterface master, RequestStream<GetReadVersionRequest> getConsistentReadVersion, Version recoveryTransactionVersion, RequestStream<CommitTransactionRequest> commit, Reference<AsyncVar<ServerDBInfo>> db, bool firstProxy)
		: dbgid(dbgid), stats(dbgid, &version, &committedVersion), master(master), 
			logAdapter(NULL), txnStateStore(NULL),
			committedVersion(recoveryTransactionVersion), version(0), 
			lastVersionTime(0), commitVersionRequestNumber(1), mostRecentProcessedRequestNumber(0),
			getConsistentReadVersion(getConsistentReadVersion), commit(commit), lastCoalesceTime(0),
			localCommitBatchesStarted(0), locked(false), firstProxy(firstProxy),
			cx(openDBOnServer(db, TaskDefaultEndpoint, true, true)), singleKeyMutationEvent(LiteralStringRef("SingleKeyMutation"))
	{}
};

struct ResolutionRequestBuilder {
	ProxyCommitData* self;
	vector<ResolveTransactionBatchRequest> requests;
	vector<vector<int>> transactionResolverMap;
	vector<CommitTransactionRef*> outTr;

	ResolutionRequestBuilder( ProxyCommitData* self, Version version, Version prevVersion, Version lastReceivedVersion) : self(self), requests(self->resolvers.size()) {
		for(auto& req : requests) {
			req.prevVersion = prevVersion;
			req.version = version;
			req.lastReceivedVersion = lastReceivedVersion;
		}
	}

	CommitTransactionRef& getOutTransaction(int resolver, Version read_snapshot) {
		CommitTransactionRef *& out = outTr[resolver];
		if (!out) {
			ResolveTransactionBatchRequest& request = requests[resolver];
			request.transactions.resize(request.arena, request.transactions.size() + 1);
			out = &request.transactions.back();
			out->read_snapshot = read_snapshot;
		}
		return *out;
	}

	void addTransaction(CommitTransactionRef& trIn, int transactionNumberInBatch) {
		// SOMEDAY: There are a couple of unnecessary O( # resolvers ) steps here
		outTr.assign(requests.size(), NULL);
		ASSERT( transactionNumberInBatch >= 0 && transactionNumberInBatch < 32768 );

		bool isTXNStateTransaction = false;
		for (auto & m : trIn.mutations) {
			if (m.type == MutationRef::SetVersionstampedKey) {
				transformSetVersionstampedKey( m, requests[0].version, transactionNumberInBatch );
				trIn.write_conflict_ranges.push_back( requests[0].arena, singleKeyRange( m.param1, requests[0].arena ) );
			} else if (m.type == MutationRef::SetVersionstampedValue ) {
				transformSetVersionstampedValue( m, requests[0].version, transactionNumberInBatch );
			}
			if (isMetadataMutation(m)) {
				isTXNStateTransaction = true;
				getOutTransaction(0, trIn.read_snapshot).mutations.push_back(requests[0].arena, m);
			}
		}
		for(auto& r : trIn.read_conflict_ranges) {
			auto ranges = self->keyResolvers.intersectingRanges( r );
			std::set<int> resolvers;
			for(auto &ir : ranges) {
				auto& version_resolver = ir.value();
				for(int i = version_resolver.size()-1; i >= 0; i--) {
					resolvers.insert(version_resolver[i].second);
					if( version_resolver[i].first < trIn.read_snapshot )
						break;
				}
			}
			ASSERT(resolvers.size());
			for(int resolver : resolvers)
				getOutTransaction( resolver, trIn.read_snapshot ).read_conflict_ranges.push_back( requests[resolver].arena, r );
		}
		for(auto& r : trIn.write_conflict_ranges) {
			auto ranges = self->keyResolvers.intersectingRanges( r );
			std::set<int> resolvers;
			for(auto &ir : ranges)
				resolvers.insert(ir.value().back().second);
			ASSERT(resolvers.size());
			for(int resolver : resolvers)
				getOutTransaction( resolver, trIn.read_snapshot ).write_conflict_ranges.push_back( requests[resolver].arena, r );
		}
		if (isTXNStateTransaction)
			for (int r = 0; r<requests.size(); r++) {
				int transactionNumberInRequest = &getOutTransaction(r, trIn.read_snapshot) - requests[r].transactions.begin();
				requests[r].txnStateTransactions.push_back(requests[r].arena, transactionNumberInRequest);
			}

		vector<int> resolversUsed;
		for (int r = 0; r<outTr.size(); r++)
			if (outTr[r])
				resolversUsed.push_back(r);
		transactionResolverMap.push_back(std::move(resolversUsed));
	}
};

ACTOR Future<Void> commitBatch(
	ProxyCommitData* self,
	vector<CommitTransactionRequest> trs,
	double *commitBatchTime)
{
	state int64_t localBatchNumber = ++self->localCommitBatchesStarted;
	state LogPushData toCommit(self->logSystem);
	state double t1 = now();
	state Optional<UID> debugID;
	state bool forceRecovery = false;

	ASSERT(SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS <= SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT);  // since we are using just the former to limit the number of versions actually in flight!

	self->lastVersionTime = t1;

	++self->stats.commitBatchIn;

	for (int t = 0; t<trs.size(); t++) {
		if (trs[t].debugID.present()) {
			if (!debugID.present())
				debugID = g_nondeterministic_random->randomUniqueID();
			g_traceBatch.addAttach("CommitAttachID", trs[t].debugID.get().first(), debugID.get().first());
		}
	}

	if(localBatchNumber == 2 && !debugID.present() && self->firstProxy && !g_network->isSimulated()) {
		debugID = g_random->randomUniqueID();
		TraceEvent("SecondCommitBatch", self->dbgid).detail("debugID", debugID.get());
	}

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.Before");

	if (trs.empty()) {
		// We are sending an empty batch, so we have to trigger the version fetcher
		self->commitBatchStartNotifications.send(Void());
	}

	/////// Phase 1: Pre-resolution processing (CPU bound except waiting for a version # which is separately pipelined and *should* be available by now (unless empty commit); ordered; currently atomic but could yield)
	TEST(self->latestLocalCommitBatchResolving.get() < localBatchNumber-1); // Queuing pre-resolution commit processing 
	Void _ = wait(self->latestLocalCommitBatchResolving.whenAtLeast(localBatchNumber-1));
	Void _ = wait(yield());

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.GettingCommitVersion");

	Future<GetCommitVersionReply> fVersionReply = waitNext(self->commitBatchVersions.getFuture());
	GetCommitVersionReply versionReply = wait(fVersionReply);
	self->mostRecentProcessedRequestNumber = versionReply.requestNum;

	self->stats.txnCommitVersionAssigned += trs.size();
	self->stats.lastCommitVersionAssigned = versionReply.version;

	state Version commitVersion = versionReply.version;
	state Version prevVersion = versionReply.prevVersion;

	for(auto it : versionReply.resolverChanges) {
		auto rs = self->keyResolvers.modify(it.range);
		for(auto r = rs.begin(); r != rs.end(); ++r)
			r->value().push_back(std::make_pair(versionReply.resolverChangesVersion,it.dest));
	}

	//TraceEvent("ProxyGotVer", self->dbgid).detail("commit", commitVersion).detail("prev", prevVersion);

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.GotCommitVersion");

	ResolutionRequestBuilder requests( self, commitVersion, prevVersion, self->version );
	int conflictRangeCount = 0;
	for (int t = 0; t<trs.size(); t++) {
		requests.addTransaction(trs[t].transaction, t);
		conflictRangeCount += trs[t].transaction.read_conflict_ranges.size() + trs[t].transaction.write_conflict_ranges.size();
		//TraceEvent("MPTransactionDump", self->dbgid).detail("Snapshot", trs[t].transaction.read_snapshot);
		//for(auto& m : trs[t].transaction.mutations)
		//	TraceEvent("MPTransactionsDump", self->dbgid).detail("Mutation", m.toString());
	}
	self->stats.conflictRanges += conflictRangeCount;

	for (int r = 1; r<self->resolvers.size(); r++)
		ASSERT(requests.requests[r].txnStateTransactions.size() == requests.requests[0].txnStateTransactions.size());

	// Sending these requests is the fuzzy border between phase 1 and phase 2; it could conceivably overlap with resolution processing but is still using CPU
	self->stats.txnCommitResolving += trs.size();
	vector< Future<ResolveTransactionBatchReply> > replies;
	for (int r = 0; r<self->resolvers.size(); r++) {
		requests.requests[r].debugID = debugID;
		replies.push_back(brokenPromiseToNever(self->resolvers[r].resolve.getReply(requests.requests[r], TaskProxyResolverReply)));
	}

	state vector<vector<int>> transactionResolverMap = std::move( requests.transactionResolverMap );

	ASSERT(self->latestLocalCommitBatchResolving.get() == localBatchNumber-1);
	self->latestLocalCommitBatchResolving.set(localBatchNumber);

	/////// Phase 2: Resolution (waiting on the network; pipelined)
	state vector<ResolveTransactionBatchReply> resolution = wait( getAll(replies) );

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.AfterResolution");

	////// Phase 3: Post-resolution processing (CPU bound except for very rare situations; ordered; currently atomic but doesn't need to be)
	TEST(self->latestLocalCommitBatchLogging.get() < localBatchNumber-1); // Queuing post-resolution commit processing 
	Void _ = wait(self->latestLocalCommitBatchLogging.whenAtLeast(localBatchNumber-1));
	Void _ = wait(yield());

	self->stats.txnCommitResolved += trs.size();

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.ProcessingMutations");

	state Arena arena;
	state bool isMyFirstBatch = !self->version;
	state Optional<Value> oldCoordinators = self->txnStateStore->readValue(coordinatorsKey).get();

	//TraceEvent("ResolutionResult", self->dbgid).detail("Sequence", sequence).detail("Version", commitVersion).detail("StateMutationProxies", resolution[0].stateMutations.size()).detail("WaitForResolution", now()-t1).detail("R0Committed", resolution[0].committed.size())
	//	.detail("Transactions", trs.size());

	for(int r=1; r<resolution.size(); r++) {
		ASSERT( resolution[r].stateMutations.size() == resolution[0].stateMutations.size() );
		for(int s=0; s<resolution[r].stateMutations.size(); s++)
			ASSERT( resolution[r].stateMutations[s].size() == resolution[0].stateMutations[s].size() );
	}

	// Compute and apply "metadata" effects of each other proxy's most recent batch
	bool initialState = isMyFirstBatch;
	state bool firstStateMutations = isMyFirstBatch;
	state vector< std::pair<Future<LogSystemDiskQueueAdapter::CommitMessage>, Future<Void>> > storeCommits;
	for (int versionIndex = 0; versionIndex < resolution[0].stateMutations.size(); versionIndex++) {
		// self->logAdapter->setNextVersion( ??? );  << Ideally we would be telling the log adapter that the pushes in this commit will be in the version at which these state mutations were committed by another proxy, but at present we don't have that information here.  So the disk queue may be unnecessarily conservative about popping.

		for (int transactionIndex = 0; transactionIndex < resolution[0].stateMutations[versionIndex].size() && !forceRecovery; transactionIndex++) {
			bool committed = true;
			for (int resolver = 0; resolver < resolution.size(); resolver++)
				committed = committed && resolution[resolver].stateMutations[versionIndex][transactionIndex].committed;
			if (committed)
				applyMetadataMutations( self->dbgid, arena, resolution[0].stateMutations[versionIndex][transactionIndex].mutations, self->txnStateStore, NULL, &forceRecovery, self->logSystem, 0, &self->vecBackupKeys, &self->keyInfo, self->firstProxy ? &self->uid_applyMutationsData : NULL, self->commit, self->cx, &self->committedVersion, &self->storageCache );
			
			if( resolution[0].stateMutations[versionIndex][transactionIndex].mutations.size() && firstStateMutations ) {
				ASSERT(committed);
				firstStateMutations = false;
				forceRecovery = false;
			}
			//TraceEvent("MetadataTransaction", self->dbgid).detail("Committed", committed).detail("Mutations", resolution[0].stateMutations[versionIndex][transactionIndex].second.size()).detail("R1Mutations", resolution.back().stateMutations[versionIndex][transactionIndex].second.size());
		}
		//TraceEvent("MetadataBatch", self->dbgid).detail("Transactions", resolution[0].stateMutations[versionIndex].size());

		// These changes to txnStateStore will be committed by the other proxy, so we simply discard the commit message
		auto fcm = self->logAdapter->getCommitMessage();
		storeCommits.push_back(std::make_pair(fcm, self->txnStateStore->commit()));
		//discardCommit( dbgid, fcm, txnStateStore->commit() );

		if (initialState) {
			//TraceEvent("ResyncLog", dbgid);
			initialState = false;
			forceRecovery = false;
			self->txnStateStore->resyncLog();

			for (auto &p : storeCommits) {
				ASSERT(!p.second.isReady());
				p.first.get().acknowledge.send(Void());
				ASSERT(p.second.isReady());
			}
			storeCommits.clear();
		}
	}

	// Determine which transactions actually committed (conservatively) by combining results from the resolvers
	state vector<uint8_t> committed(trs.size());
	ASSERT(transactionResolverMap.size() == committed.size());
	vector<int> nextTr(resolution.size());
	for (int t = 0; t<trs.size(); t++) {
		uint8_t commit = ConflictBatch::TransactionCommitted;
		for (int r : transactionResolverMap[t])
		{
			commit = std::min(resolution[r].committed[nextTr[r]++], commit);
		}
		committed[t] = commit;
	}
	for (int r = 0; r<resolution.size(); r++)
		ASSERT(nextTr[r] == resolution[r].committed.size());

	self->logAdapter->setNextVersion(commitVersion);

	state Optional<Key> lockedKey = self->txnStateStore->readValue(databaseLockedKey).get();
	state bool locked = lockedKey.present() && lockedKey.get().size();

	if(forceRecovery) {
		Void _ = wait( Future<Void>(Never()) );
	}

	// This first pass through committed transactions deals with "metadata" effects (modifications of txnStateStore, changes to storage servers' responsibilities)
	int t;
	state int commitCount = 0;
	for (t = 0; t < trs.size() && !forceRecovery; t++)
	{
		if (committed[t] == ConflictBatch::TransactionCommitted && (!locked || trs[t].isLockAware)) {
			commitCount++;
			applyMetadataMutations(self->dbgid, arena, trs[t].transaction.mutations, self->txnStateStore, &toCommit, &forceRecovery, self->logSystem, commitVersion+1, &self->vecBackupKeys, &self->keyInfo, self->firstProxy ? &self->uid_applyMutationsData : NULL, self->commit, self->cx, &self->committedVersion, &self->storageCache);
		}
		if(firstStateMutations) {
			ASSERT(committed[t] == ConflictBatch::TransactionCommitted);
			firstStateMutations = false;
			forceRecovery = false;
		}
	}
	if (forceRecovery) {
		for (; t<trs.size(); t++)
			committed[t] = ConflictBatch::TransactionConflict;
		TraceEvent(SevWarn, "RestartingTxnSubsystem", self->dbgid).detail("Stage", "AwaitCommit");
	}

	lockedKey = self->txnStateStore->readValue(databaseLockedKey).get();
	state bool lockedAfter = lockedKey.present() && lockedKey.get().size();

	auto fcm = self->logAdapter->getCommitMessage();
	storeCommits.push_back(std::make_pair(fcm, self->txnStateStore->commit()));
	self->version = commitVersion;
	if (!self->validState.isSet()) self->validState.send(Void());
	ASSERT(commitVersion);

	if (!isMyFirstBatch && self->txnStateStore->readValue( coordinatorsKey ).get().get() != oldCoordinators.get()) {
		Void _ = wait( brokenPromiseToNever( self->master.changeCoordinators.getReply( ChangeCoordinatorsRequest( self->txnStateStore->readValue( coordinatorsKey ).get().get() ) ) ) );
		ASSERT(false);   // ChangeCoordinatorsRequest should always throw
	}

	// This second pass through committed transactions assigns the actual mutations to the appropriate storage servers' tags
	int mutationCount = 0, mutationBytes = 0;
	
	state std::map<Key, MutationListRef> logRangeMutations;
	state Arena logRangeMutationsArena;
	state uint32_t v = commitVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;

	for (int t = 0; t<trs.size(); t++) {

		if (committed[t] == ConflictBatch::TransactionCommitted && (!locked || trs[t].isLockAware)) {

			for (auto m : trs[t].transaction.mutations) {
				mutationCount++;
				mutationBytes += m.expectedSize();
				// Determine the set of tags (responsible storage servers) for the mutation, splitting it
				// if necessary.  Serialize (splits of) the mutation into the message buffer and add the tags.
				// FIXME: Make this process not disgustingly CPU intensive

				if (isSingleKeyMutation((MutationRef::Type) m.type)) {
					auto& tags = self->keyInfo[m.param1].tags;
	
					if(self->singleKeyMutationEvent->enabled) {
						KeyRangeRef shard = self->keyInfo.rangeContaining(m.param1).range();
						self->singleKeyMutationEvent->tag1 = (int64_t)tags[0];
						self->singleKeyMutationEvent->tag2 = (int64_t)tags[1];
						self->singleKeyMutationEvent->tag3 = (int64_t)tags[2];
						self->singleKeyMutationEvent->shardBegin = shard.begin;
						self->singleKeyMutationEvent->shardEnd = shard.end;
						self->singleKeyMutationEvent->log();
					}

					if (debugMutation("ProxyCommit", commitVersion, m))
						TraceEvent("ProxyCommitTo", self->dbgid).detail("To", describe(tags)).detail("Mutation", m.toString()).detail("Version", commitVersion);
					for (auto& tag : tags)
						toCommit.addTag(tag);
					toCommit.addTypedMessage(m);
				}
				else if (m.type == MutationRef::ClearRange) {
					auto ranges = self->keyInfo.intersectingRanges(KeyRangeRef(m.param1, m.param2));
					auto firstRange = ranges.begin();
					++firstRange;
					if (firstRange == ranges.end()) {
						// Fast path
						if (debugMutation("ProxyCommit", commitVersion, m))
							TraceEvent("ProxyCommitTo", self->dbgid).detail("To", describe(ranges.begin().value().tags)).detail("Mutation", m.toString()).detail("Version", commitVersion);
						for (auto& tag : ranges.begin().value().tags)
							toCommit.addTag(tag);
					}
					else {
						TEST(true); //A clear range extends past a shard boundary
						std::set<Tag> allSources;
						for (auto r : ranges)
							allSources.insert(r.value().tags.begin(), r.value().tags.end());
						if (debugMutation("ProxyCommit", commitVersion, m))
							TraceEvent("ProxyCommitTo", self->dbgid).detail("To", describe(allSources)).detail("Mutation", m.toString()).detail("Version", commitVersion);
						for (auto& tag : allSources)
							toCommit.addTag(tag);
					}
					toCommit.addTypedMessage(m);
				}
				else
					UNREACHABLE();

				// Check on backing up key, if backup ranges are defined and a normal key
				if  ((self->vecBackupKeys.size() > 1) && normalKeys.contains(m.param1)) {

					if (isAtomicOp((MutationRef::Type)m.type)) {
						// Add the mutation to the relevant backup tag
						for (auto backupName : self->vecBackupKeys[m.param1]) {
							logRangeMutations[backupName].push_back_deep(logRangeMutationsArena, m);
						}
					}
					else {
						switch (m.type)
						{
							// Backup the mutation, if within a backup range
						case MutationRef::Type::SetValue:
							// Add the mutation to the relevant backup tag
							for (auto backupName : self->vecBackupKeys[m.param1]) {
								logRangeMutations[backupName].push_back_deep(logRangeMutationsArena, m);
							}
							break;

						case MutationRef::Type::ClearRange:
							{
								KeyRangeRef mutationRange(m.param1, m.param2);
								KeyRangeRef intersectionRange;

								// Identify and add the intersecting ranges of the mutation to the array of mutations to serialize
								for (auto backupRange : self->vecBackupKeys.intersectingRanges(mutationRange))
								{
									// Get the backup sub range
									const auto&		backupSubrange = backupRange.range();

									// Determine the intersecting range
									intersectionRange = mutationRange & backupSubrange;

									// Create the custom mutation for the specific backup tag
									MutationRef		backupMutation(MutationRef::Type::ClearRange, intersectionRange.begin, intersectionRange.end);

									// Add the mutation to the relevant backup tag
									for (auto backupName : backupRange.value()) {
										logRangeMutations[backupName].push_back_deep(logRangeMutationsArena, backupMutation);
									}
								}
							}
							break;

						default:
							UNREACHABLE();
							break;
						}
					}
				}
			}
		}
	}

	// Serialize and backup the mutations as a single mutation
	if ((self->vecBackupKeys.size() > 1) && logRangeMutations.size()) {

		Key			val;
		MutationRef backupMutation;
		uint32_t*	partBuffer = NULL;

		// Serialize the log range mutations within the map
		for (auto& logRangeMutation : logRangeMutations)
		{
			BinaryWriter wr(Unversioned());

			// Serialize the log destination
			wr.serializeBytes( logRangeMutation.first );

			// Write the log keys and version information
			wr << (uint8_t)hashlittle(&v, sizeof(v), 0);
			wr << bigEndian64(commitVersion);

			backupMutation.type = MutationRef::SetValue;
			partBuffer = NULL;

			val = BinaryWriter::toValue(logRangeMutation.second, IncludeVersion());

			for (int part = 0; part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE < val.size(); part++) {

				// Assign the second parameter as the part
				backupMutation.param2 = val.substr(part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE,
					std::min(val.size() - part * CLIENT_KNOBS->MUTATION_BLOCK_SIZE, CLIENT_KNOBS->MUTATION_BLOCK_SIZE));

				// Write the last part of the mutation to the serialization, if the buffer is not defined
				if (!partBuffer) {
					// Serialize the part to the writer
					wr << bigEndian32(part);

					// Define the last buffer part
					partBuffer = (uint32_t*) ((char*) wr.getData() + wr.getLength() - sizeof(uint32_t));
				}
				else {
					*partBuffer = bigEndian32(part);
				}

				// Define the mutation type and and location
				backupMutation.param1 = wr.toStringRef();
				ASSERT( backupMutation.param1.startsWith(logRangeMutation.first) );  // We are writing into the configured destination
					
				auto& tags = self->keyInfo[backupMutation.param1].tags;
				for (auto& tag : tags)
					toCommit.addTag(tag);
				toCommit.addTypedMessage(backupMutation);

//				if (debugMutation("BackupProxyCommit", commitVersion, backupMutation)) {
//					TraceEvent("BackupProxyCommitTo", self->dbgid).detail("To", describe(tags)).detail("BackupMutation", backupMutation.toString())
//						.detail("BackupMutationSize", val.size()).detail("Version", commitVersion).detail("destPath", printable(logRangeMutation.first))
//						.detail("partIndex", part).detail("partIndexEndian", bigEndian32(part)).detail("partData", printable(backupMutation.param1));
//				}
			}
		}
	}

	self->stats.mutations += mutationCount;
	self->stats.mutationBytes += mutationBytes;

	// Storage servers mustn't make durable versions which are not fully committed (because then they are impossible to roll back)
	// We prevent this by limiting the number of versions which are semi-committed but not fully committed to be less than the MVCC window
	while (self->committedVersion.get() < commitVersion - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) {
		// This should be *extremely* rare in the real world, but knob buggification should make it happen in simulation
		TEST(true);  // Semi-committed pipeline limited by MVCC window
		//TraceEvent("ProxyWaitingForCommitted", self->dbgid).detail("CommittedVersion", self->committedVersion.get()).detail("NeedToCommit", commitVersion);
		choose{
			when(Void _ = wait(self->committedVersion.whenAtLeast(commitVersion - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS))) {
				Void _ = wait(yield());
				break; 
			}
			when(GetReadVersionReply v = wait(self->getConsistentReadVersion.getReply(GetReadVersionRequest(0, GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE | GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY)))) {
				if(v.version > self->committedVersion.get()) {
					self->locked = v.locked;
					self->committedVersion.set(v.version);
				}
				
				if (self->committedVersion.get() < commitVersion - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS)
					Void _ = wait(delay(SERVER_KNOBS->PROXY_SPIN_DELAY));
			}
		}
	}

	LogSystemDiskQueueAdapter::CommitMessage msg = wait(storeCommits.back().first); // Should just be doing yields

	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.AfterStoreCommits");

	// txnState (transaction subsystem state) tag: message extracted from log adapter
	bool firstMessage = true;
	for(auto m : msg.messages) {
		toCommit.addTag(txsTag);
		toCommit.addMessage(StringRef(m.begin(), m.size()), !firstMessage);
		firstMessage = false;
	}

	self->logSystem->pop(msg.popTo, txsTag);

	if ( prevVersion && commitVersion - prevVersion < SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT/2 )
		debug_advanceMaxCommittedVersion( UID(), commitVersion );  //< Is this valid?

	//TraceEvent("ProxyPush", self->dbgid).detail("PrevVersion", prevVersion).detail("Version", commitVersion)
	//	.detail("TransactionsSubmitted", trs.size()).detail("TransactionsCommitted", commitCount)
	//	.detail("txsBytes", msg.message.size()).detail("TxsPopTo", msg.popTo);

	if ( prevVersion && commitVersion - prevVersion < SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT/2 )
		debug_advanceMaxCommittedVersion(UID(), commitVersion);

	Future<Void> loggingComplete = self->logSystem->push( prevVersion, commitVersion, self->committedVersion.get(), toCommit, debugID ) 
		|| self->committedVersion.whenAtLeast( commitVersion+1 );

	if (!forceRecovery) {
		ASSERT(self->latestLocalCommitBatchLogging.get() == localBatchNumber-1);
		self->latestLocalCommitBatchLogging.set(localBatchNumber);
	}

	/////// Phase 4: Logging (network bound; pipelined up to MAX_READ_TRANSACTION_LIFE_VERSIONS (limited by loop above))
	Void _ = wait(loggingComplete);
	Void _ = wait(yield());

	/////// Phase 5: Replies (CPU bound; no particular order required, though ordered execution would be best for latency)	
	if ( prevVersion && commitVersion - prevVersion < SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT/2 )
		debug_advanceMinCommittedVersion(UID(), commitVersion);

	//TraceEvent("ProxyPushed", self->dbgid).detail("PrevVersion", prevVersion).detail("Version", commitVersion);
	if (debugID.present())
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "MasterProxyServer.commitBatch.AfterLogPush");

	for (auto &p : storeCommits) {
		ASSERT(!p.second.isReady());
		p.first.get().acknowledge.send(Void());
		ASSERT(p.second.isReady());
	}

	TEST(self->committedVersion.get() > commitVersion);   // A later version was reported committed first
	if( commitVersion > self->committedVersion.get() ) {
		self->locked = lockedAfter;
		self->committedVersion.set(commitVersion);
	} 

	if (forceRecovery) {
		TraceEvent(SevWarn, "RestartingTxnSubsystem", self->dbgid).detail("Stage", "ProxyShutdown");
		throw worker_removed();
	}

	// Send replies to clients
	for (int t = 0; t < trs.size(); t++)
	{
		if (committed[t] == ConflictBatch::TransactionCommitted && (!locked || trs[t].isLockAware)) {
			ASSERT_WE_THINK(commitVersion != invalidVersion);
			trs[t].reply.send(CommitID(commitVersion, t));
		}
		else if (committed[t] == ConflictBatch::TransactionTooOld)
			trs[t].reply.sendError(transaction_too_old());
		else
			trs[t].reply.sendError(not_committed());
	}

	++self->stats.commitBatchOut;
	self->stats.txnCommitOut += trs.size();
	self->stats.txnConflicts += trs.size() - commitCount;
	self->stats.txnCommitOutSuccess += commitCount;

	if(now() - self->lastCoalesceTime > SERVER_KNOBS->RESOLVER_COALESCE_TIME) {
		self->lastCoalesceTime = now();
		int lastSize = self->keyResolvers.size();
		auto rs = self->keyResolvers.ranges();
		Version oldestVersion = prevVersion - SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS;
		for(auto r = rs.begin(); r != rs.end(); ++r) {
			while(r->value().size() > 1 && r->value()[1].first < oldestVersion)
				r->value().pop_front();
			if(r->value().size() && r->value().front().first < oldestVersion)
				r->value().front().first = 0;
		}
		self->keyResolvers.coalesce(allKeys);
		if(self->keyResolvers.size() != lastSize)
			TraceEvent("KeyResolverSize", self->dbgid).detail("size", self->keyResolvers.size());
	}

	// Dynamic batching for commits
	double target_latency = (now() - t1) * SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
	*commitBatchTime = 
		std::max(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MIN, 
			std::min(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MAX, 
				target_latency * SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA + *commitBatchTime * (1-SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA)));


	return Void();
}


ACTOR Future<GetReadVersionReply> getLiveCommittedVersion(ProxyCommitData* commitData, uint32_t flags, vector<MasterProxyInterface> *otherProxies, Optional<UID> debugID, int transactionCount, int systemTransactionCount, int defaultPriTransactionCount, int batchPriTransactionCount)
{
	// Returns a version which (1) is committed, and (2) is >= the latest version reported committed (by a commit response) when this request was sent
	// (1) The version returned is the committedVersion of some proxy at some point before the request returns, so it is committed.
	// (2) No proxy on our list reported committed a higher version before this request was received, because then its committedVersion would have been higher,
	//     and no other proxy could have already committed anything without first ending the epoch
	++commitData->stats.txnStartBatch;

	state vector<Future<GetReadVersionReply>> proxyVersions;
	for (auto const& p : *otherProxies)
		proxyVersions.push_back(brokenPromiseToNever(p.getRawCommittedVersion.getReply(GetRawCommittedVersionRequest(debugID), TaskTLogConfirmRunningReply)));

	if (!(flags&GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY))
	{
		Void _ = wait(commitData->logSystem->confirmEpochLive(debugID));
	}

	if (debugID.present())
		g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "MasterProxyServer.getLiveCommittedVersion.confirmEpochLive");

	vector<GetReadVersionReply> versions = wait(getAll(proxyVersions));
	GetReadVersionReply rep;
	rep.version = commitData->committedVersion.get();
	rep.locked = commitData->locked;
	
	for (auto v : versions) {
		if(v.version > rep.version) {
			rep = v;
		}
	}

	if (debugID.present())
		g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "MasterProxyServer.getLiveCommittedVersion.After");

	commitData->stats.txnStartOut += transactionCount;
	commitData->stats.txnSystemPriorityStartOut += systemTransactionCount;
	commitData->stats.txnDefaultPriorityStartOut += defaultPriTransactionCount;
	commitData->stats.txnBatchPriorityStartOut += batchPriTransactionCount;

	return rep;
}

ACTOR Future<Void> fetchVersions(ProxyCommitData *commitData) {
	loop {
		Void _ = waitNext(commitData->commitBatchStartNotifications.getFuture());
		GetCommitVersionRequest req(commitData->commitVersionRequestNumber++, commitData->mostRecentProcessedRequestNumber, commitData->dbgid);
		commitData->commitBatchVersions.send(brokenPromiseToNever(commitData->master.getCommitVersion.getReply(req)));
	}
}

ACTOR static Future<Void> transactionStarter(
	MasterProxyInterface proxy,
	MasterInterface master,
	Reference<AsyncVar<ServerDBInfo>> db,
	PromiseStream<Future<Void>> addActor,
	ProxyCommitData* commitData
	)
{
	state double lastGRVTime = 0;
	state PromiseStream<Void> GRVTimer;
	state double GRVBatchTime = SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MIN;

	state int64_t transactionCount = 0;
	state double transactionBudget = 0;
	state double transactionRate = 10;
	state std::priority_queue<std::pair<GetReadVersionRequest, int64_t>, std::vector<std::pair<GetReadVersionRequest, int64_t>>> transactionQueue;
	state vector<MasterProxyInterface> otherProxies;

	state PromiseStream<double> replyTimes;
	addActor.send(getRate(proxy.id(), master, &transactionCount, &transactionRate));
	addActor.send(queueTransactionStartRequests(&transactionQueue, proxy.getConsistentReadVersion.getFuture(), GRVTimer, &lastGRVTime, &GRVBatchTime, replyTimes.getFuture(), &commitData->stats));

	// Get a list of the other proxies that go together with us
	while (std::find(db->get().client.proxies.begin(), db->get().client.proxies.end(), proxy) == db->get().client.proxies.end())
		Void _ = wait(db->onChange());
	for (MasterProxyInterface mp : db->get().client.proxies) {
		if (mp != proxy)
			otherProxies.push_back(mp);
	}

	ASSERT(db->get().recoveryState == RecoveryState::FULLY_RECOVERED);  // else potentially we could return uncommitted read versions (since self->committedVersion is only a committed version if this recovery succeeds)

	TraceEvent("ProxyReadyForTxnStarts", proxy.id());

	loop{
		Void _ = waitNext(GRVTimer.getFuture());
		// Select zero or more transactions to start
		double t = now();
		double elapsed = std::min<double>(now() - lastGRVTime, SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MAX);
		lastGRVTime = t;

		if(elapsed == 0) elapsed = 1e-15; // resolve a possible indeterminant multiplication with infinite transaction rate
		double nTransactionsToStart = std::min(transactionRate * elapsed, SERVER_KNOBS->START_TRANSACTION_MAX_TRANSACTIONS_TO_START) + transactionBudget;

		int transactionsStarted[2] = {0,0};
		int systemTransactionsStarted[2] = {0,0};
		int defaultPriTransactionsStarted[2] = { 0, 0 };
		int batchPriTransactionsStarted[2] = { 0, 0 };

		vector<vector<ReplyPromise<GetReadVersionReply>>> start(2);  // start[0] is transactions starting with !(flags&CAUSAL_READ_RISKY), start[1] is transactions starting with flags&CAUSAL_READ_RISKY
		Optional<UID> debugID;

		double leftToStart = 0;
		while (!transactionQueue.empty()) {
			auto& req = transactionQueue.top().first;
			int tc = req.transactionCount;
			leftToStart = nTransactionsToStart - transactionsStarted[0] - transactionsStarted[1];

			bool startNext = tc < leftToStart || req.priority() >= GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE || tc * g_random->random01() < leftToStart - std::max(0.0, transactionBudget);
			if (!startNext) break;

			if (req.debugID.present()) {
				if (!debugID.present()) debugID = g_nondeterministic_random->randomUniqueID();
				g_traceBatch.addAttach("TransactionAttachID", req.debugID.get().first(), debugID.get().first());
			}
			start[req.flags & 1].push_back(std::move(req.reply));  static_assert(GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY == 1, "Implementation dependent on flag value");

			transactionsStarted[req.flags&1] += tc;
			if (req.priority() >= GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE)
				systemTransactionsStarted[req.flags & 1] += tc;
			else if (req.priority() >= GetReadVersionRequest::PRIORITY_DEFAULT)
				defaultPriTransactionsStarted[req.flags & 1] += tc;
			else
				batchPriTransactionsStarted[req.flags & 1] += tc;

			transactionQueue.pop();
		}

		if (!transactionQueue.empty())
			forwardPromise(GRVTimer, delayJittered(SERVER_KNOBS->START_TRANSACTION_BATCH_QUEUE_CHECK_INTERVAL, TaskProxyGRVTimer));

		/*TraceEvent("GRVBatch", proxy.id())
		.detail("elapsed", elapsed)
		.detail("nTransactionToStart", nTransactionsToStart)
		.detail("transactionRate", transactionRate)
		.detail("transactionQueueSize", transactionQueue.size())
		.detail("numTransactionsStarted", transactionsStarted[0] + transactionsStarted[1]) 
		.detail("numSystemTransactionsStarted", systemTransactionsStarted[0] + systemTransactionsStarted[1])
		.detail("numNonSystemTransactionsStarted", transactionsStarted[0] + transactionsStarted[1] - systemTransactionsStarted[0] - systemTransactionsStarted[1])
		.detail("transactionBudget", transactionBudget)
		.detail("lastLeftToStart", leftToStart);*/

		// dynamic batching
		ReplyPromise<GetReadVersionReply> GRVReply;
		if (start[0].size()){
			start[0].push_back(GRVReply); // for now, base dynamic batching on the time for normal requests (not read_risky)
			addActor.send(timeReply(GRVReply.getFuture(), replyTimes));
		}

		transactionCount += transactionsStarted[0] + transactionsStarted[1];
		transactionBudget = std::max(std::min(nTransactionsToStart - transactionsStarted[0] - transactionsStarted[1], SERVER_KNOBS->START_TRANSACTION_MAX_BUDGET_SIZE), -SERVER_KNOBS->START_TRANSACTION_MAX_BUDGET_SIZE);
		if (debugID.present())
			g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "MasterProxyServer.masterProxyServerCore.Broadcast");
		for (int i = 0; i<start.size(); i++) {
			if (start[i].size()) {
				addActor.send(broadcast(getLiveCommittedVersion(commitData, i, &otherProxies, debugID, transactionsStarted[i], systemTransactionsStarted[i], defaultPriTransactionsStarted[i], batchPriTransactionsStarted[i]), start[i]));
			}
		}
	}
}

ACTOR static Future<Void> readRequestServer(
	MasterProxyInterface proxy,
	ProxyCommitData* commitData
	)
{
	// Implement read-only parts of the proxy interface

	// We can't respond to these requests until we have valid txnStateStore
	Void _ = wait(commitData->validState.getFuture());

	TraceEvent("ProxyReadyForReads", proxy.id());

	loop {
		choose{
			when(GetKeyServerLocationsRequest req = waitNext(proxy.getKeyServersLocations.getFuture())) {
				GetKeyServerLocationsReply rep;
				if(!req.end.present()) {
					auto r = req.reverse ? commitData->keyInfo.rangeContainingKeyBefore(req.begin) : commitData->keyInfo.rangeContaining(req.begin);
					vector<StorageServerInterface> ssis;
					ssis.reserve(r.value().info.size());
					for(auto& it : r.value().info) {
						ssis.push_back(it->interf);
					}
					rep.results.push_back(std::make_pair(r.range(), ssis));
				} else if(!req.reverse) {
					int count = 0;
					for(auto r = commitData->keyInfo.rangeContaining(req.begin); r != commitData->keyInfo.ranges().end() && count < req.limit && r.begin() < req.end.get(); ++r) {
						vector<StorageServerInterface> ssis;
						ssis.reserve(r.value().info.size());
						for(auto& it : r.value().info) {
							ssis.push_back(it->interf);
						}
						rep.results.push_back(std::make_pair(r.range(), ssis));
						count++;
					}
				} else {
					int count = 0;
					auto r = commitData->keyInfo.rangeContainingKeyBefore(req.end.get());
					while( count < req.limit && req.begin < r.end() ) {
						vector<StorageServerInterface> ssis;
						ssis.reserve(r.value().info.size());
						for(auto& it : r.value().info) {
							ssis.push_back(it->interf);
						}
						rep.results.push_back(std::make_pair(r.range(), ssis));
						if(r == commitData->keyInfo.ranges().begin()) {
							break;
						}
						count++;
						--r;
					}
				}
				req.reply.send(rep);
			}
			when(GetStorageServerRejoinInfoRequest req = waitNext(proxy.getStorageServerRejoinInfo.getFuture())) {
				if (commitData->txnStateStore->readValue(serverListKeyFor(req.id)).get().present()) {
					GetStorageServerRejoinInfoReply rep;
					rep.version = commitData->version;
					rep.tag = decodeServerTagValue( commitData->txnStateStore->readValue(serverTagKeyFor(req.id)).get().get() );
					req.reply.send(rep);
				} else
					req.reply.sendError(worker_removed());
			}
		}
		Void _ = wait(yield());
	}
}

ACTOR Future<Void> masterProxyServerCore(
	MasterProxyInterface proxy,
	MasterInterface master,
	Reference<AsyncVar<ServerDBInfo>> db,
	LogEpoch epoch,
	Version recoveryTransactionVersion,
	bool firstProxy)
{
	state ProxyCommitData commitData(proxy.id(), master, proxy.getConsistentReadVersion, recoveryTransactionVersion, proxy.commit, db, firstProxy);

	state Future<Sequence> sequenceFuture = (Sequence)0;
	state PromiseStream< vector<CommitTransactionRequest> > batchedCommits;
	state Future<Void> commitBatcher;

	state Future<Void> lastCommitComplete = Void();

	state PromiseStream<Future<Void>> addActor;
	state Future<Void> onError = actorCollection(addActor.getFuture());
	state double lastCommit = 0;
	state std::set<Sequence> txnSequences;
	state Sequence maxSequence = std::numeric_limits<Sequence>::max();
	state double commitBatchInterval = SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_MIN;

	addActor.send( fetchVersions(&commitData) );
	addActor.send( waitFailureServer(proxy.waitFailure.getFuture()) );

	//TraceEvent("ProxyInit1", proxy.id());

	// Wait until we can load the "real" logsystem, since we don't support switching them currently
	while (!(db->get().master.id() == master.id() && db->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION)) {
		//TraceEvent("ProxyInit2", proxy.id()).detail("LSEpoch", db->get().logSystemConfig.epoch).detail("Need", epoch);
		Void _ = wait(db->onChange());
	}

	//TraceEvent("ProxyInit3", proxy.id());

	commitData.resolvers = db->get().resolvers;
	ASSERT(commitData.resolvers.size() != 0);

	auto rs = commitData.keyResolvers.modify(allKeys);
	for(auto r = rs.begin(); r != rs.end(); ++r)
		r->value().push_back(std::make_pair<Version,int>(0,0));

	commitData.logSystem = ILogSystem::fromServerDBInfo(proxy.id(), db->get());
	commitData.logAdapter = new LogSystemDiskQueueAdapter(commitData.logSystem, txsTag, false);
	commitData.txnStateStore = keyValueStoreLogSystem(commitData.logAdapter, proxy.id(), 2e9, true);
	onError = onError || commitData.logSystem->onError();

	addActor.send(transactionStarter(proxy, master, db, addActor, &commitData));
	addActor.send(readRequestServer(proxy, &commitData));

	// wait for txnStateStore recovery
	Optional<Value> _ = wait(commitData.txnStateStore->readValue(StringRef()));

	int commitBatchByteLimit = 
		(int)std::min<double>(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_MAX, 
			std::max<double>(SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_MIN, 
				SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_SCALE_BASE * pow(db->get().client.proxies.size(), SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_BYTES_SCALE_POWER)));
	commitBatcher = batcher(batchedCommits, proxy.commit.getFuture(), SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_INTERVAL_FROM_IDLE, &commitBatchInterval, SERVER_KNOBS->MAX_COMMIT_BATCH_INTERVAL, SERVER_KNOBS->COMMIT_TRANSACTION_BATCH_COUNT_MAX, commitBatchByteLimit, CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT, commitData.commitBatchStartNotifications, TaskProxyCommitBatcher, &commitData.stats.txnCommitIn);
	loop choose{
		when(Void _ = wait(onError)) {}
		when(vector<CommitTransactionRequest> trs = waitNext(batchedCommits.getFuture())) {
			//TraceEvent("MasterProxyCTR", proxy.id()).detail("CommitTransactions", trs.size()).detail("TransactionRate", transactionRate).detail("TransactionQueue", transactionQueue.size()).detail("ReleasedTransactionCount", transactionCount);
			if (trs.size() || (db->get().recoveryState == RecoveryState::FULLY_RECOVERED && now() - lastCommit >= SERVER_KNOBS->MAX_COMMIT_BATCH_INTERVAL)) {
				lastCommit = now();

				if (trs.size() || lastCommitComplete.isReady()) {
					lastCommitComplete = commitBatch(&commitData, trs, &commitBatchInterval);
					addActor.send(lastCommitComplete);
				}
			}
		}
		when(GetRawCommittedVersionRequest req = waitNext(proxy.getRawCommittedVersion.getFuture())) {
			//TraceEvent("ProxyGetRCV", proxy.id());
			if (req.debugID.present())
				g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "MasterProxyServer.masterProxyServerCore.GetRawCommittedVersion");
			GetReadVersionReply rep;
			rep.locked = commitData.locked;
			rep.version = commitData.committedVersion.get();
			req.reply.send(rep);
		}
		when(TxnStateRequest req = waitNext(proxy.txnState.getFuture())) {
			state ReplyPromise<Void> reply = req.reply;
			if(req.last) maxSequence = req.sequence + 1;
			if (!txnSequences.count(req.sequence)) {
				txnSequences.insert(req.sequence);
				
				ASSERT(!commitData.validState.isSet()); // Although we may receive the CommitTransactionRequest for the recovery transaction before all of the TxnStateRequest, we will not get a resolution result from any resolver until the master has submitted its initial (sequence 0) resolution request, which it doesn't do until we have acknowledged all TxnStateRequests

				for(auto& kv : req.data)
					commitData.txnStateStore->set(kv, &req.arena);
				commitData.txnStateStore->commit(true);

				if(txnSequences.size() == maxSequence) {
					state KeyRange txnKeys = allKeys;
					loop {
						Void _ = wait(yield());
						Standalone<VectorRef<KeyValueRef>> data = commitData.txnStateStore->readRange(txnKeys, SERVER_KNOBS->BUGGIFIED_ROW_LIMIT, SERVER_KNOBS->APPLY_MUTATION_BYTES).get();
						if(!data.size()) break;
						((KeyRangeRef&)txnKeys) = KeyRangeRef( keyAfter(data.back().key, txnKeys.arena()), txnKeys.end );

						Standalone<VectorRef<MutationRef>> mutations;
						std::vector<std::pair<MapPair<Key,ServerCacheInfo>,int>> keyInfoData;
						vector<UID> src, dest;
						Reference<StorageInfo> storageInfo;
						ServerCacheInfo info;
						for(auto &kv : data) {
							if( kv.key.startsWith(keyServersPrefix) ) {
								KeyRef k = kv.key.removePrefix(keyServersPrefix);
								if(k != allKeys.end) {
									decodeKeyServersValue(kv.value, src, dest);
									info.tags.clear();
									info.info.clear();
									for(auto& id : src) {
										auto cacheItr = commitData.storageCache.find(id);
										if(cacheItr == commitData.storageCache.end()) {
											storageInfo = Reference<StorageInfo>( new StorageInfo() );
											storageInfo->tag = decodeServerTagValue( commitData.txnStateStore->readValue( serverTagKeyFor(id) ).get().get() );
											storageInfo->interf = decodeServerListValue( commitData.txnStateStore->readValue( serverListKeyFor(id) ).get().get() );
											commitData.storageCache[id] = storageInfo;
										} else {
											storageInfo = cacheItr->second;
										}
										ASSERT(storageInfo->tag != invalidTag);
										info.tags.push_back( storageInfo->tag );
										info.info.push_back( storageInfo );
									}
									for(auto& id : dest) {
										auto cacheItr = commitData.storageCache.find(id);
										if(cacheItr == commitData.storageCache.end()) {
											storageInfo = Reference<StorageInfo>( new StorageInfo() );
											storageInfo->tag = decodeServerTagValue( commitData.txnStateStore->readValue( serverTagKeyFor(id) ).get().get() );
											storageInfo->interf = decodeServerListValue( commitData.txnStateStore->readValue( serverListKeyFor(id) ).get().get() );
											commitData.storageCache[id] = storageInfo;
										} else {
											storageInfo = cacheItr->second;
										}
										ASSERT(storageInfo->tag != invalidTag);
										info.tags.push_back( storageInfo->tag );
									}
									uniquify(info.tags);
									keyInfoData.push_back( std::make_pair(MapPair<Key,ServerCacheInfo>(k, info), 1) );
								}
							} else {
								mutations.push_back(mutations.arena(), MutationRef(MutationRef::SetValue, kv.key, kv.value));
							}
						}
						
						//insert keyTag data separately from metadata mutations so that we can do one bulk insert which avoids a lot of map lookups.
						commitData.keyInfo.rawInsert(keyInfoData); 

						Arena arena;
						bool confChanges;
						applyMetadataMutations(commitData.dbgid, arena, mutations, commitData.txnStateStore, NULL, &confChanges, Reference<ILogSystem>(), 0, &commitData.vecBackupKeys, &commitData.keyInfo, commitData.firstProxy ? &commitData.uid_applyMutationsData : NULL, commitData.commit, commitData.cx, &commitData.committedVersion, &commitData.storageCache, true);
					}

					auto lockedKey = commitData.txnStateStore->readValue(databaseLockedKey).get();
					commitData.locked = lockedKey.present() && lockedKey.get().size();

					commitData.txnStateStore->enableSnapshot();
				}
			}
			reply.send(Void());
			Void _ = wait(yield());
		}
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo>> db, uint64_t recoveryCount, MasterProxyInterface myInterface) {
	loop{
		if (db->get().recoveryCount >= recoveryCount && !std::count(db->get().client.proxies.begin(), db->get().client.proxies.end(), myInterface))
		throw worker_removed();
		Void _ = wait(db->onChange());
	}
}

ACTOR Future<Void> masterProxyServer(
	MasterProxyInterface proxy,
	InitializeMasterProxyRequest req,
	Reference<AsyncVar<ServerDBInfo>> db)
{
	try {
		state Future<Void> core = masterProxyServerCore(proxy, req.master, db, req.recoveryCount, req.recoveryTransactionVersion, req.firstProxy);
		loop choose{
			when(Void _ = wait(core)) { return Void(); }
			when(Void _ = wait(checkRemoved(db, req.recoveryCount, proxy))) {}
		}
	}
	catch (Error& e) {
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_worker_removed || e.code() == error_code_tlog_stopped ||
			e.code() == error_code_master_tlog_failed || e.code() == error_code_coordinators_changed || e.code() == error_code_coordinated_state_conflict ||
			e.code() == error_code_new_coordinators_timed_out)
		{
			TraceEvent("MasterProxyTerminated", proxy.id()).error(e, true);
			return Void();
		}
		throw;
	}
}
