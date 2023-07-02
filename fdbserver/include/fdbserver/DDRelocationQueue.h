/*
 * DDRelocationQueue.h
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
#ifndef FOUNDATIONDB_DDRELOCATIONQUEUE_H
#define FOUNDATIONDB_DDRELOCATIONQUEUE_H

#include <numeric>

#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/MovingWindow.h"

// send request/signal to DDRelocationQueue through interface
// call synchronous method from components outside DDRelocationQueue
class IDDRelocationQueue {
public:
	virtual int getUnhealthyRelocationCount() const = 0;
	virtual ~IDDRelocationQueue() = default;
	;
};

// DDQueue use RelocateData to track proposed movements
class RelocateData {
	// If this rs comes from a splitting, parent range is the original range.
	Optional<KeyRange> parent_range;

public:
	KeyRange keys;
	int priority;
	int boundaryPriority;
	int healthPriority;
	RelocateReason reason;
	DataMovementReason dmReason;

	double startTime;
	UID randomId; // inherit from RelocateShard.traceId
	UID dataMoveId;
	int workFactor;
	std::vector<UID> src;
	std::vector<UID> completeSources;
	std::vector<UID> completeDests;
	bool wantsNewServers;
	bool cancellable;
	TraceInterval interval;
	std::shared_ptr<DataMove> dataMove;

	RelocateData();
	explicit RelocateData(RelocateShard const& rs);

	static bool isHealthPriority(int priority) {
		return priority == SERVER_KNOBS->PRIORITY_POPULATE_REGION ||
		       priority == SERVER_KNOBS->PRIORITY_TEAM_UNHEALTHY || priority == SERVER_KNOBS->PRIORITY_TEAM_2_LEFT ||
		       priority == SERVER_KNOBS->PRIORITY_TEAM_1_LEFT || priority == SERVER_KNOBS->PRIORITY_TEAM_0_LEFT ||
		       priority == SERVER_KNOBS->PRIORITY_TEAM_REDUNDANT || priority == SERVER_KNOBS->PRIORITY_TEAM_HEALTHY ||
		       priority == SERVER_KNOBS->PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER ||
		       priority == SERVER_KNOBS->PRIORITY_PERPETUAL_STORAGE_WIGGLE;
	}

	static bool isBoundaryPriority(int priority) {
		return priority == SERVER_KNOBS->PRIORITY_SPLIT_SHARD || priority == SERVER_KNOBS->PRIORITY_MERGE_SHARD;
	}

	bool isRestore() const;
	Optional<KeyRange> getParentRange() const;

	bool operator>(const RelocateData& rhs) const;
	bool operator==(const RelocateData& rhs) const;
	bool operator!=(const RelocateData& rhs) const;
};

struct RelocateDecision {
	const RelocateData& rd;
	const std::vector<UID>& destIds;
	const std::vector<UID>& extraIds;
	const StorageMetrics& metrics;
	const Optional<StorageMetrics>& parentMetrics;
};

// DDQueue uses Busyness to throttle too many movement to/from a same server
struct Busyness {
	std::vector<int> ledger;

	Busyness() : ledger(10, 0) {}
	bool canLaunch(int prio, int work) const;
	void addWork(int prio, int work);
	void removeWork(int prio, int work);
	std::string toString();
};

struct DDQueueInitParams {
	UID const& id;
	MoveKeysLock const& lock;
	Reference<IDDTxnProcessor> db;
	std::vector<TeamCollectionInterface> const& teamCollections;
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
	Reference<PhysicalShardCollection> physicalShardCollection;
	PromiseStream<Promise<int64_t>> const& getAverageShardBytes;
	int const& teamSize;
	int const& singleRegionTeamSize;
	PromiseStream<RelocateShard> const& relocationProducer;
	FutureStream<RelocateShard> const& relocationConsumer;
	PromiseStream<GetMetricsRequest> const& getShardMetrics;
	PromiseStream<GetTopKMetricsRequest> const& getTopKMetrics;
};

// DDQueue receives RelocateShard from any other DD components and schedules the actual movements
class DDQueue : public IDDRelocationQueue, public ReferenceCounted<DDQueue> {
	const DDEnabledState* ddEnabledState = nullptr;

public:
	friend struct DDQueueImpl;

	typedef Reference<IDataDistributionTeam> ITeamRef;
	typedef std::pair<ITeamRef, ITeamRef> SrcDestTeamPair;

	struct DDDataMove {
		DDDataMove() = default;
		explicit DDDataMove(UID id) : id(id) {}

		bool isValid() const { return id.isValid(); }

		UID id;
		Future<Void> cancel;
	};

	class ServerCounter {
	public:
		enum CountType : uint8_t { ProposedSource = 0, QueuedSource, LaunchedSource, LaunchedDest, __COUNT };

	private:
		typedef std::array<int, (int)__COUNT> Item; // one for each CountType
		typedef std::array<Item, RelocateReason::typeCount()> ReasonItem; // one for each RelocateReason

		std::unordered_map<UID, ReasonItem> counter;

		std::string toString(const Item& item) const {
			return format("%d %d %d %d", item[0], item[1], item[2], item[3]);
		}

		void traceReasonItem(TraceEvent* event, const ReasonItem& item) const {
			for (int i = 0; i < item.size(); ++i) {
				if (std::accumulate(item[i].cbegin(), item[i].cend(), 0) > 0) {
					// "PQSD" corresponding to CounterType
					event->detail(RelocateReason(i).toString() + "PQSD", toString(item[i]));
				}
			}
		}

		bool countNonZero(const ReasonItem& item, CountType type) const {
			return std::any_of(item.cbegin(), item.cend(), [type](const Item& item) { return item[(int)type] > 0; });
		}

		void increase(const UID& id, RelocateReason reason, CountType type) {
			int idx = (int)(reason);
			// if (idx < 0 || idx >= RelocateReason::typeCount()) {
			// 	TraceEvent(SevWarnAlways, "ServerCounterDebug").detail("Reason", reason.toString());
			// }
			ASSERT(idx >= 0 && idx < RelocateReason::typeCount());
			counter[id][idx][(int)type] += 1;
		}

		void summarizeLaunchedServers(decltype(counter.cbegin()) begin,
		                              decltype(counter.cend()) end,
		                              TraceEvent* event) const {
			if (begin == end)
				return;

			std::string execSrc, execDest;
			for (; begin != end; ++begin) {
				if (countNonZero(begin->second, LaunchedSource)) {
					execSrc += begin->first.shortString() + ",";
				}
				if (countNonZero(begin->second, LaunchedDest)) {
					execDest += begin->first.shortString() + ",";
				}
			}
			event->detail("RemainedLaunchedSources", execSrc).detail("RemainedLaunchedDestinations", execDest);
		}

	public:
		void clear() { counter.clear(); }

		int get(const UID& id, RelocateReason reason, CountType type) const {
			return counter.at(id)[(int)reason][(int)type];
		}

		void increaseForTeam(const std::vector<UID>& ids, RelocateReason reason, CountType type) {
			for (auto& id : ids) {
				increase(id, reason, type);
			}
		}

		void traceAll(const UID& debugId = UID()) const {
			auto it = counter.cbegin();
			int count = 0;
			for (; count < SERVER_KNOBS->DD_QUEUE_COUNTER_MAX_LOG && it != counter.cend(); ++count, ++it) {
				TraceEvent event("DDQueueServerCounter", debugId);
				event.detail("ServerId", it->first);
				traceReasonItem(&event, it->second);
			}

			if (it != counter.cend()) {
				TraceEvent e(SevWarn, "DDQueueServerCounterTooMany", debugId);
				e.detail("Servers", size());
				if (SERVER_KNOBS->DD_QUEUE_COUNTER_SUMMARIZE) {
					summarizeLaunchedServers(it, counter.cend(), &e);
					return;
				}
			}
		}

		size_t size() const { return counter.size(); }

		// for random test
		static CountType randomCountType() {
			int i = deterministicRandom()->randomInt(0, (int)__COUNT);
			return (CountType)i;
		}
	};

	ActorCollectionNoErrors noErrorActors; // has to be the last one to be destroyed because other Actors may use it.
	UID distributorId;
	MoveKeysLock lock;
	// Should always use txnProcessor to access Database object
	Reference<IDDTxnProcessor> txnProcessor;

	std::vector<TeamCollectionInterface> teamCollections;
	Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure;
	Reference<PhysicalShardCollection> physicalShardCollection;
	PromiseStream<Promise<int64_t>> getAverageShardBytes;

	FlowLock startMoveKeysParallelismLock;
	FlowLock finishMoveKeysParallelismLock;
	FlowLock cleanUpDataMoveParallelismLock;
	Reference<FlowLock> fetchSourceLock;

	int activeRelocations;
	int queuedRelocations;
	int64_t bytesWritten;
	int teamSize;
	int singleRegionTeamSize;

	std::map<UID, Busyness> busymap; // UID is serverID
	std::map<UID, Busyness> destBusymap; // UID is serverID

	KeyRangeMap<RelocateData> queueMap;
	std::set<RelocateData, std::greater<RelocateData>> fetchingSourcesQueue;
	std::set<RelocateData, std::greater<RelocateData>> fetchKeysComplete;
	KeyRangeActorMap getSourceActors;
	std::map<UID, std::set<RelocateData, std::greater<RelocateData>>>
	    queue; // Key UID is serverID, value is the serverID's set of RelocateData to relocate
	// The last time one server was selected as source team for read rebalance reason. We want to throttle read
	// rebalance on time bases because the read workload sample update has delay after the previous moving
	std::map<UID, double> lastAsSource;
	ServerCounter serverCounter;

	KeyRangeMap<RelocateData> inFlight;
	// Track all actors that relocates specified keys to a good place; Key: keyRange; Value: actor
	KeyRangeActorMap inFlightActors;
	KeyRangeMap<DDDataMove> dataMoves;

	Promise<Void> error;
	PromiseStream<RelocateData> dataTransferComplete;
	PromiseStream<RelocateData> relocationComplete;
	PromiseStream<RelocateData> fetchSourceServersComplete; // find source SSs for a relocate range

	PromiseStream<RelocateShard> output;
	FutureStream<RelocateShard> input;
	PromiseStream<GetMetricsRequest> getShardMetrics;
	PromiseStream<GetTopKMetricsRequest> getTopKMetrics;
	PromiseStream<Future<Void>> addBackgroundCleanUpDataMoveActor;

	double lastInterval;
	int suppressIntervals;

	Reference<AsyncVar<bool>> rawProcessingUnhealthy; // many operations will remove relocations before adding a new
	                                                  // one, so delay a small time before settling on a new number.
	Reference<AsyncVar<bool>> rawProcessingWiggle;

	std::map<int, int> priority_relocations;
	int unhealthyRelocations;

	Reference<EventCacheHolder> movedKeyServersEventHolder;

	int moveReusePhysicalShard;
	int moveCreateNewPhysicalShard;
	enum RetryFindDstReason {
		None = 0,
		RemoteBestTeamNotReady,
		PrimaryNoHealthyTeam,
		RemoteNoHealthyTeam,
		RemoteTeamIsFull,
		RemoteTeamIsNotHealthy,
		NoAvailablePhysicalShard,
		UnknownForceNew,
		NoAnyHealthy,
		DstOverloaded,
		RetryLimitReached,
		NumberOfTypes,
	};
	std::vector<int> retryFindDstReasonCount;

	MovingWindow<int64_t> moveBytesRate;

	DDQueue() = default;

	void startRelocation(int priority, int healthPriority);

	void finishRelocation(int priority, int healthPriority);

	void validate();

	// This function cannot handle relocation requests which split a shard into three pieces
	void queueRelocation(RelocateShard rs, std::set<UID>& serversToLaunchFrom);

	void completeSourceFetch(const RelocateData& results);

	void logRelocation(const RelocateData& rd, const char* title);

	void launchQueuedWork(KeyRange keys, const DDEnabledState* ddEnabledState);

	void launchQueuedWork(const std::set<UID>& serversToLaunchFrom, const DDEnabledState* ddEnabledState);

	void launchQueuedWork(RelocateData launchData, const DDEnabledState* ddEnabledState);

	// For each relocateData rd in the queue, check if there exist inflight relocate data whose keyrange is overlapped
	// with rd. If there exist, cancel them by cancelling their actors and reducing the src servers' busyness of those
	// canceled inflight relocateData. Launch the relocation for the rd.
	void launchQueuedWork(std::set<RelocateData, std::greater<RelocateData>> combined,
	                      const DDEnabledState* ddEnabledState);

	int getHighestPriorityRelocation() const;

	// return true if the servers are throttled as source for read rebalance
	bool timeThrottle(const std::vector<UID>& ids) const;

	void updateLastAsSource(const std::vector<UID>& ids, double t = now());

	// Schedules cancellation of a data move.
	void enqueueCancelledDataMove(UID dataMoveId, KeyRange range, const DDEnabledState* ddEnabledState);

	Future<Void> periodicalRefreshCounter();

	int getUnhealthyRelocationCount() const override;

	Future<SrcDestTeamPair> getSrcDestTeams(const int& teamCollectionIndex,
	                                        const GetTeamRequest& srcReq,
	                                        const GetTeamRequest& destReq,
	                                        const int& priority,
	                                        TraceEvent* traceEvent);

	Future<bool> rebalanceReadLoad(DataMovementReason moveReason,
	                               Reference<IDataDistributionTeam> sourceTeam,
	                               Reference<IDataDistributionTeam> destTeam,
	                               bool primary,
	                               TraceEvent* traceEvent);

	Future<bool> rebalanceTeams(DataMovementReason moveReason,
	                            Reference<IDataDistributionTeam const> sourceTeam,
	                            Reference<IDataDistributionTeam const> destTeam,
	                            bool primary,
	                            TraceEvent* traceEvent);

	static Future<Void> run(Reference<DDQueue> self,
	                        Reference<AsyncVar<bool>> processingUnhealthy,
	                        Reference<AsyncVar<bool>> processingWiggle,
	                        FutureStream<Promise<int>> getUnhealthyRelocationCount,
	                        const DDEnabledState* ddEnabledState);

	explicit DDQueue(DDQueueInitParams const& params);
};

#endif // FOUNDATIONDB_DDRELOCATIONQUEUE_H
