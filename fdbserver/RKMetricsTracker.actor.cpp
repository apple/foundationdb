/**
 * RKMetricsTracker.actor.cpp
 */

#include "fdbserver/IRKMetricsTracker.h"
#include "fdbserver/Knobs.h"

ACTOR static Future<Void> splitError(Future<Void> in, Promise<Void> errOut) {
	try {
		wait(in);
		return Void();
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && !errOut.isSet())
			errOut.sendError(e);
		throw;
	}
}

class RKMetricsTrackerImpl {
public:
	ACTOR static Future<Void> monitorServerListChange(RKMetricsTracker* self) {
		state std::map<UID, StorageServerInterface> oldServers;
		state Transaction tr(self->db);

		loop {
			try {
				if (now() - self->lastSSListFetchedTimestamp > 2 * SERVER_KNOBS->SERVER_LIST_DELAY) {
					TraceEvent(SevWarnAlways, "RatekeeperGetSSListLongLatency", self->ratekeeperId)
					    .detail("Latency", now() - self->lastSSListFetchedTimestamp);
				}
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				std::vector<std::pair<StorageServerInterface, ProcessClass>> results =
				    wait(NativeAPI::getServerListAndProcessClasses(&tr));
				self->lastSSListFetchedTimestamp = now();

				std::map<UID, StorageServerInterface> newServers;
				for (const auto& [ssi, _] : results) {
					const UID serverId = ssi.id();
					newServers[serverId] = ssi;

					if (oldServers.count(serverId)) {
						if (ssi.getValue.getEndpoint() != oldServers[serverId].getValue.getEndpoint() ||
						    ssi.isAcceptingRequests() != oldServers[serverId].isAcceptingRequests()) {
							self->serverChanges.send(std::make_pair(serverId, Optional<StorageServerInterface>(ssi)));
						}
						oldServers.erase(serverId);
					} else {
						self->serverChanges.send(std::make_pair(serverId, Optional<StorageServerInterface>(ssi)));
					}
				}

				for (const auto& it : oldServers) {
					self->serverChanges.send(std::make_pair(it.first, Optional<StorageServerInterface>()));
				}

				oldServers.swap(newServers);
				tr = Transaction(self->db);
				wait(delay(SERVER_KNOBS->SERVER_LIST_DELAY));
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent("RatekeeperGetSSListError", self->ratekeeperId).errorUnsuppressed(e).suppressFor(1.0);
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> trackStorageServerQueueInfo(RKMetricsTracker* self, StorageServerInterface ssi) {
		self->storageQueueInfo.insert(mapPair(ssi.id(), StorageQueueInfo(self->ratekeeperId, ssi.id(), ssi.locality)));
		TraceEvent("RkTracking", self->ratekeeperId)
		    .detail("StorageServer", ssi.id())
		    .detail("Locality", ssi.locality.toString());
		try {
			loop {
				ErrorOr<StorageQueuingMetricsReply> reply = wait(ssi.getQueuingMetrics.getReplyUnlessFailedFor(
				    StorageQueuingMetricsRequest(), 0, 0)); // SOMEDAY: or tryGetReply?
				Map<UID, StorageQueueInfo>::iterator myQueueInfo = self->storageQueueInfo.find(ssi.id());
				if (reply.present()) {
					myQueueInfo->value.update(reply.get(), self->smoothTotalDurableBytes);
					myQueueInfo->value.acceptingRequests = ssi.isAcceptingRequests();
				} else {
					if (myQueueInfo->value.valid) {
						TraceEvent("RkStorageServerDidNotRespond", self->ratekeeperId)
						    .detail("StorageServer", ssi.id());
					}
					myQueueInfo->value.valid = false;
				}

				wait(delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) &&
				     IFailureMonitor::failureMonitor().onStateEqual(ssi.getQueuingMetrics.getEndpoint(),
				                                                    FailureStatus(false)));
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				self->storageQueueInfo.erase(ssi.id());
				self->storageServerInterfaces.erase(ssi.id());
			}
			throw e;
		}
	}

	ACTOR static Future<Void> trackEachStorageServer(RKMetricsTracker* self) {
		state std::unordered_map<UID, Future<Void>> storageServerTrackers;
		state Promise<Void> err;

		loop choose {
			when(state std::pair<UID, Optional<StorageServerInterface>> change =
			         waitNext(self->serverChanges.getFuture())) {
				wait(delay(0)); // prevent storageServerTracker from getting cancelled while on the call stack

				const UID& id = change.first;
				if (change.second.present()) {
					if (!change.second.get().isTss()) {

						auto& a = storageServerTrackers[id];
						a = Future<Void>();
						a = splitError(trackStorageServerQueueInfo(self, change.second.get()), err);

						self->storageServerInterfaces[id] = change.second.get();
					}
				} else {
					storageServerTrackers.erase(id);
					self->storageServerInterfaces.erase(id);
					self->storageQueueInfo.erase(id);
				}
			}
			when(wait(err.getFuture())) {}
		}
	}

	ACTOR static Future<Void> refreshStorageServerCommitCosts(RKMetricsTracker* self) {
		state double lastBusiestCommitTagPick;
		state std::vector<Future<Void>> replies;
		loop {
			lastBusiestCommitTagPick = now();
			wait(delay(SERVER_KNOBS->TAG_MEASUREMENT_INTERVAL));

			replies.clear();

			double elapsed = now() - lastBusiestCommitTagPick;
			// for each SS, select the busiest commit tag from ssTrTagCommitCost
			for (auto& [ssId, ssQueueInfo] : self->storageQueueInfo) {
				// NOTE: In some cases, for unknown reason SS will not respond to the updateCommitCostRequest. Since the
				// information is not time-sensitive, we do not wait for the replies.
				replies.push_back(self->storageServerInterfaces[ssId].updateCommitCostRequest.getReply(
				    ssQueueInfo.refreshCommitCost(elapsed)));
			}
		}
	}

	ACTOR static Future<Void> receiveCommitCostEstimations(RKMetricsTracker* self) {
		loop {
			ReportCommitCostEstimationRequest req = waitNext(self->reportCommitCostEstimation);
			self->updateCommitCostEstimation(req.ssTrTagCommitCost);
			req.reply.send(Void());
		}
	}

	ACTOR static Future<Void> trackTLogQueueInfo(RKMetricsTracker* self, TLogInterface tli) {
		self->tlogQueueInfo.insert(mapPair(tli.id(), TLogQueueInfo(tli.id())));
		state Map<UID, TLogQueueInfo>::iterator myQueueInfo = self->tlogQueueInfo.find(tli.id());
		TraceEvent("RkTracking", self->ratekeeperId).detail("TransactionLog", tli.id());
		try {
			loop {
				ErrorOr<TLogQueuingMetricsReply> reply = wait(tli.getQueuingMetrics.getReplyUnlessFailedFor(
				    TLogQueuingMetricsRequest(), 0, 0)); // SOMEDAY: or tryGetReply?
				if (reply.present()) {
					myQueueInfo->value.update(reply.get(), self->smoothTotalDurableBytes);
				} else {
					if (myQueueInfo->value.valid) {
						TraceEvent("RkTLogDidNotRespond", self->ratekeeperId).detail("TransactionLog", tli.id());
					}
					myQueueInfo->value.valid = false;
				}

				wait(delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) &&
				     IFailureMonitor::failureMonitor().onStateEqual(tli.getQueuingMetrics.getEndpoint(),
				                                                    FailureStatus(false)));
			}
		} catch (Error& e) {
			// including cancellation
			if (e.code() != error_code_actor_cancelled) {
				self->tlogQueueInfo.erase(myQueueInfo);
			}
			throw e;
		}
	}

	ACTOR static Future<Void> runTlogTrackers(RKMetricsTracker* self) {
		state std::vector<Future<Void>> tlogTrackers;
		state std::vector<TLogInterface> tlogInterfs;
		state Promise<Void> err;

		tlogInterfs = self->dbInfo->get().logSystemConfig.allLocalLogs();
		tlogTrackers.reserve(tlogInterfs.size());
		for (auto tli : tlogInterfs) {
			tlogTrackers.push_back(splitError(trackTLogQueueInfo(self, tli), err));
		}
		loop choose {
			when(wait(self->dbInfo->onChange())) {
				if (tlogInterfs != self->dbInfo->get().logSystemConfig.allLocalLogs()) {
					tlogInterfs = self->dbInfo->get().logSystemConfig.allLocalLogs();
					self->tlogQueueInfo.clear();
					tlogTrackers = std::vector<Future<Void>>();
					for (auto tli : tlogInterfs) {
						tlogTrackers.push_back(splitError(trackTLogQueueInfo(self, tli), err));
					}
				}
			}
			when(wait(err.getFuture())) {
				ASSERT(false);
			}
		}
	}
}; // class RKMetricsTrackerImpl

RKMetricsTracker::RKMetricsTracker(UID ratekeeperId,
                                   Database db,
                                   FutureStream<ReportCommitCostEstimationRequest> reportCommitCostEstimation,
                                   Reference<AsyncVar<ServerDBInfo> const> dbInfo)
  : ratekeeperId(ratekeeperId), db(db), reportCommitCostEstimation(reportCommitCostEstimation),
    lastSSListFetchedTimestamp(now()), dbInfo(dbInfo), smoothTotalDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT) {}

RKMetricsTracker::~RKMetricsTracker() = default;

void RKMetricsTracker::updateCommitCostEstimation(
    UIDTransactionTagMap<TransactionCommitCostEstimation> const& costEstimation) {
	for (auto it = storageQueueInfo.begin(); it != storageQueueInfo.end(); ++it) {
		auto tagCostIt = costEstimation.find(it->key);
		if (tagCostIt == costEstimation.end())
			continue;
		for (const auto& [tagName, cost] : tagCostIt->second) {
			it->value.addCommitCost(tagName, cost);
		}
	}
}

Map<UID, StorageQueueInfo> const& RKMetricsTracker::getStorageQueueInfo() const& {
	return storageQueueInfo;
}

bool RKMetricsTracker::ssListFetchTimedOut() const {
	return now() - lastSSListFetchedTimestamp > SERVER_KNOBS->STORAGE_SERVER_LIST_FETCH_TIMEOUT;
}

Map<UID, TLogQueueInfo> const& RKMetricsTracker::getTlogQueueInfo() const& {
	return tlogQueueInfo;
}

double RKMetricsTracker::getSmoothTotalDurableBytesRate() const {
	return smoothTotalDurableBytes.smoothRate();
}

Future<Void> RKMetricsTracker::run() {
	actors.add(RKMetricsTrackerImpl::monitorServerListChange(this));
	actors.add(RKMetricsTrackerImpl::trackEachStorageServer(this));
	actors.add(RKMetricsTrackerImpl::refreshStorageServerCommitCosts(this));
	actors.add(RKMetricsTrackerImpl::receiveCommitCostEstimations(this));
	actors.add(RKMetricsTrackerImpl::runTlogTrackers(this));
	return actors.getResult();
}

StorageQueueInfo::StorageQueueInfo(const UID& ratekeeperID_, const UID& id_, const LocalityData& locality_)
  : valid(false), ratekeeperID(ratekeeperID_), id(id_), locality(locality_), acceptingRequests(false),
    smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
    verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT), smoothDurableVersion(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothLatestVersion(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT), limitReason(limitReason_t::unlimited) {
	// FIXME: this is a tacky workaround for a potential uninitialized use in trackStorageServerQueueInfo
	lastReply.instanceID = -1;
}

StorageQueueInfo::StorageQueueInfo(const UID& id_, const LocalityData& locality_)
  : StorageQueueInfo(UID(), id_, locality_) {}

void StorageQueueInfo::addCommitCost(TransactionTagRef tagName, TransactionCommitCostEstimation const& cost) {
	tagCostEst[tagName] += cost;
	totalWriteCosts += cost.getCostSum();
	totalWriteOps += cost.getOpsSum();
}

void StorageQueueInfo::update(StorageQueuingMetricsReply const& reply, Smoother& smoothTotalDurableBytes) {
	valid = true;
	auto prevReply = std::move(lastReply);
	lastReply = reply;
	if (prevReply.instanceID != reply.instanceID) {
		smoothDurableBytes.reset(reply.bytesDurable);
		verySmoothDurableBytes.reset(reply.bytesDurable);
		smoothInputBytes.reset(reply.bytesInput);
		smoothFreeSpace.reset(reply.storageBytes.available);
		smoothTotalSpace.reset(reply.storageBytes.total);
		smoothDurableVersion.reset(reply.durableVersion);
		smoothLatestVersion.reset(reply.version);
	} else {
		smoothTotalDurableBytes.addDelta(reply.bytesDurable - prevReply.bytesDurable);
		smoothDurableBytes.setTotal(reply.bytesDurable);
		verySmoothDurableBytes.setTotal(reply.bytesDurable);
		smoothInputBytes.setTotal(reply.bytesInput);
		smoothFreeSpace.setTotal(reply.storageBytes.available);
		smoothTotalSpace.setTotal(reply.storageBytes.total);
		smoothDurableVersion.setTotal(reply.durableVersion);
		smoothLatestVersion.setTotal(reply.version);
	}

	busiestReaders = reply.busiestReaders;
}

UpdateCommitCostRequest StorageQueueInfo::refreshCommitCost(double elapsed) {
	busiestWriters.clear();
	TransactionTag busiestTag;
	TransactionCommitCostEstimation maxCost;
	double maxRate = 0, maxBusyness = 0;
	for (const auto& [tag, cost] : tagCostEst) {
		double rate = cost.getCostSum() / elapsed;
		if (rate > maxRate) {
			busiestTag = tag;
			maxRate = rate;
			maxCost = cost;
		}
	}
	if (maxRate > SERVER_KNOBS->MIN_TAG_WRITE_PAGES_RATE * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) {
		// TraceEvent("RefreshSSCommitCost").detail("TotalWriteCost", totalWriteCost).detail("TotalWriteOps",totalWriteOps);
		ASSERT_GT(totalWriteCosts, 0);
		maxBusyness = double(maxCost.getCostSum()) / totalWriteCosts;
		busiestWriters.emplace_back(ThrottlingId::fromTag(busiestTag), maxRate, maxBusyness);
	}

	UpdateCommitCostRequest updateCommitCostRequest{ ratekeeperID,
		                                             now(),
		                                             elapsed,
		                                             busiestTag,
		                                             maxCost.getOpsSum(),
		                                             maxCost.getCostSum(),
		                                             totalWriteCosts,
		                                             !busiestWriters.empty(),
		                                             ReplyPromise<Void>() };

	// reset statistics
	tagCostEst.clear();
	totalWriteOps = 0;
	totalWriteCosts = 0;

	return updateCommitCostRequest;
}

Optional<double> StorageQueueInfo::getTagThrottlingRatio(int64_t storageTargetBytes, int64_t storageSpringBytes) const {
	auto const storageQueue = getStorageQueueBytes();
	// TODO: Remove duplicate calculation from RKRateUpdater::updateRate
	double inverseResult = std::min(
	    2.0, (storageQueue - storageTargetBytes + storageSpringBytes) / static_cast<double>(storageSpringBytes));
	if (inverseResult > 0) {
		return 1.0 / inverseResult;
	} else {
		return {};
	}
}

TLogQueueInfo::TLogQueueInfo(UID id)
  : valid(false), id(id), smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
    smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT) {
	// FIXME: this is a tacky workaround for a potential uninitialized use in trackTLogQueueInfo (copied
	// from storageQueueInfo)
	lastReply.instanceID = -1;
}

void TLogQueueInfo::update(TLogQueuingMetricsReply const& reply, Smoother& smoothTotalDurableBytes) {
	valid = true;
	auto prevReply = std::move(lastReply);
	lastReply = reply;
	if (prevReply.instanceID != reply.instanceID) {
		smoothDurableBytes.reset(reply.bytesDurable);
		verySmoothDurableBytes.reset(reply.bytesDurable);
		smoothInputBytes.reset(reply.bytesInput);
		smoothFreeSpace.reset(reply.storageBytes.available);
		smoothTotalSpace.reset(reply.storageBytes.total);
	} else {
		smoothTotalDurableBytes.addDelta(reply.bytesDurable - prevReply.bytesDurable);
		smoothDurableBytes.setTotal(reply.bytesDurable);
		verySmoothDurableBytes.setTotal(reply.bytesDurable);
		smoothInputBytes.setTotal(reply.bytesInput);
		smoothFreeSpace.setTotal(reply.storageBytes.available);
		smoothTotalSpace.setTotal(reply.storageBytes.total);
	}
}
