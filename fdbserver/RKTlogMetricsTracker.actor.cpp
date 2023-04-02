/**
 * RKTlogMetricsTracker.actor.cpp
 */

#include "fdbserver/IRKTlogMetricsTracker.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // must be last include

// FIXME: Remove duplicate code from Ratekeeper.actor.cpp
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

class RKTlogMetricsTrackerImpl {
public:
	ACTOR static Future<Void> trackTLogQueueInfo(RKTlogMetricsTracker* self,
	                                             TLogInterface tli,
	                                             Smoother* smoothTotalDurableBytes) {
		self->tlogQueueInfo.insert(mapPair(tli.id(), TLogQueueInfo(tli.id())));
		state Map<UID, TLogQueueInfo>::iterator myQueueInfo = self->tlogQueueInfo.find(tli.id());
		TraceEvent("RkTracking", self->ratekeeperId).detail("TransactionLog", tli.id());
		try {
			loop {
				ErrorOr<TLogQueuingMetricsReply> reply = wait(tli.getQueuingMetrics.getReplyUnlessFailedFor(
				    TLogQueuingMetricsRequest(), 0, 0)); // SOMEDAY: or tryGetReply?
				if (reply.present()) {
					myQueueInfo->value.update(reply.get(), *smoothTotalDurableBytes);
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

	ACTOR static Future<Void> run(RKTlogMetricsTracker* self, Smoother* smoothTotalDurableBytes) {
		state std::vector<Future<Void>> tlogTrackers;
		state std::vector<TLogInterface> tlogInterfs;
		state Promise<Void> err;

		tlogInterfs = self->dbInfo->get().logSystemConfig.allLocalLogs();
		tlogTrackers.reserve(tlogInterfs.size());
		for (auto tli : tlogInterfs) {
			tlogTrackers.push_back(splitError(trackTLogQueueInfo(self, tli, smoothTotalDurableBytes), err));
		}
		loop choose {
			when(wait(self->dbInfo->onChange())) {
				if (tlogInterfs != self->dbInfo->get().logSystemConfig.allLocalLogs()) {
					tlogInterfs = self->dbInfo->get().logSystemConfig.allLocalLogs();
					tlogTrackers = std::vector<Future<Void>>();
					for (auto tli : tlogInterfs) {
						tlogTrackers.push_back(splitError(trackTLogQueueInfo(self, tli, smoothTotalDurableBytes), err));
					}
				}
			}
			when(wait(err.getFuture())) {
				ASSERT(false);
			}
		}
	}

}; // class RKTlogMetricsTrackerImpl

RKTlogMetricsTracker::RKTlogMetricsTracker(UID ratekeeperId, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
  : ratekeeperId(ratekeeperId), dbInfo(dbInfo) {}

RKTlogMetricsTracker::~RKTlogMetricsTracker() = default;

Future<Void> RKTlogMetricsTracker::run(Smoother& smoothTotalDurableBytes) {
	return RKTlogMetricsTrackerImpl::run(this, &smoothTotalDurableBytes);
}

Map<UID, TLogQueueInfo> const& RKTlogMetricsTracker::getTlogQueueInfo() const {
	return tlogQueueInfo;
}

TLogQueueInfo::TLogQueueInfo(UID id)
  : valid(false), id(id), smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
    smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT) {
	// FIXME: this is a tacky workaround for a potential uninitialized use in trackTLogQueueInfo (copied
	// from storageQueueInfO)
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
