/**
 * RKRateServer.h
 */

#include "fdbserver/IRKRateServer.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TagThrottler.h"

class RKRateServerImpl {
public:
	ACTOR static Future<Void> run(RKRateServer* self,
	                              HealthMetrics const* healthMetrics,
	                              RatekeeperLimits const* normalLimits,
	                              RatekeeperLimits const* batchLimits,
	                              ITagThrottler* tagThrottler,
	                              IRKRecoveryTracker* recoveryTracker,
	                              bool const* lastLimited) {
		loop {
			GetRateInfoRequest req = waitNext(self->getRateInfo);
			GetRateInfoReply reply;

			auto& p = self->grvProxyInfo[req.requesterID];
			//TraceEvent("RKMPU", req.requesterID).detail("TRT", req.totalReleasedTransactions).detail("Last", p.totalTransactions).detail("Delta", req.totalReleasedTransactions - p.totalTransactions);
			if (p.totalTransactions > 0) {
				self->smoothReleasedTransactions.addDelta(req.totalReleasedTransactions - p.totalTransactions);

				for (auto const& [tag, count] : req.throttledTagCounts) {
					tagThrottler->addRequests(tag, count);
				}
			}
			if (p.batchTransactions > 0) {
				self->smoothBatchReleasedTransactions.addDelta(req.batchReleasedTransactions - p.batchTransactions);
			}

			p.totalTransactions = req.totalReleasedTransactions;
			p.batchTransactions = req.batchReleasedTransactions;
			p.version = req.version;

			recoveryTracker->updateMaxVersion(req.version);

			p.lastUpdateTime = now();

			reply.transactionRate = normalLimits->tpsLimit / self->grvProxyInfo.size();
			reply.batchTransactionRate = batchLimits->tpsLimit / self->grvProxyInfo.size();
			reply.leaseDuration = SERVER_KNOBS->METRIC_UPDATE_RATE;

			if (p.lastThrottledTagChangeId != tagThrottler->getThrottledTagChangeId() ||
			    now() > p.lastTagPushTime + SERVER_KNOBS->TAG_THROTTLE_PUSH_INTERVAL) {
				p.lastThrottledTagChangeId = tagThrottler->getThrottledTagChangeId();
				p.lastTagPushTime = now();

				bool returningTagsToProxy{ false };
				if (SERVER_KNOBS->ENFORCE_TAG_THROTTLING_ON_PROXIES) {
					reply.proxyThrottledTags = tagThrottler->getProxyRates(self->grvProxyInfo.size());
					returningTagsToProxy =
					    reply.proxyThrottledTags.present() && reply.proxyThrottledTags.get().size() > 0;
				} else {
					reply.clientThrottledTags = tagThrottler->getClientRates();
					returningTagsToProxy =
					    reply.clientThrottledTags.present() && reply.clientThrottledTags.get().size() > 0;
				}
				CODE_PROBE(returningTagsToProxy, "Returning tag throttles to a proxy");
			}

			reply.healthMetrics.update(*healthMetrics, true, req.detailed);
			reply.healthMetrics.tpsLimit = normalLimits->tpsLimit;
			reply.healthMetrics.batchLimited = lastLimited;

			req.reply.send(reply);
		}
	}
}; // class RKRateServerImpl

RKRateServer::RKRateServer(FutureStream<GetRateInfoRequest> getRateInfo)
  : getRateInfo(getRateInfo), smoothReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothBatchReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT) {}

RKRateServer::~RKRateServer() = default;

double RKRateServer::getSmoothReleasedTransactionRate() const {
	return smoothReleasedTransactions.smoothRate();
}

double RKRateServer::getSmoothBatchReleasedTransactionRate() const {
	return smoothBatchReleasedTransactions.smoothRate();
}

std::map<UID, RKGrvProxyInfo> const& RKRateServer::getGrvProxyInfo() const {
	return grvProxyInfo;
}

void RKRateServer::cleanupExpiredGrvProxies() {
	double tooOld = now() - 1.0;
	for (auto p = grvProxyInfo.begin(); p != grvProxyInfo.end();) {
		if (p->second.lastUpdateTime < tooOld)
			p = grvProxyInfo.erase(p);
		else
			++p;
	}
}

Future<Void> RKRateServer::run(HealthMetrics const& healthMetrics,
                               RatekeeperLimits const& normalLimits,
                               RatekeeperLimits const& batchLimits,
                               ITagThrottler& tagThrottler,
                               IRKRecoveryTracker& recoveryTracker,
                               bool const& lastLimited) {
	return RKRateServerImpl::run(
	    this, &healthMetrics, &normalLimits, &batchLimits, &tagThrottler, &recoveryTracker, &lastLimited);
}
