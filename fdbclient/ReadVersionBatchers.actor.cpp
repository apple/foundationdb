/**
 * ReadVersionBatchers.actor.cpp
 */

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ReadVersionBatchers.h"
#include "flow/actorcompiler.h" // must be last include

ACTOR static Future<GetReadVersionReply> getConsistentReadVersion(SpanContext parentSpan,
                                                                  DatabaseContext* cx,
                                                                  uint32_t transactionCount,
                                                                  TransactionPriority priority,
                                                                  uint32_t flags,
                                                                  TransactionTagMap<uint32_t> tags,
                                                                  Optional<TenantGroupName> tenantGroup,
                                                                  Optional<UID> debugID) {
	state Span span("NAPI:getConsistentReadVersion"_loc, parentSpan);

	++cx->transactionReadVersionBatches;
	if (debugID.present())
		g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "NativeAPI.getConsistentReadVersion.Before");
	loop {
		try {
			state GetReadVersionRequest req(span.context,
			                                transactionCount,
			                                priority,
			                                cx->ssVersionVectorCache.getMaxVersion(),
			                                flags,
			                                tags,
			                                tenantGroup,
			                                debugID);
			state Future<Void> onProxiesChanged = cx->onProxiesChanged();

			choose {
				when(wait(onProxiesChanged)) {
					onProxiesChanged = cx->onProxiesChanged();
				}
				when(GetReadVersionReply v =
				         wait(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies(
				                                   flags & GetReadVersionRequest::FLAG_USE_PROVISIONAL_PROXIES)),
				                               &GrvProxyInterface::getConsistentReadVersion,
				                               req,
				                               cx->taskID))) {
					CODE_PROBE(v.proxyTagThrottledDuration > 0.0,
					           "getConsistentReadVersion received GetReadVersionReply delayed by proxy tag throttling");
					if (tags.size() != 0) {
						auto& priorityThrottledTags = cx->throttledTags[priority];
						for (auto& tag : tags) {
							auto itr = v.tagThrottleInfo.find(tag.first);
							if (itr == v.tagThrottleInfo.end()) {
								CODE_PROBE(true, "Removing client throttle");
								priorityThrottledTags.erase(tag.first);
							} else {
								CODE_PROBE(true, "Setting client throttle");
								auto result = priorityThrottledTags.try_emplace(tag.first, itr->second);
								if (!result.second) {
									result.first->second.update(itr->second);
								}
							}
						}
					}

					if (debugID.present())
						g_traceBatch.addEvent(
						    "TransactionDebug", debugID.get().first(), "NativeAPI.getConsistentReadVersion.After");
					ASSERT(v.version > 0);
					cx->minAcceptableReadVersion = std::min(cx->minAcceptableReadVersion, v.version);
					if (cx->versionVectorCacheActive(v.ssVersionVectorDelta)) {
						if (cx->isCurrentGrvProxy(v.proxyId)) {
							cx->ssVersionVectorCache.applyDelta(v.ssVersionVectorDelta);
						} else {
							continue; // stale GRV reply, retry
						}
					}
					return v;
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_broken_promise && e.code() != error_code_batch_transaction_throttled &&
			    e.code() != error_code_grv_proxy_memory_limit_exceeded && e.code() != error_code_proxy_tag_throttled)
				TraceEvent(SevError, "GetConsistentReadVersionError").error(e);
			if (e.code() == error_code_batch_transaction_throttled && !cx->apiVersionAtLeast(630)) {
				wait(delayJittered(5.0));
			} else if (e.code() == error_code_grv_proxy_memory_limit_exceeded) {
				// FIXME(xwang): the better way is to let this error broadcast to transaction.onError(e), otherwise
				// the txn->cx counter doesn't make sense
				wait(delayJittered(CLIENT_KNOBS->GRV_ERROR_RETRY_DELAY));
			} else {
				throw;
			}
		}
	}
}

class Batcher {
	struct VersionRequest {
		SpanContext spanContext;
		Promise<GetReadVersionReply> reply;
		TagSet tags;
		Optional<UID> debugID;

		VersionRequest(SpanContext spanContext, TagSet tags = TagSet(), Optional<UID> debugID = {})
		  : spanContext(spanContext), tags(tags), debugID(debugID) {}
	};

	int outstandingRequests = 0;
	PromiseStream<VersionRequest> stream;
	Future<Void> actor;
	double lastRequestTime = now();

	ACTOR static Future<Void> readVersionBatcher(Batcher* self,
	                                             DatabaseContext* cx,
	                                             TransactionPriority priority,
	                                             uint32_t flags,
	                                             Optional<TenantGroupName> tenantGroup) {
		state std::vector<Promise<GetReadVersionReply>> requests;
		state PromiseStream<Future<Void>> addActor;
		state Future<Void> collection = actorCollection(addActor.getFuture(), &self->outstandingRequests);
		state Future<Void> timeout;
		state Optional<UID> debugID;
		state bool send_batch;
		state Reference<Histogram> batchSizeDist = Histogram::getHistogram("GrvBatcher"_sr,
		                                                                   "ClientGrvBatchSize"_sr,
		                                                                   Histogram::Unit::countLinear,
		                                                                   0,
		                                                                   CLIENT_KNOBS->MAX_BATCH_SIZE * 2);
		state Reference<Histogram> batchIntervalDist =
		    Histogram::getHistogram("GrvBatcher"_sr,
		                            "ClientGrvBatchInterval"_sr,
		                            Histogram::Unit::milliseconds,
		                            0,
		                            CLIENT_KNOBS->GRV_BATCH_TIMEOUT * 1000000 * 2);
		state Reference<Histogram> grvReplyLatencyDist =
		    Histogram::getHistogram("GrvBatcher"_sr, "ClientGrvReplyLatency"_sr, Histogram::Unit::milliseconds);
		state double lastRequestTime = now();

		state TransactionTagMap<uint32_t> tags;

		// dynamic batching
		state PromiseStream<double> replyTimes;
		state double batchTime = 0;
		state Span span("NAPI:readVersionBatcher"_loc);
		loop {
			send_batch = false;
			choose {
				when(VersionRequest req = waitNext(self->stream.getFuture())) {
					if (req.debugID.present()) {
						if (!debugID.present()) {
							debugID = nondeterministicRandom()->randomUniqueID();
						}
						g_traceBatch.addAttach("TransactionAttachID", req.debugID.get().first(), debugID.get().first());
					}
					span.addLink(req.spanContext);
					requests.push_back(req.reply);
					for (auto tag : req.tags) {
						++tags[tag];
					}

					if (requests.size() == CLIENT_KNOBS->MAX_BATCH_SIZE) {
						send_batch = true;
						++cx->transactionGrvFullBatches;
					} else if (!timeout.isValid()) {
						timeout = delay(batchTime, TaskPriority::GetConsistentReadVersion);
					}
				}
				when(wait(timeout.isValid() ? timeout : Never())) {
					send_batch = true;
					++cx->transactionGrvTimedOutBatches;
				}
				// dynamic batching monitors reply latencies
				when(double reply_latency = waitNext(replyTimes.getFuture())) {
					double target_latency = reply_latency * 0.5;
					batchTime = std::min(0.1 * target_latency + 0.9 * batchTime, CLIENT_KNOBS->GRV_BATCH_TIMEOUT);
					grvReplyLatencyDist->sampleSeconds(reply_latency);
				}
				when(wait(collection)) {} // for errors
			}
			if (send_batch) {
				int count = requests.size();
				ASSERT(count);

				batchSizeDist->sampleRecordCounter(count);
				auto requestTime = now();
				batchIntervalDist->sampleSeconds(requestTime - lastRequestTime);
				lastRequestTime = requestTime;

				// dynamic batching
				Promise<GetReadVersionReply> GRVReply;
				requests.push_back(GRVReply);
				addActor.send(ready(timeReply(GRVReply.getFuture(), replyTimes)));

				Future<Void> batch = incrementalBroadcastWithError(
				    getConsistentReadVersion(
				        span.context, cx, count, priority, flags, std::move(tags), tenantGroup, std::move(debugID)),
				    std::move(requests),
				    CLIENT_KNOBS->BROADCAST_BATCH_SIZE);

				span = Span("NAPI:readVersionBatcher"_loc);
				tags.clear();
				debugID = Optional<UID>();
				requests.clear();
				addActor.send(batch);
				timeout = Future<Void>();
			}
		}
	}

public:
	Future<GetReadVersionReply> getReadVersion(SpanContext spanContext, TagSet tags, Optional<UID> debugId) {
		lastRequestTime = now();
		auto const req = VersionRequest(spanContext, tags, debugId);
		stream.send(req);
		return req.reply.getFuture();
	}

	void startActor(Database cx,
	                TransactionPriority priority,
	                uint32_t flags,
	                Optional<TenantGroupName> const& tenantGroup) {
		if (!actor.isValid()) {
			actor = readVersionBatcher(this, cx.getPtr(), priority, flags, tenantGroup);
		}
	}

	bool isActive(double expirationTimeout) const {
		return (now() - lastRequestTime < expirationTimeout) || (outstandingRequests > 0);
	}
};

class Index {
	uint32_t flags;
	Optional<ThrottlingId> throttlingId;

public:
	Index(uint32_t flags, Optional<ThrottlingId> const& throttlingId) : flags(flags), throttlingId(throttlingId) {}
	bool operator==(Index const& rhs) const { return (flags == rhs.flags) && (throttlingId == rhs.throttlingId); }
	size_t hash() const {
		size_t result = 0;
		boost::hash_combine(result, flags);
		boost::hash_combine(result, throttlingId);
		return result;
	}
};

struct HashIndex {
	size_t operator()(Index const& index) const { return index.hash(); }
};

class ReadVersionBatchersImpl {
	std::unordered_map<Index, Batcher, HashIndex> batchers;
	int capacity;
	Future<Void> cleaner;

	ACTOR static Future<Void> cleanerActor(ReadVersionBatchersImpl* self,
	                                       double expirationTimeout,
	                                       double expirationCheckInterval) {
		loop {
			wait(delay(expirationCheckInterval));
			auto const _expirationTimeout = expirationTimeout;
			std::erase_if(self->batchers, [_expirationTimeout](auto const& it) {
				auto const& [_, batcher] = it;
				CODE_PROBE(!batcher.isActive(_expirationTimeout), "Expiring GRV batcher");
				return !batcher.isActive(_expirationTimeout);
			});
		}
	}

public:
	ReadVersionBatchersImpl(int capacity, double expirationTimeout, double expirationCheckInterval)
	  : capacity(capacity) {
		cleaner = cleanerActor(this, expirationTimeout, expirationCheckInterval);
	}

	Future<GetReadVersionReply> getReadVersion(Database cx,
	                                           TransactionPriority priority,
	                                           uint32_t flags,
	                                           Optional<TenantGroupName> const& tenantGroup,
	                                           SpanContext spanContext,
	                                           TagSet tags,
	                                           Optional<UID> debugID) {
		Optional<ThrottlingId> throttlingId;
		if (tenantGroup.present()) {
			throttlingId = ThrottlingIdRef::fromTenantGroup(tenantGroup.get());
		} else if (tags.size()) {
			throttlingId = ThrottlingIdRef::fromTag(*tags.begin());
		}
		Index index(flags, throttlingId);
		auto it = batchers.find(index);
		if (it == batchers.end()) {
			if (batchers.size() == capacity) {
				CODE_PROBE(true, "Too many GRV batchers");
				throw too_many_grv_batchers();
			}
			it = batchers.try_emplace(index).first;
		}
		auto& batcher = it->second;
		batcher.startActor(cx, priority, flags, tenantGroup);
		return batcher.getReadVersion(spanContext, tags, debugID);
	}
};

Future<GetReadVersionReply> ReadVersionBatchers::getReadVersion(Database cx,
                                                                TransactionPriority priority,
                                                                uint32_t flags,
                                                                Optional<TenantGroupName> const& tenantGroup,
                                                                SpanContext context,
                                                                TagSet tags,
                                                                Optional<UID> debugID) {
	return impl->getReadVersion(cx, priority, flags, tenantGroup, context, tags, debugID);
}

ReadVersionBatchers::ReadVersionBatchers(int capacity, double expirationTimeout, double expirationCheckInterval)
  : impl(PImpl<ReadVersionBatchersImpl>::create(capacity, expirationTimeout, expirationCheckInterval)) {}

ReadVersionBatchers::~ReadVersionBatchers() = default;
