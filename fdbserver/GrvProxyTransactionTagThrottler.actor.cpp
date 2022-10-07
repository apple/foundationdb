#include "fdbserver/GrvProxyTransactionTagThrottler.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

void GrvProxyTransactionTagThrottler::updateRates(TransactionTagMap<double> const& newRates) {
	for (const auto& [tag, rate] : newRates) {
		auto it = queues.find(tag);
		if (it == queues.end()) {
			queues[tag] = TagQueue(rate);
		} else {
			it->second.setRate(rate);
		}
	}

	// Clean up tags that did not appear in newRates
	for (auto& [tag, queue] : queues) {
		if (newRates.find(tag) == newRates.end()) {
			queue.rateInfo.reset();
			if (queue.requests.empty()) {
				// FIXME: Use cleaner method of cleanup
				queues.erase(tag);
			}
		}
	}
}

void GrvProxyTransactionTagThrottler::addRequest(GetReadVersionRequest const& req) {
	if (req.tags.empty()) {
		untaggedRequests.push_back(req);
	} else {
		auto const& tag = req.tags.begin()->first;
		if (req.tags.size() > 1) {
			// The GrvProxyTransactionTagThrottler assumes that each GetReadVersionRequest
			// has at most one tag. If a transaction uses multiple tags and
			// SERVER_KNOBS->ENFORCE_TAG_THROTTLING_ON_PROXIES is enabled, there may be
			// unexpected behaviour, because only one tag is used for throttling.
			TraceEvent(SevWarnAlways, "GrvProxyTransactionTagThrottler_MultipleTags")
			    .detail("NumTags", req.tags.size())
			    .detail("UsingTag", printable(tag));
		}
		queues[tag].requests.emplace_back(req);
	}
}

void GrvProxyTransactionTagThrottler::TagQueue::releaseTransactions(
    double elapsed,
    SpannedDeque<GetReadVersionRequest>& outBatchPriority,
    SpannedDeque<GetReadVersionRequest>& outDefaultPriority) {
	Deque<DelayedRequest> newDelayedRequests;
	if (rateInfo.present())
		rateInfo.get().startReleaseWindow();
	int transactionsReleased = 0;
	while (!requests.empty()) {
		auto& delayedReq = requests.front();
		auto& req = delayedReq.req;
		auto const count = req.tags.begin()->second;
		if (!rateInfo.present() || rateInfo.get().canStart(transactionsReleased, count)) {
			req.proxyTagThrottledDuration = now() - delayedReq.startTime;
			transactionsReleased += count;
			if (req.priority == TransactionPriority::BATCH) {
				outBatchPriority.push_back(req);
			} else if (req.priority == TransactionPriority::DEFAULT) {
				outDefaultPriority.push_back(req);
			} else {
				// Immediate priority transactions should bypass the GrvProxyTransactionTagThrottler
				ASSERT(false);
			}
		} else {
			newDelayedRequests.push_back(delayedReq);
		}
		requests.pop_front();
	}
	if (rateInfo.present())
		rateInfo.get().endReleaseWindow(transactionsReleased, false, elapsed);
	requests = std::move(newDelayedRequests);
}

void GrvProxyTransactionTagThrottler::releaseTransactions(double elapsed,
                                                          SpannedDeque<GetReadVersionRequest>& outBatchPriority,
                                                          SpannedDeque<GetReadVersionRequest>& outDefaultPriority) {
	for (auto& [_, tagQueue] : queues) {
		tagQueue.releaseTransactions(elapsed, outBatchPriority, outDefaultPriority);
	}
}

ACTOR static Future<Void> mockClient(GrvProxyTransactionTagThrottler* throttler,
                                     TransactionPriority priority,
                                     TagSet tagSet,
                                     int batchSize,
                                     double desiredRate,
                                     TransactionTagMap<uint32_t>* counters) {
	state Future<Void> timer;
	state TransactionTagMap<uint32_t> tags;
	for (const auto& tag : tagSet) {
		tags[tag] = batchSize;
	}
	loop {
		timer = delayJittered(static_cast<double>(batchSize) / desiredRate);
		GetReadVersionRequest req;
		req.tags = tags;
		req.priority = priority;
		throttler->addRequest(req);
		wait(success(req.reply.getFuture()) && timer);
		for (auto& [tag, _] : tags) {
			(*counters)[tag] += batchSize;
		}
	}
}

ACTOR static Future<Void> mockServer(GrvProxyTransactionTagThrottler* throttler) {
	state SpannedDeque<GetReadVersionRequest> outBatchPriority("TestGrvProxyTransactionTagThrottler_Batch"_loc);
	state SpannedDeque<GetReadVersionRequest> outDefaultPriority("TestGrvProxyTransactionTagThrottler_Default"_loc);
	loop {
		state double elapsed = (0.009 + 0.002 * deterministicRandom()->random01());
		wait(delay(elapsed));
		throttler->releaseTransactions(elapsed, outBatchPriority, outDefaultPriority);
		while (!outBatchPriority.empty()) {
			outBatchPriority.front().reply.send(GetReadVersionReply{});
			outBatchPriority.pop_front();
		}
		while (!outDefaultPriority.empty()) {
			outDefaultPriority.front().reply.send(GetReadVersionReply{});
			outDefaultPriority.pop_front();
		}
	}
}

static bool isNear(double desired, int64_t actual) {
	return std::abs(desired - actual) * 10 < desired;
}

// Rate limit set at 10, but client attempts 20 transactions per second.
// Client should be throttled to only 10 transactions per second.
TEST_CASE("/GrvProxyTransactionTagThrottler/Simple") {
	state GrvProxyTransactionTagThrottler throttler;
	state TagSet tagSet;
	state TransactionTagMap<uint32_t> counters;
	{
		TransactionTagMap<double> rates;
		rates["sampleTag"_sr] = 10.0;
		throttler.updateRates(rates);
	}
	tagSet.addTag("sampleTag"_sr);

	state Future<Void> client = mockClient(&throttler, TransactionPriority::DEFAULT, tagSet, 1, 20.0, &counters);
	state Future<Void> server = mockServer(&throttler);
	wait(timeout(client && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_Simple").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 10.0));
	return Void();
}

// Clients share the available 30 transaction/second budget
TEST_CASE("/GrvProxyTransactionTagThrottler/MultiClient") {
	state GrvProxyTransactionTagThrottler throttler;
	state TagSet tagSet;
	state TransactionTagMap<uint32_t> counters;
	{
		TransactionTagMap<double> rates;
		rates["sampleTag"_sr] = 30.0;
		throttler.updateRates(rates);
	}
	tagSet.addTag("sampleTag"_sr);

	state std::vector<Future<Void>> clients;
	clients.reserve(10);
	for (int i = 0; i < 10; ++i) {
		clients.push_back(mockClient(&throttler, TransactionPriority::DEFAULT, tagSet, 1, 10.0, &counters));
	}

	state Future<Void> server = mockServer(&throttler);
	wait(timeout(waitForAll(clients) && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_MultiClient").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 30.0));
	return Void();
}

TEST_CASE("/GrvProxyTransactionTagThrottler/Batch") {
	state GrvProxyTransactionTagThrottler throttler;
	state TagSet tagSet;
	state TransactionTagMap<uint32_t> counters;
	{
		TransactionTagMap<double> rates;
		rates["sampleTag"_sr] = 10.0;
		throttler.updateRates(rates);
	}
	tagSet.addTag("sampleTag"_sr);

	state Future<Void> client = mockClient(&throttler, TransactionPriority::DEFAULT, tagSet, 5, 20.0, &counters);
	state Future<Void> server = mockServer(&throttler);
	wait(timeout(client && server, 60.0, Void()));

	TraceEvent("TagQuotaTest_Batch").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 10.0));
	return Void();
}
