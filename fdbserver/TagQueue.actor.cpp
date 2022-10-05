#include "fdbserver/TagQueue.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

void TagQueue::updateRates(TransactionTagMap<double> const& newRates) {
	for (const auto& [tag, rate] : newRates) {
		auto it = rateInfos.find(tag);
		if (it == rateInfos.end()) {
			rateInfos[tag] = GrvTransactionRateInfo(rate);
		} else {
			it->second.setRate(rate);
		}
	}

	for (const auto& [tag, _] : rateInfos) {
		if (newRates.find(tag) == newRates.end()) {
			rateInfos.erase(tag);
		}
	}
}

bool TagQueue::canStart(TransactionTag tag, int64_t alreadyReleased, int64_t count) const {
	auto it = rateInfos.find(tag);
	if (it == rateInfos.end()) {
		return true;
	}
	return it->second.canStart(alreadyReleased, count);
}

bool TagQueue::canStart(GetReadVersionRequest req, TransactionTagMap<int64_t>& releasedInEpoch) const {
	for (const auto& [tag, count] : req.tags) {
		if (!canStart(tag, releasedInEpoch[tag], count)) {
			return false;
		}
	}
	return true;
}

void TagQueue::addRequest(GetReadVersionRequest req) {
	newRequests.push_back(req);
}

void TagQueue::runEpoch(double elapsed,
                        SpannedDeque<GetReadVersionRequest>& outBatchPriority,
                        SpannedDeque<GetReadVersionRequest>& outDefaultPriority) {
	for (auto& [_, rateInfo] : rateInfos) {
		rateInfo.startReleaseWindow();
	}

	Deque<DelayedRequest> newDelayedRequests;
	TransactionTagMap<int64_t> releasedInEpoch;

	while (!delayedRequests.empty()) {
		auto& delayedReq = delayedRequests.front();
		auto& req = delayedReq.req;
		if (canStart(req, releasedInEpoch)) {
			for (const auto& [tag, count] : req.tags) {
				releasedInEpoch[tag] += count;
			}
			req.proxyTagThrottledDuration = delayedReq.delayTime();
			if (req.priority == TransactionPriority::BATCH) {
				outBatchPriority.push_back(req);
			} else if (req.priority == TransactionPriority::DEFAULT) {
				outDefaultPriority.push_back(req);
			} else {
				// Immediate priority transactions should bypass the TagQueue
				ASSERT(false);
			}
		} else {
			newDelayedRequests.push_back(delayedReq);
		}
		delayedRequests.pop_front();
	}

	while (!newRequests.empty()) {
		auto const& req = newRequests.front();
		if (canStart(req, releasedInEpoch)) {
			for (const auto& [tag, count] : req.tags) {
				releasedInEpoch[tag] += count;
			}
			if (req.priority == TransactionPriority::BATCH) {
				outBatchPriority.push_back(req);
			} else if (req.priority == TransactionPriority::DEFAULT) {
				outDefaultPriority.push_back(req);
			} else {
				// Immediate priority transactions should bypass the TagQueue
				ASSERT(false);
			}
		} else {
			newDelayedRequests.emplace_back(req);
		}
		newRequests.pop_front();
	}

	delayedRequests = std::move(newDelayedRequests);
	for (auto& [tag, rateInfo] : rateInfos) {
		rateInfo.endReleaseWindow(std::move(releasedInEpoch)[tag], false, elapsed);
	}
}

ACTOR static Future<Void> mockClient(TagQueue* tagQueue,
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
		tagQueue->addRequest(req);
		wait(success(req.reply.getFuture()) && timer);
		for (auto& [tag, _] : tags) {
			(*counters)[tag] += batchSize;
		}
	}
}

ACTOR static Future<Void> mockServer(TagQueue* tagQueue) {
	state SpannedDeque<GetReadVersionRequest> outBatchPriority("TestTagQueue_Batch"_loc);
	state SpannedDeque<GetReadVersionRequest> outDefaultPriority("TestTagQueue_Default"_loc);
	loop {
		state double elapsed = (0.009 + 0.002 * deterministicRandom()->random01());
		wait(delay(elapsed));
		tagQueue->runEpoch(elapsed, outBatchPriority, outDefaultPriority);
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
TEST_CASE("/TagQueue/Simple") {
	state TagQueue tagQueue;
	state TagSet tagSet;
	state TransactionTagMap<uint32_t> counters;
	{
		TransactionTagMap<double> rates;
		rates["sampleTag"_sr] = 10.0;
		tagQueue.updateRates(rates);
	}
	tagSet.addTag("sampleTag"_sr);

	state Future<Void> client = mockClient(&tagQueue, TransactionPriority::DEFAULT, tagSet, 1, 20.0, &counters);
	state Future<Void> server = mockServer(&tagQueue);
	wait(timeout(client && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_Simple").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 10.0));
	return Void();
}

// Throttle based on the tag with the lowest rate
TEST_CASE("/TagQueue/MultiTag") {
	state TagQueue tagQueue;
	state TagSet tagSet;
	state TransactionTagMap<uint32_t> counters;
	{
		TransactionTagMap<double> rates;
		rates["sampleTag1"_sr] = 10.0;
		rates["sampleTag2"_sr] = 20.0;
		tagQueue.updateRates(rates);
	}
	tagSet.addTag("sampleTag1"_sr);
	tagSet.addTag("sampleTag2"_sr);

	state Future<Void> client = mockClient(&tagQueue, TransactionPriority::DEFAULT, tagSet, 1, 30.0, &counters);
	state Future<Void> server = mockServer(&tagQueue);
	wait(timeout(client && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_MultiTag").detail("Counter", counters["sampleTag1"_sr]);
	ASSERT_EQ(counters["sampleTag1"_sr], counters["sampleTag2"_sr]);
	ASSERT(isNear(counters["sampleTag1"_sr], 60.0 * 10.0));

	return Void();
}

// Clients share the available 30 transaction/second budget
TEST_CASE("/TagQueue/MultiClient") {
	state TagQueue tagQueue;
	state TagSet tagSet;
	state TransactionTagMap<uint32_t> counters;
	{
		TransactionTagMap<double> rates;
		rates["sampleTag"_sr] = 30.0;
		tagQueue.updateRates(rates);
	}
	tagSet.addTag("sampleTag"_sr);

	state std::vector<Future<Void>> clients;
	clients.reserve(10);
	for (int i = 0; i < 10; ++i) {
		clients.push_back(mockClient(&tagQueue, TransactionPriority::DEFAULT, tagSet, 1, 10.0, &counters));
	}

	state Future<Void> server = mockServer(&tagQueue);
	wait(timeout(waitForAll(clients) && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_MultiClient").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 30.0));
	return Void();
}

TEST_CASE("/TagQueue/Batch") {
	state TagQueue tagQueue;
	state TagSet tagSet;
	state TransactionTagMap<uint32_t> counters;
	{
		TransactionTagMap<double> rates;
		rates["sampleTag"_sr] = 10.0;
		tagQueue.updateRates(rates);
	}
	tagSet.addTag("sampleTag"_sr);

	state Future<Void> client = mockClient(&tagQueue, TransactionPriority::DEFAULT, tagSet, 5, 20.0, &counters);
	state Future<Void> server = mockServer(&tagQueue);
	wait(timeout(client && server, 60.0, Void()));

	TraceEvent("TagQuotaTest_Batch").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 10.0));
	return Void();
}
