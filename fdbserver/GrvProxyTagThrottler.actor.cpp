/*
 * GrvProxyTagThrottler.actor.cpp
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

#include "fdbclient/Knobs.h"
#include "fdbserver/GrvProxyTagThrottler.h"
#include "fdbserver/Knobs.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

uint64_t GrvProxyTagThrottler::DelayedRequest::lastSequenceNumber = 0;

void GrvProxyTagThrottler::DelayedRequest::updateProxyTagThrottledDuration(LatencyBandsMap& latencyBandsMap) {
	req.proxyTagThrottledDuration = now() - startTime;
	auto const& [tag, count] = *req.tags.begin();
	latencyBandsMap.addMeasurement(tag, req.proxyTagThrottledDuration, count);
}

bool GrvProxyTagThrottler::DelayedRequest::isMaxThrottled(double maxThrottleDuration) const {
	return now() - startTime > maxThrottleDuration;
}

void GrvProxyTagThrottler::TagQueue::setRate(double rate) {
	if (rateInfo.present()) {
		rateInfo.get().setRate(rate);
	} else {
		rateInfo = GrvTransactionRateInfo(rate);
	}
}

bool GrvProxyTagThrottler::TagQueue::isMaxThrottled(double maxThrottleDuration) const {
	return !requests.empty() && requests.front().isMaxThrottled(maxThrottleDuration);
}

void GrvProxyTagThrottler::TagQueue::rejectRequests(LatencyBandsMap& latencyBandsMap) {
	CODE_PROBE(true, "GrvProxyTagThrottler rejecting requests");
	while (!requests.empty()) {
		auto& delayedReq = requests.front();
		delayedReq.updateProxyTagThrottledDuration(latencyBandsMap);
		delayedReq.req.reply.sendError(proxy_tag_throttled());
		requests.pop_front();
	}
}

void GrvProxyTagThrottler::TagQueue::endReleaseWindow(int64_t numStarted, double elapsed) {
	if (rateInfo.present()) {
		CODE_PROBE(requests.empty(), "Tag queue ending release window with empty request queue");
		CODE_PROBE(!requests.empty(), "Tag queue ending release window with requests still queued");
		rateInfo.get().endReleaseWindow(numStarted, requests.empty(), elapsed);
	}
}

GrvProxyTagThrottler::GrvProxyTagThrottler(double maxThrottleDuration)
  : maxThrottleDuration(maxThrottleDuration),
    latencyBandsMap("GrvProxyTagThrottler",
                    deterministicRandom()->randomUniqueID(),
                    SERVER_KNOBS->GLOBAL_TAG_THROTTLING_PROXY_LOGGING_INTERVAL,
                    SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED) {}

void GrvProxyTagThrottler::updateRates(TransactionTagMap<double> const& newRates) {
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
		}
	}

	// TODO: Use std::erase_if in C++20
	for (auto it = queues.begin(); it != queues.end();) {
		const auto& [tag, queue] = *it;
		if (queue.requests.empty() && !queue.rateInfo.present()) {
			it = queues.erase(it);
		} else {
			++it;
		}
	}
}

void GrvProxyTagThrottler::addRequest(GetReadVersionRequest const& req) {
	ASSERT(req.isTagged());
	auto const& tag = req.tags.begin()->first;
	if (req.tags.size() > 1) {
		// The GrvProxyTagThrottler assumes that each GetReadVersionRequest
		// has at most one tag. If a transaction uses multiple tags and
		// SERVER_KNOBS->ENFORCE_TAG_THROTTLING_ON_PROXIES is enabled, there may be
		// unexpected behaviour, because only one tag is used for throttling.
		TraceEvent(SevWarnAlways, "GrvProxyTagThrottler_MultipleTags")
		    .suppressFor(60.0)
		    .detail("NumTags", req.tags.size())
		    .detail("UsingTag", printable(tag));
	}
	queues[tag].requests.emplace_back(req);
}

void GrvProxyTagThrottler::releaseTransactions(double elapsed,
                                               Deque<GetReadVersionRequest>& outBatchPriority,
                                               Deque<GetReadVersionRequest>& outDefaultPriority) {
	// Pointer to a TagQueue with some extra metadata stored alongside
	struct TagQueueHandle {
		// Store pointers here to avoid frequent std::unordered_map lookups
		TagQueue* queue;
		// Cannot be stored directly because we need to
		uint32_t* numReleased;
		// Sequence number of the first queued request
		int64_t nextSeqNo;
		bool operator>(TagQueueHandle const& rhs) const { return nextSeqNo > rhs.nextSeqNo; }
		explicit TagQueueHandle(TagQueue& queue, uint32_t& numReleased) : queue(&queue), numReleased(&numReleased) {
			ASSERT(!this->queue->requests.empty());
			nextSeqNo = this->queue->requests.front().sequenceNumber;
		}
	};

	// Priority queue of queues for each tag, ordered by the sequence number of the
	// next request to process in each queue
	std::priority_queue<TagQueueHandle, std::vector<TagQueueHandle>, std::greater<TagQueueHandle>> pqOfQueues;

	// Track transactions released for each tag
	std::vector<std::pair<TransactionTag, uint32_t>> transactionsReleased;
	transactionsReleased.reserve(queues.size());
	auto const transactionsReleasedInitialCapacity = transactionsReleased.capacity();

	for (auto& [tag, queue] : queues) {
		if (queue.rateInfo.present()) {
			queue.rateInfo.get().startReleaseWindow();
		}
		if (!queue.requests.empty()) {
			// First place the count in the transactionsReleased object,
			// then pass a reference to the count to the TagQueueHandle object
			// emplaced into pqOfQueues.
			//
			// Because we've reserved enough space in transactionsReleased
			// to avoid resizing, this reference should remain valid.
			// This allows each TagQueueHandle to update its number of
			// numReleased counter without incurring the cost of a std::unordered_map lookup.
			auto& [_, count] = transactionsReleased.emplace_back(tag, 0);
			pqOfQueues.emplace(queue, count);
		}
	}

	while (!pqOfQueues.empty()) {
		auto tagQueueHandle = pqOfQueues.top();
		pqOfQueues.pop();
		// Used to determine when it is time to start processing another tag
		auto const nextQueueSeqNo =
		    pqOfQueues.empty() ? std::numeric_limits<int64_t>::max() : pqOfQueues.top().nextSeqNo;

		while (!tagQueueHandle.queue->requests.empty()) {
			auto& delayedReq = tagQueueHandle.queue->requests.front();
			auto count = delayedReq.req.tags.begin()->second;
			ASSERT_EQ(tagQueueHandle.nextSeqNo, delayedReq.sequenceNumber);
			if (tagQueueHandle.queue->rateInfo.present() &&
			    !tagQueueHandle.queue->rateInfo.get().canStart(*(tagQueueHandle.numReleased), count)) {
				// Cannot release any more transaction from this tag (don't push the tag queue handle back into
				// pqOfQueues)
				CODE_PROBE(true, "GrvProxyTagThrottler throttling transaction");
				if (tagQueueHandle.queue->isMaxThrottled(maxThrottleDuration)) {
					// Requests in this queue have been throttled too long and errors
					// should be sent to clients.
					tagQueueHandle.queue->rejectRequests(latencyBandsMap);
				}
				break;
			} else {
				if (tagQueueHandle.nextSeqNo < nextQueueSeqNo) {
					// Releasing transaction
					*(tagQueueHandle.numReleased) += count;
					delayedReq.updateProxyTagThrottledDuration(latencyBandsMap);
					if (delayedReq.req.priority == TransactionPriority::BATCH) {
						outBatchPriority.push_back(delayedReq.req);
					} else if (delayedReq.req.priority == TransactionPriority::DEFAULT) {
						outDefaultPriority.push_back(delayedReq.req);
					} else {
						// Immediate priority transactions should bypass the GrvProxyTagThrottler
						ASSERT(false);
					}
					tagQueueHandle.queue->requests.pop_front();
					if (!tagQueueHandle.queue->requests.empty()) {
						tagQueueHandle.nextSeqNo = tagQueueHandle.queue->requests.front().sequenceNumber;
					}
				} else {
					CODE_PROBE(true, "GrvProxyTagThrottler switching tags to preserve FIFO");
					pqOfQueues.push(tagQueueHandle);
					break;
				}
			}
		}
	}

	// End release windows for all tag queues
	{
		TransactionTagMap<uint32_t> transactionsReleasedMap;
		for (const auto& [tag, count] : transactionsReleased) {
			transactionsReleasedMap[tag] = count;
		}
		for (auto& [tag, queue] : queues) {
			queue.endReleaseWindow(transactionsReleasedMap[tag], elapsed);
		}
	}
	// If the capacity is increased, that means the vector has been illegally resized, potentially
	// corrupting memory
	ASSERT_EQ(transactionsReleased.capacity(), transactionsReleasedInitialCapacity);
}

void GrvProxyTagThrottler::addLatencyBandThreshold(double value) {
	CODE_PROBE(size() > 0, "GrvProxyTagThrottler adding latency bands while actively throttling");
	latencyBandsMap.addThreshold(value);
}

uint32_t GrvProxyTagThrottler::size() const {
	return queues.size();
}

ACTOR static Future<Void> mockClient(GrvProxyTagThrottler* throttler,
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

ACTOR static Future<Void> mockFifoClient(GrvProxyTagThrottler* throttler) {
	state TransactionTagMap<uint32_t> tagSet1;
	state TransactionTagMap<uint32_t> tagSet2;
	state std::vector<GetReadVersionRequest> reqs;
	state int i = 0;
	// Used to track the order in which replies are received
	state std::vector<int> replyIndices;

	// Tag half of requests with one tag, half with another, then randomly shuffle
	tagSet1["sampleTag1"_sr] = 1;
	tagSet2["sampleTag2"_sr] = 1;
	for (i = 0; i < 2000; ++i) {
		auto& req = reqs.emplace_back();
		req.priority = TransactionPriority::DEFAULT;
		if (i < 1000) {
			req.tags = tagSet1;
		} else {
			req.tags = tagSet2;
		}
	}
	deterministicRandom()->randomShuffle(reqs);

	// Send requests to throttler and assert that responses are received in FIFO order
	for (const auto& req : reqs) {
		throttler->addRequest(req);
	}
	state std::vector<Future<Void>> futures;
	for (int j = 0; j < 2000; ++j) {
		// Flow hack to capture replyIndices
		auto* _replyIndices = &replyIndices;
		futures.push_back(map(reqs[j].reply.getFuture(), [_replyIndices, j](auto const&) {
			(*_replyIndices).push_back(j);
			return Void();
		}));
	}
	wait(waitForAll(futures));
	for (i = 0; i < 2000; ++i) {
		ASSERT_EQ(replyIndices[i], i);
	}
	return Void();
}

ACTOR static Future<Void> mockServer(GrvProxyTagThrottler* throttler) {
	state Deque<GetReadVersionRequest> outBatchPriority;
	state Deque<GetReadVersionRequest> outDefaultPriority;
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

static TransactionTag getRandomTag() {
	TransactionTag result;
	auto arr = new (result.arena()) uint8_t[32];
	for (int i = 0; i < 32; ++i) {
		arr[i] = (uint8_t)deterministicRandom()->randomInt(0, 256);
	}
	result.contents() = TransactionTagRef(arr, 32);
	return result;
}

static bool isNear(double desired, int64_t actual) {
	return std::abs(desired - actual) * 10 < desired;
}

// Rate limit set at 10, but client attempts 20 transactions per second.
// Client should be throttled to only 10 transactions per second.
TEST_CASE("/GrvProxyTagThrottler/Simple") {
	state GrvProxyTagThrottler throttler(5.0);
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
TEST_CASE("/GrvProxyTagThrottler/MultiClient") {
	state GrvProxyTagThrottler throttler(5.0);
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

// Test processing GetReadVersionRequests that batch several transactions
TEST_CASE("/GrvProxyTagThrottler/Batch") {
	state GrvProxyTagThrottler throttler(5.0);
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

// Tests cleanup of tags that are no longer throttled.
TEST_CASE("/GrvProxyTagThrottler/Cleanup1") {
	GrvProxyTagThrottler throttler(5.0);
	for (int i = 0; i < 1000; ++i) {
		auto const tag = getRandomTag();
		TransactionTagMap<double> rates;
		rates[tag] = 10.0;
		throttler.updateRates(rates);
		ASSERT_EQ(throttler.size(), 1);
	}
	return Void();
}

// Tests cleanup of tags once queues have been emptied
TEST_CASE("/GrvProxyTagThrottler/Cleanup2") {
	GrvProxyTagThrottler throttler(5.0);
	{
		GetReadVersionRequest req;
		req.tags["sampleTag"_sr] = 1;
		req.priority = TransactionPriority::DEFAULT;
		throttler.addRequest(req);
	}
	ASSERT_EQ(throttler.size(), 1);
	throttler.updateRates(TransactionTagMap<double>{});
	ASSERT_EQ(throttler.size(), 1);
	{
		Deque<GetReadVersionRequest> outBatchPriority;
		Deque<GetReadVersionRequest> outDefaultPriority;
		throttler.releaseTransactions(0.1, outBatchPriority, outDefaultPriority);
	}
	// Calling updates cleans up the queues in throttler
	throttler.updateRates(TransactionTagMap<double>{});
	ASSERT_EQ(throttler.size(), 0);
	return Void();
}

// Tests that unthrottled transactions are released in FIFO order, even when they
// have different tags
TEST_CASE("/GrvProxyTagThrottler/Fifo") {
	state GrvProxyTagThrottler throttler(5.0);
	state Future<Void> server = mockServer(&throttler);
	wait(mockFifoClient(&throttler));
	return Void();
}

// Tests that while throughput is low, the tag throttler
// does not accumulate too much budget.
//
// A server is setup to server 10 transactions per second,
// then runs idly for 60 seconds. Then a client starts
// and attempts 20 transactions per second for 60 seconds.
// The server throttles the client to only achieve
// 10 transactions per second during this 60 second window.
// If the throttler is allowed to accumulate budget indefinitely
// during the idle 60 seconds, this test will fail.
TEST_CASE("/GrvProxyTagThrottler/LimitedIdleBudget") {
	state GrvProxyTagThrottler throttler(5.0);
	state TagSet tagSet;
	state TransactionTagMap<uint32_t> counters;
	{
		TransactionTagMap<double> rates;
		rates["sampleTag"_sr] = 10.0;
		throttler.updateRates(rates);
	}
	tagSet.addTag("sampleTag"_sr);

	state Future<Void> server = mockServer(&throttler);
	wait(delay(60.0));
	state Future<Void> client = mockClient(&throttler, TransactionPriority::DEFAULT, tagSet, 1, 20.0, &counters);
	wait(timeout(client && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_LimitedIdleBudget").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 10.0));
	return Void();
}
