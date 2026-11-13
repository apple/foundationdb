/*
 * AsyncFileBlobStore.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "AsyncFileBlobStore.h"
#include "fdbclient/S3BlobStore.h"
#include "flow/UnitTest.h"

Future<int64_t> AsyncFileBlobStoreRead::size() const {
	if (!m_size.isValid())
		m_size = m_bstore->objectSize(m_bucket, m_object);
	return m_size;
}

Future<int> AsyncFileBlobStoreRead::read(void* data, int length, int64_t offset) {
	return m_bstore->readObject(m_bucket, m_object, data, length, offset);
}

Future<Void> sendStuff(int id, Reference<IRateControl> t, int bytes) {
	printf("Starting fake sender %d which will send send %d bytes.\n", id, bytes);
	double ts = timer();
	int total = 0;
	while (total < bytes) {
		int r = std::min<int>(deterministicRandom()->randomInt(0, 1000), bytes - total);
		co_await t->getAllowance(r);
		total += r;
	}
	double dur = timer() - ts;
	printf("Sender %d: Sent %d in %fs, %f/s\n", id, total, dur, total / dur);
}

TEST_CASE("/backup/throttling") {
	// Test will not work in simulation.
	if (g_network->isSimulated())
		co_return;

	int limit = 100000;
	Reference<IRateControl> t(new SpeedLimit(limit, 1));

	int id = 1;
	std::vector<Future<Void>> f;
	double ts = timer();
	int total = 0;
	int s = 500000;
	f.push_back(sendStuff(id++, t, s));
	total += s;
	f.push_back(sendStuff(id++, t, s));
	total += s;
	s = 50000;
	f.push_back(sendStuff(id++, t, s));
	total += s;
	f.push_back(sendStuff(id++, t, s));
	total += s;
	s = 5000;
	f.push_back(sendStuff(id++, t, s));
	total += s;

	co_await waitForAll(f);
	double dur = timer() - ts;
	int speed = int(total / dur);
	printf("Speed limit was %d, measured speed was %d\n", limit, speed);
	// Host scheduling can delay completions; only exceeding the rate limit is an error.
	ASSERT(speed <= limit * 1.01);
}

namespace {

class MultipartTestEndpoint final : public S3BlobStoreEndpoint {
public:
	MultipartTestEndpoint() : S3BlobStoreEndpoint("localhost", "80", "test", {}, {}, {}) {
		knobs.multipart_min_part_size = 4;
		knobs.concurrent_writes_per_file = 3;
	}

	Future<std::string> beginMultiPartUpload(const std::string&, const std::string&) override {
		std::string id = std::to_string(++uploadsStarted);
		parts.emplace(id, std::set<unsigned int>{});
		return id;
	}

	Future<std::string> uploadPart(const std::string&,
	                               const std::string&,
	                               const std::string& id,
	                               unsigned int number,
	                               UnsentPacketQueue*,
	                               int,
	                               const std::string&) override {
		if (id == "1" && number == 2) {
			return secondPart.getFuture();
		}
		if (id == "1" && number == 3) {
			return map(thirdPart.getFuture(), [this, id, number](Void) {
				parts.at(id).insert(number);
				return std::to_string(number);
			});
		}
		parts.at(id).insert(number);
		return std::to_string(number);
	}

	Future<Optional<std::string>> finishMultiPartUpload(const std::string&,
	                                                    const std::string&,
	                                                    const std::string& id,
	                                                    const MultiPartSetT&,
	                                                    int64_t) override {
		ASSERT_EQ(parts.at(id).size(), 3);
		published = id;
		parts.erase(id);
		return Optional<std::string>();
	}

	Future<Void> abortMultiPartUpload(const std::string& bucket,
	                                  const std::string& object,
	                                  const std::string& id) override {
		ASSERT_EQ(bucket, "bucket");
		ASSERT_EQ(object, "log");
		ASSERT(parts.at(id) == (std::set<unsigned int>{ 1, 3 }));
		aborted.push_back(id);
		parts.erase(id);
		if (abortCalled.canBeSet()) {
			abortCalled.send(Void());
		}
		return Void();
	}

	Future<Void> deleteObject(const std::string&, const std::string&) override {
		published.clear();
		return Void();
	}

	int uploadsStarted = 0;
	std::map<std::string, std::set<unsigned int>> parts;
	std::vector<std::string> aborted;
	std::string published = "previous";
	Promise<std::string> secondPart;
	Promise<Void> thirdPart;
	Promise<Void> abortCalled;
};

} // namespace

TEST_CASE("/backup/blobstore/AbortAbandonedMultipartUpload") {
	auto endpoint = makeReference<MultipartTestEndpoint>();
	auto writer = makeReference<AsyncFileBlobStoreWrite>(endpoint, "bucket", "log");
	const std::string contents = "123456789012";
	co_await writer->write(contents.data(), contents.size(), 0);
	// All three parts have been dispatched by append. Finishing must wait for their acknowledgements.
	auto finish = writer->sync();
	ASSERT(!finish.isReady());
	ASSERT(endpoint->parts.at("1") == std::set<unsigned int>{ 1 });
	endpoint->secondPart.sendError(io_timeout());
	auto result = co_await coro::errorOr(finish);
	ASSERT(result.isError());
	ASSERT_EQ(result.getError().code(), error_code_io_timeout);
	writer.clear();

	// The third PUT still owns its request buffer and can store a part at the remote endpoint.
	// Wait for it before aborting the failed upload. Leave the previously published object alone.
	ASSERT(endpoint->aborted.empty());
	endpoint->thirdPart.send(Void());
	co_await timeoutError(endpoint->abortCalled.getFuture(), 1.0);
	ASSERT_EQ(endpoint->aborted, std::vector<std::string>{ "1" });
	ASSERT(endpoint->parts.empty());
	ASSERT_EQ(endpoint->published, "previous");
	auto retry = makeReference<AsyncFileBlobStoreWrite>(endpoint, "bucket", "log");
	co_await retry->write(contents.data(), contents.size(), 0);
	co_await retry->sync();
	retry.clear();
	ASSERT_EQ(endpoint->published, "2");
	ASSERT(endpoint->parts.empty());
	ASSERT_EQ(endpoint->aborted, std::vector<std::string>{ "1" });
}
