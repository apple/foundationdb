/*
 * AsyncFileS3BlobStore.cpp
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

#include "fdbclient/AsyncFileS3BlobStore.h"
#include "fdbrpc/AsyncFileReadAhead.h"
#include "flow/UnitTest.h"
#include "flow/IConnection.h"

Future<int64_t> AsyncFileS3BlobStoreRead::size() const {
	if (!m_size.isValid())
		m_size = m_bstore->objectSize(m_bucket, m_object);
	return m_size;
}

Future<int> AsyncFileS3BlobStoreRead::read(void* data, int length, int64_t offset) {
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
	ASSERT(abs(speed - limit) / limit < .01);
}
