/*
 * AsyncFileS3BlobStore.actor.cpp
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

#include "fdbclient/AsyncFileS3BlobStore.actor.h"
#include "fdbrpc/AsyncFileReadAhead.actor.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

Future<int64_t> AsyncFileS3BlobStoreRead::size() const {
	if (!m_size.isValid())
		m_size = m_bstore->objectSize(m_bucket, m_object);
	return m_size;
}

Future<int> AsyncFileS3BlobStoreRead::read(void* data, int length, int64_t offset) {
	return m_bstore->readObject(m_bucket, m_object, data, length, offset);
}

ACTOR Future<Void> sendStuff(int id, Reference<IRateControl> t, int bytes) {
	printf("Starting fake sender %d which will send send %d bytes.\n", id, bytes);
	state double ts = timer();
	state int total = 0;
	while (total < bytes) {
		state int r = std::min<int>(deterministicRandom()->randomInt(0, 1000), bytes - total);
		wait(t->getAllowance(r));
		total += r;
	}
	double dur = timer() - ts;
	printf("Sender %d: Sent %d in %fs, %f/s\n", id, total, dur, total / dur);
	return Void();
}

TEST_CASE("/backup/throttling") {
	// Test will not work in simulation.
	if (g_network->isSimulated())
		return Void();

	state int limit = 100000;
	state Reference<IRateControl> t(new SpeedLimit(limit, 1));

	state int id = 1;
	std::vector<Future<Void>> f;
	state double ts = timer();
	state int total = 0;
	int s;
	s = 500000;
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

	wait(waitForAll(f));
	double dur = timer() - ts;
	int speed = int(total / dur);
	printf("Speed limit was %d, measured speed was %d\n", limit, speed);
	ASSERT(abs(speed - limit) / limit < .01);

	return Void();
}
