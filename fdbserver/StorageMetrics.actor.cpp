/*
 * StorageMetrics.actor.cpp
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

#include "flow/UnitTest.h"
#include "fdbserver/StorageMetrics.h"
#include "flow/actorcompiler.h" // This must be the last #include.

TEST_CASE("/fdbserver/StorageMetricSample/simple") {
	StorageMetricSample s(1000);
	s.sample.insert(LiteralStringRef("Apple"), 1000);
	s.sample.insert(LiteralStringRef("Banana"), 2000);
	s.sample.insert(LiteralStringRef("Cat"), 1000);
	s.sample.insert(LiteralStringRef("Cathode"), 1000);
	s.sample.insert(LiteralStringRef("Dog"), 1000);

	ASSERT(s.getEstimate(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("D"))) == 5000);
	ASSERT(s.getEstimate(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("E"))) == 6000);
	ASSERT(s.getEstimate(KeyRangeRef(LiteralStringRef("B"), LiteralStringRef("C"))) == 2000);

	// ASSERT(s.splitEstimate(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("D")), 3500) ==
	// LiteralStringRef("Cat"));

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/simple") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 800 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(
	    KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("C")), 2000 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 1 && t[0] == LiteralStringRef("Bah"));

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/multipleReturnedPoints") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 800 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(
	    KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("C")), 600 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 3 && t[0] == LiteralStringRef("Absolute") && t[1] == LiteralStringRef("Apple") &&
	       t[2] == LiteralStringRef("Bah"));

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/noneSplitable") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 800 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(
	    KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("C")), 10000 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 0);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/rangeSplitPoints/chunkTooLarge") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 10 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 10 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 30 * sampleUnit);

	std::vector<KeyRef> t = ssm.getSplitPoints(
	    KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("C")), 1000 * sampleUnit, Optional<Key>());

	ASSERT(t.size() == 0);

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/readHotDetect/simple") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.bytesReadSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Banana"), 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cat"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cathode"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Dog"), 1000 * sampleUnit);

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);

	std::vector<ReadHotRangeWithMetrics> t =
	    ssm.getReadHotRanges(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("C")), 2.0, 200 * sampleUnit, 0);

	ASSERT(t.size() == 1 && (*t.begin()).keys.begin == LiteralStringRef("Bah") &&
	       (*t.begin()).keys.end == LiteralStringRef("Bob"));

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/readHotDetect/moreThanOneRange") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.bytesReadSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Banana"), 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cat"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cathode"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Dog"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Final"), 2000 * sampleUnit);

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Dah"), 300 * sampleUnit);

	std::vector<ReadHotRangeWithMetrics> t =
	    ssm.getReadHotRanges(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("D")), 2.0, 200 * sampleUnit, 0);

	ASSERT(t.size() == 2 && (*t.begin()).keys.begin == LiteralStringRef("Bah") &&
	       (*t.begin()).keys.end == LiteralStringRef("Bob"));
	ASSERT(t.at(1).keys.begin == LiteralStringRef("Cat") && t.at(1).keys.end == LiteralStringRef("Dah"));

	return Void();
}

TEST_CASE("/fdbserver/StorageMetricSample/readHotDetect/consecutiveRanges") {

	int64_t sampleUnit = SERVER_KNOBS->BYTES_READ_UNITS_PER_SAMPLE;
	StorageServerMetrics ssm;

	ssm.bytesReadSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Banana"), 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Bucket"), 2000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cat"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Cathode"), 1000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Dog"), 5000 * sampleUnit);
	ssm.bytesReadSample.sample.insert(LiteralStringRef("Final"), 2000 * sampleUnit);

	ssm.byteSample.sample.insert(LiteralStringRef("A"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Absolute"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Apple"), 1000 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bah"), 20 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Banana"), 80 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Bob"), 200 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("But"), 100 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Cat"), 300 * sampleUnit);
	ssm.byteSample.sample.insert(LiteralStringRef("Dah"), 300 * sampleUnit);

	std::vector<ReadHotRangeWithMetrics> t =
	    ssm.getReadHotRanges(KeyRangeRef(LiteralStringRef("A"), LiteralStringRef("D")), 2.0, 200 * sampleUnit, 0);

	ASSERT(t.size() == 2 && (*t.begin()).keys.begin == LiteralStringRef("Bah") &&
	       (*t.begin()).keys.end == LiteralStringRef("But"));
	ASSERT(t.at(1).keys.begin == LiteralStringRef("Cat") && t.at(1).keys.end == LiteralStringRef("Dah"));

	return Void();
}
