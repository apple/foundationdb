/*
 * ReadLatencySamples.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/ReadLatencySamples.h"
#include "fdbserver/Knobs.h"

namespace {

std::unique_ptr<LatencySample> createSample(std::string_view prefix, std::string_view metricName, UID serverId) {
	return std::make_unique<LatencySample>(fmt::format("{}{}", prefix, metricName),
	                                       serverId,
	                                       SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                                       SERVER_KNOBS->LATENCY_SKETCH_ACCURACY,
	                                       /*skipTraceOnSilentInterval=*/true);
}

} // namespace

ReadLatencySamples::Entry::Entry(std::string_view prefix, UID serverId)
  : samples({ createSample(prefix, "ReadLatencyMetrics", serverId),
              createSample(prefix, "GetKeyMetrics", serverId),
              createSample(prefix, "GetValueMetrics", serverId),
              createSample(prefix, "GetRangeMetrics", serverId),
              createSample(prefix, "ReadVersionWaitMetrics", serverId),
              createSample(prefix, "ReadQueueWaitMetrics", serverId),
              createSample(prefix, "KVGetRangeMetrics", serverId),
              createSample(prefix, "GetMappedRangeMetrics", serverId),
              createSample(prefix, "GetMappedRangeRemoteMetrics", serverId),
              createSample(prefix, "GetMappedRangeLocalMetrics", serverId) }) {}

ReadLatencySamples::ReadLatencySamples(UID serverId)
  : aggregate("", serverId), perType({ Entry("Eager", serverId),
                                       Entry("Fetch", serverId),
                                       Entry("LowPriority", serverId),
                                       Entry("NormalPriority", serverId),
                                       Entry("HighPriority", serverId) }) {}

void ReadLatencySamples::sample(double latency, SampleType sampleType, Optional<ReadType> readType) {
	aggregate.samples[sampleType]->addMeasurement(latency);
	if (readType.present()) {
		perType[readType.get()].samples[sampleType]->addMeasurement(latency);
	}
}
