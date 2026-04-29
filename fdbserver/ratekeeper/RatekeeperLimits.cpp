/*
 * RatekeeperLimits.cpp
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

#include "Ratekeeper.h"
#include "fdbserver/core/Knobs.h"

RatekeeperLimits::RatekeeperLimits(TransactionPriority priority,
                                   std::string context,
                                   int64_t storageTargetBytes,
                                   int64_t storageSpringBytes,
                                   int64_t logTargetBytes,
                                   int64_t logSpringBytes,
                                   double maxVersionDifference,
                                   int64_t durabilityLagTargetVersions)
  : tpsLimit(std::numeric_limits<double>::infinity()), tpsLimitMetric(StringRef("Ratekeeper.TPSLimit" + context)),
    reasonMetric(StringRef("Ratekeeper.Reason" + context)), storageTargetBytes(storageTargetBytes),
    storageSpringBytes(storageSpringBytes), logTargetBytes(logTargetBytes), logSpringBytes(logSpringBytes),
    maxVersionDifference(maxVersionDifference),
    durabilityLagTargetVersions(durabilityLagTargetVersions +
                                SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS), // The read transaction life versions
                                                                                   // are expected to not
    // be durable on the storage servers
    lastDurabilityLag(0), durabilityLagLimit(std::numeric_limits<double>::infinity()),
    priority(priority), context(context),
    rkUpdateEventCacheHolder(makeReference<EventCacheHolder>("RkUpdate" + context)) {}
