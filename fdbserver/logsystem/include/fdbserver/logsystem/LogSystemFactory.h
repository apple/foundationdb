/*
 * LogSystemFactory.h
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

#ifndef FDBSERVER_LOGSYSTEM_LOGSYSTEMFACTORY_H
#define FDBSERVER_LOGSYSTEM_LOGSYSTEMFACTORY_H
#pragma once

#include "fdbserver/logsystem/LogSystem.h"

Reference<LogSystem> makeLogSystemFromServerDBInfo(
    UID const& dbgid,
    ServerDBInfo const& db,
    bool useRecoveredAt = false,
    Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>());

Reference<LogSystem> makeLogSystemFromLogSystemConfig(
    UID const& dbgid,
    LocalityData const& locality,
    LogSystemConfig const& conf,
    bool excludeRemote = false,
    bool useRecoveredAt = false,
    Optional<PromiseStream<Future<Void>>> addActor = Optional<PromiseStream<Future<Void>>>());

Reference<LogSystem> makeOldLogSystemFromLogSystemConfig(UID const& dbgid,
                                                         LocalityData const& locality,
                                                         LogSystemConfig const& conf);

Future<Void> recoverAndEndLogSystemEpoch(Reference<AsyncVar<Reference<LogSystem>>> const& outLogSystem,
                                         UID const& dbgid,
                                         DBCoreState const& oldState,
                                         FutureStream<TLogRejoinRequest> const& rejoins,
                                         LocalityData const& locality,
                                         bool* forceRecovery);

#endif
