/*
 * WorkerEvents.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_CORE_WORKEREVENTS_ACTOR_G_H)
#define FDBSERVER_CORE_WORKEREVENTS_ACTOR_G_H
#include "fdbserver/core/WorkerEvents.actor.g.h"
#elif !defined(FDBSERVER_CORE_WORKEREVENTS_ACTOR_H)
#define FDBSERVER_CORE_WORKEREVENTS_ACTOR_H

#include <map>
#include <set>
#include <string>

#include "flow/ITrace.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct WorkerEvents : std::map<NetworkAddress, TraceEventFields> {};

ACTOR Future<Optional<std::pair<WorkerEvents, std::set<std::string>>>> latestEventOnWorkers(
    std::vector<WorkerDetails> workers,
    std::string eventName);

#include "flow/unactorcompiler.h"

#endif
