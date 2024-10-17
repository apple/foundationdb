/*
 * ActorUID.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_ACTOR_UID_H
#define FLOW_ACTOR_UID_H
#pragma once

#include "flow/ActorContext.h"

#include <string_view>
#include <unordered_map>

namespace ActorMonitoring {
struct ActorDebuggingData {
    std::string_view path;
    uint32_t lineNumber;
    std::string_view actorName;
    // TODO Use an enum?
    std::string_view type;
};

extern const GUID BINARY_GUID;
extern const std::unordered_map<GUID, ActorDebuggingData> ACTOR_DEBUGGING_DATA;

} // ActorMonitoring

#endif  // FLOW_ACTOR_UID_H