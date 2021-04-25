/*
 * Knobs.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include <string>

#include "fdbclient/FDBTypes.h"

using ConfigClassSetRef = VectorRef<KeyRef>;
using ConfigClassSet = Standalone<ConfigClassSetRef>;

class ConfigUpdate {
  Arena arena;
  KeyRef configKnobName;
  KeyRef description;
  ValueRef value;
  double timestamp;
  ConfigClassSetRef affectedClasses;
public:

  ConfigUpdate()=default;
  explicit ConfigUpdate(Arena &arena, KeyRef configKnobNode, KeyRef description, ValueRef value, double timestamp, ConfigClassSetRef affectedClasses)
    : arena(arena), configKnobName(arena, configKnobNode), description(arena, description), value(arena, value), timestamp(timestamp) {
    for (const auto &c : affectedClasses) {
      this->affectedClasses.emplace_back_deep(arena, c);
    }
  }

  template<class Ar>
  void serialize(Ar &ar) {
    serializer(ar, arena, configKnobName, description, value, timestamp, affectedClasses);
  }
};
