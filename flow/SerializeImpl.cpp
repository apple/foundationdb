/*
 * flow.cpp
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

#include "flow/flow.h"
#include "flow/SerializeImpl.h"

MAKE_SERIALIZABLE(Void);
MAKE_SERIALIZABLE(Error);
MAKE_SERIALIZABLE(double);
MAKE_SERIALIZABLE(bool);
MAKE_SERIALIZABLE(int8_t);
MAKE_SERIALIZABLE(uint8_t);
MAKE_SERIALIZABLE(int16_t);
MAKE_SERIALIZABLE(uint16_t);
MAKE_SERIALIZABLE(int32_t);
MAKE_SERIALIZABLE(uint32_t);
MAKE_SERIALIZABLE(int64_t);
MAKE_SERIALIZABLE(uint64_t);
