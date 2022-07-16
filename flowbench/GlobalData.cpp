/*
 * GlobalData.cpp
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

#include "fdbclient/FDBTypes.h"
#include "flow/IRandom.h"

static constexpr size_t globalDataSize = 1 << 20;
static uint8_t* globalData = nullptr;

static inline void initGlobalData() {
	if (!globalData) {
		globalData = static_cast<uint8_t*>(allocateFast(globalDataSize));
	}
	generateRandomData(globalData, globalDataSize);
}

KeyValueRef getKV(size_t keySize, size_t valueSize) {
	initGlobalData();
	ASSERT(keySize + valueSize <= globalDataSize);
	return KeyValueRef(KeyRef(globalData, keySize), ValueRef(globalData + keySize, valueSize));
}

KeyRef getKey(size_t keySize) {
	initGlobalData();
	ASSERT(keySize);
	return KeyRef(globalData, keySize);
}
