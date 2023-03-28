/*
 * IKeyValueStore.actor.cpp
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

#include "fdbserver/IKeyValueStore.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR static Future<Void> replaceRange_impl(IKeyValueStore* self,
                                            KeyRange range,
                                            Standalone<VectorRef<KeyValueRef>> data) {
	state int sinceYield = 0;
	state const KeyValueRef* kvItr = data.begin();
	state KeyRangeRef rangeRef = range;
	if (rangeRef.empty()) {
		return Void();
	}
	self->clear(rangeRef);
	for (; kvItr != data.end(); kvItr++) {
		self->set(*kvItr);
		if (++sinceYield > 1000) {
			wait(yield());
			sinceYield = 0;
		}
	}
	return Void();
}

// Default implementation for replaceRange(), which writes the key one by one.
Future<Void> IKeyValueStore::replaceRange(KeyRange range, Standalone<VectorRef<KeyValueRef>> data) {
	return replaceRange_impl(this, range, data);
}
