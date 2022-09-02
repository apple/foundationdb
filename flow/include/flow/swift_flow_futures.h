/*
 * network.h
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

#ifndef FLOW_SWIFT_FUTURES_H
#define FLOW_SWIFT_FUTURES_H

#include "swift.h"
#include "flow.h"
#include <stdint.h>

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: type aliases, since we cannot work with templates yet in Swift

using FlowPromiseInt = Promise<int>;
using FlowFutureInt = Future<int>;

// ==== ----------------------------------------------------------------------------------------------------------------

FlowPromiseInt* makePromiseInt() {
	 return new Promise<int>();
}

FlowFutureInt* getFutureOfPromise(FlowPromiseInt p) {
	Future<int> f = p.getFuture();
	return &f;
}

void sendPromiseInt(FlowPromiseInt p, int value) {
	printf("[c++] send %d\n", value);
	p.send(value);
	printf("[c++] sent %d\n", p.getFuture().get());
}


#endif