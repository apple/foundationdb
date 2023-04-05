/*
 * DDRelocationQueue.h
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
#ifndef FOUNDATIONDB_DDRELOCATIONQUEUE_H
#define FOUNDATIONDB_DDRELOCATIONQUEUE_H

#include "fdbserver/DataDistribution.actor.h"
// send request/signal to DDRelocationQueue through interface
// call synchronous method from components outside DDRelocationQueue
struct IDDRelocationQueue {
	PromiseStream<RelocateShard> relocationProducer, relocationConsumer; // FIXME(xwang): not used yet
	// PromiseStream<Promise<int>> getUnhealthyRelocationCount; // FIXME(xwang): change it to a synchronous call

	virtual int getUnhealthyRelocationCount() = 0;
	virtual ~IDDRelocationQueue() = default;
	;
};

#endif // FOUNDATIONDB_DDRELOCATIONQUEUE_H
