/*
 * IndexedSet.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_INDEXEDSET_ACTOR_G_H)
#define FLOW_INDEXEDSET_ACTOR_G_H
#include "flow/IndexedSet.actor.g.h"
#elif !defined(FLOW_INDEXEDSET_ACTOR_H)
#define FLOW_INDEXEDSET_ACTOR_H

#include "flow/flow.h"
#include "flow/Platform.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR template <class Node>
[[flow_allow_discard]] Future<Void> ISFreeNodes(std::vector<Node*> toFree, bool synchronous) {
	// Frees the forest of nodes in the 'toFree' vector.
	// If 'synchronous' is true, then there can be no waits.

	state int eraseCount = 0;

	// Freeing many items from a large tree is bound by the memory latency to
	// fetch each node from main memory.  This code does a largely depth first
	// traversal of the forest to be destroyed (using a stack) but prefetches
	// each node and puts it on a short queue before actually processing it, so
	// that several memory transactions can be outstanding simultaneously.
	state Deque<Node*> prefetchQueue;
	while (!prefetchQueue.empty() || !toFree.empty()) {

		while (prefetchQueue.size() < 10 && !toFree.empty()) {
			_mm_prefetch((const char*)toFree.back(), _MM_HINT_T0);
			prefetchQueue.push_back(toFree.back());
			toFree.pop_back();
		}

		auto n = prefetchQueue.front();
		prefetchQueue.pop_front();

		if (n->child[0])
			toFree.push_back(n->child[0]);
		if (n->child[1])
			toFree.push_back(n->child[1]);
		n->child[0] = n->child[1] = 0;
		delete n;
		++eraseCount;

		if (!synchronous && eraseCount % 1000 == 0)
			wait(yield());
	}

	return Void();
}

#include "flow/unactorcompiler.h"
#endif
