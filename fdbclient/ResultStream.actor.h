/*
 * ResultStream.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_RESULTSTREAM_ACTOR_G_H)
#define FDBCLIENT_RESULTSTREAM_ACTOR_G_H
#include "fdbclient/ResultStream.actor.g.h"
#elif !defined(FDBCLIENT_RESULTSTREAM_ACTOR_H)
#define FDBCLIENT_RESULTSTREAM_ACTOR_H

#include <deque>
#include <vector>

#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // must be last include

template <class T>
class ResultStream {
	FlowLock semaphore;

public:
	class FragmentStream {
		ResultStream* resultStream;
		std::deque<T> buffer;
		bool completed{ false };
		friend class ResultStream;
		FlowLock::Releaser releaser;
		FragmentStream(ResultStream* resultStream) : resultStream(resultStream), releaser(resultStream->semaphore) {}

	public:
		void send(const T& value) {
			buffer.push_back(value);
			resultStream->flushToClient();
		}
		void sendError(Error e) { resultStream->sendError(e); }
		void finish() {
			ASSERT(!completed);
			completed = true;
			resultStream->flushToClient();
			releaser.release();
		}
	};

private:
	std::vector<FragmentStream> fragments;
	typename std::vector<FragmentStream>::iterator nextFragment = fragments.begin();
	PromiseStream<T> results;

	// TODO: Fix potential slow task
	void flushToClient() {
		while (nextFragment != fragments.end()) {
			auto& fragment = *nextFragment;
			while (!fragment.buffer.empty()) {
				results.send(std::move(fragment.buffer.front()));
				fragment.buffer.pop_front();
			}
			if (fragment.completed) {
				fragment.releaser.release();
				++nextFragment;
			} else {
				break;
			}
		}
	}

public:
	ResultStream(PromiseStream<T> results, size_t concurrency) : results(results), semaphore(concurrency) {}

	ACTOR static Future<FragmentStream*> createFragmentStreamImpl(ResultStream<T>* self) {
		wait(self->semaphore.take());
		return &self->fragments.emplace_back(FragmentStream(self));
	}

	Future<FragmentStream*> createFragmentStream() { return createFragmentStreamImpl(this); }

	void sendError(Error e) { results.sendError(e); }
};

#include "flow/unactorcompiler.h"

#endif
