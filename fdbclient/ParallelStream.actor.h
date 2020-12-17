/*
 * ParallelStream.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_PARALLEL_STREAM_ACTOR_G_H)
#define FDBCLIENT_PARALLEL_STREAM_ACTOR_G_H
#include "fdbclient/ParallelStream.actor.g.h"
#elif !defined(FDBCLIENT_PARALLEL_STREAM_ACTOR_H)
#define FDBCLIENT_PARALLEL_STREAM_ACTOR_H

#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // must be last include

template <class T>
class ParallelStream {
	FlowLock semaphore;
	struct FragmentConstructorTag {
		explicit FragmentConstructorTag() = default;
	};

public:
	class Fragment : public ReferenceCounted<Fragment> {
		ParallelStream* parallelStream;
		PromiseStream<T> stream;
		FlowLock::Releaser releaser;
		friend class ParallelStream;

	public:
		Fragment(ParallelStream* parallelStream, FragmentConstructorTag)
		  : parallelStream(parallelStream), releaser(parallelStream->semaphore) {}
		template<class U>
		void send(U &&value) {
			stream.send(std::forward<U>(value));
		}
		void sendError(Error e) { parallelStream->sendError(e); }
		void finish() {
			releaser.release(); // Release before destruction to free up pending fragments
			stream.sendError(end_of_stream());
		}
	};

private:
	PromiseStream<Reference<Fragment>> fragments;
	size_t fragmentsProcessed { 0 };
	PromiseStream<T> results;
	Future<Void> flusher;

public:

	ACTOR static Future<Void> flushToClient(ParallelStream<T> *self) {
		state const int bytesPerTaskLimit = 1e6; // TODO: buggify
		state int bytesFlushedInTask = 0;
		loop {
			if (!self->fragments.getFuture().isReady()) {
				bytesFlushedInTask = 0;
			}
			if (bytesFlushedInTask > bytesPerTaskLimit) {
				wait(yield());
			}
			state Reference<Fragment> fragment = waitNext(self->fragments.getFuture());
			loop {
				try {
					if (!fragment->stream.getFuture().isReady()) {
						bytesFlushedInTask = 0;
					}
					if (bytesFlushedInTask > bytesPerTaskLimit) {
						wait(yield());
					}
					T value = waitNext(fragment->stream.getFuture());
					bytesFlushedInTask += value.expectedSize();
					self->results.send(value);
					wait(yield());
				} catch (Error &e) {
					if (e.code() == error_code_end_of_stream) {
						fragment.clear();
						break;
					} else {
						throw e;
					}
				}
			}
		}
	}

	ParallelStream(PromiseStream<T> results, size_t concurrency) : results(results), semaphore(concurrency) {
		flusher = flushToClient(this);
	}

	ACTOR static Future<Fragment*> createFragmentImpl(ParallelStream<T>* self) {
		wait(self->semaphore.take());
		auto fragment = makeReference<Fragment>(self, FragmentConstructorTag());
		self->fragments.send(fragment);
		return fragment.getPtr();
	}

	Future<Fragment*> createFragment() { return createFragmentImpl(this); }

	void sendError(Error e) { results.sendError(e); }
};

#include "flow/unactorcompiler.h"

#endif
