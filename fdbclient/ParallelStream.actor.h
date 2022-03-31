/*
 * ParallelStream.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_PARALLEL_STREAM_ACTOR_G_H)
#define FDBCLIENT_PARALLEL_STREAM_ACTOR_G_H
#include "fdbclient/ParallelStream.actor.g.h"
#elif !defined(FDBCLIENT_PARALLEL_STREAM_ACTOR_H)
#define FDBCLIENT_PARALLEL_STREAM_ACTOR_H

#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // must be last include

// ParallelStream is used to fetch data from multiple streams in parallel and then merge them back into a single stream
// in order.
template <class T>
class ParallelStream {
	Reference<BoundedFlowLock> semaphore;
	struct FragmentConstructorTag {
		explicit FragmentConstructorTag() = default;
	};

public:
	// A Fragment is a single stream that will get results to be merged back into the main output stream
	class Fragment : public ReferenceCounted<Fragment> {
		Reference<BoundedFlowLock> semaphore;
		PromiseStream<T> stream;
		BoundedFlowLock::Releaser releaser;
		friend class ParallelStream;

	public:
		Fragment(Reference<BoundedFlowLock> semaphore, int64_t permitNumber, FragmentConstructorTag)
		  : semaphore(semaphore), releaser(semaphore.getPtr(), permitNumber) {}
		template <class U>
		void send(U&& value) {
			stream.send(std::forward<U>(value));
		}
		void sendError(Error e) { stream.sendError(e); }
		void finish() {
			releaser.release(); // Release before destruction to free up pending fragments
			stream.sendError(end_of_stream());
		}
		Future<Void> onEmpty() { return stream.onEmpty(); }
	};

private:
	PromiseStream<Reference<Fragment>> fragments;
	size_t fragmentsProcessed{ 0 };
	PromiseStream<T> results;
	Future<Void> flusher;

public:
	// A background actor which take results from the oldest fragment and sends them to the main output stream
	ACTOR static Future<Void> flushToClient(ParallelStream<T>* self) {
		state const int messagesBetweenYields = 1000;
		state int messagesSinceYield = 0;
		try {
			loop {
				state Reference<Fragment> fragment = waitNext(self->fragments.getFuture());
				loop {
					try {
						wait(self->results.onEmpty());
						T value = waitNext(fragment->stream.getFuture());
						self->results.send(value);
						if (++messagesSinceYield == messagesBetweenYields) {
							wait(yield());
							messagesSinceYield = 0;
						}
					} catch (Error& e) {
						if (e.code() == error_code_end_of_stream) {
							fragment.clear();
							break;
						} else {
							throw e;
						}
					}
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			self->results.sendError(e);
			return Void();
		}
	}

	ParallelStream(PromiseStream<T> results, size_t bufferLimit) : results(results) {
		semaphore = makeReference<BoundedFlowLock>(1, bufferLimit);
		flusher = flushToClient(this);
	}

	// Creates a fragment to get merged into the main output stream
	ACTOR static Future<Fragment*> createFragmentImpl(ParallelStream<T>* self) {
		int64_t permitNumber = wait(self->semaphore->take());
		auto fragment = makeReference<Fragment>(self->semaphore, permitNumber, FragmentConstructorTag());
		self->fragments.send(fragment);
		return fragment.getPtr();
	}

	Future<Fragment*> createFragment() { return createFragmentImpl(this); }

	Future<Void> finish() {
		fragments.sendError(end_of_stream());
		return flusher;
	}
};

#include "flow/unactorcompiler.h"

#endif
