/*
 * FlowThread.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_FLOW_THREAD_H
#define FLOW_FLOW_THREAD_H
#include "flow/Buggify.h"
#pragma once

#include <flow/flow.h>
#include <flow/FastAlloc.h>
#include <flow/ThreadPrimitives.h>
#include <flow/ThreadHelper.actor.h>
#include <flow/ScopeExit.h>

// NOTE: Currently futures should only be used from main thread.
template <class T>
class ThreadNotifiedQueue : private SingleCallback<T>, public FastAllocated<ThreadNotifiedQueue<T>> {
public:
	int promises;
	int futures;
	Error error;

	// Invariant: SingleCallback<T>::next==this || (queue.empty() && !error.isValid())
	std::queue<T, Deque<T>> queue;
	ThreadSpinLock mutex;

	ThreadNotifiedQueue(int futures, int promises) : promises(promises), futures(futures) {
		SingleCallback<T>::next = this;
	}

	virtual ~ThreadNotifiedQueue() = default;

	bool isReady() {
		ThreadSpinLockHolder holder(mutex);
		return !queue.empty() || error.isValid();
	}

	// Returns `true` if error exists and there are no pending items to be popped from queue.
	// E.g. if producer three values before sending error, client will first see the first
	// three values before error is visible.
	bool isError() {
		ThreadSpinLockHolder holder(mutex);
		return queue.empty() && error.isValid();
	}

	bool hasError() {
		ThreadSpinLockHolder holder(mutex);
		return error.isValid();
	} // there is an error queued

	uint32_t size() {
		ThreadSpinLockHolder holder(mutex);
		return queue.size();
	}

	virtual T pop() {
		ThreadSpinLockHolder holder(mutex);
		if (queue.empty()) {
			if (error.isValid())
				throw error;
			throw internal_error();
		}
		auto copy = std::move(queue.front());
		queue.pop();
		return copy;
	}

	template <class U>
	void send(U&& value) {
		if (error.isValid())
			return;

		addPromiseRef();
		onMainThreadVoid([this, value = std::forward<U>(value)]() {
			ScopeExit scope([&]() { this->delPromiseRef(); });
			mutex.enter();
			if (this->next != this) {
				auto n = this->next;
				mutex.leave();
				n->fire(value);
			} else {
				this->queue.emplace(value);
				mutex.leave();
			}
		});
	}

	void sendError(Error err) {
		if (error.isValid())
			return;

		ASSERT(this->error.code() != error_code_success);
		this->error = err;

		addPromiseRef();
		onMainThreadVoid([this, err]() {
			ScopeExit scope([&]() { this->delPromiseRef(); });
			// end_of_stream error is "expected", don't terminate reading stream early for this
			SingleCallback<T>* n = nullptr;
			{
				ThreadSpinLockHolder holder(mutex);
				if (this->shouldFireImmediatelyUnsafe()) {
					n = this->next;
				}
			}

			if (n)
				n->error(err);
		});
	}

	void addPromiseRef() {
		ThreadSpinLockHolder holder(mutex);
		promises++;
	}

	void addFutureRef() {
		ThreadSpinLockHolder holder(mutex);
		futures++;
	}

	void delPromiseRef() {
		mutex.enter();
		if (!--promises) {
			if (futures) {
				mutex.leave();
				sendError(broken_promise());
			} else {
				mutex.leave();
				destroy();
			}
		} else {
			mutex.leave();
		}
	}

	void delFutureRef() {
		mutex.enter();
		if (!--futures) {
			if (promises) {
				mutex.leave();
				cancel();
			} else {
				mutex.leave();
				destroy();
			}
		} else {
			mutex.leave();
		}
	}

	int getFutureReferenceCount() {
		ThreadSpinLockHolder holder(mutex);
		return futures;
	}

	int getPromiseReferenceCount() {
		ThreadSpinLockHolder holder(mutex);
		return promises;
	}

	void addCallbackAndDelFutureRef(SingleCallback<T>* cb) {
		ThreadSpinLockHolder holder(mutex);
		ASSERT(SingleCallback<T>::next == this);
		cb->insert(this);
	}

	virtual void destroy() { delete this; }
	virtual void cancel() {}
	virtual void unwait() override { delFutureRef(); }
	virtual void fire(T const&) override { ASSERT(false); }
	virtual void fire(T&&) override { ASSERT(false); }

protected:
	bool shouldFireImmediatelyUnsafe() { return SingleCallback<T>::next != this; }
};

template <class T>
class ThreadFutureStream {
public:
	ThreadFutureStream() : queue(nullptr) {}
	ThreadFutureStream(const ThreadFutureStream& rhs) : queue(rhs.queue) { queue->addFutureRef(); }
	ThreadFutureStream(ThreadFutureStream&& rhs) noexcept : queue(rhs.queue) { rhs.queue = 0; }
	explicit ThreadFutureStream(ThreadNotifiedQueue<T>* queue) : queue(queue) {}
	~ThreadFutureStream() {
		if (queue)
			queue->delFutureRef();
	}

	bool isValid() const { return queue != nullptr; }
	bool isReady() const { return queue->isReady(); }
	bool isError() const {
		// This means that the next thing to be popped is an error - it will be false if there is an
		// error in the stream but some actual data first
		return queue->isError();
	}

	void addCallbackAndClear(SingleCallback<T>* cb) {
		queue->addCallbackAndDelFutureRef(cb);
		queue = nullptr;
	}

	void operator=(const ThreadFutureStream& rhs) {
		rhs.queue->addFutureRef();
		if (queue)
			queue->delFutureRef();
		queue = rhs.queue;
	}

	void operator=(ThreadFutureStream&& rhs) noexcept {
		if (rhs.queue != queue) {
			if (queue)
				queue->delFutureRef();
			queue = rhs.queue;
			rhs.queue = nullptr;
		}
	}

	bool operator==(const ThreadFutureStream& rhs) { return rhs.queue == queue; }
	bool operator!=(const ThreadFutureStream& rhs) { return rhs.queue != queue; }

	T pop() {
		ASSERT(g_network->isOnMainThread());
		return queue->pop();
	}

	Error getError() const {
		// TODO: (vishesh)
		ASSERT(queue->isError());
		return queue->error;
	}

private:
	ThreadNotifiedQueue<T>* queue;
};

template <class T>
class ThreadReturnPromiseStream {
public:
	ThreadReturnPromiseStream() : queue(new ThreadNotifiedQueue<T>(0, 1)) {}
	ThreadReturnPromiseStream(const ThreadReturnPromiseStream& rhs) = delete;
	ThreadReturnPromiseStream(ThreadReturnPromiseStream&& rhs) noexcept : queue(rhs.queue) { rhs.queue = 0; }
	~ThreadReturnPromiseStream() {
		if (queue)
			queue->delPromiseRef();
	}

	void send(const T& value) { queue->send(value); }
	void send(T&& value) { queue->send(std::forward<T>(value)); }
	void sendError(Error error) { queue->sendError(error); }

	ThreadFutureStream<T> getFuture() {
		queue->addFutureRef(); // TODO: (vishesh) - put it in constructor.
		return ThreadFutureStream<T>(queue);
	}

	void operator=(const ThreadReturnPromiseStream& rhs) {
		rhs.queue->addPromiseRef();
		if (queue)
			queue->delPromiseRef();
		queue = rhs.queue;
	}

	void operator=(ThreadReturnPromiseStream&& rhs) noexcept {
		if (queue != rhs.queue) {
			if (queue)
				queue->delPromiseRef();
			queue = rhs.queue;
			rhs.queue = 0;
		}
	}

	int getFutureReferenceCount() const { return queue->getFutureReferenceCount(); }
	int getPromiseReferenceCount() const { return queue->getPromiseReferenceCount(); }

private:
	ThreadNotifiedQueue<T>* queue;
};

// Fixes IDE build.
#ifndef NO_INTELLISENSE
template <class T>
T waitNext(const ThreadFutureStream<T>&);
#endif

#endif
