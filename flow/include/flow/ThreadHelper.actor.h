/*
 * ThreadHelper.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated
// version of this file.  In intellisense use the source version.
#include "flow/Error.h"
#include <cstddef>
#if defined(NO_INTELLISENSE) && !defined(FLOW_THREADHELPER_ACTOR_G_H)
#define FLOW_THREADHELPER_ACTOR_G_H
#include "flow/ThreadHelper.actor.g.h"
#elif !defined(FLOW_THREADHELPER_ACTOR_H)
#define FLOW_THREADHELPER_ACTOR_H

#include <utility>

#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Helper actor. Do not use directly!
namespace internal_thread_helper {

ACTOR template <class F>
void doOnMainThreadVoid(Future<Void> signal, F f) {
	wait(signal);
	try {
		f();
	} catch (Error& e) {
	}
}

ACTOR template <class F, class T>
void doOnMainThreadVoid(Future<Void> signal, F f, T* t, Error T::*member) {
	wait(signal);
	if (t && (t->*member).code() != invalid_error_code)
		return;
	try {
		f();
	} catch (Error& e) {
		if (t)
			t->*member = e;
	}
}

} // namespace internal_thread_helper

// onMainThreadVoid runs a functor on the FDB network thread. The value returned by the functor is ignored.
// There is no way to wait for the functor run to finish. For cases where you need a result back or simply need
// to know when the functor has finished running, use `onMainThread`.
//
// WARNING: Successive invocations of `onMainThreadVoid` with different task priorities may not run in the order they
// were called.
//
// WARNING: The error returned in `err` can only be read on the FDB network thread because there is no way to
// order the write to `err` with actions on other threads.
//
// WARNING: The Error member of `T` is accepted as a pointer to a data member so the caller can avoid dereferencing
// `T` until it is initialized on the main thread.
//
// `onMainThreadVoid` is defined here because of the dependency in `ThreadSingleAssignmentVarBase`.
template <class F, class T>
void onMainThreadVoid(F f, T* t, Error T::*member, TaskPriority taskID = TaskPriority::DefaultOnMainThread) {
	Promise<Void> signal;
	internal_thread_helper::doOnMainThreadVoid(signal.getFuture(), f, t, member);
	g_network->onMainThread(std::move(signal), taskID);
}

template <class F>
void onMainThreadVoid(F f, TaskPriority taskID = TaskPriority::DefaultOnMainThread) {
	Promise<Void> signal;
	internal_thread_helper::doOnMainThreadVoid(signal.getFuture(), f);
	g_network->onMainThread(std::move(signal), taskID);
}

class ThreadMultiCallback;

struct ThreadCallback {
	virtual bool canFire(int notMadeActive) const = 0;
	virtual void fire(const Void& unused, int& userParam) = 0;
	virtual void error(const Error&, int& userParam) = 0;
	virtual ThreadCallback* addCallback(ThreadCallback* cb);

	virtual bool clearCallback(ThreadCallback* cb) {
		// If this is the only registered callback this will be called with (possibly) arbitrary pointers
		return false;
	}

	// Note that when a ThreadCallback is destroyed it must have no MultiCallbackHolders, but this can't be
	// asserted on destruction because throwing is not allowed in ~ThreadCallback() and the default destroy()
	// implementation here is never called.
	// However, ThreadMultiCallback::destroy() ensures that no ThreadMultiCallback will be destroyed while
	// still holding a ThreadCallback so the invariant is effectively enforced there.  See
	// ThreadMultiCallback::destroy() for more details.
	virtual void destroy() { UNSTOPPABLE_ASSERT(false); }
	virtual bool isMultiCallback() const { return false; }

	// MultiCallbackHolder is a helper object for ThreadMultiCallback which allows it to store its index
	// within the callback vector inside the ThreadCallback rather than having a map of pointers or
	// some other scheme to store the indices by callback.
	// MultiCallbackHolder objects can form a doubly linked list.
	struct MultiCallbackHolder : public FastAllocated<MultiCallbackHolder> {
		// Construction requires no arguments or all the arguments
		MultiCallbackHolder() : multiCallback(nullptr), index(0), previous(nullptr), next(nullptr) {}
		MultiCallbackHolder(ThreadMultiCallback* multiCallback,
		                    int index,
		                    MultiCallbackHolder* prev,
		                    MultiCallbackHolder* next)
		  : multiCallback(multiCallback), index(0), previous(prev), next(next) {}

		ThreadMultiCallback* multiCallback;
		int index;
		MultiCallbackHolder* previous;
		MultiCallbackHolder* next;
	};

	// firstHolder is both the inline first record of a MultiCallbackHolder and the head of the
	// doubly linked list of MultiCallbackHolder entries.
	MultiCallbackHolder firstHolder;

	// Return a MultiCallbackHolder for the given holder, using the firstHolder if free or allocating
	// a new one.  No check for an existing record for holder is done.
	MultiCallbackHolder* addHolder(ThreadMultiCallback* multiCallback, int index) {
		if (firstHolder.multiCallback == nullptr) {
			firstHolder.multiCallback = multiCallback;
			firstHolder.index = index;
			return &firstHolder;
		}
		firstHolder.next = new MultiCallbackHolder(multiCallback, index, &firstHolder, firstHolder.next);
		return firstHolder.next;
	}

	// Get the MultiCallbackHolder for holder if it exists, or nullptr.
	MultiCallbackHolder* getHolder(ThreadMultiCallback* multiCallback) {
		MultiCallbackHolder* h = &firstHolder;
		while (h != nullptr && h->multiCallback != multiCallback) {
			h = h->next;
		}
		return h;
	}

	// Destroy the given MultiCallbackHolder, freeing it if it is not firstHolder.
	void destroyHolder(MultiCallbackHolder* h) {
		UNSTOPPABLE_ASSERT(h != nullptr);

		// If h is the firstHolder just clear its holder pointer to indicate unusedness
		if (h == &firstHolder) {
			h->multiCallback = nullptr;
		} else {
			// Otherwise unlink h from the doubly linked list and free it
			// h->previous is definitely valid
			h->previous->next = h->next;
			if (h->next) {
				h->next->previous = h->previous;
			}
			delete h;
		}
	}
};

class ThreadMultiCallback final : public ThreadCallback, public FastAllocated<ThreadMultiCallback> {
public:
	ThreadMultiCallback() {}

	ThreadCallback* addCallback(ThreadCallback* callback) override {
		// May be triggered by a waitForAll on a vector with the same future in it more than once
		UNSTOPPABLE_ASSERT(callback->getHolder(this) == nullptr);

		callback->addHolder(this, callbacks.size());
		callbacks.push_back(callback);
		return (ThreadCallback*)this;
	}

	bool clearCallback(ThreadCallback* callback) override {
		MultiCallbackHolder* h = callback->getHolder(this);
		if (h == nullptr) {
			return false;
		}

		UNSTOPPABLE_ASSERT(h->index < callbacks.size() && h->index >= 0);

		// Swap callback with last callback if it isn't the last
		if (h->index != callbacks.size() - 1) {
			callbacks[h->index] = callbacks.back();
			// Update the index of the Holder entry for the moved callback
			callbacks[h->index]->getHolder(this)->index = h->index;
		}

		callbacks.pop_back();
		callback->destroyHolder(h);
		return true;
	}

	bool canFire(int notMadeActive) const override { return true; }

	void fire(const Void& value, int& loopDepth) override {
		if (callbacks.size() > 10000)
			TraceEvent(SevWarn, "LargeMultiCallback").detail("CallbacksSize", callbacks.size());

		UNSTOPPABLE_ASSERT(loopDepth == 0);

		while (callbacks.size()) {
			auto cb = callbacks.back();
			callbacks.pop_back();
			cb->destroyHolder(cb->getHolder(this));
			if (cb->canFire(0)) {
				int ld = 0;
				cb->fire(value, ld);
			}
		}
	}

	void error(const Error& err, int& loopDepth) override {
		if (callbacks.size() > 10000)
			TraceEvent(SevWarn, "LargeMultiCallback").detail("CallbacksSize", callbacks.size());

		UNSTOPPABLE_ASSERT(loopDepth == 0);

		while (callbacks.size()) {
			auto cb = callbacks.back();
			callbacks.pop_back();
			cb->destroyHolder(cb->getHolder(this));
			if (cb->canFire(0)) {
				int ld = 0;
				cb->error(err, ld);
			}
		}
	}

	void destroy() override {
		// This assert assures that all ThreadMultiCallbacks remove themselves as a holder from
		// every ThreadCallback they hold prior to destruction, because if they do not then this
		// assert will fire, so ThreadCallback does not attempt to destroy its MultiCallbackHolder
		// linked list or verify that it is empty.
		UNSTOPPABLE_ASSERT(callbacks.empty());
		delete this;
	}

	bool isMultiCallback() const override { return true; }

private:
	std::vector<ThreadCallback*> callbacks;
};

struct SetCallbackResult {
	enum Result { FIRED, CANNOT_FIRE, CALLBACK_SET };
};

class ThreadSingleAssignmentVarBase {
public:
	enum Status { Unset, NeverSet, Set, ErrorSet }; // order is important
	// volatile long referenceCount;
	ThreadSpinLock mutex;
	std::atomic<Status> status;
	Error error;
	ThreadCallback* callback;

	bool isReady() {
		ThreadSpinLockHolder holder(mutex);
		return isReadyUnsafe();
	}

	bool isError() {
		ThreadSpinLockHolder holder(mutex);
		return isErrorUnsafe();
	}

	int getErrorCode() {
		ThreadSpinLockHolder holder(mutex);
		if (!isReadyUnsafe())
			return error_code_future_not_set;
		if (!isErrorUnsafe())
			return error_code_success;
		return error.code();
	}

	bool canBeSet() {
		ThreadSpinLockHolder holder(mutex);
		return canBeSetUnsafe();
	}

	class BlockCallback : public ThreadCallback {
	public:
		Event ev;

		BlockCallback(ThreadSingleAssignmentVarBase& sav) {
			int ignore = 0;
			sav.callOrSetAsCallback(this, ignore, 0);
			ev.block();
		}

		bool canFire(int notMadeActive) const override { return true; }
		void fire(const Void& unused, int& userParam) override { ev.set(); }
		void error(const Error&, int& userParam) override { ev.set(); }
	};

	void blockUntilReady() {
		if (!isReady()) {
			BlockCallback cb(*this);
		}
	}

	void blockUntilReadyCheckOnMainThread() {
		if (!isReady()) {
			if (g_network->isOnMainThread()) {
				throw blocked_from_network_thread();
			}
			BlockCallback cb(*this);
		}
	}

	ThreadSingleAssignmentVarBase() : status(Unset), callback(NULL), valueReferenceCount(0) {} //, referenceCount(1) {}
	~ThreadSingleAssignmentVarBase() {
		this->mutex.assertNotEntered();

		if (callback)
			callback->destroy();
	}

	virtual void addref() = 0;
	virtual void delref() = 0;

	void send(Never) {
		if (TRACE_SAMPLE())
			TraceEvent(SevSample, "Promise_sendNever").log();
		ThreadSpinLockHolder holder(mutex);
		if (!canBeSetUnsafe())
			ASSERT(false); // Promise fulfilled twice
		this->status = NeverSet;
	}

	// Sends an error through the assignment var if it is not already set. Otherwise does nothing.
	// Returns true if the assignment var was not already set; otherwise returns false.
	bool trySendError(const Error& err) {
		if (TRACE_SAMPLE())
			TraceEvent(SevSample, "Promise_sendError").detail("ErrorCode", err.code());
		this->mutex.enter();
		if (!canBeSetUnsafe()) {
			this->mutex.leave();
			return false;
		}
		error = err;
		status = ErrorSet;
		if (!callback) {
			this->mutex.leave();
			return true;
		}
		auto func = callback;
		if (!callback->isMultiCallback())
			callback = nullptr;

		if (!func->canFire(0)) {
			this->mutex.leave();
		} else {
			this->mutex.leave();

			// Thread safe because status is now ErrorSet and callback is nullptr, meaning than callback cannot change
			int userParam = 0;
			func->error(err, userParam);
		}

		return true;
	}

	// Like trySendError, except that it is assumed the assignment var is not already set.
	void sendError(const Error& err) { ASSERT(trySendError(err)); }

	SetCallbackResult::Result callOrSetAsCallback(ThreadCallback* callback, int& userParam1, int notMadeActive) {
		this->mutex.enter();
		if (isReadyUnsafe()) {
			if (callback->canFire(notMadeActive)) {
				this->mutex.leave();

				// Thread safe because the Future is ready, meaning that status and this->error will not change
				if (status == ErrorSet) {
					auto error = this->error; // Since callback might free this
					callback->error(error, userParam1);
				} else {
					callback->fire(Void(), userParam1);
				}

				return SetCallbackResult::FIRED;
			} else {
				this->mutex.leave();
				return SetCallbackResult::CANNOT_FIRE;
			}
		} else {
			if (this->callback)
				this->callback = this->callback->addCallback(callback);
			else
				this->callback = callback;

			this->mutex.leave();
			return SetCallbackResult::CALLBACK_SET;
		}
	}

	// If this function returns false, then this SAV has already been set and the callback has been or will be called.
	// If this function returns true, then the callback has not and will not be called by this SAV (unless it is set
	// later). This doesn't clear callbacks that are nested multiple levels inside of multi-callbacks
	bool clearCallback(ThreadCallback* cb) {
		this->mutex.enter();

		// If another thread is calling fire in send/sendError, it would be unsafe to clear the callback
		if (isReadyUnsafe()) {
			this->mutex.leave();
			return false;
		}

		// Only clear the callback if it belongs to the caller, because
		// another actor could be waiting on it now!
		bool cleared = false;
		if (callback == cb) {
			callback = nullptr;
			cleared = true;
		} else if (callback != nullptr) {
			cleared = callback->clearCallback(cb);
		}

		this->mutex.leave();
		return cleared;
	}

	void setCancel(Future<Void>&& cf) { cancelFuture = std::move(cf); }

	virtual void cancel() {
		// Cancels the action and decrements the reference count by 1. The if statement is just an optimization. It's ok
		// if we take the "wrong path" if we call this while someone else holds |mutex|. We can't take |mutex| since
		// this is called from releaseMemory. Trying to avoid going to the network thread here is important - without
		// this we see lower throughput on the client for e.g. GRV workloads.
		if (isReadyUnsafe()) {
			delref();
		} else {
			onMainThreadVoid([this]() {
				this->cancelFuture.cancel();
				this->delref();
			});
		}
	}

	void releaseMemory() {
		ThreadSpinLockHolder holder(mutex);
		if (--valueReferenceCount == 0)
			cleanupUnsafe();
	}

private:
	Future<Void> cancelFuture;
	int32_t valueReferenceCount;

protected:
	// The caller of any of these *Unsafe functions should be holding |mutex|
	//
	// |status| is an atomic, so these are not unsafe in the "data race"
	// sense. It appears that there are some class invariants (e.g. that
	// callback should be null if the future is ready), so we should still
	// hold |mutex| when calling these functions. One exception is for
	// cancel, which mustn't try to acquire |mutex| since it's called from
	// releaseMemory while holding the |mutex|. In cancel, we only need to
	// know if there's possibly work to cancel on the main thread, so it's safe to
	// call without holding |mutex|.
	//
	// A bit of history: the original implementation of cancel was not
	// thread safe (in practice it behaved as intended, but TSAN didn't like
	// it, and it was definitely a data race.) The first attempt to fix this[1]
	// was simply to cancel on the main thread, but this turns out to cause
	// a performance regression on the client. Now we simply make |status|
	// atomic so that it behaves (legally) how the original author intended.
	//
	// [1]: https://github.com/apple/foundationdb/pull/3750
	bool isReadyUnsafe() const { return status >= Set; }
	bool isErrorUnsafe() const { return status == ErrorSet; }
	bool canBeSetUnsafe() const { return status == Unset; }

	void addValueReferenceUnsafe() { ++valueReferenceCount; }

	virtual void cleanupUnsafe() {
		if (status != ErrorSet) {
			error = future_released();
			status = ErrorSet;
		}

		valueReferenceCount = 0;
		this->addref();
		cancel();
	}
};

template <class T>
class ThreadSingleAssignmentVar
  : public ThreadSingleAssignmentVarBase,
    /* public FastAllocated<ThreadSingleAssignmentVar<T>>,*/ public ThreadSafeReferenceCounted<
        ThreadSingleAssignmentVar<T>> {
public:
	virtual ~ThreadSingleAssignmentVar() {}

	T value;

	T get() {
		ThreadSpinLockHolder holder(mutex);
		if (!isReadyUnsafe())
			throw future_not_set();
		if (isErrorUnsafe())
			throw error;

		addValueReferenceUnsafe();
		return value;
	}

	void addref() override { ThreadSafeReferenceCounted<ThreadSingleAssignmentVar<T>>::addref(); }

	void delref() override { ThreadSafeReferenceCounted<ThreadSingleAssignmentVar<T>>::delref(); }

	void send(const T& value) {
		if (TRACE_SAMPLE())
			TraceEvent(SevSample, "Promise_send").log();
		this->mutex.enter();
		if (!canBeSetUnsafe()) {
			this->mutex.leave();
			ASSERT(false); // Promise fulfilled twice
		}
		this->value = value; //< Danger: polymorphic operation inside lock
		this->status = Set;
		if (!callback) {
			this->mutex.leave();
			return;
		}

		auto func = callback;
		if (!callback->isMultiCallback())
			callback = nullptr;

		if (!func->canFire(0)) {
			this->mutex.leave();
		} else {
			this->mutex.leave();

			// Thread safe because status is now Set and callback is nullptr, meaning than callback cannot change
			int userParam = 0;
			func->fire(Void(), userParam);
		}
	}

	void cleanupUnsafe() override {
		value = T();
		ThreadSingleAssignmentVarBase::cleanupUnsafe();
	}
};

template <class T>
class ThreadFuture {
public:
	T get() { return sav->get(); }
	T getBlocking() {
		sav->blockUntilReady();
		return sav->get();
	}

	void blockUntilReady() { sav->blockUntilReady(); }

	void blockUntilReadyCheckOnMainThread() { sav->blockUntilReadyCheckOnMainThread(); }

	bool isValid() const { return sav != 0; }
	bool isReady() { return sav->isReady(); }
	bool isError() { return sav->isError(); }
	Error& getError() {
		if (!isError())
			throw future_not_error();

		return sav->error;
	}

	SetCallbackResult::Result callOrSetAsCallback(ThreadCallback* callback, int& userParam1, int notMadeActive) {
		return sav->callOrSetAsCallback(callback, userParam1, notMadeActive);
	}
	bool clearCallback(ThreadCallback* cb) { return sav->clearCallback(cb); }

	void cancel() { extractPtr()->cancel(); }

	ThreadFuture() : sav(0) {}
	explicit ThreadFuture(ThreadSingleAssignmentVar<T>* sav) : sav(sav) {
		// sav->addref();
	}
	ThreadFuture(const ThreadFuture<T>& rhs) : sav(rhs.sav) {
		if (sav)
			sav->addref();
	}
	ThreadFuture(ThreadFuture<T>&& rhs) noexcept : sav(rhs.sav) { rhs.sav = 0; }
	ThreadFuture(const T& presentValue) : sav(new ThreadSingleAssignmentVar<T>()) { sav->send(presentValue); }
	ThreadFuture(Never) : sav(new ThreadSingleAssignmentVar<T>()) {}
	ThreadFuture(const Error& error) : sav(new ThreadSingleAssignmentVar<T>()) { sav->sendError(error); }
	~ThreadFuture() {
		if (sav)
			sav->delref();
	}
	void operator=(const ThreadFuture<T>& rhs) {
		if (rhs.sav)
			rhs.sav->addref();
		if (sav)
			sav->delref();
		sav = rhs.sav;
	}
	void operator=(ThreadFuture<T>&& rhs) noexcept {
		if (sav != rhs.sav) {
			if (sav)
				sav->delref();
			sav = rhs.sav;
			rhs.sav = 0;
		}
	}
	bool operator==(const ThreadFuture& rhs) { return rhs.sav == sav; }
	bool operator!=(const ThreadFuture& rhs) { return rhs.sav != sav; }

	ThreadSingleAssignmentVarBase* getPtr() const { return sav; }
	ThreadSingleAssignmentVarBase* extractPtr() {
		auto* p = sav;
		sav = nullptr;
		return p;
	}

private:
	ThreadSingleAssignmentVar<T>* sav;
};

// A callback class used to convert a ThreadFuture into a Future
template <class T>
struct CompletionCallback final : public ThreadCallback, ReferenceCounted<CompletionCallback<T>> {
	// The thread future being waited on
	ThreadFuture<T> threadFuture;

	// The promise whose future we are triggering when this callback gets called
	Promise<T> promise;

	// Unused
	int userParam;

	// Holds own reference to prevent deletion until callback is fired
	Reference<CompletionCallback<T>> self;

	CompletionCallback(ThreadFuture<T> threadFuture) { this->threadFuture = threadFuture; }

	bool canFire(int notMadeActive) const override { return true; }

	// Trigger the promise
	void fire(const Void& unused, int& userParam) override {
		promise.send(threadFuture.get());
		self.clear();
	}

	// Send the error through the promise
	void error(const Error& e, int& userParam) override {
		promise.sendError(e);
		self.clear();
	}
};

// Converts a ThreadFuture into a Future
// WARNING: This is not actually thread safe!  It can only be safely used from the main thread, on futures which are
// being set on the main thread
// FIXME: does not support cancellation
template <class T>
Future<T> unsafeThreadFutureToFuture(ThreadFuture<T> threadFuture) {
	auto callback = makeReference<CompletionCallback<T>>(threadFuture);
	callback->self = callback;
	threadFuture.callOrSetAsCallback(callback.getPtr(), callback->userParam, 0);
	return callback->promise.getFuture();
}

// A callback waiting on a thread future and will delete itself once fired
template <class T>
struct UtilCallback final : public ThreadCallback {
public:
	UtilCallback(ThreadFuture<T> f, void* userdata) : f(f), userdata(userdata) {}

	bool canFire(int notMadeActive) const override { return true; }
	void fire(const Void& unused, int& userParam) override {
		g_network->onMainThread(Promise<Void>((SAV<Void>*)userdata), TaskPriority::DefaultOnMainThread);
		delete this;
	}
	void error(const Error&, int& userParam) override {
		g_network->onMainThread(Promise<Void>((SAV<Void>*)userdata), TaskPriority::DefaultOnMainThread);
		delete this;
	}
	void destroy() override {}

private:
	ThreadFuture<T> f;
	void* userdata;
};

// The underlying actor that converts ThreadFuture to Future
// Note: should be used from main thread
// The cancellation here works both way
// If the underlying "threadFuture" is cancelled, this actor will get actor_cancelled.
// If instead, this actor is cancelled, we will also cancel the underlying "threadFuture"
// Note: we are required to have unique ownership of the "threadFuture"
ACTOR template <class T>
Future<T> safeThreadFutureToFuture(ThreadFuture<T> threadFuture) {
	Promise<Void> ready;
	Future<Void> onReady = ready.getFuture();
	UtilCallback<T>* callback = new UtilCallback<T>(threadFuture, ready.extractRawPointer());
	int unused = 0;
	threadFuture.callOrSetAsCallback(callback, unused, 0);
	try {
		wait(onReady);
	} catch (Error& e) {
		// broken_promise can be thrown if the network is already shut down
		ASSERT(e.code() == error_code_operation_cancelled || e.code() == error_code_broken_promise);
		// prerequisite: we have exclusive ownership of the threadFuture
		if (e.code() == error_code_operation_cancelled) {
			threadFuture.cancel();
		}
		throw e;
	}
	// threadFuture should be ready
	ASSERT(threadFuture.isReady());
	if (threadFuture.isError())
		throw threadFuture.getError();
	return threadFuture.get();
}

template <class T>
Future<T> safeThreadFutureToFuture(Future<T> future) {
	// Do nothing
	return future;
}

// Helper actor. Do not use directly!
namespace internal_thread_helper {

ACTOR template <class R, class F>
Future<Void> doOnMainThread(Future<Void> signal, F f, ThreadSingleAssignmentVar<R>* result) {
	try {
		wait(signal);
		R r = wait(f());
		result->send(r);
	} catch (Error& e) {
		if (!result->canBeSet()) {
			TraceEvent(SevError, "OnMainThreadSetTwice").errorUnsuppressed(e);
		}
		result->sendError(e);
	}

	ThreadFuture<R> destroyResultAfterReturning(
	    result); // Call result->delref(), but only after our return promise is no longer referenced on this thread
	return Void();
}

} // namespace internal_thread_helper

// `onMainThread` runs a functor returning a `Future` on the main thread, waits for the future, and sends either the
// value returned from the waited `Future` or an error through the `ThreadFuture` returned from the function call.
//
// A workaround for cases where your functor returns a non-`Future` value is to wrap the value in an immediately
// filled `Future`. In cases where the functor returns void, a workaround is to return a `Future<bool>(true)` that
// can be waited on.
//
// TODO: Add SFINAE overloads for functors returning void or a non-Future type.
template <class F>
ThreadFuture<decltype(std::declval<F>()().getValue())> onMainThread(F f) {
	Promise<Void> signal;
	auto returnValue = new ThreadSingleAssignmentVar<decltype(std::declval<F>()().getValue())>();
	returnValue->addref(); // For the ThreadFuture we return
	// TODO: Is this cancellation logic actually needed?
	Future<Void> cancelFuture = internal_thread_helper::doOnMainThread<decltype(std::declval<F>()().getValue()), F>(
	    signal.getFuture(), f, returnValue);
	returnValue->setCancel(std::move(cancelFuture));
	g_network->onMainThread(std::move(signal), TaskPriority::DefaultOnMainThread);
	return ThreadFuture<decltype(std::declval<F>()().getValue())>(returnValue);
}

template <class V>
class ThreadSafeAsyncVar : NonCopyable, public ThreadSafeReferenceCounted<ThreadSafeAsyncVar<V>> {
public:
	struct State {
		State(V value, ThreadFuture<Void> onChange) : value(value), onChange(onChange) {}

		V value;
		ThreadFuture<Void> onChange;
	};

	ThreadSafeAsyncVar() : value(), nextChange(new ThreadSingleAssignmentVar<Void>()) {}
	ThreadSafeAsyncVar(V const& v) : value(v), nextChange(new ThreadSingleAssignmentVar<Void>()) {}

	State get() {
		ThreadSpinLockHolder holder(lock);
		nextChange->addref();
		return State(value, ThreadFuture<Void>(nextChange.getPtr()));
	}

	void set(V const& v, bool triggerIfSame = false) {
		Reference<ThreadSingleAssignmentVar<Void>> trigger(new ThreadSingleAssignmentVar<Void>());

		lock.enter();
		bool changed = this->value != v;
		if (changed || triggerIfSame) {
			std::swap(this->nextChange, trigger);
			this->value = v;
		}
		lock.leave();

		if (changed || triggerIfSame) {
			trigger->send(Void());
		}
	}

private:
	V value;
	Reference<ThreadSingleAssignmentVar<Void>> nextChange;
	ThreadSpinLock lock;
};

// Like a future (very similar to ThreadFuture) but only for computations that already completed. Reuses the SAV's
// implementation for memory management error handling though. Essentially a future that's returned from a synchronous
// computation and guaranteed to be complete.

template <class T>
class ThreadResult {
public:
	T get() { return sav->get(); }

	bool isValid() const { return sav != 0; }
	bool isError() { return sav->isError(); }
	Error& getError() {
		if (!isError())
			throw future_not_error();

		return sav->error;
	}

	ThreadResult() : sav(0) {}
	explicit ThreadResult(ThreadSingleAssignmentVar<T>* sav) : sav(sav) {
		ASSERT(sav->isReady());
		// sav->addref();
	}
	ThreadResult(const ThreadResult<T>& rhs) : sav(rhs.sav) {
		if (sav)
			sav->addref();
	}
	ThreadResult(ThreadResult<T>&& rhs) noexcept : sav(rhs.sav) { rhs.sav = 0; }
	ThreadResult(const T& presentValue) : sav(new ThreadSingleAssignmentVar<T>()) { sav->send(presentValue); }
	ThreadResult(const Error& error) : sav(new ThreadSingleAssignmentVar<T>()) { sav->sendError(error); }
	ThreadResult(const ErrorOr<T> errorOr) : sav(new ThreadSingleAssignmentVar<T>()) {
		if (errorOr.isError()) {
			sav->sendError(errorOr.getError());
		} else {
			sav->send(errorOr.get());
		}
	}
	~ThreadResult() {
		if (sav)
			sav->delref();
	}
	void operator=(const ThreadFuture<T>& rhs) {
		if (rhs.sav)
			rhs.sav->addref();
		if (sav)
			sav->delref();
		sav = rhs.sav;
	}
	void operator=(ThreadFuture<T>&& rhs) noexcept {
		if (sav != rhs.sav) {
			if (sav)
				sav->delref();
			sav = rhs.sav;
			rhs.sav = 0;
		}
	}
	bool operator==(const ThreadResult& rhs) { return rhs.sav == sav; }
	bool operator!=(const ThreadResult& rhs) { return rhs.sav != sav; }

	ThreadSingleAssignmentVarBase* getPtr() const { return sav; }
	ThreadSingleAssignmentVarBase* extractPtr() {
		auto* p = sav;
		sav = nullptr;
		return p;
	}

private:
	ThreadSingleAssignmentVar<T>* sav;
};

#include "flow/unactorcompiler.h"
#endif
