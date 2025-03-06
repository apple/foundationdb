/*
 * IThreadPool.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_ITHREADPOOL_H
#define FLOW_ITHREADPOOL_H
#pragma once

#include <string_view>

#include "flow/flow.h"

// The IThreadPool interface represents a thread pool suitable for doing blocking disk-intensive work
// (as opposed to a one-thread-per-core pool for CPU-intensive work)

// Normally a thread pool is created by g_network->createThreadPool(), and different networks may have
// different implementations (for example, in simulation the thread pool will only be simulated and will
// not actually create threads).

// Once created, the caller must add at least one thread with addThread(), passing a user-defined instance
// of IThreadPoolReceiver that will do the work.  init() is called on it on the new thread

// Then the caller calls post() as many times as desired.  Each call will invoke the given thread action on
// any one of the thread pool receivers passed to addThread().

// TypedAction<> is a utility subclass to make it easier to create thread actions and receivers.

// ThreadReturnPromise<> can be safely use to pass return values from thread actions back to the g_network thread

class IThreadPoolReceiver {
public:
	virtual ~IThreadPoolReceiver() {}
	virtual void init() = 0;
};

struct ThreadAction {
	virtual void operator()(IThreadPoolReceiver*) = 0; // self-destructs
	virtual void cancel() = 0;
	virtual double getTimeEstimate() const = 0; // for simulation
};
typedef ThreadAction* PThreadAction;

class IThreadPool {
public:
	virtual ~IThreadPool() {}
	virtual Future<Void> getError() const = 0; // asynchronously throws an error if there is an internal error
	virtual void addThread(IThreadPoolReceiver* userData, const char* name = nullptr) = 0;
	virtual void post(PThreadAction action) = 0;
	virtual Future<Void> stop(Error const& e = success()) = 0;
	virtual bool isCoro() const { return false; }
	virtual void addref() = 0;
	virtual void delref() = 0;
};

template <class Object, class ActionType>
class TypedAction : public ThreadAction {
public:
	void operator()(IThreadPoolReceiver* p) override {
		Object* o = (Object*)p;
		o->action(*(ActionType*)this);
		delete (ActionType*)this;
	}
	void cancel() override { delete (ActionType*)this; }
};

template <class T>
class ThreadReturnPromise : NonCopyable {
public:
	ThreadReturnPromise() {}
	ThreadReturnPromise(const ThreadReturnPromise& p) = delete;
	ThreadReturnPromise(ThreadReturnPromise&& other) : promise(std::move(other.promise)) {}

	~ThreadReturnPromise() {
		if (promise.isValid())
			sendError(broken_promise());
	}

	Future<T> getFuture() const { // Call only on the originating thread!
		return promise.getFuture();
	}

	template <class U>
	void send(U&& t) { // Can be called safely from another thread.  Call send or sendError at most once.
		Promise<Void> signal;
		tagAndForward(&promise, t, signal.getFuture());
		g_network->onMainThread(std::move(signal),
		                        g_network->isOnMainThread() ? incrementPriorityIfEven(g_network->getCurrentTask())
		                                                    : TaskPriority::DefaultOnMainThread);
	}
	void sendError(Error e) { // Can be called safely from another thread.  Call send or sendError at most once.
		Promise<Void> signal;
		tagAndForwardError(&promise, e, signal.getFuture());
		g_network->onMainThread(std::move(signal),
		                        g_network->isOnMainThread() ? incrementPriorityIfEven(g_network->getCurrentTask())
		                                                    : TaskPriority::DefaultOnMainThread);
	}
	bool isValid() const { return promise.isValid(); }
	bool canBeSet() const { return promise.canBeSet(); }

	int getFutureReferenceCount() const { return promise.getFutureReferenceCount(); }
	int getPromiseReferenceCount() const { return promise.getPromiseReferenceCount(); }

private:
	Promise<T> promise;
};

template <class T>
class ThreadReturnPromiseStream : NonCopyable {
public:
	ThreadReturnPromiseStream() {}
	ThreadReturnPromiseStream(const ThreadReturnPromiseStream& p) = delete;
	ThreadReturnPromiseStream(ThreadReturnPromiseStream&& other) : promiseStream(std::move(other.promiseStream)) {}

	~ThreadReturnPromiseStream() {}

	FutureStream<T> getFuture() { // Call only on the originating thread!
		return promiseStream.getFuture();
	}

	void send(T const& t) { // Can be called safely from another thread.
		Promise<Void> signal;
		tagAndForward(&promiseStream, t, signal.getFuture());
		g_network->onMainThread(std::move(signal),
		                        g_network->isOnMainThread() ? incrementPriorityIfEven(g_network->getCurrentTask())
		                                                    : TaskPriority::DefaultOnMainThread);
	}

	void sendError(Error const& e) { // Can be called safely from another thread.
		Promise<Void> signal;
		tagAndForwardError(&promiseStream, e, signal.getFuture());
		g_network->onMainThread(std::move(signal),
		                        g_network->isOnMainThread() ? incrementPriorityIfEven(g_network->getCurrentTask())
		                                                    : TaskPriority::DefaultOnMainThread);
	}

	int getFutureReferenceCount() const { return promiseStream.getFutureReferenceCount(); }
	int getPromiseReferenceCount() const { return promiseStream.getPromiseReferenceCount(); }

private:
	PromiseStream<T> promiseStream;
};

Reference<IThreadPool> createGenericThreadPool(int stackSize = 0, int pri = 10);

class DummyThreadPool final : public IThreadPool, ReferenceCounted<DummyThreadPool> {
public:
	~DummyThreadPool() override {}
	DummyThreadPool() : thread(nullptr) {}
	Future<Void> getError() const override { return errors.getFuture(); }
	void addThread(IThreadPoolReceiver* userData, const char* name = nullptr) override {
		ASSERT(!thread);
		thread = userData;
		userData->init();
	}
	void post(PThreadAction action) override {
		try {
			(*action)(thread);
		} catch (Error& e) {
			errors.sendError(e);
		} catch (...) {
			errors.sendError(unknown_error());
		}
	}
	Future<Void> stop(Error const& e) override { return Void(); }
	void addref() override { ReferenceCounted<DummyThreadPool>::addref(); }
	void delref() override { ReferenceCounted<DummyThreadPool>::delref(); }

private:
	IThreadPoolReceiver* thread;
	Promise<Void> errors;
};

#endif
