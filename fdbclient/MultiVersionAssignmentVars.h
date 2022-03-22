/*
 * MultiVersionAssignmentVars.h
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

#ifndef FDBCLIENT_MULTIVERSIONASSIGNMENTVARS_H
#define FDBCLIENT_MULTIVERSIONASSIGNMENTVARS_H
#pragma once

#include "flow/ThreadHelper.actor.h"

template <class T>
class AbortableSingleAssignmentVar final : public ThreadSingleAssignmentVar<T>, public ThreadCallback {
public:
	AbortableSingleAssignmentVar(ThreadFuture<T> future, ThreadFuture<Void> abortSignal)
	  : future(future), abortSignal(abortSignal), hasBeenSet(false), callbacksCleared(false) {
		int userParam;

		ThreadSingleAssignmentVar<T>::addref();
		ThreadSingleAssignmentVar<T>::addref();

		// abortSignal comes first, because otherwise future could immediately call fire/error and attempt to remove
		// this callback from abortSignal prematurely
		abortSignal.callOrSetAsCallback(this, userParam, 0);
		future.callOrSetAsCallback(this, userParam, 0);
	}

	void cancel() override {
		cancelCallbacks();
		ThreadSingleAssignmentVar<T>::cancel();
	}

	void cleanupUnsafe() override {
		future.getPtr()->releaseMemory();
		ThreadSingleAssignmentVar<T>::cleanupUnsafe();
	}

	bool canFire(int notMadeActive) const override { return true; }

	void fire(const Void& unused, int& userParam) override {
		lock.enter();
		if (!hasBeenSet) {
			hasBeenSet = true;
			lock.leave();

			if (future.isReady() && !future.isError()) {
				ThreadSingleAssignmentVar<T>::send(future.get());
			} else if (abortSignal.isReady()) {
				ThreadSingleAssignmentVar<T>::sendError(cluster_version_changed());
			} else {
				ASSERT(false);
			}
		} else {
			lock.leave();
		}

		cancelCallbacks();
		ThreadSingleAssignmentVar<T>::delref();
	}

	void error(const Error& e, int& userParam) override {
		ASSERT(future.isError());
		lock.enter();
		if (!hasBeenSet) {
			hasBeenSet = true;
			lock.leave();

			ThreadSingleAssignmentVar<T>::sendError(future.getError());
		} else {
			lock.leave();
		}

		cancelCallbacks();
		ThreadSingleAssignmentVar<T>::delref();
	}

private:
	ThreadFuture<T> future;
	ThreadFuture<Void> abortSignal;

	ThreadSpinLock lock;
	bool hasBeenSet;
	bool callbacksCleared;

	void cancelCallbacks() {
		lock.enter();

		if (!callbacksCleared) {
			callbacksCleared = true;
			lock.leave();

			future.getPtr()->addref(); // Cancel will delref our future, but we don't want to destroy it until this
			                           // callback gets destroyed
			future.getPtr()->cancel();

			if (abortSignal.clearCallback(this)) {
				ThreadSingleAssignmentVar<T>::delref();
			}
		} else {
			lock.leave();
		}
	}
};

template <class T>
ThreadFuture<T> abortableFuture(ThreadFuture<T> f, ThreadFuture<Void> abortSignal) {
	return ThreadFuture<T>(new AbortableSingleAssignmentVar<T>(f, abortSignal));
}

template <class T>
class DLThreadSingleAssignmentVar final : public ThreadSingleAssignmentVar<T> {
public:
	DLThreadSingleAssignmentVar(Reference<FdbCApi> api,
	                            FdbCApi::FDBFuture* f,
	                            std::function<T(FdbCApi::FDBFuture*, FdbCApi*)> extractValue)
	  : api(api), f(f), extractValue(extractValue), futureRefCount(1) {
		ThreadSingleAssignmentVar<T>::addref();
		api->futureSetCallback(f, &futureCallback, this);
	}

	~DLThreadSingleAssignmentVar() override {
		lock.assertNotEntered();
		if (f) {
			ASSERT_ABORT(futureRefCount == 1);
			api->futureDestroy(f);
		}
	}

	bool addFutureRef() {
		lock.enter();
		bool destroyed = futureRefCount == 0;
		if (!destroyed) {
			++futureRefCount;
		}
		lock.leave();

		return !destroyed;
	}

	bool delFutureRef() {
		lock.enter();
		if (futureRefCount == 0) {
			lock.leave();
			return true;
		}

		bool destroyNow = (--futureRefCount == 0);
		lock.leave();

		if (destroyNow) {
			api->futureDestroy(f);
			f = nullptr;
		}

		return destroyNow;
	}

	void cancel() override {
		if (addFutureRef()) {
			api->futureCancel(f);
			delFutureRef();
		}

		ThreadSingleAssignmentVar<T>::cancel();
	}

	void cleanupUnsafe() override {
		delFutureRef();
		ThreadSingleAssignmentVar<T>::cleanupUnsafe();
	}

	void apply() {
		FdbCApi::fdb_error_t error = addFutureRef() ? api->futureGetError(f) : error_code_operation_cancelled;
		if (error != 0) {
			delFutureRef();
			ThreadSingleAssignmentVar<T>::sendError(Error(error));
		} else {
			T val = extractValue(f, api.getPtr());
			delFutureRef();
			ThreadSingleAssignmentVar<T>::send(val);
		}

		ThreadSingleAssignmentVar<T>::delref();
	}

	static void futureCallback(FdbCApi::FDBFuture* f, void* param) {
		auto sav = (DLThreadSingleAssignmentVar<T>*)param;

		if (MultiVersionApi::api->callbackOnMainThread) {
			onMainThreadVoid([sav]() { sav->apply(); }, nullptr);
		} else {
			sav->apply();
		}
	}

private:
	const Reference<FdbCApi> api;
	FdbCApi::FDBFuture* f;
	const std::function<T(FdbCApi::FDBFuture* f, FdbCApi* api)> extractValue;
	ThreadSpinLock lock;

	int futureRefCount;
};

template <class T>
ThreadFuture<T> toThreadFuture(Reference<FdbCApi> api,
                               FdbCApi::FDBFuture* f,
                               std::function<T(FdbCApi::FDBFuture* f, FdbCApi* api)> extractValue) {
	return ThreadFuture<T>(new DLThreadSingleAssignmentVar<T>(api, f, extractValue));
}

template <class S, class T>
class MapSingleAssignmentVar final : public ThreadSingleAssignmentVar<T>, ThreadCallback {
public:
	MapSingleAssignmentVar(ThreadFuture<S> source, std::function<ErrorOr<T>(ErrorOr<S>)> mapValue)
	  : source(source), mapValue(mapValue) {
		ThreadSingleAssignmentVar<T>::addref();

		int userParam;
		source.callOrSetAsCallback(this, userParam, 0);
	}

	void cancel() override {
		source.getPtr()->addref(); // Cancel will delref our future, but we don't want to destroy it until this callback
		                           // gets destroyed
		source.getPtr()->cancel();
		ThreadSingleAssignmentVar<T>::cancel();
	}

	void cleanupUnsafe() override {
		source.getPtr()->releaseMemory();
		ThreadSingleAssignmentVar<T>::cleanupUnsafe();
	}

	bool canFire(int notMadeActive) const override { return true; }

	void fire(const Void& unused, int& userParam) override {
		sendResult(mapValue(source.get()));
		ThreadSingleAssignmentVar<T>::delref();
	}

	void error(const Error& e, int& userParam) override {
		sendResult(mapValue(source.getError()));
		ThreadSingleAssignmentVar<T>::delref();
	}

private:
	ThreadFuture<S> source;
	const std::function<ErrorOr<T>(ErrorOr<S>)> mapValue;

	void sendResult(ErrorOr<T> result) {
		if (result.isError()) {
			ThreadSingleAssignmentVar<T>::sendError(result.getError());
		} else {
			ThreadSingleAssignmentVar<T>::send(result.get());
		}
	}
};

template <class S, class T>
ThreadFuture<T> mapThreadFuture(ThreadFuture<S> source, std::function<ErrorOr<T>(ErrorOr<S>)> mapValue) {
	return ThreadFuture<T>(new MapSingleAssignmentVar<S, T>(source, mapValue));
}

template <class S, class T>
class FlatMapSingleAssignmentVar final : public ThreadSingleAssignmentVar<T>, ThreadCallback {
public:
	FlatMapSingleAssignmentVar(ThreadFuture<S> source, std::function<ErrorOr<ThreadFuture<T>>(ErrorOr<S>)> mapValue)
	  : source(source), cancelled(false), released(false), mapValue(mapValue) {
		ThreadSingleAssignmentVar<T>::addref();

		int userParam;
		source.callOrSetAsCallback(this, userParam, 0);
	}

	void cancel() override {
		source.getPtr()->addref(); // Cancel will delref our future, but we don't want to destroy it until this callback
		                           // gets destroyed
		source.getPtr()->cancel();

		lock.enter();
		cancelled = true;
		if (mappedFuture.isValid()) {
			lock.leave();
			mappedFuture.getPtr()->addref();
			mappedFuture.getPtr()->cancel();
		} else {
			lock.leave();
		}

		ThreadSingleAssignmentVar<T>::cancel();
	}

	void cleanupUnsafe() override {
		source.getPtr()->releaseMemory();

		lock.enter();
		released = true;
		if (mappedFuture.isValid()) {
			lock.leave();
			mappedFuture.getPtr()->releaseMemory();
		} else {
			lock.leave();
		}

		ThreadSingleAssignmentVar<T>::cleanupUnsafe();
	}

	bool canFire(int notMadeActive) const override { return true; }

	void fire(const Void& unused, int& userParam) override {
		if (mappedFuture.isValid()) {
			sendResult(mappedFuture.get());
		} else {
			setMappedFuture(mapValue(source.get()));
		}

		ThreadSingleAssignmentVar<T>::delref();
	}

	void error(const Error& e, int& userParam) override {
		if (mappedFuture.isValid()) {
			sendResult(mappedFuture.getError());
		} else {
			setMappedFuture(mapValue(source.getError()));
		}

		ThreadSingleAssignmentVar<T>::delref();
	}

private:
	ThreadFuture<S> source;
	ThreadFuture<T> mappedFuture;
	bool cancelled;
	bool released;
	const std::function<ErrorOr<ThreadFuture<T>>(ErrorOr<S>)> mapValue;

	ThreadSpinLock lock;

	void setMappedFuture(ErrorOr<ThreadFuture<T>> f) {
		if (f.isError()) {
			sendResult(f.getError());
		} else {
			lock.enter();
			mappedFuture = f.get();
			bool doCancel = cancelled;
			bool doRelease = released;
			lock.leave();

			if (doCancel) {
				mappedFuture.getPtr()->addref();
				mappedFuture.getPtr()->cancel();
			}
			if (doRelease) {
				mappedFuture.getPtr()->releaseMemory();
			}

			int userParam;
			ThreadSingleAssignmentVar<T>::addref();
			mappedFuture.callOrSetAsCallback(this, userParam, 0);
		}
	}

	void sendResult(ErrorOr<T> result) {
		if (result.isError()) {
			ThreadSingleAssignmentVar<T>::sendError(result.getError());
		} else {
			ThreadSingleAssignmentVar<T>::send(result.get());
		}
	}
};

template <class S, class T>
ThreadFuture<T> flatMapThreadFuture(ThreadFuture<S> source,
                                    std::function<ErrorOr<ThreadFuture<T>>(ErrorOr<S>)> mapValue) {
	return ThreadFuture<T>(new FlatMapSingleAssignmentVar<S, T>(source, mapValue));
}

#endif
