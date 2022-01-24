/*
 * MultiVersionAssignmentVars.h
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

#ifndef FDBCLIENT_MULTIVERSIONASSIGNMENTVARS_H
#define FDBCLIENT_MULTIVERSIONASSIGNMENTVARS_H
#pragma once

#include "flow/ThreadHelper.actor.h"

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

#endif
