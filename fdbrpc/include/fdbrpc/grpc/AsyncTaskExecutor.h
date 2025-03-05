/**
 * AsyncTaskExecutor.h
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

#ifdef FLOW_GRPC_ENABLED
#ifndef FDBRPC_FLOW_GRPC_THREAD_POOL_H
#define FDBRPC_FLOW_GRPC_THREAD_POOL_H

#include "flow/flow.h"
#include "flow/IThreadPool.h"

// Checks whether `Func` return type is `void`. FDB's `Void` will not return true.
template <typename Func>
concept IsVoidReturn = std::is_void_v<std::invoke_result_t<Func>>;

// `AsyncTaskExecutor` is a lightweight wrapper around `IThreadPool`, designed to provide
// functionality similar to `std::async` or `asio::post`. It allows asynchronous task execution
// without directly managing threads, making it easier to offload work to a thread pool. . The API
// is designed to be easy-to-use and integrates well with FDB's Flow primitives.
//
// - All tasks must be posted from FDB's main thread.
// - Using this can potentially break simulation. Setting `num_threads=` to 1 in simulation should
//   serialize all the tasks and will keep the execution deterministic.
//
// Usage:
//
//     // Create an instance.
//     AsyncTaskExecutor exc(4 /* num_threads */);
//
//     // Schedule a task using `post`. This task doesn't return a value, neither we
//     // need to wait for completion.
//     exc->post([]() {
//        // Do some expensive operations.
//     });
//
//     // For waiting for task to finish just return Void.
//     Future<Void> f = exc->post([]() -> Void { ... do stuff .. });
//     wait(f);  --  or --  co_await f;

//     // Schedule a task that returns a value using `post`.
//     Object res = co_await exc->post([]() -> Object {
//       // Do some expensive operations.
//       return res_obj;
//     });
//
// - TODO: Move this to more standard location inside codebase.
// - TODO: Remove AsyncTaskThread which has similar purpose however implemented differently?
class AsyncTaskExecutor {
public:
	explicit AsyncTaskExecutor(int num_threads) {
		for (int ii = 0; ii < num_threads; ++ii) {
			pool_->addThread(new Receiver());
		}
	}
	~AsyncTaskExecutor() { pool_->stop(success()); }

	// Schedules a non-void function for asynchronous execution in a thread pool.
	//
	// This function posts a callable task (function, lambda, function) to a thread pool for
	// execution. The task must return a non-void type. This function must be called from
	// main thread.
	//
	// Returns a `Future<R>` object representing the return value of task.
	template <typename Func>
	    requires(!IsVoidReturn<Func>)
	[[nodiscard]] auto post(Func&& task) -> Future<typename std::invoke_result<Func>::type> {
		ASSERT_WE_THINK(g_network->isOnMainThread());
		auto action = new Action<Func>(std::forward<Func>(task));
		pool_->post(action);
		return action->getFuture();
	}

	// Schedules a function that returns void for asynchronous execution in a thread pool.
	//
	// This function posts a callable task (function, lambda, function) to a thread pool for
	// execution. Unlike the other overload, this version is intended for tasks that do not return a
	// result, when the caller does not need the result, or when the caller prefers to use custom
	// primitives to interact with the underlying task (e.g., streaming).
	//
	// Returns a `Future<R>` object representing the return value of task.
	template <typename Func>
	    requires(IsVoidReturn<Func>)
	void post(Func&& task) {
		ASSERT_WE_THINK(g_network->isOnMainThread());
		auto action = new Action<Func>(std::forward<Func>(task));
		pool_->post(action);
	}

private:
	// Serves no purpose for us, but needed for `IThreadPool`.
	struct Receiver : IThreadPoolReceiver {
		void init() {}
	};

	template <typename Func, class __Enable = void>
	struct Action;

	Reference<IThreadPool> pool_ = createGenericThreadPool();
};

//-- Internal types.

// `ThreadAction` implementation for tasks that return non-void values.
template <typename Func>
struct AsyncTaskExecutor::Action<Func, typename std::enable_if_t<!IsVoidReturn<Func>>> : ThreadAction {
	using Ret = std::invoke_result<Func>::type;

	Action(Func&& fn) : fn_(std::forward<Func>(fn)) {}

	void operator()(IThreadPoolReceiver* action) override {
		promise_.send(fn_());
		delete this;
	}

	// Returns `Future` representing the value returned by `fn_`.
	Future<Ret> getFuture() { return promise_.getFuture(); }

	// `promise_` will automatically send `broken_promise` to its futures.
	void cancel() override {}

	// TODO:
	double getTimeEstimate() const override { return 0.1; }

private:
	Func fn_;
	ThreadReturnPromise<Ret> promise_;
};

// `ThreadAction` implementation for tasks that return void.
template <typename Func>
struct AsyncTaskExecutor::Action<Func, typename std::enable_if_t<IsVoidReturn<Func>>> : ThreadAction {
	using Ret = std::invoke_result<Func>::type;

	Action(Func&& fn) : fn_(std::forward<Func>(fn)) {}

	void operator()(IThreadPoolReceiver* action) override {
		fn_();
		delete this;
	}

	// `promise_` will automatically send `broken_promise` to its futures.
	void cancel() override {}

	// TODO:
	double getTimeEstimate() const override { return 0.1; }

private:
	Func fn_;
};

#endif // FDBRPC_FLOW_GRPC_THREAD_POOL_H
#endif // FLOW_GRPC_ENABLED
