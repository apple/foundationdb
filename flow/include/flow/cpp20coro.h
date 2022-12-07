#ifndef CPP20_CORO_H
#define CPP20_CORO_H

#pragma once

#if __has_include(<coroutine>)
#include <coroutine>
namespace n_coroutine = ::std;
#elif __has_include(<experimental/coroutine>)
#include <experimental/coroutine>
namespace n_coroutine = ::std::experimental;
#endif

#include "flow/flow.h"
#include "Error.h"

template <class T>
struct CoroutineCallback : public Callback<T> {

	CoroutineCallback(n_coroutine::coroutine_handle<> h) : h(h) {}
	~CoroutineCallback() {}

	virtual void fire(T const&) override { h.resume(); }
	virtual void fire(T&&) override { h.resume(); }

	virtual void error(Error) override { h.resume(); }

private:
	n_coroutine::coroutine_handle<> h;
};

template <class T>
struct CoroutineSingleCallback : public SingleCallback<T> {

	CoroutineSingleCallback(n_coroutine::coroutine_handle<> h) : h(h) {}
	~CoroutineSingleCallback() {
		// if (isSet) {
		// value().~T();
		// }
	}

	virtual void fire(T&& value) override {
		// new (&value_storage) T(std::move(value));
		_value = std::move(value);

		isSet = true;

		h.resume();
	}

	virtual void fire(T const& value) override {
		// std::cerr << "CoroutineSingleCallback::fire " << std::endl;

		// new (&value_storage) T(value);
		_value = value;

		isSet = true;

		h.resume();
	}

	virtual void error(Error e) override { h.resume(); }

	// T& value() { return *(T*)&value_storage; }

	T const& get() const {
		assert(isSet);
		// return *(T const*)&value_storage;
		return _value;
	}

private:
	// typename std::aligned_storage<sizeof(T), __alignof(T)>::type value_storage;

	T _value;

	bool isSet;

	n_coroutine::coroutine_handle<> h;
};

template <typename ReturnValue>
struct Accept;

template <typename ReturnValue, typename... Args>
struct n_coroutine::coroutine_traits<Future<ReturnValue>, Args...> {
	struct promise_type : public Actor<ReturnValue> {
		n_coroutine::coroutine_handle<promise_type> h;

		promise_type() : Actor<ReturnValue>(), h(n_coroutine::coroutine_handle<promise_type>::from_promise(*this)) {}
		~promise_type() {}

		static void* operator new(size_t s) { return allocateFast(s); }
		static void operator delete(void* p, size_t s) { freeFast(s, p); }

		Future<ReturnValue> get_return_object() noexcept { return Future<ReturnValue>(this); }

		n_coroutine::suspend_never initial_suspend() const noexcept { return {}; }

		struct final_awaitable {
			bool destroy;

			final_awaitable(bool destroy) : destroy(destroy) {}

			bool await_ready() const noexcept { return destroy; }
			void await_resume() const noexcept {}
			constexpr void await_suspend(n_coroutine::coroutine_handle<>) const noexcept {}
		};

		final_awaitable final_suspend() noexcept {
			return { !--SAV<ReturnValue>::promises && !SAV<ReturnValue>::futures };
		}

		template <class U>
		void return_value(U&& value) {
			if (SAV<ReturnValue>::futures) {
				SAV<ReturnValue>::send(std::forward<U>(value));
			}
		}

		void unhandled_exception() {
			// The exception should always be type Error.
			try {
				std::rethrow_exception(std::current_exception());
			} catch (const Error& error) {
				// if (Actor<ReturnValue>::actor_wait_state == -1 && error.code() == error_code_operation_cancelled) {
				// 	return;
				// }

				if (SAV<ReturnValue>::futures) {
					SAV<ReturnValue>::sendError(error);
				}
			}
		}

		void cancel() override {
			auto prev_wait_state = Actor<ReturnValue>::actor_wait_state;

			// Set wait state to -1
			Actor<ReturnValue>::actor_wait_state = -1;

			// If the actor is waiting, then resume the coroutine to throw actor_cancelled().
			if (prev_wait_state > 0) {
				h.resume();
			}
		}

		virtual void destroy() override { h.destroy(); }

		template <typename U>
		auto await_transform(const Future<U>& future) {

			struct AwaitableFuture {
				promise_type* pt = nullptr;

				CoroutineCallback<U>* cb = nullptr;

				const Future<U>& future;

				AwaitableFuture(const Future<U>& f, promise_type* pt) : future(f), pt(pt){};

				~AwaitableFuture() {
					if (cb) {
						delete cb;
						cb = nullptr;
					}
				}

				bool await_ready() const { return future.isValid() && future.isReady(); }

				void await_suspend(n_coroutine::coroutine_handle<> h) {
					// Create a coroutine callback if it's the first time being suspended
					if (!cb) {
						cb = new CoroutineCallback<U>(h);
					}

					// Set wait_state and add callback
					pt->actor_wait_state = 1;

					StrictFuture<U> sf = future;

					sf.addCallbackAndClear(cb);
				}
				U const& await_resume() {

					// If actor is cancelled, then throw actor_cancelled()
					if (pt->actor_wait_state < 0) {
						if (cb) {
							cb->remove();
							delete cb;
							cb = nullptr;
						}

						throw actor_cancelled();
					}

					// Actor return from waiting, remove callback and reset wait_state.
					if (pt->actor_wait_state > 0) {
						cb->remove();
						delete cb;
						cb = nullptr;

						pt->actor_wait_state = 0;
					}

					if (future.isError()) {
						throw future.getError();
					} else {
						return future.get();
					}
				}
			};

			return AwaitableFuture{ future, this };
		}

		template <typename U>
		auto await_transform(const FutureStream<U>& futureStream) {
			struct AwaitableFutureStream : FutureStream<U> {

				promise_type* pt = nullptr;

				CoroutineSingleCallback<U>* cb = nullptr;

				AwaitableFutureStream(const FutureStream<U>& fs, promise_type* pt) : FutureStream<U>(fs), pt(pt) {}
				~AwaitableFutureStream() {
					if (cb) {
						delete cb;
						cb = nullptr;
					}
				}

				bool await_ready() const { return FutureStream<U>::isValid() && FutureStream<U>::isReady(); }

				void await_suspend(n_coroutine::coroutine_handle<> h) {

					if (!cb) {
						cb = new CoroutineSingleCallback<U>(h);
					}

					// Set wait_state and add callback
					pt->actor_wait_state = 1;

					FutureStream<U> fs = *this;

					fs.addCallbackAndClear(cb);
				}

				U await_resume() {

					if (pt->actor_wait_state < 0) {
						cb->remove();
						throw actor_cancelled();
					}

					// Callback fired.
					if (pt->actor_wait_state > 0) {
						pt->actor_wait_state = 0;

						cb->remove();

						// Get value from callback
						if (FutureStream<U>::isError()) {
							throw FutureStream<U>::getError();
						} else {
							return cb->get();
						}

					} else { // actor_wait_state == 0. Queue was ready

						if (FutureStream<U>::isError()) {
							throw FutureStream<U>::getError();
						} else {
							return FutureStream<U>::pop();
						}
					}
				}
			};

			return AwaitableFutureStream{ futureStream, this };
		}

		// template <typename U>
		// auto await_transform(Accept<U>& accept) {
		// 	return await_transform(accept.First());
		// }
	};
};

/*
template <typename ReturnValue>
struct Accept {
    struct WhenInterface {
        virtual Future<Void> whenReady(int id, PromiseStream<int>& ready) = 0;
        virtual Future<ReturnValue> run() = 0;
        virtual ~WhenInterface() = default;
    };

    template <typename Type, typename Function>
    struct WhenFuture : public WhenInterface {
        WhenFuture(Future<Type> future, Function func) : future(future), func(func) {}

        Future<Void> whenReady(int id, PromiseStream<int>& ready) override {
            try {
                co_await future;
            } catch (Error& e) {
                if (e.code() == error_code_actor_cancelled) {
                    // std::cerr << "whenReady cancelled" << std::endl;
                    co_return Void();
                }
                ready.send(id);
                co_return Void();
            }
            ready.send(id);

            co_return Void();
        }

        Future<ReturnValue> run() override {
            if constexpr (std::is_invocable_r_v<ReturnValue, Function>) {
                // [](){}
                co_return func();
            } else if constexpr (std::is_invocable_r_v<Future<ReturnValue>, Function>) {
                // []() -> Future {}
                co_return co_await func();
            } else if constexpr (std::is_invocable_r_v<ReturnValue, Function, Type> ||
                                 std::is_invocable_r_v<ReturnValue, Function, Type&>) {
                // [](Type){}
                co_return func(future.get());
            } else if constexpr (std::is_invocable_r_v<Future<ReturnValue>, Function, Type> ||
                                 std::is_invocable_r_v<Future<ReturnValue>, Function, Type&>) {
                // [](Type) -> Future {}
                co_return co_await func(future.get());
            } else {
                []<bool flag = false>() { static_assert(flag, "Incorrect function signature."); }
                ();
            }
        }

        ~WhenFuture() override {
            // std::cerr << "WhenFuture deleted" << std::endl;
        }

        Future<Type> future;
        Function func;
    };

    template <typename Type, typename Function>
    struct WhenFutureStream : public WhenInterface {

        WhenFutureStream(FutureStream<Type>& futureSteam, Function& func) : futureSteam(futureSteam), func(func) {}

        Future<Void> whenReady(int id, PromiseStream<int>& ready) override {
            try {
                value = co_await futureSteam;
                // std::cout << "futureSteam ready " << std::endl;
            } catch (Error& e) {
                if (e.code() == error_code_actor_cancelled) {
                    // std::cerr << "whenReady cancelled" << std::endl;
                    co_return Void();
                }

                throw e;
            }
            ready.send(id);

            co_return Void();
        }

        Future<ReturnValue> run() override {
            if constexpr (std::is_invocable_r_v<ReturnValue, Function, Type> ||
                          std::is_invocable_r_v<ReturnValue, Function, Type&>) {
                // [](Type){}
                co_return func(value);
            } else if constexpr (std::is_invocable_r_v<Future<ReturnValue>, Function, Type> ||
                                 std::is_invocable_r_v<Future<ReturnValue>, Function, Type&>) {
                // [](Type) -> Future {}
                co_return co_await func(value);
            } else if constexpr (std::is_invocable_r_v<ReturnValue, Function>) {
                // [](){}
                co_return func();
            } else if constexpr (std::is_invocable_r_v<Future<ReturnValue>, Function>) {
                // []() -> Future {}
                co_return co_await func();
            } else {
                []<bool flag = false>() { static_assert(flag, "Incorrect function signature."); }
                ();
            }
        }

        ~WhenFutureStream() override {
            // std::cerr << "WhenFuture deleted" << std::endl;
        }

        Type value;
        FutureStream<Type> futureSteam;
        Function func;
    };
    std::vector<WhenInterface*> m_whens;

    Accept() = default;

    Accept(const Accept& rhs) = delete;
    Accept(Accept&& rhs) = delete;

    ~Accept() {
        // std::cout << "delete" << std::endl;
        for (auto p : m_whens) {
            delete p;
        }
    }

    template <typename Type, typename Function>
    Accept& When(Future<Type> future, Function func) {
        m_whens.push_back(new WhenFuture<Type, Function>(future, func));

        return *this;
    }

    template <typename Type, typename Function>
    Accept& When(FutureStream<Type> future, Function func) {
        m_whens.push_back(new WhenFutureStream<Type, Function>(future, func));

        return *this;
    }

    Future<ReturnValue> First() {
        int id;
        {
            PromiseStream<int> readyStream;

            std::vector<Future<Void>> helpers;
            helpers.reserve(m_whens.size());

            for (int i = 0; i < m_whens.size(); ++i) {
                helpers.push_back(m_whens.at(i)->whenReady(i, readyStream));
            }

            id = co_await readyStream.getFuture();
        } // destruct helpers, and cancels callbacks
        // std::cout << "ready id:" << id << std::endl;

        co_return co_await m_whens.at(id)->run();
    }
};

*/

#endif