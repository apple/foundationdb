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
		if (isSet) {
			value().~T();
		}
	}

	virtual void fire(T&& value) override {
		std::cerr << "CoroutineSingleCallback::fire " << std::endl;

		new (&value_storage) T(std::move(value));

		isSet = true;

		h.resume();
	}

	virtual void fire(T const& value) override {
		std::cerr << "CoroutineSingleCallback::fire " << std::endl;

		new (&value_storage) T(value);

		isSet = true;

		h.resume();
	}

	virtual void error(Error e) override { h.resume(); }

	T& value() { return *(T*)&value_storage; }

	T const& get() const {
		assert(isSet);
		return *(T const*)&value_storage;
	}

private:
	typename std::aligned_storage<sizeof(T), __alignof(T)>::type value_storage;

	bool isSet;

	n_coroutine::coroutine_handle<> h;
};

template <typename ReturnValue, typename... Args>
struct n_coroutine::coroutine_traits<Future<ReturnValue>, Args...> {
	struct promise_type : public Actor<ReturnValue> {
		n_coroutine::coroutine_handle<promise_type> h;

		promise_type() : Actor<ReturnValue>(), h(n_coroutine::coroutine_handle<promise_type>::from_promise(*this)) {
			// std::cerr << "promise_type()" << std::endl;
		}
		~promise_type() {
			// std::cout << "~promise_type() " << SAV<ReturnValue>::getFutureReferenceCount() << " "
			//           << SAV<ReturnValue>::getPromiseReferenceCount() << std::endl;
		}

		// TODO: FastAlloc
		// Use the global new and delete operator for now. The state and the variables of the coroutine are also
		// allocated in this promise_type struct, resulting in sizeof(promise_type) != size_t s . So we cannot use the
		// FastAlloc class.
		static void* operator new(size_t s) {
			// std::cerr << "promise_type::new(" << s << ")" << std::endl;
			return ::malloc(s);
		}
		static void operator delete(void* p) {
			// std::cerr << "promise_type::delete()" << std::endl;
			::free(p);
		}

		Future<ReturnValue> get_return_object() noexcept { return Future<ReturnValue>(this); }

		n_coroutine::suspend_never initial_suspend() const noexcept { return {}; }
		n_coroutine::suspend_always final_suspend() noexcept {
			// std::cerr << "final_suspend()" << SAV<ReturnValue>::getFutureReferenceCount() << " "
			//           << SAV<ReturnValue>::getPromiseReferenceCount() << std::endl;

			SAV<ReturnValue>::delPromiseRef();

			return {};
		}

		template <class U>
		void return_value(U&& value) {
			// std::cerr << "return_value()" << std::endl;
			if (SAV<ReturnValue>::futures) {
				SAV<ReturnValue>::send(std::forward<U>(value));
			}
		}

		void unhandled_exception() {
			// std::cerr << "unhandled_exception()" << std::endl;

			// The exception should always be type Error, otherwise crash the program.
			try {
				std::rethrow_exception(std::current_exception());
			} catch (const Error& error) {
				SAV<ReturnValue>::sendError(error);
			}
		}

		template <typename U>
		auto await_transform(Future<U> future) {

			struct AwaitableFuture : Future<U> {
				promise_type* pt = nullptr;

				CoroutineCallback<U>* cb = nullptr;

				AwaitableFuture(Future<U> f, promise_type* pt) : Future<U>(f), pt(pt){};

				~AwaitableFuture() {
					if (cb) {
						delete cb;
						cb = nullptr;
					}
				}

				bool await_ready() const {
					// std::cerr << "await_ready " << std::endl;
					return Future<U>::isValid() && Future<U>::isReady();
				}

				void await_suspend(n_coroutine::coroutine_handle<> h) {
					// std::cerr << "await_suspend" << std::endl;

					// Create a coroutine callback if it's the first time being suspended
					if (!cb) {
						cb = new CoroutineCallback<U>(h);
					}

					// Set wait_state and add callback
					pt->actor_wait_state = 1;

					StrictFuture<U> sf = *this;

					sf.addCallbackAndClear(cb);
				}
				U const& await_resume() {

					// std::cerr << "await_resume wait_state:" << pt->actor_wait_state << std::endl;

					// If actor is cancelled, then throw actor_cancelled()
					if (pt->actor_wait_state < 0) {
						cb->remove();

						throw actor_cancelled();
					}

					// Actor return from waiting, remove callback and reset wait_state.
					if (pt->actor_wait_state > 0) {
						cb->remove();
						pt->actor_wait_state = 0;
					}

					if (Future<U>::isError()) {
						// std::cerr << "await_resume error" << std::endl;
						throw Future<U>::getError();
					} else {
						// std::cerr << "await_resume value" << std::endl;
						return Future<U>::get();
					}
				}
			};

			return AwaitableFuture{ future, this };
		}

		template <typename U>
		auto await_transform(FutureStream<U> futureStream) {
			struct AwaitableFutureStream : FutureStream<U> {

				promise_type* pt = nullptr;

				CoroutineSingleCallback<U>* cb = nullptr;

				AwaitableFutureStream(FutureStream<U> fs, promise_type* pt) : FutureStream<U>(fs), pt(pt) {}
				~AwaitableFutureStream() {
					std::cout << " ~AwaitableFutureStream() " << std::endl;
					if (cb) {
						delete cb;
						cb = nullptr;
					}
				}

				bool await_ready() const {
					std::cerr << "await_ready() " << (FutureStream<U>::isValid() && FutureStream<U>::isReady())
					          << std::endl;
					return FutureStream<U>::isValid() && FutureStream<U>::isReady();
				}

				void await_suspend(n_coroutine::coroutine_handle<> h) {
					std::cerr << "await_suspend" << std::endl;

					if (!cb) {
						cb = new CoroutineSingleCallback<U>(h);
					}

					// Set wait_state and add callback
					pt->actor_wait_state = 1;

					FutureStream<U> fs = *this;

					fs.addCallbackAndClear(cb);
				}

				U await_resume() {
					std::cerr << "await_resume" << std::endl;

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
		void cancel() override {
			// std::cerr << "cancel()" << std::endl;

			auto prev_wait_state = Actor<ReturnValue>::actor_wait_state;

			// Set wait state to -1
			Actor<ReturnValue>::actor_wait_state = -1;

			// If the actor is waiting, then resume the coroutine to throw actor_cancelled().
			if (prev_wait_state > 0) {
				h.resume();
			}
		}

		virtual void destroy() override {
			// std::cerr << "destroy()" << std::endl;

			h.destroy();
		}
	};
};

#endif