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
		// std::cerr << "CoroutineSingleCallback::fire " << std::endl;

		new (&value_storage) T(std::move(value));

		isSet = true;

		h.resume();
	}

	virtual void fire(T const& value) override {
		// std::cerr << "CoroutineSingleCallback::fire " << std::endl;

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
			// std::cerr << "promise_type() " << this << std::endl;
		}
		~promise_type() {
			// std::cout << "~promise_type() " << this << " " << SAV<ReturnValue>::getFutureReferenceCount() << " "
			//           << SAV<ReturnValue>::getPromiseReferenceCount() << std::endl;
		}

		// TODO: FastAlloc
		// Use the global new and delete operator for now. The state and the variables of the coroutine are also
		// allocated in this promise_type struct, resulting in sizeof(promise_type) != size_t s . So we cannot use the
		// FastAlloc class.
		static void* operator new(size_t s) {
			// std::cerr << "promise_type::new(" << s << ")" << std::endl;
			// return ::malloc(s);
			return allocateFast(s);
		}
		static void operator delete(void* p, size_t s) {
			// std::cerr << "promise_type::delete(" << p << " " << s << ")" << std::endl;
			// ::free(p);
			freeFast(s, p);
		}

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
			// std::cerr << "final_suspend() " << this << " " << SAV<ReturnValue>::getFutureReferenceCount() << " "
			//   << SAV<ReturnValue>::getPromiseReferenceCount() << " " << Actor<ReturnValue>::actor_wait_state
			//   << std::endl;
			return { !--SAV<ReturnValue>::promises && !SAV<ReturnValue>::futures };
		}

		template <class U>
		void return_value(U&& value) {
			// std::cerr << "return_value() " << this << " " << SAV<ReturnValue>::futures << " "
			//           << SAV<ReturnValue>::promises << std::endl;

			if (SAV<ReturnValue>::futures) {
				SAV<ReturnValue>::send(std::forward<U>(value));
			}
		}

		void unhandled_exception() {
			// std::cerr << "unhandled_exception() " << this << " " << SAV<ReturnValue>::futures << " "
			//   << SAV<ReturnValue>::promises << std::endl;

			// The exception should always be type Error.
			try {
				std::rethrow_exception(std::current_exception());
			} catch (const Error& error) {
				if (error.code() == error_code_operation_cancelled)
					return;

				if (SAV<ReturnValue>::futures) {
					SAV<ReturnValue>::sendError(error);
				}
			}
		}

		void cancel() override {
			// std::cerr << "cancel() " << this << std::endl;

			auto prev_wait_state = Actor<ReturnValue>::actor_wait_state;

			// Set wait state to -1
			Actor<ReturnValue>::actor_wait_state = -1;

			// If the actor is waiting, then resume the coroutine to throw actor_cancelled().
			if (prev_wait_state > 0) {
				h.resume();
			}
		}

		virtual void destroy() override {
			// std::cerr << "destroy() " << this << std::endl;

			h.destroy();
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
					// std::cout << " ~AwaitableFutureStream() " << std::endl;
					if (cb) {
						delete cb;
						cb = nullptr;
					}
				}

				bool await_ready() const {
					// std::cerr << "await_ready() "
					//           << (FutureStream<U>::isValid() && FutureStream<U>::isReady())
					//           << std::endl;
					return FutureStream<U>::isValid() && FutureStream<U>::isReady();
				}

				void await_suspend(n_coroutine::coroutine_handle<> h) {
					// std::cerr << "await_suspend" << std::endl;

					if (!cb) {
						cb = new CoroutineSingleCallback<U>(h);
					}

					// Set wait_state and add callback
					pt->actor_wait_state = 1;

					FutureStream<U> fs = *this;

					fs.addCallbackAndClear(cb);
				}

				U await_resume() {
					// std::cerr << "await_resume" << std::endl;

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
	};
};

#endif