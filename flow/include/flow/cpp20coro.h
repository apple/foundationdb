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

	n_coroutine::coroutine_handle<> h;

	CoroutineCallback(n_coroutine::coroutine_handle<> h) : h(h) {
		// std::cerr << "CoroutineCallback" << std::endl;
	}
	~CoroutineCallback() {
		// std::cerr << "~CoroutineCallback" << std::endl;
	}

	virtual void fire(T const&) override {
		// std::cerr << "CoroutineCallback::fire()" << std::endl;
		h.resume();
	}
	virtual void error(Error) override {
		// std::cerr << "CoroutineCallback::error()" << std::endl;
		h.resume();
	}
};

template <typename T, typename... Args>
struct n_coroutine::coroutine_traits<Future<T>, Args...> {
	struct promise_type {
		SAV<T>* sav;

		promise_type() : sav(new SAV<T>(0, 1)) {
			// std::cerr << "promise_type()" << std::endl;
		}
		~promise_type() {
			// std::cerr << "~promise_type()" << std::endl;
			if (sav)
				sav->delPromiseRef();
		}

		Future<T> get_return_object() noexcept {
			// return p.getFuture();
			sav->addFutureRef();
			return Future<T>(sav);
		}

		n_coroutine::suspend_never initial_suspend() const noexcept { return {}; }
		n_coroutine::suspend_never final_suspend() const noexcept { return {}; }

		template <class U>
		void return_value(U&& value) const {
			// std::cerr << "return_value()" << std::endl;
			// p.send(value);
			sav->send(std::forward<U>(value));
		}

		void unhandled_exception() {
			// TODO: this->sendError(std::current_exception());
			// std::cerr << "unhandled_exception()" << std::endl;
			try {
				std::rethrow_exception(std::current_exception());
			} catch (const Error& E) {
				sav->sendError(E);
			}
		}

		template <typename U>
		auto await_transform(Future<U> future) {

			// std::cerr << "await_transform" << std::endl;

			struct awaitable {
				Future<U> f;

				// TODO: Determine the lifetime of the callback
				CoroutineCallback<U>* cb = nullptr;

				awaitable(Future<U>& f) : f(f) {}

				bool await_ready() const {
					// std::cerr << "await_ready " << f.canGet() << std::endl;
					return f.canGet();
				}
				void await_suspend(n_coroutine::coroutine_handle<> h) {
					// std::cerr << "await_suspend" << std::endl;

					cb = new CoroutineCallback<U>(h);

					StrictFuture<U> sf = f;

					sf.addCallbackAndClear(cb);
				}
				U const& await_resume() {
					// std::cerr << "await_resume" << std::endl;
					if (cb)
						cb->remove();
					if (f.isError()) {
						throw f.getError();
					} else if (f.canGet()) {
						return f.get();
					}
				}

				~awaitable() {
					if (cb)
						delete cb;
				}
			};

			return awaitable{ future };
		}
	};
};

#endif