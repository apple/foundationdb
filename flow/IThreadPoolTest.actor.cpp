// Thread naming only works on Linux.
#if defined(__linux__)

#include "flow/IThreadPool.h"

#include <pthread.h>
#include <ostream>

#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

void forceLinkIThreadPoolTests() {}

static ThreadReturnPromiseStream<std::string> notifications;

struct ThreadNameReceiver final : IThreadPoolReceiver {
	ThreadNameReceiver(const bool stream_signal = false) : stream_signal(stream_signal) {}

	void init() override {}

	struct GetNameAction final : TypedAction<ThreadNameReceiver, GetNameAction> {
		ThreadReturnPromise<std::string> name;

		double getTimeEstimate() const override { return 3.; }
	};

	void action(GetNameAction& a) {
		pthread_t t = pthread_self();
		const size_t arrayLen = 16;
		char name[arrayLen];
		int err = pthread_getname_np(t, name, arrayLen);
		if (err != 0) {
			std::cout << "Get name failed with error code: " << err << std::endl;
			a.name.sendError(platform_error());
			if (stream_signal) {
				notifications.sendError(platform_error());
			}
			return;
		}
		std::string s = name;
		if (stream_signal) {
			notifications.send(s);
		}
		a.name.send(std::move(s));
	}

	bool stream_signal; 
};

TEST_CASE("/flow/IThreadPool/NamedThread") {
	noUnseed = true;

	state Reference<IThreadPool> pool = createGenericThreadPool();
	pool->addThread(new ThreadNameReceiver(), "thread-foo");

	// Warning: this action is a little racy with the call to `pthread_setname_np`. In practice,
	// ~nothing should depend on the thread name being set instantaneously. If this test ever
	// flakes, we can make `startThread` in platform a little bit more complex to clearly order
	// the actions.
	auto* a = new ThreadNameReceiver::GetNameAction();
	auto fut = a->name.getFuture();
	pool->post(a);

	std::string name = wait(fut);
	if (name != "thread-foo") {
		std::cout << "Incorrect thread name: " << name << std::endl;
		ASSERT(false);
	}

	wait(pool->stop());

	return Void();
}

TEST_CASE("/flow/IThreadPool/ThreadReturnPromiseStream") {
	noUnseed = true;

	state Reference<IThreadPool> pool = createGenericThreadPool();
	pool->addThread(new ThreadNameReceiver(/*stream_signal=*/true), "thread-foo");

	// Warning: this action is a little racy with the call to `pthread_setname_np`. In practice,
	// ~nothing should depend on the thread name being set instantaneously. If this test ever
	// flakes, we can make `startThread` in platform a little bit more complex to clearly order
	// the actions.
	state int num = 3;
	for (int i = 0; i < num; ++i) {
		auto* a = new ThreadNameReceiver::GetNameAction();
		pool->post(a);
	}

	state FutureStream<std::string> futs = notifications.getFuture();

	state int n = 0;
	while (n < num) {
		std::string name = waitNext(futs);
		if (name != "thread-foo") {
			std::cout << "Incorrect thread name: " << name << std::endl;
			ASSERT(false);
		}
		++n;
	}

	ASSERT(n == num);

	wait(pool->stop());

	return Void();
}

#else
void forceLinkIThreadPoolTests() {}
#endif
