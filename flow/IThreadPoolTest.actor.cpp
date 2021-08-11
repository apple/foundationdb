// Thread naming only works on Linux.
#if defined(__linux__)

#include "flow/IThreadPool.h"

#include <pthread.h>
#include <ostream>

#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

void forceLinkIThreadPoolTests() {}


struct ThreadNameReceiver final : IThreadPoolReceiver {
	ThreadNameReceiver(){};

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
			return;
		}
		std::string s = name;
		a.name.send(std::move(s));
	}
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

struct ThreadSafePromiseStreamSender final : IThreadPoolReceiver {
	ThreadSafePromiseStreamSender(
		ThreadReturnPromiseStream<std::string>* notifications)
		 : notifications(notifications) {}
	void init() override {}

	struct GetNameAction final : TypedAction<ThreadSafePromiseStreamSender, GetNameAction> {
		double getTimeEstimate() const override { return 3.; }
	};

	void action(GetNameAction& a) {
		pthread_t t = pthread_self();
		const size_t arrayLen = 16;
		char name[arrayLen];
		int err = pthread_getname_np(t, name, arrayLen);
		if (err != 0) {
			std::cout << "Get name failed with error code: " << err << std::endl;
			notifications->sendError(platform_error());
			return;
		}
		notifications->send(std::move(name));
	}

private:
	ThreadReturnPromiseStream<std::string>* notifications;
};

TEST_CASE("/flow/IThreadPool/ThreadReturnPromiseStream") {
	noUnseed = true;

	state std::unique_ptr<ThreadReturnPromiseStream<std::string>> 
		notifications(new ThreadReturnPromiseStream<std::string>());

	state Reference<IThreadPool> pool = createGenericThreadPool();
	pool->addThread(new ThreadSafePromiseStreamSender(notifications.get()), "thread-foo");

	// Warning: this action is a little racy with the call to `pthread_setname_np`. In practice,
	// ~nothing should depend on the thread name being set instantaneously. If this test ever
	// flakes, we can make `startThread` in platform a little bit more complex to clearly order
	// the actions.
	state int num = 3;
	for (int i = 0; i < num; ++i) {
		auto* a = new ThreadSafePromiseStreamSender::GetNameAction();
		pool->post(a);
	}

	state FutureStream<std::string> futs = notifications->getFuture();

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
