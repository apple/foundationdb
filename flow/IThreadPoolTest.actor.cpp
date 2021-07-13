#include "flow/IThreadPool.h"

#include <pthread.h>
#include <ostream>

#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

// Thread naming only works on Linux.
#if defined(__linux__)

void forceLinkIThreadPoolTests() {}

struct ThreadNameReceiver : IThreadPoolReceiver {
	void init() override {}

	struct GetNameAction : TypedAction<ThreadNameReceiver, GetNameAction> {
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

#endif
