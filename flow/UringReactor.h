#ifndef FLOW_URINGREACTOR_H
#define FLOW_URINGREACTOR_H
#pragma once

#include <mutex>
#include <inttypes.h>
#include <liburing.h>
#include <boost/asio.hpp>

#include "flow/flow.h"

struct AssertingMutex {
	AssertingMutex() : is_in_use(0) {}

	void lock() { ASSERT(++is_in_use == 1); }
	void unlock() { ASSERT(--is_in_use == 0); }

private:
	std::atomic<signed char> is_in_use;
};

namespace N2 {

class UringReactor {
private:
	::io_uring ring;
	int sqeCount;
	int evfd;
	int64_t fdVal;
	AssertingMutex submit;
	AssertingMutex consume;
	void rearm();

public:
	UringReactor(unsigned entries, unsigned flags);
	int poll();
	void write(int fd, const SendBuffer* buffer, int limit, Promise<int>&& p);
	void read(int fd, uint8_t* buff, int limit, Promise<int>&& p);
	void poll(int fd, unsigned flags, Promise<Void>&& p);
	void sleep(double time);
	void wake();

	int getFD();
	~UringReactor();

	UringReactor(UringReactor&&) = delete;
	UringReactor(const UringReactor&) = delete;
	UringReactor& operator=(UringReactor&&) = delete;
	UringReactor& operator=(const UringReactor&) = delete;
	class EventFD : public IEventFD {
	public:
		UringReactor* ureactor;
		int fd;
		int64_t fdVal;

		static Future<int64_t> handle_read(EventFD* const& self);

		EventFD(UringReactor* reactor) : ureactor(reactor), fd(open()) {}
		~EventFD() override {
			::close(fd); // Also closes the fd, I assume...
		}
		int getFD() override { return fd; }
		Future<int64_t> read() override { return handle_read(this); }

		int open() {
			fd = eventfd(0, EFD_CLOEXEC);
			if (fd < 0) {
				TraceEvent(SevError, "EventfdError").GetLastError();
				throw platform_error();
			}
			return fd;
		}
	};
	static IEventFD* getEventFD() { return static_cast<IEventFD*>((void*)g_network->global(INetwork::enEventFD)); }
	static EventFD* newEventFD(UringReactor& reactor) { return new EventFD(&reactor); }
};

} // namespace N2

#endif
