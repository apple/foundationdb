#ifndef FLOW_URINGREACTOR_H
#define FLOW_URINGREACTOR_H
#pragma once

#include <mutex>
#include <inttypes.h>
#include <liburing.h>
#include <boost/asio.hpp>

#include "flow/flow.h"


namespace N2 {

class UringReactor {
private:
    ::io_uring ring;
    int sqeCount;
    struct __kernel_timespec ts;
    std::mutex submit;
    std::mutex consume;
public:
    UringReactor(unsigned entries, unsigned flags);
    int poll();
    void write(int fd, const SendBuffer* buffer, int limit, Promise<int> &&p);
    void read(int fd, uint8_t *buff, int limit, Promise<int> &&p);
    void poll(int fd, unsigned flags, Promise<Void> &&p);
    void sleep(double time);
    void wake();

    int getFD();
    ~UringReactor();

    UringReactor(UringReactor &&) = delete;
    UringReactor(const UringReactor &) = delete;
    UringReactor &operator=(UringReactor &&) = delete;
    UringReactor &operator=(const UringReactor &) = delete;
    class EventFD : public IEventFD {
    public:
		int fd;
		UringReactor* ureactor;
		int64_t fdVal;

        static Future<int64_t> handle_read(EventFD* const& self);

		EventFD(UringReactor* reactor) : ureactor(reactor), fd(open()) {}
		~EventFD() override {
			::close(fd); // Also closes the fd, I assume...
		}
		int getFD() override { return fd; }
		Future<int64_t> read() override {
			return handle_read(this);
		}

		int open() {
			fd = eventfd(0, EFD_CLOEXEC);
			if (fd < 0) {
				TraceEvent(SevError, "EventfdError").GetLastError();
				throw platform_error();
			}
			return fd;
		}
	};

public:
	static IEventFD* getEventFD() { return static_cast<IEventFD*>((void*)g_network->global(INetwork::enEventFD)); }
	static EventFD* newEventFD(UringReactor& reactor) { return new EventFD(&reactor); }

};

}

#endif
