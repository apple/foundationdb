#ifndef FLOW_URINGREACTOR_H
#define FLOW_URINGREACTOR_H
#pragma once

#include <inttypes.h>
#include <liburing.h>
#include <boost/asio.hpp>

#include "flow/flow.h"


namespace N2 {

class UringReactor {
private:
    ::io_uring ring;
public:
    UringReactor(unsigned entries, unsigned flags);
    void poll();
    void write(int fd, const SendBuffer* buffer, int limit, Promise<int> &&p);
    ~UringReactor();

    UringReactor(UringReactor &&) = delete;
    UringReactor(const UringReactor &) = delete;
    UringReactor &operator=(UringReactor &&) = delete;
    UringReactor &operator=(const UringReactor &) = delete;
};

}

#endif
