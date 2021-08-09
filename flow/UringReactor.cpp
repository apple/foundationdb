
#include <inttypes.h>
#include <liburing.h>

#include "flow/flow.h"
#include "flow/FastAlloc.h"
#include "flow/UringReactor.h"

namespace N2 {

class OwnedWrite: public FastAllocated<OwnedWrite>{
public:
    struct iovec iov[64];
    Promise<int> p;
    OwnedWrite(Promise<int> &&p) : p(p) {}
};

UringReactor::UringReactor(unsigned entries, unsigned flags){
    int ret = ::io_uring_queue_init(entries, &ring, flags);
    // https://github.com/spacejam/sled/issues/899
    ASSERT(ret==0);
    sqeCount = 0;
}

void UringReactor::poll(){
    struct io_uring_cqe *cqe;
    //std::cout<<"polling"<<std::endl;
    unsigned head;
    unsigned count = 0;
    io_uring_for_each_cqe(&ring, head, cqe) {
        count++;
        OwnedWrite *ow = (OwnedWrite *)::io_uring_cqe_get_data(cqe);
        Promise<int> p = ow->p;
        delete ow;
        //std::cout<<"cq "<<cqe->res<<std::endl;
        if (cqe->res > 0){
            p.send(int(cqe->res));
        } else if (cqe->res == -EAGAIN || cqe->res == -EWOULDBLOCK) {
            p.send(int(0));
        } else {
            p.sendError(connection_failed());
        }

    }
    ::io_uring_cq_advance(&ring, count);
    /*if(sqeCount) {
        int ret = ::io_uring_submit(&ring);
        ASSERT(ret>=0);
        sqeCount -= ret;
    }*/
}
void UringReactor::write(int fd, const SendBuffer* buffer, int limit, Promise<int> &&p){
    OwnedWrite *ow = new OwnedWrite(std::move(p));
    struct iovec *iov = ow->iov;
    int count = 0;
    int len = 0;
    while(count < 64 && limit > 0 && buffer){
        iov[count].iov_base = (void*)(buffer->data() + buffer->bytes_sent);
        iov[count].iov_len = std::min(limit, buffer->bytes_written - buffer->bytes_sent);
        len += iov[count].iov_len;
        //std::cout<<"buff "<<count<<" done "<<buffer->bytes_sent<<" left "<<buffer->bytes_written<<std::endl;
        limit -= buffer->bytes_written - buffer->bytes_sent;
        if (limit > 0)
            buffer = buffer->next;
        else
            buffer = nullptr;
        ++count;
    }
    if(count==64)std::cout<<"full"<<std::endl;
    struct io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
    ::io_uring_prep_writev(sqe, fd, iov, count, 0);
    ::io_uring_sqe_set_data(sqe, ow);
    int ret = ::io_uring_submit(&ring);
    ASSERT(ret>=0);
}

void UringReactor::read(int fd, uint8_t *buff, int limit, Promise<int> &&p){
    OwnedWrite *ow = new OwnedWrite(std::move(p));
    struct iovec *iov = ow->iov;
    iov[0].iov_base = (void*)buff;
    iov[0].iov_len = limit;
    struct io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
    ::io_uring_prep_readv(sqe, fd, iov, 1, 0);
    ::io_uring_sqe_set_data(sqe, ow);
    int ret = ::io_uring_submit(&ring);
    ASSERT(ret>=0);
}
UringReactor::~UringReactor(){
    ::io_uring_queue_exit(&ring);
}
}
