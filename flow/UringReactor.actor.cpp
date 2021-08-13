
#include <inttypes.h>
#include <liburing.h>

#include "flow/flow.h"
#include "flow/FastAlloc.h"
#include "flow/UringReactor.h"

namespace N2 {

class OwnedWrite: public FastAllocated<OwnedWrite>{
public:
    struct iovec iov[64];
    Promise<int> pi;
    Promise<Void> pv;
    bool type;
    OwnedWrite(Promise<int> &&p) : pi(p), type(0) {}
    OwnedWrite(Promise<Void> &&p) : pv(p), type(1) {}
};

UringReactor::UringReactor(unsigned entries, unsigned flags){
    int ret = ::io_uring_queue_init(entries, &ring, flags);
    // https://github.com/spacejam/sled/issues/899
    ASSERT(ret==0);
    sqeCount = 0;
}

void UringReactor::poll(){
    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned count = 0;
    /*if(sqeCount) {
        int ret = ::io_uring_submit(&ring);
        ASSERT(ret>=0);
        sqeCount -= ret;
    }*/
    io_uring_for_each_cqe(&ring, head, cqe) {
        count++;
        OwnedWrite *ow = (OwnedWrite *)::io_uring_cqe_get_data(cqe);
        if(ow->type == 0){
            Promise<int> p = ow->pi;
            if (cqe->res > 0){
                p.send(int(cqe->res));
            } else if (cqe->res == -EAGAIN || cqe->res == -EWOULDBLOCK) {
                p.send(int(0));
            } else {
                p.sendError(connection_failed());
            }
        } else {
            if (cqe->res & POLLERR) {
                ow->pv.sendError(connection_failed());
            } else {
                ow->pv.send(Void());
            }
        }
        delete ow;


    }
    ::io_uring_cq_advance(&ring, count);
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
    //sqeCount++;
    int ret = ::io_uring_submit(&ring);
    ASSERT(ret>0);
}

void UringReactor::read(int fd, uint8_t *buff, int limit, Promise<int> &&p){
    OwnedWrite *ow = new OwnedWrite(std::move(p));
    struct iovec *iov = ow->iov;
    iov[0].iov_base = (void*)buff;
    iov[0].iov_len = limit;
    struct io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
    ::io_uring_prep_readv(sqe, fd, iov, 1, 0);
    ::io_uring_sqe_set_data(sqe, ow);
    //sqeCount++;
    int ret = ::io_uring_submit(&ring);
    ASSERT(ret>0);
}

void UringReactor::poll(int fd, unsigned int flags, Promise<Void> &&p){
    OwnedWrite *ow = new OwnedWrite(std::move(p));
    struct io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
    ::io_uring_prep_poll_add(sqe, fd, flags);
    ::io_uring_sqe_set_data(sqe, ow);
    //sqeCount++;
    int ret = ::io_uring_submit(&ring);
    ASSERT(ret>0);
}



int UringReactor::getFD(){
    return ring.ring_fd;
}
UringReactor::~UringReactor(){
    ::io_uring_queue_exit(&ring);
}
}
