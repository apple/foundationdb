
#include <inttypes.h>
#include <liburing.h>

#include "flow/flow.h"
#include "flow/FastAlloc.h"
#include "flow/UringReactor.h"

#include "flow/actorcompiler.h"

namespace N2 {

class OwnedWrite: public FastAllocated<OwnedWrite>{
public:
    struct iovec iov[64];
    struct __kernel_timespec ts;
    Promise<int> pi;
    Promise<Void> pv;
    int type;
    int fd;
    int seen;
    OwnedWrite* other;
    OwnedWrite(Promise<int> &&p, int type, int fd) : pi(p), type(type), fd(fd), seen(0) {}
    OwnedWrite(Promise<Void> &&p, int type, int fd) : pv(p), type(type), fd(fd), seen(0) {}
    OwnedWrite(OwnedWrite* other, int type, int fd) : other(other), type(type), fd(fd), seen(0) {}
    OwnedWrite(int type, int fd) : type(type), fd(fd), seen(0) {}
};

UringReactor::UringReactor(unsigned entries, unsigned flags){
    int ret = ::io_uring_queue_init(entries, &ring, flags);
    // https://github.com/spacejam/sled/issues/899
    ASSERT(ret==0);
    evfd = eventfd(0, EFD_CLOEXEC);
    ASSERT(evfd>0);
    rearm();
    sqeCount = 0;
}

void UringReactor::rearm(){
    OwnedWrite *ow = new OwnedWrite(5, evfd);
    //printf("add %d %d\n",ow->type,ow->fd);
    struct iovec *iov = ow->iov;
    iov[0].iov_base = &fdVal;
    iov[0].iov_len = sizeof(fdVal);
    submit.lock();
    struct io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
    ::io_uring_prep_readv(sqe, evfd, iov, 1, 0);
    ::io_uring_sqe_set_data(sqe, ow);
    //sqeCount++;
    int ret = ::io_uring_submit(&ring);
    submit.unlock();
    ASSERT(ret>=0);
}

int UringReactor::poll(){
    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned count = 0;
    int res;
    /*if(sqeCount) {
        int ret = ::io_uring_submit(&ring);
        ASSERT(ret>=0);
        sqeCount -= ret;
    }*/
    consume.lock();
    io_uring_for_each_cqe(&ring, head, cqe) {
        count++;
        OwnedWrite *ow = (OwnedWrite *)::io_uring_cqe_get_data(cqe);
        res = cqe->res;
        if(ow == nullptr) continue;
        if(ow->seen){
            printf("REPLAY? %d %d\n",ow->fd, ow->type);
            continue;
        }
        if(ow->type == 0 || ow->type == 1){
            Promise<int> p = ow->pi;
            if (res > 0){
                p.send(int(res));
            } else if (res == -EAGAIN || res == -EWOULDBLOCK) {
                p.send(int(0));
            } else {
                p.sendError(connection_failed());
            }
        } else if (ow->type == 2){
            if (res & POLLERR) {
                ow->pv.sendError(connection_failed());
            } else {
                ow->pv.send(Void());
            }
        } else if (ow->type == 3){
            // pass
        } else if (ow->type == 4){
            if(res == 0) {
                ow->other->seen++;
                delete ow->other;
            }
        } else if (ow->type == 5){
            rearm();
        }
        ow->seen++;
        delete ow;
    }
    ::io_uring_cq_advance(&ring, count);
    consume.unlock();
    return count;
}
void UringReactor::write(int fd, const SendBuffer* buffer, int limit, Promise<int> &&p){
    OwnedWrite *ow = new OwnedWrite(std::move(p), 0, fd);
    //printf("add %d %d\n",ow->type,ow->fd);
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
    submit.lock();
    struct io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
    ::io_uring_prep_writev(sqe, fd, iov, count, 0);
    ::io_uring_sqe_set_data(sqe, ow);
    //sqeCount++;
    int ret = ::io_uring_submit(&ring);
    ASSERT(ret>=0);
    submit.unlock();
}

void UringReactor::read(int fd, uint8_t *buff, int limit, Promise<int> &&p){
    OwnedWrite *ow = new OwnedWrite(std::move(p), 1, fd);
    //printf("add %d %d\n",ow->type,ow->fd);
    struct iovec *iov = ow->iov;
    iov[0].iov_base = (void*)buff;
    iov[0].iov_len = limit;
    submit.lock();
    struct io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
    ::io_uring_prep_readv(sqe, fd, iov, 1, 0);
    ::io_uring_sqe_set_data(sqe, ow);
    //sqeCount++;
    int ret = ::io_uring_submit(&ring);
    submit.unlock();
    ASSERT(ret>=0);
}

void UringReactor::poll(int fd, unsigned int flags, Promise<Void> &&p){
    OwnedWrite *ow = new OwnedWrite(std::move(p), 2, fd);
    //printf("add %d %d\n",ow->type,ow->fd);
    submit.lock();
    struct io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
    ::io_uring_prep_poll_add(sqe, fd, flags);
    ::io_uring_sqe_set_data(sqe, ow);
    //sqeCount++;
    int ret = ::io_uring_submit(&ring);
    submit.unlock();
    ASSERT(ret>=0);
}

void UringReactor::sleep(double sleepTime){
    if (poll()) return;
    if (sleepTime > FLOW_KNOBS->BUSY_WAIT_THRESHOLD) {
        OwnedWrite *ow = nullptr;
        if (sleepTime < 4e12) {
            ow = new OwnedWrite(3, -1);
            ow->ts.tv_sec = 0;
            ow->ts.tv_nsec = 2000; //sleepTime * 1e6;
            submit.lock();
            struct io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
            ::io_uring_prep_timeout(sqe, &ow->ts, 0, 0);
            ::io_uring_sqe_set_data(sqe, ow);
            int ret = ::io_uring_submit(&ring);
            submit.unlock();
            ASSERT(ret>=0);
        }
        {
            struct io_uring_cqe *cqe;
            consume.lock();
            int ret = ::io_uring_wait_cqe(&ring, &cqe);
            consume.unlock();
            ASSERT(ret>=0 || ret==-EINTR);
        };
        if (ow&&0){
            OwnedWrite *ow2 = new OwnedWrite(ow, 4, -1);
            submit.lock();
            struct io_uring_sqe *sqe = ::io_uring_get_sqe(&ring);
            ::io_uring_prep_timeout_remove(sqe,(uint64_t)ow,0);
            ::io_uring_sqe_set_data(sqe, ow2);
            int ret = ::io_uring_submit(&ring);
            submit.unlock();
            ASSERT(ret>=0);
        }
	} else if (sleepTime > 0) {
		if (!(FLOW_KNOBS->REACTOR_FLAGS & 8))
			threadYield();
	}
}

void UringReactor::wake(){
    int64_t fdVal = 1;
    int ret = ::write(evfd, &fdVal, sizeof(fdVal));
    ASSERT(ret == sizeof(fdVal));
}



int UringReactor::getFD(){
    return ring.ring_fd;
}
UringReactor::~UringReactor(){
    ::io_uring_queue_exit(&ring);
}

ACTOR Future<int64_t> UringReactor::EventFD::handle_read(UringReactor::EventFD* self) {
    Promise<int> p;
    auto f = p.getFuture();
    self->ureactor->read(self->fd, (uint8_t*) &self->fdVal, sizeof(self->fdVal), std::move(p));
    int size = wait(f);
    ASSERT(size == sizeof(self->fdVal));
    return self->fdVal;
}

}
