#include <sys/socket.h>

#include <boost/asio.hpp>

#include "flow/network.h"

#include "flow/UringConnection.h"
#include "flow/UringReactor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

static boost::asio::ip::address tcpAddress(IPAddress const& n) {
	if (n.isV6()) {
		return boost::asio::ip::address_v6(n.toV6());
	} else {
		return boost::asio::ip::address_v4(n.toV4());
	}
}

static IPAddress toIPAddress(boost::asio::ip::address const& addr) {
	if (addr.is_v4()) {
		return IPAddress(addr.to_v4().to_uint());
	} else {
		return IPAddress(addr.to_v6().to_bytes());
	}
}

static boost::asio::ip::tcp::endpoint tcpEndpoint(NetworkAddress const& n) {
	return boost::asio::ip::tcp::endpoint(tcpAddress(n.ip), n.port);
}


namespace N2 {


// This is not part of the IConnection interface, because it is wrapped by INetwork::connect()
ACTOR Future<Reference<IConnection>> UringConnection::connect(UringReactor *ureactor, NetworkAddress addr) {
    state Reference<UringConnection> self(new UringConnection(ureactor));

    self->peer_address = addr;
    self->fd = ::socket(addr.isV6()? AF_INET6 : AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    ASSERT(self->fd!=-1);
    try {
        auto to = tcpEndpoint(addr);
        int ret = ::connect(self->fd, to.data(), to.size());
        if (ret == 0){
            return self;
        } else if (errno != EALREADY && errno != EWOULDBLOCK && errno != EINPROGRESS) {
            throw connection_failed();
        }
        Promise<Void> p;
        Future<Void> onConnected = p.getFuture();
        ureactor->poll(self->fd, POLLOUT, std::move(p));

        wait(onConnected);
        return self;
    } catch (Error&) {
        // Either the connection failed, or was cancelled by the caller
        self->closeSocket();
        throw;
    }
}



ACTOR Future<int> UringConnection::asyncRead(UringConnection* self, uint8_t* begin, uint8_t* end) {
    Promise<int> p;
    auto f = p.getFuture();
    size_t toRead = end - begin;
    self->ureactor->read(self->fd, begin, toRead, std::move(p));
    try {
        int size = wait(f);
        return size;
    } catch (Error& err) {
        self->closeSocket();
        throw err;
    }
}

ACTOR Future<int> UringConnection::asyncWrite(UringConnection* self, SendBuffer const* data, int limit) {
    Promise<int> p;
    auto f = p.getFuture();
    self->ureactor->write(self->fd, data, limit, std::move(p));
    try {
        int size = wait(f);
        return size;
    } catch (Error& err) {
        ASSERT(limit > 0);
        bool notEmpty = false;
        for (auto p = data; p; p = p->next)
            if (p->bytes_written - p->bytes_sent > 0) {
                notEmpty = true;
                break;
            }
        ASSERT(notEmpty);
        self->closeSocket();
        throw err;
    }
}



UringListener::UringListener(UringReactor *ureactor, NetworkAddress listenAddress)
    : ureactor(ureactor), listenAddress(listenAddress) {
    fd = ::socket(listenAddress.isV6()? AF_INET6 : AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if(fd==-1) throw boost::system::error_code(errno, boost::asio::error::get_system_category());
    auto to = tcpEndpoint(listenAddress);
    int ret = ::bind(fd, to.data(), to.size());
    if(ret==-1) throw boost::system::error_code(errno, boost::asio::error::get_system_category());
    ret = ::listen(fd, 64);
    if(ret==-1) throw boost::system::error_code(errno, boost::asio::error::get_system_category());
}

ACTOR Future<Reference<IConnection>> UringListener::doAccept(UringListener* self) {
    state Reference<UringConnection> conn(new UringConnection(self->ureactor));
    state ::sockaddr_storage peer_endpoint;
    try {
        loop {
            Promise<Void> p;
            auto f = p.getFuture();
            self->ureactor->poll(self->fd, POLLIN, std::move(p));
            wait(f);
            socklen_t peer_size = sizeof(peer_endpoint);
            int fd = ::accept4(self->fd, (::sockaddr *) &peer_endpoint,&peer_size, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd == -1){
                if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                throw connection_failed();
            }
            if (peer_endpoint.ss_family == AF_INET){
                ::sockaddr_in *addrv4 = (sockaddr_in*) &peer_endpoint;
                conn->accept(NetworkAddress(IPAddress(addrv4->sin_addr.s_addr), addrv4->sin_port), fd);
            } else {
                ::sockaddr_in6 *addrv6 = (sockaddr_in6*) &peer_endpoint;
                std::array<uint8_t, 16> ipv6_addr;
                memcpy(ipv6_addr.data(), addrv6->sin6_addr.s6_addr, 16 * sizeof(uint8_t));
                conn->accept(NetworkAddress(IPAddress(ipv6_addr), addrv6->sin6_port), fd);
            }
            return conn;
        }
    } catch (...) {
        conn->close();
        throw;
    }
}
}
