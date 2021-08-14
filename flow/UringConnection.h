#ifndef URINGCONNECTION_H_INCLUDED
#define URINGCONNECTION_H_INCLUDED

#include <sys/socket.h>
#include <boost/asio.hpp>

#include "flow/network.h"
#include "flow/UringReactor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace N2 {
class UringConnection final : public IConnection, ReferenceCounted<UringConnection> {
public:
	void addref() override { ReferenceCounted<UringConnection>::addref(); }
	void delref() override { ReferenceCounted<UringConnection>::delref(); }

	void close() override { closeSocket(); }

	explicit UringConnection(UringReactor *ureactor)
	  : id(nondeterministicRandom()->randomUniqueID()), fd(-1), ureactor(ureactor){}

	// This is not part of the IConnection interface, because it is wrapped by INetwork::connect()
    static Future<Reference<IConnection>> connect(UringReactor * const& ureactor, NetworkAddress const& addr);

	// This is not part of the IConnection interface, because it is wrapped by IListener::accept()
	void accept(NetworkAddress peerAddr, int fd) {
		this->peer_address = peerAddr;
		this->fd = fd;
	}

	Future<Void> acceptHandshake() override { return Void(); }

	Future<Void> connectHandshake() override { return Void(); }

    // returns when write() can write at least one byte
	Future<Void> onWritable() override {
		Promise<Void> p;
		auto f = p.getFuture();
		ureactor->poll(fd, POLLOUT, std::move(p));
		return f;
	}

	// returns when read() can read at least one byte
	Future<Void> onReadable() override {
		Promise<Void> p;
		auto f = p.getFuture();
		ureactor->poll(fd, POLLIN, std::move(p));
		return f;
	}

	Future<int> asyncRead(uint8_t* begin, uint8_t* end) override { return asyncRead(this, begin, end); };

	Future<int> asyncWrite(SendBuffer const* data, int limit) override { return asyncWrite(this, data, limit); }

	// Reads as many bytes as possible from the read buffer into [begin,end) and returns the number of bytes read (might
	// be 0)
	int read(uint8_t* begin, uint8_t* end) override {
        ASSERT(0);
		return 0;
	}

	// Writes as many bytes as possible from the given SendBuffer chain into the write buffer and returns the number of
	// bytes written (might be 0)
	int write(SendBuffer const* data, int limit) override {
        ASSERT(0);
		return 0;
	}

	NetworkAddress getPeerAddress() const override { return peer_address; }

	UID getDebugID() const override { return id; }

	UID id;
	int fd;
    UringReactor *ureactor;
	NetworkAddress peer_address;
	void closeSocket() {
        if (fd == -1) return;
		int ret = ::close(fd);
        fd = -1;
		if (ret != 0)
			TraceEvent(SevWarn, "N2_CloseError", id)
			    .suppressFor(1.0)
			    .detail("ErrorCode", errno)
			    .detail("Message", strerror(errno));
	}

private:
    static Future<int> asyncRead(UringConnection* const& self, uint8_t* const& begin, uint8_t* const& end);

    static Future<int> asyncWrite(UringConnection* const& self, SendBuffer const* const& data, int const& limit);
};

class UringListener final : public IListener, ReferenceCounted<UringListener> {
public:
    UringReactor *ureactor;
	NetworkAddress listenAddress;
	int fd;

	UringListener(UringReactor *ureactor, NetworkAddress listenAddress);

	void addref() override { ReferenceCounted<UringListener>::addref(); }
	void delref() override { ReferenceCounted<UringListener>::delref(); }

	// Returns one incoming connection when it is available
	Future<Reference<IConnection>> accept() override { return doAccept(this); }

	NetworkAddress getListenAddress() const override { return listenAddress; }

private:
    static Future<Reference<IConnection>> doAccept(UringListener* const& self);
};
}
#endif // URINGCONNECTION_H_INCLUDED
