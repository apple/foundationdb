#ifndef FLOW_IUDPSOCKET_H
#define FLOW_IUDPSOCKET_H

#include <boost/asio/ip/udp.hpp>

#include "flow/network.h"

class IUDPSocket {
public:
	//  see https://en.wikipedia.org/wiki/User_Datagram_Protocol - the max size of a UDP packet
	// This is enforced in simulation
	constexpr static size_t MAX_PACKET_SIZE = 65535;
	virtual ~IUDPSocket();
	virtual void addref() = 0;
	virtual void delref() = 0;

	virtual void close() = 0;
	virtual Future<int> send(uint8_t const* begin, uint8_t const* end) = 0;
	virtual Future<int> sendTo(uint8_t const* begin, uint8_t const* end, NetworkAddress const& peer) = 0;
	virtual Future<int> receive(uint8_t* begin, uint8_t* end) = 0;
	virtual Future<int> receiveFrom(uint8_t* begin, uint8_t* end, NetworkAddress* sender) = 0;
	virtual void bind(NetworkAddress const& addr) = 0;

	virtual UID getDebugID() const = 0;
	virtual NetworkAddress localAddress() const = 0;
	virtual boost::asio::ip::udp::socket::native_handle_type native_handle() = 0;
};

#endif // FLOW_IUDPSOCKET_H