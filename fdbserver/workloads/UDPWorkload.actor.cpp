/*
 * UDPWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/serialize.h"
#include <functional>
#include <limits>
#include <unordered_map>
#include <vector>
#include <memory>
#include <functional>

#include "flow/actorcompiler.h" // has to be last include

namespace {

struct UDPWorkload : TestWorkload {
	constexpr static const char* name = "UDPWorkload";
	// config
	Key keyPrefix;
	double runFor;
	int minPort, maxPort;
	//  members
	NetworkAddress serverAddress;
	Reference<IUDPSocket> serverSocket;
	std::unordered_map<NetworkAddress, unsigned> sent, received, acked, successes;
	PromiseStream<NetworkAddress> toAck;

	UDPWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		keyPrefix = getOption(options, "keyPrefix"_sr, "/udp/"_sr);
		runFor = getOption(options, "runFor"_sr, 60.0);
		minPort = getOption(options, "minPort"_sr, 5000);
		maxPort = getOption(options, "minPort"_sr, 6000);
		for (auto p : { minPort, maxPort }) {
			ASSERT(p > 0 && p < std::numeric_limits<unsigned short>::max());
		}
	}

	std::string description() const override { return name; }
	ACTOR static Future<Void> _setup(UDPWorkload* self, Database cx) {
		state NetworkAddress localAddress(g_network->getLocalAddress().ip,
		                                  deterministicRandom()->randomInt(self->minPort, self->maxPort + 1),
		                                  true,
		                                  false);
		state Key key = self->keyPrefix.withSuffix(BinaryWriter::toValue(self->clientId, Unversioned()));
		state Value serializedLocalAddress = BinaryWriter::toValue(localAddress, IncludeVersion());
		state ReadYourWritesTransaction tr(cx);
		Reference<IUDPSocket> s = wait(INetworkConnections::net()->createUDPSocket(localAddress.isV6()));
		self->serverSocket = std::move(s);
		self->serverSocket->bind(localAddress);
		self->serverAddress = localAddress;
		loop {
			try {
				Optional<Value> v = wait(tr.get(key));
				if (v.present()) {
					return Void();
				}
				tr.set(key, serializedLocalAddress);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
	Future<Void> setup(Database const& cx) override { return _setup(this, cx); }

	class Message {
		int _type = 0;

	public:
		enum class Type : uint8_t { PING, PONG };
		Message() {}
		explicit Message(Type t) {
			switch (t) {
			case Type::PING:
				_type = 0;
				break;
			case Type::PONG:
				_type = 1;
				break;
			default:
				UNSTOPPABLE_ASSERT(false);
			}
		}

		Type type() const {
			switch (_type) {
			case 0:
				return Type::PING;
			case 1:
				return Type::PONG;
			default:
				UNSTOPPABLE_ASSERT(false);
			}
		}

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, _type);
		}
	};

	static Message ping() { return Message{ Message::Type::PING }; }
	static Message pong() { return Message{ Message::Type::PONG }; }

	ACTOR static Future<Void> _receiver(UDPWorkload* self) {
		state Standalone<StringRef> packetString = makeString(IUDPSocket::MAX_PACKET_SIZE);
		state uint8_t* packet = mutateString(packetString);
		state NetworkAddress peerAddress;
		loop {
			int sz = wait(self->serverSocket->receiveFrom(packet, packet + IUDPSocket::MAX_PACKET_SIZE, &peerAddress));
			auto msg = BinaryReader::fromStringRef<Message>(packetString.substr(0, sz), IncludeVersion());
			if (msg.type() == Message::Type::PONG) {
				self->successes[peerAddress] += 1;
			} else if (msg.type() == Message::Type::PING) {
				self->received[peerAddress] += 1;
				self->toAck.send(peerAddress);
			} else {
				UNSTOPPABLE_ASSERT(false);
			}
		}
	}

	ACTOR static Future<Void> serverSender(UDPWorkload* self, std::vector<NetworkAddress>* remotes) {
		state Standalone<StringRef> packetString;
		state NetworkAddress peer;
		loop {
			choose {
				when(wait(delay(0.1))) {
					peer = deterministicRandom()->randomChoice(*remotes);
					packetString = BinaryWriter::toValue(Message{ Message::Type::PING }, IncludeVersion());
					self->sent[peer] += 1;
				}
				when(NetworkAddress p = waitNext(self->toAck.getFuture())) {
					peer = p;
					packetString = BinaryWriter::toValue(pong(), IncludeVersion());
					self->acked[peer] += 1;
				}
			}
			int res = wait(self->serverSocket->sendTo(packetString.begin(), packetString.end(), peer));
			ASSERT(res == packetString.size());
		}
	}

	ACTOR static Future<Void> clientReceiver(UDPWorkload* self, Reference<IUDPSocket> socket, Future<Void> done) {
		state Standalone<StringRef> packetString = makeString(IUDPSocket::MAX_PACKET_SIZE);
		state uint8_t* packet = mutateString(packetString);
		state NetworkAddress peer;
		state Future<Void> finished = Never();
		loop {
			choose {
				when(int sz = wait(socket->receiveFrom(packet, packet + IUDPSocket::MAX_PACKET_SIZE, &peer))) {
					auto res = BinaryReader::fromStringRef<Message>(packetString.substr(0, sz), IncludeVersion());
					ASSERT(res.type() == Message::Type::PONG);
					self->successes[peer] += 1;
				}
				when(wait(done)) {
					finished = delay(1.0);
					done = Never();
				}
				when(wait(finished)) { return Void(); }
			}
		}
	}

	ACTOR static Future<Void> clientSender(UDPWorkload* self, std::vector<NetworkAddress>* remotes) {
		state AsyncVar<Reference<IUDPSocket>> socket;
		state Standalone<StringRef> sendString;
		state ActorCollection actors(false);
		state NetworkAddress peer;

		loop {
			choose {
				when(wait(delay(0.1))) {}
				when(wait(actors.getResult())) { UNSTOPPABLE_ASSERT(false); }
			}
			if (!socket.get().isValid() || deterministicRandom()->random01() < 0.05) {
				peer = deterministicRandom()->randomChoice(*remotes);
				Reference<IUDPSocket> s = wait(INetworkConnections::net()->createUDPSocket(peer));
				socket.set(s);
				socket = s;
				actors.add(clientReceiver(self, socket.get(), socket.onChange()));
			}
			sendString = BinaryWriter::toValue(ping(), IncludeVersion());
			int res = wait(socket.get()->send(sendString.begin(), sendString.end()));
			ASSERT(res == sendString.size());
			self->sent[peer] += 1;
		}
	}

	ACTOR static Future<Void> _start(UDPWorkload* self, Database cx) {
		state ReadYourWritesTransaction tr(cx);
		state std::vector<NetworkAddress> remotes;
		loop {
			try {
				RangeResult range = wait(tr.getRange(prefixRange(self->keyPrefix), CLIENT_KNOBS->TOO_MANY));
				ASSERT(!range.more);
				for (auto const& p : range) {
					auto cID = BinaryReader::fromStringRef<decltype(self->clientId)>(
					    p.key.removePrefix(self->keyPrefix), Unversioned());
					if (cID != self->clientId) {
						remotes.emplace_back(BinaryReader::fromStringRef<NetworkAddress>(p.value, IncludeVersion()));
					}
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		wait(clientSender(self, &remotes) && serverSender(self, &remotes) && _receiver(self));
		UNSTOPPABLE_ASSERT(false);
		return Void();
	}
	Future<Void> start(Database const& cx) override { return delay(runFor) || _start(this, cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {
		unsigned totalReceived = 0, totalSent = 0, totalAcked = 0, totalSuccess = 0;
		for (const auto& p : sent) {
			totalSent += p.second;
		}
		for (const auto& p : received) {
			totalReceived += p.second;
		}
		for (const auto& p : acked) {
			totalAcked += p.second;
		}
		for (const auto& p : successes) {
			totalSuccess += p.second;
		}
		m.emplace_back("Sent", totalSent, Averaged::False);
		m.emplace_back("Received", totalReceived, Averaged::False);
		m.emplace_back("Acknknowledged", totalAcked, Averaged::False);
		m.emplace_back("Successes", totalSuccess, Averaged::False);
	}
};

} // namespace

WorkloadFactory<UDPWorkload> UDPWorkloadFactory(UDPWorkload::name);
