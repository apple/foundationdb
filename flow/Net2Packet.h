/*
 * Net2Packet.h
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

#ifndef FLOW_NET2PACKET_H
#define FLOW_NET2PACKET_H
#pragma once

#include "flow/flow.h"
#include "flow/Histogram.h"

// PacketWriter and PacketBuffer are in serialize.h because they are needed by the SerializeSource<> template

struct ReliablePacket : FastAllocated<ReliablePacket> {
	PacketBuffer* buffer;
	ReliablePacket* cont; // More bytes in the same packet
	ReliablePacket *prev,
	    *next; // Linked list of reliable packets on the same connection (only for the first packet in the cont chain)
	int begin, end;

	ReliablePacket() {}

	void insertBefore(ReliablePacket* p);
	void remove(); // Deletes this and cont chain, unlinks prev and next
};

class UnsentPacketQueue : NonCopyable {
public:
	UnsentPacketQueue()
	  : unsent_first(0), unsent_last(0),
	    sendQueueLatencyHistogram(Histogram::getHistogram(LiteralStringRef("UnsentPacketQueue"),
	                                                      LiteralStringRef("QueueWait"),
	                                                      Histogram::Unit::microseconds)) {}

	~UnsentPacketQueue() {
		discardAll();
		unsent_first = (PacketBuffer*)0xDEADBEEF;
		unsent_last = (PacketBuffer*)0xCAFEBABE;
		sendQueueLatencyHistogram = Reference<Histogram>(nullptr);
	}

	// Get a PacketBuffer to write new packets into
	PacketBuffer* getWriteBuffer(size_t sizeHint = 0) {
		if (!unsent_last) {
			ASSERT(!unsent_first);
			unsent_first = unsent_last = PacketBuffer::create(sizeHint);
		};
		return unsent_last;
	}
	// Call after potentially adding to the chain returned by getWriteBuffer()
	void setWriteBuffer(PacketBuffer* pb) { unsent_last = pb; }

	// Prepend the given range of packetBuffers to the beginning of the unsent queue
	void prependWriteBuffer(PacketBuffer* first, PacketBuffer* last) {
		last->next = unsent_first;
		unsent_first = first;
		if (!unsent_last)
			unsent_last = last;
	}

	// false if there is anything unsent
	bool empty() const { return !unsent_first || unsent_first->bytes_sent == unsent_first->bytes_written; }

	// Get the next PacketBuffer to send data from
	PacketBuffer* getUnsent() const { return unsent_first; }
	// Call after sending bytes from getUnsent()
	void sent(int bytes);

	// Discard all unsent buffers
	void discardAll();

private:
	PacketBuffer *unsent_first, *unsent_last; // Both nullptr, or inclusive range of PacketBuffers that haven't been
	                                          // sent.  The last one may have space for more packets to be written.
	Reference<Histogram> sendQueueLatencyHistogram;
};

class ReliablePacketList : NonCopyable {
public:
	ReliablePacketList() {
		reliable.buffer = 0;
		reliable.prev = reliable.next = &reliable;
	}
	bool empty() const { return reliable.next == &reliable; }
	void insert(ReliablePacket* rp) { rp->insertBefore(&reliable); }

	// Concatenate those reliable packets which have already been sent (are not in the unsent range)
	// into the given chain of packet buffers, and return the tail of that chain
	PacketBuffer* compact(PacketBuffer* into, PacketBuffer* stopAt);

	void discardAll(); // just for testing
private:
	ReliablePacket reliable; // Head/tail of a circularly linked list of reliable packets to be resent after a close
};

#endif
