/*
 * Net2Packet.cpp
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

#include "flow/Net2Packet.h"

void PacketWriter::init(PacketBuffer* buf, ReliablePacket* reliable) {
	this->buffer = buf;
	this->reliable = reliable;
	this->length = 0;
	length -= buffer->bytes_written;
	if (reliable) {
		reliable->buffer = buffer;
		buffer->addref();
		reliable->begin = buffer->bytes_written;
	}
}

PacketBuffer* PacketWriter::finish() {
	length += buffer->bytes_written;
	if (reliable) {
		reliable->cont = nullptr;
		reliable->end = buffer->bytes_written;
	}
	return buffer;
}

void PacketWriter::serializeBytesAcrossBoundary(const void* data, int bytes) {
	while (true) {
		int b = std::min(bytes, buffer->bytes_unwritten());
		memcpy(buffer->data() + buffer->bytes_written, data, b);
		buffer->bytes_written += b;
		bytes -= b;
		if (!bytes)
			break;

		data = (uint8_t*)data + b;
		nextBuffer(bytes);
	}
}

void PacketWriter::nextBuffer(size_t size) {
	auto last_buffer_bytes_written = buffer->bytes_written;
	length += last_buffer_bytes_written;

	buffer->next = PacketBuffer::create(size);
	buffer = buffer->nextPacketBuffer();

	if (reliable) {
		reliable->end = last_buffer_bytes_written;
		reliable->cont = new ReliablePacket;
		reliable = reliable->cont;
		reliable->buffer = buffer;
		buffer->addref();
		reliable->begin = 0;
	}
}

// Adds exactly bytes of unwritten length to the buffer, possibly across packet buffer boundaries,
// and initializes buf to point to the packet buffer(s) that contain the unwritten space
void PacketWriter::writeAhead(int bytes, struct SplitBuffer* buf) {
	if (bytes <= buffer->bytes_unwritten()) {
		buf->begin = buffer->data() + buffer->bytes_written;
		buf->first_length = bytes;
		buffer->bytes_written += bytes;
		buf->next = 0;
	} else {
		buf->begin = buffer->data() + buffer->bytes_written;
		buf->first_length = buffer->bytes_unwritten();
		buffer->bytes_written = buffer->size();
		size_t remaining = bytes - buf->first_length;
		nextBuffer(remaining);
		buf->next = buffer->data();
		buffer->bytes_written = remaining;
	}
}

void SplitBuffer::write(const void* data, int len) {
	write(data, len, 0);
}

void SplitBuffer::write(const void* data, int len, int offset) {
	if (len + offset <= first_length)
		memcpy(begin + offset, data, len);
	else {
		if (offset >= first_length) {
			memcpy(next + offset - first_length, data, len);
		} else {
			memcpy(begin + offset, data, first_length - offset);
			memcpy(next, (uint8_t*)data + first_length - offset, len - first_length + offset);
		}
	}
}

void SplitBuffer::writeAndShrink(const void* data, int len) {
	if (len <= first_length) {
		memcpy(begin, data, len);
		begin += len;
		first_length -= len;
	} else {
		memcpy(begin, data, first_length);
		int s = len - first_length;
		memcpy(next, (uint8_t*)data + first_length, s);
		next += s;
		first_length = 0;
	}
}

void ReliablePacket::insertBefore(ReliablePacket* p) {
	next = p;
	prev = p->prev;
	prev->next = next->prev = this;
}

void ReliablePacket::remove() {
	next->prev = prev;
	prev->next = next;
	for (ReliablePacket* c = this; c;) {
		ReliablePacket* n = c->cont;
		c->buffer->delref();
		delete c;
		c = n;
	}
}

void UnsentPacketQueue::sent(int bytes) {
	while (bytes) {
		ASSERT(unsent_first);
		PacketBuffer* b = unsent_first;

		if (b->bytes_sent + bytes <= b->bytes_written &&
		    (b->bytes_sent + bytes != b->bytes_written || (!b->next && b->bytes_unwritten()))) {
			b->bytes_sent += bytes;
			ASSERT(b->bytes_sent <= b->size());
			break;
		}

		// We've sent an entire buffer
		bytes -= b->bytes_written - b->bytes_sent;
		b->bytes_sent = b->bytes_written;
		ASSERT(b->bytes_written <= b->size());
		double queue_time = now() - b->enqueue_time;
		sendQueueLatencyHistogram->sampleSeconds(queue_time);
		unsent_first = b->nextPacketBuffer();
		if (!unsent_first)
			unsent_last = nullptr;
		b->delref();
	}
}

void UnsentPacketQueue::discardAll() {
	while (unsent_first) {
		auto n = unsent_first->nextPacketBuffer();
		unsent_first->delref();
		unsent_first = n;
	}
	unsent_last = 0;
}

PacketBuffer* ReliablePacketList::compact(PacketBuffer* into, PacketBuffer* end) {
	ReliablePacket* r = reliable.next;
	while (r != &reliable) {
		for (ReliablePacket* c = r; c; c = c->cont) {
			if (c->buffer == end /*&& c->begin>=c->buffer->bytes_written*/) // quit when we hit the unsent range
				return into;
			if (into->bytes_written == into->size()) {
				into->next = PacketBuffer::create(into->size());
				into = into->nextPacketBuffer();
			}

			uint8_t* data = &c->buffer->data()[c->begin];
			int len = c->end - c->begin;

			if (len > into->bytes_unwritten()) {
				// We have to split this ReliablePacket
				len = into->bytes_unwritten();
				ReliablePacket* e = new ReliablePacket;
				e->cont = c->cont;
				e->buffer = c->buffer;
				e->buffer->addref();
				e->begin = c->begin + len;
				e->end = c->end;
				c->cont = e;
			}

			memcpy(into->data() + into->bytes_written, data, len);
			c->buffer->delref();
			c->buffer = into;
			c->buffer->addref();
			c->begin = into->bytes_written;
			into->bytes_written += len;
			c->end = into->bytes_written;
		}

		r = r->next;
	}
	return into;
}

void ReliablePacketList::discardAll() {
	while (reliable.next != &reliable)
		reliable.next->remove();
}
