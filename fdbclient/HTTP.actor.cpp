/*
 * HTTP.actor.cpp
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

#include "fdbclient/HTTP.h"
#include "fdbclient/md5/md5.h"
#include "fdbclient/libb64/encode.h"
#include <cctype>
#include "flow/actorcompiler.h" // has to be last include

namespace HTTP {

// AWS V4 headers require this encoding for its signature calculation
std::string awsV4URIEncode(const std::string& s, bool encodeSlash) {
	std::string o;
	o.reserve(s.size() * 3);
	char buf[4];
	for (auto c : s) {
		if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
			o.append(&c, 1);
		else if (c == '/')
			o.append(encodeSlash ? "%2F" : "/");
		else {
			sprintf(buf, "%%%.02X", c);
			o.append(buf);
		}
	}
	return o;
}

std::string urlEncode(const std::string& s) {
	std::string o;
	o.reserve(s.size() * 3);
	char buf[4];
	for (auto c : s)
		if (std::isalnum(c) || c == '?' || c == '/' || c == '-' || c == '_' || c == '.' || c == ',' || c == ':')
			o.append(&c, 1);
		else {
			sprintf(buf, "%%%.02X", c);
			o.append(buf);
		}
	return o;
}

bool Response::verifyMD5(bool fail_if_header_missing, Optional<std::string> content_sum) {
	auto i = headers.find("Content-MD5");
	if (i != headers.end()) {
		// If a content sum is not provided, calculate one from the response content
		if (!content_sum.present()) {
			MD5_CTX sum;
			::MD5_Init(&sum);
			::MD5_Update(&sum, content.data(), content.size());
			std::string sumBytes;
			sumBytes.resize(16);
			::MD5_Final((unsigned char*)sumBytes.data(), &sum);
			std::string sumStr = base64::encoder::from_string(sumBytes);
			sumStr.resize(sumStr.size() - 1);
			content_sum = sumStr;
		}
		return i->second == content_sum.get();
	}
	return !fail_if_header_missing;
}

std::string Response::toString() {
	std::string r = format("Response Code: %d\n", code);
	r += format("Response ContentLen: %lld\n", contentLen);
	for (auto h : headers)
		r += format("Reponse Header: %s: %s\n", h.first.c_str(), h.second.c_str());
	r.append("-- RESPONSE CONTENT--\n");
	r.append(content);
	r.append("\n--------\n");
	return r;
}

PacketBuffer* writeRequestHeader(std::string const& verb,
                                 std::string const& resource,
                                 HTTP::Headers const& headers,
                                 PacketBuffer* dest) {
	PacketWriter writer(dest, nullptr, Unversioned());
	writer.serializeBytes(verb);
	writer.serializeBytes(" ", 1);
	writer.serializeBytes(resource);
	writer.serializeBytes(LiteralStringRef(" HTTP/1.1\r\n"));
	for (auto h : headers) {
		writer.serializeBytes(h.first);
		writer.serializeBytes(LiteralStringRef(": "));
		writer.serializeBytes(h.second);
		writer.serializeBytes(LiteralStringRef("\r\n"));
	}
	writer.serializeBytes(LiteralStringRef("\r\n"));
	return writer.finish();
}

// Read at least 1 bytes from conn and up to maxlen in a single read, append read data into *buf
// Returns the number of bytes read.
ACTOR Future<int> read_into_string(Reference<IConnection> conn, std::string* buf, int maxlen) {
	loop {
		// Read into buffer
		int originalSize = buf->size();
		// TODO:  resize is zero-initializing the space we're about to overwrite, so do something else, which probably
		// means not using a string for this buffer
		buf->resize(originalSize + maxlen);
		uint8_t* wptr = (uint8_t*)buf->data() + originalSize;
		int len = conn->read(wptr, wptr + maxlen);
		buf->resize(originalSize + len);

		// Make sure data was actually read, it's possible for there to be none.
		if (len > 0)
			return len;

		// Wait for connection to have something to read
		wait(conn->onReadable());
		wait(delay(0, TaskPriority::ReadSocket));
	}
}

// Returns the position of delim within buf, relative to pos.  If delim is not found, continues to read from conn until
// either it is found or the connection ends, at which point connection_failed is thrown and buf contains
// everything that was read up to that point.
ACTOR Future<size_t> read_delimited_into_string(Reference<IConnection> conn,
                                                const char* delim,
                                                std::string* buf,
                                                size_t pos) {
	state size_t sPos = pos;
	state int lookBack = strlen(delim) - 1;
	ASSERT(lookBack >= 0);

	loop {
		size_t endPos = buf->find(delim, sPos);
		if (endPos != std::string::npos)
			return endPos - pos;
		// Next search will start at the current end of the buffer - delim size + 1
		if (sPos >= lookBack)
			sPos -= lookBack;
		wait(success(read_into_string(conn, buf, CLIENT_KNOBS->HTTP_READ_SIZE)));
	}
}

// Reads from conn (as needed) until there are at least len bytes starting at pos in buf
ACTOR Future<Void> read_fixed_into_string(Reference<IConnection> conn, int len, std::string* buf, size_t pos) {
	state int stop_size = pos + len;
	while (buf->size() < stop_size)
		wait(success(read_into_string(conn, buf, CLIENT_KNOBS->HTTP_READ_SIZE)));
	return Void();
}

ACTOR Future<Void> read_http_response_headers(Reference<IConnection> conn,
                                              Headers* headers,
                                              std::string* buf,
                                              size_t* pos) {
	loop {
		// Get a line, reading more data from conn if necessary
		size_t lineLen = wait(read_delimited_into_string(conn, "\r\n", buf, *pos));

		// If line is empty we have reached the end of the headers.
		if (lineLen == 0) {
			// Increment pos to move past the empty line.
			*pos += 2;
			return Void();
		}

		int nameEnd = -1, valueStart = -1, valueEnd = -1;
		int len = -1;

		// Read header of the form "Name: Value\n"
		// Note that multi line header values are not supported here.
		// Format string breaks down as follows:
		//   %*[^:]%n      Some characters other than ':' which are discarded, save the end position
		//   :%*[ \t]%n    A colon followed by 0 or more spaces or tabs only, save the end position
		//   %*[^\r]%n     Some characters other than \r which are discarded, save the end position
		//   %*1[\r]       Exactly one \r
		//   %*1[\n]       Exactly one \n
		//   %n            Save final end position
		if (sscanf(buf->c_str() + *pos,
		           "%*[^:]%n:%*[ \t]%n%*[^\r]%n%*1[\r]%*1[\n]%n",
		           &nameEnd,
		           &valueStart,
		           &valueEnd,
		           &len) >= 0 &&
		    len > 0) {
			const std::string name(buf->substr(*pos, nameEnd));
			const std::string value(buf->substr(*pos + valueStart, valueEnd - valueStart));
			(*headers)[name] = value;
			*pos += len;
			len = -1;
		} else // Malformed header line (at least according to this simple parsing)
			throw http_bad_response();
	}
}

// Reads an HTTP response from a network connection
// If the connection fails while being read the exception will emitted
// If the response is not parsable or complete in some way, http_bad_response will be thrown
ACTOR Future<Void> read_http_response(Reference<HTTP::Response> r, Reference<IConnection> conn, bool header_only) {
	state std::string buf;
	state size_t pos = 0;

	// Read HTTP reponse code and version line
	size_t lineLen = wait(read_delimited_into_string(conn, "\r\n", &buf, pos));

	int reachedEnd = -1;
	sscanf(buf.c_str() + pos, "HTTP/%f %d%n", &r->version, &r->code, &reachedEnd);
	if (reachedEnd < 0)
		throw http_bad_response();

	// Move position past the line found and the delimiter length
	pos += lineLen + 2;

	// Read headers
	r->headers.clear();

	wait(read_http_response_headers(conn, &r->headers, &buf, &pos));

	auto i = r->headers.find("Content-Length");
	if (i != r->headers.end())
		r->contentLen = atoi(i->second.c_str());
	else
		r->contentLen = -1; // Content length unknown

	state std::string transferEncoding;
	i = r->headers.find("Transfer-Encoding");
	if (i != r->headers.end())
		transferEncoding = i->second;

	r->content.clear();

	// If this is supposed to be a header-only response and the buffer has been fully processed then stop.  Otherwise,
	// there must be response content.
	if (header_only && pos == buf.size())
		return Void();

	// There should be content (or at least metadata describing that there is no content.
	// Chunked transfer and 'normal' mode (content length given, data in one segment after headers) are supported.
	if (r->contentLen >= 0) {
		// Use response content as the buffer so there's no need to copy it later.
		r->content = buf.substr(pos);
		pos = 0;

		// Read until there are at least contentLen bytes available at pos
		wait(read_fixed_into_string(conn, r->contentLen, &r->content, pos));

		// There shouldn't be any bytes after content.
		if (r->content.size() != r->contentLen)
			throw http_bad_response();
	} else if (transferEncoding == "chunked") {
		// Copy remaining buffer data to content which will now be the read buffer for the chunk encoded data.
		// Overall this will be fairly efficient since most bytes will only be written once but some bytes will
		// have to be copied forward in the buffer when removing chunk overhead bytes.
		r->content = buf.substr(pos);
		pos = 0;

		loop {
			{
				// Read the line that contains the chunk length as text in hex
				size_t lineLen = wait(read_delimited_into_string(conn, "\r\n", &r->content, pos));
				state int chunkLen = strtol(r->content.substr(pos, lineLen).c_str(), nullptr, 16);

				// Instead of advancing pos, erase the chunk length header line (line length + delimiter size) from the
				// content buffer
				r->content.erase(pos, lineLen + 2);

				// If chunkLen is 0 then this marks the end of the content chunks.
				if (chunkLen == 0)
					break;

				// Read (if needed) until chunkLen bytes are available at pos, then advance pos by chunkLen
				wait(read_fixed_into_string(conn, chunkLen, &r->content, pos));
				pos += chunkLen;
			}

			{
				// Read the final empty line at the end of the chunk (the required "\r\n" after the chunk bytes)
				size_t lineLen = wait(read_delimited_into_string(conn, "\r\n", &r->content, pos));
				if (lineLen != 0)
					throw http_bad_response();

				// Instead of advancing pos, erase the empty line from the content buffer
				r->content.erase(pos, 2);
			}
		}

		// The content buffer now contains the de-chunked, contiguous content at position 0 to pos.  Save this length.
		r->contentLen = pos;

		// Next is the post-chunk header block, so read that.
		wait(read_http_response_headers(conn, &r->headers, &r->content, &pos));

		// If the header parsing did not consume all of the buffer then something is wrong
		if (pos != r->content.size())
			throw http_bad_response();

		// Now truncate the buffer to just the dechunked contiguous content.
		r->content.erase(r->contentLen);
	} else {
		// Some unrecogize response content scheme is being used.
		throw http_bad_response();
	}

	// If there is actual response content, check the MD5 sum against the Content-MD5 response header
	if (r->content.size() > 0)
		if (!r->verifyMD5(false)) // false arg means do not fail if the Content-MD5 header is missing.
			throw http_bad_response();

	return Void();
}

Future<Void> HTTP::Response::read(Reference<IConnection> conn, bool header_only) {
	return read_http_response(Reference<HTTP::Response>::addRef(this), conn, header_only);
}

// Do a request, get a Response.
// Request content is provided as UnsentPacketQueue *pContent which will be depleted as bytes are sent but the queue
// itself must live for the life of this actor and be destroyed by the caller
// TODO:  pSent is very hackish, do something better.
ACTOR Future<Reference<HTTP::Response>> doRequest(Reference<IConnection> conn,
                                                  std::string verb,
                                                  std::string resource,
                                                  HTTP::Headers headers,
                                                  UnsentPacketQueue* pContent,
                                                  int contentLen,
                                                  Reference<IRateControl> sendRate,
                                                  int64_t* pSent,
                                                  Reference<IRateControl> recvRate,
                                                  std::string requestIDHeader) {
	state TraceEvent event(SevDebug, "HTTPRequest");

	state UnsentPacketQueue empty;
	if (pContent == nullptr)
		pContent = &empty;

	// There is no standard http request id header field, so either a global default can be set via a knob
	// or it can be set per-request with the requestIDHeader argument (which overrides the default)
	if (requestIDHeader.empty()) {
		requestIDHeader = CLIENT_KNOBS->HTTP_REQUEST_ID_HEADER;
	}

	state bool earlyResponse = false;
	state int total_sent = 0;
	state double send_start;

	event.detail("DebugID", conn->getDebugID());
	event.detail("RemoteAddress", conn->getPeerAddress());
	event.detail("Verb", verb);
	event.detail("Resource", resource);
	event.detail("RequestContentLen", contentLen);

	try {
		state std::string requestID;
		if (!requestIDHeader.empty()) {
			requestID = deterministicRandom()->randomUniqueID().toString();
			requestID = requestID.insert(20, "-");
			requestID = requestID.insert(16, "-");
			requestID = requestID.insert(12, "-");
			requestID = requestID.insert(8, "-");

			headers[requestIDHeader] = requestID;
			event.detail("RequestIDSent", requestID);
		}

		// Write headers to a packet buffer chain
		PacketBuffer* pFirst = PacketBuffer::create();
		PacketBuffer* pLast = writeRequestHeader(verb, resource, headers, pFirst);
		// Prepend headers to content packer buffer chain
		pContent->prependWriteBuffer(pFirst, pLast);

		if (CLIENT_KNOBS->HTTP_VERBOSE_LEVEL > 1)
			printf("[%s] HTTP starting %s %s ContentLen:%d\n",
			       conn->getDebugID().toString().c_str(),
			       verb.c_str(),
			       resource.c_str(),
			       contentLen);
		if (CLIENT_KNOBS->HTTP_VERBOSE_LEVEL > 2) {
			for (auto h : headers)
				printf("Request Header: %s: %s\n", h.first.c_str(), h.second.c_str());
		}

		state Reference<HTTP::Response> r(new HTTP::Response());
		state Future<Void> responseReading = r->read(conn, verb == "HEAD" || verb == "DELETE");

		send_start = timer();

		loop {
			// If we already got a response, before finishing sending the request, then close the connection,
			// set the Connection header to "close" as a hint to the caller that this connection can't be used
			// again, and break out of the send loop.
			if (responseReading.isReady()) {
				conn->close();
				r->headers["Connection"] = "close";
				earlyResponse = true;
				break;
			}

			state int trySend = CLIENT_KNOBS->HTTP_SEND_SIZE;
			wait(sendRate->getAllowance(trySend));
			int len = conn->write(pContent->getUnsent(), trySend);
			if (pSent != nullptr)
				*pSent += len;
			sendRate->returnUnused(trySend - len);
			total_sent += len;
			pContent->sent(len);
			if (pContent->empty())
				break;

			wait(conn->onWritable());
			wait(yield(TaskPriority::WriteSocket));
		}

		wait(responseReading);
		double elapsed = timer() - send_start;

		event.detail("ResponseCode", r->code);
		event.detail("ResponseContentLen", r->contentLen);
		event.detail("Elapsed", elapsed);

		Optional<Error> err;
		if (!requestIDHeader.empty()) {
			std::string responseID;
			auto iid = r->headers.find(requestIDHeader);
			if (iid != r->headers.end()) {
				responseID = iid->second;
			}
			event.detail("RequestIDReceived", responseID);

			// If the response code is 5xx (server error) then a response ID is not expected
			// so a missing id will be ignored but a mismatching id will still be an error.
			bool serverError = r->code >= 500 && r->code < 600;

			// If request/response IDs do not match and either this is not a server error
			// or it is but the response ID is not empty then log an error.
			if (requestID != responseID && (!serverError || !responseID.empty())) {
				err = http_bad_request_id();

				TraceEvent(SevError, "HTTPRequestFailedIDMismatch")
				    .error(err.get())
				    .detail("DebugID", conn->getDebugID())
				    .detail("RemoteAddress", conn->getPeerAddress())
				    .detail("Verb", verb)
				    .detail("Resource", resource)
				    .detail("RequestContentLen", contentLen)
				    .detail("ResponseCode", r->code)
				    .detail("ResponseContentLen", r->contentLen)
				    .detail("RequestIDSent", requestID)
				    .detail("RequestIDReceived", responseID);
			}
		}

		if (CLIENT_KNOBS->HTTP_VERBOSE_LEVEL > 0) {
			printf("[%s] HTTP %scode=%d early=%d, time=%fs %s %s contentLen=%d [%d out, response content len %d]\n",
			       conn->getDebugID().toString().c_str(),
			       (err.present() ? format("*ERROR*=%s ", err.get().name()).c_str() : ""),
			       r->code,
			       earlyResponse,
			       elapsed,
			       verb.c_str(),
			       resource.c_str(),
			       contentLen,
			       total_sent,
			       (int)r->contentLen);
		}
		if (CLIENT_KNOBS->HTTP_VERBOSE_LEVEL > 2) {
			printf("[%s] HTTP RESPONSE:  %s %s\n%s\n",
			       conn->getDebugID().toString().c_str(),
			       verb.c_str(),
			       resource.c_str(),
			       r->toString().c_str());
		}

		if (err.present()) {
			throw err.get();
		}

		return r;
	} catch (Error& e) {
		double elapsed = timer() - send_start;
		// A bad_request_id error would have already been logged in verbose mode before err is thrown above.
		if (CLIENT_KNOBS->HTTP_VERBOSE_LEVEL > 0 && e.code() != error_code_http_bad_request_id) {
			printf("[%s] HTTP *ERROR*=%s early=%d, time=%fs %s %s contentLen=%d [%d out]\n",
			       conn->getDebugID().toString().c_str(),
			       e.name(),
			       earlyResponse,
			       elapsed,
			       verb.c_str(),
			       resource.c_str(),
			       contentLen,
			       total_sent);
		}
		event.errorUnsuppressed(e);
		throw;
	}
}

} // namespace HTTP
