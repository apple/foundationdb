/*
 * SendBufferIterator.h
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

#ifndef __SENDBUFFER_ITERATOR_H__
#define __SENDBUFFER_ITERATOR_H__

#include "flow/serialize.h"

#include <boost/asio.hpp>

class SendBufferIterator {
	SendBuffer const* p;
	int limit;

public:
	using value_type = boost::asio::const_buffer;
	using iterator_category = std::forward_iterator_tag;
	using difference_type = size_t;
	using pointer = boost::asio::const_buffer*;
	using reference = boost::asio::const_buffer&;

	SendBufferIterator(SendBuffer const* p = nullptr, int limit = std::numeric_limits<int>::max());

	bool operator==(SendBufferIterator const& r) const { return p == r.p; }
	bool operator!=(SendBufferIterator const& r) const { return p != r.p; }
	void operator++();

	boost::asio::const_buffer operator*() const;
};

#endif
