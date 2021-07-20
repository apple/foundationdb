/*
 * multithread.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_MULTITHREAD_H
#define FLOW_MULTITHREAD_H

#pragma once

#include <cstdio>
#include <memory>

#include <vector>
#include <queue>
#include <map>
#include <unordered_map>
#include <set>
#include <functional>
#include <iostream>
#include <string>
#include <utility>
#include <algorithm>

#include "boost/lockfree/spsc_queue.hpp"
#include "boost/lockfree/policies.hpp"

template <class T>
class ThreadFutureStream {
public:
	explicit ThreadFutureStream(std::shared_ptr<boost::lockfree::spsc_queue<T>> queue) : queue(queue) {}

	ThreadFutureStream() : queue(nullptr) {}

	// to get the message:
	// message << stream  or stream >> message
	template <typename Type>
	friend bool operator>>(ThreadFutureStream<Type>& stream, Type& out);

	template <typename Type>
	friend bool operator<<(Type& out, ThreadFutureStream<Type>& stream);

private:
	std::shared_ptr<boost::lockfree::spsc_queue<T>> queue;
};

template <class T>
class ThreadPromiseStream {
public:
	explicit ThreadPromiseStream(std::size_t capacity)
	  : queue(std::make_shared<boost::lockfree::spsc_queue<T>>(capacity)) {}

	ThreadPromiseStream() : queue(nullptr) {}

	// to pass the message:
	// message >> stream  or stream << message
	template <typename Type>
	friend bool operator<<(ThreadPromiseStream<Type>& stream, const Type& in);

	template <typename Type>
	friend bool operator>>(const Type& in, ThreadPromiseStream<Type>& stream);

	ThreadFutureStream<T> getFutureStream() { return ThreadFutureStream<T>(queue); }

private:
	std::shared_ptr<boost::lockfree::spsc_queue<T>> queue;
};

#endif