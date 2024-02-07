/*
 * DirectBufferPool.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * A singleton that manages a pool of {@link DirectByteBuffer}, that will be
 * shared by the {@link DirectBufferIterator} instances. It is responsibility of
 * user to return the borrowed buffers.
 */
class DirectBufferPool {
	static final DirectBufferPool __instance = new DirectBufferPool();

	// When tuning this, make sure that the size of the buffer,
	// is always greater than the maximum size KV allowed by FDB.
	// Current limits is :
	//     10kB for key + 100kB for value + 1 int for count + 1 int for more + 2 int for KV size
	static public final int MIN_BUFFER_SIZE = (10 + 100) * 1000 + Integer.BYTES * 4;

	static private final int DEFAULT_NUM_BUFFERS = 128;
	static private final int DEFAULT_BUFFER_SIZE = 1024 * 512;

	private ArrayBlockingQueue<ByteBuffer> buffers;
	private int currentBufferCapacity;

	public DirectBufferPool() {
		resize(DEFAULT_NUM_BUFFERS, DEFAULT_BUFFER_SIZE);
	}

	public static DirectBufferPool getInstance() {
		return __instance;
	}

	/**
	 * Resizes buffer pool with given capacity and buffer size. Throws OutOfMemory exception
	 * if unable to allocate as asked.
	 */
	public synchronized void resize(int newPoolSize, int bufferSize) {
		if (bufferSize < MIN_BUFFER_SIZE) {
			throw new IllegalArgumentException("'bufferSize' must be at-least: " + MIN_BUFFER_SIZE + " bytes");
		}
		buffers = new ArrayBlockingQueue<>(newPoolSize);
		currentBufferCapacity = bufferSize;
		while (buffers.size() < newPoolSize) {
			ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
			buffers.add(buffer);
		}
	}

	/**
	 * Requests a {@link DirectByteBuffer} from our pool. Returns null if pool is empty.
	 */
	public synchronized ByteBuffer poll() {
		return buffers.poll();
	}

	/**
	 * Returns the {@link DirectByteBuffer} that was borrowed from our pool.
	 */
	public synchronized void add(ByteBuffer buffer) {
		if (buffer.capacity() != currentBufferCapacity) {
			// This can happen when a resize is called while there are outstanding requests,
			// older buffers will be returned eventually.
			return;
		}

		buffers.offer(buffer);
	}
}
