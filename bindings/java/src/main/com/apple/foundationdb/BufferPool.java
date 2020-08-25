/*
 * DirectBufferIterator.java
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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

/**
 * A singleton that manages a pool of {@link DirectByteBuffer}, that will be
 * shared by the {@link DirectBufferIterator} instances. It is responsibilty of
 * user to return the borrowed buffers.
 */
class DirectBufferPool {
	static final DirectBufferPool __instance = new DirectBufferPool();

	static private final int DEFAULT_NUM_BUFFERS = 128;
	static private final int DEFAULT_BUFFER_SIZE = 1024 * 512;

	private ArrayBlockingQueue<ByteBuffer> buffers;

	public DirectBufferPool() {
		resize(DEFAULT_NUM_BUFFERS, DEFAULT_BUFFER_SIZE);
	}

	public static DirectBufferPool getInstance() {
		return __instance;
	}

	public synchronized void resize(int newSize, int bufferSize) {
		buffers = new ArrayBlockingQueue<>(newSize);
		while (buffers.size() < newSize) {
			ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
			assert (buffer != null);
			buffers.add(buffer);
		}
	}

	/**
	 * Requests a {@link #DirectByteBuffer} from our pool. Returns null if pool is empty.
	 */
	public synchronized ByteBuffer poll() {
		return buffers.poll();
	}

	/**
	 * Returns the {@link #DirectByteBuffer} that was borrowed from our pool. This
	 * is non-blocking as it was borrowed from this pool.
	 */
	public synchronized void add(ByteBuffer buffer) {
		buffers.offer(buffer);
	}
}
