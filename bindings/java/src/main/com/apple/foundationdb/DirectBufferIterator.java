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
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

/**
 * A singleton that manages a pool of {@link DirectByteBuffer}, that will be shared
 * by the {@link DirectBufferIterator} instances. It is responsibilty of user to
 * return the borrowed buffers.
 */
class BufferPool {
	static final BufferPool __instance = new BufferPool();
	static private final int NUM_BUFFERS = 128;
	private ArrayBlockingQueue<ByteBuffer> buffers = new ArrayBlockingQueue<>(NUM_BUFFERS);

	public BufferPool() {
		while (buffers.size() < NUM_BUFFERS) {
			ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 512);
			assert  (buffer != null);
			buffers.add(buffer);
		}
	}

	public static BufferPool getInstance() {
		return __instance;
	}

	/**
     * Requests a {@link #DirectByteBuffer} from our pool, and block if needed.
     */
	public synchronized ByteBuffer poll() {
		return buffers.poll();
	}

	/**
	 * Returns the {@link #DirectByteBuffer} that was borrowed from our pool. This is
	 * non-blocking as it was borrowed from this pool.
	 */
	public synchronized void add(ByteBuffer buffer) {
		buffers.offer(buffer);
	}
}

/**
 * Holds the direct buffer that is shared with JNI wrapper. A typical usage is as follows:
 * 
 * 1. Call {@link #init()} which borrows {@code DirectByteBuffer} from {@link BufferPool}.
 *    This step will block until a buffer is successfully borrowed from {@link BufferPool}.
 * 2. Prepare the request by calling {@link #prepareRequest()} function. This will serialize
 *    the request into ByteBuffer.
 * 3. Use {@link #onResultReady()} to get future, when the results are ready.
 * 4. Call {@link #readSummary()} to read the metadata of the result from buffer. At this point,
 *    results will be ready.
 * 5. Use it like normal iterator.
 * 6. When done, call {@link #close()} to return the buffer to {@link BufferPool}.
 *
 * TODO (Vishesh): Document the binary format.
 */
class DirectBufferIterator implements Iterator<KeyValue>, Closeable {
	private ByteBuffer byteBuffer;
	private int current = 0;
	private int resultOffset = 0;
	private RangeResultSummary summary;
	private final CompletableFuture<Boolean> promise = new CompletableFuture<>();

	public DirectBufferIterator() {
		byteBuffer = BufferPool.getInstance().poll();
		byteBuffer.order(ByteOrder.nativeOrder());
	}

	@Override
	public void close() {
		if (byteBuffer != null) {
			BufferPool.getInstance().add(byteBuffer);
			byteBuffer = null;
		}
	}

	public CompletableFuture<Boolean> onResultReady() {
		return promise;
	}

	public boolean hasResultReady() {
		return summary != null;
	}

	@Override
	public boolean hasNext() {
		assert (hasResultReady());
		return current < summary.keyCount;
	}

	@Override
	public KeyValue next() {
		assert (hasNext());
		assert (hasResultReady());

		final int keyLen = byteBuffer.getInt();
		final int valueLen = byteBuffer.getInt();
		byte[] key = new byte[keyLen];
		byteBuffer.get(key);

		byte[] value = new byte[valueLen];
		byteBuffer.get(value);

		current += 1;
		return new KeyValue(key, value);
	}

	public ByteBuffer getBuffer() {
		return byteBuffer;
	}

	public RangeResultSummary getSummary() {
		assert (hasResultReady());
		return summary;
	}

	// public String toString() {
	// 	return String.format("DirectBufferIterator{KeyCount=%d, Current=%d, More=%b, LastKey=\"%s\", Ref=%s}\n",
	// 			summary.keyCount, current, summary.more, summary.lastKey, super.toString());
	// }

	public int currentIndex() {
		return current;
	}

	public void readSummary() {
		byteBuffer.rewind();
		byteBuffer.position(0);

		final int keyCount = byteBuffer.getInt();
		final boolean more = byteBuffer.getInt() > 0;
		final int lastKeyLen = byteBuffer.getInt();

		byte[] lastKey = null;
		if (lastKeyLen > 0) {
			lastKey = new byte[lastKeyLen];
			byteBuffer.get(lastKey);
		}

		summary = new RangeResultSummary(lastKey, keyCount, more);
	}
}
