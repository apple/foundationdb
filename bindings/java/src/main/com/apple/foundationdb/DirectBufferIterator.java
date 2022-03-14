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
import java.util.NoSuchElementException;

/**
 * Holds the direct buffer that is shared with JNI wrapper. A typical usage is as follows:
 * 
 * The serialization format of result is =>
 *     [int keyCount, boolean more, ListOf<(int keyLen, int valueLen, byte[] key, byte[] value)>]
 */
abstract class DirectBufferIterator implements AutoCloseable {
	protected ByteBuffer byteBuffer;
	protected int current = 0;
	protected int keyCount = -1;
	protected boolean more = false;

	public DirectBufferIterator(ByteBuffer buffer) {
		byteBuffer = buffer;
		byteBuffer.order(ByteOrder.nativeOrder());
	}

	@Override
	public void close() {
		if (byteBuffer != null) {
			DirectBufferPool.getInstance().add(byteBuffer);
			byteBuffer = null;
		}
	}

	public boolean hasResultReady() {
		return keyCount > -1;
	}

	public boolean hasNext() {
		assert (hasResultReady());
		return current < keyCount;
	}

	public ByteBuffer getBuffer() {
		return byteBuffer;
	}

	public int count() {
		assert (hasResultReady());
		return keyCount;
	}

	public boolean hasMore() {
		assert (hasResultReady());
		return more;
	}

	public int currentIndex() {
		return current;
	}

	public void readResultsSummary() {
		byteBuffer.rewind();
		byteBuffer.position(0);

		keyCount = byteBuffer.getInt();
		more = byteBuffer.getInt() > 0;
	}
}
