/*
 * MappedRangeResultDirectBufferIterator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Holds the direct buffer that is shared with JNI wrapper.
 */
class MappedRangeResultDirectBufferIterator extends DirectBufferIterator implements Iterator<KeyValue> {

	MappedRangeResultDirectBufferIterator(ByteBuffer buffer) { super(buffer); }

	@Override
	public boolean hasNext() {
		return super.hasNext();
	}

	@Override
	public MappedKeyValue next() {
		assert (hasResultReady()); // Must be called once its ready.
		if (!hasNext()) {
			throw new NoSuchElementException();
		}

		final byte[] key = getString();
		final byte[] value = getString();
		final byte[] rangeBegin = getString();
		final byte[] rangeEnd = getString();
		final int rangeResultSize = byteBuffer.getInt();
		List<KeyValue> rangeResult = new ArrayList();
		for (int i = 0; i < rangeResultSize; i++) {
			final byte[] k = getString();
			final byte[] v = getString();
			rangeResult.add(new KeyValue(k, v));
		}
		current += 1;
		return new MappedKeyValue(key, value, rangeBegin, rangeEnd, rangeResult);
	}

	private byte[] getString() {
		final int len = byteBuffer.getInt();
		byte[] s = new byte[len];
		byteBuffer.get(s);
		return s;
	}
}