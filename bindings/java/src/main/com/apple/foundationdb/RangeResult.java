/*
 * RangeResult.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

import java.util.ArrayList;
import java.util.List;

class RangeResult {
	final List<KeyValue> values;
	final boolean more;

	//mostly present for testing purposes
	RangeResult(List<KeyValue> values, boolean more) {
		this.values = values;
		this.more = more;
	}

	RangeResult(byte[] keyValues, int[] lengths, boolean more) {
		if(lengths.length % 2 != 0) {
			throw new IllegalArgumentException("There needs to be an even number of lenghts!");
		}

		int count = lengths.length / 2;
		values = new ArrayList<KeyValue>(count);

		int offset = 0;
		for(int i = 0; i < count; i++) {
			int keyLength = lengths[i * 2];
			int valueLength = lengths[(i * 2) + 1];

			byte[] k = new byte[keyLength];
			System.arraycopy(keyValues, offset, k, 0, keyLength);

			byte[] v = new byte[valueLength];
			System.arraycopy(keyValues, offset + keyLength, v, 0, valueLength);

			offset += keyLength + valueLength;
			values.add(new KeyValue(k, v));
		}
		this.more = more;
	}

	RangeResult(RangeResultDirectBufferIterator iterator) {
		iterator.readResultsSummary();
		more = iterator.hasMore();

		int count = iterator.count();
		values = new ArrayList<KeyValue>(count);

		for (int i = 0; i < count; ++i) {
			values.add(iterator.next());
		}
	}

	public RangeResultSummary getSummary() {
		final int keyCount = values.size();
		final byte[] lastKey = keyCount > 0 ? values.get(keyCount -1).getKey() : null;
		return new RangeResultSummary(lastKey, keyCount, more);
	}
}
