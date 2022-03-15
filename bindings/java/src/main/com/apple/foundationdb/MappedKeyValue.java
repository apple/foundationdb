/*
 * MappedKeyValue.java
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

import com.apple.foundationdb.tuple.ByteArrayUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class MappedKeyValue extends KeyValue {
	private final byte[] rangeBegin;
	private final byte[] rangeEnd;
	private final List<KeyValue> rangeResult;

	MappedKeyValue(byte[] key, byte[] value, byte[] rangeBegin, byte[] rangeEnd, List<KeyValue> rangeResult) {
		super(key, value);
		this.rangeBegin = rangeBegin;
		this.rangeEnd = rangeEnd;
		this.rangeResult = rangeResult;
	}

	public byte[] getRangeBegin() { return rangeBegin; }

	public byte[] getRangeEnd() { return rangeEnd; }

	public List<KeyValue> getRangeResult() { return rangeResult; }

	public static MappedKeyValue fromBytes(byte[] bytes, int[] lengths) {
		// Lengths include: key, value, rangeBegin, rangeEnd, count * (underlying key, underlying value)
		if (lengths.length < 4) {
			throw new IllegalArgumentException("There needs to be at least 4 lengths to cover the metadata");
		}

		Offset offset = new Offset();
		byte[] key = takeBytes(offset, bytes, lengths);
		byte[] value = takeBytes(offset, bytes, lengths);
		byte[] rangeBegin = takeBytes(offset, bytes, lengths);
		byte[] rangeEnd = takeBytes(offset, bytes, lengths);

		if ((lengths.length - 4) % 2 != 0) {
			throw new IllegalArgumentException("There needs to be an even number of lengths!");
		}
		int count = (lengths.length - 4) / 2;
		List<KeyValue> rangeResult = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			byte[] k = takeBytes(offset, bytes, lengths);
			byte[] v = takeBytes(offset, bytes, lengths);
			rangeResult.add(new KeyValue(k, v));
		}
		return new MappedKeyValue(key, value, rangeBegin, rangeEnd, rangeResult);
	}

	static class Offset {
		int bytes = 0;
		int lengths = 0;
	}

	static byte[] takeBytes(Offset offset, byte[] bytes, int[] lengths) {
		int len = lengths[offset.lengths];
		byte[] b = new byte[len];
		System.arraycopy(bytes, offset.bytes, b, 0, len);
		offset.lengths++;
		offset.bytes += len;
		return b;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("MappedKeyValue{");
		sb.append("rangeBegin=").append(ByteArrayUtil.printable(rangeBegin));
		sb.append(", rangeEnd=").append(ByteArrayUtil.printable(rangeEnd));
		sb.append(", rangeResult=").append(rangeResult);
		sb.append('}');
		return super.toString() + "->" + sb.toString();
	}
}