/*
 * MappedKeyValueV2.java
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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class MappedKeyValueV2 extends KeyValue {
	private final byte[] rangeBegin;
	private final byte[] rangeEnd;
	private final byte[] paramsBuffer;
	private final List<KeyValue> rangeResult;

	// now it has 4 fields, key, value, getRange.begin, getRange.end
	// this needs to change if FDBMappedKeyValueV2 definition is changed.
	private static final int TOTAL_SERIALIZED_FIELD_FDBMappedKeyValueV2 = 5;

	public MappedKeyValueV2(byte[] key, byte[] value, byte[] rangeBegin, byte[] rangeEnd, List<KeyValue> rangeResult,
	                        byte[] paramsBuffer) {
		super(key, value);
		this.rangeBegin = rangeBegin;
		this.rangeEnd = rangeEnd;
		this.rangeResult = rangeResult;
		this.paramsBuffer = paramsBuffer;
	}

	public byte[] getRangeBegin() { return rangeBegin; }

	public byte[] getRangeEnd() { return rangeEnd; }

	public List<KeyValue> getRangeResult() { return rangeResult; }

	public byte[] getParamsBuffer() { return paramsBuffer; }

	public static MappedKeyValueV2 fromBytes(byte[] bytes, int[] lengths) {
		// Lengths include: key, value, rangeBegin, rangeEnd, count * (underlying key, underlying value)
		if (lengths.length < TOTAL_SERIALIZED_FIELD_FDBMappedKeyValueV2) {
			throw new IllegalArgumentException("There needs to be at least " +
			                                   TOTAL_SERIALIZED_FIELD_FDBMappedKeyValueV2 +
			                                   " lengths to cover the metadata");
		}

		Offset offset = new Offset();
		byte[] key = takeBytes(offset, bytes, lengths);
		byte[] value = takeBytes(offset, bytes, lengths);
		byte[] paramsBuffer = takeBytes(offset, bytes, lengths);
		byte[] rangeBegin = takeBytes(offset, bytes, lengths);
		byte[] rangeEnd = takeBytes(offset, bytes, lengths);

		if ((lengths.length - TOTAL_SERIALIZED_FIELD_FDBMappedKeyValueV2) % 2 != 0) {
			throw new IllegalArgumentException("There needs to be an even number of lengths!");
		}
		int count = (lengths.length - TOTAL_SERIALIZED_FIELD_FDBMappedKeyValueV2) / 2;
		List<KeyValue> rangeResult = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			byte[] k = takeBytes(offset, bytes, lengths);
			byte[] v = takeBytes(offset, bytes, lengths);
			rangeResult.add(new KeyValue(k, v));
		}
		return new MappedKeyValueV2(key, value, rangeBegin, rangeEnd, rangeResult, paramsBuffer);
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
	public boolean equals(Object obj) {
		if (obj == null) return false;
		if (obj == this) return true;
		if (!(obj instanceof MappedKeyValueV2)) return false;

		MappedKeyValueV2 rhs = (MappedKeyValueV2)obj;
		return Arrays.equals(rangeBegin, rhs.rangeBegin) && Arrays.equals(rangeEnd, rhs.rangeEnd) &&
		    Objects.equals(rangeResult, rhs.rangeResult) && Arrays.equals(paramsBuffer, rhs.paramsBuffer);
	}

	@Override
	public int hashCode() {
		int hashForResult = rangeResult == null ? 0 : rangeResult.hashCode();
		return 17 + (29 * hashForResult + +37 * Arrays.hashCode(rangeBegin) + Arrays.hashCode(rangeEnd) +
		             3 * Arrays.hashCode(paramsBuffer));
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("MappedKeyValueV2{");
		sb.append("rangeBegin=").append(ByteArrayUtil.printable(rangeBegin));
		sb.append(", rangeEnd=").append(ByteArrayUtil.printable(rangeEnd));
		sb.append(", paramsBuffer=").append(ByteArrayUtil.printable(paramsBuffer));
		sb.append(", rangeResult=").append(rangeResult);
		sb.append('}');
		return super.toString() + "->" + sb.toString();
	}
}
