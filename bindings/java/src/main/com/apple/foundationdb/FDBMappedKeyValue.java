/*
 * FDBMappedKeyValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

class FDBMappedKeyValue extends FDBKeyValue implements MappedKeyValue {
	private final byte[] rangeBegin;
	private final byte[] rangeEnd;
	private final List<KeyValue> rangeResult;

	// now it has 4 fields, key, value, getRange.begin, getRange.end
	// this needs to change if the FDB C FDBMappedKeyValue definition is changed.
	private static final int TOTAL_SERIALIZED_FIELD_FDBMappedKeyValue = 4;

	FDBMappedKeyValue(byte[] key, byte[] value, byte[] rangeBegin, byte[] rangeEnd,
						  List<KeyValue> rangeResult, float serverBusyness, float rangeBusyness) {
		super(key, value, serverBusyness, rangeBusyness);
		this.rangeBegin = rangeBegin;
		this.rangeEnd = rangeEnd;
		this.rangeResult = rangeResult;
	}

	@Override
	public byte[] getRangeBegin() { return rangeBegin; }

	@Override
	public byte[] getRangeEnd() { return rangeEnd; }

	@Override
	public List<KeyValue> getRangeResult() { return rangeResult; }

	static MappedKeyValue fromBytes(byte[] bytes, int[] lengths, float serverBusyness, float rangeBusyness) {
		// Lengths include: key, value, rangeBegin, rangeEnd, count * (underlying key, underlying value)
		if (lengths.length < TOTAL_SERIALIZED_FIELD_FDBMappedKeyValue) {
			throw new IllegalArgumentException("There needs to be at least " +
			                                   TOTAL_SERIALIZED_FIELD_FDBMappedKeyValue +
			                                   " lengths to cover the metadata");
		}

		Offset offset = new Offset();
		byte[] key = takeBytes(offset, bytes, lengths);
		byte[] value = takeBytes(offset, bytes, lengths);
		byte[] rangeBegin = takeBytes(offset, bytes, lengths);
		byte[] rangeEnd = takeBytes(offset, bytes, lengths);

		if ((lengths.length - TOTAL_SERIALIZED_FIELD_FDBMappedKeyValue) % 2 != 0) {
			throw new IllegalArgumentException("There needs to be an even number of lengths!");
		}
		int count = (lengths.length - TOTAL_SERIALIZED_FIELD_FDBMappedKeyValue) / 2;
		List<KeyValue> rangeResult = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			byte[] k = takeBytes(offset, bytes, lengths);
			byte[] v = takeBytes(offset, bytes, lengths);
			rangeResult.add(new FDBKeyValue(k, v, serverBusyness, rangeBusyness));
		}
		return new FDBMappedKeyValue(key, value, rangeBegin, rangeEnd, rangeResult, serverBusyness, rangeBusyness);
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

	// Equality comparison does not consider the the read metrics
	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj == this)
			return true;
		if (!(obj instanceof MappedKeyValue))
			return false;

		MappedKeyValue rhs = (MappedKeyValue) obj;
		return Arrays.equals(rangeBegin, rhs.getRangeBegin()) && Arrays.equals(rangeEnd, rhs.getRangeEnd()) &&
		    Objects.equals(rangeResult, rhs.getRangeResult());
	}

	// Hash code does not consider the the read metrics
	@Override
	public int hashCode() {
		int hashForResult = rangeResult == null ? 0 : rangeResult.hashCode();
		return 17 + (29 * hashForResult + 37 * Arrays.hashCode(rangeBegin) + Arrays.hashCode(rangeEnd));
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