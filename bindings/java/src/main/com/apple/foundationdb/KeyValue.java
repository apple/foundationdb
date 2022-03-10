/*
 * KeyValue.java
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

import java.util.Arrays;

/**
 * A key/value pair. Range read operation on FoundationDB return {@code KeyValue}s.
 *  This is a simple value type; mutating it won't affect your {@code Transaction} or
 *  the {@code Database}.
 *
 */
public class KeyValue {
	private final byte[] key, value;

	/**
	 * Constructs a new {@code KeyValue} from the specified key and value.
	 *
	 * @param key the key portion of the pair
	 * @param value the value portion of the pair
	 */
	public KeyValue(byte[] key, byte[] value) {
		this.key = key;
		this.value = value;
	}

	/**
	 * Gets the key from the pair.
	 *
	 * @return the key
	 */
	public byte[] getKey() {
		return this.key;
	}

	/**
	 * Gets the value from the pair.
	 *
	 * @return the value
	 */
	public byte[] getValue() {
		return this.value;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj == this)
			return true;
		if (!(obj instanceof KeyValue))
			return false;

		KeyValue rhs = (KeyValue) obj;
		return Arrays.equals(key, rhs.key) && Arrays.equals(value, rhs.value);
	}

	@Override
	public int hashCode() {
		return 17 + (37 * Arrays.hashCode(key) + Arrays.hashCode(value));
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("KeyValue{");
		sb.append("key=").append(ByteArrayUtil.printable(key));
		sb.append(", value=").append(ByteArrayUtil.printable(value));
		sb.append('}');
		return sb.toString();
	}
}