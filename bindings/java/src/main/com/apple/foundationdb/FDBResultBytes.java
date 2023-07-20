/*
 * FDBResultBytes.java
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

import java.util.Arrays;
import com.apple.foundationdb.tuple.ByteArrayUtil;

class FDBResultBytes implements ResultBytes {
	private byte[] bytes;

	private float serverBusyness;
	private float rangeBusyness;

	FDBResultBytes(byte[] bytes, float serverBusyness, float rangeBusyness) {
		this.bytes = bytes;
		this.serverBusyness = serverBusyness;
		this.rangeBusyness = rangeBusyness;
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}

	@Override
	public float getServerBusyness() {
		return serverBusyness;
	}

	@Override
	public float getRangeBusyness() {
		return rangeBusyness;
	}

	// Equality comparison does not consider the read metrics
	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj == this)
			return true;
		if (!(obj instanceof ResultBytes))
			return false;

		ResultBytes rhs = (ResultBytes)obj;
		return Arrays.equals(bytes, rhs.getBytes());
	}

	// Hash code does not consider the read metrics
	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}

	@Override
	public String toString() {
		return ByteArrayUtil.printable(bytes);
	}
}