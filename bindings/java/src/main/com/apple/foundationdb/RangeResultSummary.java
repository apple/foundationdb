/*
 * RangeResultSummary.java
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

class RangeResultSummary {
	final byte[] lastKey;
	final int keyCount;
	final boolean more;

	RangeResultSummary(byte[] lastKey, int keyCount, boolean more) {
		this.lastKey = lastKey;
		this.keyCount = keyCount;
		this.more = more;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("RangeResultSummary{");
		sb.append("lastKey=").append(ByteArrayUtil.printable(lastKey));
		sb.append(", keyCount=").append(keyCount);
		sb.append(", more=").append(more);
		sb.append('}');
		return sb.toString();
	}
}
