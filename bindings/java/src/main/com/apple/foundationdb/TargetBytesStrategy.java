/*
 * TargetBytesStrategy.java
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

/**
 * Strategy interface  for defining how to execute range queries and set the soft
 * target bytes limit more flexibly in java.
 * 
 * Notes on operational realities:
 * 
 * The target_bytes flag in the FDB client is a soft hint limiting the amount of 
 * <em>record data</em> that comes back (it does not include key and value size fields
 * in its calculations). However, every getRange call is guaranteed to return at least
 * 1 record (assuming that there is 1 record within the scan boundaries). This occurs
 * even if the target_bytes limit is set to a lower value than the size of the record being
 * returned. So if you are trying to read a record that is 1k in size, but the first buffer 
 * is configured to 256, you'll still get that record back.
 * 
 * What these strategies do is allow the RangeQuery to control those buffer sizes more effectively.
 * Larger buffer sizes will work more effectively for larger record sizes, but may consume more memory
 * so be careful. Conversely, smaller buffer sizes may use less memory, but can be considerably slower, 
 * depending on the size of the records being returned. 
 * 
 * The specific strategy itself matters as well. For known large scans, it may be more effective to use
 * a fixed-size buffer space, to avoid wasting calls on small buffers. Similarly, for a known small scan,
 * using fixed small buffers might be optimal. For most other cases, the power-of-2 strategy is probably
 * the best performing overall.
 * 
 * Note that, for legacy compatibility reasons, the default strategy is to defer to the native library, which
 * is a fixed-array strategy that grows such that the next buffer size is (more of less) 1.5 times larger
 * than the previous (for more detail see {@code iteration_progression} in {@code bindings/c/fdb_c.cpp}). 
 * This strategy performs well for very small keys, but may not be optimal when there are larger keys in the 
 * dataset.
 * 
 */
public interface TargetBytesStrategy {
	// setting target limit bytes to be 0 will cause the native library to use its own internal bytes limiting
	TargetBytesStrategy DEFAULT_NATIVE_STRATEGY = new Fixed(0);

	/**
	 * Get the target byte size to use for this iteration. Note that the target byte size is a hint,
	 * not a requirement of the native layer (the native layer may ignore this value if it specifies a value that is
	 * too large--by default around 2^16)
	 *
	 * @param iteration the iteration in the range scan
	 * @return the target byte size for this iteration
	 */
	int getTargetByteSize(int iteration);

	/**
	 * Generate a Target bytes strategy such that at each iteration the buffer size is {@code 1<<(iteration+basePower)} where 
	 * {@code basePower} is the smallest power of 2 that is larger than {@code initialSize}.
	 * 
	 * Note that if the power exceeds the maximum allowed native buffer size (around 2^16), then the returned buffer is scaled
	 * down to be the maximum possible buffer size.
	 * 
	 * @param initialSize the initial buffer size. The first buffer size is actually the smallest power of 2 that is >= {@code initialSize}
	 * @return a strategy which uses power of two growth in buffer sizes.
	 */
	public static TargetBytesStrategy powerOf2(int initialSize) { 
		if(initialSize <0){
			throw new IllegalArgumentException("Cannot create a buffer with a negative size");
		} 	
		return new PowerOf2(initialSize); 
	}

	/**
	 * Generate a byte strategy such that the buffer size is the same for every iteration.
	 * 
	 * Note that if the size the maximum allowed native buffer size (around 2^16), then the returned buffer is scaled
	 * down to be the maximum possible buffer size.
	 * 
	 * @param size the size of the buffer to use.
	 * @return a strategy which uses a fixed size for every iteration
	 */
	public static TargetBytesStrategy fixed(int size) { 
		if (size <-1 ){
			throw new IllegalArgumentException("Cannot create a buffer with a negative size");
		}
		return new Fixed(size); 
	}

	/**
	 * Generate a byte strategy which defers to the native buffering logic.
	 * 
	 * See {@code bindings/c/fdb_c.cpp} for more information on how the native buffering
	 * logic works.
	 * 
	 * @return a strategy which defers to the native library for buffer sizing
	 */
	static TargetBytesStrategy deferToNative() { return DEFAULT_NATIVE_STRATEGY; }

	/**
	 * Create a strategy which uses a fixed set of values for the buffer sizes. If the iteration
	 * exceeds the length of the passed in array, it will use the large value in the array for all subsequent
	 * buffer sizes.
	 * 
	 * Note that if any size is larger than the maximum allowed buffer size, then it will be downscaled to
	 * the largest allowed buffer size.
	 * 
	 * @param bufferSizes the array of buffers that are allowed. Must have at least one value
	 * @return a strategy using {@code bufferSizes} as it's set of buffer sizes.
	 */
	static TargetBytesStrategy fixedArray(int... bufferSizes) { 
		if(bufferSizes==null || bufferSizes.length <1 ){
			throw new IllegalArgumentException("Cannot create a fixed array strategy without at least one buffer size");
		}
		return new FixedArray(bufferSizes); 
	}

	/**
	 * Class representing a Fixed TargetBytesStrategy. To create, use {@link #fixed(int)}.
	 */
	static class Fixed implements TargetBytesStrategy {
		// taken from the native layer, adjust if the native layer changes
		private static final int MAX_FIXED_SIZE = 1<<17; 
		private final int size;

		@Override
		public int getTargetByteSize(int iteration) {
			return size;
		}

		private Fixed(int size) {
			if (size > MAX_FIXED_SIZE) {
				size = MAX_FIXED_SIZE;
			}
			this.size = size;
		}
	}

	/**
	 * Class representing a power-of-2 TargetBytesStrategy. To create, use {@link #powerOf2(int)}
	 */
	static class PowerOf2 implements TargetBytesStrategy {
		private static final int MAX_ITERATION_POWER =
		    17; // keep the buffer size at 131k or below (in practice, the native layer will bound it to 80k anyway)
		private final int base;

		private PowerOf2(int minSize) {
			// find the smallest power of 2 lower than minSize, and that's the base
			int b = 0;
			int c = 1;
			while (c < minSize) {
				c <<= 1;
				b++;
			}
			this.base = b;
		}

		@Override
		public int getTargetByteSize(int iteration) {
			int power = iteration + base;
			if (power > MAX_ITERATION_POWER) {
				power = MAX_ITERATION_POWER;
			}
			return 1 << (iteration + base);
		}
	}


	/**
	 * Class representing a fixed-array strategy. To create, use {@link #fixedArray(int...)}.
	 */
	static class FixedArray implements TargetBytesStrategy {
		private final int[] values;

		private FixedArray(int[] values) { this.values = values; }

		@Override
		public int getTargetByteSize(int iteration) {
			if (iteration >= values.length) {
				return values[values.length - 1];
			} else {
				return values[iteration];
			}
		}

		@Override
		public String toString() {
			return "FixedArray [" +values[0] + "]";
		}
	}
}
