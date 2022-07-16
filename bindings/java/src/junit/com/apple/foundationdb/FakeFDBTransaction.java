/*
 * FakeFDBTransaction.java
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.apple.foundationdb.tuple.ByteArrayUtil;

/**
 * A convenience class that makes it easier to construct a mock FDBTransaction.
 * This class does no native library calls: instead, it seeks to mimic (in a
 * simplistic way) what the FDB native API promises. The intent is to make it
 * easier to unit test specific java code without requiring a running server or
 * needing to test the entire C library as well.
 *
 * Note that this is a bit of a work-in-progress. The idea here is to use this
 * where handy, and discover (in the process) which native calls are being made,
 * then modify this class as appropriate to avoid them.
 */
public class FakeFDBTransaction extends FDBTransaction {
	private final NavigableMap<byte[], byte[]> backingData;
	private final Executor executor;

	private int numRangeCalls = 0;

	protected FakeFDBTransaction(long cPtr, Database database, Executor executor) {
		super(cPtr, database, executor);
		this.backingData = new TreeMap<>(ByteArrayUtil.comparator());
		this.executor = executor;
	}

	public FakeFDBTransaction(Map<byte[], byte[]> backingData, long cPtr, Database db, Executor executor) {
		this(cPtr, db, executor);
		this.backingData.putAll(backingData);
	}

	public FakeFDBTransaction(Collection<Map.Entry<byte[], byte[]>> backingData, long cPtr, Database db,
	                          Executor executor) {
		this(cPtr, db, executor);

		for (Map.Entry<byte[], byte[]> entry : backingData) {
			this.backingData.put(entry.getKey(), entry.getValue());
		}
	}

	public FakeFDBTransaction(List<KeyValue> backingData, long cPtr, Database db,
	                          Executor executor) {
		this(cPtr, db, executor);

		for (KeyValue entry : backingData) {
			this.backingData.put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public CompletableFuture<byte[]> get(byte[] key) {
		return CompletableFuture.completedFuture(this.backingData.get(key));
	}

	/**
	 * @return the number of times getRange_internal() was called. Useful for
	 *         checking underlying behavior.
	 */
	public int getNumRangeCalls() { return numRangeCalls; }

	@Override
	protected FutureResults getRange_internal(KeySelector begin, KeySelector end,
	                                          int rowLimit, int targetBytes, int streamingMode, int iteration,
	                                          boolean isSnapshot, boolean reverse) {
		numRangeCalls++;
		// TODO this is probably not correct for all KeySelector instances--we'll want to match with real behavior
		NavigableMap<byte[], byte[]> range =
		    backingData.subMap(begin.getKey(), begin.orEqual(), end.getKey(), end.orEqual());
		if (reverse) {
			// reverse the order of the scan
			range = range.descendingMap();
		}

		// holder variable so that we can pass the range to the results function safely
		final NavigableMap<byte[], byte[]> retMap = range;
		FutureResults fr = new FutureResults(-1L, false, executor, null) {
			@Override
			protected void registerMarshalCallback(Executor executor) {
				// no-op
			}

			@Override
			protected RangeResultInfo getIfDone_internal(long cPtr) throws FDBException {
				return new RangeResultInfo(this);
			}

			@Override
			public RangeResult getResults() {
				List<KeyValue> kvs = new ArrayList<>();
				boolean more = false;
				int rowCount = 0;
				int sizeBytes = 0;
				for (Map.Entry<byte[], byte[]> kvEntry : retMap.entrySet()) {
					kvs.add(new KeyValue(kvEntry.getKey(), kvEntry.getValue()));
					rowCount++;
					if (rowLimit > 0 && rowCount == rowLimit) {
						more = true;
						break;
					}
					sizeBytes += kvEntry.getKey().length + kvEntry.getValue().length;
					if (targetBytes > 0 && sizeBytes >= targetBytes) {
						more = true;
						break;
					}
				}

				return new RangeResult(kvs, more);
			}

			@Override
			public void close() {
				// no-op
			}
		};

		fr.complete(new RangeResultInfo(fr));
		return fr;
	}

	@Override
	protected void closeInternal(long cPtr) {
		// no-op
	}

	@Override
	public void close() {
		// no-op
	}

	@Override
	protected void finalize() throws Throwable {
		// no-op
	}
}
