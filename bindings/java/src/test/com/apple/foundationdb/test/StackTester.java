/*
 * StackTester.java
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

package com.apple.foundationdb.test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.LocalityUtil;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

/**
 * Implements a cross-binding test of the FoundationDB API.
 *
 */
public class StackTester {
	static final String DIRECTORY_PREFIX = "DIRECTORY_";

	static class WaitEmpty implements Function<Transaction, Void> {
		private final byte[] prefix;
		WaitEmpty(byte[] prefix) {
			this.prefix = prefix;
		}

		@Override
		public Void apply(Transaction tr) {
			List<KeyValue> asList = tr.getRange(Range.startsWith(prefix)).asList().join();
			if(asList.size() > 0) {
				//System.out.println(" - Throwing new fake commit error...");
				throw new FDBException("ERROR: Fake commit conflict", 1020);
			}
			return null;
		}
	}

	static void processInstruction(Instruction inst) {
		try {
			StackOperation op = StackOperation.valueOf(inst.op);
			if(op == StackOperation.PUSH) {
				Object item = inst.tokens.get(1);
				inst.push(item);
				/*if(item instanceof byte[])
					System.out.println(inst.context.preStr + " - " + "Pushing '" + ByteArrayUtil.printable((byte[]) item) + "'");
				else if(item instanceof Number)
					System.out.println(inst.context.preStr + " - " + "Pushing " + ((Number)item).longValue());
				else if(item instanceof String)
					System.out.println(inst.context.preStr + " - " + "Pushing (utf8) '" + item.toString() + "'");
				else if(item == null)
					System.out.println(inst.context.preStr + " - " + "Pushing null");
				else
					System.out.println(inst.context.preStr + " - " + "Pushing item of type " + item.getClass().getName());*/
			}
			else if(op == StackOperation.POP) {
				inst.pop();
			}
			else if(op == StackOperation.DUP) {
				if(inst.size() == 0)
					throw new RuntimeException("No stack bro!! (" + inst.context.preStr + ")");
				StackEntry e = inst.pop();
				inst.push(e);
				inst.push(e);
			}
			else if(op == StackOperation.EMPTY_STACK) {
				inst.clear();
			}
			else if(op == StackOperation.SWAP) {
				List<Object> params = inst.popParams(1).join();
				int index = StackUtils.getInt(params, 0);
				inst.swap(index);
			}
			else if(op == StackOperation.WAIT_FUTURE) {
				StackEntry e = inst.pop();
				inst.push(e.idx, StackUtils.serializeFuture(e.value));
			}
			else if(op == StackOperation.WAIT_EMPTY) {
				List<Object> params = inst.popParams(1).join();
				inst.context.db.run(new WaitEmpty((byte[])params.get(0)));
				inst.push("WAITED_FOR_EMPTY".getBytes());
			}
			else if(op == StackOperation.START_THREAD) {
				List<Object> params = inst.popParams(1).join();
				//System.out.println(inst.context.preStr + " - " + "Starting new thread at prefix: " + ByteArrayUtil.printable((byte[]) params.get(0)));
				inst.context.addContext((byte[])params.get(0));
			}
			else if(op == StackOperation.NEW_TRANSACTION) {
				inst.context.newTransaction();
			}
			else if (op == StackOperation.USE_TRANSACTION) {
				List<Object> params = inst.popParams(1).join();
				inst.context.switchTransaction((byte[])params.get(0));
			}
			else if(op == StackOperation.SET) {
				final List<Object> params = inst.popParams(2).join();
				//System.out.println(inst.context.preStr + " - " + "Setting '" + ArrayUtils.printable((byte[]) params.get(0)) +
				//		"' to '" + ArrayUtils.printable((byte[]) params.get(1)) + "'");
				executeMutation(inst, tr -> {
					tr.set((byte[])params.get(0), (byte[])params.get(1));
					return null;
				});
			}
			else if(op == StackOperation.CLEAR) {
				final List<Object> params = inst.popParams(1).join();
				//System.out.println(inst.context.preStr + " - " + "Clearing: '" + ByteArrayUtil.printable((byte[]) params.get(0)) + "'");
				executeMutation(inst, tr -> {
					tr.clear((byte[])params.get(0));
					return null;
				});
			}
			else if(op == StackOperation.CLEAR_RANGE) {
				final List<Object> params = inst.popParams(2).join();
				executeMutation(inst, tr -> {
					tr.clear((byte[])params.get(0), (byte[])params.get(1));
					return null;
				});
			}
			else if(op == StackOperation.CLEAR_RANGE_STARTS_WITH) {
				final List<Object> params = inst.popParams(1).join();
				executeMutation(inst, tr -> {
					tr.clear(Range.startsWith((byte[])params.get(0)));
					return null;
				});
			}
			else if(op == StackOperation.ATOMIC_OP) {
				final List<Object> params = inst.popParams(3).join();
				final MutationType optype = MutationType.valueOf((String)params.get(0));
				executeMutation(inst, tr -> {
					tr.mutate(optype, (byte[])params.get(1), (byte[])params.get(2));
					return null;
				});
			}
			else if(op == StackOperation.COMMIT) {
				inst.push(inst.tr.commit());
			}
			else if(op == StackOperation.READ_CONFLICT_RANGE) {
				List<Object> params = inst.popParams(2).join();
				inst.tr.addReadConflictRange((byte[])params.get(0), (byte[])params.get(1));
				inst.push("SET_CONFLICT_RANGE".getBytes());
			}
			else if(op == StackOperation.WRITE_CONFLICT_RANGE) {
				List<Object> params = inst.popParams(2).join();
				inst.tr.addWriteConflictRange((byte[])params.get(0), (byte[])params.get(1));
				inst.push("SET_CONFLICT_RANGE".getBytes());
			}
			else if(op == StackOperation.READ_CONFLICT_KEY) {
				List<Object> params = inst.popParams(1).join();
				inst.tr.addReadConflictKey((byte[])params.get(0));
				inst.push("SET_CONFLICT_KEY".getBytes());
			}
			else if(op == StackOperation.WRITE_CONFLICT_KEY) {
				List<Object> params = inst.popParams(1).join();
				inst.tr.addWriteConflictKey((byte[])params.get(0));
				inst.push("SET_CONFLICT_KEY".getBytes());
			}
			else if(op == StackOperation.DISABLE_WRITE_CONFLICT) {
				inst.tr.options().setNextWriteNoWriteConflictRange();
			}
			else if(op == StackOperation.RESET) {
				inst.context.newTransaction();
			}
			else if(op == StackOperation.CANCEL) {
				inst.tr.cancel();
			}
			else if(op == StackOperation.GET) {
				List<Object> params = inst.popParams(1).join();
				CompletableFuture<byte[]> f = inst.readTcx.read(readTr -> readTr.get((byte[])params.get(0)));
				inst.push(f);
			}
			else if (op == StackOperation.GET_ESTIMATED_RANGE_SIZE) {
				List<Object> params = inst.popParams(2).join();
				Long size = inst.readTr.getEstimatedRangeSizeBytes((byte[])params.get(0), (byte[])params.get(1)).join();
				inst.push("GOT_ESTIMATED_RANGE_SIZE".getBytes());
			}
			else if(op == StackOperation.GET_RANGE) {
				List<Object> params = inst.popParams(5).join();

				byte[] begin = (byte[])params.get(0);
				byte[] end = (byte[])params.get(1);
				int limit = StackUtils.getInt(params.get(2));
				boolean reverse = StackUtils.getBoolean(params.get(3));
				StreamingMode mode = inst.context.streamingModeFromCode(
						StackUtils.getInt(params.get(4), StreamingMode.ITERATOR.code()));

				/*System.out.println("GetRange: " + ByteArrayUtil.printable(begin) +
						", " + ByteArrayUtil.printable(end));*/

				List<byte[]> items = inst.readTcx.read(readTr -> executeRangeQuery(readTr.getRange(begin, end, limit, reverse, mode)));
				inst.push(Tuple.fromItems(items).pack());
			}
			else if(op == StackOperation.GET_RANGE_SELECTOR) {
				List<Object> params = inst.popParams(10).join();

				KeySelector start = StackUtils.createSelector(params.get(0),params.get(1), params.get(2));
				KeySelector end = StackUtils.createSelector(params.get(3), params.get(4), params.get(5));
				int limit = StackUtils.getInt(params.get(6));
				boolean reverse = StackUtils.getBoolean(params.get(7));
				StreamingMode mode = inst.context.streamingModeFromCode(
						StackUtils.getInt(params.get(8), StreamingMode.ITERATOR.code()));

				List<byte[]> items = inst.readTcx.read(readTr -> executeRangeQuery(readTr.getRange(start, end, limit, reverse, mode), (byte[])params.get(9)));
				inst.push(Tuple.fromItems(items).pack());
			}
			else if(op == StackOperation.GET_RANGE_STARTS_WITH) {
				List<Object> params = inst.popParams(4).join();

				byte[] prefix = (byte[])params.get(0);
				int limit = StackUtils.getInt(params.get(1));
				boolean reverse = StackUtils.getBoolean(params.get(2));
				StreamingMode mode = inst.context.streamingModeFromCode(
						StackUtils.getInt(params.get(3), StreamingMode.ITERATOR.code()));

				List<byte[]> items = inst.readTcx.read(readTr -> executeRangeQuery(readTr.getRange(Range.startsWith(prefix), limit, reverse, mode)));
				inst.push(Tuple.fromItems(items).pack());
			}
			else if(op == StackOperation.GET_KEY) {
				List<Object> params = inst.popParams(4).join();
				KeySelector start = StackUtils.createSelector(params.get(0),params.get(1), params.get(2));
				byte[] key = inst.readTcx.read(readTr -> filterKeyResult(readTr.getKey(start).join(), (byte[])params.get(3)));
				inst.push(key);
			}
			else if(op == StackOperation.GET_READ_VERSION) {
				inst.context.lastVersion = inst.readTr.getReadVersion().join();
				inst.push("GOT_READ_VERSION".getBytes());
			}
			else if(op == StackOperation.GET_COMMITTED_VERSION) {
				inst.context.lastVersion = inst.tr.getCommittedVersion();
				inst.push("GOT_COMMITTED_VERSION".getBytes());
			}
			else if(op == StackOperation.GET_APPROXIMATE_SIZE) {
				Long size = inst.tr.getApproximateSize().join();
				inst.push("GOT_APPROXIMATE_SIZE".getBytes());
			}
			else if(op == StackOperation.GET_VERSIONSTAMP) {
				inst.push(inst.tr.getVersionstamp());
			}
			else if(op == StackOperation.SET_READ_VERSION) {
				if(inst.context.lastVersion == null)
					throw new IllegalArgumentException("Read version has not been read " + inst.context.preStr);
				inst.tr.setReadVersion(inst.context.lastVersion);
			}
			else if(op == StackOperation.ON_ERROR) {
				List<Object> params = inst.popParams(1).join();
				int errorCode = StackUtils.getInt(params, 0);

				// 1102 (future_released) and 2015 (future_not_set) are not errors to Java.
				//  This is never encountered by user code, so we have to do something rather
				//  messy here to get compatibility with other languages.
				//
				// First, try on error with a retryable error. If it fails, then the transaction is in
				//  a failed state and we should rethrow the error. Otherwise, throw the original error.
				boolean filteredError = errorCode == 1102 || errorCode == 2015;

				FDBException err = new FDBException("Fake testing error", filteredError ? 1020 : errorCode);

				try {
					Transaction tr = inst.tr.onError(err).join();
					if(!inst.setTransaction(tr)) {
						tr.close();
					}
				}
				catch(Throwable t) {
					inst.context.newTransaction(); // Other bindings allow reuse of non-retryable transactions, so we need to emulate that behavior.
					throw t;
				}

				if(filteredError) {
					throw new FDBException("Fake testing error", errorCode);
				}

				inst.push(CompletableFuture.completedFuture((Void)null));
			}
			else if(op == StackOperation.SUB) {
				List<Object> params = inst.popParams(2).join();
				BigInteger a = StackUtils.getBigInteger(params.get(0));
				BigInteger b = StackUtils.getBigInteger(params.get(1));
				inst.push(a.subtract(b));
			}
			else if(op == StackOperation.CONCAT) {
				List<Object> params = inst.popParams(2).join();
				if(params.get(0) instanceof String) {
					inst.push((String)params.get(0) + (String)params.get(1));
				}
				else {
					inst.push(ByteArrayUtil.join((byte[])params.get(0), (byte[])params.get(1)));
				}
			}
			else if(op == StackOperation.TUPLE_PACK) {
				List<Object> params = inst.popParams(1).join();
				int tupleSize = StackUtils.getInt(params.get(0));
				//System.out.println(inst.context.preStr + " - " + "Packing top " + tupleSize + " items from stack");
				List<Object> elements = inst.popParams(tupleSize).join();
				byte[] coded = Tuple.fromItems(elements).pack();
				//System.out.println(inst.context.preStr + " - " + " -> result '" + ByteArrayUtil.printable(coded) + "'");
				inst.push(coded);
			}
			else if(op == StackOperation.TUPLE_PACK_WITH_VERSIONSTAMP) {
				List<Object> params = inst.popParams(2).join();
				byte[] prefix = (byte[])params.get(0);
				int tupleSize = StackUtils.getInt(params.get(1));
				//System.out.println(inst.context.preStr + " - " + "Packing top " + tupleSize + " items from stack");
				Tuple tuple = Tuple.fromItems(inst.popParams(tupleSize).join());
				if(!tuple.hasIncompleteVersionstamp() && Math.random() < 0.5) {
					inst.push("ERROR: NONE".getBytes());
				} else {
					try {
						byte[] coded = tuple.packWithVersionstamp(prefix);
						inst.push("OK".getBytes());
						inst.push(coded);
					} catch (IllegalArgumentException e) {
						if (e.getMessage().startsWith("No incomplete")) {
							inst.push("ERROR: NONE".getBytes());
						} else {
							inst.push("ERROR: MULTIPLE".getBytes());
						}
					}
				}
			}
			else if(op == StackOperation.TUPLE_UNPACK) {
				List<Object> params = inst.popParams(1).join();
				/*System.out.println(inst.context.preStr + " - " + "Unpacking tuple code: " +
						ByteArrayUtil.printable((byte[]) params.get(0)));*/
				Tuple t = Tuple.fromBytes((byte[])params.get(0));
				for(Object o : t.getItems()) {
					byte[] itemBytes = Tuple.from(o).pack();
					inst.push(itemBytes);
				}
			}
			else if(op == StackOperation.TUPLE_RANGE) {
				List<Object> params = inst.popParams(1).join();
				int tupleSize = StackUtils.getInt(params, 0);
				//System.out.println(inst.context.preStr + " - " + "Tuple range with top " + tupleSize + " items from stack");
				List<Object> elements = inst.popParams(tupleSize).join();
				Range range = Tuple.fromItems(elements).range();
				inst.push(range.begin);
				inst.push(range.end);
			}
			else if (op == StackOperation.TUPLE_SORT) {
				int listSize = StackUtils.getInt(inst.popParam().join());
				List<Object> rawElements = inst.popParams(listSize).join();
				List<Tuple> tuples = new ArrayList<>(listSize);
				for(Object o : rawElements) {
					// Unpacking a tuple keeps around the serialized representation and uses
					// it for comparison if it's available. To test semantic comparison, recreate
					// the tuple from the item list.
					Tuple t = Tuple.fromBytes((byte[])o);
					tuples.add(Tuple.fromList(t.getItems()));
				}
				Collections.sort(tuples);
				for(Tuple t : tuples) {
					inst.push(t.pack());
				}
			}
			else if (op == StackOperation.ENCODE_FLOAT) {
				Object param = inst.popParam().join();
				byte[] fBytes = (byte[])param;
				float value = ByteBuffer.wrap(fBytes).order(ByteOrder.BIG_ENDIAN).getFloat();
				inst.push(value);
			}
			else if (op == StackOperation.ENCODE_DOUBLE) {
				Object param = inst.popParam().join();
				byte[] dBytes = (byte[])param;
				double value = ByteBuffer.wrap(dBytes).order(ByteOrder.BIG_ENDIAN).getDouble();
				inst.push(value);
			}
			else if (op == StackOperation.DECODE_FLOAT) {
				Object param = inst.popParam().join();
				float value = ((Number)param).floatValue();
				inst.push(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(value).array());
			}
			else if (op == StackOperation.DECODE_DOUBLE) {
				Object param = inst.popParam().join();
				double value = ((Number)param).doubleValue();
				inst.push(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(value).array());
			}
			else if(op == StackOperation.UNIT_TESTS) {
				try {
					inst.context.db.options().setLocationCacheSize(100001);
					inst.context.db.run(tr -> {
						FDB fdb = FDB.instance();

						String alreadyStartedMessage = "FoundationDB API already started at different version";
						try {
							FDB.selectAPIVersion(fdb.getAPIVersion() + 1);
							throw new IllegalStateException("Was not stopped from selecting two API versions");
						}
						catch(IllegalArgumentException e) {
							if(!e.getMessage().equals(alreadyStartedMessage)) {
								throw e;
							}
						}
						try {
							FDB.selectAPIVersion(fdb.getAPIVersion() - 1);
							throw new IllegalStateException("Was not stopped from selecting two API versions");
						}
						catch(IllegalArgumentException e) {
							if(!e.getMessage().equals(alreadyStartedMessage)) {
								throw e;
							}
						}

						Database db = tr.getDatabase();
						db.options().setLocationCacheSize(100001);
						db.options().setMaxWatches(10001);
						db.options().setDatacenterId("dc_id");
						db.options().setMachineId("machine_id");
						db.options().setSnapshotRywEnable();
						db.options().setSnapshotRywDisable();
						db.options().setTransactionLoggingMaxFieldLength(1000);
						db.options().setTransactionTimeout(100000);
						db.options().setTransactionTimeout(0);
						db.options().setTransactionMaxRetryDelay(100);
						db.options().setTransactionRetryLimit(10);
						db.options().setTransactionRetryLimit(-1);
						db.options().setTransactionCausalReadRisky();
						db.options().setTransactionIncludePortInAddress();

						tr.options().setPrioritySystemImmediate();
						tr.options().setPriorityBatch();
						tr.options().setCausalReadRisky();
						tr.options().setCausalWriteRisky();
						tr.options().setReadYourWritesDisable();
						tr.options().setReadSystemKeys();
						tr.options().setAccessSystemKeys();
						tr.options().setTransactionLoggingMaxFieldLength(1000);
						tr.options().setTimeout(60*1000);
						tr.options().setRetryLimit(50);
						tr.options().setMaxRetryDelay(100);
						tr.options().setUsedDuringCommitProtectionDisable();
						tr.options().setDebugTransactionIdentifier("my_transaction");
						tr.options().setLogTransaction();
						tr.options().setReadLockAware();
						tr.options().setLockAware();
						tr.options().setIncludePortInAddress();

						if(!(new FDBException("Fake", 1020)).isRetryable() ||
								(new FDBException("Fake", 10)).isRetryable())
							throw new RuntimeException("Unit test failed: Error predicate incorrect");

						byte[] test = {(byte) 0xff};
						tr.get(test).join();

						return null;
					});

					testWatches(inst.context.db);
					testLocality(inst.context.db);
				}
				catch(Exception e) {
					throw new RuntimeException("Unit tests failed: " + e.getMessage());
				}
			}
			else if(op == StackOperation.LOG_STACK) {
				List<Object> params = inst.popParams(1).join();
				byte[] prefix = (byte[]) params.get(0);

				Map<Integer, StackEntry> entries = new HashMap<>();
				while(inst.size() > 0) {
					entries.put(inst.size()-1, inst.pop());
					if(entries.size() == 100) {
						logStack(inst.context.db, entries, prefix);
						entries.clear();
					}
				}

				logStack(inst.context.db, entries, prefix);
			}
			else {
				throw new IllegalArgumentException("Unrecognized (or unimplemented) operation");
			}
		} catch (FDBException e) {
			//System.out.println(" Pushing error! (" + e.getMessage() + ")");
			StackUtils.pushError(inst, e);
			//throw e;
		} catch (CompletionException e) {
			FDBException ex = StackUtils.getRootFDBException(e);
			if(ex == null) {
				throw e;
			}

			StackUtils.pushError(inst, ex);
		}
	}

	static class SynchronousContext extends Context {
		DirectoryExtension directoryExtension = new DirectoryExtension();

		SynchronousContext(Database db, byte[] prefix) {
			super(db, prefix);
		}

		@Override
		Context createContext(byte[] prefix) {
			return new SynchronousContext(this.db, prefix);
		}

		void processOp(byte[] operation) {
			Tuple tokens = Tuple.fromBytes(operation);
			Instruction inst = new Instruction(this, tokens);
			
			//if(!inst.op.equals("PUSH") && !inst.op.equals("SWAP"))
			//	System.out.println(inst.context.preStr + " - " + "OP (" + inst.context.instructionIndex + "):" + inst.op);
			/*for(Object o : inst.tokens.getItems())
				System.out.print(", " + o);*/

			if(inst.op.startsWith(DIRECTORY_PREFIX))
				directoryExtension.processInstruction(inst);
			else
				processInstruction(inst);

			inst.releaseTransaction();
		}

		@Override
		void executeOperations() {
			while(true) {
				List<KeyValue> keyValues = db.read(readTr -> readTr.getRange(nextKey, endKey/*, 1000*/).asList().join());
				if(keyValues.size() == 0) {
					break;
				}
				//System.out.println(" * Got " + keyValues.size() + " instructions");

				for(KeyValue next : keyValues) {
					nextKey = KeySelector.firstGreaterThan(next.getKey());
					processOp(next.getValue());
					instructionIndex++;
				}
			}

			//System.out.println(" * Completed " + instructionIndex + " instructions");
		}
	}

	private static void executeMutation(Instruction inst, Function<Transaction, Void> r) {
		// run this with a retry loop (and commit)
		inst.tcx.run(r);
		if(inst.isDatabase)
			inst.push("RESULT_NOT_PRESENT".getBytes());
	}

	static byte[] filterKeyResult(byte[] key, final byte[] prefixFilter) {
		if(ByteArrayUtil.startsWith(key, prefixFilter)) {
			return key;
		}
		else if(ByteArrayUtil.compareUnsigned(key, prefixFilter) < 0) { 
			return prefixFilter;
		}
		else {
			return ByteArrayUtil.strinc(prefixFilter);
		}
	}

	private static List<byte[]> executeRangeQuery(AsyncIterable<KeyValue> itr) {
		return executeRangeQuery(itr, null);
	}

	private static List<byte[]> executeRangeQuery(AsyncIterable<KeyValue> itr, byte[] prefixFilter) {
		if(Math.random() < 0.5)
			return getRange(itr, prefixFilter);
		else
			return getRangeAsList(itr, prefixFilter);
	}

	private static List<byte[]> getRange(AsyncIterable<KeyValue> itr, byte[] prefixFilter) {
		//System.out.println("GetRange");
		List<byte[]> o = new LinkedList<>();
		for(KeyValue kv : itr) {
			if(prefixFilter == null || ByteArrayUtil.startsWith(kv.getKey(), prefixFilter)) {
				o.add(kv.getKey());
				o.add(kv.getValue());
			}
		}

		return o;
	}

	private static List<byte[]> getRangeAsList(AsyncIterable<KeyValue> itr, byte[] prefixFilter) {
		//System.out.println("GetRangeAsList");
		List<KeyValue> list = itr.asList().join();
		List<byte[]> o = new LinkedList<>();
		for(KeyValue kv : list) {
			if(prefixFilter == null || ByteArrayUtil.startsWith(kv.getKey(), prefixFilter)) {
				o.add(kv.getKey());
				o.add(kv.getValue());
			}
		}

		return o;
	}

	private static void logStack(Database db, Map<Integer, StackEntry> entries, byte[] prefix) {
		db.run(tr -> {
			for(Map.Entry<Integer, StackEntry> it : entries.entrySet()) {
				byte[] pk = Tuple.from(it.getKey(), it.getValue().idx).pack(prefix);
				byte[] pv = Tuple.from(StackUtils.serializeFuture(it.getValue().value)).pack();
				tr.set(pk, pv.length < 40000 ? pv : Arrays.copyOfRange(pv, 0, 40000));
			}

			return null;
		});
	}

	private static boolean checkWatches(List<CompletableFuture<Void>> watches, Database db, boolean expected) {
		for(CompletableFuture<Void> w : watches) {
			if(w.isDone() || expected) {
				try {
					w.join();
					if(!expected) {
						throw new IllegalStateException("A watch triggered too early");
					}
				}
				catch(FDBException e) {
					try(Transaction tr = db.createTransaction()) {
						tr.onError(e).join();
						return false;
					}
				}
			}
		}

		return true;
	}

	private static void testWatches(Database db) {
		while(true) {
			db.run(tr -> {
				tr.set("foo".getBytes(), "f".getBytes());
				tr.clear("bar".getBytes());
				return null;
			});

			List<CompletableFuture<Void>> watches = db.run(tr -> {
				List<CompletableFuture<Void>> watchList = new LinkedList<>();
				watchList.add(tr.watch("foo".getBytes()));
				watchList.add(tr.watch("bar".getBytes()));

				tr.set("foo".getBytes(), "f".getBytes());
				return watchList;
			});

			db.run(tr -> {
				tr.clear("bar".getBytes());
				return null;
			});

			try {
				Thread.sleep(5000);
			}
			catch(InterruptedException e) {
				e.printStackTrace();
				// continue...
			}

			if(!checkWatches(watches, db, false)) {
				continue;
			}

			db.run(tr -> {
				tr.set("foo".getBytes(), "bar".getBytes());
				tr.set("bar".getBytes(), "foo".getBytes());
				return null;
			});

			if(checkWatches(watches, db, true)) {
				return;
			}
		}
	}

	private static void testLocality(Database db) {
		db.run(tr -> {
			tr.options().setTimeout(60*1000);
			tr.options().setReadSystemKeys();
			tr.getReadVersion().join();
			CloseableAsyncIterator<byte[]> boundaryKeys = LocalityUtil.getBoundaryKeys(
					tr, new byte[0], new byte[]{(byte) 255, (byte) 255});
			try {
				List<byte[]> keys = AsyncUtil.collectRemaining(boundaryKeys).join();
				for(int i = 0; i < keys.size() - 1; i++) {
					byte[] start = keys.get(i);
					byte[] end = tr.getKey(KeySelector.lastLessThan(keys.get(i + 1))).join();
					List<String> startAddresses = Arrays.asList(LocalityUtil.getAddressesForKey(tr, start).join());
					List<String> endAddresses = Arrays.asList(LocalityUtil.getAddressesForKey(tr, end).join());
					for(String a : startAddresses) {
						if(!endAddresses.contains(a)) {
							throw new RuntimeException("Locality not internally consistent.");
						}
					}
				}

				return null;
			}
			finally {
				boundaryKeys.close();
			}
		});
	}

	/**
	 * Run a stack-machine based test.
	 *
	 * @param args 0: version, 1: snapshot
	 */
	public static void main(String[] args) {
		if(args.length < 1)
			throw new IllegalArgumentException("StackTester needs parameters <prefix> <optional_cluster_file>");
		byte[] prefix = args[0].getBytes();

		if(FDB.isAPIVersionSelected()) {
			throw new IllegalStateException("API version already set to " + FDB.instance().getAPIVersion());
		}
		try {
			FDB.instance();
			throw new IllegalStateException("Able to get API instance before selecting API version");
		}
		catch(FDBException e) {
			if(e.getCode() != 2200) {
				throw e;
			}
		}
		int apiVersion = Integer.parseInt(args[1]);
		FDB fdb = FDB.selectAPIVersion(apiVersion);
		if(FDB.instance().getAPIVersion() != apiVersion) {
			throw new IllegalStateException("API version not correctly set to " + apiVersion);
		}
		Database db;
		if(args.length == 2)
			db = fdb.open();
		else
			db = fdb.open(args[2]);

		Context c = new SynchronousContext(db, prefix);
		//System.out.println("Starting test...");
		c.run();
		//System.out.println("Done with test.");
		db.close();
		System.gc();
	}

	private StackTester() {}
}

