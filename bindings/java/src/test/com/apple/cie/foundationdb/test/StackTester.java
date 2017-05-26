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

package com.apple.cie.foundationdb.test;

import java.util.*;

import com.apple.cie.foundationdb.Database;
import com.apple.cie.foundationdb.FDB;
import com.apple.cie.foundationdb.FDBException;
import com.apple.cie.foundationdb.KeySelector;
import com.apple.cie.foundationdb.KeyValue;
import com.apple.cie.foundationdb.LocalityUtil;
import com.apple.cie.foundationdb.MutationType;
import com.apple.cie.foundationdb.Range;
import com.apple.cie.foundationdb.ReadTransaction;
import com.apple.cie.foundationdb.StreamingMode;
import com.apple.cie.foundationdb.Transaction;
import com.apple.cie.foundationdb.async.AsyncIterable;
import com.apple.cie.foundationdb.async.Function;
import com.apple.cie.foundationdb.async.Future;
import com.apple.cie.foundationdb.async.ReadyFuture;
import com.apple.cie.foundationdb.tuple.ByteArrayUtil;
import com.apple.cie.foundationdb.tuple.Tuple;

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
			List<KeyValue> asList = tr.getRange(Range.startsWith(prefix)).asList().get();
			if(asList.size() > 0) {
				//System.out.println(" - Throwing new fake commit error...");
				throw new FDBException("ERROR: Fake commit conflict", 1020);
			}
			return null;
		}
	}

	static void processInstruction(final Instruction inst) throws Throwable {
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
				List<Object> params = inst.popParams(1).get();
				int index = StackUtils.getInt(params, 0);
				inst.swap(index);
			}
			else if(op == StackOperation.WAIT_FUTURE) {
				StackEntry e = inst.pop();
				inst.push(e.idx, StackUtils.serializeFuture(e.value));
			}
			else if(op == StackOperation.WAIT_EMPTY) {
				List<Object> params = inst.popParams(1).get();
				inst.context.db.run(new WaitEmpty((byte [])params.get(0)));
				inst.push("WAITED_FOR_EMPTY".getBytes());
			}
			else if(op == StackOperation.START_THREAD) {
				List<Object> params = inst.popParams(1).get();
				//System.out.println(inst.context.preStr + " - " + "Starting new thread at prefix: " + ByteArrayUtil.printable((byte[]) params.get(0)));
				inst.context.addContext((byte[])params.get(0));
			}
			else if(op == StackOperation.NEW_TRANSACTION) {
				inst.context.newTransaction();
			}
			else if (op == StackOperation.USE_TRANSACTION) {
				List<Object> params = inst.popParams(1).get();
				inst.context.switchTransaction((byte[])params.get(0));
			}
			else if(op == StackOperation.SET) {
				final List<Object> params = inst.popParams(2).get();
				//System.out.println(inst.context.preStr + " - " + "Setting '" + ArrayUtils.printable((byte[]) params.get(0)) +
				//		"' to '" + ArrayUtils.printable((byte[]) params.get(1)) + "'");
				executeMutation(inst,
					new Function<Transaction, Void>() {
						@Override
						public Void apply(Transaction tr) {
							tr.set((byte[])params.get(0), (byte[])params.get(1));
							return null;
						}
					});
			}
			else if(op == StackOperation.CLEAR) {
				final List<Object> params = inst.popParams(1).get();
				//System.out.println(inst.context.preStr + " - " + "Clearing: '" + ByteArrayUtil.printable((byte[]) params.get(0)) + "'");
				executeMutation(inst,
					new Function<Transaction, Void>() {
						@Override
						public Void apply(Transaction tr) {
							tr.clear((byte[])params.get(0));
							return null;
						}
					}
				);
			}
			else if(op == StackOperation.CLEAR_RANGE) {
				final List<Object> params = inst.popParams(2).get();
				executeMutation(inst,
						new Function<Transaction, Void>() {
						@Override
						public Void apply(Transaction tr) {
							tr.clear((byte[])params.get(0), (byte[])params.get(1));
							return null;
						}
					});
			}
			else if(op == StackOperation.CLEAR_RANGE_STARTS_WITH) {
				final List<Object> params = inst.popParams(1).get();
				executeMutation(inst,
						new Function<Transaction, Void>() {
						@Override
						public Void apply(Transaction tr) {
							tr.clear(Range.startsWith((byte[])params.get(0)));
							return null;
						}
					});
			}
			else if(op == StackOperation.ATOMIC_OP) {
				final List<Object> params = inst.popParams(3).get();
				final MutationType optype = MutationType.valueOf((String)params.get(0));
				executeMutation(inst,
					new Function<Transaction, Void>() {
						@Override
						public Void apply(Transaction tr) {
							tr.mutate(optype, (byte[])params.get(1), (byte[])params.get(2));
							return null;
						}
					}
				);
			}
			else if(op == StackOperation.COMMIT) {
				inst.push(inst.tr.commit());
			}
			else if(op == StackOperation.READ_CONFLICT_RANGE) {
				List<Object> params = inst.popParams(2).get();
				inst.tr.addReadConflictRange((byte[])params.get(0), (byte[])params.get(1));
				inst.push("SET_CONFLICT_RANGE".getBytes());
			}
			else if(op == StackOperation.WRITE_CONFLICT_RANGE) {
				List<Object> params = inst.popParams(2).get();
				inst.tr.addWriteConflictRange((byte[])params.get(0), (byte[])params.get(1));
				inst.push("SET_CONFLICT_RANGE".getBytes());
			}
			else if(op == StackOperation.READ_CONFLICT_KEY) {
				List<Object> params = inst.popParams(1).get();
				inst.tr.addReadConflictKey((byte[])params.get(0));
				inst.push("SET_CONFLICT_KEY".getBytes());
			}
			else if(op == StackOperation.WRITE_CONFLICT_KEY) {
				List<Object> params = inst.popParams(1).get();
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
				final List<Object> params = inst.popParams(1).get();
				Future<byte[]> f = inst.readTcx.read(new Function<ReadTransaction, Future<byte[]>>() {
					@Override
					public Future<byte[]> apply(ReadTransaction readTr) {
						return inst.readTr.get((byte[])params.get(0));
					}
				});

				inst.push(f);
			}
			else if(op == StackOperation.GET_RANGE) {
				List<Object> params = inst.popParams(5).get();

				final byte[] begin = (byte[])params.get(0);
				final byte[] end = (byte[])params.get(1);
				final int limit = StackUtils.getInt(params.get(2));
				final boolean reverse = StackUtils.getBoolean(params.get(3));
				final StreamingMode mode = inst.context.streamingModeFromCode(
						StackUtils.getInt(params.get(4), StreamingMode.ITERATOR.code()));

				/*System.out.println("GetRange: " + ByteArrayUtil.printable(begin) +
						", " + ByteArrayUtil.printable(end));*/

				List<byte[]> items = inst.readTcx.read(new Function<ReadTransaction, List<byte[]>>() {
					@Override
					public List<byte[]> apply(ReadTransaction readTr) {
						return executeRangeQuery(readTr.getRange(begin, end, limit, reverse, mode));
					}
				});

				inst.push(Tuple.fromItems(items).pack());
			}
			else if(op == StackOperation.GET_RANGE_SELECTOR) {
				final List<Object> params = inst.popParams(10).get();

				final KeySelector start = StackUtils.createSelector(params.get(0),params.get(1), params.get(2));
				final KeySelector end = StackUtils.createSelector(params.get(3), params.get(4), params.get(5));
				final int limit = StackUtils.getInt(params.get(6));
				final boolean reverse = StackUtils.getBoolean(params.get(7));
				final StreamingMode mode = inst.context.streamingModeFromCode(
						StackUtils.getInt(params.get(8), StreamingMode.ITERATOR.code()));

				List<byte[]> items = inst.readTcx.read(new Function<ReadTransaction, List<byte[]>>() {
					@Override
					public List<byte[]> apply(ReadTransaction readTr) {
						return executeRangeQuery(readTr.getRange(start, end, limit, reverse, mode), (byte[])params.get(9));
					}
				});

				inst.push(Tuple.fromItems(items).pack());
			}
			else if(op == StackOperation.GET_RANGE_STARTS_WITH) {
				List<Object> params = inst.popParams(4).get();

				final byte[] prefix = (byte[])params.get(0);
				final int limit = StackUtils.getInt(params.get(1));
				final boolean reverse = StackUtils.getBoolean(params.get(2));
				final StreamingMode mode = inst.context.streamingModeFromCode(
						StackUtils.getInt(params.get(3), StreamingMode.ITERATOR.code()));

				List<byte[]> items = inst.readTcx.read(new Function<ReadTransaction, List<byte[]>>() {
					@Override
					public List<byte[]> apply(ReadTransaction readTr) {
						return executeRangeQuery(readTr.getRange(Range.startsWith(prefix), limit, reverse, mode));
					}
				});

				inst.push(Tuple.fromItems(items).pack());
			}
			else if(op == StackOperation.GET_KEY) {
				List<Object> params = inst.popParams(4).get();
				final KeySelector start = StackUtils.createSelector(params.get(0),params.get(1), params.get(2));

				byte[] key = inst.readTcx.read(new Function<ReadTransaction, byte[]>() {
					@Override
					public byte[] apply(ReadTransaction readTr) {
						return inst.readTr.getKey(start).get();
					}
				});

				inst.push(filterKeyResult(key, (byte[])params.get(3)));
			}
			else if(op == StackOperation.GET_READ_VERSION) {
				inst.context.lastVersion = inst.readTr.getReadVersion().get();
				inst.push("GOT_READ_VERSION".getBytes());
			}
			else if(op == StackOperation.GET_COMMITTED_VERSION) {
				inst.context.lastVersion = inst.tr.getCommittedVersion();
				inst.push("GOT_COMMITTED_VERSION".getBytes());
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
				List<Object> params = inst.popParams(1).get();
				int errorCode = StackUtils.getInt(params, 0);

				// 1102 (future_released) is not an error to Java. This is never encountered by user code,
				//  so we have to do something rather messy here to get compatibility with other languages.
				//
				// First, try on error with a retryable error. If it fails, then the transaction is in
				//  a failed state and we should rethrow the error. Otherwise, throw the original error.
				boolean filteredError = errorCode == 1102;

				FDBException err = new FDBException("Fake testing error", filteredError ? 1020 : errorCode);

				try {
					inst.setTransaction(inst.tr.onError(err).get());
				}
				catch(Throwable t) {
					inst.context.newTransaction(); // Other bindings allow reuse of non-retryable transactions, so we need to emulate that behavior.
					throw t;
				}

				if(filteredError) {
					throw new FDBException("Fake testing error", errorCode);
				}

				inst.push(ReadyFuture.DONE);
			}
			else if(op == StackOperation.SUB) {
				List<Object> params = inst.popParams(2).get();
				long result = StackUtils.getNumber(params.get(0)).longValue() - StackUtils.getNumber(params.get(1)).longValue();
				inst.push(result);
			}
			else if(op == StackOperation.CONCAT) {
				List<Object> params = inst.popParams(2).get();
				if(params.get(0) instanceof String) {
					inst.push((String)params.get(0) + (String)params.get(1));
				}
				else {
					inst.push(ByteArrayUtil.join((byte[])params.get(0), (byte[])params.get(1)));
				}
			}
			else if(op == StackOperation.TUPLE_PACK) {
				List<Object> params = inst.popParams(1).get();
				int tupleSize = StackUtils.getInt(params.get(0));
				//System.out.println(inst.context.preStr + " - " + "Packing top " + tupleSize + " items from stack");
				List<Object> elements = inst.popParams(tupleSize).get();
				byte[] coded = Tuple.fromItems(elements).pack();
				//System.out.println(inst.context.preStr + " - " + " -> result '" + ByteArrayUtil.printable(coded) + "'");
				inst.push(coded);
			}
			else if(op == StackOperation.TUPLE_UNPACK) {
				List<Object> params = inst.popParams(1).get();
				/*System.out.println(inst.context.preStr + " - " + "Unpacking tuple code: " +
						ByteArrayUtil.printable((byte[]) params.get(0)));*/
				Tuple t = Tuple.fromBytes((byte[])params.get(0));
				for(Object o : t.getItems()) {
					byte[] itemBytes = Tuple.from(o).pack();
					inst.push(itemBytes);
				}
			}
			else if(op == StackOperation.TUPLE_RANGE) {
				List<Object> params = inst.popParams(1).get();
				int tupleSize = StackUtils.getInt(params, 0);
				//System.out.println(inst.context.preStr + " - " + "Tuple range with top " + tupleSize + " items from stack");
				List<Object> elements = inst.popParams(tupleSize).get();
				Range range = Tuple.fromItems(elements).range();
				inst.push(range.begin);
				inst.push(range.end);
			}
			else if(op == StackOperation.UNIT_TESTS) {
				try {
					inst.context.db.options().setLocationCacheSize(100001);
					inst.context.db.run(new Function<Transaction, Void>() {
						@Override
						public Void apply(Transaction tr) {
							tr.options().setPrioritySystemImmediate();
							tr.options().setPriorityBatch();
							tr.options().setCausalReadRisky();
							tr.options().setCausalWriteRisky();
							tr.options().setReadYourWritesDisable();
							tr.options().setReadAheadDisable();
							tr.options().setReadSystemKeys();
							tr.options().setAccessSystemKeys();
							tr.options().setDurabilityDevNullIsWebScale();
							tr.options().setTimeout(60*1000);
							tr.options().setRetryLimit(50);
							tr.options().setMaxRetryDelay(100);
							tr.options().setUsedDuringCommitProtectionDisable();
							tr.options().setTransactionLoggingEnable("my_transaction");

							if(!(new FDBException("Fake", 1020)).isRetryable() ||
									(new FDBException("Fake", 10)).isRetryable())
								throw new RuntimeException("Unit test failed: Error predicate incorrect");

							byte[] test = {(byte) 0xff};
							tr.get(test).get();

							return null;
						}
					});

					testWatches(inst.context.db);
					testLocality(inst.context.db);
				}
				catch(Exception e) {
					throw new Exception("Unit tests failed: " + e.getMessage());
				}
			}
			else if(op == StackOperation.LOG_STACK) {
				List<Object> params = inst.popParams(1).get();
				byte[] prefix = (byte[]) params.get(0);

				Map<Integer, StackEntry> entries = new HashMap<Integer, StackEntry>();
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
		} catch (IllegalStateException e) {
			//Java throws this instead of an FDBException for error code 2015, so we have to translate it
			if(e.getMessage().equals("Future not ready")) 
				StackUtils.pushError(inst, new FDBException("", 2015));
			else
				throw e;
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

		void processOp(byte[] operation) throws FDBException, Throwable {
			Tuple tokens = Tuple.fromBytes(operation);
			Instruction inst = new Instruction(this, tokens);
			
			//if(!inst.op.equals("PUSH") && !inst.op.equals("SWAP"))
			//	System.out.println(inst.context.preStr + " - " + "OP (" + inst.context.instructionIndex + "):" + inst.op);
			/*for(Object o : op.tokens.getItems())
				System.out.print(", " + o);*/

			if(inst.op.startsWith(DIRECTORY_PREFIX))
				directoryExtension.processInstruction(inst);
			else
				processInstruction(inst);
		}

		@Override
		void executeOperations() throws Throwable {
			KeySelector begin = nextKey;
			Transaction t = db.createTransaction();
			while(true) {
				List<KeyValue> keyValues = null;
				try {
					keyValues = t.getRange(begin, endKey/*, 1000*/).asList().get();
				}
				catch(FDBException e) {
					t = t.onError(e).get();
					continue;
				}

				//System.out.println(" * Got " + keyValues.size() + " instructions");
				if(keyValues.size() == 0)
					break;

				for(KeyValue next : keyValues) {
					begin = KeySelector.firstGreaterThan(next.getKey());
					processOp(next.getValue());
					instructionIndex++;
				}
			}
			//System.out.println(" * Completed " + instructionIndex + " instructions");
		}
	}

	private static void executeMutation(Instruction inst, Function<Transaction, Void> r)
	throws FDBException, Exception {
		// run this with a retry loop (and commit)
		inst.tcx.run(r);
		if(inst.isDatabase)
			inst.push("RESULT_NOT_PRESENT".getBytes());
	}

	private static byte[] filterKeyResult(byte[] key, final byte[] prefixFilter) {
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
		List<byte[]> o = new LinkedList<byte[]>();
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
		List<KeyValue> list = itr.asList().get();
		List<byte[]> o = new LinkedList<byte[]>();
		for(KeyValue kv : list) {
			if(prefixFilter == null || ByteArrayUtil.startsWith(kv.getKey(), prefixFilter)) {
				o.add(kv.getKey());
				o.add(kv.getValue());
			}
		}

		return o;
	}

	private static void logStack(final Database db, final Map<Integer, StackEntry> entries, final byte[] prefix) {
		db.run(new Function<Transaction, Void>() {
		    @Override
			public Void apply(Transaction tr) {
				for(Map.Entry<Integer, StackEntry> it : entries.entrySet()) {
					byte[] pk = ByteArrayUtil.join(prefix, Tuple.from(it.getKey(), it.getValue().idx).pack());
					byte[] pv = Tuple.from(StackUtils.serializeFuture(it.getValue().value)).pack();
					tr.set(pk, pv.length < 40000 ? pv : Arrays.copyOfRange(pv, 0, 40000));
				}

				return null;
			}
		});
	}

	private static boolean checkWatches(List<Future<Void>> watches, Database db, boolean expected) {
		for(Future<Void> w : watches) {
			if(w.isDone() || expected) {
				try {
					w.get();
					if(!expected) {
						throw new IllegalStateException("A watch triggered too early");
					}
				}
				catch(FDBException e) {
					Transaction tr = db.createTransaction();
					tr.onError(e).get();
					return false;
				}
			}
		}

		return true;
	}

	private static void testWatches(Database db) {
		while(true) {
			db.run(new Function<Transaction, Void>() {
				@Override
				public Void apply(Transaction tr) {
					tr.set("foo".getBytes(), "f".getBytes());
					tr.clear("bar".getBytes());
					return null;
				}
			});

			List<Future<Void>> watches = db.run(new Function<Transaction, List<Future<Void>>>() {
				@Override
				public List<Future<Void>> apply(Transaction tr) {
					List<Future<Void>> watchList = new LinkedList<Future<Void>>();
					watchList.add(tr.watch("foo".getBytes()));
					watchList.add(tr.watch("bar".getBytes()));
					tr.set("foo".getBytes(), "f".getBytes());
					return watchList;
				}
			});

			db.run(new Function<Transaction, Void>() {
				@Override
				public Void apply(Transaction tr) {
					tr.clear("bar".getBytes());
					return null;
				}
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

			db.run(new Function<Transaction, Void>() {
				@Override
				public Void apply(Transaction tr) {
					tr.set("foo".getBytes(), "bar".getBytes());
					tr.set("bar".getBytes(), "foo".getBytes());
					return null;
				}
			});

			if(checkWatches(watches, db, true)) {
				return;
			}
		}
	}

	private static void testLocality(Database db) {
		db.run(new Function<Transaction, Void>() {
			@Override
			public Void apply(Transaction tr) {
				tr.options().setTimeout(60*1000);
				tr.options().setReadSystemKeys();
				tr.getReadVersion().get();

				AsyncIterable<byte[]> boundaryKeys = LocalityUtil.getBoundaryKeys(
						tr, new byte[0], new byte[]{(byte) 255, (byte) 255});

				List<byte[]> keys = boundaryKeys.asList().get();
				for(int i = 0; i < keys.size() - 1; i++) {
					byte[] start = keys.get(i);
					byte[] end = tr.getKey(KeySelector.lastLessThan(keys.get(i + 1))).get();
					List<String> startAddresses = Arrays.asList(LocalityUtil.getAddressesForKey(tr, start).get());
					List<String> endAddresses = Arrays.asList(LocalityUtil.getAddressesForKey(tr, end).get());
					for(String a : startAddresses) {
						if(!endAddresses.contains(a)) {
							throw new RuntimeException("Locality not internally consistent.");
						}
					}
				}

				return null;
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

		/*Thread t = new Thread(new Runnable(){
			@Override
			public void run() {
				try {
					Thread.sleep(1000 * 60 * 2);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
				System.out.println("Printing traces for " + traces.size() + " threads.");
				for(Thread thread : traces.keySet()) {
					System.out.println(" Thread (" + thread.getName() + ")");
					StackTraceElement[] tr = traces.get(thread);
					for(StackTraceElement e : tr) {
						System.out.println("  " + e);
					}
				}
			}});
		t.setDaemon(true);
		t.start();*/

		byte[] prefix = args[0].getBytes();

		FDB fdb = FDB.selectAPIVersion(Integer.parseInt(args[1]));
		Database db;
		/*fdb.startNetwork();
		Cluster cluster = fdb.createCluster(args.length > 1 ? args[1] : null, new Executor() {
			public void execute(Runnable r) {
				r.run();
			}
		});

		db = cluster.openDatabase();*/
		if(args.length == 2)
			db = fdb.open();
		else
			db = fdb.open(args[2]);

		Context c = new SynchronousContext(db, prefix);
		//System.out.println("Starting test...");
		c.run();
		//System.out.println("Done with test.");
	}
}

