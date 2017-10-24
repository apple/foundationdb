/*
 * AsyncStackTester.java
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
import java.util.*;

import com.apple.foundationdb.Cluster;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.Function;
import com.apple.foundationdb.async.Future;
import com.apple.foundationdb.async.ReadyFuture;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

public class AsyncStackTester {
	static final String DIRECTORY_PREFIX = "DIRECTORY_";

	static class WaitEmpty implements Function<Transaction, Future<Void>> {
		private final byte[] prefix;
		WaitEmpty(byte[] prefix) {
			this.prefix = prefix;
		}

		@Override
		public Future<Void> apply(Transaction tr) {
			return tr.getRange(Range.startsWith(prefix)).asList().map(new Function<List<KeyValue>, Void>() {
				@Override
				public Void apply(List<KeyValue> list) {
					if(list.size() > 0) {
						//System.out.println(" - Throwing new fake commit error...");
						throw new FDBException("ERROR: Fake commit conflict", 1020);
					}
					return null;
				}
			});
		}
	}

	static Future<Void> processInstruction(final Instruction inst) {
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
			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.POP) {
			inst.pop();
			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.DUP) {
			if(inst.size() == 0)
				throw new RuntimeException("No stack bro!! (" + inst.context.preStr + ")");
			StackEntry e = inst.pop();
			inst.push(e);
			inst.push(e);
			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.EMPTY_STACK) {
			inst.clear();
			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.SWAP) {
			return inst.popParam()
			.flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					int index = StackUtils.getInt(param);
					if(index >= inst.size())
						throw new IllegalArgumentException("Stack index not valid");

					inst.swap(index);
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.WAIT_FUTURE) {
			return popAndWait(inst)
			.flatMap(new Function<StackEntry, Future<Void>>() {
				@Override
				public Future<Void> apply(StackEntry e) {
					inst.push(e);
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.WAIT_EMPTY) {
			return inst.popParam()
			.flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					WaitEmpty retryable = new WaitEmpty((byte[])param);
					return inst.context.db.runAsync(retryable).map(new Function<Void, Void>() {
						@Override
						public Void apply(Void o) {
							inst.push( "WAITED_FOR_EMPTY".getBytes());
							return null;
						}
					});
				}
			});
		}
		else if(op == StackOperation.START_THREAD) {
			return inst.popParam()
			.flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					//System.out.println(inst.context.preStr + " - " + "Starting new thread at prefix: " + ByteArrayUtil.printable((byte[]) params.get(0)));
					inst.context.addContext((byte[])param);
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.NEW_TRANSACTION) {
			inst.context.newTransaction();
			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.USE_TRANSACTION) {
			return inst.popParam()
			.map(new Function<Object, Void>() {
				public Void apply(Object param) {
					inst.context.switchTransaction((byte[])param);
					return null;
				}
			});
		}
		else if(op == StackOperation.SET) {
			return inst.popParams(2).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(final List<Object> params) {
					/*System.out.println(inst.context.preStr + " - " + "Setting '" + ByteArrayUtil.printable((byte[]) params.get(0)) +
							"' to '" + ByteArrayUtil.printable((byte[]) params.get(1)) + "'"); */
					return executeMutation(inst, new Function<Transaction, Future<Void>>() {
						@Override
						public Future<Void> apply(Transaction tr) {
							tr.set((byte[])params.get(0), (byte[])params.get(1));
							return ReadyFuture.DONE;
						}
					});
				}
			});
		}
		else if(op == StackOperation.CLEAR) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(final Object param) {
					//System.out.println(inst.context.preStr + " - " + "Clearing: '" + ByteArrayUtil.printable((byte[])param) + "'");
					return executeMutation(inst, new Function<Transaction, Future<Void>>() {
						@Override
						public Future<Void> apply(Transaction tr) {
							tr.clear((byte[])param);
							return ReadyFuture.DONE;
						}
					});
				}
			});
		}
		else if(op == StackOperation.CLEAR_RANGE) {
			return inst.popParams(2).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(final List<Object> params) {
					return executeMutation(inst, new Function<Transaction, Future<Void>>() {
						@Override
						public Future<Void> apply(Transaction tr) {
							tr.clear((byte[])params.get(0), (byte[])params.get(1));
							return ReadyFuture.DONE;
						}
					});
				}
			});
		}
		else if(op == StackOperation.CLEAR_RANGE_STARTS_WITH) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(final Object param) {
					return executeMutation(inst, new Function<Transaction, Future<Void>>() {
						@Override
						public Future<Void> apply(Transaction tr) {
							tr.clear(Range.startsWith((byte[])param));
							return ReadyFuture.DONE;
						}
					});
				}
			});
		}
		else if(op == StackOperation.ATOMIC_OP) {
			return inst.popParams(3).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(final List<Object> params) {
					final MutationType optype = MutationType.valueOf((String)params.get(0));
					return executeMutation(inst,
						new Function<Transaction, Future<Void>>() {
							@Override
							public Future<Void> apply(Transaction tr) {
								tr.mutate(optype, (byte[])params.get(1), (byte[])params.get(2));
								return ReadyFuture.DONE;
							}
						}
					);
				}
			});
		}
		else if(op == StackOperation.COMMIT) {
			inst.push(inst.tr.commit());
			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.RESET) {
			inst.context.newTransaction();
			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.CANCEL) {
			inst.tr.cancel();
			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.READ_CONFLICT_RANGE) {
			return inst.popParams(2).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(List<Object> params) {
					inst.tr.addReadConflictRange((byte[])params.get(0), (byte[])params.get(1));
					inst.push("SET_CONFLICT_RANGE".getBytes());
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.WRITE_CONFLICT_RANGE) {
			return inst.popParams(2).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(List<Object> params) {
					inst.tr.addWriteConflictRange((byte[])params.get(0), (byte[])params.get(1));
					inst.push("SET_CONFLICT_RANGE".getBytes());
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.READ_CONFLICT_KEY) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					inst.tr.addReadConflictKey((byte[])param);
					inst.push("SET_CONFLICT_KEY".getBytes());
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.WRITE_CONFLICT_KEY) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					inst.tr.addWriteConflictKey((byte[])param);
					inst.push("SET_CONFLICT_KEY".getBytes());
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.DISABLE_WRITE_CONFLICT) {
			inst.tr.options().setNextWriteNoWriteConflictRange();
			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.GET) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(final Object param) {
					Future<byte[]> f = inst.readTcx.readAsync(new Function<ReadTransaction, Future<byte[]>>() {
						@Override
						public Future<byte[]> apply(ReadTransaction readTr) {
							return inst.readTr.get((byte[])param);
						}
					});
					inst.push(f);
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.GET_RANGE) {
			return inst.popParams(5).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(final List<Object> params) {
					final int limit = StackUtils.getInt(params.get(2));
					final boolean reverse = StackUtils.getBoolean(params.get(3));
					final StreamingMode mode = inst.context.streamingModeFromCode(
							StackUtils.getInt(params.get(4), StreamingMode.ITERATOR.code()));

					Future<List<KeyValue>> range = inst.readTcx.readAsync(new Function<ReadTransaction, Future<List<KeyValue>>>() {
						@Override
						public Future<List<KeyValue>> apply(ReadTransaction readTr) {
							return readTr.getRange((byte[])params.get(0), (byte[])params.get(1), limit, reverse, mode).asList();
						}
					});

					return pushRange(inst, range);
				}
			});
		}
		else if(op == StackOperation.GET_RANGE_SELECTOR) {
			return inst.popParams(10).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(final List<Object> params) {
					final int limit = StackUtils.getInt(params.get(6));
					final boolean reverse = StackUtils.getBoolean(params.get(7));
					final StreamingMode mode = inst.context.streamingModeFromCode(
							StackUtils.getInt(params.get(8), StreamingMode.ITERATOR.code()));

					final KeySelector start = StackUtils.createSelector(params.get(0),params.get(1), params.get(2));
					final KeySelector end = StackUtils.createSelector(params.get(3), params.get(4), params.get(5));

					Future<List<KeyValue>> range = inst.readTcx.readAsync(new Function<ReadTransaction, Future<List<KeyValue>>>() {
						@Override
						public Future<List<KeyValue>> apply(ReadTransaction readTr) {
							return readTr.getRange(start, end, limit, reverse, mode).asList();
						}
					});

					return pushRange(inst, range, (byte[])params.get(9));
				}
			});
		}
		else if(op == StackOperation.GET_RANGE_STARTS_WITH) {
			return inst.popParams(4).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(final List<Object> params) {
					final int limit = StackUtils.getInt(params.get(1));
					final boolean reverse = StackUtils.getBoolean(params.get(2));
					final StreamingMode mode = inst.context.streamingModeFromCode(
							StackUtils.getInt(params.get(3), StreamingMode.ITERATOR.code()));

					Future<List<KeyValue>> range = inst.readTcx.readAsync(new Function<ReadTransaction, Future<List<KeyValue>>>() {
						@Override
						public Future<List<KeyValue>> apply(ReadTransaction readTr) {
							return readTr.getRange(Range.startsWith((byte[])params.get(0)), limit, reverse, mode).asList();
						}
					});

					return pushRange(inst, range);
				}
			});
		}
		else if(op == StackOperation.GET_KEY) {
			return inst.popParams(4).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(List<Object> params) {
					final KeySelector start = StackUtils.createSelector(params.get(0),params.get(1), params.get(2));
					Future<byte[]> key = inst.readTcx.readAsync(new Function<ReadTransaction, Future<byte[]>>() {
						@Override
						public Future<byte[]> apply(ReadTransaction readTr) {
							return inst.readTr.getKey(start);
						}
					});

					inst.push(executeGetKey(key, (byte[])params.get(3)));
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.GET_READ_VERSION) {
			return inst.readTr.getReadVersion().map(new Function<Long, Void>() {
				@Override
				public Void apply(Long readVersion) {
					inst.context.lastVersion = readVersion;
					inst.push("GOT_READ_VERSION".getBytes());
					return null;
				}
			});
		}
		else if(op == StackOperation.GET_COMMITTED_VERSION) {
			try {
				inst.context.lastVersion = inst.tr.getCommittedVersion();
				inst.push("GOT_COMMITTED_VERSION".getBytes());
			}
			catch(FDBException e) {
				StackUtils.pushError(inst, e);
			}

			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.GET_VERSIONSTAMP) {
			try {
				inst.push(inst.tr.getVersionstamp());
			}
			catch(FDBException e) {
				StackUtils.pushError(inst, e);
			}

			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.SET_READ_VERSION) {
			if(inst.context.lastVersion == null)
				throw new IllegalArgumentException("Read version has not been read");
			inst.tr.setReadVersion(inst.context.lastVersion);
			return ReadyFuture.DONE;
		}
		else if(op == StackOperation.ON_ERROR) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					int errorCode = StackUtils.getInt(param);

					// 1102 (future_released) is not an error to Java. This is never encountered by user code,
					//  so we have to do something rather messy here to get compatibility with other languages.
					//
					// First, try on error with a retryable error. If it fails, then the transaction is in
					//  a failed state and we should rethrow the error. Otherwise, throw the original error.
					boolean filteredError = errorCode == 1102;

					FDBException err = new FDBException("Fake testing error", filteredError ? 1020 : errorCode);
					final Transaction oldTr = inst.tr;
					Future<Void> f = oldTr.onError(err)
						.map(new Function<Transaction, Void>() {
							@Override
							public Void apply(final Transaction tr) {
								// Replace the context's transaction if it is still using the old one
								inst.context.updateCurrentTransaction(oldTr, tr);
								return null;
							}
						})
						.rescueRuntime(new Function<RuntimeException, Future<Void>>() {
							@Override
							public Future<Void> apply(RuntimeException ex) {
								// Create a new transaction for the context if it is still using the old one
								inst.context.newTransaction(oldTr);
								throw ex;
							}
						});

					if(filteredError) {
						f.get();
						throw new FDBException("Fake testing error", errorCode);
					}

					inst.push(f);
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.SUB) {
			return inst.popParams(2).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(List<Object> params) {
					BigInteger result = StackUtils.getBigInteger(params.get(0)).subtract(
							StackUtils.getBigInteger(params.get(1))
					);
					inst.push(result);
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.CONCAT) {
			return inst.popParams(2).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(List<Object> params) {
					if(params.get(0) instanceof String) {
						inst.push((String)params.get(0) + (String)params.get(1));
					}
					else {
						inst.push(ByteArrayUtil.join((byte[])params.get(0), (byte[])params.get(1)));
					}

					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.TUPLE_PACK) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					int tupleSize = StackUtils.getInt(param);
					//System.out.println(inst.context.preStr + " - " + "Packing top " + tupleSize + " items from stack");
					return inst.popParams(tupleSize).flatMap(new Function<List<Object>, Future<Void>>() {
						@Override
						public Future<Void> apply(List<Object> elements) {
							byte[] coded = Tuple.fromItems(elements).pack();
							//System.out.println(inst.context.preStr + " - " + " -> result '" + ByteArrayUtil.printable(coded) + "'");
							inst.push(coded);
							return ReadyFuture.DONE;
						}
					});
				}
			});
		}
		else if(op == StackOperation.TUPLE_PACK_WITH_VERSIONSTAMP) {
			return inst.popParams(2).flatMap(new Function<List<Object>, Future<Void>>() {
				@Override
				public Future<Void> apply(List<Object> params) {
					final byte[] prefix = (byte[])params.get(0);
					int tupleSize = StackUtils.getInt(params.get(1));
					//System.out.println(inst.context.preStr + " - " + "Packing top " + tupleSize + " items from stack");
					return inst.popParams(tupleSize).flatMap(new Function<List<Object>, Future<Void>>() {
						@Override
						public Future<Void> apply(List<Object> elements) {
							Tuple tuple = Tuple.fromItems(elements);
							if(!tuple.hasIncompleteVersionstamp() && Math.random() < 0.5) {
								inst.push("ERROR: NONE".getBytes());
								return ReadyFuture.DONE;
							}
							try {
								byte[] coded = tuple.packWithVersionstamp(prefix);
								//System.out.println(inst.context.preStr + " - " + " -> result '" + ByteArrayUtil.printable(coded) + "'");
								inst.push("OK".getBytes());
								inst.push(coded);
							} catch(IllegalArgumentException e) {
								//System.out.println(inst.context.preStr + " - " + " -> result '" + e.getMessage() + "'");
								if(e.getMessage().startsWith("No incomplete"))
									inst.push("ERROR: NONE".getBytes());
								else if(e.getMessage().startsWith("Multiple incomplete"))
									inst.push("ERROR: MULTIPLE".getBytes());
								else
									throw e;
							}
							return ReadyFuture.DONE;
						}
					});
				}
			});
		}
		else if(op == StackOperation.TUPLE_UNPACK) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					/*System.out.println(inst.context.preStr + " - " + "Unpacking tuple code: " +
							ByteArrayUtil.printable((byte[]) param)); */
					Tuple t = Tuple.fromBytes((byte[])param);
					for(Object o : t.getItems()) {
						byte[] itemBytes = Tuple.from(o).pack();
						inst.push(itemBytes);
					}
					return ReadyFuture.DONE;
				}
			});
		}
		else if(op == StackOperation.TUPLE_SORT) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					final int listSize = StackUtils.getInt(param);
					return inst.popParams(listSize).flatMap(new Function<List<Object>, Future<Void>>() {
						@Override
						public Future<Void> apply(List<Object> rawElements) {
							List<Tuple> tuples = new ArrayList(listSize);
							for(Object o : rawElements) {
								tuples.add(Tuple.fromBytes((byte[])o));
							}
							Collections.sort(tuples);
							for(Tuple t : tuples) {
								inst.push(t.pack());
							}
							return ReadyFuture.DONE;
						}
					});
				}
			});
		}
		else if(op == StackOperation.TUPLE_RANGE) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					int tupleSize = StackUtils.getInt(param);
					//System.out.println(inst.context.preStr + " - " + "Tuple range with top " + tupleSize + " items from stack");
					return inst.popParams(tupleSize).flatMap(new Function<List<Object>, Future<Void>>() {
						@Override
						public Future<Void> apply(List<Object> elements) {
							Range range = Tuple.fromItems(elements).range();
							inst.push(range.begin);
							inst.push(range.end);
							return ReadyFuture.DONE;
						}
					});
				}
			});
		}
		else if (op == StackOperation.ENCODE_FLOAT) {
			return inst.popParam().map(new Function<Object, Void>() {
				@Override
				public Void apply(Object param) {
					byte[] fBytes = (byte[])param;
					float value = ByteBuffer.wrap(fBytes).order(ByteOrder.BIG_ENDIAN).getFloat();
					inst.push(value);
					return null;
				}
			});
		}
		else if (op == StackOperation.ENCODE_DOUBLE) {
			return inst.popParam().map(new Function<Object, Void>() {
				@Override
				public Void apply(Object param) {
					byte[] dBytes = (byte[])param;
					double value = ByteBuffer.wrap(dBytes).order(ByteOrder.BIG_ENDIAN).getDouble();
					inst.push(value);
					return null;
				}
			});
		}
		else if (op == StackOperation.DECODE_FLOAT) {
			return inst.popParam().map(new Function<Object, Void>() {
				@Override
				public Void apply(Object param) {
					float value = ((Number)param).floatValue();
					inst.push(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(value).array());
					return null;
				}
			});
		}
		else if (op == StackOperation.DECODE_DOUBLE) {
			return inst.popParam().map(new Function<Object, Void>() {
				@Override
				public Void apply(Object param) {
					double value = ((Number)param).doubleValue();
					inst.push(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(value).array());
					return null;
				}
			});
		}
		else if(op == StackOperation.UNIT_TESTS) {
			inst.context.db.options().setLocationCacheSize(100001);

			return inst.context.db.runAsync(new Function<Transaction, Future<Void>>() {
				@Override
				public Future<Void> apply(Transaction tr) {
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

					byte[] test = {(byte)0xff};
					return tr.get(test).map(new Function<byte[], Void>() {
						@Override
						public Void apply(byte[] val) {
							return null;
						}
					});
				}
			}).rescue(new Function<Exception, Future<Void>>() {
				@Override
				public Future<Void> apply(Exception t) {
					throw new RuntimeException("Unit tests failed: " + t.getMessage());
				}
			});
		}
		else if(op == StackOperation.LOG_STACK) {
			return inst.popParam().flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object param) {
					final byte[] prefix = (byte[])param;
					return doLogStack(inst, prefix);

				}
			});
		}

		throw new IllegalArgumentException("Unrecognized (or unimplemented) operation");
	}

	private static Future<Void> executeMutation(final Instruction inst, Function<Transaction, Future<Void>> r) {
		// run this with a retry loop
		return inst.tcx.runAsync(r).map(new Function<Void, Void>() {
			@Override
			public Void apply(Void a) {
				if(inst.isDatabase)
					inst.push("RESULT_NOT_PRESENT".getBytes());
				return null;
			}
		});
	}

	private static Future<byte[]> executeGetKey(final Future<byte[]> keyFuture, final byte[] prefixFilter) {
		return keyFuture.map(new Function<byte[], byte[]>() {
			@Override
			public byte[] apply(byte[] key) {
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
		});
	}

	private static Future<Void> doLogStack(final Instruction inst, final byte[] prefix) {
		Map<Integer, StackEntry> entries = new HashMap<Integer, StackEntry>();
		while(inst.size() > 0) {
			entries.put(inst.size() - 1, inst.pop());
			if(entries.size() == 100) {
				return logStack(inst.context.db, entries, prefix).flatMap(new Function<Void, Future<Void>>() {
					@Override
					public Future<Void> apply(Void v) {
						return doLogStack(inst, prefix);
					}
				});
			}
		}

		return logStack(inst.context.db, entries, prefix);
	}

	private static Future<Void> logStack(final Database db, final Map<Integer, StackEntry> entries, final byte[] prefix) {
		return db.runAsync(new Function<Transaction, Future<Void>>() {
			@Override
			public Future<Void> apply(Transaction tr) {
				for(Map.Entry<Integer, StackEntry> it : entries.entrySet()) {
					byte[] pk = Tuple.from(it.getKey(), it.getValue().idx).pack(prefix);
					byte[] pv = Tuple.from(StackUtils.serializeFuture(it.getValue().value)).pack();
					tr.set(pk, pv.length < 40000 ? pv : Arrays.copyOfRange(pv, 0, 40000));
				}

				return ReadyFuture.DONE;
			}
		});
	}

	private static Future<Void> pushRange(Instruction inst, Future<List<KeyValue>> range) {
		return pushRange(inst, range, null);
	}

	private static Future<Void> pushRange(Instruction inst, Future<List<KeyValue>> range, byte[] prefixFilter) {
		//System.out.println("Waiting on range data to push...");
		return range.map(new ListPusher(inst, prefixFilter));
	}

	/**
	 * Pushes the result of a range query onto the stack as a {@code Tuple}
	 */
	private static class ListPusher implements Function<List<KeyValue>, Void> {
		final Instruction inst;
		final byte[] prefixFilter; 

		ListPusher(Instruction inst, byte[] prefixFilter) {
			this.inst = inst;
			this.prefixFilter = prefixFilter;
		}
		@Override
		public Void apply(List<KeyValue> list) {
			List<byte[]> o = new LinkedList<byte[]>();
			for(KeyValue kv : list) {
				if(prefixFilter == null || ByteArrayUtil.startsWith(kv.getKey(), prefixFilter)) {
					o.add(kv.getKey());
					o.add(kv.getValue());
				}
			}
			//System.out.println("Added " + o.size() / 2 + " pairs to stack/tuple");
			inst.push(Tuple.fromItems(o).pack());
			return null;
		}
	}

	static class AsynchronousContext extends Context {
		List<KeyValue> operations = null;
		int currentOp = 0;

		AsyncDirectoryExtension directoryExtension = new AsyncDirectoryExtension();

		AsynchronousContext(Database db, byte[] prefix) {
			super(db, prefix);
		}

		@Override
		Context createContext(byte[] prefix) {
			return new AsynchronousContext(this.db, prefix);
		}

		Future<Void> processOp(byte[] operation) {
			Tuple tokens = Tuple.fromBytes(operation);
			final Instruction inst = new Instruction(this, tokens);

			/*if(!inst.op.equals("PUSH") && !inst.op.equals("SWAP")) {
				System.out.println(inst.context.preStr + "\t- " + Thread.currentThread().getName() +
				  "\t- OP (" + inst.context.instructionIndex + "):" + inst.op);
			}*/

			if(inst.op.startsWith(DIRECTORY_PREFIX))
				return directoryExtension.processInstruction(inst);
			else {
				return processInstruction(inst)
				.rescueRuntime(new Function<RuntimeException, Future<Void>>() {
					@Override
					public Future<Void> apply(RuntimeException e) {
						if(e instanceof FDBException) {
							StackUtils.pushError(inst, (FDBException)e);
							return ReadyFuture.DONE;
						}
						else if(e instanceof IllegalStateException && e.getMessage().equals("Future not ready")) {
							StackUtils.pushError(inst, new FDBException("", 2015));
							return ReadyFuture.DONE;
						}
						else
							return new ReadyFuture<Void>(e);
					}
				});
			}
		}

		@Override void executeOperations() throws Throwable {
			executeRemainingOperations().get();
		}

		Future<Void> executeRemainingOperations() {
			final Function<Void, Future<Void>> processNext = new Function<Void, Future<Void>>() {
				@Override
				public Future<Void> apply(Void ignore) {
					instructionIndex++;
					return executeRemainingOperations();
				}
			};

			if(operations == null || ++currentOp == operations.size()) {
				return db.runAsync(new Function<Transaction, Future<List<KeyValue>>>() {
					@Override
					public Future<List<KeyValue>> apply(Transaction tr) {
						return tr.getRange(nextKey, endKey, 1000).asList();
					}
				})
				.flatMap(new Function<List<KeyValue>, Future<Void>>() {
					@Override
					public Future<Void> apply(List<KeyValue> next) {
						if(next.size() < 1) {
							//System.out.println("No key found after: " + ByteArrayUtil.printable(nextKey.getKey()));
							return ReadyFuture.DONE;
						}

						operations = next;
						currentOp = 0;
						nextKey = KeySelector.firstGreaterThan(next.get(next.size()-1).getKey());

						return processOp(next.get(0).getValue()).flatMap(processNext);
					}
				});
			}

			return processOp(operations.get(currentOp).getValue()).flatMap(processNext);
		}
	}

	static Future<StackEntry> popAndWait(Stack stack) {
		StackEntry entry = stack.pop();
		Object item = entry.value;
		if(!(item instanceof Future)) {
			return new ReadyFuture<StackEntry>(entry);
		}
		final int idx = entry.idx;

		@SuppressWarnings("unchecked")
		final Future<Object> future = (Future<Object>)item;
		Future<Object> flattened = flatten(future);

		return flattened.map(new Function<Object, StackEntry>() {
			@Override
			public StackEntry apply(Object o) {
				return new StackEntry(idx, o);
			}
		});
	}

	private static Future<Object> flatten(final Future<Object> future) {
		return future.map(new Function<Object, Object>() {
			@Override
			public Object apply(Object o) {
				if(o == null)
					return "RESULT_NOT_PRESENT".getBytes();
				return o;
			}
		}).rescue(new Function<Exception, Future<Object>>() {
			@Override
			public Future<Object> apply(Exception t) {
				if(t instanceof FDBException) {
					return new ReadyFuture<Object>(StackUtils.getErrorBytes((FDBException)t));
				}
				else if(t instanceof IllegalStateException && t.getMessage().equals("Future not ready")) {
					return new ReadyFuture<Object>(StackUtils.getErrorBytes(new FDBException("", 2015)));
				}
				return new ReadyFuture<Object>(t);
			}
		});
	}


	/**
	 * Run a stack-machine based test.
	 */
	public static void main(String[] args) {
		if(args.length < 1)
			throw new IllegalArgumentException("StackTester needs parameters <prefix> <optional_cluster_file>");

		//System.out.println("Prefix: " + args[0]);

		byte[] prefix = args[0].getBytes();

		FDB fdb = FDB.selectAPIVersion(Integer.parseInt(args[1]));
		//ExecutorService executor = Executors.newFixedThreadPool(2);
		Cluster cl = fdb.createCluster(args.length > 2 ? args[2] : null);

		Database db = cl.openDatabase();

		Context c = new AsynchronousContext(db, prefix);
		//System.out.println("Starting test...");
		c.run();
		//System.out.println("Done with test.");

		/*byte[] key = Tuple.from("test_results".getBytes(), 5).pack();
		byte[] bs = db.createTransaction().get(key).get();
		System.out.println("output of " + ByteArrayUtil.printable(key) + " as: " + ByteArrayUtil.printable(bs));*/

		/*fdb.stopNetwork();
		executor.shutdown();*/
	}

}
