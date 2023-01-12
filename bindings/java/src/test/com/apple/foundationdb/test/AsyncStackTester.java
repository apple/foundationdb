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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.KeyArrayResult;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.TenantManagement;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.async.CloseableAsyncIterator;

public class AsyncStackTester {
	static final String DIRECTORY_PREFIX = "DIRECTORY_";

	static class WaitEmpty implements Function<Transaction, CompletableFuture<Void>> {
		private final byte[] prefix;
		WaitEmpty(byte[] prefix) {
			this.prefix = prefix;
		}

		@Override
		public CompletableFuture<Void> apply(Transaction tr) {
			return tr.getRange(Range.startsWith(prefix)).asList().thenAcceptAsync(list -> {
				if(list.size() > 0) {
					//System.out.println(" - Throwing new fake commit error...");
					throw new FDBException("ERROR: Fake commit conflict", 1020);
				}
			}, FDB.DEFAULT_EXECUTOR);
		}
	}

	static CompletableFuture<Void> processInstruction(final Instruction inst) {
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
			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.POP) {
			inst.pop();
			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.DUP) {
			if(inst.size() == 0)
				throw new RuntimeException("No stack bro!! (" + inst.context.preStr + ")");
			StackEntry e = inst.pop();
			inst.push(e);
			inst.push(e);
			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.EMPTY_STACK) {
			inst.clear();
			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.SWAP) {
			return inst.popParam().thenAcceptAsync(param -> {
				int index = StackUtils.getInt(param);
				if(index >= inst.size())
					throw new IllegalArgumentException("Stack index not valid");

				inst.swap(index);
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.WAIT_FUTURE) {
			return popAndWait(inst).thenAccept(inst::push);
		}
		else if(op == StackOperation.WAIT_EMPTY) {
			return inst.popParam().thenComposeAsync(param -> {
				WaitEmpty retryable = new WaitEmpty((byte[])param);
				return inst.context.db.runAsync(retryable).thenRun(() -> inst.push("WAITED_FOR_EMPTY".getBytes()));
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.START_THREAD) {
			return inst.popParam().thenAcceptAsync(param -> {
				//System.out.println(inst.context.preStr + " - " + "Starting new thread at prefix: " + ByteArrayUtil.printable((byte[]) params.get(0)));
				inst.context.addContext((byte[])param);
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.NEW_TRANSACTION) {
			inst.context.newTransaction();
			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.USE_TRANSACTION) {
			return inst.popParam().thenAcceptAsync(param -> {
				inst.context.switchTransaction((byte[])param);
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.SET) {
			return inst.popParams(2).thenComposeAsync(params -> {
				/*System.out.println(inst.context.preStr + " - " + "Setting '" + ByteArrayUtil.printable((byte[]) params.get(0)) +
						"' to '" + ByteArrayUtil.printable((byte[]) params.get(1)) + "'"); */
				return executeMutation(inst, tr -> {
					tr.set((byte[])params.get(0), (byte[])params.get(1));
					return AsyncUtil.DONE;
				});
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.CLEAR) {
			return inst.popParam().thenComposeAsync(param -> {
				//System.out.println(inst.context.preStr + " - " + "Clearing: '" + ByteArrayUtil.printable((byte[])param) + "'");
				return executeMutation(inst, tr -> {
					tr.clear((byte[])param);
					return AsyncUtil.DONE;
				});
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.CLEAR_RANGE) {
			return inst.popParams(2).thenComposeAsync(params ->
				executeMutation(inst, tr -> {
					tr.clear((byte[])params.get(0), (byte[])params.get(1));
					return AsyncUtil.DONE;
				}),
				FDB.DEFAULT_EXECUTOR
			);
		}
		else if(op == StackOperation.CLEAR_RANGE_STARTS_WITH) {
			return inst.popParam().thenComposeAsync(param ->
				executeMutation(inst, tr -> {
					tr.clear(Range.startsWith((byte[])param));
					return AsyncUtil.DONE;
				}),
				FDB.DEFAULT_EXECUTOR
			);
		}
		else if(op == StackOperation.ATOMIC_OP) {
			return inst.popParams(3).thenComposeAsync(params -> {
				final MutationType optype = MutationType.valueOf((String)params.get(0));
				return executeMutation(inst, tr -> {
					tr.mutate(optype, (byte[])params.get(1), (byte[])params.get(2));
					return AsyncUtil.DONE;
				});
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.COMMIT) {
			inst.push(inst.tr.commit());
			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.RESET) {
			inst.context.resetTransaction();
			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.CANCEL) {
			inst.tr.cancel();
			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.READ_CONFLICT_RANGE) {
			return inst.popParams(2).thenAcceptAsync(params -> {
				inst.tr.addReadConflictRange((byte[])params.get(0), (byte[])params.get(1));
				inst.push("SET_CONFLICT_RANGE".getBytes());
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.WRITE_CONFLICT_RANGE) {
			return inst.popParams(2).thenAcceptAsync(params -> {
				inst.tr.addWriteConflictRange((byte[])params.get(0), (byte[])params.get(1));
				inst.push("SET_CONFLICT_RANGE".getBytes());
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.READ_CONFLICT_KEY) {
			return inst.popParam().thenAcceptAsync(param -> {
				inst.tr.addReadConflictKey((byte[])param);
				inst.push("SET_CONFLICT_KEY".getBytes());
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.WRITE_CONFLICT_KEY) {
			return inst.popParam().thenAcceptAsync(param -> {
				inst.tr.addWriteConflictKey((byte[])param);
				inst.push("SET_CONFLICT_KEY".getBytes());
			});
		}
		else if(op == StackOperation.DISABLE_WRITE_CONFLICT) {
			inst.tr.options().setNextWriteNoWriteConflictRange();
			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.GET) {
			return inst.popParam().thenAcceptAsync(param -> {
				inst.push(inst.readTcx.readAsync(readTr -> readTr.get((byte[]) param)));
			});
		}
		else if (op == StackOperation.GET_ESTIMATED_RANGE_SIZE) {
			List<Object> params = inst.popParams(2).join();
			return inst.readTr.getEstimatedRangeSizeBytes((byte[])params.get(0), (byte[])params.get(1)).thenAcceptAsync(size -> {
				inst.push("GOT_ESTIMATED_RANGE_SIZE".getBytes());
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if (op == StackOperation.GET_RANGE_SPLIT_POINTS) {
			List<Object> params = inst.popParams(3).join();
			return inst.readTr.getRangeSplitPoints((byte[])params.get(0), (byte[])params.get(1), (long)params.get(2)).thenAcceptAsync(splitPoints -> {
				inst.push("GOT_RANGE_SPLIT_POINTS".getBytes());
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.GET_RANGE) {
			return inst.popParams(5).thenComposeAsync(params -> {
				int limit = StackUtils.getInt(params.get(2));
				boolean reverse = StackUtils.getBoolean(params.get(3));
				StreamingMode mode = inst.context.streamingModeFromCode(
						StackUtils.getInt(params.get(4), StreamingMode.ITERATOR.code()));

				CompletableFuture<List<KeyValue>> range = inst.readTcx.readAsync(readTr -> readTr.getRange((byte[])params.get(0), (byte[])params.get(1), limit, reverse, mode).asList());
				return pushRange(inst, range);
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.GET_RANGE_SELECTOR) {
			return inst.popParams(10).thenComposeAsync(params -> {
				int limit = StackUtils.getInt(params.get(6));
				boolean reverse = StackUtils.getBoolean(params.get(7));
				StreamingMode mode = inst.context.streamingModeFromCode(
						StackUtils.getInt(params.get(8), StreamingMode.ITERATOR.code()));

				KeySelector start = StackUtils.createSelector(params.get(0),params.get(1), params.get(2));
				KeySelector end = StackUtils.createSelector(params.get(3), params.get(4), params.get(5));

				CompletableFuture<List<KeyValue>> range = inst.readTcx.readAsync(readTr -> readTr.getRange(start, end, limit, reverse, mode).asList());
				return pushRange(inst, range, (byte[])params.get(9));
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.GET_RANGE_STARTS_WITH) {
			return inst.popParams(4).thenComposeAsync(params -> {
				int limit = StackUtils.getInt(params.get(1));
				boolean reverse = StackUtils.getBoolean(params.get(2));
				StreamingMode mode = inst.context.streamingModeFromCode(
						StackUtils.getInt(params.get(3), StreamingMode.ITERATOR.code()));

				CompletableFuture<List<KeyValue>> range = inst.readTcx.readAsync(readTr -> readTr.getRange(Range.startsWith((byte[])params.get(0)), limit, reverse, mode).asList());
				return pushRange(inst, range);
			});
		}
		else if(op == StackOperation.GET_KEY) {
			return inst.popParams(4).thenAcceptAsync(params -> {
				KeySelector start = StackUtils.createSelector(params.get(0),params.get(1), params.get(2));
				inst.push(inst.readTcx.readAsync(readTr -> executeGetKey(readTr.getKey(start), (byte[])params.get(3))));
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.GET_READ_VERSION) {
			return inst.readTr.getReadVersion().thenAcceptAsync(readVersion -> {
				inst.context.lastVersion = readVersion;
				inst.push("GOT_READ_VERSION".getBytes());
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.GET_COMMITTED_VERSION) {
			try {
				inst.context.lastVersion = inst.tr.getCommittedVersion();
				inst.push("GOT_COMMITTED_VERSION".getBytes());
			}
			catch(FDBException e) {
				StackUtils.pushError(inst, e);
			}

			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.GET_APPROXIMATE_SIZE) {
			return inst.tr.getApproximateSize().thenAcceptAsync(size -> {
				inst.push("GOT_APPROXIMATE_SIZE".getBytes());
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.GET_VERSIONSTAMP) {
			try {
				inst.push(inst.tr.getVersionstamp());
			}
			catch(FDBException e) {
				StackUtils.pushError(inst, e);
			}

			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.SET_READ_VERSION) {
			if(inst.context.lastVersion == null)
				throw new IllegalArgumentException("Read version has not been read");
			inst.tr.setReadVersion(inst.context.lastVersion);
			return AsyncUtil.DONE;
		}
		else if(op == StackOperation.ON_ERROR) {
			return inst.popParam().thenComposeAsync(param -> {
				int errorCode = StackUtils.getInt(param);

				// 1102 (future_released) and 2015 (future_not_set) are not errors to Java.
				//  This is never encountered by user code, so we have to do something rather
				//  messy here to get compatibility with other languages.
				//
				// First, try on error with a retryable error. If it fails, then the transaction is in
				//  a failed state and we should rethrow the error. Otherwise, throw the original error.
				boolean filteredError = errorCode == 1102 || errorCode == 2015;

				FDBException err = new FDBException("Fake testing error", filteredError ? 1020 : errorCode);
				final Transaction oldTr = inst.tr;
				CompletableFuture<Void> f = oldTr.onError(err).whenComplete((tr, t) -> {
					if(t != null) {
						inst.context.resetTransaction(oldTr); // Other bindings allow reuse of non-retryable transactions, so we need to emulate that behavior.
					}
					else if(!inst.replaceTransaction(oldTr, tr)) {
						tr.close();
					}
				}).thenApply(v -> null);

				if(filteredError) {
					f.join();
					throw new FDBException("Fake testing error", errorCode);
				}

				inst.push(f);
				return AsyncUtil.DONE;
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.SUB) {
			return inst.popParams(2).thenAcceptAsync(params -> {
				BigInteger result = StackUtils.getBigInteger(params.get(0)).subtract(
						StackUtils.getBigInteger(params.get(1))
				);
				inst.push(result);
			});
		}
		else if(op == StackOperation.CONCAT) {
			return inst.popParams(2).thenAcceptAsync(params -> {
				if(params.get(0) instanceof String) {
					inst.push((String)params.get(0) + (String)params.get(1));
				}
				else {
					inst.push(ByteArrayUtil.join((byte[])params.get(0), (byte[])params.get(1)));
				}
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.TUPLE_PACK) {
			return inst.popParam().thenComposeAsync(param -> {
				int tupleSize = StackUtils.getInt(param);
				//System.out.println(inst.context.preStr + " - " + "Packing top " + tupleSize + " items from stack");
				return inst.popParams(tupleSize).thenAcceptAsync(elements -> {
					byte[] coded = Tuple.fromItems(elements).pack();
					//System.out.println(inst.context.preStr + " - " + " -> result '" + ByteArrayUtil.printable(coded) + "'");
					inst.push(coded);
				}, FDB.DEFAULT_EXECUTOR);
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.TUPLE_PACK_WITH_VERSIONSTAMP) {
			return inst.popParams(2).thenComposeAsync(params -> {
				byte[] prefix = (byte[])params.get(0);
				int tupleSize = StackUtils.getInt(params.get(1));
				//System.out.println(inst.context.preStr + " - " + "Packing top " + tupleSize + " items from stack");
				return inst.popParams(tupleSize).thenAcceptAsync(elements -> {
					Tuple tuple = Tuple.fromItems(elements);
					if(!tuple.hasIncompleteVersionstamp() && Math.random() < 0.5) {
						inst.push("ERROR: NONE".getBytes());
						return;
					}
					try {
						byte[] coded = tuple.packWithVersionstamp(prefix);
						inst.push("OK".getBytes());
						inst.push(coded);
					} catch(IllegalArgumentException e) {
						if(e.getMessage().startsWith("No incomplete")) {
							inst.push("ERROR: NONE".getBytes());
						} else {
							inst.push("ERROR: MULTIPLE".getBytes());
						}
					}
				}, FDB.DEFAULT_EXECUTOR);
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.TUPLE_UNPACK) {
			return inst.popParam().thenAcceptAsync(param -> {
				/*System.out.println(inst.context.preStr + " - " + "Unpacking tuple code: " +
						ByteArrayUtil.printable((byte[]) param)); */
				Tuple t = Tuple.fromBytes((byte[])param);
				for(Object o : t.getItems()) {
					byte[] itemBytes = Tuple.from(o).pack();
					inst.push(itemBytes);
				}
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.TUPLE_RANGE) {
			return inst.popParam().thenComposeAsync(param -> {
				int tupleSize = StackUtils.getInt(param);
				//System.out.println(inst.context.preStr + " - " + "Tuple range with top " + tupleSize + " items from stack");
				return inst.popParams(tupleSize).thenAcceptAsync(elements -> {
					Range range = Tuple.fromItems(elements).range();
					inst.push(range.begin);
					inst.push(range.end);
				}, FDB.DEFAULT_EXECUTOR);
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if(op == StackOperation.TUPLE_SORT) {
			return inst.popParam().thenComposeAsync(param -> {
				final int listSize = StackUtils.getInt(param);
				return inst.popParams(listSize).thenAcceptAsync(rawElements -> {
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
				}, FDB.DEFAULT_EXECUTOR);
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if (op == StackOperation.ENCODE_FLOAT) {
			return inst.popParam().thenAcceptAsync(param -> {
				byte[] fBytes = (byte[])param;
				float value = ByteBuffer.wrap(fBytes).order(ByteOrder.BIG_ENDIAN).getFloat();
				inst.push(value);
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if (op == StackOperation.ENCODE_DOUBLE) {
			return inst.popParam().thenAcceptAsync(param -> {
				byte[] dBytes = (byte[])param;
				double value = ByteBuffer.wrap(dBytes).order(ByteOrder.BIG_ENDIAN).getDouble();
				inst.push(value);
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if (op == StackOperation.DECODE_FLOAT) {
			return inst.popParam().thenAcceptAsync(param -> {
				float value = ((Number)param).floatValue();
				inst.push(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putFloat(value).array());
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if (op == StackOperation.DECODE_DOUBLE) {
			return inst.popParam().thenAcceptAsync(param -> {
				double value = ((Number)param).doubleValue();
				inst.push(ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(value).array());
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if (op == StackOperation.TENANT_CREATE) {
			return inst.popParam().thenAcceptAsync(param -> {
				byte[] tenantName = (byte[])param;
				inst.push(TenantManagement.createTenant(inst.context.db, tenantName));
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if (op == StackOperation.TENANT_DELETE) {
			return inst.popParam().thenAcceptAsync(param -> {
				byte[] tenantName = (byte[])param;
				inst.push(TenantManagement.deleteTenant(inst.context.db, tenantName));
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if (op == StackOperation.TENANT_LIST) {
			return inst.popParams(3).thenAcceptAsync(params -> {
				byte[] begin = (byte[])params.get(0);
				byte[] end = (byte[])params.get(1);
				int limit = StackUtils.getInt(params.get(2));
				CloseableAsyncIterator<KeyValue> tenantIter = TenantManagement.listTenants(inst.context.db, begin, end, limit);
				List<byte[]> result = new ArrayList();
				try {
					while (tenantIter.hasNext()) {
						KeyValue next = tenantIter.next();
						String metadata = new String(next.getValue());
						assert StackUtils.validTenantMetadata(metadata) : "Invalid Tenant Metadata";
						result.add(next.getKey());
					}
				} finally {
					tenantIter.close();
				}
				inst.push(Tuple.fromItems(result).pack());
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if (op == StackOperation.TENANT_SET_ACTIVE) {
			return inst.popParam().thenAcceptAsync(param -> {
				byte[] tenantName = (byte[])param;
				inst.context.setTenant(Optional.of(tenantName));
			}, FDB.DEFAULT_EXECUTOR);
		}
		else if (op == StackOperation.TENANT_CLEAR_ACTIVE) {
			inst.context.setTenant(Optional.empty());
			return AsyncUtil.DONE;
		}
		else if (op == StackOperation.TENANT_GET_ID) {
			if (inst.context.tenant.isPresent()) {
				return inst.context.tenant.get().getId().thenAcceptAsync(id -> {
					inst.push("GOT_TENANT_ID".getBytes());
				}, FDB.DEFAULT_EXECUTOR);
			} else {
				inst.push("NO_ACTIVE_TENANT".getBytes());
				return AsyncUtil.DONE;
			}
		}
		else if (op == StackOperation.UNIT_TESTS) {
			inst.context.db.options().setLocationCacheSize(100001);
			return inst.context.db.runAsync(tr -> {
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

				// Test network busyness
				double busyness = db.getMainThreadBusyness();
				if (busyness < 0) {
					throw new IllegalStateException("Network busyness cannot be less than 0");
				}

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

				byte[] test = {(byte)0xff};
				return tr.get(test).thenRunAsync(() -> {});
			}).exceptionally(t -> {
				throw new RuntimeException("Unit tests failed: " + t.getMessage());
			});
		}
		else if (op == StackOperation.LOG_STACK) {
			return inst.popParam().thenComposeAsync(prefix -> doLogStack(inst, (byte[])prefix), FDB.DEFAULT_EXECUTOR);
		}

		throw new IllegalArgumentException("Unrecognized (or unimplemented) operation");
	}

	private static CompletableFuture<Void> executeMutation(final Instruction inst, Function<Transaction, CompletableFuture<Void>> r) {
		// run this with a retry loop
		return inst.tcx.runAsync(r).thenRunAsync(() -> {
			if(inst.isDatabase || inst.isTenant)
				inst.push("RESULT_NOT_PRESENT".getBytes());
		}, FDB.DEFAULT_EXECUTOR);
	}

	private static CompletableFuture<byte[]> executeGetKey(final CompletableFuture<byte[]> keyFuture, final byte[] prefixFilter) {
		return keyFuture.thenApplyAsync(key -> {
			if(ByteArrayUtil.startsWith(key, prefixFilter)) {
				return key;
			}
			else if(ByteArrayUtil.compareUnsigned(key, prefixFilter) < 0) {
				return prefixFilter;
			}
			else {
				return ByteArrayUtil.strinc(prefixFilter);
			}
		}, FDB.DEFAULT_EXECUTOR);
	}

	private static CompletableFuture<Void> doLogStack(final Instruction inst, final byte[] prefix) {
		Map<Integer, StackEntry> entries = new HashMap<>();
		while(inst.size() > 0) {
			entries.put(inst.size() - 1, inst.pop());
			if(entries.size() == 100) {
				return logStack(inst.context.db, entries, prefix).thenComposeAsync(v -> doLogStack(inst, prefix), FDB.DEFAULT_EXECUTOR);
			}
		}

		return logStack(inst.context.db, entries, prefix);
	}

	private static CompletableFuture<Void> logStack(final Database db, final Map<Integer, StackEntry> entries, final byte[] prefix) {
		return db.runAsync(tr -> {
			for(Map.Entry<Integer, StackEntry> it : entries.entrySet()) {
				byte[] pk = Tuple.from(it.getKey(), it.getValue().idx).pack(prefix);
				byte[] pv = Tuple.from(StackUtils.serializeFuture(it.getValue().value)).pack();
				tr.set(pk, pv.length < 40000 ? pv : Arrays.copyOfRange(pv, 0, 40000));
			}

			return AsyncUtil.DONE;
		});
	}

	private static CompletableFuture<Void> pushRange(Instruction inst, CompletableFuture<List<KeyValue>> range) {
		return pushRange(inst, range, null);
	}

	private static CompletableFuture<Void> pushRange(Instruction inst, CompletableFuture<List<KeyValue>> range, byte[] prefixFilter) {
		//System.out.println("Waiting on range data to push...");
		return range.thenApplyAsync(new ListPusher(inst, prefixFilter));
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
			List<byte[]> o = new LinkedList<>();
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

		CompletableFuture<Void> processOp(byte[] operation) {
			Tuple tokens = Tuple.fromBytes(operation);
			final Instruction inst = new Instruction(this, tokens);

			/*if(!inst.op.equals("PUSH") && !inst.op.equals("SWAP")) {
				System.out.println(inst.context.preStr + "\t- " + Thread.currentThread().getName() +
				  "\t- OP (" + inst.context.instructionIndex + "):" + inst.op);
			}*/

			if(inst.op.startsWith(DIRECTORY_PREFIX))
				return directoryExtension.processInstruction(inst).whenComplete((x, t) -> inst.releaseTransaction());
			else {
				return AsyncUtil.composeExceptionally(processInstruction(inst), (e) -> {
					FDBException ex = StackUtils.getRootFDBException(e);
					if(ex != null) {
						StackUtils.pushError(inst, ex);
						return AsyncUtil.DONE;
					}
					else {
						CompletableFuture<Void> f = new CompletableFuture<>();
						f.completeExceptionally(e);
						return f;
					}
				})
				.whenComplete((x, t) -> inst.releaseTransaction());
			}
		}

		@Override void executeOperations() {
			executeRemainingOperations().join();
		}

		CompletableFuture<Void> executeRemainingOperations() {
			final Function<Void, CompletableFuture<Void>> processNext = ignore -> {
				instructionIndex++;
				return executeRemainingOperations();
			};

			if(operations == null || ++currentOp == operations.size()) {
				return db.readAsync(readTr -> readTr.getRange(nextKey, endKey, 1000).asList())
				.thenComposeAsync(next -> {
					if(next.size() < 1) {
						//System.out.println("No key found after: " + ByteArrayUtil.printable(nextKey.getKey()));
						return AsyncUtil.DONE;
					}

					operations = next;
					currentOp = 0;
					nextKey = KeySelector.firstGreaterThan(next.get(next.size()-1).getKey());

					return processOp(next.get(0).getValue()).thenComposeAsync(processNext);
				}, FDB.DEFAULT_EXECUTOR);
			}

			return processOp(operations.get(currentOp).getValue()).thenComposeAsync(processNext, FDB.DEFAULT_EXECUTOR);
		}
	}

	static CompletableFuture<StackEntry> popAndWait(Stack stack) {
		StackEntry entry = stack.pop();
		Object item = entry.value;
		if(!(item instanceof CompletableFuture)) {
			return CompletableFuture.completedFuture(entry);
		}
		final int idx = entry.idx;

		final CompletableFuture<?> future = (CompletableFuture<?>)item;
		CompletableFuture<Object> flattened = flatten(future);

		return flattened.thenApplyAsync(o -> new StackEntry(idx, o));
	}

	private static CompletableFuture<Object> flatten(final CompletableFuture<?> future) {
		CompletableFuture<Object> f = future.thenApply(o -> {
			if(o == null)
				return "RESULT_NOT_PRESENT".getBytes();
			return o;
		});

		return AsyncUtil.composeExceptionally(f, t -> {
			FDBException e = StackUtils.getRootFDBException(t);
			if(e != null) {
				return CompletableFuture.completedFuture(StackUtils.getErrorBytes(e));
			}

			CompletableFuture<Object> error = new CompletableFuture<>();
			error.completeExceptionally(t);
			return error;
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
		//ExecutorService executor = Executors.newFixedThreadPool(2);
		Database db = fdb.open(args.length > 2 ? args[2] : null);

		Context c = new AsynchronousContext(db, prefix);
		//System.out.println("Starting test...");
		c.run();
		//System.out.println("Done with test.");

		db.close();
		System.gc();

		/*fdb.stopNetwork();
		executor.shutdown();*/
	}

	private AsyncStackTester() {}
}
