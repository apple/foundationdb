/*
 * Context.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Tenant;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

abstract class Context implements Runnable, AutoCloseable {
	final Stack stack = new Stack();
	final Database db;
	Optional<Tenant> tenant = Optional.empty();
	final String preStr;
	int instructionIndex = 0;
	KeySelector nextKey, endKey;
	Long lastVersion = null;

	private static class TransactionState {
		public Transaction transaction;
		public Optional<Tenant> tenant;

		public TransactionState(Transaction transaction, Optional<Tenant> tenant) {
			this.transaction = transaction;
			this.tenant = tenant;
		}
	}

	private String trName;
	private List<Thread> children = new LinkedList<>();
	private static Map<String, TransactionState> transactionMap = new HashMap<>();
	private static Map<Transaction, AtomicInteger> transactionRefCounts = new HashMap<>();
	private static Map<byte[], Tenant> tenantMap = new ConcurrentHashMap<>();

	Context(Database db, byte[] prefix) {
		this.db = db;
		Range r = Tuple.from(prefix).range();
		this.nextKey = KeySelector.firstGreaterOrEqual(r.begin);
		this.endKey = KeySelector.firstGreaterOrEqual(r.end);

		this.trName = ByteArrayUtil.printable(prefix);
		this.preStr = ByteArrayUtil.printable(prefix);

		newTransaction();
	}

	@Override
	public void run() {
		try {
			executeOperations();
		} catch(Throwable t) {
			t.printStackTrace();
			System.exit(1);
		}
		while(children.size() > 0) {
			//System.out.println("Shutting down...waiting on " + children.size() + " threads");
			final Thread t = children.get(0);
			while(t.isAlive()) {
				try {
					t.join();
				} catch (InterruptedException e) {
					// EAT
				}
			}
			children.remove(0);
		}
	}

	public synchronized CompletableFuture<Long> setTenant(Optional<byte[]> tenantName) {
		if (tenantName.isPresent()) {
			tenant = Optional.of(tenantMap.computeIfAbsent(tenantName.get(), tn -> db.openTenant(tenantName.get())));
			return tenant.get().getId();
		}
		else {
			tenant = Optional.empty();
			return CompletableFuture.completedFuture(-1L);
		}
	}

	public static synchronized void addTransactionReference(Transaction tr) {
		transactionRefCounts.computeIfAbsent(tr, x -> new AtomicInteger(0)).incrementAndGet();
	}

	private static synchronized Transaction getTransaction(String trName) {
		TransactionState state = transactionMap.get(trName);
		assert state != null : "Null transaction";
		addTransactionReference(state.transaction);
		return state.transaction;
	}

	public Transaction getCurrentTransaction() {
		return getTransaction(trName);
	}

	public static synchronized void releaseTransaction(Transaction tr) {
		if(tr != null) {
			AtomicInteger count = transactionRefCounts.get(tr);
			if(count.decrementAndGet() == 0) {
				transactionRefCounts.remove(tr);
				tr.close();
			}
		}
	}

	private static Transaction createTransaction(Database db, Optional<Tenant> creatingTenant) {
		if (creatingTenant.isPresent()) {
			return creatingTenant.get().createTransaction();
		}
		else {
			return db.createTransaction();
		}
	}

	private static synchronized boolean newTransaction(Database db, Optional<Tenant> tenant, String trName, boolean allowReplace) {
		TransactionState oldState = transactionMap.get(trName);
		if (oldState != null) {
			if (allowReplace) {
				releaseTransaction(oldState.transaction);
			} else {
				return false;
			}
		}

		TransactionState newState = new TransactionState(createTransaction(db, tenant), tenant);

		transactionMap.put(trName, newState);
		addTransactionReference(newState.transaction);

		return true;
	}

	private static synchronized boolean replaceTransaction(Database db, String trName, Transaction oldTr, Transaction newTr) {
		TransactionState trState = transactionMap.get(trName);
		assert trState != null : "Null transaction";

		if(oldTr == null || trState.transaction == oldTr) {
			if(newTr == null) {
				newTr = createTransaction(db, trState.tenant);
			}
			releaseTransaction(trState.transaction);
			addTransactionReference(newTr);
			trState.transaction = newTr;
			return true;
		}

		return false;
	}

	public void newTransaction() {
		newTransaction(db, tenant, trName, true);
	}

	public void replaceTransaction(Transaction tr) {
		replaceTransaction(db, trName, null, tr);
	}

	public boolean replaceTransaction(Transaction oldTr, Transaction newTr) {
		return replaceTransaction(db, trName, oldTr, newTr);
	}

	public void resetTransaction() {
		replaceTransaction(db, trName, null, null);
	}

	public boolean resetTransaction(Transaction oldTr) {
		return replaceTransaction(db, trName, oldTr, null);
	}

	public void switchTransaction(byte[] rawTrName) {
		trName = ByteArrayUtil.printable(rawTrName);
		newTransaction(db, tenant, trName, false);
	}

	abstract void executeOperations() throws Throwable;
	abstract Context createContext(byte[] prefix);

	void addContext(byte[] prefix) {
		Thread t = new Thread(createContext(prefix));
		t.start();
		children.add(t);
	}

	StreamingMode streamingModeFromCode(int code) {
		for(StreamingMode x : StreamingMode.values()) {
			if(x.code() == code) {
				return x;
			}
		}
		throw new IllegalArgumentException("Invalid code: " + code);
	}

	private void popParams(int num, final List<Object> params, final CompletableFuture<Void> done) {
		while(num-- > 0) {
			Object item = stack.pop().value;
			if(item instanceof CompletableFuture) {
				final CompletableFuture<?> future = (CompletableFuture<?>)item;
				final int nextNum = num;
				future.whenCompleteAsync((o, t) -> {
					if(t != null) {
						FDBException root = StackUtils.getRootFDBException(t);
						if(root != null) {
							params.add(StackUtils.getErrorBytes(root));
							popParams(nextNum, params, done);
						}
						else {
							done.completeExceptionally(t);
						}
					}
					else {
						if(o == null)
							params.add("RESULT_NOT_PRESENT".getBytes());
						else
							params.add(o);

						popParams(nextNum, params, done);
					}
				}, FDB.DEFAULT_EXECUTOR);

				return;
			}
			else
				params.add(item);
		}

		done.complete(null);
	}

	CompletableFuture<List<Object>> popParams(int num) {
		final List<Object> params = new ArrayList<>(num);
		CompletableFuture<Void> done = new CompletableFuture<>();
		popParams(num, params, done);

		return done.thenApply(x -> params);
	}

	@Override
	public void close() {
		for(TransactionState tr : transactionMap.values()) {
			tr.transaction.close();
		}

		for(Tenant tenant : tenantMap.values()) {
			tenant.close();
		}
	}
}
