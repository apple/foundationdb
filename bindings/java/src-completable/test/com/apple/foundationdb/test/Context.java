/*
 * Context.java
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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

abstract class Context implements Runnable {
	final Stack stack = new Stack();
	final Database db;
	final String preStr;
	int instructionIndex = 0;
	KeySelector nextKey, endKey;
	Long lastVersion = null;

	private String trName;
	private List<Thread> children = new LinkedList<>();
	static private Map<String, Transaction> transactionMap = new HashMap<>();
	static private Map<Transaction, AtomicInteger> transactionRefCounts = new HashMap<>();

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
			// EAT
			t.printStackTrace();
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

	public synchronized Transaction getCurrentTransaction() {
		Transaction tr = Context.transactionMap.get(this.trName);
		Context.transactionRefCounts.get(tr).incrementAndGet();
		return tr;
	}

	public synchronized void releaseTransaction(Transaction tr) {
		if(tr != null) {
			AtomicInteger count = Context.transactionRefCounts.get(tr);
			if(count.decrementAndGet() == 0) {
				Context.transactionRefCounts.remove(tr);
				tr.dispose();
			}
		}
	}

	public synchronized void updateCurrentTransaction(Transaction tr) {
		Context.transactionRefCounts.computeIfAbsent(tr, x -> new AtomicInteger(1));
		releaseTransaction(Context.transactionMap.put(this.trName, tr));
	}

	public synchronized boolean updateCurrentTransaction(Transaction oldTr, Transaction newTr) {
		if(Context.transactionMap.replace(this.trName, oldTr, newTr)) {
			AtomicInteger count = Context.transactionRefCounts.computeIfAbsent(newTr, x -> new AtomicInteger(0));
			count.incrementAndGet();
			releaseTransaction(oldTr);
			return true;
		}

		return false;
	}

	public void newTransaction() {
		Transaction tr = db.createTransaction();
		updateCurrentTransaction(tr);
	}

	public void newTransaction(Transaction oldTr) {
		Transaction newTr = db.createTransaction();
		if(!updateCurrentTransaction(oldTr, newTr)) {
			newTr.dispose();
		}
	}

	public synchronized void switchTransaction(byte[] trName) {
		this.trName = ByteArrayUtil.printable(trName);
		Transaction tr = Context.transactionMap.computeIfAbsent(this.trName, x -> db.createTransaction());
		Context.transactionRefCounts.computeIfAbsent(tr, x -> new AtomicInteger(1));
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
				@SuppressWarnings("unchecked")
				final CompletableFuture<Object> future = (CompletableFuture<Object>)item;
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
				});

				return;
			}
			else
				params.add(item);
		}

		done.complete(null);
	}

	CompletableFuture<List<Object>> popParams(int num) {
		final List<Object> params = new LinkedList<>();
		CompletableFuture<Void> done = new CompletableFuture<>();
		popParams(num, params, done);

		return done.thenApplyAsync((x) -> params);
	}

	void dispose() {
		for(Transaction tr : transactionMap.values()) {
			tr.dispose();
		}
	}
}
