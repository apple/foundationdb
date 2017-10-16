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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.Function;
import com.apple.foundationdb.async.Future;
import com.apple.foundationdb.async.Settable;
import com.apple.foundationdb.async.SettableFuture;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

abstract class Context implements Runnable {
	final Stack stack = new Stack();
	final Database db;
	final String preStr;
	int instructionIndex = 0;
	String trName;
	KeySelector nextKey, endKey;
	Long lastVersion = null;
	List<Thread> children = new LinkedList<Thread>();

	static Map<String, Transaction> transactionMap = new HashMap<String, Transaction>();

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

	public Transaction getCurrentTransaction() {
		synchronized(Context.transactionMap) {
			return Context.transactionMap.get(this.trName);
		}
	}

	public void updateCurrentTransaction(Transaction tr) {
		synchronized(Context.transactionMap) {
			Context.transactionMap.put(this.trName, tr);
		}
	}

	public Transaction newTransaction() {
		Transaction tr = db.createTransaction();

		synchronized(Context.transactionMap) {
			Context.transactionMap.put(this.trName, tr);
		}

		return tr;
	}

	public void switchTransaction(byte[] trName) {
		synchronized(Context.transactionMap) {
			this.trName = ByteArrayUtil.printable(trName);
			if(!Context.transactionMap.containsKey(this.trName)) {
				newTransaction();
			}
		}
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

	void popParams(int num, final List<Object> params, final Settable<Void> done) {
		while(num-- > 0) {
			Object item = stack.pop().value;
			if(item instanceof Future) {
				@SuppressWarnings("unchecked")
				final Future<Object> future = (Future<Object>)item;
				final int nextNum = num;
				future.onReady(new Runnable() {
					@Override
					public void run() {
						try {
							Object o = future.get();
							if(o == null)
								params.add("RESULT_NOT_PRESENT".getBytes());
							else
								params.add(o);

							popParams(nextNum, params, done);
						} catch(FDBException e) {
							params.add(StackUtils.getErrorBytes(e));
							popParams(nextNum, params, done);
						} catch(IllegalStateException e) {
							if(e.getMessage().equals("Future not ready")) {
								params.add(StackUtils.getErrorBytes(new FDBException("", 2015)));
								popParams(nextNum, params, done);
							}
							else {
								done.setError(e);
							}
						} catch(RuntimeException e) {
							done.setError(e);
						} catch(Error e) {
							done.setError(e);
							throw e;
						}
					}
				});

				return;
			}
			else
				params.add(item);
		}

		done.set(null);
	}

	Future<List<Object>> popParams(int num) {
		final List<Object> params = new LinkedList<Object>();
		SettableFuture<Void> done = new SettableFuture<Void>();
		popParams(num, params, done);

		return done.map(new Function<Void, List<Object>>() {
			@Override
			public List<Object> apply(Void n) {
				return params;
			}
		});
	}
}
