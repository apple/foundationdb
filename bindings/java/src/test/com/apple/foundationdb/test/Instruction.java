/*
 * Instruction.java
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

import java.util.concurrent.CompletableFuture;
import java.util.List;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.tuple.Tuple;


class Instruction extends Stack {
	private static final String SUFFIX_SNAPSHOT = "_SNAPSHOT";
	private static final String SUFFIX_DATABASE = "_DATABASE";

	final String op;
	final Tuple tokens;
	final Context context;
	final boolean isDatabase;
	final boolean isSnapshot;
	final Transaction tr;
	final ReadTransaction readTr;
	final TransactionContext tcx;
	final ReadTransactionContext readTcx;

	Instruction(Context context, Tuple tokens) {
		this.context = context;
		this.tokens = tokens;

		String fullOp = tokens.getString(0);
		isDatabase = fullOp.endsWith(SUFFIX_DATABASE);
		isSnapshot = fullOp.endsWith(SUFFIX_SNAPSHOT);

		if(isDatabase) {
			tr = null;
			readTr = null;
			op = fullOp.substring(0, fullOp.length() - SUFFIX_DATABASE.length());
		}
		else if(isSnapshot) {
			tr = context.getCurrentTransaction();
			readTr = tr.snapshot();
			op = fullOp.substring(0, fullOp.length() - SUFFIX_SNAPSHOT.length());
		}
		else {
			tr = context.getCurrentTransaction();
			readTr = tr;
			op = fullOp;
		}

		tcx = isDatabase ? context.db : tr;
		readTcx = isDatabase ? context.db : readTr;
	}

	void setTransaction(Transaction newTr) {
		if(!isDatabase) {
			context.updateCurrentTransaction(newTr);
		}
	}

	void setTransaction(Transaction oldTr, Transaction newTr) {
		if(!isDatabase) {
			context.updateCurrentTransaction(oldTr, newTr);
		}
	}

	void releaseTransaction() {
		Context.releaseTransaction(tr);
	}

	void push(Object o) {
		if(o instanceof CompletableFuture && tr != null) {
			CompletableFuture<?> future = (CompletableFuture<?>)o;
			Context.addTransactionReference(tr);
			future.whenComplete((x, t) -> Context.releaseTransaction(tr));
		}
		context.stack.push(context.instructionIndex, o);
	}

	void push(int idx, Object o) {
		context.stack.push(idx, o);
	}

	void push(StackEntry e) {
		context.stack.push(e);
	}

	StackEntry pop() { 
		return context.stack.pop();
	}

	void swap(int index) {
		context.stack.swap(index);
	}

	int size() {
		return context.stack.size();
	}

	void clear() {
		context.stack.clear();
	}

	CompletableFuture<List<Object>> popParams(int num) {
		return context.popParams(num);
	}

	CompletableFuture<Object> popParam() {
		return popParams(1)
		.thenApplyAsync((params) -> params.get(0));
	}
}
