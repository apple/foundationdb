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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.tuple.Tuple;

import java.util.List;

class Instruction extends Stack {
	private final static String SUFFIX_SNAPSHOT = "_SNAPSHOT";
	private final static String SUFFIX_DATABASE = "_DATABASE";

	String op;
	Tuple tokens;
	Context context;
	boolean isDatabase;
	boolean isSnapshot;
	Transaction tr;
	ReadTransaction readTr;
	TransactionContext tcx;
	ReadTransactionContext readTcx;

	public Instruction(Context context, Tuple tokens) {
		this.context = context;
		this.tokens = tokens;

		op = tokens.getString(0);
		isDatabase = op.endsWith(SUFFIX_DATABASE);
		isSnapshot = op.endsWith(SUFFIX_SNAPSHOT);

		if(isDatabase) {
			this.tr = null;
			readTr = null;
			op = op.substring(0, op.length() - SUFFIX_DATABASE.length());
		}
		else if(isSnapshot) {
			this.tr = context.getCurrentTransaction();
			readTr = this.tr.snapshot();
			op = op.substring(0, op.length() - SUFFIX_SNAPSHOT.length());
		}
		else {
			this.tr = context.getCurrentTransaction();
			readTr = this.tr;
		}

		tcx = isDatabase ? context.db : this.tr;
		readTcx = isDatabase ? context.db : this.readTr;
	}

	void setTransaction(Transaction tr) {
		if(!isDatabase) {
			context.releaseTransaction(this.tr);
			context.updateCurrentTransaction(tr);

			this.tr = context.getCurrentTransaction();
			if(isSnapshot) {
				readTr = this.tr.snapshot();
			}
			else {
				readTr = tr;
			}
		}
	}

	void setTransaction(Transaction oldTr, Transaction newTr) {
		if(!isDatabase) {
			context.updateCurrentTransaction(oldTr, newTr);

			this.tr = context.getCurrentTransaction();
			if(isSnapshot) {
				readTr = this.tr.snapshot();
			}
			else {
				readTr = tr;
			}
		}
	}

	void releaseTransaction() {
		context.releaseTransaction(this.tr);
	}

	void push(Object o) {
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
