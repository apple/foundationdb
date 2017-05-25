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

package com.apple.cie.foundationdb.test;

import com.apple.cie.foundationdb.ReadTransaction;
import com.apple.cie.foundationdb.ReadTransactionContext;
import com.apple.cie.foundationdb.Transaction;
import com.apple.cie.foundationdb.TransactionContext;
import com.apple.cie.foundationdb.async.Function;
import com.apple.cie.foundationdb.async.Future;
import com.apple.cie.foundationdb.tuple.Tuple;

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
			this.tr = context.db.createTransaction();
			readTr = this.tr;
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
		this.tr = tr;
		if(isSnapshot) {
			readTr = this.tr.snapshot();
		}
		else {
			readTr = tr;
		}

		if(!isDatabase) {
			context.updateCurrentTransaction(tr);
		}
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

	Future<List<Object>> popParams(int num) {
		return context.popParams(num);
	}

	Future<Object> popParam() {
		return popParams(1)
		.map(new Function<List<Object>, Object>() {
			public Object apply(List<Object> params) {
				return params.get(0);
			}
		});
	}
}
