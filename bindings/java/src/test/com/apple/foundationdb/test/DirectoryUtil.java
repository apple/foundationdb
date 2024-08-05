/*
 * DirectoryUtil.java
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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.Tuple;

class DirectoryUtil {
	private static class TuplePopper {
		private Instruction inst;
		private int num;
		private List<Tuple> tuples = new ArrayList<Tuple>();

		TuplePopper(Instruction inst, int num) {
			this.inst = inst;
			this.num = num;
		}

		CompletableFuture<List<Tuple>> pop() {
			return AsyncUtil.whileTrue(() -> {
				if(num-- == 0) {
					return AsyncUtil.READY_FALSE;
				}
				return inst.popParam()
						.thenComposeAsync(count -> inst.popParams(StackUtils.getInt(count)), FDB.DEFAULT_EXECUTOR)
						.thenApplyAsync(elements -> {
							tuples.add(Tuple.fromItems(elements));
							return num > 0;
						}, FDB.DEFAULT_EXECUTOR);
			})
			.thenApply(ignore -> tuples);
		}
	}

	static CompletableFuture<List<Tuple>> popTuples(Instruction inst, int num) {
		return new TuplePopper(inst, num).pop();
	}

	static CompletableFuture<Tuple> popTuple(Instruction inst) {
		return popTuples(inst, 1).thenApply(tuples -> tuples.get(0));
	}

	static CompletableFuture<List<List<String>>> popPaths(Instruction inst, int num) {
		return popTuples(inst, num).thenApplyAsync(tuples -> {
			List<List<String>> paths = new ArrayList<>(tuples.size());
			for(Tuple t : tuples) {
				List<String> path = new ArrayList<>(t.size());
				for(int i = 0; i < t.size(); ++i)
					path.add(t.getString(i));

				paths.add(path);
			}

			return paths;
		}, FDB.DEFAULT_EXECUTOR);
	}

	static CompletableFuture<List<String>> popPath(Instruction inst) {
		return popPaths(inst, 1).thenApply(paths -> paths.get(0));
	}

	static void pushError(Instruction inst, Throwable t, List<Object> dirList) {
		//System.err.println(t.getMessage());
		//t.printStackTrace();
		inst.push("DIRECTORY_ERROR".getBytes());
		DirectoryOperation op = DirectoryOperation.valueOf(inst.op);
		if(op.createsDirectory)
			dirList.add(null);
	}

	private DirectoryUtil() {}
}
