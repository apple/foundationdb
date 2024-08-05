/*
 * DirectoryExtension.java
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
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

class DirectoryExtension {
	List<Object> dirList = new ArrayList<Object>();
	int dirIndex = 0;
	int errorIndex = 0;

	DirectoryExtension() {
		dirList.add(DirectoryLayer.getDefault());
	}

	Directory directory() {
		return (Directory)dirList.get(dirIndex);
	}

	Subspace subspace() {
		return (Subspace)dirList.get(dirIndex);
	}

	void processInstruction(final Instruction inst) {
		try {
			//System.out.println("Processing operation " + inst.op + " (" + inst.context.instructionIndex + ")");
			DirectoryOperation op = DirectoryOperation.valueOf(inst.op);
			if(op == DirectoryOperation.DIRECTORY_CREATE_SUBSPACE) {
				Tuple prefix = DirectoryUtil.popTuple(inst).get();
				byte[] rawPrefix = (byte[])inst.popParam().get();
				dirList.add(new Subspace(prefix, rawPrefix));
			}
			else if(op == DirectoryOperation.DIRECTORY_CREATE_LAYER) {
				List<Object> params = inst.popParams(3).get();
				Subspace nodeSubspace = (Subspace)dirList.get(StackUtils.getInt(params.get(0)));
				Subspace contentSubspace = (Subspace)dirList.get(StackUtils.getInt(params.get(1)));
				boolean allowManualPrefixes = StackUtils.getInt(params.get(2)) == 1;

				if(nodeSubspace == null || contentSubspace == null)
					dirList.add(null);
				else
					dirList.add(new DirectoryLayer(nodeSubspace, contentSubspace, allowManualPrefixes));
			}
			else if(op == DirectoryOperation.DIRECTORY_CHANGE) {
				dirIndex = StackUtils.getInt(inst.popParam().get());
				if(dirList.get(dirIndex) == null)
					dirIndex = errorIndex;
			}
			else if(op == DirectoryOperation.DIRECTORY_SET_ERROR_INDEX) {
				errorIndex = StackUtils.getInt(inst.popParam().get());
			}
			else if(op == DirectoryOperation.DIRECTORY_CREATE_OR_OPEN || op == DirectoryOperation.DIRECTORY_OPEN) {
				List<String> path = DirectoryUtil.popPath(inst).get();
				byte[] layer = (byte[])inst.popParam().get();

				CompletableFuture<DirectorySubspace> dir;
				if(layer == null) {
					if(op == DirectoryOperation.DIRECTORY_CREATE_OR_OPEN)
						dir = directory().createOrOpen(inst.tcx, path);
					else
						dir = directory().open(inst.readTcx, path);
				}
				else {
					if(op == DirectoryOperation.DIRECTORY_CREATE_OR_OPEN)
						dir = directory().createOrOpen(inst.tcx, path, layer);
					else
						dir = directory().open(inst.readTcx, path, layer);
				}

				dirList.add(dir.get());
			}
			else if(op == DirectoryOperation.DIRECTORY_CREATE) {
				List<String> path = DirectoryUtil.popPath(inst).get();
				List<Object> params = inst.popParams(2).get();

				byte[] layer = (byte[])params.get(0);
				byte[] prefix = (byte[])params.get(1);

				CompletableFuture<DirectorySubspace> dir;
				if(layer == null && prefix == null)
					dir = directory().create(inst.tcx, path);
				else if(prefix == null)
					dir = directory().create(inst.tcx, path, layer);
				else {
					if(layer == null)
						layer = new byte[0];

					dir = directory().create(inst.tcx, path, layer, prefix);
				}

				dirList.add(dir.get());
			}
			else if(op == DirectoryOperation.DIRECTORY_MOVE) {
				List<List<String>> paths = DirectoryUtil.popPaths(inst, 2).get();
				DirectorySubspace dir = directory().move(inst.tcx, paths.get(0), paths.get(1)).get();
				dirList.add(dir);
			}
			else if(op == DirectoryOperation.DIRECTORY_MOVE_TO) {
				List<String> newAbsolutePath = DirectoryUtil.popPath(inst).get();
				DirectorySubspace dir = directory().moveTo(inst.tcx, newAbsolutePath).get();
				dirList.add(dir);
			}
			else if(op == DirectoryOperation.DIRECTORY_REMOVE) {
				int count = StackUtils.getInt(inst.popParam().get());
				List<List<String>> path = DirectoryUtil.popPaths(inst, count).get();
				if(path.size() == 0)
					directory().remove(inst.tcx).get();
				else
					directory().remove(inst.tcx, path.get(0)).get();
			}
			else if(op == DirectoryOperation.DIRECTORY_REMOVE_IF_EXISTS) {
				int count = StackUtils.getInt(inst.popParam().get());
				List<List<String>> path = DirectoryUtil.popPaths(inst, count).get();
				if(path.size() == 0)
					directory().removeIfExists(inst.tcx).get();
				else
					directory().removeIfExists(inst.tcx, path.get(0)).get();
			}
			else if(op == DirectoryOperation.DIRECTORY_LIST) {
				int count = StackUtils.getInt(inst.popParam().get());
				List<List<String>> path = DirectoryUtil.popPaths(inst, count).get();
				List<String> children;
				if(path.size() == 0)
					children = directory().list(inst.readTcx).get();
				else
					children = directory().list(inst.readTcx, path.get(0)).get();

				inst.push(Tuple.fromItems(children).pack());
			}
			else if(op == DirectoryOperation.DIRECTORY_EXISTS) {
				int count = StackUtils.getInt(inst.popParam().get());
				List<List<String>> path = DirectoryUtil.popPaths(inst, count).get();
				boolean exists;

				// In Java, DirectoryLayer.exists can return true without doing any reads.
				// Other bindings will always do a read, so we get a read version now to be compatible with that behavior.
				inst.readTcx.read(tr -> tr.getReadVersion().join());

				if(path.size() == 0)
					exists = directory().exists(inst.readTcx).get();
				else
					exists = directory().exists(inst.readTcx, path.get(0)).get();

				inst.push(exists ? 1 : 0);
			}
			else if(op == DirectoryOperation.DIRECTORY_PACK_KEY) {
				Tuple keyTuple = DirectoryUtil.popTuple(inst).get();
				inst.push(subspace().pack(keyTuple));
			}
			else if(op == DirectoryOperation.DIRECTORY_UNPACK_KEY) {
				byte[] key = (byte[])inst.popParam().get();
				Tuple tup = subspace().unpack(key);
				for(Object o : tup)
					inst.push(o);
			}
			else if(op == DirectoryOperation.DIRECTORY_RANGE) {
				Tuple tup = DirectoryUtil.popTuple(inst).get();
				Range range = subspace().range(tup);
				inst.push(range.begin);
				inst.push(range.end);
			}
			else if(op == DirectoryOperation.DIRECTORY_CONTAINS) {
				byte[] key = (byte[])inst.popParam().get();
				inst.push(subspace().contains(key) ? 1 : 0);
			}
			else if(op == DirectoryOperation.DIRECTORY_OPEN_SUBSPACE) {
				Tuple prefix = DirectoryUtil.popTuple(inst).get();
				dirList.add(subspace().subspace(prefix));
			}
			else if(op == DirectoryOperation.DIRECTORY_LOG_SUBSPACE) {
				final byte[] prefix = (byte[])inst.popParam().get();
				inst.tr.set(Tuple.from(dirIndex).pack(prefix), subspace().getKey());
			}
			else if(op == DirectoryOperation.DIRECTORY_LOG_DIRECTORY) {
				final byte[] prefix = (byte[])inst.popParam().get();
				boolean exists = directory().exists(inst.tr).get();
				List<String> children = exists ? directory().list(inst.tr).get() : new ArrayList<String>();
				Subspace logSubspace = new Subspace(new Tuple().add(dirIndex), prefix);
				inst.tr.set(logSubspace.pack("path"), Tuple.fromItems(directory().getPath()).pack());
				inst.tr.set(logSubspace.pack("layer"), new Tuple().add(directory().getLayer()).pack());
				inst.tr.set(logSubspace.pack("exists"), new Tuple().add(exists ? 1 : 0).pack());
				inst.tr.set(logSubspace.pack("children"), Tuple.fromItems(children).pack());
			}
			else if(op == DirectoryOperation.DIRECTORY_STRIP_PREFIX) {
				byte[] str = (byte[])inst.popParam().get();
				byte[] rawPrefix = subspace().getKey();

				if(str.length < rawPrefix.length)
					throw new RuntimeException("String does not start with raw prefix");

				for(int i = 0; i < rawPrefix.length; ++i)
					if(str[i] != rawPrefix[i])
						throw new RuntimeException("String does not start with raw prefix");

				inst.push(Arrays.copyOfRange(str, rawPrefix.length, str.length));
			}
			else {
				throw new RuntimeException("Unknown operation:" + inst.op);
			}
		}
		catch(Throwable t) {
			DirectoryUtil.pushError(inst, t, dirList);
		}
	}
}
