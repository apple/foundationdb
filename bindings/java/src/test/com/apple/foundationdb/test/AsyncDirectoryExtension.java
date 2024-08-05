/*
 * AsyncDirectoryExtension.java
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

class AsyncDirectoryExtension {
	List<Object> dirList = new ArrayList<>();
	int dirIndex = 0;
	int errorIndex = 0;

	AsyncDirectoryExtension() {
		dirList.add(DirectoryLayer.getDefault());
	}

	Directory directory() {
		return (Directory)dirList.get(dirIndex);
	}

	Subspace subspace() {
		return (Subspace)dirList.get(dirIndex);
	}

	CompletableFuture<Void> processInstruction(final Instruction inst) {
		return executeInstruction(inst).exceptionally(e -> {
			DirectoryUtil.pushError(inst, e, dirList);
			return null;
		});
	}

	CompletableFuture<Void> executeInstruction(final Instruction inst) {
		final DirectoryOperation op = DirectoryOperation.valueOf(inst.op);

		if(op == DirectoryOperation.DIRECTORY_CREATE_SUBSPACE) {
			return DirectoryUtil.popTuple(inst)
			.thenComposeAsync(prefix -> inst.popParam()
			.thenAccept(rawPrefix -> dirList.add(new Subspace(prefix, (byte[])rawPrefix))));
		}
		else if(op == DirectoryOperation.DIRECTORY_CREATE_LAYER) {
			return inst.popParams(3).thenAcceptAsync(params -> {
				Subspace nodeSubspace = (Subspace)dirList.get(StackUtils.getInt(params.get(0)));
				Subspace contentSubspace = (Subspace)dirList.get(StackUtils.getInt(params.get(1)));
				boolean allowManualPrefixes = StackUtils.getInt(params.get(2)) == 1;

				if(nodeSubspace == null || contentSubspace == null)
					dirList.add(null);
				else
					dirList.add(new DirectoryLayer(nodeSubspace, contentSubspace, allowManualPrefixes));
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_CHANGE) {
			return inst.popParam().thenAcceptAsync(index -> {
				dirIndex = StackUtils.getInt(index);
				if(dirList.get(dirIndex) == null)
					dirIndex = errorIndex;
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_SET_ERROR_INDEX) {
			return inst.popParam().thenAcceptAsync(index -> errorIndex = StackUtils.getInt(index));
		}
		else if(op == DirectoryOperation.DIRECTORY_CREATE_OR_OPEN || op == DirectoryOperation.DIRECTORY_OPEN) {
			return DirectoryUtil.popPath(inst)
			.thenComposeAsync(path -> inst.popParam().thenComposeAsync(layer -> {
				CompletableFuture<DirectorySubspace> dir;
				if(layer == null) {
					if(op == DirectoryOperation.DIRECTORY_CREATE_OR_OPEN)
						dir = directory().createOrOpen(inst.tcx, path);
					else
						dir = directory().open(inst.readTcx, path);
				}
				else {
					if(op == DirectoryOperation.DIRECTORY_CREATE_OR_OPEN)
						dir = directory().createOrOpen(inst.tcx, path, (byte[])layer);
					else
						dir = directory().open(inst.readTcx, path, (byte[])layer);
				}

				return dir.thenAccept(dirList::add);
			}));
		}
		else if(op == DirectoryOperation.DIRECTORY_CREATE) {
			return DirectoryUtil.popPath(inst).thenComposeAsync(path -> inst.popParams(2).thenComposeAsync(params -> {
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

				return dir.thenAccept(dirList::add);
			}));
		}
		else if(op == DirectoryOperation.DIRECTORY_MOVE) {
			return DirectoryUtil.popPaths(inst, 2)
			.thenComposeAsync(paths -> directory().move(inst.tcx, paths.get(0), paths.get(1)))
			.thenAccept(dirList::add);
		}
		else if(op == DirectoryOperation.DIRECTORY_MOVE_TO) {
			return DirectoryUtil.popPath(inst)
			.thenComposeAsync(newAbsolutePath -> directory().moveTo(inst.tcx, newAbsolutePath))
			.thenAccept(dirList::add);
		}
		else if(op == DirectoryOperation.DIRECTORY_REMOVE) {
			return inst.popParam()
			.thenComposeAsync(count -> DirectoryUtil.popPaths(inst, StackUtils.getInt(count)))
			.thenComposeAsync(path -> {
				if(path.size() == 0)
					return directory().remove(inst.tcx);
				else
					return directory().remove(inst.tcx, path.get(0));
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_REMOVE_IF_EXISTS) {
			return inst.popParam()
			.thenComposeAsync(count -> DirectoryUtil.popPaths(inst, StackUtils.getInt(count)))
			.thenComposeAsync(path -> {
				if(path.size() == 0)
					return AsyncUtil.success(directory().removeIfExists(inst.tcx));
				else
					return AsyncUtil.success(directory().removeIfExists(inst.tcx, path.get(0)));
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_LIST) {
			return inst.popParam()
			.thenComposeAsync(count -> DirectoryUtil.popPaths(inst, StackUtils.getInt(count)))
			.thenComposeAsync(path -> {
				if(path.size() == 0)
					return directory().list(inst.readTcx);
				else
					return directory().list(inst.readTcx, path.get(0));
			})
			.thenAccept(children -> inst.push(Tuple.fromItems(children).pack()));
		}
		else if(op == DirectoryOperation.DIRECTORY_EXISTS) {
			// In Java, DirectoryLayer.exists can return true without doing any reads.
			// Other bindings will always do a read, so we get a read version now to be compatible with that behavior.
			return inst.readTcx.readAsync(tr -> tr.getReadVersion())
			.thenComposeAsync(v -> inst.popParam())
			.thenComposeAsync(count -> DirectoryUtil.popPaths(inst, StackUtils.getInt(count)))
			.thenComposeAsync(path -> {
				if(path.size() == 0)
					return directory().exists(inst.readTcx);
				else
					return directory().exists(inst.readTcx, path.get(0));
			})
			.thenAccept(exists -> inst.push(exists ? 1 : 0));
		}
		else if(op == DirectoryOperation.DIRECTORY_PACK_KEY) {
			return DirectoryUtil.popTuple(inst).thenAccept(keyTuple -> inst.push(subspace().pack(keyTuple)));
		}
		else if(op == DirectoryOperation.DIRECTORY_UNPACK_KEY) {
			return inst.popParam().thenAcceptAsync(key -> {
				Tuple tup = subspace().unpack((byte[])key);
				for(Object o : tup)
					inst.push(o);
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_RANGE) {
			return DirectoryUtil.popTuple(inst).thenAcceptAsync(tup -> {
				Range range = subspace().range(tup);
				inst.push(range.begin);
				inst.push(range.end);
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_CONTAINS) {
			return inst.popParam().thenAccept(key -> inst.push(subspace().contains((byte[])key) ? 1 : 0));
		}
		else if(op == DirectoryOperation.DIRECTORY_OPEN_SUBSPACE) {
			return DirectoryUtil.popTuple(inst).thenAcceptAsync(prefix -> dirList.add(subspace().subspace(prefix)));
		}
		else if(op == DirectoryOperation.DIRECTORY_LOG_SUBSPACE) {
			return inst.popParam().thenComposeAsync(prefix ->
				inst.tcx.runAsync(tr -> {
					tr.set(Tuple.from(dirIndex).pack((byte[])prefix), subspace().getKey());
					return AsyncUtil.DONE;
				})
			);
		}
		else if(op == DirectoryOperation.DIRECTORY_LOG_DIRECTORY) {
			return inst.popParam().thenComposeAsync(prefix -> {
				final Subspace logSubspace = new Subspace(new Tuple().add(dirIndex), (byte[])prefix);
				return inst.tcx.runAsync(tr -> directory().exists(tr)
					.thenComposeAsync(exists -> {
						tr.set(logSubspace.pack("path"), Tuple.fromItems(directory().getPath()).pack());
						tr.set(logSubspace.pack("layer"), new Tuple().add(directory().getLayer()).pack());
						tr.set(logSubspace.pack("exists"), new Tuple().add(exists ? 1 : 0).pack());
						if(exists)
							return directory().list(tr);
						else
							return CompletableFuture.completedFuture(Collections.emptyList());
					})
					.thenAcceptAsync(children -> tr.set(logSubspace.pack("children"), Tuple.fromItems(children).pack()))
				);
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_STRIP_PREFIX) {
			return inst.popParam().thenAcceptAsync(param -> {
				byte[] str = (byte[])param;
				byte[] rawPrefix = subspace().getKey();

				if(str.length < rawPrefix.length)
					throw new RuntimeException("String does not start with raw prefix");

				for(int i = 0; i < rawPrefix.length; ++i)
					if(str[i] != rawPrefix[i])
						throw new RuntimeException("String does not start with raw prefix");

				inst.push(Arrays.copyOfRange(str, rawPrefix.length, str.length));
			});
		}
		else {
			throw new RuntimeException("Unknown operation:" + inst.op);
		}
	}
}
