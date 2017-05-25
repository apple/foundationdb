/*
 * AsyncDirectoryExtension.java
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.apple.cie.foundationdb.Range;
import com.apple.cie.foundationdb.Transaction;
import com.apple.cie.foundationdb.async.AsyncUtil;
import com.apple.cie.foundationdb.directory.Directory;
import com.apple.cie.foundationdb.directory.DirectoryLayer;
import com.apple.cie.foundationdb.directory.DirectorySubspace;
import com.apple.cie.foundationdb.subspace.Subspace;
import com.apple.cie.foundationdb.tuple.ByteArrayUtil;
import com.apple.cie.foundationdb.tuple.Tuple;

class AsyncDirectoryExtension {
	List<Object> dirList = new ArrayList<Object>();
	int dirIndex = 0;
	int errorIndex = 0;

	public AsyncDirectoryExtension() {
		dirList.add(DirectoryLayer.getDefault());
	}

	Directory directory() {
		return (Directory)dirList.get(dirIndex);
	}

	Subspace subspace() {
		return (Subspace)dirList.get(dirIndex);
	}

	CompletableFuture<Void> processInstruction(final Instruction inst) {
		return executeInstruction(inst)
		.exceptionally(new Function<Throwable, Void>() {
			@Override
			public Void apply(Throwable e) {
				DirectoryUtil.pushError(inst, e, dirList);
				return null;
			}
		});
	}

	CompletableFuture<Void> executeInstruction(final Instruction inst) {
		final DirectoryOperation op = DirectoryOperation.valueOf(inst.op);

		if(op == DirectoryOperation.DIRECTORY_CREATE_SUBSPACE) {
			return DirectoryUtil.popTuple(inst)
			.thenComposeAsync(new Function<Tuple, CompletableFuture<Void>>() {
				@Override
				public CompletableFuture<Void> apply(final Tuple prefix) {
					return inst.popParam()
					.thenApplyAsync(new Function<Object, Void>() {
						@Override
						public Void apply(Object rawPrefix) {
							dirList.add(new Subspace(prefix, (byte[])rawPrefix));
							return null;
						}
					});
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_CREATE_LAYER) {
			return inst.popParams(3)
			.thenApplyAsync(new Function<List<Object>, Void>() {
				@Override
				public Void apply(List<Object> params) {
					Subspace nodeSubspace = (Subspace)dirList.get(StackUtils.getInt(params.get(0)));
					Subspace contentSubspace = (Subspace)dirList.get(StackUtils.getInt(params.get(1)));
					boolean allowManualPrefixes = StackUtils.getInt(params.get(2)) == 1;

					if(nodeSubspace == null || contentSubspace == null)
						dirList.add(null);
					else
						dirList.add(new DirectoryLayer(nodeSubspace, contentSubspace, allowManualPrefixes));

					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_CHANGE) {
			return inst.popParam()
			.thenApplyAsync(new Function<Object, Void>() {
				@Override
				public Void apply(Object index) {
					dirIndex = StackUtils.getInt(index);
					if(dirList.get(dirIndex) == null)
						dirIndex = errorIndex;

					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_SET_ERROR_INDEX) {
			return inst.popParam()
			.thenApplyAsync(new Function<Object, Void>() {
				@Override
				public Void apply(Object index) {
					errorIndex = StackUtils.getInt(index);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_CREATE_OR_OPEN || op == DirectoryOperation.DIRECTORY_OPEN) {
			return DirectoryUtil.popPath(inst)
			.thenComposeAsync(new Function<List<String>, CompletableFuture<Void>>() {
				@Override
				public CompletableFuture<Void> apply(final List<String> path) {
					return inst.popParam()
					.thenComposeAsync(new Function<Object, CompletableFuture<Void>>() {
						@Override
						public CompletableFuture<Void> apply(Object layer) {
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

							return dir.thenApplyAsync(new Function<DirectorySubspace, Void>() {
								@Override
								public Void apply(DirectorySubspace dirSubspace) {
									dirList.add(dirSubspace);
									return null;
								}
							});
						}
					});
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_CREATE) {
			return DirectoryUtil.popPath(inst)
			.thenComposeAsync(new Function<List<String>, CompletableFuture<Void>>() {
				@Override
				public CompletableFuture<Void> apply(final List<String> path) {
					return inst.popParams(2)
					.thenComposeAsync(new Function<List<Object>, CompletableFuture<Void>>() {
						@Override
						public CompletableFuture<Void> apply(List<Object> params) {
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

							return dir.thenApplyAsync(new Function<DirectorySubspace, Void>() {
								@Override
								public Void apply(DirectorySubspace dirSubspace) {
									dirList.add(dirSubspace);
									return null;
								}
							});
						}
					});
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_MOVE) {
			return DirectoryUtil.popPaths(inst, 2)
			.thenComposeAsync(new Function<List<List<String>>, CompletableFuture<DirectorySubspace>>() {
				@Override
				public CompletableFuture<DirectorySubspace> apply(List<List<String>> paths) {
					return directory().move(inst.tcx, paths.get(0), paths.get(1));
				}
			})
			.thenApplyAsync(new Function<DirectorySubspace, Void>() {
				@Override
				public Void apply(DirectorySubspace dirSubspace) {
					dirList.add(dirSubspace);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_MOVE_TO) {
			return DirectoryUtil.popPath(inst)
			.thenComposeAsync(new Function<List<String>, CompletableFuture<DirectorySubspace>>() {
				@Override
				public CompletableFuture<DirectorySubspace> apply(List<String> newAbsolutePath) {
					return directory().moveTo(inst.tcx, newAbsolutePath);
				}
			})
			.thenApplyAsync(new Function<DirectorySubspace, Void>() {
				@Override
				public Void apply(DirectorySubspace dirSubspace) {
					dirList.add(dirSubspace);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_REMOVE) {
			return inst.popParam()
			.thenComposeAsync(new Function<Object, CompletableFuture<List<List<String>>>>() {
				@Override
				public CompletableFuture<List<List<String>>> apply(Object count) {
					return DirectoryUtil.popPaths(inst, StackUtils.getInt(count));
				}
			})
			.thenComposeAsync(new Function<List<List<String>>, CompletableFuture<Void>>() {
				@Override
				public CompletableFuture<Void> apply(List<List<String>> path) {
					if(path.size() == 0)
						return directory().remove(inst.tcx);
					else
						return directory().remove(inst.tcx, path.get(0));
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_REMOVE_IF_EXISTS) {
			return inst.popParam()
			.thenComposeAsync(new Function<Object, CompletableFuture<List<List<String>>>>() {
				@Override
				public CompletableFuture<List<List<String>>> apply(Object count) {
					return DirectoryUtil.popPaths(inst, StackUtils.getInt(count));
				}
			})
			.thenComposeAsync(new Function<List<List<String>>, CompletableFuture<Void>>() {
				@Override
				public CompletableFuture<Void> apply(List<List<String>> path) {
					if(path.size() == 0)
						return AsyncUtil.success(directory().removeIfExists(inst.tcx));
					else
						return AsyncUtil.success(directory().removeIfExists(inst.tcx, path.get(0)));
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_LIST) {
			return inst.popParam()
			.thenComposeAsync(new Function<Object, CompletableFuture<List<List<String>>>>() {
				@Override
				public CompletableFuture<List<List<String>>> apply(Object count) {
					return DirectoryUtil.popPaths(inst, StackUtils.getInt(count));
				}
			})
			.thenComposeAsync(new Function<List<List<String>>, CompletableFuture<List<String>>>() {
				@Override
				public CompletableFuture<List<String>> apply(List<List<String>> path) {
					if(path.size() == 0)
						return directory().list(inst.readTcx);
					else
						return directory().list(inst.readTcx, path.get(0));
				}
			})
			.thenApplyAsync(new Function<List<String>, Void>() {
				@Override
				public Void apply(List<String> children) {
					inst.push(Tuple.fromItems(children).pack());
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_EXISTS) {
			return inst.popParam()
			.thenComposeAsync(new Function<Object, CompletableFuture<List<List<String>>>>() {
				@Override
				public CompletableFuture<List<List<String>>> apply(Object count) {
					return DirectoryUtil.popPaths(inst, StackUtils.getInt(count));
				}
			})
			.thenComposeAsync(new Function<List<List<String>>, CompletableFuture<Boolean>>() {
				@Override
				public CompletableFuture<Boolean> apply(List<List<String>> path) {
					if(path.size() == 0)
						return directory().exists(inst.readTcx);
					else
						return directory().exists(inst.readTcx, path.get(0));
				}
			})
			.thenApplyAsync(new Function<Boolean, Void>() {
				@Override
				public Void apply(Boolean exists){
					inst.push(exists ? 1 : 0);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_PACK_KEY) {
			return DirectoryUtil.popTuple(inst)
			.thenApplyAsync(new Function<Tuple, Void>() {
				@Override
				public Void apply(Tuple keyTuple) {
					inst.push(subspace().pack(keyTuple));
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_UNPACK_KEY) {
			return inst.popParam()
			.thenApplyAsync(new Function<Object, Void>() {
				@Override
				public Void apply(Object key) {
					Tuple tup = subspace().unpack((byte[])key);
					for(Object o : tup)
						inst.push(o);

					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_RANGE) {
			return DirectoryUtil.popTuple(inst)
			.thenApplyAsync(new Function<Tuple, Void>() {
				@Override
				public Void apply(Tuple tup) {
					Range range = subspace().range(tup);
					inst.push(range.begin);
					inst.push(range.end);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_CONTAINS) {
			return inst.popParam()
			.thenApplyAsync(new Function<Object, Void>() {
				@Override
				public Void apply(Object key) {
					inst.push(subspace().contains((byte[])key) ? 1 : 0);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_OPEN_SUBSPACE) {
			return DirectoryUtil.popTuple(inst)
			.thenApplyAsync(new Function<Tuple, Void>() {
				@Override
				public Void apply(Tuple prefix) {
					dirList.add(subspace().subspace(prefix));
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_LOG_SUBSPACE) {
			return inst.popParam()
			.thenComposeAsync(new Function<Object, CompletableFuture<Void>>() {
				@Override
				public CompletableFuture<Void> apply(final Object prefix) {
					return inst.tcx.runAsync(new Function<Transaction, CompletableFuture<Void>>() {
						@Override
						public CompletableFuture<Void> apply(Transaction tr) {
							tr.set(ByteArrayUtil.join((byte[])prefix, new Tuple().add(dirIndex).pack()), subspace().getKey());
							return CompletableFuture.completedFuture(null);
						}
					});
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_LOG_DIRECTORY) {
			return inst.popParam()
			.thenComposeAsync(new Function<Object, CompletableFuture<Void>>() {
				@Override
				public CompletableFuture<Void> apply(Object prefix) {
					final Subspace logSubspace = new Subspace(new Tuple().add(dirIndex), (byte[])prefix);
					return inst.tcx.runAsync(new Function<Transaction, CompletableFuture<Void>>() {
						@Override
						public CompletableFuture<Void> apply(final Transaction tr) {
							return directory().exists(tr)
							.thenComposeAsync(new Function<Boolean, CompletableFuture<List<String>>>() {
								@Override
								public CompletableFuture<List<String>> apply(Boolean exists) {
									tr.set(logSubspace.pack("path"), Tuple.fromItems(directory().getPath()).pack());
									tr.set(logSubspace.pack("layer"), new Tuple().add(directory().getLayer()).pack());
									tr.set(logSubspace.pack("exists"), new Tuple().add(exists ? 1 : 0).pack());
									if(exists)
										return directory().list(tr);
									else
										return CompletableFuture.completedFuture(new ArrayList<String>());
								}
							})
							.thenApplyAsync(new Function<List<String>, Void>() {
								@Override
								public Void apply(List<String> children) {
									tr.set(logSubspace.pack("children"), Tuple.fromItems(children).pack());
									return null;
								}
							});
						}
					});
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_STRIP_PREFIX) {
			return inst.popParam()
			.thenApplyAsync(new Function<Object, Void>() {
				@Override
				public Void apply(Object param) {
					byte[] str = (byte[])param;
					byte[] rawPrefix = subspace().getKey();

					if(str.length < rawPrefix.length)
						throw new RuntimeException("String does not start with raw prefix");

					for(int i = 0; i < rawPrefix.length; ++i)
						if(str[i] != rawPrefix[i])
							throw new RuntimeException("String does not start with raw prefix");

					inst.push(Arrays.copyOfRange(str, rawPrefix.length, str.length));
					return null;
				}
			});
		}
		else {
			throw new RuntimeException("Unknown operation:" + inst.op);
		}
	}
}
