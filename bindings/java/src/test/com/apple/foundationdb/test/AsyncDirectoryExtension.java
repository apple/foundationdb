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

package com.apple.foundationdb.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.Function;
import com.apple.foundationdb.async.Future;
import com.apple.foundationdb.async.ReadyFuture;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

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

	Future<Void> processInstruction(final Instruction inst) {
		return executeInstruction(inst)
		.rescueRuntime(new Function<RuntimeException, Future<Void>>() {
			@Override
			public Future<Void> apply(RuntimeException e) {
				DirectoryUtil.pushError(inst, e, dirList);
				return ReadyFuture.DONE;
			}
		});
	}

	Future<Void> executeInstruction(final Instruction inst) {
		final DirectoryOperation op = DirectoryOperation.valueOf(inst.op);

		if(op == DirectoryOperation.DIRECTORY_CREATE_SUBSPACE) {
			return DirectoryUtil.popTuple(inst)
			.flatMap(new Function<Tuple, Future<Void>>() {
				@Override
				public Future<Void> apply(final Tuple prefix) {
					return inst.popParam()
					.map(new Function<Object, Void>() {
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
			.map(new Function<List<Object>, Void>() {
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
			.map(new Function<Object, Void>() {
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
			.map(new Function<Object, Void>() {
				@Override
				public Void apply(Object index) {
					errorIndex = StackUtils.getInt(index);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_CREATE_OR_OPEN || op == DirectoryOperation.DIRECTORY_OPEN) {
			return DirectoryUtil.popPath(inst)
			.flatMap(new Function<List<String>, Future<Void>>() {
				@Override
				public Future<Void> apply(final List<String> path) {
					return inst.popParam()
					.flatMap(new Function<Object, Future<Void>>() {
						@Override
						public Future<Void> apply(Object layer) {
							Future<DirectorySubspace> dir;
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

							return dir.map(new Function<DirectorySubspace, Void>() {
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
			.flatMap(new Function<List<String>, Future<Void>>() {
				@Override
				public Future<Void> apply(final List<String> path) {
					return inst.popParams(2)
					.flatMap(new Function<List<Object>, Future<Void>>() {
						@Override
						public Future<Void> apply(List<Object> params) {
							byte[] layer = (byte[])params.get(0);
							byte[] prefix = (byte[])params.get(1);

							Future<DirectorySubspace> dir;
							if(layer == null && prefix == null)
								dir = directory().create(inst.tcx, path);
							else if(prefix == null)
								dir = directory().create(inst.tcx, path, layer);
							else {
								if(layer == null)
									layer = new byte[0];

								dir = directory().create(inst.tcx, path, layer, prefix);
							}

							return dir.map(new Function<DirectorySubspace, Void>() {
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
			.flatMap(new Function<List<List<String>>, Future<DirectorySubspace>>() {
				@Override
				public Future<DirectorySubspace> apply(List<List<String>> paths) {
					return directory().move(inst.tcx, paths.get(0), paths.get(1));
				}
			})
			.map(new Function<DirectorySubspace, Void>() {
				@Override
				public Void apply(DirectorySubspace dirSubspace) {
					dirList.add(dirSubspace);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_MOVE_TO) {
			return DirectoryUtil.popPath(inst)
			.flatMap(new Function<List<String>, Future<DirectorySubspace>>() {
				@Override
				public Future<DirectorySubspace> apply(List<String> newAbsolutePath) {
					return directory().moveTo(inst.tcx, newAbsolutePath);
				}
			})
			.map(new Function<DirectorySubspace, Void>() {
				@Override
				public Void apply(DirectorySubspace dirSubspace) {
					dirList.add(dirSubspace);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_REMOVE) {
			return inst.popParam()
			.flatMap(new Function<Object, Future<List<List<String>>>>() {
				@Override
				public Future<List<List<String>>> apply(Object count) {
					return DirectoryUtil.popPaths(inst, StackUtils.getInt(count));
				}
			})
			.flatMap(new Function<List<List<String>>, Future<Void>>() {
				@Override
				public Future<Void> apply(List<List<String>> path) {
					if(path.size() == 0)
						return directory().remove(inst.tcx);
					else
						return directory().remove(inst.tcx, path.get(0));
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_REMOVE_IF_EXISTS) {
			return inst.popParam()
			.flatMap(new Function<Object, Future<List<List<String>>>>() {
				@Override
				public Future<List<List<String>>> apply(Object count) {
					return DirectoryUtil.popPaths(inst, StackUtils.getInt(count));
				}
			})
			.flatMap(new Function<List<List<String>>, Future<Void>>() {
				@Override
				public Future<Void> apply(List<List<String>> path) {
					if(path.size() == 0)
						return AsyncUtil.success(directory().removeIfExists(inst.tcx));
					else
						return AsyncUtil.success(directory().removeIfExists(inst.tcx, path.get(0)));
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_LIST) {
			return inst.popParam()
			.flatMap(new Function<Object, Future<List<List<String>>>>() {
				@Override
				public Future<List<List<String>>> apply(Object count) {
					return DirectoryUtil.popPaths(inst, StackUtils.getInt(count));
				}
			})
			.flatMap(new Function<List<List<String>>, Future<List<String>>>() {
				@Override
				public Future<List<String>> apply(List<List<String>> path) {
					if(path.size() == 0)
						return directory().list(inst.readTcx);
					else
						return directory().list(inst.readTcx, path.get(0));
				}
			})
			.map(new Function<List<String>, Void>() {
				@Override
				public Void apply(List<String> children) {
					inst.push(Tuple.fromItems(children).pack());
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_EXISTS) {
			return inst.popParam()
			.flatMap(new Function<Object, Future<List<List<String>>>>() {
				@Override
				public Future<List<List<String>>> apply(Object count) {
					return DirectoryUtil.popPaths(inst, StackUtils.getInt(count));
				}
			})
			.flatMap(new Function<List<List<String>>, Future<Boolean>>() {
				@Override
				public Future<Boolean> apply(List<List<String>> path) {
					if(path.size() == 0)
						return directory().exists(inst.readTcx);
					else
						return directory().exists(inst.readTcx, path.get(0));
				}
			})
			.map(new Function<Boolean, Void>() {
				@Override
				public Void apply(Boolean exists){
					inst.push(exists ? 1 : 0);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_PACK_KEY) {
			return DirectoryUtil.popTuple(inst)
			.map(new Function<Tuple, Void>() {
				@Override
				public Void apply(Tuple keyTuple) {
					inst.push(subspace().pack(keyTuple));
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_UNPACK_KEY) {
			return inst.popParam()
			.map(new Function<Object, Void>() {
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
			.map(new Function<Tuple, Void>() {
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
			.map(new Function<Object, Void>() {
				@Override
				public Void apply(Object key) {
					inst.push(subspace().contains((byte[])key) ? 1 : 0);
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_OPEN_SUBSPACE) {
			return DirectoryUtil.popTuple(inst)
			.map(new Function<Tuple, Void>() {
				@Override
				public Void apply(Tuple prefix) {
					dirList.add(subspace().subspace(prefix));
					return null;
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_LOG_SUBSPACE) {
			return inst.popParam()
			.map(new Function<Object, Void>() {
				@Override
				public Void apply(final Object prefix) {
					return inst.tcx.run(new Function<Transaction, Void>() {
						@Override
						public Void apply(Transaction tr) {
							tr.set(ByteArrayUtil.join((byte[])prefix, new Tuple().add(dirIndex).pack()), subspace().getKey());
							return null;
						}
					});
				}
			});
		}
		else if(op == DirectoryOperation.DIRECTORY_LOG_DIRECTORY) {
			return inst.popParam()
			.flatMap(new Function<Object, Future<Void>>() {
				@Override
				public Future<Void> apply(Object prefix) {
					final Subspace logSubspace = new Subspace(new Tuple().add(dirIndex), (byte[])prefix);
					return inst.tcx.run(new Function<Transaction, Future<Void>>() {
						@Override
						public Future<Void> apply(final Transaction tr) {
							return directory().exists(tr)
							.flatMap(new Function<Boolean, Future<List<String>>>() {
								@Override
								public Future<List<String>> apply(Boolean exists) {
									tr.set(logSubspace.pack("path"), Tuple.fromItems(directory().getPath()).pack());
									tr.set(logSubspace.pack("layer"), new Tuple().add(directory().getLayer()).pack());
									tr.set(logSubspace.pack("exists"), new Tuple().add(exists ? 1 : 0).pack());
									if(exists)
										return directory().list(tr);
									else
										return new ReadyFuture<List<String>>(new ArrayList<String>());
								}
							})
							.map(new Function<List<String>, Void>() {
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
			.map(new Function<Object, Void>() {
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
