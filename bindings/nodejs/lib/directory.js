/*
 * directory.js
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

/*************
 * Utilities *
 *************/

function whileLoop(func, cb) {
	return future.create(function(futureCb) {
		fdbUtil.whileLoop(function(f) {
			func()(f);
		}, futureCb);
	}, cb);
}

function startsWith(str, prefix) {
	return str.length >= prefix.length && fdbUtil.buffersEqual(str.slice(0, prefix.length), prefix);
}

function valueOrDefault(value, defaultValue) {
	if(typeof value === 'undefined')
		return defaultValue;

	return value;
}

function pathsEqual(path1, path2) {
	if(path1.length !== path2.length)
		return false;

	for(var i = 0; i < path1.length; ++i)
		if(path1[i] !== path2[i])
			return false;

	return true;
}

function tuplifyPath(path) {
	if(!(path instanceof Array))
		path = [path];

	return path;
}

function checkLayer(layer, required) {
	if(layer && layer.length > 0 && !fdbUtil.buffersEqual(layer, required))
		throw new Error('The directory was created with an incompatible layer.');
}

/***************************
 * HighContentionAllocator *
 ***************************/

var HighContentionAllocator = function(subspace) {
	this.counters = subspace.subspace([0]);
	this.recent = subspace.subspace([1]);
};

HighContentionAllocator.prototype.allocate = transactional(function(tr) {
	var self = this;
	var windowStart = 0;
	return whileLoop(function() {
		return tr.snapshot.getRange(self.counters.range().begin, self.counters.range().end, { limit: 1, reverse: true })
		.toArray()
		.then(function(arr) {
			if(arr.length > 0) {
				windowStart = self.counters.unpack(arr[0].key)[0];
			}
		})
		.then(function() { 
			return self.chooseWindow(tr, windowStart);
	   	})
		.then(function(window) { 
			return self.choosePrefix(tr, window); 
		})
		.then(function(prefix) { 
			if(prefix !== null) {
				prefix = tuple.pack([prefix]); // exit the loop
				return prefix;
			}
		});
	})
});

HighContentionAllocator.prototype.chooseWindow = function(tr, windowStart) {
	var self = this;

	var increment = new Buffer(8);
	increment.fill(0);
	increment.writeUInt32LE(1, 0);

	var window = { start: windowStart, size: 0 };

	return whileLoop(function() {
		// Cannot yield to event loop in this block {
		if(window.start > windowStart) {
			tr.clearRange(self.counters, self.counters.get(window.start));
			tr.options.setNextWriteNoWriteConflictRange();
			tr.clearRange(self.recent, self.recent.get(window.start));
		}

		tr.add(self.counters.pack([window.start]), increment);
		return tr.snapshot.get(self.counters.get(window.start))
		// }
		.then(function(newCountBuffer) {
			var newCount = (newCountBuffer === null) ? 0 : newCountBuffer.readUInt32LE(0);
			window.size = windowSize(window.start);
			if(newCount * 2 < window.size) {
				return window; // exit the loop
			}

			window.start += window.size;
		});
	});
};

HighContentionAllocator.prototype.choosePrefix = function(tr, window) {
	var self = this;

	return whileLoop(function() {
		var candidate = Math.floor(Math.random() * window.size) + window.start;
		var allocationKey = self.recent.pack([candidate]);

		// Cannot yield to event loop in this block {
		var counterRange = tr.snapshot.getRange(self.counters.range().begin, self.counters.range().end, { limit: 1, reverse: true }).toArray();
		var allocation = tr.get(allocationKey);
		tr.options.setNextWriteNoWriteConflictRange();
		tr.set(allocationKey, buffer(''));
		// }

		return future.all([counterRange, allocation])
   		.then(function(vals) {
			var currentWindowStart = vals[0].length > 0 ? self.counters.unpack(vals[0][0].key)[0] : 0;
			if(currentWindowStart > window.start) {
				return null; // exit the loop and force find() to retry
			}
			if(vals[1] === null) {
				tr.addWriteConflictKey(allocationKey);
				return candidate; // exit the loop
			}
		});
	});
};

function windowSize(start) {
	if(start < 255)
		return 64;
	if(start < 65535)
		return 1024;

	return 8192;
}

/******************
 * DirectoryLayer *
******************/

var VERSION = [1, 0, 0];
var SUBDIRS = 0;

var DirectoryLayer = function(options) {
	options = valueOrDefault(options, {});

	this._nodeSubspace = valueOrDefault(options.nodeSubspace, new Subspace([], buffer.fromByteLiteral('\xfe')));
	
	// If specified, new automatically allocated prefixes will all fall within the contentSubspace
	this._contentSubspace = valueOrDefault(options.contentSubspace, new Subspace());
	this._allowManualPrefixes = valueOrDefault(options.allowManualPrefixes, false);

	this._rootNode = this._nodeSubspace.subspace([this._nodeSubspace.key()]);
	this._allocator = new HighContentionAllocator(this._rootNode.subspace([buffer('hca')]));

	this._path = [];
};

var createOrOpen = transactional(function(tr, self, path, options, allowCreate, allowOpen, cb) {
	options = valueOrDefault(options, {});
	var layer = valueOrDefault(options.layer, buffer(''));
	var prefix = options.prefix;

	allowCreate = valueOrDefault(allowCreate, true);
	allowOpen = valueOrDefault(allowOpen, true);

	return checkVersion(self, tr, false)
	.then(function() {
		if(typeof prefix !== 'undefined') {
			if(allowCreate && allowOpen)
				throw new Error('Cannot specify a prefix when calling create_or_open.');
			else if(!self._allowManualPrefixes) {
				if(self._path.length === 0)
					throw new Error('Cannot specify a prefix unless manual prefixes are enabled.');
				else
					throw new Error('Cannot specify a prefix in a partition.');
			}
		}

		path = toUnicodePath(path);
		if(path.length === 0)
			throw new Error('The root directory cannot be opened.');

		return find(self, tr, path).then(loadMetadata(tr));
	})
	.then(function(existingNode) {
		if(existingNode.exists()) {
			if(existingNode.isInPartition(false)) {
				var subpath = existingNode.getPartitionSubpath();
				var directoryLayer = existingNode.getContents(self)._directoryLayer;
				return createOrOpen(tr, 
									existingNode.getContents(self)._directoryLayer, 
									subpath, 
									options,
									allowCreate, 
									allowOpen);
			}

			return openDirectory(tr, self, path, layer, existingNode, allowOpen);
		}
		else
			return createDirectory(tr, self, path, layer, prefix, allowCreate);
	})(cb);
});

var openDirectory = function(tr, self, path, layer, existingNode, allowOpen) {
		if(!allowOpen)
			throw new Error('The directory already exists.');

		checkLayer(layer, existingNode.layer);

		return existingNode.getContents(self);
};

var createDirectory = function(tr, self, path, layer, prefix, allowCreate) {
	if(!allowCreate)
		throw new Error('The directory does not exist.');

	var prefixIsAllocated = typeof(prefix) === 'undefined';
	return checkVersion(self, tr, true)
	.then(function() {
		return getPrefix(self, tr, prefix);
	})
	.then(function(prefix) {
		return isPrefixFree(self, prefixIsAllocated ? tr.snapshot : tr, prefix)
		.then(function(isFree) {
			if(!isFree) {
				if(prefixIsAllocated)
					throw new Error('The directory layer has manually allocated prefixes that conflict with the automatic prefix allocator.');
				else
					throw new Error('The given prefix is already in use.');	
			}

			return getParentNode(self, tr, path);
		})
		.then(function(parentNode) {
			if(!parentNode)
				throw new Error('The parent directory doesn\'t exist.');

			var node = nodeWithPrefix(self, prefix);
			tr.set(parentNode.subspace([SUBDIRS]).pack([path[path.length-1]]), prefix);
			tr.set(node.pack([buffer('layer')]), layer);

			return contentsOfNode(self, node, path, layer);
		});
	});
};

DirectoryLayer.prototype.getLayer = function() {
	return new Buffer(0);
};

DirectoryLayer.prototype.getPath = function() {
	return this._path.slice(0);
};

DirectoryLayer.prototype.createOrOpen = function(databaseOrTransaction, path, options, cb) {
	return createOrOpen(databaseOrTransaction, this, path, options, true, true, cb);
};

DirectoryLayer.prototype.open = function(databaseOrTransaction, path, options, cb) {
	return createOrOpen(databaseOrTransaction, this, path, options, false, true, cb);
};

DirectoryLayer.prototype.create = function(databaseOrTransaction, path, options, cb) {
	return createOrOpen(databaseOrTransaction, this, path, options, true, false, cb);
};

DirectoryLayer.prototype.moveTo = function(databaseOrTransaction, newAbsolutePath, cb) {
	return future.reject(new Error('The root directory cannot be moved.'))(cb);
};

DirectoryLayer.prototype.move = transactional(function(tr, oldPath, newPath, cb) {
	var self = this;
	var oldNode, newNode;

	return checkVersion(self, tr, true)
	.then(function() {
		oldPath = toUnicodePath(oldPath);
		newPath = toUnicodePath(newPath);

		if(pathsEqual(oldPath, newPath.slice(0, oldPath.length)))
			throw new Error('The destination directory cannot be a subdirectory of the source directory.');

		var oldNodeFuture = find(self, tr, oldPath).then(loadMetadata(tr));
		var newNodeFuture = find(self, tr, newPath).then(loadMetadata(tr));
		return future.all([oldNodeFuture, newNodeFuture]);
	})
	.then(function(nodes) {
		oldNode = nodes[0];
		newNode = nodes[1];

		if(!oldNode.exists())
			throw new Error('The source directory does not exist.');

		if(oldNode.isInPartition(false) || newNode.isInPartition(false)) {
			if(!oldNode.isInPartition(false) || !newNode.isInPartition(false) || !pathsEqual(oldNode.path, newNode.path))
				throw new Error('Cannot move between partitions.');

			return newNode.getContents(self).move(tr, oldNode.getPartitionSubpath(), newNode.getPartitionSubpath());
		}
		
		if(newNode.exists())
			throw new Error('The destination directory already exists. Remove it first.');

		return find(self, tr, newPath.slice(0, newPath.length-1))
		.then(function(parentNode) {
			if(!parentNode.exists())
				throw new Error('The parent of the destination directory does not exist. Create it first.');

			tr.set(parentNode.subspace.subspace([SUBDIRS]).pack([newPath[newPath.length-1]]), 
					self._nodeSubspace.unpack(oldNode.subspace.key())[0]);

			return removeFromParent(self, tr, oldPath);
		})
		.then(function() {
			return contentsOfNode(self, oldNode.subspace, newPath, oldNode.layer);
		});
	})(cb);
});

DirectoryLayer.prototype.remove = transactional(function(tr, path, cb) {
	return removeInternal(this, tr, path, true)(cb);
});

DirectoryLayer.prototype.removeIfExists = transactional(function(tr, path, cb) {
	return removeInternal(this, tr, path, false)(cb);
});

function removeInternal(self, tr, path, failOnNonexistent) {
	return checkVersion(self, tr, true)
	.then(function() {
		path = valueOrDefault(path, []);
		if(path.length === 0)
			return future.reject(new Error('The root directory cannot be removed.'));

		path = toUnicodePath(path);
		return find(self, tr, path).then(loadMetadata(tr));
	})
	.then(function(node) {
		if(!node.exists()) {
			if(failOnNonexistent)
				throw new Error('The directory doesn\'t exist');
			else
				return false;
		}

		if(node.isInPartition(false)) {
			return removeInternal(node.getContents(self)._directoryLayer, 
									tr, 
									node.getPartitionSubpath(), 
									failOnNonexistent);
		}

		return removeRecursive(self, tr, node.subspace)
		.then(function() {
			return removeFromParent(self, tr, path);
		}).
		then(function() {
			return true;	
		});
	});
}

DirectoryLayer.prototype.list = transactional(function(tr, path, cb) {
	var self = this;
	return checkVersion(self, tr, false)
	.then(function() {
		path = valueOrDefault(path, []);
		path = toUnicodePath(path);

		return find(self, tr, path).then(loadMetadata(tr));
	})
	.then(function(node) {
		if(!node.exists())
			throw new Error('The given directory does not exist');
	
		if(node.isInPartition(true))
			return node.getContents(self).list(tr, node.getPartitionSubpath());

		var subdir = node.subspace.subspace([SUBDIRS]);

		return tr.getRange(subdir.range().begin, subdir.range().end).toArray()
		.then(function(arr) {
			return arr.map(function(kv) { return subdir.unpack(kv.key)[0].toString('utf8'); });
		});
	})(cb);
});

DirectoryLayer.prototype.exists = transactional(function(tr, path, cb) {
	var self = this;
	return checkVersion(self, tr, false)
	.then(function() {
		path = valueOrDefault(path, []);
		path = toUnicodePath(path);
		return find(self, tr, path).then(loadMetadata(tr));
	})
	.then(function(node) {
		if(!node.exists())
			return false;

		if(node.isInPartition(false))
			return node.getContents(self).exists(tr, node.getPartitionSubpath());

		return true;
	})(cb);
});

// Private functions:

function checkVersion(self, tr, writeAccess) {
	return tr.get(self._rootNode.pack([buffer('version')]))
	.then(function(versionBuf) {
		if(!versionBuf) {
			if(writeAccess)
				initializeDirectory(self, tr);

			return;
		}	

		var version = [];
		for(var i = 0; i < 3; ++i)
			version.push(versionBuf.readInt32LE(4*i));

		var dirVersion = util.format('%d.%d.%d', version[0], version[1], version[2]);
		var layerVersion = util.format('%d.%d.%d', VERSION[0], VERSION[1], VERSION[2]);

		if(version[0] > VERSION[0]) {
			throw new Error(util.format('Cannot load directory with version %s using directory layer %s', 
										dirVersion, 
										layerVersion));
		}

		if(version[1] > VERSION[1]) {
			throw new Error(util.format('Directory with version %s is read-only when opened using directory layer %s', 
										dirVersion, 
										layerVersion));
		}
	});
}

function initializeDirectory(self, tr) {
	var versionBuf = new Buffer(12);
	for(var i = 0; i < 3; ++i)
		versionBuf.writeUInt32LE(VERSION[i], i*4);

	tr.set(self._rootNode.pack([buffer('version')]), versionBuf);
}

function nodeWithPrefix(self, prefix) {
	if(prefix === null)
		return null;

	return self._nodeSubspace.subspace([prefix]);
}

function find(self, tr, path) {
	var pathIndex = 0;
	var node = new Node(self._rootNode, [], path);

	return whileLoop(function() {
		if(pathIndex === path.length)
			return future.resolve(node);
	
		return tr.get(node.subspace.subspace([SUBDIRS]).pack([path[pathIndex++]]))
		.then(function(val) {
			node = new Node(nodeWithPrefix(self, val), path.slice(0, pathIndex), path);
			if(!node.exists())
				return node;
			return node.loadMetadata(tr)
			.then(function() {
				if(fdbUtil.buffersEqual(node.layer, buffer('partition')))
					return node;
			});
		});
	});
}

function contentsOfNode(self, node, path, layer) {
	var prefix = self._nodeSubspace.unpack(node.key())[0];

	if(fdbUtil.buffersEqual(layer, buffer('partition')))
		return new DirectoryPartition(self._path.concat(path), prefix, self);
	else
		return new DirectorySubspace(self._path.concat(path), prefix, self, layer);
}

function getPrefix(self, tr, prefix) {
	if(typeof prefix === 'undefined') {
		return self._allocator.allocate(tr)
		.then(function(prefix) {
			var allocated = Buffer.concat([self._contentSubspace.key(), prefix], self._contentSubspace.key().length + prefix.length);
			return tr.getRangeStartsWith(allocated, { limit: 1 })
			.toArray()
			.then(function(arr) {
				if(arr.length > 0)
					throw new Error('The database has keys stored at the prefix chosen by the automatic prefix allocator: ' + prefix);

				return allocated;
			});
		});
	}
	else
		return future.resolve(buffer(prefix));
}

function getNodeContainingKey(self, tr, key) {
	if(self._nodeSubspace.contains(key))
		return future.resolve(self._rootNode);

	return tr.getRange(self._nodeSubspace.range([]).begin, 
						self._nodeSubspace.subspace([key]).range().begin, 
						{ limit: 1, reverse: true })
	.toArray()
	.then(function(arr) {
		if(arr.length > 0) {
			var prevPrefix = self._nodeSubspace.unpack(arr[0].key)[0];
			if(startsWith(key, prevPrefix))
				return nodeWithPrefix(self, prevPrefix);
		}

		return null;
	});
}

function isPrefixFree(self, tr, prefix) {
	if(!prefix || prefix.length === 0)
		return future.resolve(false);

	return getNodeContainingKey(self, tr, prefix)
	.then(function(node) {
		if(node)
			return false;

		return tr.getRange(self._nodeSubspace.pack([prefix]), 
							self._nodeSubspace.pack([fdbUtil.strinc(prefix)]), 
							{ limit: 1 })
		.toArray()
		.then(function(arr) {
			return arr.length === 0;
		});
	});
}

function getParentNode(self, tr, path) {
	if(path.length > 1) {
		return self.createOrOpen(tr, path.slice(0, path.length-1))
		.then(function(dir) {
			return nodeWithPrefix(self, dir.key());
		});
	}
	else
		return future.resolve(self._rootNode);
}

function removeFromParent(self, tr, path) {
	return find(self, tr, path.slice(0, path.length-1))
	.then(function(parentNode) {
		tr.clear(parentNode.subspace.subspace([SUBDIRS]).pack([path[path.length-1]]));
	});
}

function removeRecursive(self, tr, node) {
	var subdir = node.subspace([SUBDIRS]);
	return tr.getRange(subdir.range().begin, subdir.range().end)
	.forEach(function(kv, loopCb) {
		removeRecursive(self, tr, nodeWithPrefix(self, kv.value))(loopCb);
	})
	.then(function() {
		tr.clearRangeStartsWith(self._nodeSubspace.unpack(node.key())[0]);
		tr.clearRange(node.range().begin, node.range().end);
	});
}

function toUnicodePath(path) {
	if(Buffer.isBuffer(path) || path instanceof ArrayBuffer || path instanceof Uint8Array)
		path = buffer(path).toString('utf8');

	if(typeof path === 'string')
		return [path];

	if(path instanceof Array) {
		for(var i = 0; i < path.length; ++i) {
			if(Buffer.isBuffer(path[i]) || path[i] instanceof ArrayBuffer || path[i] instanceof Uint8Array)
				path[i] = buffer(path[i]).toString('utf8');
			if(typeof path[i] !== 'string')
				throw new TypeError('Invalid path: must be a string, Buffer, ArrayBuffer, Uint8Array, or an array of such items');
		}

		return path;
	}

	throw new TypeError('Invalid path: must be a string, Buffer, ArrayBuffer, Uint8Array, or an array of such items');
}

/*********************
 * DirectorySubspace *
 *********************/

var DirectorySubspace = function(path, prefix, directoryLayer, layer) {
	Subspace.call(this, undefined, prefix);
	this._path = path;
	this._directoryLayer = directoryLayer;
	this._layer = layer;
};

DirectorySubspace.prototype = new Subspace();
DirectorySubspace.constructor = DirectorySubspace;

DirectorySubspace.prototype.getLayer = function() {
	return this._layer;
};

DirectorySubspace.prototype.getPath = function() {
	return this._path.slice(0);
};

DirectorySubspace.prototype.createOrOpen = function(databaseOrTransaction, nameOrPath, options, cb) {
	var path = tuplifyPath(nameOrPath);
	return this._directoryLayer.createOrOpen(databaseOrTransaction, partitionSubpath(this, path), options, cb);
};

DirectorySubspace.prototype.open = function(databaseOrTransaction, nameOrPath, options, cb) {
	var path = tuplifyPath(nameOrPath);
	return this._directoryLayer.open(databaseOrTransaction, partitionSubpath(this, path), options, cb);
};

DirectorySubspace.prototype.create = function(databaseOrTransaction, nameOrPath, options, cb) {
	var path = tuplifyPath(nameOrPath);
	return this._directoryLayer.create(databaseOrTransaction, partitionSubpath(this, path), options, cb);
};

DirectorySubspace.prototype.list = function(databaseOrTransaction, nameOrPath, cb) {
	var path = tuplifyPath(valueOrDefault(nameOrPath, []));
	return this._directoryLayer.list(databaseOrTransaction, partitionSubpath(this, path), cb);
};

DirectorySubspace.prototype.move = function(databaseOrTransaction, oldNameOrPath, newNameOrPath, cb) {
	var oldPath = tuplifyPath(oldNameOrPath);
	var newPath = tuplifyPath(newNameOrPath);
	return this._directoryLayer.move(databaseOrTransaction, 
									partitionSubpath(this, oldPath), 
									partitionSubpath(this, newPath), 
									cb);
};

DirectorySubspace.prototype.moveTo = function(databaseOrTransaction, newAbsoluteNameOrPath, cb) {
	var directoryLayer;
	var newAbsolutePath;
	try {
		directoryLayer = getLayerForPath(this, []);
		newAbsolutePath = toUnicodePath(newAbsoluteNameOrPath);
		var partitionPath = newAbsolutePath.slice(0, directoryLayer._path.length);
		if(!pathsEqual(partitionPath, directoryLayer._path))
			throw new Error('Cannot move between partitions.');
	}
	catch(err) {
		return future.reject(err)(cb);
	}

	return directoryLayer.move(databaseOrTransaction, 
								this._path.slice(directoryLayer._path.length),
								newAbsolutePath.slice(directoryLayer._path.length), 
								cb);	
};

DirectorySubspace.prototype.remove = function(databaseOrTransaction, nameOrPath, cb) {
	var path = tuplifyPath(valueOrDefault(nameOrPath, []));
	var directoryLayer = getLayerForPath(this, path);
	return directoryLayer.remove(databaseOrTransaction, partitionSubpath(this, path, directoryLayer), cb);
};

DirectorySubspace.prototype.removeIfExists = function(databaseOrTransaction, nameOrPath, cb) {
	var path = tuplifyPath(valueOrDefault(nameOrPath, []));
	var directoryLayer = getLayerForPath(this, path);
	return directoryLayer.removeIfExists(databaseOrTransaction, partitionSubpath(this, path, directoryLayer), cb);
};

DirectorySubspace.prototype.exists = function(databaseOrTransaction, nameOrPath, cb) {
	var path = tuplifyPath(valueOrDefault(nameOrPath, []));
	var directoryLayer = getLayerForPath(this, path);
	return directoryLayer.exists(databaseOrTransaction, partitionSubpath(this, path, directoryLayer), cb);
};

var partitionSubpath = function(directorySubspace, path, directoryLayer) {
	directoryLayer = valueOrDefault(directoryLayer, directorySubspace._directoryLayer);
	return directorySubspace._path.slice(directoryLayer._path.length).concat(path);
};

/**********************
 * DirectoryPartition *
 **********************/

var DirectoryPartition = function(path, prefix, parentDirectoryLayer) {
	var directoryLayer = new DirectoryLayer({ 
		nodeSubspace: new Subspace(undefined, Buffer.concat([prefix, buffer.fromByteLiteral('\xfe')], prefix.length+1)), 
		contentSubspace: new Subspace(undefined, prefix) 
	});

	directoryLayer._path = path;
	DirectorySubspace.call(this, path, prefix, directoryLayer, buffer('partition'));
	this._parentDirectoryLayer = parentDirectoryLayer;
};

DirectoryPartition.prototype = new DirectorySubspace();
DirectoryPartition.constructor = DirectoryPartition;

DirectoryPartition.prototype.key = function() {
	throw new Error('Cannot get key for the root of a directory partition.');
};

DirectoryPartition.prototype.pack = function(arr) {
	throw new Error('Cannot pack keys using the root of a directory partition.');
};

DirectoryPartition.prototype.unpack = function(arr) {
	throw new Error('Cannot unpack keys using the root of a directory partition.');
};

DirectoryPartition.prototype.range = function(arr) {
	throw new Error('Cannot get range for the root of a directory partition.');
};

DirectoryPartition.prototype.contains = function(key) {
	throw new Error('Cannot check whether a key belongs to the root of a directory partition.');
};

DirectoryPartition.prototype.get = function(name) {
	throw new Error('Cannot open subspace in the root of a directory partition.');
};

DirectoryPartition.prototype.subspace = function(arr) {
	throw new Error('Cannot open subspace in the root of a directory partition.');
};

DirectoryPartition.prototype.asFoundationDBKey = function() {
	throw new Error('Cannot use the root of a directory partition as a key.');
};

var getLayerForPath = function(directorySubspace, path) {
	if(directorySubspace instanceof DirectoryPartition && path.length === 0)
		return directorySubspace._parentDirectoryLayer;
	else
		return directorySubspace._directoryLayer;
};

/********
 * Node *
 ********/

var Node = function(subspace, path, targetPath) {
	this.subspace = subspace;
	this.path = path;
	this.targetPath = targetPath;
};

Node.prototype.exists = function() {
	return typeof(this.subspace) !== 'undefined' && this.subspace !== null;
};

Node.prototype.loadMetadata = function(tr) {
	var self = this;
	if(!self.exists()) {
		self.loadedMetadata = true;
		return future.resolve(self);
	}

	return tr.get(self.subspace.pack([buffer('layer')]))
	.then(function(layer) {
		self.loadedMetadata = true;
		self.layer = layer;
		return self;
	});
};

Node.prototype.ensureMetadataLoaded = function() {
	if(!this.loadedMetadata)
		throw new Error('Metadata for node has not been loaded');
};

Node.prototype.isInPartition = function(includeEmptySubpath) {
	this.ensureMetadataLoaded();
	return this.exists() && 
		fdbUtil.buffersEqual(this.layer, buffer('partition')) &&
		(includeEmptySubpath || this.targetPath.length > this.path.length);
};

Node.prototype.getPartitionSubpath = function() {
	this.ensureMetadataLoaded();
	return this.targetPath.slice(this.path.length);
};

Node.prototype.getContents = function(directoryLayer) {
	this.ensureMetadataLoaded();
	return contentsOfNode(directoryLayer, this.subspace, this.path, this.layer);
};

var loadMetadata = function(tr) {
	return function(node) {
		return node.loadMetadata(tr);
	};
};

module.exports = { directory: new DirectoryLayer(), DirectoryLayer: DirectoryLayer };
