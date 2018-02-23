/*
 * directory_extension.js
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

"use strict";

var util = require('util');
var fdb = require('../lib/fdb.js').apiVersion(parseInt(process.argv[3]));
var fdbUtil = require('../lib/fdbUtil.js');
var dirUtil = require('./directory_util.js');

var logAll = false;

var logInstructions = false;
var logOps = false;
var logDirs = false;
var logErrors = false;

var logOp = function(message, force) {
	if(logOps || logAll || force)
		console.log(message);
};

var DirectoryExtension = function() {
	this.dirList = [fdb.directory];
	this.dirIndex = 0;
	this.errorIndex = 0;
};

DirectoryExtension.prototype.processInstruction = function(inst, cb) {
	var self = this;
	var directory = this.dirList[this.dirIndex];

	var promiseCb = function(err) {
		if(err && (logErrors || logAll)) {
			console.log(err);
			//console.log(err.stack);
		}

		dirUtil.pushError(self, inst, err);
		cb();
	};

	var appendDir = function(dir) {
		if(logDirs || logAll)
			console.log(util.format('pushed at %d (op=%s)',  self.dirList.length, inst.op));

		self.dirList.push(dir);
	};

	if(logAll || logInstructions)
		console.log(inst.context.instructionIndex, inst.tokens[0].toString());

	if(inst.op === 'DIRECTORY_CREATE_SUBSPACE') {
		dirUtil.popTuples(inst)
		.then(function(path) {
			return inst.pop()
			.then(function(rawPrefix) {
				logOp(util.format('created subspace at (%s): %s',  path, fdb.buffer.printable(rawPrefix)));
				appendDir(new fdb.Subspace(path, rawPrefix));
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_CREATE_LAYER') {
		inst.pop({count: 3})
		.then(function(params) {
			var index1 = params[0];
			var index2 = params[1];
			var allowManualPrefixes = params[2];

			var dir1 = self.dirList[params[0]];
			var dir2 = self.dirList[params[1]];
			if(dir1 === null || dir2 === null) {
				logOp('create directory layer: None');
				appendDir(null);
			}
			else {
				logOp(util.format('create directory layer: node_subspace (%d) = %s, content_subspace (%d) = %s, allow_manual_prefixes = %d', index1, fdb.buffer.printable(dir1.rawPrefix), index2, fdb.buffer.printable(dir2.rawPrefix), allowManualPrefixes));
				appendDir(new fdb.DirectoryLayer({ nodeSubspace: dir1,
														   contentSubspace: dir2,
														   allowManualPrefixes: allowManualPrefixes === 1 }));
			}
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_CHANGE') {
		inst.pop()
		.then(function(index) {
			if(self.dirList[index] === null)
				self.dirIndex = self.errorIndex;
			else
				self.dirIndex = index;

			if(logDirs || logAll) {
				var dir = self.dirList[self.dirIndex];
				console.log(util.format('changed directory to %d ((%s))', self.dirIndex, dir._path));
			}
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_SET_ERROR_INDEX') {
		inst.pop()
		.then(function(errorIndex) {
			self.errorIndex = errorIndex;
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_CREATE_OR_OPEN') {
		dirUtil.popTuples(inst)
		.then(function(path) {
			return inst.pop()
			.then(function(layer) {
				logOp(util.format('create_or_open (%s): layer=%s', directory._path + path, fdb.buffer.printable(layer) || ''));
				return directory.createOrOpen(inst.tr, path, {'layer': layer || undefined});
			})
			.then(function(dir) {
				appendDir(dir);
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_CREATE') {
		dirUtil.popTuples(inst)
		.then(function(path) {
			return inst.pop({count: 2})
			.then(function(params) {
				logOp(util.format('create (%s): layer=%s, prefix=%s', directory._path + path, fdb.buffer.printable(params[0]) || '', fdb.buffer.printable(params[1] || '')));
				return directory.create(inst.tr, path, {'layer': params[0] || undefined, 'prefix': params[1] || undefined});
			})
			.then(function(dir) {
				appendDir(dir);
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_OPEN') {
		dirUtil.popTuples(inst)
		.then(function(path) {
			return inst.pop()
			.then(function(layer) {
				logOp(util.format('open (%s): layer=%s', directory._path + path, fdb.buffer.printable(layer) || ''));
				return directory.open(inst.tr, path, {'layer': layer});
			})
			.then(function(dir) {
				appendDir(dir);
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_MOVE') {
		dirUtil.popTuples(inst, 2)
		.then(function(paths) {
			logOp(util.format('move (%s) to (%s)', directory._path + paths[0], directory._path + paths[1]));
			return directory.move(inst.tr, paths[0], paths[1])
			.then(function(dir) {
				appendDir(dir);
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_MOVE_TO') {
		dirUtil.popTuples(inst)
		.then(function(newAbsolutePath) {
			logOp(util.format('move (%s) to (%s)', directory._path, newAbsolutePath));
			return directory.moveTo(inst.tr, newAbsolutePath)
			.then(function(dir) {
				appendDir(dir);
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_REMOVE') {
		inst.pop()
		.then(function(count) {
			return dirUtil.popTuples(inst, count)
			.then(function(path) {
				logOp(util.format('remove (%s)', directory._path + (path ? path : '')));
				return directory.remove(inst.tr, path);
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_REMOVE_IF_EXISTS') {
		inst.pop()
		.then(function(count) {
			return dirUtil.popTuples(inst, count)
			.then(function(path) {
				logOp(util.format('remove_if_exists (%s)', directory._path + (path ? path : '')));
				return directory.removeIfExists(inst.tr, path);
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_LIST') {
		inst.pop()
		.then(function(count) {
			return dirUtil.popTuples(inst, count)
			.then(function(path) {
				return directory.list(inst.tr, path);
			})
			.then(function(children) {
				inst.push(fdb.tuple.pack(children));
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_EXISTS') {
		var path;
		inst.pop()
		.then(function(count) {
			return dirUtil.popTuples(inst, count)
			.then(function(p) {
				path = p;
				return directory.exists(inst.tr, path);
			})
			.then(function(exists) {
				logOp(util.format('exists (%s): %d', directory._path + (path ? path : ''), exists ? 1 : 0));
				inst.push(exists ? 1 : 0);
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_PACK_KEY') {
		dirUtil.popTuples(inst)
		.then(function(keyTuple) {
			inst.push(directory.pack(keyTuple));
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_UNPACK_KEY') {
		inst.pop()
		.then(function(key) {
			logOp(util.format('unpack %s in subspace with prefix %s', fdb.buffer.printable(key), fdb.buffer.printable(directory.rawPrefix)));
			var tup = directory.unpack(key);
			for(var i = 0; i < tup.length; ++i)
				inst.push(tup[i]);
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_RANGE') {
		dirUtil.popTuples(inst)
		.then(function(tup) {
			var rng = directory.range(tup);
			inst.push(rng.begin);
			inst.push(rng.end);
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_CONTAINS') {
		inst.pop()
		.then(function(key) {
			inst.push(directory.contains(key) ? 1 : 0);
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_OPEN_SUBSPACE') {
		dirUtil.popTuples(inst)
		.then(function(path) {
			logOp(util.format('open_subspace (%s)', path));
			appendDir(directory.subspace(path));
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_LOG_SUBSPACE') {
		inst.pop()
		.then(function(prefix) {
			inst.tr.set(Buffer.concat([prefix, fdb.tuple.pack([self.dirIndex])]), directory.key());
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_LOG_DIRECTORY') {
		inst.pop()
		.then(function(prefix) {
			var exists;
			return directory.exists(inst.tr)
			.then(function(e) {
				exists = e;
				if(exists)
					return directory.list(inst.tr);
				else
					return [];
			})
			.then(function(children) {
				var logSubspace = new fdb.Subspace([self.dirIndex], prefix);
				inst.tr.set(logSubspace.get('path'), fdb.tuple.pack(directory.getPath()));
				inst.tr.set(logSubspace.get('layer'), fdb.tuple.pack([directory.getLayer()]));
				inst.tr.set(logSubspace.get('exists'), fdb.tuple.pack([exists ? 1 : 0]));
				inst.tr.set(logSubspace.get('children'), fdb.tuple.pack(children));
			});
		})(promiseCb);
	}
	else if(inst.op === 'DIRECTORY_STRIP_PREFIX') {
		inst.pop()
		.then(function(str) {
			if(!fdbUtil.buffersEqual(fdb.buffer(str).slice(0, directory.key().length), directory.key()))
				throw new Error('String ' + str + ' does not start with raw prefix ' + directory.key());

			inst.push(str.slice(directory.key().length));
		})(promiseCb);
	}
	else {
		throw new Error('Unknown op: ' + inst.op);
	}
};

module.exports = DirectoryExtension;
