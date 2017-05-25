"use strict";

var fdb = require('../lib/fdb.js').apiVersion(parseInt(process.argv[3]));
var fdbUtil = require('../lib/fdbUtil.js');
var dirUtil = require('./directory_util.js');

var StreamlineDirectoryExtension = function() {
	this.dirList = [fdb.directory];
	this.dirIndex = 0;
	this.errorIndex = 0;
};

StreamlineDirectoryExtension.prototype.processInstruction = function(inst, _) {
	var directory = this.dirList[this.dirIndex];

	try {
		//console.log(inst.op);

		if(inst.op === 'DIRECTORY_CREATE_SUBSPACE') {
			var path = dirUtil.popTuples(inst)(_);
			var rawPrefix = inst.pop()(_);
			this.dirList.push(new fdb.Subspace(path, rawPrefix));
		}
		else if(inst.op === 'DIRECTORY_CREATE_LAYER') {
			var params = inst.pop({count: 3})(_);
			if(this.dirList[params[0]] === null || this.dirList[params[1]] === null)
				this.dirList.push(null);
			else {
				this.dirList.push(new fdb.DirectoryLayer({ nodeSubspace: this.dirList[params[0]], 
														   contentSubspace: this.dirList[params[1]],
														   allowManualPrefixes: params[2] }));
			}
		}
		else if(inst.op === 'DIRECTORY_CHANGE') {
			var index = inst.pop()(_);
			if(this.dirList[index] === null)
				this.dirIndex = this.errorIndex;
			else
				this.dirIndex = index;
		}
		else if(inst.op === 'DIRECTORY_SET_ERROR_INDEX') {
			this.errorIndex = inst.pop()(_);
		}
		else if(inst.op === 'DIRECTORY_CREATE_OR_OPEN') {
			var path = dirUtil.popTuples(inst)(_);
			var layer = inst.pop()(_);
			var dir = directory.createOrOpen(inst.tr, path, {'layer': layer || undefined})(_);
			this.dirList.push(dir);
		}
		else if(inst.op === 'DIRECTORY_CREATE') {
			var path = dirUtil.popTuples(inst)(_);
			var params = inst.pop({count: 2})(_);
			var dir = directory.create(inst.tr, path, {'layer': params[0] || undefined, 'prefix': params[1] || undefined})(_);
			this.dirList.push(dir);
		}
		else if(inst.op === 'DIRECTORY_OPEN') {
			var path = dirUtil.popTuples(inst)(_);
			var layer = inst.pop()(_);
			var dir = directory.open(inst.tr, path, {'layer': layer || undefined})(_);
			this.dirList.push(dir);
		}
		else if(inst.op === 'DIRECTORY_MOVE') {
			var paths = dirUtil.popTuples(inst, 2)(_);
			var movedDir = directory.move(inst.tr, paths[0], paths[1])(_);
			this.dirList.push(movedDir);
		}
		else if(inst.op === 'DIRECTORY_MOVE_TO') {
			var newAbsolutePath = dirUtil.popTuples(inst)(_);
			var movedDir = directory.moveTo(inst.tr, newAbsolutePath)(_);
			this.dirList.push(movedDir);
		}
		else if(inst.op === 'DIRECTORY_REMOVE') {
			var count = inst.pop()(_);
			var path = dirUtil.popTuples(inst, count)(_);
			directory.remove(inst.tr, path)(_);
		}
		else if(inst.op === 'DIRECTORY_REMOVE_IF_EXISTS') {
			var count = inst.pop()(_);
			var path = dirUtil.popTuples(inst, count)(_);
			directory.removeIfExists(inst.tr, path)(_);
		}
		else if(inst.op === 'DIRECTORY_LIST') {
			var count = inst.pop()(_);
			var path = dirUtil.popTuples(inst, count)(_);
			var children = directory.list(inst.tr, path)(_);
			inst.push(fdb.tuple.pack(children));
		}
		else if(inst.op === 'DIRECTORY_EXISTS') {
			var count = inst.pop()(_);
			var path = dirUtil.popTuples(inst, count)(_);
			var exists = directory.exists(inst.tr, path)(_);
			inst.push(exists ? 1 : 0);
		}
		else if(inst.op === 'DIRECTORY_PACK_KEY') {
			var keyTuple = dirUtil.popTuples(inst)(_);
			inst.push(directory.pack(keyTuple));
		}
		else if(inst.op === 'DIRECTORY_UNPACK_KEY') {
			var key = inst.pop()(_);
			var tup = directory.unpack(key);
			for(var i = 0; i < tup.length; ++i)
				inst.push(tup[i]);
		}
		else if(inst.op === 'DIRECTORY_RANGE') {
			var tup = dirUtil.popTuples(inst)(_);
			var rng = directory.range(tup);
			inst.push(rng.begin);
			inst.push(rng.end);
		}
		else if(inst.op === 'DIRECTORY_CONTAINS') {
			var key = inst.pop()(_);
			inst.push(directory.contains(key) ? 1 : 0);
		}
		else if(inst.op === 'DIRECTORY_OPEN_SUBSPACE') {
			var path = dirUtil.popTuples(inst)(_);
			this.dirList.push(directory.subspace(path));
		}
		else if(inst.op === 'DIRECTORY_LOG_SUBSPACE') {
			var prefix = inst.pop()(_);
			inst.tr.set(Buffer.concat([prefix, fdb.tuple.pack([this.dirIndex])]), directory.key());
		}
		else if(inst.op === 'DIRECTORY_LOG_DIRECTORY') {
			var prefix = inst.pop()(_);
			var exists = directory.exists(inst.tr)(_);
			var children = exists ? directory.list(inst.tr)(_) : [];
			var logSubspace = new fdb.Subspace([this.dirIndex], prefix);
			inst.tr.set(logSubspace.get('path'), fdb.tuple.pack(directory.getPath()));
			inst.tr.set(logSubspace.get('layer'), fdb.tuple.pack([directory.getLayer()]));
			inst.tr.set(logSubspace.get('exists'), fdb.tuple.pack([exists ? 1 : 0]));
			inst.tr.set(logSubspace.get('children'), fdb.tuple.pack(children));
		}
		else if(inst.op === 'DIRECTORY_STRIP_PREFIX') {
			var str = inst.pop()(_);
			if(!fdbUtil.buffersEqual(fdb.buffer(str).slice(0, directory.key().length), directory.key()))
				throw new Error('String ' + str + ' does not start with raw prefix ' + directory.key());

			inst.push(str.slice(directory.key().length));
		}
		else {
			throw new Error('Unknown op: ' + inst.op);
		}
	}
	catch(err) {
		dirUtil.pushError(this, inst, err);
	}
};

module.exports = StreamlineDirectoryExtension;
