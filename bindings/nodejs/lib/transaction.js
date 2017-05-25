/*
 * transaction.js
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

var future = require('./future');
var KeySelector = require('./keySelector');
var rangeIterator = require('./rangeIterator');
var buffer = require('./bufferConversion');
var FDBError = require('./error');
var fdb = require('./fdbModule');
var fdbUtil = require('./fdbUtil');

function addReadOperations(object, snapshot) {
	object.prototype.get = function(key, cb) {
		var tr = this.tr;
		key = fdbUtil.keyToBuffer(key);

		return future.create(function(futureCb) {
			tr.get(key, snapshot, futureCb);
		}, cb);
	};

	object.prototype.getKey = function(keySelector, cb) {
		var tr = this.tr;
		return future.create(function(futureCb) {
			tr.getKey(keySelector.key, keySelector.orEqual, keySelector.offset, snapshot, futureCb);
		}, cb);
	};

	object.prototype.getRange = function(start, end, options) {
		if(!KeySelector.isKeySelector(start))
			start = KeySelector.firstGreaterOrEqual(start);
		if(!KeySelector.isKeySelector(end))
			end = KeySelector.firstGreaterOrEqual(end);

		return rangeIterator(this.tr, start, end, options, snapshot);
	};

	object.prototype.getRangeStartsWith = function(prefix, options) {
		prefix = fdbUtil.keyToBuffer(prefix);
		return this.getRange(prefix, fdbUtil.strinc(prefix), options, snapshot);
	};

	object.prototype.getReadVersion = function(cb) {
		var tr = this.tr;
		return future.create(function(futureCb) {
			tr.getReadVersion(futureCb, snapshot);
		}, cb);
	};
}

var atomic = function(tr, op) {
	return function(key, value) { fdb.atomic[op].call(tr, fdbUtil.keyToBuffer(key), fdbUtil.valueToBuffer(value)); };
};

var Transaction = function(db, tr) {
	this.db = db;
	this.tr = tr;

	this.options = tr.options;
	this.snapshot = new Transaction.SnapshotTransaction(tr);

	for(var op in fdb.atomic)
		this[op] = atomic(tr, op);
};

Transaction.SnapshotTransaction = function(tr) {
	this.tr = tr;
};

addReadOperations(Transaction, false);
addReadOperations(Transaction.SnapshotTransaction, true);

Transaction.prototype.doTransaction = function(func, cb) {
	var self = this;
	return future.create(function(futureCb) {
		func(self, futureCb);
	}, cb);
};

Transaction.prototype.set = function(key, value) {
	key = fdbUtil.keyToBuffer(key);
	value = fdbUtil.valueToBuffer(value);

	this.tr.set(key, value);
};

Transaction.prototype.clear = function(key) {
	key = fdbUtil.keyToBuffer(key);

	this.tr.clear(key);
};

Transaction.prototype.clearRange = function(start, end) {
	start = fdbUtil.keyToBuffer(start);
	end = fdbUtil.keyToBuffer(end);

	this.tr.clearRange(start, end);
};

Transaction.prototype.clearRangeStartsWith = function(prefix) {
	prefix = fdbUtil.keyToBuffer(prefix);
	this.clearRange(prefix, fdbUtil.strinc(prefix));
};

Transaction.prototype.watch = function(key) {
	key = fdbUtil.keyToBuffer(key);

	var self = this;
	var watchFuture = future.create(function(futureCb) {
		// 'this' is the future that is being created.
		// We set its cancel method to cancel the watch.
		this._watch = self.tr.watch(key, futureCb);
		this.cancel = function() { this._watch.cancel(); };
	});

	return watchFuture;
};

Transaction.prototype.addReadConflictRange = function(start, end) {
	start = fdbUtil.keyToBuffer(start);
	end = fdbUtil.keyToBuffer(end);
	this.tr.addReadConflictRange(start, end);
};

Transaction.prototype.addReadConflictKey = function(key) {
	key = fdbUtil.keyToBuffer(key);
	this.tr.addReadConflictRange(key, Buffer.concat([key, buffer.fromByteLiteral('\x00')], key.length + 1));
};

Transaction.prototype.addWriteConflictRange = function(start, end) {
	start = fdbUtil.keyToBuffer(start);
	end = fdbUtil.keyToBuffer(end);

	this.tr.addWriteConflictRange(start, end);
};

Transaction.prototype.addWriteConflictKey = function(key) {
	key = fdbUtil.keyToBuffer(key);
	this.tr.addWriteConflictRange(key, Buffer.concat([key, buffer.fromByteLiteral('\x00')], key.length + 1));
};

Transaction.prototype.commit = function(cb) {
	var tr = this.tr;
	return future.create(function(futureCb) {
		tr.commit(futureCb);
	}, cb);
};

Transaction.prototype.onError = function(fdbError, cb) {
	var tr = this.tr;
	return future.create(function(futureCb) {
		if(fdbError instanceof FDBError)
			tr.onError(fdbError.code, futureCb);
		else
			futureCb(fdbError, null);
	}, cb);
};

Transaction.prototype.reset = function() {
	this.tr.reset();
};

Transaction.prototype.setReadVersion = function(version) {
	this.tr.setReadVersion(version);
};

Transaction.prototype.getCommittedVersion = function() {
	return this.tr.getCommittedVersion();
};

Transaction.prototype.getVersionstamp = function(cb) {
	var tr = this.tr;
	return future.create(function(futureCb) {
		tr.getVersionstamp(futureCb);
	}, cb);
};

Transaction.prototype.cancel = function() {
	this.tr.cancel();
};

module.exports = Transaction;

