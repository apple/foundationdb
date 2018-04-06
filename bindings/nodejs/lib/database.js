/*
 * database.js
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

var Transaction = require('./transaction');
var future = require('./future');
var fdb = require('./fdbModule');
var fdbUtil = require('./fdbUtil');

var onError = function(tr, err, func, cb) {
	tr.onError(err, function(retryErr, retryRes) {
		if(retryErr)
			cb(retryErr, retryRes);
		else
			retryLoop(tr, func, cb);
	});
};

var retryLoop = function(tr, func, cb) {
	func(tr, function(err, res) {
		if(err) {
			onError(tr, err, func, cb);
		}
		else {
			tr.commit(function(commitErr, commitRes) {
				if(commitErr)
					onError(tr, commitErr, func, cb);
				else
					cb(commitErr, res);
			});
		}
	});
};

var atomic = function(db, op) {
	return function(key, value, cb) {
		return db.doTransaction(function(tr, innerCb) {
			fdb.atomic[op].call(tr.tr, fdbUtil.keyToBuffer(key), fdbUtil.valueToBuffer(value));
			innerCb();
		}, cb);
	};
};

var Database = function(_db) {
	this._db = _db;
	this.options = _db.options;

	for(var op in fdb.atomic)
		this[op] = atomic(this, op);
};

Database.prototype.createTransaction = function() {
	return new Transaction(this, this._db.createTransaction());
};

Database.prototype.doTransaction = function(func, cb) {
	var tr = this.createTransaction();

	return future.create(function(futureCb) {
		retryLoop(tr, func, futureCb);
	}, cb);
};

Database.prototype.get = function(key, cb) {
	return this.doTransaction(function(tr, innerCb) {
		tr.get(key, innerCb);
	}, cb);
};

Database.prototype.getKey = function(keySelector, cb) {
	return this.doTransaction(function(tr, innerCb) {
		tr.getKey(keySelector, innerCb);
	}, cb);
};

Database.prototype.getRange = function(start, end, options, cb) {
	return this.doTransaction(function(tr, innerCb) {
		tr.getRange(start, end, options).toArray(innerCb);
	}, cb);
};

Database.prototype.getRangeStartsWith = function(prefix, options, cb) {
	return this.doTransaction(function(tr, innerCb) {
		try {
			tr.getRangeStartsWith(prefix, options).toArray(innerCb);
		}
		catch(e) {
			innerCb(e);
		}
	}, cb);
};

Database.prototype.getAndWatch = function(key, cb) {
	return this.doTransaction(function(tr, innerCb) {
		tr.get(key, function(err, val) {
			if(err)
				innerCb(err);
			else
				innerCb(undefined, { value: val, watch: tr.watch(key) });
		});
	}, cb);
};

Database.prototype.setAndWatch = function(key, value, cb) {
	return this.doTransaction(function(tr, innerCb) {
		tr.set(key, value);
		var watchObj = tr.watch(key);
		innerCb(undefined, { watch: watchObj });
	}, cb);
};

Database.prototype.clearAndWatch = function(key, cb) {
	return this.doTransaction(function(tr, innerCb) {
		tr.clear(key);
		var watchObj = tr.watch(key);
		innerCb(undefined, { watch: watchObj });
	}, cb);
};

Database.prototype.set = function(key, value, cb) {
	return this.doTransaction(function(tr, innerCb) {
		tr.set(key, value);
		innerCb();
	}, cb);
};

Database.prototype.clear = function(key, cb) {
	return this.doTransaction(function(tr, innerCb) {
		tr.clear(key);
		innerCb();
	}, cb);
};

Database.prototype.clearRange = function(start, end, cb) {
	return this.doTransaction(function(tr, innerCb) {
		tr.clearRange(start, end);
		innerCb();
	}, cb);
};

Database.prototype.clearRangeStartsWith = function(prefix, cb) {
	return this.doTransaction(function(tr, innerCb) {
		try {
			tr.clearRangeStartsWith(prefix);
			innerCb();
		}
		catch(e) {
			innerCb(e);
		}
	}, cb);
};

module.exports = Database;
