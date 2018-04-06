/*
 * retryDecorator.js
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

var Database = require('./database');
var Transaction = require('./transaction');

module.exports = function(func) {
	return function(db) {
		var self = this;

		if(db instanceof Database) {
			var cb = arguments[func.length - 1];

			if(typeof cb !== "undefined" && !(cb instanceof Function))
				throw new TypeError("fdb.transactional function must declare a callback function as last argument");
			else {
				var args = Array.prototype.slice.call(arguments);
				return db.doTransaction(function(tr, innerCb) {
					args[0] = tr;
					args[func.length - 1] = innerCb;
					func.apply(self, args);
				})(cb);
			}
		}
		else if(db instanceof Transaction || db instanceof Transaction.SnapshotTransaction)
			return func.apply(self, arguments);
		else
			throw new TypeError("fdb.transactional function must pass a Database, Transaction, or SnapshotTransaction as first argument");
	};
};
