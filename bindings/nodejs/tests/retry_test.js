/*
 * retry_test.js
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

var fdb = require('../lib/fdb').apiVersion(200);

function set(db, key, value, cb) {
	db.doTransaction(function(tr, cb2) {
		console.log("setting key");
		tr.set(new Buffer(key), new Buffer(value));
		cb2(null);
	}, cb);
};

function setTxn(tr, key, value, cb) {
	console.log('calling set');
	tr.set(new Buffer(key), new Buffer(value));
	cb();
};
setTxn = fdb.transactional(setTxn);

function getAndClear(db, key, cb) {
	db.doTransaction(function(tr, cb2) {
		console.log("getting key");
		tr.get(new Buffer(key), function(err, res) {
			tr.clear(new Buffer(key));
			//setTimeout(function() { cb2(err, res); }, 6000);
			cb2(err, res);
		});
	}, cb);
};

fdb.open(null, null, function(dbErr, db) {
	console.log("created database", dbErr);
	setTxn(db, 'foo', 'bar', function(err) {
		console.log("Called transactional function", err);
		getAndClear(db, 'foo', function(err, res) {
			console.log("Called get and clear", err, res.toString());
		});
	});
});

