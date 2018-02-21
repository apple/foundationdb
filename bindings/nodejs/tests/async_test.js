/*
 * async_test.js
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

fdb.open(null, null, function(dbErr, dbVal) {
	fdb.open(null, null, function(dbErr, dbVal) {
		if(dbVal == null)
			console.log("database is null");
		console.log("created database", dbErr);
		console.log(JSON.stringify(dbVal));
		var tr = dbVal.createTransaction();
		console.log("created transaction");

		tr.get('foo', function(err, val) {
			console.log("get called", val, err);
			tr.set('foo', 'bar');
			tr.commit(function(err) {
				console.log("commit called", err);
				var x = tr.get('foo');
				x(function(err, val) {
					console.log("get called", val.toString(), err);
					tr.clear('foo')
					tr.commit(function(err) {
						console.log("commit called", err);
						tr.get('foo', function(err, val) {
							console.log("get called", val, err);
						});
					});
				});
			});
		});
	});
});
