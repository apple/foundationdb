/*
 * get_range.js
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

var db = fdb.open(null, null)

var tr = db.createTransaction();

for(var i = 0; i < 10000; i++)
	tr.set('foo' + i, 'bar' + i)

tr.commit(function(err) {
	if(err)
		console.log('commit error', err);

	console.log('get range: foo-fooa');
	var itr = tr.getRange('foo', 'fooa', null);

	itr.forEach(
		function(val, cb) {
			console.log(val.key.toString(), val.value.toString());
			cb();
		},
		function(err, res) {
			if(err)
				console.log(err);
			else {
				itr.forEach(
					function(val, cb) {
						console.log('pass2: ' + val.key.toString(), val.value.toString());
						cb();
					},
					function(err, res) {
						if(err)
							console.log(err);
						else { }
					}
				);
			}
		}
	);
});


