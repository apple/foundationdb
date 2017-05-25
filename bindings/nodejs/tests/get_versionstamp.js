/*
 * get_versionstamp.js
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

var bufferEqual = function (a, b) {
    if (!Buffer.isBuffer(a)) return undefined;
    if (!Buffer.isBuffer(b)) return undefined;
    if (typeof a.equals === 'function') return a.equals(b);
    if (a.length !== b.length) return false;

    for (var i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
    }

    return true;
};

var fdb = require('../lib/fdb').apiVersion(410);

var db = fdb.open(null, null)

var tr = db.createTransaction();

tr.getVersionstamp(function(error, vs) {
    db.get('foo', function(error, val) {
        if(bufferEqual(val, vs))
            console.log('versionstamps match!')
        else {
            console.log("versionstamps don't match!")
            console.log("database verionstamp: " + val)
            console.log("transaction versionstamp: " + vs)
        }
    });
});

tr.setVersionstampedValue('foo', 'blahblahbl')

tr.commit(function(err) {
	if(err)
		console.log('commit error', err);

	console.log(tr.getCommittedVersion())
});
