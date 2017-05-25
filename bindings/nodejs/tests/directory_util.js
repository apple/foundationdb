/*
 * directory_util.js
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
var fdb = require('../lib/fdb.js').apiVersion(parseInt(process.argv[3]));
var util = require('../lib/fdbUtil.js');

var opsThatCreateDirs = [
    'DIRECTORY_CREATE_SUBSPACE',
    'DIRECTORY_CREATE_LAYER',
    'DIRECTORY_CREATE_OR_OPEN',
    'DIRECTORY_CREATE',
    'DIRECTORY_OPEN',
    'DIRECTORY_MOVE',
    'DIRECTORY_MOVE_TO',
    'DIRECTORY_OPEN_SUBSPACE'
];

var popTuples = function(inst, num, cb) {
	if(typeof num === 'undefined')
		num = 1;
	return fdb.future.create(function(futureCb) {
		var tuples = [];
		if(num === 0) return futureCb();
		util.whileLoop(function(loopCb) {
			inst.pop()
			.then(function(count) {
				return inst.pop({count: count})
				.then(function(tuple) {
					tuples.push(tuple);
					if(--num === 0)
						return null;
				});
			})(loopCb);
		}, function() {
			if(tuples.length == 1)
				futureCb(undefined, tuples[0]);
			else 
				futureCb(undefined, tuples);
		});
	})(cb);
};

var pushError = function(self, inst, err) {
	if(err) {
		//console.log(err.toString());
		//console.log(err.stack);
		inst.push(fdb.buffer('DIRECTORY_ERROR'));

		if(opsThatCreateDirs.indexOf(inst.op) >= 0)
			self.dirList.push(null);
	}

	return err;
};

module.exports = {
	popTuples: popTuples,
	pushError: pushError
};
