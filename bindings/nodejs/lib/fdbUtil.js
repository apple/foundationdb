/*
 * fdbUtil.js
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

var buffer = require('./bufferConversion');
var future = require('./future');

var strinc = function(str) {
	var buf = buffer(str);

	var lastNonFFByte;
	for(lastNonFFByte = buf.length-1; lastNonFFByte >= 0; --lastNonFFByte)
		if(buf[lastNonFFByte] != 0xFF)
			break;

	if(lastNonFFByte < 0)
		throw new Error('invalid argument \'' + str + '\': prefix must have at least one byte not equal to 0xFF');

	var copy = new Buffer(lastNonFFByte + 1);
	str.copy(copy, 0, 0, copy.length);
	++copy[lastNonFFByte];

	return copy;
};

var whileLoop = function(func, cb) {
	return future.create(function(futureCb) {
		var calledCallback = true;
		function outer(err, res) {
			if(err || typeof(res) !== 'undefined') {
				futureCb(err, res);
			}
			else if(!calledCallback) {
				calledCallback = true;
			}
			else {
				while(calledCallback) {
					calledCallback = false;
					func(outer);
				}

				calledCallback = true;
			}
		}

		outer();
	}, cb);
};

var keyToBuffer = function(key) {
	if(typeof(key.asFoundationDBKey) == 'function')
		return buffer(key.asFoundationDBKey());

	return buffer(key);
};

var valueToBuffer = function(val) {
	if(typeof(val.asFoundationDBValue) == 'function')
		return buffer(val.asFoundationDBValue());

	return buffer(val);
};

var buffersEqual = function(buf1, buf2) {
	if(!buf1 || !buf2)
		return buf1 === buf2;

	if(buf1.length !== buf2.length)
		return false;

	for(var i = 0; i < buf1.length; ++i)
		if(buf1[i] !== buf2[i])
			return false;

	return true;
};

module.exports = {
	strinc: strinc,
	whileLoop: whileLoop,
	keyToBuffer: keyToBuffer,
	valueToBuffer: valueToBuffer,
	buffersEqual: buffersEqual
};

