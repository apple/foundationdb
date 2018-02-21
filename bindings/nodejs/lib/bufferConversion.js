/*
 * bufferConversion.js
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

var toBuffer = function(obj) {
	if(Buffer.isBuffer(obj))
		return obj;

	if(obj instanceof ArrayBuffer)
		obj = new Uint8Array(obj);

	if(obj instanceof Uint8Array) {
		var buf = new Buffer(obj.length);
		for(var i = 0; i < obj.length; ++i)
			buf[i] = obj[i];

		return buf;
	}

	if(typeof obj === 'string')
		return new Buffer(obj, 'utf8');

	throw new TypeError('toBuffer function expects a string, buffer, ArrayBuffer, or Uint8Array');
};

toBuffer.fromByteLiteral = function(str) {
	if(typeof str === 'string') {
		var buf = new Buffer(str.length);
		for(var i = 0; i < str.length; ++i) {
			if(str[i] > 255)
				throw new RangeError('fromByteLiteral string argument cannot have codepoints larger than 1 byte');
			buf[i] = str.charCodeAt(i);
		}
		return buf;
	}
	else
		throw new TypeError('fromByteLiteral function expects a string');
};

toBuffer.toByteLiteral = function(buf) {
	if(Buffer.isBuffer(buf))
		return String.fromCharCode.apply(null, buf);
	else
		throw new TypeError('toByteLiteral function expects a buffer');
};

toBuffer.printable = function(buf) {
	buf = toBuffer(buf);
	var out = '';
	for(var i = 0; i < buf.length; ++i) {
		if(buf[i] >= 32 && buf[i] < 127 && buf[i] !== 92)
			out += String.fromCharCode(buf[i]);
		else if(buf[i] === 92)
			out += '\\\\';
		else {
			var str = buf[i].toString(16);
			out += '\\x';
			if(str.length == 1)
				out += '0';
			out += str;
		}
	}

	return out;
};

module.exports = toBuffer;
