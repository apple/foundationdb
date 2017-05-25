/*
 * tuple.js
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

var assert = require('assert');
var buffer = require('./bufferConversion');
var fdbUtil = require('./fdbUtil');

var sizeLimits = new Array(8);

function setupSizeLimits() {
	sizeLimits[0] = 1;
	for(var i = 1; i < sizeLimits.length; i++) {
		sizeLimits[i] = sizeLimits[i-1] * 256;
		sizeLimits[i-1] -= 1;
	}
	sizeLimits[7] -= 1;
}

setupSizeLimits();

var maxInt = Math.pow(2, 53) - 1;
var minInt = -Math.pow(2, 53);

var nullByte = new Buffer('00', 'hex');

function findNullBytes(buf, pos, searchForTerminators) {
	var nullBytes = [];

	var found;
	for(pos; pos < buf.length; ++pos) {
		if(searchForTerminators && found && buf[pos] !== 255) {
			break;
		}

		found = false;
		if(buf[pos] === 0) {
			found = true;
			nullBytes.push(pos);
		}
	}

	if(!found && searchForTerminators) {
		nullBytes.push(buf.length);
	}

	return nullBytes;
}

function encode(item, buf, pos) {
	var encodedString;
	if(typeof item === 'undefined')
		throw new TypeError('Packed element cannot be undefined');

	else if(item === null)
		return nullByte;

	//byte string or unicode
	else if(Buffer.isBuffer(item) || item instanceof ArrayBuffer || item instanceof Uint8Array || typeof item === 'string') {
		var unicode = typeof item === 'string';

		if(unicode) {
			item = new Buffer(item, 'utf8');
		}
		else {
			item = buffer(item);
		}

		var nullBytes = findNullBytes(item, 0);

		encodedString = new Buffer(2 + item.length + nullBytes.length);
		encodedString[0] = unicode ? 2 : 1;

		var srcPos = 0;
		var targetPos = 1;
		for(var i = 0; i < nullBytes.length; ++i) {
			item.copy(encodedString, targetPos, srcPos, nullBytes[i]+1);
			targetPos += nullBytes[i]+1 - srcPos;
			srcPos = nullBytes[i]+1;
			encodedString[targetPos++] = 255;
		}

		item.copy(encodedString, targetPos, srcPos);
		encodedString[encodedString.length-1] = 0;

		return encodedString;
	}

	//64-bit integer
	else if(item % 1 === 0) {
		var negative = item < 0;
		var posItem = Math.abs(item);

		var length = 0;
		for(; length < sizeLimits.length; ++length) {
			if(posItem <= sizeLimits[length])
				break;
		}

		if(item > maxInt || item < minInt)
			throw new RangeError('Cannot pack signed integer larger than 54 bits');

		var prefix = negative ? 20 - length : 20 + length;

		var outBuf = new Buffer(length+1);
		outBuf[0] = prefix;
		for(var byteIdx = length-1; byteIdx >= 0; --byteIdx) {
			var b = posItem & 0xff;
			if(negative)
				outBuf[byteIdx+1] = ~b;
			else {
				outBuf[byteIdx+1] = b;
			}

			posItem = (posItem - b) / 0x100;
		}

		return outBuf;
	}

	else
		throw new TypeError('Packed element must either be a string, a buffer, an integer, or null');
}

function pack(arr) {
	if(!(arr instanceof Array))
		throw new TypeError('fdb.tuple.pack must be called with a single array argument');

	var totalLength = 0;

	var outArr = [];
	for(var i = 0; i < arr.length; ++i) {
		outArr.push(encode(arr[i]));
		totalLength += outArr[i].length;
	}

	return Buffer.concat(outArr, totalLength);
}

function decodeNumber(buf, offset, bytes) {
	var negative = bytes < 0;
	bytes = Math.abs(bytes);

	var num = 0;
	var mult = 1;
	var odd;
	for(var i = bytes-1; i >= 0; --i) {
		var b = buf[offset+i];
		if(negative)
			b = -(~b & 0xff);

		if(i == bytes-1)
			odd = b & 0x01;

		num += b * mult;
		mult *= 0x100;
	}

	if(num > maxInt || num < minInt || (num === minInt && odd))
		throw new RangeError('Cannot unpack signed integers larger than 54 bits');

	return num;
}

function decode(buf, pos) {
	var code = buf[pos];
	var value;

	if(code === 0) {
		value = null;
		pos++;
	}
	else if(code === 1 || code === 2) {
		var nullBytes = findNullBytes(buf, pos+1, true);

		var start = pos+1;
		var end = nullBytes[nullBytes.length-1];

		if(code === 2 && nullBytes.length === 1) {
			value = buf.toString('utf8', start, end);
		}
		else {
			value = new Buffer(end-start-(nullBytes.length-1));
			var valuePos = 0;

			for(var i=0; i < nullBytes.length && start < end; ++i) {
				buf.copy(value, valuePos, start, nullBytes[i]);
				valuePos += nullBytes[i] - start;
				start = nullBytes[i] + 2;
				if(start <= end) {
					value[valuePos++] = 0;
				}
			}

			if(code === 2)
				value = value.toString('utf8');
		}

		pos = end + 1;
	}
	else if(Math.abs(code-20) <= 7) {
		if(code === 20)
			value = 0;
		else
			value = decodeNumber(buf, pos+1, code-20);

		pos += Math.abs(20-code) + 1;
	}
	else if(Math.abs(code-20) <= 8)
		throw new RangeError('Cannot unpack signed integers larger than 54 bits');
	else
		throw new TypeError('Unknown data type in DB: ' + buf + ' at ' + pos);

	return { pos: pos, value: value };
}

function unpack(key) {
	var res = { pos: 0 };
	var arr = [];

	key = fdbUtil.keyToBuffer(key);

	while(res.pos < key.length) {
		res = decode(key, res.pos);
		arr.push(res.value);
	}

	return arr;
}

function range(arr) {
	var packed = pack(arr);
	return { begin: Buffer.concat([packed, nullByte]), end: Buffer.concat([packed, new Buffer('ff', 'hex')]) };
}

module.exports = {pack: pack, unpack: unpack, range: range};
