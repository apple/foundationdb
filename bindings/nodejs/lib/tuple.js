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
var fdb = require('./fdbModule');
var FDBError = require('./error');

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

var BYTES_CODE    = 0x01;
var STRING_CODE   = 0x02;
var NESTED_CODE   = 0x05;
var INT_ZERO_CODE = 0x14;
var POS_INT_END   = 0x1d;
var NEG_INT_END   = 0x0b;
var FLOAT_CODE    = 0x20;
var DOUBLE_CODE   = 0x21;
var FALSE_CODE    = 0x26;
var TRUE_CODE     = 0x27;
var UUID_CODE     = 0x30;

function validateData(data, length) {
	if(!(data instanceof Buffer) && !(data instanceof Array)) {
		throw new TypeError('Data for FDB tuple type not array or buffer.');
	} else if(data.length != length) {
		throw new RangeError('Data for FDB tuple type has length ' + data.length + ' instead of expected length ' + length);
	}
}

// If encoding and sign bit is 1 (negative), flip all of the bits. Otherwise, just flip sign.
// If decoding and sign bit is 0 (negative), flip all of the bits. Otherwise, just flip sign.
function adjustFloat(data, start, encode) {
	if((encode && (data[start] & 0x80) === 0x80) || (!encode && (data[start] & 0x80) === 0x00)) {
		for(var i = start; i < data.length; i++) {
			data[i] = data[i] ^ 0xff;
		}
	} else {
		data[start] = data[start] ^ 0x80;
	}
}

function Float(value) {
	this.value = value;
	this.toBytes = function () {
		if (this.rawData !== undefined) {
			return this.rawData;
		} else {
			var buf = new Buffer(4);
			buf.writeFloatBE(fdb.toFloat(this.value), 0);
			return buf;
		}
	};
}

Float.fromBytes = function (buf) {
	validateData(buf, 4);
	var f = new Float(buf.readFloatBE(0));
	if(isNaN(f.value)) {
		f.rawData = buf;
	}
	return f;
}

function Double(value) {
	this.value = value;
	this.toBytes = function () {
		if (this.rawData !== undefined) {
			return this.rawData;
		} else {
			var buf = new Buffer(8);
			buf.writeDoubleBE(this.value, 0);
			return buf;
		}
	};
}

Double.fromBytes = function (buf) {
	validateData(buf, 8);
	var d = new Double(buf.readDoubleBE(0));
	if(isNaN(d.value)) {
		d.rawData = buf;
	}
	return d;
}

function UUID(data) {
    if (data.length != 16) {
        // There's a special code for this, so we check it first and throw the error if appropriate.
        throw new FDBError("invalid_uuid_size", 2268);
    }
	validateData(data, 16);
	this.data = new Buffer(data);
}

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
		encodedString[0] = unicode ? STRING_CODE : BYTES_CODE;

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
	else if((typeof item === 'number' || item instanceof Number) && item % 1 === 0) {
		var negative = item < 0;
		var posItem = Math.abs(item);

		var length = 0;
		for(; length < sizeLimits.length; ++length) {
			if(posItem <= sizeLimits[length])
				break;
		}

		if(item > maxInt || item < minInt)
			throw new RangeError('Cannot pack signed integer larger than 54 bits');

		var prefix = negative ? INT_ZERO_CODE - length : INT_ZERO_CODE + length;

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

	// Floats
	else if(item instanceof Float) {
		var outBuf = new Buffer(5);
		outBuf[0] = FLOAT_CODE;
		if (isNaN(item.value) && item.rawData !== undefined) {
			item.rawData.copy(outBuf, 1, 0, 4);
		} else {
			outBuf.writeFloatBE(fdb.toFloat(item.value), 1);
		}
		adjustFloat(outBuf, 1, true);
		return outBuf;
	}

	// Doubles
	else if(item instanceof Double) {
		var outBuf = new Buffer(9);
		outBuf[0] = DOUBLE_CODE;
		if (isNaN(item.value) && item.rawData !== undefined) {
			item.rawData.copy(outBuf, 1, 0, 8);
		} else {
			outBuf.writeDoubleBE(item.value, 1);
		}
		adjustFloat(outBuf, 1, true);
		return outBuf;
	}

	// UUIDs
	else if(item instanceof UUID) {
		var outBuf = new Buffer(17);
		outBuf[0] = UUID_CODE;
		item.data.copy(outBuf, 1);
		return outBuf;
	}

	// booleans
	else if(item instanceof Boolean || typeof item === 'boolean') {
		var outBuf = new Buffer(1);
		var boolItem;
		if(item instanceof Boolean) {
			boolItem = item.valueOf();
		} else {
			boolItem = item;
		}
		if(boolItem) {
			outBuf[0] = TRUE_CODE;
		} else {
			outBuf[0] = FALSE_CODE;
		}
		return outBuf;
	}

	// nested tuples
	else if(item instanceof Array) {
		var totalLength = 2;
		var outArr = [new Buffer('05', 'hex')];
		for(var i = 0; i < item.length; ++i) {
			if(item[i] === null) {
				outArr.push(new Buffer('00ff', 'hex'));
				totalLength += 2;
			} else {
				outArr.push(encode(item[i]));
				totalLength += outArr[i+1].length;
			}
		}
		outArr.push(new Buffer('00', 'hex'))
		return Buffer.concat(outArr, totalLength);
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

function decode(buf, pos, nested) {
	if(typeof nested === 'undefined') nested = false;

	var code = buf[pos];
	var value;

	if(code === 0) {
		value = null;
		if(nested) {
			pos += 2;
		} else {
			pos++;
		}
	}
	else if(code === BYTES_CODE || code === STRING_CODE) {
		var nullBytes = findNullBytes(buf, pos+1, true);

		var start = pos+1;
		var end = nullBytes[nullBytes.length-1];

		if(code === STRING_CODE && nullBytes.length === 1) {
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

			if(code === STRING_CODE)
				value = value.toString('utf8');
		}

		pos = end + 1;
	}
	else if(Math.abs(code-INT_ZERO_CODE) <= 7) {
		if(code === INT_ZERO_CODE)
			value = 0;
		else
			value = decodeNumber(buf, pos+1, code-INT_ZERO_CODE);

		pos += Math.abs(INT_ZERO_CODE-code) + 1;
	}
	else if(Math.abs(code-INT_ZERO_CODE) <= 8)
		throw new RangeError('Cannot unpack signed integers larger than 54 bits');
	else if(code === FLOAT_CODE) {
		var valBuf = new Buffer(4);
		buf.copy(valBuf, 0, pos+1, pos+5);
		adjustFloat(valBuf, 0, false);
		value = Float.fromBytes(valBuf);
		pos += 5;
	}
	else if(code === DOUBLE_CODE) {
		var valBuf = new Buffer(8);
		buf.copy(valBuf, 0, pos+1, pos+9);
		adjustFloat(valBuf, 0, false);
		value = Double.fromBytes(valBuf);
		pos += 9;
	}
	else if(code === UUID_CODE) {
		var valBuf = new Buffer(16);
		buf.copy(valBuf, 0, pos+1, pos+17);
		value = new UUID(valBuf);
		pos += 17;
	}
	else if(code === FALSE_CODE) {
		pos++;
		value = false;
	}
	else if(code === TRUE_CODE) {
		pos++;
		value = true;
	}
	else if(code === NESTED_CODE) {
		pos++;
		value = []
		while (buf[pos] != 0 || pos+1 < buf.length && buf[pos+1] === 0xff) {
			var nestedVal = decode(buf, pos, true)
			pos = nestedVal.pos
			value.push(nestedVal.value)
		}
		pos++;
	}
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

function compare(arr1, arr2) {
	// NOTE: There is built-in comparison function included in 0.11.13 that we might want to switch to.
	var buf1 = pack(arr1);
	var buf2 = pack(arr2);
	var pos = 0;

	while(pos < buf1.length && pos < buf2.length) {
		if(buf1[pos] != buf2[pos]) {
			if(buf1[pos] < buf2[pos]) {
				return -1;
			} else {
				return 1;
			}
		}
		pos += 1;
	}

	// The two arrays begin with a common prefix.
	if(buf1.length < buf2.length) {
		return -1;
	} else if(buf1.length == buf2.length) {
		return 0;
	} else {
		return 1;
	}
}

module.exports = {pack: pack, unpack: unpack, range: range, compare: compare, Float: Float, Double: Double, UUID: UUID};
