/*
 * subspace.js
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
var fdbUtil = require('./fdbUtil');
var tuple = require('./tuple');

var Subspace = function(prefixArray, rawPrefix) {
	if(typeof rawPrefix === 'undefined')
		rawPrefix = new Buffer(0);
	if(typeof prefixArray === 'undefined')
		prefixArray = [];

	rawPrefix = fdbUtil.keyToBuffer(rawPrefix);
	var packed = tuple.pack(prefixArray);

	this.rawPrefix = Buffer.concat([rawPrefix, packed], rawPrefix.length + packed.length);
};

Subspace.prototype.key = function() {
	return this.rawPrefix;
};

Subspace.prototype.pack = function(arr) {
	var packed = tuple.pack(arr);
	return Buffer.concat([this.rawPrefix, packed], this.rawPrefix.length + packed.length) ;
};

Subspace.prototype.unpack = function(key) {
	key = fdbUtil.keyToBuffer(key);
	if(!this.contains(key))
		throw new Error('Cannot unpack key that is not in subspace.');

	return tuple.unpack(key.slice(this.rawPrefix.length));
};

Subspace.prototype.range = function(arr) {
	if(typeof arr === 'undefined')
		arr = [];

	var range = tuple.range(arr);
	return {
		begin: Buffer.concat([this.rawPrefix, range.begin], this.rawPrefix.length + range.begin.length),
		end: Buffer.concat([this.rawPrefix, range.end], this.rawPrefix.length + range.end.length)
	};
};

Subspace.prototype.contains = function(key) {
	key = fdbUtil.keyToBuffer(key);
	return key.length >= this.rawPrefix.length && fdbUtil.buffersEqual(key.slice(0, this.rawPrefix.length), this.rawPrefix);
};

Subspace.prototype.get = function(item) {
	return this.subspace([item]);
};

Subspace.prototype.subspace = function(arr) {
	return new Subspace(arr, this.rawPrefix);
};

Subspace.prototype.asFoundationDBKey = function() {
	return this.key();
};

module.exports = Subspace;
