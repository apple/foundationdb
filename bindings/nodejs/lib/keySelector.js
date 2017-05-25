/*
 * keySelector.js
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

var fdbUtil = require('./fdbUtil');

var KeySelector = function(key, orEqual, offset) {
	this.key = fdbUtil.keyToBuffer(key);
	this.orEqual = orEqual;
	this.offset = offset;
};

KeySelector.prototype.next = function() {
	return this.add(1);
};

KeySelector.prototype.prev = function() {
	return this.add(-1);
};

KeySelector.prototype.add = function(addOffset) {
	return new KeySelector(this.key, this.orEqual, this.offset + addOffset);
};

KeySelector.isKeySelector = function(sel) {
	return sel instanceof KeySelector;
};

KeySelector.lastLessThan = function(key) {
	return new KeySelector(key, false, 0);
};

KeySelector.lastLessOrEqual = function(key) {
	return new KeySelector(key, true, 0);
};

KeySelector.firstGreaterThan = function(key) {
	return new KeySelector(key, true, 1);
};

KeySelector.firstGreaterOrEqual = function(key) {
	return new KeySelector(key, false, 1);
};

module.exports = KeySelector;

