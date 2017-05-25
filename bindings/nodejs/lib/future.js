/*
 * future.js
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
var semver = require('semver');

function isFunction(f) {
	return typeof(f) == 'function';
}

function isObject(o) {
	return o === Object(o);
}

var resolvePromise = function(promise, value) {
	var called = false;
	try {
		if(promise === value)
			promise._state.reject(new TypeError('promise.then cannot be fulfilled with itself as the argument.'));

		if(isObject(value)) {
			var then = value.then;
			if(isFunction(then)) {
				then.call(value, function(res) {
					if(!called) {
						called = true;
						resolvePromise(promise, res, promise);
					}
				}, function(err) {
					if(!called) {
						called = true;
						promise._state.reject(err);
					}
				});
			}
			else
				promise._state.fulfill(value);
		}
		else
			promise._state.fulfill(value);
	}
	catch(error) {
		if(!called)
			promise._state.reject(error);
	}
};

var FuturePrototype = {
	cancel: function() {
		//cancel is not implemented for most futures
	},

	then: function(onFulfilled, onRejected) {
		var self = this;
		var future = create();
		this._state.addCallback(function(err, res) {
			var setImmediateFunc;
			if(semver.satisfies(process.version, '>=0.10.0')) {
				setImmediateFunc = setImmediate;
			}
			else {
				setImmediateFunc = process.nextTick;
			}
			setImmediateFunc(function() {
				try {
					if(self._state.rejected) {
						if(isFunction(onRejected))
							res = onRejected(err);
						else {
							future._state.reject(err);
							return;
						}
					}
					else if(isFunction(onFulfilled))
						res = onFulfilled(res);

					resolvePromise(future, res);
				}
				catch(error) {
					future._state.reject(error);
				}
			});
		});

		return future;
	},

	"catch": function(onRejected) {
		this.then(undefined, onRejected);
	}
};

FuturePrototype.__proto__ = Function.__proto__;

var FutureState = function() {
	this.callbacks = [];
	this.fulfilled = false;
	this.rejected = false;
};

FutureState.prototype.triggerCallbacks = function() {
	for(var i = 0; i < this.callbacks.length; ++i)
		this.callbacks[i](this.error, this.value);

	this.callbacks = [];
};

FutureState.prototype.addCallback = function(cb) {
	if(!this.rejected && !this.fulfilled)
		this.callbacks.push(cb);
	else
		cb(this.error, this.value);
};

FutureState.prototype.fulfill = function(value) {
	if(!this.fulfilled && !this.rejected) {
		this.fulfilled = true;
		this.value = value;
		this.triggerCallbacks();
	}
};

FutureState.prototype.reject = function(reason) {
	if(!this.fulfilled && !this.rejected) {
		this.rejected = true;
		this.error = reason;
		this.triggerCallbacks();
	}
};

var getFutureCallback = function(futureState) {
	return function(err, val) {
		if(err)
			futureState.reject(err);
		else
			futureState.fulfill(val);
	};
};

var create = function(func, cb) {
	if(cb)
		func(cb);
	else {
		// This object is used to break a reference cycle with C++ objects
		var futureState = new FutureState();

		var future = function(callback) {
			if(typeof callback === 'undefined')
				return future;

			future.then(function(val) { callback(undefined, val); }, callback);
		};

		future._state = futureState;
		future.__proto__ = FuturePrototype;

		if(func)
			func.call(future, getFutureCallback(futureState));

		return future;
	}
};

var resolve = function(value) {
	var f = create();
	f._state.fulfill(value);
	return f;
};

var reject = function(reason) {
	var f = create();
	f._state.reject(reason);
	return f;
};

var all = function(futures) {
	var future = create(function(futureCb) {
		var count = futures.length;

		if(count === 0)
			futureCb(undefined, []);

		var successCallback = function() {
			if(--count === 0)
				futureCb(undefined, futures.map(function(f) { return f._state.value; }));
		};

		for(var i = 0; i < futures.length; ++i) {
			if(futures[i] && isFunction(futures[i].then))
				futures[i].then(successCallback, futureCb);
			else
				successCallback();
		}
	});

	return future;
};

var race = function(futures) {
	var future = create(function(futureCb) {
		var successCallback = function(val) {
			futureCb(undefined, val);
		};

		for(var i = 0; i < futures.length; ++i) {
			if(futures[i] && isFunction(futures[i].then))
				futures[i].then(successCallback, futureCb);
			else {
				futureCb(undefined, futures[i]);
				break;
			}
		}
	});

	return future;
};

module.exports = {
	FuturePrototype: FuturePrototype,
	create: create,
	resolve: resolve,
	reject: reject,
	all: all,
	race: race
};

