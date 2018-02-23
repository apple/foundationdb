/*
 * util.js
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

function Stack() {
	this.stack = [];
}

Stack.prototype.length = function() {
	return this.stack.length;
};

Stack.prototype.get = function(index) {
	return this.stack[index];
};

Stack.prototype.set = function(index, val) {
	this.stack[index] = val;
};

Stack.prototype.push = function(instructionIndex, item, isFuture) {
	if(typeof isFuture === 'undefined')
			isFuture = false;
	
	this.pushEntry({ instructionIndex: instructionIndex, item: item, isFuture: isFuture });
};

Stack.prototype.pushEntry = function(entry) {
	this.stack.push(entry);
};

Stack.prototype.pop = function(options, callback) {
	var self = this;
	return fdb.future.create(function(futureCb) {
		if(typeof options === 'undefined')
			options = {};

		var count = options.count;
		if(typeof count === 'undefined')
			count = 1;

		var params = self.stack.slice(self.stack.length-count).reverse();
		self.stack = self.stack.slice(0, self.stack.length-count);

		var index = 0;

		var itemCallback = function(err, val) {
			if(err) {
				//console.log(err);
				params[index].item = fdb.tuple.pack([fdb.buffer('ERROR'), fdb.buffer(err.code.toString())]);
			}
			else if(val)
				params[index].item = val;
			else
				params[index].item = fdb.buffer('RESULT_NOT_PRESENT');

			params[index].isFuture = false;

			if(!options.withMetadata)
				params[index] = params[index].item;

			index++;
			processNext();
		};

		var processNext = function() {
			while(true) {
				if(index >= params.length) {
					if(typeof options.count === 'undefined')
						futureCb(undefined, params[0]);
					else
						futureCb(undefined, params);

					return;
				}

				if(params[index].isFuture) {
					params[index].item(itemCallback);
					return;
				}

				if(!options.withMetadata)
					params[index] = params[index].item;

				index++;
			}
		};

		processNext();
	})(callback);
};

Stack.prototype.popEntry = function() {
	return this.stack.pop();
};

function Context(db, prefix, processInstruction, directoryExtension) {
	var range = fdb.tuple.range([fdb.buffer(prefix)]);

	this.prefix = prefix;
	this.stack = new Stack();
	this.db = db;
	this.next = range.begin;
	this.end = range.end;
	this.processInstruction = processInstruction;
	this.instructionIndex = -1;
	this.directoryExtension = directoryExtension;
	this.trName = prefix;
}

Context.trMap = {}

Context.prototype.newTransaction = function() {
	Context.trMap[this.trName] = this.db.createTransaction();
};

Context.prototype.switchTransaction = function(name) {
	this.trName = name;
	if(typeof Context.trMap[this.trName] === 'undefined') {
		this.newTransaction();
	}
};

Context.prototype.updateResults = function(results) {
	this.ops = results;
	this.current = 0;
	this.next = fdb.KeySelector.firstGreaterThan(results[results.length-1].key);
};

var issueInstruction = function(context, cb) {
	try {
		var tokens = fdb.tuple.unpack(context.ops[context.current].value);
		var op = tokens[0].toString();

		var snapshotStr = '_SNAPSHOT';
		var databaseStr = '_DATABASE';

		var isSnapshot = endsWith(op, snapshotStr);
		var isDatabase = endsWith(op, databaseStr);

		var tr = Context.trMap[context.trName];
		if(isSnapshot) {
			op = op.substr(0, op.length - snapshotStr.length);
			tr = tr.snapshot;
		}
		else if(isDatabase) {
			op = op.substr(0, op.length - databaseStr.length);
			tr = context.db;
		}

		var inst = new Instruction(context, tr, op, tokens, isDatabase, isSnapshot);
		context.processInstruction(context, inst, cb);
	}
	catch(e) {
		cb(e);
	}
};

Context.prototype.run = function(cb) {
	var self = this;

	function getInstructions(instCb) {
		self.db.doTransaction(function(tr, trCb) {
			tr.getRange(self.next, self.end, { limit: 1000 } ).toArray(function(rangeErr, rangeRes) {
				if(rangeErr) return trCb(rangeErr);

				trCb(undefined, rangeRes);
			});
		}, function(err, rangeRes) {
			if(err) return instCb(err);
			if(rangeRes.length > 0)
				self.updateResults(rangeRes);
			instCb();
		});
	}

	function readAndExecuteInstructions(loopCb) {
		++self.instructionIndex;
		if(!self.ops || ++self.current === self.ops.length) {
			getInstructions(function(err) {
				if(err) return loopCb(err);
				if(self.current < self.ops.length)
					issueInstruction(self, loopCb);
				else
					loopCb(undefined, null); // terminate the loop	
			});
		}
		else
			issueInstruction(self, loopCb);
	}

	util.whileLoop(readAndExecuteInstructions, function(err) {
		if(err) {
			if(self.ops && self.current < self.ops.length)
				console.error('ERROR during operation \'' + self.ops[self.current].value.toString() + '\':');
			else
				console.error('ERROR getting operations:');

			if(err.stack)
				console.error(err.stack);
			else
				console.error(err);
		}

		cb(err);
	});
};

function Instruction(context, tr, op, tokens, isDatabase, isSnapshot) {
	this.context = context;
	this.tr = tr;
	this.op = op;
	this.tokens = tokens;
	this.isDatabase = isDatabase;
	this.isSnapshot = isSnapshot;
}

Instruction.prototype.pop = function(options, callback) {
	return this.context.stack.pop(options, callback);
};

Instruction.prototype.push = function(item, isFuture) {
	this.context.stack.push(this.context.instructionIndex, item, isFuture);
};

function toJavaScriptName(name) {
	name = name.toString().toLowerCase();
	var start = 0;
	while(start < name.length) {
		start = name.indexOf('_', start);
		if(start === -1)
			break;

		name = name.slice(0, start) + name[start+1].toUpperCase() + name.slice(start+2);
	}

	return name.replace(/_/g, '');
}

function startsWith(str, prefixStr) {
	return str.length >= prefixStr.length && str.substr(0, prefixStr.length) === prefixStr;
}

function endsWith(str, endStr) {
	return str.length >= endStr.length && str.substr(str.length - endStr.length) === endStr;
}

module.exports = { 
	Stack: Stack, 
	Context: Context, 
	Instruction: Instruction, 
	toJavaScriptName: toJavaScriptName,
	startsWith: startsWith,
	endsWith: endsWith
};
