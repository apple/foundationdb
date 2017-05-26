/*
 * tester.js
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

#!/usr/bin/env node

"use strict";

//cmd line: node tester.js <test_prefix> <optional_cluster_file>
var startTestPrefix = process.argv[2];
if(process.argv.length === 5)
	var clusterFile = process.argv[4];
else
	var clusterFile = '';

var assert = require('assert');
var fdb = require('../lib/fdb.js').apiVersion(parseInt(process.argv[3]));
var fdbUtil = require('../lib/fdbUtil.js');
var testerUtil = require('./util.js');
var DirectoryExtension = require('./directory_extension.js');
//fdb.options.setTraceEnable()

var db = fdb.open(clusterFile);

function pushError(inst, err) {
	if(err) {
		if(!err.code) {
			console.error('ERROR during operation \'' + inst.op + '\':');
			console.error(err.stack);
			context.cb(err);
		}

		inst.push(fdb.tuple.pack([fdb.buffer('ERROR'), fdb.buffer(err.code.toString())]));
	}

	return err;
}

var rangeChoice = 0;
function pushRange(itr, inst, prefixFilter, cb) {
	return fdb.future.create(function(futureCb) {
		var outArray = [];

		function pushKV(kv) {
			if(typeof prefixFilter === 'undefined' || prefixFilter === null ||  fdbUtil.buffersEqual(kv.key.slice(0, prefixFilter.length), prefixFilter)) {
				outArray.push(kv.key);
				outArray.push(kv.value);
			}
		}

		function finish(err) {
			if(!pushError(inst, err))
				inst.push(fdb.tuple.pack(outArray));

			futureCb();
		}

		//Test different methods for getting a range
		if(inst.isDatabase) {
			for(var i = 0; i < itr.length; ++i)
				pushKV(itr[i]);

			finish();
		}
		else if(rangeChoice % 4 === 0) {
			itr.forEachBatch(function(res, itrCb) {
				for(var i = 0 ; i < res.length; ++i)
					pushKV(res[i]);

				if(rangeChoice % 8 === 0)
					setTimeout(itrCb, 0);
				else
					itrCb();
			}, function(err, res) {
				finish(err);
			});
		}
		else if(rangeChoice % 4 === 1) {
			itr.forEach(function(res, itrCb) {
				pushKV(res);

				if(rangeChoice % 8 === 1)
					setTimeout(itrCb, 0);
				else
					itrCb();
			}, function(err, res) {
				finish(err);
			});
		}
		else if(rangeChoice % 4 === 2) {
			itr.toArray(function(err, arr) {
				if(!err) {
					for(var i = 0; i < arr.length; ++i)
						pushKV(arr[i]);
				}

				finish(err);
			});
		}
		else {
			fdbUtil.whileLoop(function(loopCb) {
				itr.next(function(err, res) {
					if(err)
						loopCb(err);
					else if(!res)
						loopCb(undefined, null);
					else {
						pushKV(res);
						loopCb();
					}
				});
			}, finish);
		}

		rangeChoice++;
	})(cb);
}

var waitEmpty = fdb.transactional(function(tr, prefix, cb) {
	var itr = tr.getRangeStartsWith(prefix, { limit: 1 });
	itr.toArray(function(err, res) {
		if(err)
			cb(err, null);
		else if(res.length > 0)
			cb(new fdb.FDBError('', 1020), null);
		else
			cb(null, null);
	});
});

var timeoutFuture = function(time) {
	return fdb.future.create(function(futureCb) {
		setTimeout(futureCb, time);
	});
};

var checkWatches = function(db, watches, ready, error, expected, cb) {
	var i = 0;
	return fdbUtil.whileLoop(function(loopCb) {
		if(i == watches.length) return loopCb(undefined, true); // terminate loop
		if(!ready[i] && expected) {
			return watches[i]
			.then(function() {
				loopCb(); // Recheck this watch when it finishes
			})
			.catch(function() {
				loopCb(); // Check the error
			});
		}
		assert.strictEqual(!ready[i] || expected, true, 'watch shouldnt be ready: ' + i);
		if(typeof error[i] !== 'undefined') {
			var tr = db.createTransaction();
			return tr.onError(error[i])
			.then(function() {
				return false;
			})(loopCb);
		}

		i++;
		loopCb();
	})(cb);
}

var testWatches = function(db, cb) {
	return fdbUtil.whileLoop(function(loopCb) {
		var ready = [ false, false, false, false ];
		var error = [ undefined, undefined, undefined, undefined ];
		var watches = [];

		db.doTransaction(function(tr, innerCb) {
			tr.set('w0', '0')
			tr.set('w3', '3');
			innerCb();
		})
		.then(function() {
			return db.doTransaction(function(tr, innerCb) {
				watches[0] = tr.watch('w0');
				innerCb();
			});
		})
		.then(function() {
			return db.clearAndWatch('w1');
		})
		.then(function(w) {
			watches[1] = w.watch;
			return db.setAndWatch('w2', '2');
		})
		.then(function(w) {
			watches[2] = w.watch;
			return db.getAndWatch('w3');
		})
		.then(function(w) {
			assert.strictEqual(w.value.toString(), '3', 'get and watch');
			watches[3] = w.watch;

			for(var i = 0; i < watches.length; ++i) {
				(function(i) {
					watches[i](function(err) {
						if(!err) ready[i] = true;
						else error[i] = err;
					});
				})(i);
			}

			return timeoutFuture(1000);
		})
		.then(function() {
			return checkWatches(db, watches, ready, error, false);
		})
		.then(function(result) {
			if(!result) return; // go around the loop again
			return db.doTransaction(function(tr, innerCb) {
				tr.set('w0', '0');
				innerCb();
			})
			.then(function() {
				return db.clear('w1');
			})
			.then(function() {
				return timeoutFuture(5000);
			})
			.then(function() {
				return checkWatches(db, watches, ready, error, false);
			})
			.then(function(result) {
				if(!result) return; // go around the loop again
				return db.set('w0', 'a')
				.then(function() {
					return db.set('w1', 'b');
				})
				.then(function() {
					return db.clear('w2');
				})
				.then(function() {
					return db.xor('w3', fdb.buffer.fromByteLiteral('\xff\xff'));
				})
				.then(function() {
					return timeoutFuture(2000);
				})
				.then(function() {
					return checkWatches(db, watches, ready, error, true);
				})
				.then(function(result) {
					if(result) return null; //terminate loop
					return;
				});
			});
		})(loopCb);
	})(cb);
};

var testLocality = function(db, cb) {
	return db.doTransaction(function(tr, innerCb) {
		tr.options.setTimeout(60*1000);
		tr.options.setReadSystemKeys();

		fdb.locality.getBoundaryKeys(tr, '', fdb.buffer.fromByteLiteral('\xff\xff'), function(err, itr) {
			if(err) return innerCb(err);

			var index = 0;
			var start;
			var end;
			itr.forEach(function(boundaryKey, loopCb) {
				if(err) return loopCb(err);

				start = end;
				end = boundaryKey;
				if(index++ == 0)
					return loopCb();

				tr.getKey(fdb.KeySelector.lastLessThan(end), function(err, end) {
					if(err) return loopCb(err);

					fdb.locality.getAddressesForKey(tr, start, function(err, startAddresses) {
						if(err) return loopCb(err);

						fdb.locality.getAddressesForKey(tr, end, function(err, endAddresses) {
							if(err) return loopCb(err);

							for(var j = 0; j < startAddresses.length; ++j) {
								var found = false;
								for(var k = 0; k < endAddresses.length; ++k) {
									if(startAddresses[j].toString() === endAddresses[k].toString()) {
										found = true;
										break;
									}
								}

								if(!found) {
									return loopCb(new Error('Locality not internally consistent'));
								}
							}

							loopCb();
						});
					});
				});
			}, innerCb);
		});
	}, cb);
};

var numOperations = 0;
function processOperation(context, inst, cb) {
	//if(inst.op !== 'SWAP' && inst.op !== 'PUSH')
		//console.log(context.prefix + ':', context.instructionIndex + '.', inst.op);

	var promiseCb = function(err) {
		pushError(inst, err);
		cb();
	};

	if(inst.op === 'PUSH') {
		inst.push(inst.tokens[1]);
		cb();
	}
	else if(inst.op === 'POP') {
		inst.pop()(promiseCb);
	}
	else if(inst.op === 'DUP') {
		context.stack.pushEntry(context.stack.get(context.stack.length()-1));
		cb();
	}
	else if(inst.op === 'EMPTY_STACK') {
		context.stack = new testerUtil.Stack();
		cb();
	}
	else if(inst.op === 'SWAP') {
		inst.pop()
		.then(function(index) {
			assert.strictEqual(context.stack.length() > index, true, 'Cannot swap; stack too small');
			index = context.stack.length() - index - 1;
			if(context.stack.length() > index + 1) {
				var tmp = context.stack.get(index);
				context.stack.set(index, context.stack.popEntry());
				context.stack.pushEntry(tmp);
			}
		})(promiseCb);
	}
	else if(inst.op === 'WAIT_FUTURE') {
		inst.pop({withMetadata: true})
		.then(function(stackEntry) {
			context.stack.pushEntry(stackEntry);
		})(promiseCb);
	}
	else if(inst.op === 'WAIT_EMPTY') {
		inst.pop()
		.then(function(waitKey) {
			return waitEmpty(db, waitKey)
			.then(function() {
				inst.push('WAITED_FOR_EMPTY');
			});
		})(promiseCb);
	}
	else if(inst.op === 'START_THREAD') {
		inst.pop()
		.then(function(prefix) {
			processTest(prefix, function(err, res) {
				if(err) {
					console.error('ERROR in Thread', prefix + ':');
					console.error(err.stack);
					process.exit(1);
				}
			});
		})(promiseCb);
	}
	else if(inst.op === 'NEW_TRANSACTION') {
		context.newTransaction();
		cb();
	}
	else if(inst.op === 'USE_TRANSACTION') {
		inst.pop()
		.then(function(name) {
			context.switchTransaction(name);
		})(promiseCb);
	}
	else if(inst.op === 'SET') {
		inst.pop({count: 2})
		.then(function(params) {
			var res = inst.tr.set(params[0], params[1]);

			if(inst.isDatabase)
				inst.push(res, true);
		})(promiseCb);
	}
	else if(inst.op === 'CLEAR') {
		inst.pop()
		.then(function(key) {
			var res = inst.tr.clear(key);

			if(inst.isDatabase)
				inst.push(res, true);
		})(promiseCb);
	}
	else if(inst.op === 'CLEAR_RANGE') {
		inst.pop({count: 2})
		.then(function(params) {
			var res = inst.tr.clearRange(params[0], params[1]);

			if(inst.isDatabase)
				inst.push(res, true);
		})(promiseCb);
	}
	else if(inst.op === 'CLEAR_RANGE_STARTS_WITH') {
		inst.pop()
		.then(function(prefix) {
			var res = inst.tr.clearRangeStartsWith(prefix);

			if(inst.isDatabase)
				inst.push(res, true);
		})(promiseCb);
	}
	else if(inst.op === 'ATOMIC_OP') {
		inst.pop({count: 3})
		.then(function(params) {
			var res = inst.tr[testerUtil.toJavaScriptName(params[0])](params[1], params[2]);

			if(inst.isDatabase)
				inst.push(res, true);
		})(promiseCb);
	}
	else if(inst.op === 'COMMIT') {
		inst.push(inst.tr.commit(), true);
		cb();
	}
	else if(inst.op === 'RESET') {
		inst.tr.reset();
		cb();
	}
	else if(inst.op === 'CANCEL') {
		inst.tr.cancel();
		cb();
	}
	else if(inst.op === 'GET') {
		inst.pop()
		.then(function(key) {
			inst.push(inst.tr.get(key), true);
		})(promiseCb);
	}
	else if(inst.op === 'GET_RANGE') {
		inst.pop({count: 5})
		.then(function(params) {
			var itr = inst.tr.getRange(params[0], params[1], { limit: params[2], reverse: params[3], streamingMode: params[4] });
			if(inst.isDatabase) {
				return itr.then(function(arr) {
					return pushRange(arr, inst);
				});
			}
			else {
				return pushRange(itr, inst);
			}
		})(promiseCb);
	}
	else if(inst.op === 'GET_RANGE_SELECTOR') {
		inst.pop({count: 10})
		.then(function(params) {
			var start = new fdb.KeySelector(params[0], params[1], params[2]);
			var end = new fdb.KeySelector(params[3], params[4], params[5]);
			var itr = inst.tr.getRange(start, end, { limit: params[6], reverse: params[7], streamingMode: params[8] });
			if(inst.isDatabase) {
				return itr.then(function(arr) {
					return pushRange(arr, inst, params[9]);
				});
			}
			else {
				return pushRange(itr, inst, params[9]);
			}
		})(promiseCb);
	}
	else if(inst.op === 'GET_RANGE_STARTS_WITH') {
		inst.pop({count: 4})
		.then(function(params) {
			var itr = inst.tr.getRangeStartsWith(params[0], { limit: params[1], reverse: params[2], streamingMode: params[3] });
			if(inst.isDatabase) {
				return itr.then(function(arr) {
					return pushRange(arr, inst);
				});
			}
			else {
				return pushRange(itr, inst);
			}
		})(promiseCb);
	}
	else if(inst.op === 'GET_KEY') {
		inst.pop({count: 4})
		.then(function(params) {
			var result = inst.tr.getKey(new fdb.KeySelector(params[0], params[1], params[2]))
			.then(function(key) {
				if(fdbUtil.buffersEqual(key.slice(0, params[3].length), params[3])) {
					return key;
				}
				else if(fdb.buffer.toByteLiteral(key) < fdb.buffer.toByteLiteral(params[3])) {
					return params[3];
				}
				else {
					return fdbUtil.strinc(params[3]);
				}
			});

			inst.push(result, true);
		})(promiseCb);
	}
	else if(inst.op === 'READ_CONFLICT_RANGE') {
		inst.pop({count: 2})
		.then(function(params) {
			inst.tr.addReadConflictRange(params[0], params[1]);
			inst.push(fdb.buffer('SET_CONFLICT_RANGE'));
		})(promiseCb);
	}
	else if(inst.op === 'WRITE_CONFLICT_RANGE') {
		inst.pop({count: 2})
		.then(function(params) {
			inst.tr.addWriteConflictRange(params[0], params[1]);
			inst.push(fdb.buffer('SET_CONFLICT_RANGE'));
		})(promiseCb);
	}
	else if(inst.op === 'READ_CONFLICT_KEY') {
		inst.pop()
		.then(function(key) {
			inst.tr.addReadConflictKey(key);
			inst.push(fdb.buffer('SET_CONFLICT_KEY'));
		})(promiseCb);
	}
	else if(inst.op === 'WRITE_CONFLICT_KEY') {
		inst.pop()
		.then(function(key) {
			inst.tr.addWriteConflictKey(key);
			inst.push(fdb.buffer('SET_CONFLICT_KEY'));
		})(promiseCb);
	}
	else if(inst.op === 'DISABLE_WRITE_CONFLICT') {
		inst.tr.options.setNextWriteNoWriteConflictRange();
		cb();
	}
	else if(inst.op === 'GET_READ_VERSION') {
		inst.tr.getReadVersion(function(err, res) {
			if(!pushError(inst, err)) {
				context.lastVersion = res;
				inst.push(fdb.buffer('GOT_READ_VERSION'));
			}
			cb();
		});
	}
	else if(inst.op === 'GET_COMMITTED_VERSION') {
		try {
			context.lastVersion = inst.tr.getCommittedVersion();

			inst.push(fdb.buffer('GOT_COMMITTED_VERSION'));
			cb();
		}
		catch(err) {
			pushError(inst, err);
			cb();
		}
	}
	else if(inst.op === 'GET_VERSIONSTAMP') {
		try {
			inst.push(inst.tr.getVersionstamp(), true)
			cb();
		}
		catch(err) {
			pushError(inst, err);
			cb();
		}
	}
	else if(inst.op === 'SET_READ_VERSION') {
		assert.notStrictEqual(typeof context.lastVersion, 'undefined', 'Cannot set read version; version has never been read');
		inst.tr.setReadVersion(context.lastVersion);
		cb();
	}
	else if(inst.op === 'ON_ERROR') {
		inst.pop()
		.then(function(errorCode) {
			var testErr = new fdb.FDBError('', errorCode);

			inst.push(inst.tr.onError(testErr), true);
		})(promiseCb);
	}
	else if(inst.op === 'TUPLE_PACK') {
		inst.pop()
		.then(function(numParams) {
			return inst.pop({count: numParams})
			.then(function(params) {
				inst.push(fdb.tuple.pack(params));
			});
		})(promiseCb);
	}
	else if(inst.op === 'TUPLE_UNPACK') {
		inst.pop()
		.then(function(packedTuple) {
			var arr = fdb.tuple.unpack(packedTuple);
			for(var i = 0; i < arr.length; ++i)
				inst.push(fdb.tuple.pack([arr[i]]));
		})(promiseCb);
	}
	else if(inst.op === 'TUPLE_RANGE') {
		inst.pop()
		.then(function(numParams) {
			return inst.pop({count: numParams})
			.then(function(params) {
				var range = fdb.tuple.range(params);
				inst.push(range.begin);
				inst.push(range.end);
			});
		})(promiseCb);
	}
	else if(inst.op === 'TUPLE_SORT') {
		inst.pop()
		.then(function(numParams) {
			return inst.pop({count: numParams})
			.then(function(params) {
				var tuples = [];
				for(var i = 0; i < params.length; ++i)
					tuples.push(fdb.tuple.unpack(params[i]));
				tuples.sort(fdb.tuple.compare);
				for(var i = 0; i < tuples.length; ++i)
					inst.push(fdb.tuple.pack(tuples[i]));
			});
		})(promiseCb);
	}
	else if(inst.op === 'SUB') {
		inst.pop({count: 2})
		.then(function(params) {
			inst.push(params[0] - params[1]);
		})(promiseCb);
	}
	else if(inst.op === 'ENCODE_FLOAT') {
		inst.pop()
		.then(function(fBytes) {
			inst.push(fdb.tuple.Float.fromBytes(fBytes));
		})(promiseCb);
	}
	else if(inst.op === 'ENCODE_DOUBLE') {
		inst.pop()
		.then(function(dBytes) {
			inst.push(fdb.tuple.Double.fromBytes(dBytes));
		})(promiseCb);
	}
	else if(inst.op === 'DECODE_FLOAT') {
		inst.pop()
		.then(function(fVal) {
			inst.push(fVal.toBytes());
		})(promiseCb);
	}
	else if(inst.op === 'DECODE_DOUBLE') {
		inst.pop()
		.then(function(dVal) {
			inst.push(dVal.toBytes());
		})(promiseCb);
	}
	else if(inst.op === 'CONCAT') {
		inst.pop({count: 2})
		.then(function(params) {
			if(Buffer.isBuffer(params[0])) {
				inst.push(Buffer.concat([params[0], params[1]]))
			}
			else {
				inst.push(params[0] + params[1]);
			}
		})(promiseCb);
	}
	else if(inst.op === 'LOG_STACK') {
		inst.pop()
		.then(function(prefix) {
			return fdbUtil.whileLoop(function(loopCb) {
				inst.pop({count: 100, withMetadata: true})
				.then(function(items) {
					if(items.length == 0) {
						return null
					}
					return db.doTransaction(function(tr, innerCb) {
						for(var index = 0; index < items.length; ++index) {
							var entry = items[items.length - index - 1];
							var packedSubKey = fdb.tuple.pack([context.stack.length() + index, entry.instructionIndex]);

							var packedValue = fdb.tuple.pack([entry.item]);
							if(packedValue.length > 40000)
								packedValue = packedValue.slice(0, 40000);

							tr.set(Buffer.concat([prefix, packedSubKey], prefix.length + packedSubKey.length), packedValue);
						}

						innerCb();
					});
				})(loopCb);
			});
		})(promiseCb);
	}
	else if(inst.op === 'UNIT_TESTS') {
		db.options.setLocationCacheSize(100001);
		db.doTransaction(function(tr, innerCb) {
			tr.options.setPrioritySystemImmediate();
			tr.options.setPriorityBatch();
			tr.options.setCausalReadRisky();
			tr.options.setCausalWriteRisky();
			tr.options.setReadYourWritesDisable();
			tr.options.setReadAheadDisable();
			tr.options.setReadSystemKeys();
			tr.options.setAccessSystemKeys();
			tr.options.setDurabilityDevNullIsWebScale();
			tr.options.setTimeout(60*1000);
			tr.options.setRetryLimit(50);
			tr.options.setMaxRetryDelay(100);
			tr.options.setUsedDuringCommitProtectionDisable();
			tr.options.setTransactionLoggingEnable('my_transaction');

			tr.get(fdb.buffer.fromByteLiteral('\xff'), innerCb);
		})
		.then(function() {
			return testWatches(db);
		})
		.then(function() {
			return testLocality(db);
		})
		.then(cb)
		.catch(function(err) {
			cb('Unit tests failed: ' + err + "\n" + err.stack);
		});
	}
	else if(testerUtil.startsWith(inst.op, 'DIRECTORY_')) {
		context.directoryExtension.processInstruction(inst, cb);
	}
	else {
		cb('Unrecognized operation');
	}
}

function processTest(prefix, cb) {
	var context = new testerUtil.Context(db, prefix, processOperation, new DirectoryExtension());
	context.run(cb);
}

processTest(startTestPrefix, function(err, res) {
	if(err)
		process.exit(1);
});
