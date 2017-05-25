#!/usr/bin/env _node

"use strict";

//cmd line: _node streamline_tester._js <test_prefix> <optional_cluster_file>
var startTestPrefix = process.argv[2];

var assert = require('assert');
var fdb = require('../lib/fdb.js').apiVersion(parseInt(process.argv[3]));
var fdbUtil = require('../lib/fdbUtil.js');
var testerUtil = require('./util.js');
var DirectoryExtension = require('./streamline_directory_extension._js');

var db = fdb.open(process.argv[4]);

function pushError(inst, err) {
	if(err) {
		if(!err.code)
			throw err;

		inst.push(fdb.tuple.pack([fdb.buffer('ERROR'), fdb.buffer(err.code.toString())]));
	}

	return err;
}

var rangeChoice = 0;
function pushRange(itr, inst, prefixFilter, _) {
	var outArray = [];

	function pushKV(kv) { 
		if(typeof prefixFilter === 'undefined' || prefixFilter === null ||  fdbUtil.buffersEqual(kv.key.slice(0, prefixFilter.length), prefixFilter)) {
			outArray.push(kv.key);
			outArray.push(kv.value);
		}
	}

	//Test the different methods for getting data from a range
	if(inst.isDatabase) {
		for(var i = 0; i < itr.length; ++i)
			pushKV(itr[i]);
	}
	else if(rangeChoice % 4 === 0) {
		itr.forEachBatch(function(res, _) {
			for(var i = 0; i < res.length; ++i)
				pushKV(res[i]);

			if(rangeChoice % 8 === 0)
				setTimeout(_, 0);
		}, _);
	}
	else if(rangeChoice % 4 === 1) {
		itr.forEach(function(res, _) {
			pushKV(res);
			if(rangeChoice % 8 === 1)
				setTimeout(_, 0);
		}, _);
	}
	else if(rangeChoice % 4 === 2) {
		var arr = itr.toArray(_);
		for(var i = 0; i < arr.length; ++i)
			pushKV(arr[i]);
	}
	else {
		fdbUtil.whileLoop(function(_) {
			var kv = itr.next(_);
			if(!kv)
				return null;
			else
				pushKV(kv);
		}, _);
	}

	rangeChoice++;
	inst.push(fdb.tuple.pack(outArray));
}

var waitEmpty = fdb.transactional(function(tr, prefix, _) {
	var itr = tr.getRangeStartsWith(prefix, { limit: 1 });
	var arr = itr.toArray(_);

	if(arr.length > 0)
		throw new fdb.FDBError('', 1020);
});

var testWatches = function(db, _) {
	db.set('w0', '0', _);
	db.set('w3', '3', _);

	var ready = [ false, false, false, false ];

	var watches = [];
	watches[0] = db.doTransaction(function(tr, _) {
		return tr.watch('w0');
	}, _);

	watches[1] = db.clearAndWatch('w1', _).watch;
	watches[2] = db.setAndWatch('w2', '2', _).watch;
	watches[3] = db.getAndWatch('w3', _);

	assert.strictEqual(watches[3].value.toString(), '3', 'get and watch');
	watches[3] = watches[3].watch;

	for(var i = 0; i < watches.length; ++i) {
		(function(i) {
			watches[i](function(err) { if(!err) ready[i] = true; });
		})(i);
	}

	function checkWatches(expected, testName) {
		for(var i = 0; i < watches.length; ++i)
			assert.strictEqual(ready[i], expected, 'testName' + i);
	}

	setTimeout(_, 1000);
	checkWatches(false, 'test 1');

	db.set('w0', '0', _);
	db.clear('w1', _);

	setTimeout(_, 5000);
	checkWatches(false, 'test 2');

	db.set('w0', 'a', _);
	db.set('w1', 'b', _);
	db.clear('w2', _);
	db.xor('w3', fdb.buffer.fromByteLiteral('\xff\xff'), _);

	setTimeout(_, 2000);
	checkWatches(true, 'test 3');
};

var testLocality = function(_) {
	db.doTransaction(function(tr, _) {
		tr.options.setTimeout(60*1000);
		tr.options.setReadSystemKeys();

		var boundaryKeys = fdb.locality.getBoundaryKeys(tr, '', fdb.buffer.fromByteLiteral('\xff\xff'), _).toArray(_);
		var success = true;

		for(var i = 0; i < boundaryKeys.length-1; ++i) {
			var start = boundaryKeys[i];
			var end = tr.getKey(fdb.KeySelector.lastLessThan(boundaryKeys[i+1]), _);
			var startAddresses = fdb.locality.getAddressesForKey(tr, start, _);
			var endAddresses = fdb.locality.getAddressesForKey(tr, end, _);
			for(var j = 0; j < startAddresses.length; ++j) {
				var found = false;
				for(var k = 0; k < endAddresses.length; ++k) {
					if(startAddresses[j].toString() === endAddresses[k].toString()) {
						found = true;
						break;
					}
				}

				if(!found) {
					success = false;
					break;
				}
			}

			if(!success)
				break;
		}

		if(!success)
			throw(new Error('Locality not internally consistent'));
	}, _);
}

var numOperations = 0;
function processOperation(context, inst, _) {
	//if(inst.op !== 'SWAP' && inst.op !== 'PUSH')
		//console.log(context.prefix + ':', context.instructionIndex + '.', inst.op);

	var params, numParams, res, itr;

	try {
		if(inst.op === 'PUSH')
			inst.push(inst.tokens[1]);
		else if(inst.op === 'POP')
			inst.pop()(_);
		else if(inst.op === 'DUP')
			context.stack.pushEntry(context.stack.get(context.stack.length()-1));
		else if(inst.op === 'EMPTY_STACK')
			context.stack = new testerUtil.Stack();
		else if(inst.op === 'SWAP') {
			var index = inst.pop()(_);
			assert.strictEqual(context.stack.length() > index, true, 'Cannot swap; stack too small');
			index = context.stack.length() - index - 1;
			if(context.stack.length() > index + 1) {
				var tmp = context.stack.get(index);
				context.stack.set(index, context.stack.popEntry());
				context.stack.pushEntry(tmp);
			}
		}
		else if(inst.op === 'WAIT_FUTURE') {
			var stackEntry = inst.pop({withMetadata: true})(_);
			context.stack.pushEntry(stackEntry);
		}
		else if(inst.op === 'WAIT_EMPTY') {
			var waitKey = inst.pop()(_);
			waitEmpty(db, waitKey, _);
			inst.push('WAITED_FOR_EMPTY');
		}
		else if(inst.op === 'START_THREAD') { 
			var prefix = inst.pop()(_);
			processTest(prefix, function(err, res) {
				if(err) {
					console.error('ERROR in Thread', prefix + ':');
					console.error(err.stack);
					process.exit(1);
				}
			});
		}
		else if(inst.op === 'NEW_TRANSACTION') {
			context.newTransaction();
		}
		else if(inst.op === 'USE_TRANSACTION') {
			var name = inst.pop()(_);
			context.switchTransaction(name);
		}
		else if(inst.op === 'SET') {
			params = inst.pop({count: 2})(_);

			res = inst.tr.set(params[0], params[1]);
			if(inst.isDatabase)
				inst.push(res, true);
		}
		else if(inst.op === 'CLEAR') {
			var key = inst.pop()(_);

			res = inst.tr.clear(key);
			if(inst.isDatabase)
				inst.push(res, true);
		}
		else if(inst.op === 'CLEAR_RANGE') {
			params = inst.pop({count: 2})(_);

			res = inst.tr.clearRange(params[0], params[1]);
			if(inst.isDatabase)
				inst.push(res, true);
		}
		else if(inst.op === 'CLEAR_RANGE_STARTS_WITH') {
			var prefix = inst.pop()(_);

			res = inst.tr.clearRangeStartsWith(prefix);
			if(inst.isDatabase)
				inst.push(res, true);
		}
		else if(inst.op === 'ATOMIC_OP') {
			params = inst.pop({count: 3})(_);

			res = inst.tr[testerUtil.toJavaScriptName(params[0])](params[1], params[2]);
			if(inst.isDatabase)
				inst.push(res, true);
		}
		else if(inst.op === 'COMMIT') {
			inst.push(inst.tr.commit(), true);
		}
		else if(inst.op === 'RESET')
			inst.tr.reset();
		else if(inst.op === 'CANCEL')
			inst.tr.cancel();
		else if(inst.op === 'GET') {
			var key = inst.pop()(_);
			inst.push(inst.tr.get(key), true);
		}
		else if(inst.op === 'GET_RANGE') {
			params = inst.pop({count: 5})(_);

			if(inst.isDatabase)
				itr = inst.tr.getRange(params[0], params[1], { limit: params[2], reverse: params[3], streamingMode: params[4] }, _);
			else
				itr = inst.tr.getRange(params[0], params[1], { limit: params[2], reverse: params[3], streamingMode: params[4] });

			pushRange(itr, inst, undefined, _);
		}
		else if(inst.op === 'GET_RANGE_SELECTOR') {
			params = inst.pop({count: 10})(_);

			var start = new fdb.KeySelector(params[0], params[1], params[2]);
			var end = new fdb.KeySelector(params[3], params[4], params[5]);

			if(inst.isDatabase)
				itr = inst.tr.getRange(start, end, { limit: params[6], reverse: params[7], streamingMode: params[8] }, _);
			else
				itr = inst.tr.getRange(start, end, { limit: params[6], reverse: params[7], streamingMode: params[8] });

			pushRange(itr, inst, params[9], _);
		}
		else if(inst.op === 'GET_RANGE_STARTS_WITH') {
			params = inst.pop({count: 4})(_);

			if(inst.isDatabase)
				itr = inst.tr.getRangeStartsWith(params[0], { limit: params[1], reverse: params[2], streamingMode: params[3] }, _);
			else
				itr = inst.tr.getRangeStartsWith(params[0], { limit: params[1], reverse: params[2], streamingMode: params[3] });

			pushRange(itr, inst, undefined, _);
		}
		else if(inst.op === 'GET_KEY') {
			params = inst.pop({count: 4})(_);
			var key = inst.tr.getKey(new fdb.KeySelector(params[0], params[1], params[2]), _);

			if(fdbUtil.buffersEqual(key.slice(0, params[3].length), params[3])) {
				inst.push(key);
			}
			else if(fdb.buffer.toByteLiteral(key) < fdb.buffer.toByteLiteral(params[3])) {
				inst.push(params[3]);
			}
			else {
				inst.push(fdbUtil.strinc(params[3]));
			}
		}
		else if(inst.op === 'READ_CONFLICT_RANGE') {
			params = inst.pop({count: 2})(_);
			inst.tr.addReadConflictRange(params[0], params[1]);
			inst.push(fdb.buffer('SET_CONFLICT_RANGE'));
		}
		else if(inst.op === 'WRITE_CONFLICT_RANGE') {
			params = inst.pop({count: 2})(_);
			inst.tr.addWriteConflictRange(params[0], params[1]);
			inst.push(fdb.buffer('SET_CONFLICT_RANGE'));
		}
		else if(inst.op === 'READ_CONFLICT_KEY') {
			var key = inst.pop()(_);
			inst.tr.addReadConflictKey(key);
			inst.push(fdb.buffer('SET_CONFLICT_KEY'));
		}
		else if(inst.op === 'WRITE_CONFLICT_KEY') {
			var key = inst.pop()(_);
			inst.tr.addWriteConflictKey(key);
			inst.push(fdb.buffer('SET_CONFLICT_KEY'));
		}
		else if(inst.op === 'DISABLE_WRITE_CONFLICT') {
			inst.tr.options.setNextWriteNoWriteConflictRange();
		}
		else if(inst.op === 'GET_READ_VERSION') {
			context.lastVersion = inst.tr.getReadVersion(_);
			inst.push(fdb.buffer('GOT_READ_VERSION'));
		}
		else if(inst.op === 'GET_COMMITTED_VERSION') {
			context.lastVersion = inst.tr.getCommittedVersion();
			inst.push(fdb.buffer('GOT_COMMITTED_VERSION'));
		}
		else if(inst.op === 'GET_VERSIONSTAMP') {
			inst.push(inst.tr.getVersionstamp(), true);
		}
		else if(inst.op === 'SET_READ_VERSION') {
			assert.notStrictEqual(typeof context.lastVersion, 'undefined', 'Cannot set read version; version has never been read');
			inst.tr.setReadVersion(context.lastVersion);
		}
		else if(inst.op === 'ON_ERROR') {
			var errorCode = inst.pop()(_);
			var testErr = new fdb.FDBError('', errorCode);
			
			inst.push(inst.tr.onError(testErr), true);
		}
		else if(inst.op === 'TUPLE_PACK') {
			numParams = inst.pop()(_);
			params = inst.pop({count: numParams})(_);
			inst.push(fdb.tuple.pack(params));
		}
		else if(inst.op === 'TUPLE_UNPACK') {
			var packedTuple = inst.pop()(_);
			var arr = fdb.tuple.unpack(packedTuple);
			for(var i = 0; i < arr.length; ++i)
				inst.push(fdb.tuple.pack([arr[i]]));
		}
		else if(inst.op === 'TUPLE_RANGE') {
			numParams = inst.pop()(_);
			params = inst.pop({count: numParams})(_);
			var range = fdb.tuple.range(params);
			inst.push(range.begin);
			inst.push(range.end);
		}
		else if(inst.op === 'SUB') {
			params = inst.pop({count: 2})(_);
			inst.push(params[0] - params[1]);
		}
		else if(inst.op === 'CONCAT') {
			params = inst.pop({count: 2})(_);
			if(Buffer.isBuffer(params[0])) {
				inst.push(Buffer.concat([params[0], params[1]]))
			}
			else {
				inst.push(params[0] + params[1]);
			}
		}
		else if(inst.op === 'LOG_STACK') {
			var prefix = inst.pop()(_);
			var items = inst.pop({count: context.stack.length(), withMetadata: true})(_);

			for(var i = 0; i < items.length; ++i) {
				if(i % 100 === 0)
					inst.tr.commit(_);
					inst.tr.reset();

				var entry = items[items.length - i -1];
				var packedSubKey = fdb.tuple.pack([i, entry.instructionIndex]);

				var packedValue = fdb.tuple.pack([entry.item]);
				if(packedValue.length > 40000)
					packedValue = packedValue.slice(0, 40000);

				inst.tr.set(Buffer.concat([prefix, packedSubKey], prefix.length + packedSubKey.length), packedValue);
			}

			inst.tr.commit(_);
			inst.tr.reset();
		}
		else if(inst.op === 'UNIT_TESTS') {
			try {
				db.options.setLocationCacheSize(100001);
				db.doTransaction(function(tr, _) {
					tr.options.setPrioritySystemImmediate();
					tr.options.setPriorityBatch();
					tr.options.setCausalReadRisky();
					tr.options.setCausalWriteRisky();
					tr.options.setReadYourWritesDisable();
					tr.options.setReadAheadDisable();
					tr.options.setReadSystemKeys();
					tr.options.setAccessSystemKeys();
					tr.options.setDurabilityDevNullIsWebScale();
					tr.options.setTimeout(1000);
					tr.options.setRetryLimit(5);
					tr.options.setMaxRetryDelay(100);
					tr.options.setUsedDuringCommitProtectionDisable();
					tr.options.setTransactionLoggingEnable('my_transaction');

					tr.get(fdb.buffer.fromByteLiteral('\xff'), _);
				}, _);

				testWatches(db, _);
				testLocality(_);
			}
			catch(err) {
				throw('Unit tests failed: ' + err);
			}
		}
		else if(testerUtil.startsWith(inst.op, 'DIRECTORY_')) {
			context.directoryExtension.processInstruction(inst, _);
		}
		else
			throw new Error('Unrecognized operation');
	}
	catch(err) {
		pushError(inst, err);
	}
}

function processTest(prefix, _) {
	var context = new testerUtil.Context(db, prefix, processOperation, new DirectoryExtension());
	try {
		context.run(_);
	}
	catch(err) {
		console.error('ERROR during operation \'' + context.ops[context.current].value.toString() + '\':');
		console.error(err.stack);
		process.exit(1);
	}
}

processTest(startTestPrefix, _);
