/*
 * locality.js
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
var transactional = require('./retryDecorator');
var Database = require('./database');
var LazyIterator = require('./lazyIterator');
var fdb = require('./fdbModule');
var fdbUtil = require('./fdbUtil');

var KEY_SERVERS_PREFIX = buffer.fromByteLiteral('\xff/keyServers/');
var PAST_VERSION_ERROR_CODE = 1007;

function getBoundaryKeysImpl(tr, begin, end, callback) {
	function BoundaryFetcher(wantAll) {
		this.tr = tr;
		this.begin = begin;
		this.end = end;
		this.lastBegin = undefined;

		var fetcher = this;

		if(wantAll)
			this.streamingMode = fdb.streamingMode.wantAll;
		else
			this.streamingMode = fdb.streamingMode.iterator;

		function iteratorCb(err, res) {
			if(err) {
				if(err.code === PAST_VERSION_ERROR_CODE && fetcher.begin !== fetcher.lastBegin) {
					fetcher.tr = fetcher.tr.db.createTransaction();
					readKeys();
				}
				else {
					fetcher.tr.onError(err, function(e) {
						if(e)
							fetcher.fetchCb(e);
						else
							readKeys();
					});
				}
			}
			else
				fetcher.fetchCb();
		}

		function readKeys() {
			fetcher.lastBegin = fetcher.begin;
			fetcher.tr.options.setReadSystemKeys();
			fetcher.tr.options.setLockAware();
			fetcher.tr.snapshot.getRange(fetcher.begin, fetcher.end, {streamingMode: fetcher.streamingMode}).forEachBatch(function(kvs, innerCb) {
				fetcher.forEachCb = innerCb;
				var keys = kvs.map(function(kv) { return kv.key.slice(13); });
				var last = kvs[kvs.length-1].key;
				fetcher.begin = Buffer.concat([last, buffer.fromByteLiteral('\x00')], last.length + 1);
				fetcher.fetchCb(undefined, keys);
			}, iteratorCb);

			fetcher.streamingMode = fdb.streamingMode.wantAll;
		}

		this.fetch = function(cb) {
			this.fetchCb = cb;
			if(this.read)
				this.forEachCb();
			else {
				this.read = true;
				readKeys();
			}
		};

		this.clone = function(wantAll) {
			var clone = new BoundaryFetcher(wantAll);

			clone.tr = this.tr.db.createTransaction();
			clone.begin = this.begin;
			clone.end = this.end;
			clone.lastBegin = this.lastBegin;

			return clone;
		};
	}

	callback(null, new LazyIterator(BoundaryFetcher));
}

function getBoundaryKeys(databaseOrTransaction, begin, end, callback) {
	begin = fdbUtil.keyToBuffer(begin);
	end = fdbUtil.keyToBuffer(end);

	begin = Buffer.concat([KEY_SERVERS_PREFIX, begin], KEY_SERVERS_PREFIX.length + begin.length);
	end = Buffer.concat([KEY_SERVERS_PREFIX, end], KEY_SERVERS_PREFIX.length + end.length);

	if(databaseOrTransaction instanceof Database) {
		getBoundaryKeysImpl(databaseOrTransaction.createTransaction(), begin, end, callback);
	}
	else {
		var tr = databaseOrTransaction.db.createTransaction();
		databaseOrTransaction.getReadVersion(function(err, ver) {
			tr.setReadVersion(ver);
			getBoundaryKeysImpl(tr, begin, end, callback);
		});
	}
}

var getAddressesForKey = transactional(function (tr, key, cb) {
	key = fdbUtil.keyToBuffer(key);
	tr.tr.getAddressesForKey(key, cb);
});

module.exports = {getBoundaryKeys: getBoundaryKeys, getAddressesForKey: getAddressesForKey};
