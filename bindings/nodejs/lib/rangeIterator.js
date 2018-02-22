/*
 * rangeIterator.js
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

var KeySelector = require('./keySelector');
var Future = require('./future');
var fdb = require('./fdbModule');
var LazyIterator = require('./lazyIterator');

function getStreamingMode(requestedMode, limit, wantAll) {
	if(wantAll && requestedMode === fdb.streamingMode.iterator) {
		if(limit)
			return fdb.streamingMode.exact;
		else
			return fdb.streamingMode.wantAll;
	}

	return requestedMode;
}

module.exports = function(tr, start, end, options, snapshot) {
	if(!options)
		options = {};

	if(!options.limit)
		options.limit = 0;
	if(!options.reverse)
		options.reverse = false;
	if(!options.streamingMode && options.streamingMode !== 0)
		options.streamingMode = fdb.streamingMode.iterator;

	var RangeFetcher = function(wantAll) {
		this.finished = false;
		this.limit = options.limit;
		this.iterStart = start;
		this.iterEnd = end;
		this.iterationCount = 1;
		this.streamingMode = getStreamingMode(options.streamingMode, this.limit, wantAll);
	};

	RangeFetcher.prototype.clone = function(wantAll) {
		var clone = new RangeFetcher(wantAll);

		clone.finished = this.finished;
		clone.limit = this.limit;
		clone.iterStart = this.iterStart;
		clone.iterEnd = this.iterEnd;
		clone.iterationCount = this.iterationCount;

		return clone;
	};

	RangeFetcher.prototype.fetch = function(cb) {
		var fetcher = this;
		if(fetcher.finished) {
			cb();
		}
		else {
			tr.getRange(fetcher.iterStart.key, fetcher.iterStart.orEqual, fetcher.iterStart.offset, fetcher.iterEnd.key, fetcher.iterEnd.orEqual, fetcher.iterEnd.offset, fetcher.limit, fetcher.streamingMode, fetcher.iterationCount++, snapshot, options.reverse, function(err, res)
			{
				if(!err) {
					var results = res.array;
					if(results.length > 0) {
						if(!options.reverse)
							fetcher.iterStart = KeySelector.firstGreaterThan(results[results.length-1].key);
						else
							fetcher.iterEnd = KeySelector.firstGreaterOrEqual(results[results.length-1].key);
					}

					if(fetcher.limit !== 0) {
						fetcher.limit -= results.length;
						if(fetcher.limit <= 0)
							fetcher.finished = true;
					}
					if(!res.more)
						fetcher.finished = true;
					cb(undefined, results);
				}
				else {
					cb(err);
				}
			});
		}
	};

	return new LazyIterator(RangeFetcher);
};
