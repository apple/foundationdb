/*
 * fdb.js
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
var Cluster = require('./cluster');
var future = require('./future');
var Transactional = require('./retryDecorator');
var tuple = require('./tuple');
var buffer = require('./bufferConversion');
var fdb = require('./fdbModule');
var FDBError = require('./error');
var locality = require('./locality');
var directory = require('./directory');
var Subspace = require('./subspace');
var selectedApiVersion = require('./apiVersion');

var fdbModule = {};

module.exports = {
    FDBError: FDBError,
	apiVersion: function(version) {
		if(selectedApiVersion.value && version !== selectedApiVersion.value)
			throw new Error('Cannot select multiple different FDB API versions');
		if(version < 500)
			throw new RangeError('FDB API versions before 500 are not supported');
		if(version > 510)
			throw new RangeError('Latest known FDB API version is 510');

		if(!selectedApiVersion.value) {
			fdb.apiVersion(version);

			fdbModule.FDBError = this.FDBError;
			fdbModule.KeySelector = KeySelector;
			fdbModule.future = future;
			fdbModule.transactional = Transactional;
			fdbModule.tuple = tuple;
			fdbModule.buffer = buffer;
			fdbModule.locality = locality;
			fdbModule.directory = directory.directory;
			fdbModule.DirectoryLayer = directory.DirectoryLayer;
			fdbModule.Subspace = Subspace;

			fdbModule.options = fdb.options;
			fdbModule.streamingMode = fdb.streamingMode;

			var dbCache = {};

			var doInit = function() {
				fdb.startNetwork();

				process.on('exit', function() {
					//Clearing out the caches makes memory debugging a little easier
					dbCache = null;

					fdb.stopNetwork();
				});

				//Subsequent calls do nothing
				doInit = function() { };
			};

			fdbModule.init = function() {
				doInit();
			};

			fdbModule.createCluster = function(clusterFile, cb) {
				if(!clusterFile)
					clusterFile = '';

				return new Cluster(fdb.createCluster(clusterFile));
			};

			fdbModule.open = function(clusterFile, cb) {
				if(clusterFile)
					fdb.options.setClusterFile(clusterFile);

				this.init();

				var database = dbCache[clusterFile];
				if(!database) {
					var cluster = fdbModule.createCluster(clusterFile);
					database = cluster.openDatabase();
					dbCache[clusterFile] = database;
				}

				return database;
			};
		}

		selectedApiVersion.value = version;
		return fdbModule;
	}
};

fdb.FDBError = module.exports.FDBError;
