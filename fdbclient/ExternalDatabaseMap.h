
/*
 * ExternalDatabaseMap.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#pragma once

#ifndef FDBCLIENT_EXTERNALDATABASEMAP_H
#define FDBCLIENT_EXTERNALDATABASEMAP_H

#include "fdbclient/ClusterConnectionKey.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"

struct DbRecord {
	DbRecord(Database db) : db(db), referenceCount(0) {}

	Database db;
	int referenceCount;
	Future<Void> cleanup;
};

class ExternalDatabaseMap {
public:
	Optional<Database> get(std::string name) const {
		auto itr = dbMap.find(name);
		if (itr == dbMap.end() || itr->second.cleanup.isValid()) {
			return Optional<Database>();
		}

		return itr->second.db;
	}

	bool insert(std::string name, Database db) {
		auto result = dbMap.try_emplace(name, db);
		if (!result.second && result.first->second.cleanup.isValid()) {
			result.first->second.cleanup = Future<Void>();
			result.first->second.db = db;
			ASSERT(result.first->second.referenceCount = 0);
			return true;
		}

		return result.second;
	}

	int addDatabaseRef(std::string name) {
		auto itr = dbMap.find(name);
		if (itr == dbMap.end()) {
			return -1;
		} else {
			return ++itr->second.referenceCount;
		}
	}

	int delDatabaseRef(std::string name) {
		auto itr = dbMap.find(name);
		if (itr == dbMap.end()) {
			return -1;
		} else {
			ASSERT(itr->second.referenceCount > 0);
			return --itr->second.referenceCount;
		}
	}

	// SOMEDAY: this leaks entries in our map. If we expect this to get large,
	// we could remove entries that are completed.
	void markDeleted(std::string name, Future<Void> cleanupFuture) {
		auto itr = dbMap.find(name);
		if (itr != dbMap.end()) {
			itr->second.cleanup = cleanupFuture;
		}
	}

	void cancelCleanup(std::string name) {
		auto itr = dbMap.find(name);
		if (itr != dbMap.end()) {
			ASSERT(itr->second.cleanup.isValid());
			itr->second.cleanup.cancel();
			dbMap.erase(itr);
		}
	}

	size_t size() const { return dbMap.size(); }

private:
	std::unordered_map<std::string, DbRecord> dbMap;
};

#endif