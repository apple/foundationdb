/*
 * IVersionedStore.h
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

#ifndef FDBSERVER_IVERSIONEDSTORE_H
#define FDBSERVER_IVERSIONEDSTORE_H
#pragma once

#include "fdbserver/IKeyValueStore.h"

#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"

class IStoreCursor {
public:
	virtual Future<Void> findEqual(KeyRef key) = 0;
	virtual Future<Void> findFirstEqualOrGreater(KeyRef key, int prefetchBytes = 0) = 0;
	virtual Future<Void> findLastLessOrEqual(KeyRef key, int prefetchBytes = 0) = 0;
	virtual Future<Void> next() = 0;
	virtual Future<Void> prev() = 0;

	virtual bool isValid() = 0;
	virtual KeyRef getKey() = 0;
	virtual ValueRef getValue() = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;
};

class IVersionedStore : public IClosable {
public:
	virtual KeyValueStoreType getType() const = 0;
	virtual bool supportsMutation(int op) const = 0; // If this returns true, then mutate(op, ...) may be called
	virtual StorageBytes getStorageBytes() const = 0;

	// Writes are provided in an ordered stream.
	// A write is considered part of (a change leading to) the version determined by the previous call to
	// setWriteVersion() A write shall not become durable until the following call to commit() begins, and shall be
	// durable once the following call to commit() returns
	virtual void set(KeyValueRef keyValue) = 0;
	virtual void clear(KeyRangeRef range) = 0;
	virtual void mutate(int op, StringRef param1, StringRef param2) = 0;
	virtual void setWriteVersion(Version) = 0; // The write version must be nondecreasing
	virtual void setOldestVersion(Version v) = 0; // Set oldest readable version to be used in next commit
	virtual Version getOldestVersion() const = 0; // Get oldest readable version
	virtual Future<Void> commit() = 0;

	virtual Future<Void> init() = 0;
	virtual Version getLatestVersion() const = 0;

	// readAtVersion() may only be called on a version which has previously been passed to setWriteVersion() and never
	// previously passed
	//   to forgetVersion.  The returned results when violating this precondition are unspecified; the store is not
	//   required to be able to detect violations.
	// The returned read cursor provides a consistent snapshot of the versioned store, corresponding to all the writes
	// done with write versions less
	//   than or equal to the given version.
	// If readAtVersion() is called on the *current* write version, the given read cursor MAY reflect subsequent writes
	// at the same
	//   write version, OR it may represent a snapshot as of the call to readAtVersion().
	virtual Reference<IStoreCursor> readAtVersion(Version) = 0;
};

#endif
