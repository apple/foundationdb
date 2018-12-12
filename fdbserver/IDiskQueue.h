/*
 * IDiskQueue.h
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

#ifndef FDBSERVER_IDISKQUEUE_H
#define FDBSERVER_IDISKQUEUE_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "IKeyValueStore.h"

class IDiskQueue : public IClosable {
public:
	struct location {
		int64_t hi, lo;
		location() : hi(0), lo(0) {}
		location(int64_t lo) : hi(0), lo(lo) {}
		location(int64_t hi, int64_t lo) : hi(hi), lo(lo) {}
		operator std::string() { return format("%lld.%lld", hi, lo); }  // FIXME: Return a 'HumanReadableDescription' instead of std::string, make TraceEvent::detail accept that (for safety)

		bool operator < (location const& r) const {
			if (hi<r.hi) return true;
			if (hi>r.hi) return false;
			return lo < r.lo;
		}
	};

	// Before calling push or commit, the caller *must* perform recovery by calling readNext() until it returns less than the requested number of bytes.
	// Thereafter it may not be called again.
	virtual Future<Standalone<StringRef>> readNext( int bytes ) = 0;  // Return the next bytes in the queue (beginning, the first time called, with the first unpopped byte)
	virtual location getNextReadLocation() = 0;    // Returns a location >= the location of all bytes previously returned by readNext(), and <= the location of all bytes subsequently returned

	virtual location push( StringRef contents ) = 0;  // Appends the given bytes to the byte stream.  Returns a location token representing the *end* of the contents.
	virtual void pop( location upTo ) = 0;            // Removes all bytes before the given location token from the byte stream.
	virtual Future<Void> commit() = 0;  // returns when all prior pushes and pops are durable.  If commit does not return (due to close or a crash), any prefix of the pushed bytes and any prefix of the popped bytes may be durable.

	virtual int getCommitOverhead() = 0; // returns the amount of unused space that would be written by a commit that immediately followed this call

	virtual StorageBytes getStorageBytes() = 0;
};

IDiskQueue* openDiskQueue( std::string basename, UID dbgid, int64_t fileSizeWarningLimit = -1 );  // opens basename+"0.fdq" and basename+"1.fdq"

#endif
