/*
 * IDiskQueue.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/IKeyValueStore.h"
#include "flow/BooleanParam.h"

FDB_DECLARE_BOOLEAN_PARAM(CheckHashes);

class IDiskQueue : public IClosable {
public:
	struct location {
		// location is same with seq., specifying the index of the virtualy infinite queue.
		int64_t hi, lo; // hi is always 0, lo is always equal to seq.
		location() : hi(0), lo(0) {}
		location(int64_t lo) : hi(0), lo(lo) {}
		location(int64_t hi, int64_t lo) : hi(hi), lo(lo) {}
		operator std::string() const {
			return format("%lld.%lld", hi, lo);
		} // FIXME: Return a 'HumanReadableDescription' instead of std::string, make TraceEvent::detail accept that (for
		  // safety)

		template <class Ar>
		void serialize_unversioned(Ar& ar) {
			serializer(ar, hi, lo);
		}

		bool operator<(location const& r) const {
			if (hi < r.hi)
				return true;
			if (hi > r.hi)
				return false;
			return lo < r.lo;
		}
		bool operator>(location const& r) const { return r < *this; }
		bool operator<=(location const& r) const { return !(*this > r); }
		bool operator>=(location const& r) const { return !(*this < r); }

		bool operator==(const location& r) const { return hi == r.hi && lo == r.lo; }
	};

	//! Find the first and last pages in the disk queue, and initialize invariants.
	//!
	//! Most importantly, most invariants only hold after this function returns, and
	//! some functions assert that the IDiskQueue has been initialized.
	//!
	//! \param recoverAt The minimum location from which to start recovery.
	//! \returns True, if DiskQueue is now considered in a recovered state.
	//!          False, if the caller should call readNext until recovered is true.
	virtual Future<bool> initializeRecovery(location recoverAt) = 0;
	// Before calling push or commit, the caller *must* perform recovery by calling readNext() until it returns less
	// than the requested number of bytes. Thereafter it may not be called again.
	virtual Future<Standalone<StringRef>> readNext(int bytes) = 0; // Return the next bytes in the queue (beginning, the
	                                                               // first time called, with the first unpopped byte)
	virtual location getNextReadLocation()
	    const = 0; // Returns a location >= the location of all bytes previously returned by readNext(), and <= the
	               // location of all bytes subsequently returned
	virtual location getNextCommitLocation()
	    const = 0; // If commit() were to be called, all buffered writes would be written starting at `location`.
	virtual location getNextPushLocation()
	    const = 0; // If push() were to be called, the pushed data would be written starting at `location`.

	virtual Future<Standalone<StringRef>> read(location start, location end, CheckHashes vc) = 0;
	virtual location push(StringRef contents) = 0; // Appends the given bytes to the byte stream.  Returns a location
	                                               // token representing the *end* of the contents.
	virtual void pop(location upTo) = 0; // Removes all bytes before the given location token from the byte stream.
	virtual Future<Void>
	commit() = 0; // returns when all prior pushes and pops are durable.  If commit does not return (due to close or a
	              // crash), any prefix of the pushed bytes and any prefix of the popped bytes may be durable.

	virtual int getCommitOverhead() const = 0; // returns the amount of unused space that would be written by a commit
	                                           // that immediately followed this call

	virtual StorageBytes getStorageBytes() const = 0;
};

template <>
struct Traceable<IDiskQueue::location> : std::true_type {
	static std::string toString(const IDiskQueue::location& value) { return value; }
};

// FIXME: One should be able to use SFINAE to choose between serialize and serialize_unversioned.
template <class Ar>
void load(Ar& ar, IDiskQueue::location& loc) {
	loc.serialize_unversioned(ar);
}
template <class Ar>
void save(Ar& ar, const IDiskQueue::location& loc) {
	const_cast<IDiskQueue::location&>(loc).serialize_unversioned(ar);
}

namespace std {
template <>
struct numeric_limits<IDiskQueue::location> {
	static IDiskQueue::location max() {
		int64_t max64 = numeric_limits<int64_t>::max();
		return IDiskQueue::location(max64, max64);
	};
	static IDiskQueue::location min() { return IDiskQueue::location(0, 0); }
};
} // namespace std

// Specify which hash function to use for checksum of pages in DiskQueue
enum class DiskQueueVersion : uint16_t {
	V0 = 0, // Use hashlittle
	V1 = 1, // Use crc32, which is faster than hashlittle
	V2 = 2, // Use xxhash3
};

IDiskQueue* openDiskQueue(std::string basename,
                          std::string ext,
                          UID dbgid,
                          DiskQueueVersion diskQueueVersion,
                          int64_t fileSizeWarningLimit = -1); // opens basename+"0."+ext and basename+"1."+ext

#endif
