/*
 * KeyValueStoreSQLite.actor.cpp
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

#define SQLITE_THREADSAFE 0 // also in sqlite3.amalgamation.c!
#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "flow/crc32c.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/CoroFlow.h"
#include "fdbserver/Knobs.h"
#include "flow/Hash3.h"
#include "flow/xxhash.h"

// for unprintable
#include "fdbclient/NativeAPI.actor.h"

extern "C" {
#include "fdbserver/sqlite/sqliteInt.h"
u32 sqlite3VdbeSerialGet(const unsigned char*, u32, Mem*);
}
#include "flow/ThreadPrimitives.h"
#include "fdbserver/VFSAsync.h"
#include "fdbserver/template_fdb.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#if SQLITE_THREADSAFE == 0
#define sqlite3_mutex_enter(x)
#define sqlite3_mutex_leave(x)
#endif

void hexdump(FILE* fout, StringRef val);

/*#undef state
#include <Windows.h>*/

/*uint64_t getFileSize( const char* filename ) {
    HANDLE f = CreateFile( filename, GENERIC_READ, FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE, nullptr,
OPEN_EXISTING, 0, nullptr); if (f == INVALID_HANDLE_VALUE) return 0; DWORD hi,lo; lo = GetFileSize(f, &hi);
    CloseHandle(f);
    return (uint64_t(hi)<<32) + lo;
}*/

struct SpringCleaningStats {
	int64_t springCleaningCount;
	int64_t lazyDeletePages;
	int64_t vacuumedPages;
	double springCleaningTime;
	double vacuumTime;
	double lazyDeleteTime;

	SpringCleaningStats()
	  : springCleaningCount(0), lazyDeletePages(0), vacuumedPages(0), springCleaningTime(0.0), vacuumTime(0.0),
	    lazyDeleteTime(0.0) {}
};

struct PageChecksumCodec {
	PageChecksumCodec(std::string const& filename) : pageSize(0), reserveSize(0), filename(filename), silent(false) {}

	int pageSize;
	int reserveSize;
	std::string filename;
	bool silent;

	struct SumType {
		bool operator==(const SumType& rhs) const { return part1 == rhs.part1 && part2 == rhs.part2; }
		uint32_t part1;
		uint32_t part2;
		std::string toString() { return format("0x%08x%08x", part1, part2); }
	};

	// Calculates and then either stores or verifies a checksum.
	// The checksum is read/stored at the end of the page buffer.
	// Page size is passed in as pageLen because this->pageSize is not always appropriate.
	// If write is true then the checksum is written into the page and true is returned.
	// If write is false then the checksum is compared to the in-page sum and the return value
	// is whether or not the checksums were equal.
	bool checksum(Pgno pageNumber, void* data, int pageLen, bool write) {
		ASSERT(pageLen > sizeof(SumType));

		char* pData = (char*)data;
		int dataLen = pageLen - sizeof(SumType);
		SumType* pSumInPage = (SumType*)(pData + dataLen);

		if (write) {
			// Always write a xxHash3 checksum for new pages
			// First 8 bits are set to 0 so that with high probability,
			// checksums written with hashlittle2 don't require calculating
			// an xxHash3 checksum on read
			auto xxHash3 = XXH3_64bits(data, dataLen);
			pSumInPage->part1 = static_cast<uint32_t>((xxHash3 >> 32) & 0x00ffffff);
			pSumInPage->part2 = static_cast<uint32_t>(xxHash3 & 0xffffffff);
			return true;
		}

		SumType crc32Sum;
		if (pSumInPage->part1 == 0) {
			// part1 being 0 indicates with very high probability that a CRC32 checksum
			// was used, so check that first. If this checksum fails, there is still
			// some chance the page was written with another checksum algorithm
			crc32Sum.part1 = 0;
			crc32Sum.part2 = crc32c_append(0xfdbeefdb, static_cast<uint8_t*>(data), dataLen);
			if (crc32Sum == *pSumInPage) {
				TEST(true); // Read CRC32 checksum
				return true;
			}
		}

		// Try xxhash3
		SumType xxHash3Sum;
		if ((pSumInPage->part1 >> 24) == 0) {
			// The first 8 bits of part1 being 0 indicates with high probability that an
			// xxHash3 checksum was used, so check that next. If this checksum fails, there is
			// still some chance the page was written with hashlittle2, so fall back to checking
			// hashlittle2
			auto xxHash3 = XXH3_64bits(data, dataLen);
			xxHash3Sum.part1 = static_cast<uint32_t>((xxHash3 >> 32) & 0x00ffffff);
			xxHash3Sum.part2 = static_cast<uint32_t>(xxHash3 & 0xffffffff);
			if (xxHash3Sum == *pSumInPage) {
				TEST(true); // Read xxHash3 checksum
				return true;
			}
		}

		// Try hashlittle2
		SumType hashLittle2Sum;
		hashLittle2Sum.part1 = pageNumber; // DO NOT CHANGE
		hashLittle2Sum.part2 = 0x5ca1ab1e;
		hashlittle2(pData, dataLen, &hashLittle2Sum.part1, &hashLittle2Sum.part2);
		if (hashLittle2Sum == *pSumInPage) {
			TEST(true); // Read HashLittle2 checksum
			return true;
		}

		if (!silent) {
			TraceEvent trEvent(SevError, "SQLitePageChecksumFailure");
			trEvent.error(checksum_failed())
			    .detail("CodecPageSize", pageSize)
			    .detail("CodecReserveSize", reserveSize)
			    .detail("Filename", filename)
			    .detail("PageNumber", pageNumber)
			    .detail("PageSize", pageLen)
			    .detail("ChecksumInPage", pSumInPage->toString())
			    .detail("ChecksumCalculatedHL2", hashLittle2Sum.toString());
			if (pSumInPage->part1 == 0) {
				trEvent.detail("ChecksumCalculatedCRC", crc32Sum.toString());
			}
			if (pSumInPage->part1 >> 24 == 0) {
				trEvent.detail("ChecksumCalculatedXXHash3", xxHash3Sum.toString());
			}
		}
		return false;
	}

	static void* codec(void* vpSelf, void* data, Pgno pageNumber, int op) {
		PageChecksumCodec* self = (PageChecksumCodec*)vpSelf;

		// Page write operations are 6 for DB page and 7 for journal page
		bool write = (op == 6 || op == 7);
		// Page read is operation 3, which must be the operation if it's not a write.
		ASSERT(write || op == 3);

		// Page 1 is special.  It contains the database configuration including Page Size and Reserve Size.
		// SQLite can't get authoritative values for these things until the Pager Codec has validated (and
		// potentially decrypted) Page 1 itself, so it can't tell the Pager Codec what those things are before
		// Page 1 is handled.  It will guess a Page Size of SQLITE_DEFAULT_PAGE_SIZE, and a Reserve Size based
		// on the pre-verified (and perhaps still encrypted) header in the Page 1 data that it will then pass
		// to the Pager Codec.
		//
		// So, Page 1 must be written and verifiable as a SQLITE_DEFAULT_PAGE_SIZE sized page as well as
		// the actual configured page size for the database, if it is larger.  A configured page size lower
		// than the default (in other words 512) results in undefined behavior.
		if (pageNumber == 1) {
			if (write && self->pageSize > SQLITE_DEFAULT_PAGE_SIZE) {
				self->checksum(pageNumber, data, SQLITE_DEFAULT_PAGE_SIZE, write);
			}
		} else {
			// For Page Numbers other than 1, reserve size must be the size of the checksum.
			if (self->reserveSize != sizeof(SumType)) {
				if (!self->silent)
					TraceEvent(SevWarnAlways, "SQLitePageChecksumFailureBadReserveSize")
					    .detail("CodecPageSize", self->pageSize)
					    .detail("CodecReserveSize", self->reserveSize)
					    .detail("Filename", self->filename)
					    .detail("PageNumber", pageNumber);

				return nullptr;
			}
		}

		if (!self->checksum(pageNumber, data, self->pageSize, write))
			return nullptr;

		return data;
	}

	static void sizeChange(void* vpSelf, int new_pageSize, int new_reserveSize) {
		PageChecksumCodec* self = (PageChecksumCodec*)vpSelf;
		self->pageSize = new_pageSize;
		self->reserveSize = new_reserveSize;
	}

	static void free(void* vpSelf) {
		PageChecksumCodec* self = (PageChecksumCodec*)vpSelf;
		delete self;
	}
};

struct SQLiteDB : NonCopyable {
	std::string filename;
	sqlite3* db;
	Btree* btree;
	int table, freetable;
	bool haveMutex;
	Reference<IAsyncFile> dbFile, walFile;
	bool page_checksums;
	bool fragment_values;
	PageChecksumCodec* pPagerCodec; // we do NOT own this pointer, db does.

	void beginTransaction(bool write) { checkError("BtreeBeginTrans", sqlite3BtreeBeginTrans(btree, write)); }
	void endTransaction() { checkError("BtreeCommit", sqlite3BtreeCommit(btree)); }
	void rollback() { checkError("BtreeRollback", sqlite3BtreeRollback(btree)); }

	void open(bool writable);
	void createFromScratch();

	SQLiteDB(std::string filename, bool page_checksums, bool fragment_values)
	  : filename(filename), db(nullptr), btree(nullptr), table(-1), freetable(-1), haveMutex(false),
	    page_checksums(page_checksums), fragment_values(fragment_values) {
		TraceEvent(SevDebug, "SQLiteDBCreate").detail("This", (void*)this).detail("Filename", filename).backtrace();
	}

	~SQLiteDB() {
		TraceEvent(SevDebug, "SQLiteDBDestroy").detail("This", (void*)this).detail("Filename", filename).backtrace();

		if (db) {
			if (haveMutex) {
				sqlite3_mutex_leave(db->mutex);
			}
			sqlite3_close(db);
		}
	}

	void initPagerCodec() {
		if (page_checksums) {
			int r = sqlite3_test_control(SQLITE_TESTCTRL_RESERVE, db, sizeof(PageChecksumCodec::SumType));
			if (r != 0) {
				TraceEvent(SevError, "BtreePageReserveSizeSetError")
				    .detail("Filename", filename)
				    .detail("ErrorCode", r);
				ASSERT(false);
			}
			// Always start with a new pager codec with default options.
			pPagerCodec = new PageChecksumCodec(filename);
			sqlite3BtreePagerSetCodec(
			    btree, PageChecksumCodec::codec, PageChecksumCodec::sizeChange, PageChecksumCodec::free, pPagerCodec);
		}
	}

	void checkError(const char* context, int rc) {
		// if (deterministicRandom()->random01() < .001) rc = SQLITE_INTERRUPT;
		if (rc) {
			// Our exceptions don't propagate through sqlite, so we don't know for sure if the error that caused this
			// was an injected fault.  Assume that if VFSAsyncFile caught an injected Error that it caused this error
			// return code.
			Error err = io_error();
			if (g_network->isSimulated() && VFSAsyncFile::checkInjectedError()) {
				err = err.asInjectedFault();
			}

			if (db)
				db->errCode = rc;
			if (rc == SQLITE_NOMEM)
				platform::outOfMemory(); // SOMEDAY: Trap out of memory errors at allocation time; check out different
				                         // allocation options in sqlite

			TraceEvent(SevError, "DiskError")
			    .error(err)
			    .detail("In", context)
			    .detail("File", filename)
			    .detail("SQLiteError", sqlite3ErrStr(rc))
			    .detail("SQLiteErrorCode", rc)
			    .GetLastError();
			throw err;
		}
	}

	void checkpoint(bool restart) {
		int logSize = 0, checkpointCount = 0;
		// double t = timer();
		while (true) {
			int rc = sqlite3_wal_checkpoint_v2(
			    db, 0, restart ? SQLITE_CHECKPOINT_RESTART : SQLITE_CHECKPOINT_FULL, &logSize, &checkpointCount);
			if (!rc)
				break;
			if ((sqlite3_errcode(db) & 0xff) == SQLITE_BUSY) {
				// printf("#");
				// threadSleep(.010);
				sqlite3_sleep(10);
			} else
				checkError("checkpoint", rc);
		}
		// printf("Checkpoint (%0.1f ms): %d frames in log, %d checkpointed\n", (timer()-t)*1000, logSize,
		// checkpointCount);
	}
	uint32_t freePages() {
		u32 fp = 0;
		sqlite3BtreeGetMeta(btree, BTREE_FREE_PAGE_COUNT, &fp);
		return fp;
	}
	bool vacuum() { // Returns true if vacuum is complete or stalled by a lazy free root
		int rc = sqlite3BtreeIncrVacuum(btree);
		if (rc && rc != SQLITE_DONE)
			checkError("vacuum", rc);
		return rc == SQLITE_DONE;
	}
	int check(bool verbose) {
		int errors = 0;
		int tables[] = { 1, table, freetable };
		TraceEvent("BTreeIntegrityCheckBegin").detail("Filename", filename);
		char* e = sqlite3BtreeIntegrityCheck(btree, tables, 3, 1000, &errors, verbose);
		if (!(g_network->isSimulated() && VFSAsyncFile::checkInjectedError())) {
			TraceEvent((errors || e) ? SevError : SevInfo, "BTreeIntegrityCheckResults")
			    .detail("Filename", filename)
			    .detail("ErrorTotal", errors);
			if (e != nullptr) {
				// e is a string containing 1 or more lines.  Create a separate trace event for each line.
				char* lineStart = e;
				while (lineStart != nullptr) {
					char* lineEnd = strstr(lineStart, "\n");
					if (lineEnd != nullptr) {
						*lineEnd = '\0';
						++lineEnd;
					}

					// If the line length found is not zero then print a trace event
					if (*lineStart != '\0')
						TraceEvent(SevError, "BTreeIntegrityCheck")
						    .detail("Filename", filename)
						    .detail("ErrorDetail", lineStart);
					lineStart = lineEnd;
				}
			}
			TEST(true); // BTree integrity checked
		}
		if (e)
			sqlite3_free(e);

		return errors;
	}
	int checkAllPageChecksums();
};

class Statement : NonCopyable {
	SQLiteDB& db;
	sqlite3_stmt* stmt;

public:
	Statement(SQLiteDB& db, const char* sql) : db(db), stmt(nullptr) {
		db.checkError("prepare", sqlite3_prepare_v2(db.db, sql, -1, &stmt, nullptr));
	}
	~Statement() {
		try {
			db.checkError("finalize", sqlite3_finalize(stmt));
		} catch (...) {
		}
	}
	Statement& reset() {
		db.checkError("reset", sqlite3_reset(stmt));
		return *this;
	}
	Statement& param(int i, StringRef value) {
		db.checkError("bind", sqlite3_bind_blob(stmt, i, value.begin(), value.size(), SQLITE_STATIC));
		return *this;
	}
	Statement& param(int i, int value) {
		db.checkError("bind", sqlite3_bind_int(stmt, i, value));
		return *this;
	}
	Statement& execute() {
		int r = sqlite3_step(stmt);
		if (r == SQLITE_ROW)
			db.checkError("execute called on statement that returns rows", r);
		if (r != SQLITE_DONE)
			db.checkError("execute", r);
		return *this;
	}
	bool nextRow() {
		int r = sqlite3_step(stmt);
		if (r == SQLITE_ROW)
			return true;
		if (r == SQLITE_DONE)
			return false;
		db.checkError("nextRow", r);
		__assume(false); // NOT REACHED
	}
	StringRef column(int i) {
		return StringRef((const uint8_t*)sqlite3_column_blob(stmt, i), sqlite3_column_bytes(stmt, i));
	}
};

void hexdump(FILE* fout, StringRef val) {
	int buflen = val.size();
	const unsigned char* buf = val.begin();
	int i, j;
	for (i = 0; i < buflen; i += 32) {
		fprintf(fout, "%06x: ", i);
		for (j = 0; j < 32; j++) {
			if (j == 16)
				fprintf(fout, "  ");
			if (i + j < buflen)
				fprintf(fout, "%02x ", buf[i + j]);
			else
				fprintf(fout, "   ");
		}
		fprintf(fout, " ");
		for (j = 0; j < 32; j++) {
			if (j == 16)
				fprintf(fout, "  ");
			if (i + j < buflen)
				fprintf(fout, "%c", isprint(buf[i + j]) ? buf[i + j] : '.');
		}
		fprintf(fout, "\n");
	}
}

Value encode(KeyValueRef kv) {
	int keyCode = kv.key.size() * 2 + 12;
	int valCode = kv.value.size() * 2 + 12;
	int header_size = sqlite3VarintLen(keyCode) + sqlite3VarintLen(valCode);
	int hh = sqlite3VarintLen(header_size);
	header_size += hh;
	if (hh < sqlite3VarintLen(header_size))
		header_size++;
	int size = header_size + kv.key.size() + kv.value.size();

	Value v;
	uint8_t* d = new (v.arena()) uint8_t[size];
	((ValueRef&)v) = KeyRef(d, size);
	d += sqlite3PutVarint(d, header_size);
	d += sqlite3PutVarint(d, keyCode);
	d += sqlite3PutVarint(d, valCode);
	memcpy(d, kv.key.begin(), kv.key.size());
	d += kv.key.size();
	memcpy(d, kv.value.begin(), kv.value.size());
	d += kv.value.size();
	ASSERT(d == v.begin() + size);
	return v;
}

// Fragments are encoded as (key, index, value) tuples
// An index of 0 indicates an unfragmented KV pair.
// For fragmented KV pairs, the values will be concatenated in index order.
//
// In the current implementation, index values are chosen to enable a single linear
// pass over the fragments, in forward or backward order, to immediately know the final
// unfragmented value size accurately enough to allocate a buffer that is certainly large
// enough to hold the defragmented bytes.
//
// However, the decoder could be made to work if these index value 'hints' become inaccurate
// due to a change in splitting logic or index numbering.  The decoder would just have to support
// buffer expansion as needed.
//
// Note that changing the following value constitutes a change in index numbering.
#define KV_FRAGMENT_INDEX_SIZE_HINT_FACTOR 4
Value encodeKVFragment(KeyValueRef kv, uint32_t index) {
	int keyCode = kv.key.size() * 2 + 12;
	int valCode = kv.value.size() * 2 + 12;
	// The SQLite type code for the index is the minimal number of bytes needed to store
	// a signed representation of the index value.  The type code for 0 is 0 (which is
	// actually the null type in SQLite).
	int8_t indexCode = 0;
	if (index != 0) {
		uint32_t tmp = index;
		while (tmp != 0) {
			++indexCode;
			tmp >>= 8;
		}
		// An increment is required if the high bit of the N-byte index value is set, since it is
		// positive number but SQLite only stores signed values and would interpret it as negative.
		if (index >> (8 * indexCode - 1))
			++indexCode;
	}

	int header_size = sqlite3VarintLen(keyCode) + sizeof(indexCode) + sqlite3VarintLen(valCode);
	int hh = sqlite3VarintLen(header_size);
	header_size += hh;
	if (hh < sqlite3VarintLen(header_size))
		header_size++;
	int size = header_size + kv.key.size() + indexCode + kv.value.size();

	Value v;
	uint8_t* d = new (v.arena()) uint8_t[size];
	((ValueRef&)v) = KeyRef(d, size);
	d += sqlite3PutVarint(d, header_size);
	d += sqlite3PutVarint(d, keyCode);
	*d++ = indexCode;
	d += sqlite3PutVarint(d, valCode);

	// Write key
	memcpy(d, kv.key.begin(), kv.key.size());
	d += kv.key.size();

	// Write index bytes, if any
	for (int i = indexCode - 1; i >= 0; --i) {
		d[i] = (uint8_t)index;
		index >>= 8;
	}
	d += indexCode;

	// Write value
	memcpy(d, kv.value.begin(), kv.value.size());
	d += kv.value.size();
	ASSERT(d == v.begin() + size);
	return v;
}

int getEncodedSize(int keySize, int valuePrefixSize) {
	int keyCode = keySize * 2 + 12;
	int header_size =
	    sqlite3VarintLen(keyCode) + 8; // 8 is the maximum return value of sqlite3VarintLen(), so this is our worst case
	                                   // header size (for values larger than allowable database values)
	int hh = sqlite3VarintLen(header_size);
	header_size += hh;
	if (hh < sqlite3VarintLen(header_size))
		header_size++;
	return header_size + keySize + valuePrefixSize;
}

KeyValueRef decodeKV(StringRef encoded) {
	uint8_t const* d = encoded.begin();
	uint64_t h, len1, len2;
	d += sqlite3GetVarint(d, (u64*)&h);
	d += sqlite3GetVarint(d, (u64*)&len1);
	d += sqlite3GetVarint(d, (u64*)&len2);
	ASSERT(d == encoded.begin() + h);
	ASSERT(len1 >= 12 && !(len1 & 1));
	ASSERT(len2 >= 12 && !(len2 & 1));
	len1 = (len1 - 12) / 2;
	len2 = (len2 - 12) / 2;
	ASSERT(d + len1 + len2 == encoded.end());
	return KeyValueRef(KeyRef(d, len1), KeyRef(d + len1, len2));
}

// Given a key size and value prefix size, get the minimum bytes that must be read from the underlying
// btree tuple to safely read the prefix length from the value bytes (if the value is long enough)
int getEncodedKVFragmentSize(int keySize, int valuePrefixSize) {
	int keyCode = keySize * 2 + 12;
	int header_size = sqlite3VarintLen(keyCode) + 1 // index code length
	                  + 8; // worst case for value size (larger than fdb api allows)
	int hh = sqlite3VarintLen(header_size);
	header_size += hh;
	if (hh < sqlite3VarintLen(header_size))
		header_size++;
	return header_size + keySize + 4 // Max width allowed of index value
	       + valuePrefixSize;
}

// Decode (key, index, value) tuple.
// A present() Optional will always be returned UNLESS partial is true.
// If partial is true then the return will not be present() unless at least
// the full key and index were in the encoded buffer.  The value returned will be 0 or
// more value bytes, however many were available.
// Note that a short encoded buffer must at *least* contain the header length varint.
Optional<KeyValueRef> decodeKVFragment(StringRef encoded, uint32_t* index = nullptr, bool partial = false) {
	uint8_t const* d = encoded.begin();
	uint64_t h, len1, len2;
	d += sqlite3GetVarint(d, (u64*)&h);

	// Make sure entire header is present, else return nothing
	if (partial && encoded.size() < h)
		return Optional<KeyValueRef>();

	d += sqlite3GetVarint(d, (u64*)&len1);
	const uint8_t indexLen = *d++;
	ASSERT(indexLen <= 4);
	d += sqlite3GetVarint(d, (u64*)&len2);
	ASSERT(d == encoded.begin() + h);
	ASSERT(len1 >= 12 && !(len1 & 1));
	ASSERT(len2 >= 12 && !(len2 & 1));
	len1 = (len1 - 12) / 2;
	len2 = (len2 - 12) / 2;

	if (partial) {
		// If the key and index aren't complete, return nothing.
		if (d + len1 + indexLen > encoded.end())
			return Optional<KeyValueRef>();
		// Encoded size shouldn't be *larger* than the record described by the header no matter what.
		ASSERT(d + len1 + indexLen + len2 >= encoded.end());
		// Shorten value length to be whatever bytes remain after the header/key/index
		len2 = std::min(len2, (uint64_t)(encoded.end() - indexLen - len1 - d));
	} else {
		// But for non partial records encoded size should be exactly the size of the described record.
		ASSERT(d + len1 + indexLen + len2 == encoded.end());
	}

	// Decode big endian index
	if (index != nullptr) {
		if (indexLen == 0)
			*index = 0;
		else {
			const uint8_t* begin = d + len1;
			const uint8_t* end = begin + indexLen;
			*index = (uint8_t)*begin++;
			while (begin < end) {
				*index <<= 8;
				*index |= *begin++;
			}
		}
	}
	return KeyValueRef(KeyRef(d, len1), KeyRef(d + len1 + indexLen, len2));
}

KeyValueRef decodeKVPrefix(StringRef encoded, int maxLength) {
	uint8_t const* d = encoded.begin();
	uint64_t h, len1, len2;
	d += sqlite3GetVarint(d, (u64*)&h);
	d += sqlite3GetVarint(d, (u64*)&len1);
	d += sqlite3GetVarint(d, (u64*)&len2);
	ASSERT(d == encoded.begin() + h);
	ASSERT(len1 >= 12 && !(len1 & 1));
	ASSERT(len2 >= 12 && !(len2 & 1));
	len1 = (len1 - 12) / 2;
	len2 = (len2 - 12) / 2;
	len2 = std::min(len2, (uint64_t)maxLength);
	ASSERT(d + len1 + len2 <= encoded.end());
	return KeyValueRef(KeyRef(d, len1), KeyRef(d + len1, len2));
}

Value encodeKey(KeyRef key, bool using_fragments) {
	int keyCode = key.size() * 2 + 12;
	int header_size = sqlite3VarintLen(keyCode);
	if (using_fragments) // will be encoded as key, 0  (where 0 is really a null)
		++header_size;
	int hh = sqlite3VarintLen(header_size);
	header_size += hh;
	if (hh < sqlite3VarintLen(header_size))
		header_size++;
	int size = header_size + key.size();
	Value v;
	uint8_t* d = new (v.arena()) uint8_t[size];
	((ValueRef&)v) = KeyRef(d, size);
	d += sqlite3PutVarint(d, header_size);
	d += sqlite3PutVarint(d, keyCode);
	if (using_fragments)
		*d++ = 0;
	memcpy(d, key.begin(), key.size());
	d += key.size();
	ASSERT(d == v.begin() + size);
	return v;
}

struct SQLiteTransaction {
	SQLiteDB& db;
	bool shouldCommit;
	SQLiteTransaction(SQLiteDB& db, bool write) : db(db), shouldCommit(false) { db.beginTransaction(write); }
	void commit() { shouldCommit = true; }
	~SQLiteTransaction() {
		try {
			if (shouldCommit)
				db.endTransaction();
			else
				db.rollback();
		} catch (...) {
		}
	}
};

struct IntKeyCursor {
	SQLiteDB& db;
	BtCursor* cursor;
	IntKeyCursor(SQLiteDB& db, int table, bool write) : db(db), cursor(nullptr) {
		cursor = (BtCursor*)new char[sqlite3BtreeCursorSize()];
		sqlite3BtreeCursorZero(cursor);
		db.checkError("BtreeCursor", sqlite3BtreeCursor(db.btree, table, write, nullptr, cursor));
	}
	~IntKeyCursor() {
		if (cursor) {
			try {
				db.checkError("BtreeCloseCursor", sqlite3BtreeCloseCursor(cursor));
			} catch (...) {
			}
			delete[] (char*)cursor;
		}
	}
};

struct RawCursor {
	SQLiteDB& db;
	BtCursor* cursor;
	KeyInfo keyInfo;
	bool valid;
	int64_t kvBytesRead = 0;

	operator bool() const { return valid; }

	RawCursor(SQLiteDB& db, int table, bool write) : db(db), cursor(nullptr), valid(false) {
		keyInfo.db = db.db;
		keyInfo.enc = db.db->aDb[0].pSchema->enc;
		keyInfo.aColl[0] = db.db->pDfltColl;
		keyInfo.aSortOrder = 0;
		keyInfo.nField = 1;

		try {
			cursor = (BtCursor*)new char[sqlite3BtreeCursorSize()];
			sqlite3BtreeCursorZero(cursor);
			db.checkError("BtreeCursor", sqlite3BtreeCursor(db.btree, table, write, &keyInfo, cursor));
		} catch (...) {
			destroyCursor();
			throw;
		}
	}
	~RawCursor() { destroyCursor(); }
	void destroyCursor() {
		if (cursor) {
			try {
				db.checkError("BtreeCloseCursor", sqlite3BtreeCloseCursor(cursor));
			} catch (...) {
				TraceEvent(SevError, "RawCursorDestructionError").log();
			}
			delete[] (char*)cursor;
		}
	}
	void moveFirst() {
		int empty = 1;
		db.checkError("BtreeFirst", sqlite3BtreeFirst(cursor, &empty));
		valid = !empty;
	}
	void moveNext() {
		int empty = 1;
		db.checkError("BtreeNext", sqlite3BtreeNext(cursor, &empty));
		valid = !empty;
	}
	void movePrevious() {
		int empty = 1;
		db.checkError("BtreePrevious", sqlite3BtreePrevious(cursor, &empty));
		valid = !empty;
	}
	int size() {
		int64_t size;
		db.checkError("BtreeKeySize", sqlite3BtreeKeySize(cursor, (i64*)&size));
		ASSERT(size < (1 << 30));
		return size;
	}
	Value getEncodedRow() {
		int s = size();
		Value v;
		uint8_t* d = new (v.arena()) uint8_t[s];
		db.checkError("BtreeKey", sqlite3BtreeKey(cursor, 0, s, d));
		((ValueRef&)v) = KeyRef(d, s);
		return v;
	}
	ValueRef getEncodedRow(Arena& arena) {
		int s = size();
		uint8_t* d = new (arena) uint8_t[s];
		db.checkError("BtreeKey", sqlite3BtreeKey(cursor, 0, s, d));
		return KeyRef(d, s);
	}
	ValueRef getEncodedRowPrefix(Arena& arena, int maxEncodedSize) {
		int s = std::min(size(), maxEncodedSize);
		uint8_t* d = new (arena) uint8_t[s];
		db.checkError("BtreeKey", sqlite3BtreeKey(cursor, 0, s, d));
		return KeyRef(d, s);
	}
	void insertFragment(KeyValueRef kv, uint32_t index, int seekResult) {
		Value v = encodeKVFragment(kv, index);
		db.checkError("BtreeInsert", sqlite3BtreeInsert(cursor, v.begin(), v.size(), nullptr, 0, 0, 0, seekResult));
	}
	void remove() { db.checkError("BtreeDelete", sqlite3BtreeDelete(cursor)); }
	void set(KeyValueRef kv) {
		if (db.fragment_values) {
			// Unlike a read, where we need to access fragments in fully forward or reverse order,
			// here we just want to delete any existing fragments for the key.  It does not matter
			// what order we delete them in, and SQLite requires us to seek after every delete, so
			// the fastest way to do this is to repeatedly seek to the tuple prefix (key, ) and
			// delete the current fragment until nothing is there.
			// This should result in almost identical performance to non-fragmenting mode for single fragment kv pairs.
			int seekResult = moveTo(kv.key, true); // second arg means to ignore fragmenting and seek to (key, )
			while (seekResult == 0) {
				remove();
				seekResult = moveTo(kv.key, true);
			}

			const int primaryPageUsable = SERVER_KNOBS->SQLITE_FRAGMENT_PRIMARY_PAGE_USABLE;
			const int overflowPageUsable = SERVER_KNOBS->SQLITE_FRAGMENT_OVERFLOW_PAGE_USABLE;

			int fragments = 1;
			int valuePerFragment = kv.value.size();

			// Figure out if we would benefit from fragmenting this kv pair.  The key size must be less than
			// primary page usable size, and the value and key size together must exceeed the primary page usable size.
			if ((kv.key.size() + kv.value.size()) > primaryPageUsable && kv.key.size() < primaryPageUsable) {

				// Just the part of the value that would be in a partially-filled overflow page
				int overflowPartialBytes = (kv.expectedSize() - primaryPageUsable) % overflowPageUsable;

				// Number of bytes wasted in the unfragmented case
				int unfragmentedWaste = overflowPageUsable - overflowPartialBytes;

				// Total space used for unfragmented form
				int unfragmentedTotal = kv.expectedSize() + unfragmentedWaste;

				// Value bytes that can fit in the primary page for each fragment
				int primaryPageValueBytes = primaryPageUsable - kv.key.size();

				// Calculate how many total fragments it would take to spread the partial overflow page bytes and the
				// first fragment's primary page value bytes evenly over multiple tuples that fit in primary pages.
				fragments =
				    (primaryPageValueBytes + overflowPartialBytes + primaryPageValueBytes - 1) / primaryPageValueBytes;

				// Number of bytes wasted in the fragmented case (for the extra key copies)
				int fragmentedWaste = kv.key.size() * (fragments - 1);

				// Total bytes used for the fragmented case
				// int fragmentedTotal = kv.expectedSize() + fragmentedWaste;

				// Calculate bytes saved by having extra key instances stored vs the original partial overflow page
				// bytes.
				int savings = unfragmentedWaste - fragmentedWaste;

				double reduction = (double)savings / unfragmentedTotal;

				// printf("K: %5d  V: %6d  OVERFLOW: %5d  FRAGMENTS: %3d  SAVINGS: %4d  FRAG: %7d  UNFRAG: %7d
				// REDUCTION: %.3f\n", kv.key.size(), kv.value.size(), overflowPartialBytes, fragments, savings,
				// fragmentedTotal, unfragmentedTotal, reduction);
				if (reduction < SERVER_KNOBS->SQLITE_FRAGMENT_MIN_SAVINGS)
					fragments = 1;
				else
					valuePerFragment = (primaryPageValueBytes + overflowPartialBytes + fragments - 1) / fragments;
			}

			if (fragments == 1) {
				insertFragment(kv, 0, seekResult);
				return;
			}

			// First index is ceiling(value_size / KV_FRAGMENT_INDEX_SIZE_HINT_FACTOR)
			uint32_t nextIndex =
			    (kv.value.size() + KV_FRAGMENT_INDEX_SIZE_HINT_FACTOR - 1) / KV_FRAGMENT_INDEX_SIZE_HINT_FACTOR;
			// Last index is ceiling(value_size / (KV_FRAGMENT_INDEX_SIZE_HINT_FACTOR / 2) )
			uint32_t finalIndex = (kv.value.size() + (KV_FRAGMENT_INDEX_SIZE_HINT_FACTOR / 2) - 1) /
			                      (KV_FRAGMENT_INDEX_SIZE_HINT_FACTOR / 2);
			int bytesLeft = kv.value.size();
			int readPos = 0;
			while (bytesLeft > 0) {
				--fragments; // remaining ideal fragment count
				int fragSize = (fragments == 0) ? bytesLeft : std::min<int>(bytesLeft, valuePerFragment);

				// The last fragment must have an index of finalIndex or higher.
				if (fragSize == bytesLeft && nextIndex < finalIndex)
					nextIndex = finalIndex;
				// printf("insert ks %d vs %d  fragment %d, %dbytes\n", kv.key.size(), kv.value.size(), nextIndex,
				// fragSize);
				insertFragment(KeyValueRef(kv.key, kv.value.substr(readPos, fragSize)), nextIndex, seekResult);
				// seekResult can only be used for the first insertion.
				if (seekResult != 0)
					seekResult = 0;
				readPos += fragSize;
				bytesLeft -= fragSize;
				++nextIndex;
			}
		} else {
			int r = moveTo(kv.key);
			if (!r)
				remove();
			Value v = encode(kv);
			db.checkError("BTreeInsert", sqlite3BtreeInsert(cursor, v.begin(), v.size(), nullptr, 0, 0, 0, r));
		}
	}
	void clearOne(KeyRangeRef keys) {
		ASSERT(!db.fragment_values);
		int r = moveTo(keys.begin);
		if (r < 0)
			moveNext();
		ASSERT(valid && decodeKV(getEncodedRow()).key < keys.end);
		remove();
	}
	void clear(KeyRangeRef keys) {
		// TODO: This is really slow!
		while (true) {
			int r = moveTo(keys.begin);
			if (r < 0)
				moveNext();
			if (!valid || (db.fragment_values ? decodeKVFragment(getEncodedRow()).get().key
			                                  : decodeKV(getEncodedRow()).key) >= keys.end)
				break;
			remove();
		}
	}
	void fastClear(KeyRangeRef keys, bool& freeTableEmpty) {
		std::vector<int> clearBuffer(SERVER_KNOBS->CLEAR_BUFFER_SIZE);
		clearBuffer[0] = 0;

		while (true) {
			if (moveTo(keys.begin) < 0)
				moveNext();
			RawCursor endCursor(db, db.table, false);
			if (endCursor.moveTo(keys.end) >= 0)
				endCursor.movePrevious();

			if (!valid || !endCursor ||
			    (db.fragment_values ? (decodeKVFragment(getEncodedRow()).get().key >=
			                           decodeKVFragment(endCursor.getEncodedRow()).get().key)
			                        : (decodeKV(getEncodedRow()).key > decodeKV(endCursor.getEncodedRow()).key)))
				break; // If empty stop!

			int rc = sqlite3BtreeDeleteRange(
			    cursor, endCursor.cursor, &clearBuffer[0], &clearBuffer[0] + clearBuffer.size());
			if (rc == 201)
				continue;
			if (!rc)
				break;
			db.checkError("BtreeDeleteRange", rc);
		}

		if (clearBuffer[0]) {
			// printf("fastClear(%s,%s): %d pages freed\n", printable(keys.begin).c_str(), printable(keys.end).c_str(),
			// clearBuffer[0]);
			IntKeyCursor fc(db, db.freetable, true);
			int pagesDeleted = 0;
			db.checkError("BtreeLazyDelete",
			              sqlite3BtreeLazyDelete(
			                  fc.cursor, &clearBuffer[0], &clearBuffer[0] + clearBuffer.size(), 0, &pagesDeleted));
			ASSERT(pagesDeleted == 0);
			freeTableEmpty = false;
		}
	}
	int lazyDelete(int desiredPages) {
		std::vector<int> clearBuffer(SERVER_KNOBS->CLEAR_BUFFER_SIZE);
		clearBuffer[0] = 0;

		IntKeyCursor fc(db, db.freetable, true);
		int pagesDeleted = 0;
		db.checkError(
		    "BtreeLazyDelete",
		    sqlite3BtreeLazyDelete(
		        fc.cursor, &clearBuffer[0], &clearBuffer[0] + clearBuffer.size(), desiredPages, &pagesDeleted));
		return pagesDeleted;
	}

	// Reads and reconstitutes kv fragments given cursor, an arena to allocate in, and a direction to move the cursor.
	//   getNext() returns the next KV pair, if there is one
	//   peek() returns the next key that would be read by getNext(), if there is one
	// Both methods return Optionals.
	// Once either method returns a non-present value, using the DefragmentingReader again is undefined behavior.
	struct DefragmentingReader {
		// Use this constructor for forward/backward range reads
		DefragmentingReader(RawCursor& cur, Arena& m, bool forward)
		  : cur(cur), arena(m), forward(forward), fragmentReadLimit(-1) {
			parse();
		}

		// Use this constructor to read a SINGLE partial value from the current cursor position for an expected key.
		// This exists to support IKeyValueStore::getPrefix().
		// The reader will return exactly one KV pair if its key matches expectedKey, otherwise no KV pairs.
		DefragmentingReader(RawCursor& cur, Arena& m, KeyRef expectedKey, int maxValueLen)
		  : cur(cur), arena(m), forward(true), maxValueLen(maxValueLen) {
			fragmentReadLimit = getEncodedKVFragmentSize(expectedKey.size(), maxValueLen);
			parse();
			// If a key was found but it wasn't the expected key then
			// clear the current kv pair and invalidate the cursor.
			if (kv.present() && kv.get().key != expectedKey) {
				kv = Optional<KeyValueRef>();
				cur.valid = false;
			}
		}

	private:
		Optional<KeyValueRef> kv; // key and latest value fragment read
		uint32_t index; // index of latest value fragment read
		RawCursor& cur; // Cursor to read from
		Arena& arena; // Arena to allocate key and value bytes in
		bool forward; // true for forward iteration, false for reverse
		int maxValueLen; // truncated value length to return
		int fragmentReadLimit; // If >= 0, only read and *attempt* to decode this many fragment bytes

		// Update kv with whatever is at the current cursor position if the position is valid.
		void parse() {
			if (cur.valid) {
				// The read is either not partial or it is but the fragment read limit is at least 4 (the size of a
				// minimal header).
				bool partial = fragmentReadLimit >= 0;
				ASSERT(!partial || fragmentReadLimit >= 4);
				// Read full or part of fragment
				ValueRef encoded =
				    (partial) ? cur.getEncodedRowPrefix(arena, fragmentReadLimit) : cur.getEncodedRow(arena);
				kv = decodeKVFragment(encoded, &index, partial);
				// If this was a partial fragment then if successful update the next fragment read size, and if not
				// then invalidate the cursor.
				if (partial) {
					if (kv.present())
						fragmentReadLimit -= kv.get().value.size();
					else
						cur.valid = false;
				}
			} else
				kv = Optional<KeyValueRef>();
		}

		// advance cursor, parse and return key if valid
		Optional<KeyRef> advance() {
			if (cur.valid) {
				forward ? cur.moveNext() : cur.movePrevious();
				parse();
			}
			return kv.present() ? kv.get().key : Optional<KeyRef>();
		}

	public:
		// Get the next key that would be returned by getNext(), if there is one
		// This is more efficient than getNext() if the caller is not sure if it wants the next KV pair
		Optional<KeyRef> peek() {
			if (kv.present())
				return kv.get().key;
			return advance();
		}

		Optional<KeyValueRef> getNext() {
			if (!peek().present())
				return Optional<KeyValueRef>();

			bool partial = fragmentReadLimit >= 0;

			// Start out with the next KV fragment as the pair to return
			KeyValueRef resultKV = kv.get();

			// If index is 0 then this is an unfragmented key.  It is unnecessary to advance the cursor so
			// we won't, but we will clear kv so that the next peek/getNext will have to advance.
			if (index == 0)
				kv = Optional<KeyValueRef>();
			else {
				// First and last indexes in fragment group are size hints.
				//   First index is ceil(total_value_size / 4)
				//   Last  index is ceil(total_value_size / 2)
				// Set size depending on which of these will be first encountered and allocate buffer in arena.
				// Note that if these index hints are wrong (such as if the index scheme changes) then asserts
				// below will fail.  They will have to be changed to expand the buffer as needed.
				int size = forward ? (index * KV_FRAGMENT_INDEX_SIZE_HINT_FACTOR)
				                   : (index * (KV_FRAGMENT_INDEX_SIZE_HINT_FACTOR / 2));
				uint8_t* buf = new (arena) uint8_t[size];
				uint8_t* bufEnd = buf + size;
				// For forward iteration wptr is the place to write to next, for reverse it's where the last write
				// started.
				uint8_t* wptr = forward ? buf : bufEnd;
				int fragments = 0;
				do {
					++fragments;
					const ValueRef& val = kv.get().value;
					if (forward) {
						uint8_t* w = wptr;
						wptr += val.size();
						ASSERT(wptr <= bufEnd);
						memcpy(w, val.begin(), val.size());
						// If this is a partial value get and we have enough bytes we can stop since we are forward
						// iterating.
						if (partial && wptr - buf >= maxValueLen) {
							resultKV.value = ValueRef(buf, maxValueLen);
							// To make further calls to peek() or getNext() return nothing, reset kv and invalidate
							// cursor
							kv = Optional<KeyValueRef>();
							cur.valid = false;
							return resultKV;
						}
					} else {
						wptr -= val.size();
						ASSERT(wptr >= buf);
						memcpy(wptr, val.begin(), val.size());
					}
				} while (advance().present() && kv.get().key == resultKV.key);

				// If there was only 1 fragment, it should have been index 0 and handled above,
				ASSERT(fragments != 1);
				// Set final value based on direction of buffer fill
				resultKV.value = forward ? ValueRef(buf, wptr - buf) : ValueRef(wptr, bufEnd - wptr);
			}

			// In partial value mode, we could end up here if there was only 1 fragments or maxValueLen
			// was greater than the total unfragmented value size.
			if (partial)
				resultKV.value = resultKV.value.substr(0, std::min(resultKV.value.size(), maxValueLen));
			return resultKV;
		}
	};

	Optional<Value> get(KeyRef key) {
		int r = moveTo(key);
		if (db.fragment_values) {
			// Optimization - moveTo seeks to fragment (key, 0) so if it was exactly found then we
			// know we have a single fragment for key and can return it.
			if (r == 0) {
				Value result;
				((ValueRef&)result) = decodeKVFragment(getEncodedRow(result.arena())).get().value;
				kvBytesRead += key.size() + result.size();
				return result;
			}

			// Otherwise see if the fragments immediately after (key, 0) are for the key we want.
			if (r < 0)
				moveNext();
			Arena m;
			DefragmentingReader i(*this, m, true);
			if (i.peek() == key) {
				Optional<KeyValueRef> kv = i.getNext();
				kvBytesRead += key.size() + kv.get().value.size();
				return Value(kv.get().value, m);
			}
		} else if (r == 0) {
			Value result;
			KeyValueRef kv = decodeKV(getEncodedRow(result.arena()));
			((ValueRef&)result) = kv.value;
			kvBytesRead += key.size() + result.size();
			return result;
		}

		return Optional<Value>();
	}
	Optional<Value> getPrefix(KeyRef key, int maxLength) {
		if (db.fragment_values) {
			int r = moveTo(key);
			if (r < 0)
				moveNext();
			Arena m;
			DefragmentingReader i(*this, m, getEncodedKVFragmentSize(key.size(), maxLength));
			if (i.peek() == key) {
				Optional<KeyValueRef> kv = i.getNext();
				kvBytesRead += key.size() + kv.get().value.size();
				return Value(kv.get().value, m);
			}
		} else if (!moveTo(key)) {
			if (maxLength == 0) {
				return Value();
			}
			Value result;
			int maxEncodedSize = getEncodedSize(key.size(), maxLength);
			KeyValueRef kv = decodeKVPrefix(getEncodedRowPrefix(result.arena(), maxEncodedSize), maxLength);
			((ValueRef&)result) = kv.value;
			kvBytesRead += key.size() + result.size();
			return result;
		}
		return Optional<Value>();
	}
	RangeResult getRange(KeyRangeRef keys, int rowLimit, int byteLimit) {
		RangeResult result;
		int accumulatedBytes = 0;
		ASSERT(byteLimit > 0);
		if (rowLimit == 0) {
			return result;
		}

		if (db.fragment_values) {
			if (rowLimit > 0) {
				int r = moveTo(keys.begin);
				if (r < 0)
					moveNext();

				DefragmentingReader i(*this, result.arena(), true);
				Optional<KeyRef> nextKey = i.peek();
				while (nextKey.present() && nextKey.get() < keys.end && rowLimit != 0 && accumulatedBytes < byteLimit) {
					Optional<KeyValueRef> kv = i.getNext();
					result.push_back(result.arena(), kv.get());
					--rowLimit;
					accumulatedBytes += sizeof(KeyValueRef) + kv.get().expectedSize();
					nextKey = i.peek();
				}
			} else {
				int r = moveTo(keys.end);
				if (r >= 0)
					movePrevious();
				DefragmentingReader i(*this, result.arena(), false);
				Optional<KeyRef> nextKey = i.peek();
				while (nextKey.present() && nextKey.get() >= keys.begin && rowLimit != 0 &&
				       accumulatedBytes < byteLimit) {
					Optional<KeyValueRef> kv = i.getNext();
					result.push_back(result.arena(), kv.get());
					++rowLimit;
					accumulatedBytes += sizeof(KeyValueRef) + kv.get().expectedSize();
					nextKey = i.peek();
				}
			}
		} else {
			if (rowLimit > 0) {
				int r = moveTo(keys.begin);
				if (r < 0)
					moveNext();
				while (this->valid && rowLimit != 0 && accumulatedBytes < byteLimit) {
					KeyValueRef kv = decodeKV(getEncodedRow(result.arena()));
					if (kv.key >= keys.end)
						break;
					--rowLimit;
					accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
					result.push_back(result.arena(), kv);
					moveNext();
				}
			} else {
				int r = moveTo(keys.end);
				if (r >= 0)
					movePrevious();
				while (this->valid && rowLimit != 0 && accumulatedBytes < byteLimit) {
					KeyValueRef kv = decodeKV(getEncodedRow(result.arena()));
					if (kv.key < keys.begin)
						break;
					++rowLimit;
					accumulatedBytes += sizeof(KeyValueRef) + kv.expectedSize();
					result.push_back(result.arena(), kv);
					movePrevious();
				}
			}
		}
		result.more = rowLimit == 0 || accumulatedBytes >= byteLimit;
		if (result.more) {
			ASSERT(result.size() > 0);
			result.readThrough = result[result.size() - 1].key;
		}
		// AccumulatedBytes includes KeyValueRef overhead so subtract it
		kvBytesRead += (accumulatedBytes - result.size() * sizeof(KeyValueRef));
		return result;
	}

	int moveTo(KeyRef key, bool ignore_fragment_mode = false) {
		UnpackedRecord r;
		r.pKeyInfo = &keyInfo;
		r.flags =
		    UNPACKED_PREFIX_MATCH; // This record [key] can be considered equal to a record [key,value] for any value
		Mem tupleValues[2];
		r.aMem = tupleValues;

		// Set field 1 of tuple to key, which is a string type with typecode 12 + 2*len
		tupleValues[0].db = keyInfo.db;
		tupleValues[0].enc = keyInfo.enc;
		tupleValues[0].zMalloc = nullptr;
		ASSERT(sqlite3VdbeSerialGet(key.begin(), 12 + (2 * key.size()), &tupleValues[0]) == key.size());

		// In fragmenting mode, seek is to (k, 0, ), otherwise just (k, ).
		if (ignore_fragment_mode || !db.fragment_values)
			r.nField = 1;
		else {
			// Set field 2 of tuple to the null type which is typecode 0
			tupleValues[1].db = keyInfo.db;
			tupleValues[1].enc = keyInfo.enc;
			tupleValues[1].zMalloc = nullptr;
			ASSERT(sqlite3VdbeSerialGet(nullptr, 0, &tupleValues[1]) == 0);

			r.nField = 2;
		}

		int result;
		db.checkError("BtreeMovetoUnpacked", sqlite3BtreeMovetoUnpacked(cursor, &r, 0, 0, &result));
		valid = result >= 0 || !sqlite3BtreeEof(cursor);
		return result;
	}
};
struct Cursor : SQLiteTransaction, RawCursor {
	Cursor(SQLiteDB& db, bool write) : SQLiteTransaction(db, write), RawCursor(db, db.table, write) {}
};

struct ReadCursor : ReferenceCounted<ReadCursor>, FastAllocated<ReadCursor> {
	// Readers need to be reset (forced to move to a new snapshot) when the writer thread does a checkpoint.
	// ReadCursor is reference counted so that the writer can clear the persistent reference (readCursors[n]) and
	//   readers can hold an additional reference when they actually have a read happening.
	// ReadCursor lazily constructs its actual Cursor (and hence transaction) because it's vital that readCursors[n] be
	//   assigned before the transaction is opened.

	ReadCursor() : valid(false) {}

	void init(SQLiteDB& db) {
		new (&cursor) Cursor(db, false);
		valid = true;
	}
	~ReadCursor() {
		if (valid)
			get().~Cursor();
	}

	Cursor& get() { return *((Cursor*)&cursor); }

private:
	std::aligned_storage<sizeof(Cursor), __alignof(Cursor)>::type cursor;
	bool valid;
};

extern bool vfsAsyncIsOpen(std::string filename);

// Returns number of pages which failed checksum.
int SQLiteDB::checkAllPageChecksums() {
	ASSERT(!haveMutex);
	ASSERT(page_checksums); // This should never be called on SQLite databases that do not have page checksums.

	double startT = timer();

	// First try to open an existing file
	std::string apath = abspath(filename);
	std::string walpath = apath + "-wal";

	/* REMOVE THIS BEFORE CHECKIN */ if (!fileExists(apath))
		return 0;

	TraceEvent("SQLitePageChecksumScanBegin").detail("File", apath);

	ErrorOr<Reference<IAsyncFile>> dbFile = waitForAndGet(
	    errorOr(IAsyncFileSystem::filesystem()->open(apath, IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_LOCK, 0)));
	ErrorOr<Reference<IAsyncFile>> walFile = waitForAndGet(
	    errorOr(IAsyncFileSystem::filesystem()->open(walpath, IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_LOCK, 0)));

	if (dbFile.isError())
		throw dbFile.getError(); // If we've failed to open the file, throw an exception
	if (walFile.isError())
		throw walFile.getError(); // If we've failed to open the file, throw an exception

	// Now that the file itself is open and locked, let sqlite open the database
	// Note that VFSAsync will also call g_network->open (including for the WAL), so its flags are important, too
	// TODO:  If better performance is needed, make AsyncFileReadAheadCache work and be enabled by SQLITE_OPEN_READAHEAD
	// which was added for that purpose.
	int result = sqlite3_open_v2(apath.c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
	checkError("open", result);

	// This check has the useful side effect of actually opening/reading the database.  If we were not doing this,
	// then we could instead open a read cursor for the same effect, as currently tryReadEveryDbPage() requires it.
	Statement* jm = new Statement(*this, "PRAGMA journal_mode");
	ASSERT(jm->nextRow());
	if (jm->column(0) != LiteralStringRef("wal")) {
		TraceEvent(SevError, "JournalModeError").detail("Filename", filename).detail("Mode", jm->column(0));
		ASSERT(false);
	}
	delete jm;

	btree = db->aDb[0].pBt;
	initPagerCodec();
	sqlite3_extended_result_codes(db, 1);

	sqlite3_mutex_enter(db->mutex);
	haveMutex = true;

	pPagerCodec->silent = true;
	Pgno p = 1;
	int readErrors = 0;
	int corruptPages = 0;
	int totalErrors = 0;

	while (1) {
		int type;
		int zero;
		int rc = tryReadEveryDbPage(db, p, &p, &type, &zero);
		if (rc == SQLITE_OK)
			break;
		if (rc == SQLITE_CORRUPT) {
			TraceEvent(SevWarnAlways, "SQLitePageChecksumScanCorruptPage")
			    .detail("File", filename)
			    .detail("PageNumber", p)
			    .detail("PageType", type)
			    .detail("PageWasZeroed", zero);
			++corruptPages;
		} else {
			TraceEvent(SevWarnAlways, "SQLitePageChecksumScanReadFailed")
			    .detail("File", filename)
			    .detail("PageNumber", p)
			    .detail("SQLiteError", sqlite3ErrStr(rc))
			    .detail("SQLiteErrorCode", rc);
			++readErrors;
		}
		++p;
		if (++totalErrors >= SERVER_KNOBS->SQLITE_PAGE_SCAN_ERROR_LIMIT)
			break;
	}
	pPagerCodec->silent = false;

	haveMutex = false;
	sqlite3_mutex_leave(db->mutex);
	sqlite3_close(db);

	TraceEvent("SQLitePageChecksumScanEnd")
	    .detail("Elapsed", DEBUG_DETERMINISM ? 0 : timer() - startT)
	    .detail("Filename", filename)
	    .detail("CorruptPages", corruptPages)
	    .detail("ReadErrors", readErrors)
	    .detail("TotalErrors", totalErrors);

	ASSERT(!vfsAsyncIsOpen(filename));
	ASSERT(!vfsAsyncIsOpen(filename + "-wal"));

	return totalErrors;
}

void SQLiteDB::open(bool writable) {
	ASSERT(!haveMutex);
	double startT = timer();
	//TraceEvent("KVThreadInitStage").detail("Stage",1).detail("Filename", filename).detail("Writable", writable);

	// First try to open an existing file
	std::string apath = abspath(filename);
	std::string walpath = apath + "-wal";
	ErrorOr<Reference<IAsyncFile>> dbFile = waitForAndGet(
	    errorOr(IAsyncFileSystem::filesystem()->open(apath, IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_LOCK, 0)));
	ErrorOr<Reference<IAsyncFile>> walFile = waitForAndGet(
	    errorOr(IAsyncFileSystem::filesystem()->open(walpath, IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_LOCK, 0)));

	//TraceEvent("KVThreadInitStage").detail("Stage",15).detail("Filename", apath).detail("Writable", writable).detail("IsErr", dbFile.isError());

	if (writable) {
		if (dbFile.isError() && dbFile.getError().code() == error_code_file_not_found &&
		    !fileExists(apath) && // db file is missing
		    !walFile.isError() && fileExists(walpath)) // ..but WAL file is present
		{
			// Either we died partway through creating this DB, or died partway through deleting it, or someone is
			// monkeying with our files Create a new blank DB by backing up the WAL file (just in case it is important)
			// and then hitting the next case
			walFile = file_not_found();
			renameFile(walpath, walpath + "-old-" + deterministicRandom()->randomUniqueID().toString());
			ASSERT_WE_THINK(false); //< This code should not be hit in FoundationDB at the moment, because worker looks
			                        // for databases to open by listing .fdb files, not .fdb-wal files
			// TEST(true);  // Replace a partially constructed or destructed DB
		}

		if (dbFile.isError() && walFile.isError() && writable &&
		    dbFile.getError().code() == error_code_file_not_found &&
		    walFile.getError().code() == error_code_file_not_found && !fileExists(apath) && !fileExists(walpath)) {
			// The file doesn't exist, try to create a new one
			// Creating the WAL before the database ensures we will not try to open a database with no WAL
			walFile = waitForAndGet(IAsyncFileSystem::filesystem()->open(
			    walpath,
			    IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE |
			        IAsyncFile::OPEN_LOCK,
			    0600));
			waitFor(walFile.get()->sync());
			dbFile = waitForAndGet(IAsyncFileSystem::filesystem()->open(
			    apath,
			    IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE |
			        IAsyncFile::OPEN_LOCK,
			    0600));
			if (page_checksums)
				waitFor(
				    dbFile.get()->write(template_fdb_with_page_checksums, sizeof(template_fdb_with_page_checksums), 0));
			else
				waitFor(dbFile.get()->write(
				    template_fdb_without_page_checksums, sizeof(template_fdb_without_page_checksums), 0));
			waitFor(dbFile.get()->sync()); // renames filename.part to filename, fsyncs data and directory
			TraceEvent("CreatedDBFile").detail("Filename", apath);
		}
	}
	if (dbFile.isError())
		throw dbFile.getError(); // If we've failed to open the file, throw an exception
	if (walFile.isError())
		throw walFile.getError(); // If we've failed to open the file, throw an exception

	// Set Rate control if SERVER_KNOBS are positive
	if (SERVER_KNOBS->SQLITE_WRITE_WINDOW_LIMIT > 0 && SERVER_KNOBS->SQLITE_WRITE_WINDOW_SECONDS > 0) {
		// The writer thread is created before the readers, so it should initialize the rate controls.
		if (writable) {
			// Create a new rate control and assign it to both files.
			Reference<SpeedLimit> rc(
			    new SpeedLimit(SERVER_KNOBS->SQLITE_WRITE_WINDOW_LIMIT, SERVER_KNOBS->SQLITE_WRITE_WINDOW_SECONDS));
			dbFile.get()->setRateControl(rc);
			walFile.get()->setRateControl(rc);
		} else {
			// When a reader thread is opened, the rate controls should already be equal and not null
			ASSERT(dbFile.get()->getRateControl() == walFile.get()->getRateControl());
			ASSERT(dbFile.get()->getRateControl());
		}
	}

	//TraceEvent("KVThreadInitStage").detail("Stage",2).detail("Filename", filename).detail("Writable", writable);

	// Now that the file itself is open and locked, let sqlite open the database
	// Note that VFSAsync will also call g_network->open (including for the WAL), so its flags are important, too
	int result =
	    sqlite3_open_v2(apath.c_str(), &db, (writable ? SQLITE_OPEN_READWRITE : SQLITE_OPEN_READONLY), nullptr);
	checkError("open", result);

	int chunkSize;
	if (!g_network->isSimulated()) {
		chunkSize = 4096 * SERVER_KNOBS->SQLITE_CHUNK_SIZE_PAGES;
	} else if (BUGGIFY) {
		chunkSize = 4096 * deterministicRandom()->randomInt(0, 100);
	} else {
		chunkSize = 4096 * SERVER_KNOBS->SQLITE_CHUNK_SIZE_PAGES_SIM;
	}
	checkError("setChunkSize", sqlite3_file_control(db, nullptr, SQLITE_FCNTL_CHUNK_SIZE, &chunkSize));

	btree = db->aDb[0].pBt;
	initPagerCodec();

	sqlite3_extended_result_codes(db, 1);

	//TraceEvent("KVThreadInitStage").detail("Stage",3).detail("Filename", filename).detail("Writable", writable);

	// Statement(*this, "PRAGMA cache_size = 100").execute();

	Statement jm(*this, "PRAGMA journal_mode");
	ASSERT(jm.nextRow());
	if (jm.column(0) != LiteralStringRef("wal")) {
		TraceEvent(SevError, "JournalModeError").detail("Filename", filename).detail("Mode", jm.column(0));
		ASSERT(false);
	}

	if (writable) {
		Statement(*this, "PRAGMA synchronous = NORMAL").execute(); // OFF, NORMAL, FULL
		Statement(*this, "PRAGMA wal_autocheckpoint = -1").nextRow();
	}

	//TraceEvent("KVThreadInitStage").detail("Stage",4).detail("Filename", filename).detail("Writable", writable);

	sqlite3_mutex_enter(db->mutex);
	haveMutex = true;

	table = 3;
	freetable = 4;
	this->dbFile = dbFile.get();
	this->walFile = walFile.get();

	TraceEvent("KVThreadInitTime")
	    .detail("Elapsed", DEBUG_DETERMINISM ? 0 : timer() - startT)
	    .detail("Filename", filename)
	    .detail("Writable", writable);
	ASSERT(vfsAsyncIsOpen(filename));
}

void SQLiteDB::createFromScratch() {
	int sqliteFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	checkError("open", sqlite3_open_v2(filename.c_str(), &db, sqliteFlags, nullptr));

	Statement(*this, "PRAGMA page_size = 4096").nextRow(); // fast
	btree = db->aDb[0].pBt;
	initPagerCodec();

	Statement(*this, "PRAGMA auto_vacuum = 2").nextRow(); // slow all the time
	Statement(*this, "PRAGMA journal_mode = WAL").nextRow(); // sometimes slow
	sqlite3_extended_result_codes(db, 1);

	sqlite3_mutex_enter(db->mutex);
	haveMutex = true;

	beginTransaction(true);
	u32 pgnoRoot = -1;
	sqlite3BtreeGetMeta(btree, BTREE_LARGEST_ROOT_PAGE, &pgnoRoot);

	// We expect our tables are #3, #4 (since autovacuum is enabled, there is a pointer map page at #2)
	if (pgnoRoot == 4) {
		table = pgnoRoot - 1;
		freetable = pgnoRoot;
		rollback();
	} else if (pgnoRoot == 1) {
		// The database is empty; create tables
		checkError("BtreeCreateTable", sqlite3BtreeCreateTable(btree, &table, BTREE_BLOBKEY));
		ASSERT(table == 3);
		checkError("BtreeCreateTable2", sqlite3BtreeCreateTable(btree, &freetable, BTREE_INTKEY));
		ASSERT(freetable == table + 1);
		endTransaction();
	} else {
		TraceEvent("PgnoRoot").detail("Value", pgnoRoot);
		checkError("CheckTables", SQLITE_CORRUPT);
	}
}

struct ThreadSafeCounter {
	volatile int64_t counter;
	ThreadSafeCounter() : counter(0) {}
	void operator++() { interlockedIncrement64(&counter); }
	void operator--() { interlockedDecrement64(&counter); }
	operator int64_t() const { return counter; }
};

class KeyValueStoreSQLite final : public IKeyValueStore {
public:
	void dispose() override { doClose(this, true); }
	void close() override { doClose(this, false); }

	Future<Void> getError() const override { return delayed(readThreads->getError() || writeThread->getError()); }
	Future<Void> onClosed() const override { return stopped.getFuture(); }

	KeyValueStoreType getType() const override { return type; }
	StorageBytes getStorageBytes() const override;

	void set(KeyValueRef keyValue, const Arena* arena = nullptr) override;
	void clear(KeyRangeRef range, const Arena* arena = nullptr) override;
	Future<Void> commit(bool sequential = false) override;

	Future<Optional<Value>> readValue(KeyRef key, IKeyValueStore::ReadType, Optional<UID> debugID) override;
	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        IKeyValueStore::ReadType,
	                                        Optional<UID> debugID) override;
	Future<RangeResult> readRange(KeyRangeRef keys, int rowLimit, int byteLimit, IKeyValueStore::ReadType) override;

	KeyValueStoreSQLite(std::string const& filename,
	                    UID logID,
	                    KeyValueStoreType type,
	                    bool checkChecksums,
	                    bool checkIntegrity);
	~KeyValueStoreSQLite() override;

	struct SpringCleaningWorkPerformed {
		int lazyDeletePages = 0;
		int vacuumedPages = 0;
	};

	Future<SpringCleaningWorkPerformed> doClean();
	void startReadThreads();

private:
	KeyValueStoreType type;
	UID logID;
	std::string filename;
	Reference<IThreadPool> readThreads, writeThread;
	Promise<Void> stopped;
	Future<Void> cleaning, logging, starting, stopOnErr;

	int64_t readsRequested, writesRequested;
	ThreadSafeCounter readsComplete;
	volatile int64_t writesComplete;
	volatile SpringCleaningStats springCleaningStats;
	volatile int64_t diskBytesUsed;
	volatile int64_t freeListPages;

	std::vector<Reference<ReadCursor>> readCursors;
	Reference<IAsyncFile> dbFile, walFile;

	struct Reader : IThreadPoolReceiver {
		SQLiteDB conn;
		ThreadSafeCounter& counter;
		UID dbgid;
		Reference<ReadCursor>* ppReadCursor;

		explicit Reader(std::string const& filename,
		                bool is_btree_v2,
		                ThreadSafeCounter& counter,
		                UID dbgid,
		                Reference<ReadCursor>* ppReadCursor)
		  : conn(filename, is_btree_v2, is_btree_v2), counter(counter), dbgid(dbgid), ppReadCursor(ppReadCursor) {}
		~Reader() override { ppReadCursor->clear(); }

		void init() override { conn.open(false); }

		Reference<ReadCursor> getCursor() {
			Reference<ReadCursor> cursor = *ppReadCursor;
			if (!cursor || cursor->get().kvBytesRead > SERVER_KNOBS->SQLITE_CURSOR_MAX_LIFETIME_BYTES) {
				*ppReadCursor = cursor = makeReference<ReadCursor>();
				cursor->init(conn);
			}
			return cursor;
		}

		struct ReadValueAction final : TypedAction<Reader, ReadValueAction>, FastAllocated<ReadValueAction> {
			Key key;
			Optional<UID> debugID;
			ThreadReturnPromise<Optional<Value>> result;
			ReadValueAction(Key key, Optional<UID> debugID) : key(key), debugID(debugID){};
			double getTimeEstimate() const override { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
		};
		void action(ReadValueAction& rv) {
			// double t = timer();
			if (rv.debugID.present())
				g_traceBatch.addEvent("GetValueDebug",
				                      rv.debugID.get().first(),
				                      "Reader.Before"); //.detail("TaskID", g_network->getCurrentTask());

			rv.result.send(getCursor()->get().get(rv.key));
			++counter;

			if (rv.debugID.present())
				g_traceBatch.addEvent("GetValueDebug",
				                      rv.debugID.get().first(),
				                      "Reader.After"); //.detail("TaskID", g_network->getCurrentTask());
			// t = timer()-t;
			// if (t >= 1.0) TraceEvent("ReadValueActionSlow",dbgid).detail("Elapsed", t);
		}

		struct ReadValuePrefixAction final : TypedAction<Reader, ReadValuePrefixAction>,
		                                     FastAllocated<ReadValuePrefixAction> {
			Key key;
			int maxLength;
			Optional<UID> debugID;
			ThreadReturnPromise<Optional<Value>> result;
			ReadValuePrefixAction(Key key, int maxLength, Optional<UID> debugID)
			  : key(key), maxLength(maxLength), debugID(debugID){};
			double getTimeEstimate() const override { return SERVER_KNOBS->READ_VALUE_TIME_ESTIMATE; }
		};
		void action(ReadValuePrefixAction& rv) {
			// double t = timer();
			if (rv.debugID.present())
				g_traceBatch.addEvent("GetValuePrefixDebug",
				                      rv.debugID.get().first(),
				                      "Reader.Before"); //.detail("TaskID", g_network->getCurrentTask());

			rv.result.send(getCursor()->get().getPrefix(rv.key, rv.maxLength));
			++counter;

			if (rv.debugID.present())
				g_traceBatch.addEvent("GetValuePrefixDebug",
				                      rv.debugID.get().first(),
				                      "Reader.After"); //.detail("TaskID", g_network->getCurrentTask());
			// t = timer()-t;
			// if (t >= 1.0) TraceEvent("ReadValuePrefixActionSlow",dbgid).detail("Elapsed", t);
		}

		struct ReadRangeAction final : TypedAction<Reader, ReadRangeAction>, FastAllocated<ReadRangeAction> {
			KeyRange keys;
			int rowLimit, byteLimit;
			ThreadReturnPromise<RangeResult> result;
			ReadRangeAction(KeyRange keys, int rowLimit, int byteLimit)
			  : keys(keys), rowLimit(rowLimit), byteLimit(byteLimit) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->READ_RANGE_TIME_ESTIMATE; }
		};
		void action(ReadRangeAction& rr) {
			rr.result.send(getCursor()->get().getRange(rr.keys, rr.rowLimit, rr.byteLimit));
			++counter;
		}
	};

	struct Writer : IThreadPoolReceiver {
		KeyValueStoreSQLite* kvs;
		SQLiteDB conn;
		Cursor* cursor;
		int commits;
		int setsThisCommit;
		bool freeTableEmpty; // true if we are sure the freetable (pages pending lazy deletion) is empty
		volatile int64_t& writesComplete;
		volatile SpringCleaningStats& springCleaningStats;
		volatile int64_t& diskBytesUsed;
		volatile int64_t& freeListPages;
		UID dbgid;
		std::vector<Reference<ReadCursor>>& readThreads;
		bool checkAllChecksumsOnOpen;
		bool checkIntegrityOnOpen;

		explicit Writer(KeyValueStoreSQLite* kvs,
		                bool isBtreeV2,
		                bool checkAllChecksumsOnOpen,
		                bool checkIntegrityOnOpen,
		                volatile int64_t& writesComplete,
		                volatile SpringCleaningStats& springCleaningStats,
		                volatile int64_t& diskBytesUsed,
		                volatile int64_t& freeListPages,
		                UID dbgid,
		                std::vector<Reference<ReadCursor>>* pReadThreads)
		  : kvs(kvs), conn(kvs->filename, isBtreeV2, isBtreeV2), cursor(nullptr), commits(), setsThisCommit(),
		    freeTableEmpty(false), writesComplete(writesComplete), springCleaningStats(springCleaningStats),
		    diskBytesUsed(diskBytesUsed), freeListPages(freeListPages), dbgid(dbgid), readThreads(*pReadThreads),
		    checkAllChecksumsOnOpen(checkAllChecksumsOnOpen), checkIntegrityOnOpen(checkIntegrityOnOpen) {}
		~Writer() override {
			TraceEvent("KVWriterDestroying", dbgid).log();
			delete cursor;
			TraceEvent("KVWriterDestroyed", dbgid).log();
		}
		void init() override {
			if (checkAllChecksumsOnOpen) {
				if (conn.checkAllPageChecksums() != 0) {
					// It's not strictly necessary to discard the file immediately if a page checksum error is found
					// because most of the file could be valid and bad pages will be detected if they are read.
					// However, we shouldn't use the file unless we absolutely have to because some range(s) of keys
					// have effectively lost a replica.
					throw file_corrupt();
				}
			}
			conn.open(true);
			kvs->dbFile = conn.dbFile;
			kvs->walFile = conn.walFile;

			// If a wal file fails during the commit process before finishing a checkpoint, then it is possible that our
			// wal file will be non-empty when we reload it.  We execute a checkpoint here to remedy that situation.
			// This call must come before before creating a cursor because it will fail if there are any outstanding
			// transactions.
			fullCheckpoint();

			cursor = new Cursor(conn, true);

			if (checkIntegrityOnOpen || EXPENSIVE_VALIDATION) {
				if (conn.check(false) != 0) {
					// A corrupt btree structure must not be used.
					if (g_network->isSimulated() && VFSAsyncFile::checkInjectedError()) {
						throw file_corrupt().asInjectedFault();
					} else {
						throw file_corrupt();
					}
				}
			}
		}

		struct InitAction final : TypedAction<Writer, InitAction>, FastAllocated<InitAction> {
			ThreadReturnPromise<Void> result;
			double getTimeEstimate() const override { return 0; }
		};
		void action(InitAction& a) {
			// init() has already been called
			a.result.send(Void());
		}

		struct SetAction final : TypedAction<Writer, SetAction>, FastAllocated<SetAction> {
			KeyValue kv;
			SetAction(KeyValue kv) : kv(kv) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->SET_TIME_ESTIMATE; }
		};
		void action(SetAction& a) {
			double s = now();
			checkFreePages();
			cursor->set(a.kv);
			++setsThisCommit;
			++writesComplete;
			if (g_network->isSimulated() && g_simulator.getCurrentProcess()->rebooting)
				TraceEvent("SetActionFinished", dbgid).detail("Elapsed", now() - s);
		}

		struct ClearAction final : TypedAction<Writer, ClearAction>, FastAllocated<ClearAction> {
			KeyRange range;
			ClearAction(KeyRange range) : range(range) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->CLEAR_TIME_ESTIMATE; }
		};
		void action(ClearAction& a) {
			double s = now();
			cursor->fastClear(a.range, freeTableEmpty);
			cursor->clear(a.range); // TODO: at most one
			++writesComplete;
			if (g_network->isSimulated() && g_simulator.getCurrentProcess()->rebooting)
				TraceEvent("ClearActionFinished", dbgid).detail("Elapsed", now() - s);
		}

		struct CommitAction final : TypedAction<Writer, CommitAction>, FastAllocated<CommitAction> {
			double issuedTime;
			ThreadReturnPromise<Void> result;
			CommitAction() : issuedTime(now()) {}
			double getTimeEstimate() const override { return SERVER_KNOBS->COMMIT_TIME_ESTIMATE; }
		};
		void action(CommitAction& a) {
			double t1 = now();
			cursor->commit();
			delete cursor;
			cursor = nullptr;

			double t2 = now();

			fullCheckpoint();

			double t3 = now();

			++commits;
			// if ( !(commits % 100) )
			// printf("dbf=%lld bytes, wal=%lld bytes\n", getFileSize((kv->filename+".fdb").c_str()),
			// getFileSize((kv->filename+".fdb-wal").c_str()));

			a.result.send(Void());

			cursor = new Cursor(conn, true);
			checkFreePages();
			++writesComplete;
			if (t3 - a.issuedTime > 10.0 * deterministicRandom()->random01())
				TraceEvent("KVCommit10sSample", dbgid)
				    .detail("Queued", t1 - a.issuedTime)
				    .detail("Commit", t2 - t1)
				    .detail("Checkpoint", t3 - t2);

			diskBytesUsed = waitForAndGet(conn.dbFile->size()) + waitForAndGet(conn.walFile->size());

			if (g_network->isSimulated() && g_simulator.getCurrentProcess()->rebooting)
				TraceEvent("CommitActionFinished", dbgid).detail("Elapsed", now() - t1);
		}

		// Checkpoints the database and resets the wal file back to the beginning
		void fullCheckpoint() {
			// A checkpoint cannot succeed while there is an outstanding transaction
			ASSERT(cursor == nullptr);

			resetReaders();
			conn.checkpoint(false);

			resetReaders();
			conn.checkpoint(true);
		}

		void resetReaders() {
			for (int i = 0; i < readThreads.size(); i++)
				readThreads[i].clear();
		}
		void checkFreePages() {
			int64_t freeListSize = freeListPages;
			while (!freeTableEmpty && freeListSize < SERVER_KNOBS->CHECK_FREE_PAGE_AMOUNT) {
				int deletedPages = cursor->lazyDelete(SERVER_KNOBS->CHECK_FREE_PAGE_AMOUNT);
				freeTableEmpty = (deletedPages != SERVER_KNOBS->CHECK_FREE_PAGE_AMOUNT);
				springCleaningStats.lazyDeletePages += deletedPages;

				freeListSize = conn.freePages();
			}

			freeListPages = freeListSize;
			// if (iterations) printf("Lazy free: %d pages on freelist, %d iterations, freeTableEmpty=%d\n",
			// freeListPages, iterationsi, freeTableEmpty);
		}

		struct SpringCleaningAction final : TypedAction<Writer, SpringCleaningAction>,
		                                    FastAllocated<SpringCleaningAction> {
			ThreadReturnPromise<SpringCleaningWorkPerformed> result;
			double getTimeEstimate() const override {
				return std::max(SERVER_KNOBS->SPRING_CLEANING_LAZY_DELETE_TIME_ESTIMATE,
				                SERVER_KNOBS->SPRING_CLEANING_VACUUM_TIME_ESTIMATE);
			}
		};
		void action(SpringCleaningAction& a) {
			double s = now();
			double lazyDeleteEnd = now() + SERVER_KNOBS->SPRING_CLEANING_LAZY_DELETE_TIME_ESTIMATE;
			double vacuumEnd = now() + SERVER_KNOBS->SPRING_CLEANING_VACUUM_TIME_ESTIMATE;

			SpringCleaningWorkPerformed workPerformed;

			double lazyDeleteTime = 0;
			double vacuumTime = 0;

			const double lazyDeleteBatchProbability =
			    1.0 / (1 + SERVER_KNOBS->SPRING_CLEANING_VACUUMS_PER_LAZY_DELETE_PAGE *
			                   std::max(1, SERVER_KNOBS->SPRING_CLEANING_LAZY_DELETE_BATCH_SIZE));
			bool vacuumFinished = false;

			loop {
				double begin = now();
				bool canDelete = !freeTableEmpty &&
				                 (now() < lazyDeleteEnd || workPerformed.lazyDeletePages <
				                                               SERVER_KNOBS->SPRING_CLEANING_MIN_LAZY_DELETE_PAGES) &&
				                 workPerformed.lazyDeletePages < SERVER_KNOBS->SPRING_CLEANING_MAX_LAZY_DELETE_PAGES;

				bool canVacuum = !vacuumFinished &&
				                 (now() < vacuumEnd ||
				                  workPerformed.vacuumedPages < SERVER_KNOBS->SPRING_CLEANING_MIN_VACUUM_PAGES) &&
				                 workPerformed.vacuumedPages < SERVER_KNOBS->SPRING_CLEANING_MAX_VACUUM_PAGES;

				if (!canDelete && !canVacuum) {
					break;
				}

				if (canDelete && (!canVacuum || deterministicRandom()->random01() < lazyDeleteBatchProbability)) {
					TEST(canVacuum); // SQLite lazy deletion when vacuuming is active
					TEST(!canVacuum); // SQLite lazy deletion when vacuuming is inactive

					int pagesToDelete = std::max(
					    1,
					    std::min(SERVER_KNOBS->SPRING_CLEANING_LAZY_DELETE_BATCH_SIZE,
					             SERVER_KNOBS->SPRING_CLEANING_MAX_LAZY_DELETE_PAGES - workPerformed.lazyDeletePages));
					int pagesDeleted = cursor->lazyDelete(pagesToDelete);
					freeTableEmpty = (pagesDeleted != pagesToDelete);
					workPerformed.lazyDeletePages += pagesDeleted;
					lazyDeleteTime += now() - begin;
				} else {
					ASSERT(canVacuum);
					TEST(canDelete); // SQLite vacuuming when lazy delete is active
					TEST(!canDelete); // SQLite vacuuming when lazy delete is inactive
					TEST(SERVER_KNOBS->SPRING_CLEANING_VACUUMS_PER_LAZY_DELETE_PAGE !=
					     0); // SQLite vacuuming with nonzero vacuums_per_lazy_delete_page

					vacuumFinished = conn.vacuum();
					if (!vacuumFinished) {
						++workPerformed.vacuumedPages;
					}

					vacuumTime += now() - begin;
				}

				CoroThreadPool::waitFor(yield());
			}

			freeListPages = conn.freePages();

			TEST(workPerformed.lazyDeletePages > 0); // Pages lazily deleted
			TEST(workPerformed.vacuumedPages > 0); // Pages vacuumed
			TEST(vacuumTime > 0); // Time spent vacuuming
			TEST(lazyDeleteTime > 0); // Time spent lazy deleting

			TraceEvent("SQLiteCleanActionStats")
			    .detail("LazyDeletePages", workPerformed.lazyDeletePages)
			    .detail("VacuumedPage", workPerformed.vacuumedPages)
			    .detail("VacuumTime", vacuumTime)
			    .detail("LazyDeleteTime", lazyDeleteTime);
			++springCleaningStats.springCleaningCount;
			springCleaningStats.lazyDeletePages += workPerformed.lazyDeletePages;
			springCleaningStats.vacuumedPages += workPerformed.vacuumedPages;
			springCleaningStats.springCleaningTime += now() - s;
			springCleaningStats.vacuumTime += vacuumTime;
			springCleaningStats.lazyDeleteTime += lazyDeleteTime;

			a.result.send(workPerformed);
			++writesComplete;
			if (g_network->isSimulated() && g_simulator.getCurrentProcess()->rebooting)
				TraceEvent("SpringCleaningActionFinished", dbgid).detail("Elapsed", now() - s);
		}
	};

	ACTOR static Future<Void> logPeriodically(KeyValueStoreSQLite* self) {
		state int64_t lastReadsComplete = 0;
		state int64_t lastWritesComplete = 0;
		loop {
			wait(delay(SERVER_KNOBS->DISK_METRIC_LOGGING_INTERVAL));

			int64_t rc = self->readsComplete, wc = self->writesComplete;
			TraceEvent("DiskMetrics", self->logID)
			    .detail("ReadOps", rc - lastReadsComplete)
			    .detail("WriteOps", wc - lastWritesComplete)
			    .detail("ReadQueue", self->readsRequested - rc)
			    .detail("WriteQueue", self->writesRequested - wc)
			    .detail("GlobalSQLiteMemoryHighWater", (int64_t)sqlite3_memory_highwater(1));

			TraceEvent("SpringCleaningMetrics", self->logID)
			    .detail("SpringCleaningCount", self->springCleaningStats.springCleaningCount)
			    .detail("LazyDeletePages", self->springCleaningStats.lazyDeletePages)
			    .detail("VacuumedPages", self->springCleaningStats.vacuumedPages)
			    .detail("SpringCleaningTime", self->springCleaningStats.springCleaningTime)
			    .detail("LazyDeleteTime", self->springCleaningStats.lazyDeleteTime)
			    .detail("VacuumTime", self->springCleaningStats.vacuumTime);

			lastReadsComplete = self->readsComplete;
			lastWritesComplete = self->writesComplete;
		}
	}

	void disableRateControl() {
		if (dbFile && dbFile->getRateControl()) {
			TraceEvent(SevDebug, "KeyValueStoreSQLiteShutdownRateControl").detail("Filename", dbFile->getFilename());
			Reference<IRateControl> rc = dbFile->getRateControl();
			dbFile->setRateControl({});
			rc->wakeWaiters();
		}
		dbFile.clear();

		if (walFile && walFile->getRateControl()) {
			TraceEvent(SevDebug, "KeyValueStoreSQLiteShutdownRateControl").detail("Filename", walFile->getFilename());
			Reference<IRateControl> rc = walFile->getRateControl();
			walFile->setRateControl({});
			rc->wakeWaiters();
		}
		walFile.clear();
	}

	ACTOR static Future<Void> stopOnError(KeyValueStoreSQLite* self) {
		try {
			wait(self->readThreads->getError() || self->writeThread->getError());
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;

			self->disableRateControl();
			self->readThreads->stop(e).isReady();
			self->writeThread->stop(e).isReady();
		}

		return Void();
	}

	ACTOR static void doClose(KeyValueStoreSQLite* self, bool deleteOnClose) {
		state Error error = success();

		self->disableRateControl();

		try {
			TraceEvent("KVClose", self->logID).detail("Filename", self->filename).detail("Del", deleteOnClose);
			self->starting.cancel();
			self->cleaning.cancel();
			self->logging.cancel();
			wait(self->readThreads->stop() && self->writeThread->stop());
			if (deleteOnClose) {
				wait(IAsyncFileSystem::filesystem()->incrementalDeleteFile(self->filename, true));
				wait(IAsyncFileSystem::filesystem()->incrementalDeleteFile(self->filename + "-wal", false));
			}
		} catch (Error& e) {
			TraceEvent(SevError, "KVDoCloseError", self->logID)
			    .errorUnsuppressed(e)
			    .detail("Filename", self->filename)
			    .detail("Reason", e.code() == error_code_platform_error ? "could not delete database" : "unknown");
			error = e;
		}

		TraceEvent("KVClosed", self->logID).detail("Filename", self->filename);
		if (error.code() != error_code_actor_cancelled) {
			self->stopped.send(Void());
			delete self;
		}
	}
};
IKeyValueStore* keyValueStoreSQLite(std::string const& filename,
                                    UID logID,
                                    KeyValueStoreType storeType,
                                    bool checkChecksums,
                                    bool checkIntegrity) {
	return new KeyValueStoreSQLite(filename, logID, storeType, checkChecksums, checkIntegrity);
}

ACTOR Future<Void> cleanPeriodically(KeyValueStoreSQLite* self) {
	wait(delayJittered(SERVER_KNOBS->SPRING_CLEANING_NO_ACTION_INTERVAL));
	loop {
		KeyValueStoreSQLite::SpringCleaningWorkPerformed workPerformed = wait(self->doClean());

		double duration = std::numeric_limits<double>::max();
		if (workPerformed.lazyDeletePages >= SERVER_KNOBS->SPRING_CLEANING_LAZY_DELETE_BATCH_SIZE) {
			duration = std::min(duration, SERVER_KNOBS->SPRING_CLEANING_LAZY_DELETE_INTERVAL);
		}
		if (workPerformed.vacuumedPages > 0) {
			duration = std::min(duration, SERVER_KNOBS->SPRING_CLEANING_VACUUM_INTERVAL);
		}
		if (duration == std::numeric_limits<double>::max()) {
			duration = SERVER_KNOBS->SPRING_CLEANING_NO_ACTION_INTERVAL;
		}

		wait(delayJittered(duration));
	}
}

ACTOR static Future<Void> startReadThreadsWhen(KeyValueStoreSQLite* kv, Future<Void> onReady, UID id) {
	wait(onReady);
	kv->startReadThreads();
	return Void();
}

sqlite3_vfs* vfsAsync();
static int vfs_registered = 0;

KeyValueStoreSQLite::KeyValueStoreSQLite(std::string const& filename,
                                         UID id,
                                         KeyValueStoreType storeType,
                                         bool checkChecksums,
                                         bool checkIntegrity)
  : type(storeType), logID(id), filename(filename), readThreads(CoroThreadPool::createThreadPool()),
    writeThread(CoroThreadPool::createThreadPool()), readsRequested(0), writesRequested(0), writesComplete(0),
    diskBytesUsed(0), freeListPages(0) {
	TraceEvent(SevDebug, "KeyValueStoreSQLiteCreate").detail("Filename", filename);

	stopOnErr = stopOnError(this);

#if SQLITE_THREADSAFE == 0
	ASSERT(writeThread->isCoro());
#endif

	if (!vfs_registered && writeThread->isCoro())
		if (sqlite3_vfs_register(vfsAsync(), true) != SQLITE_OK)
			ASSERT(false);

	// The DB file should not already be open
	ASSERT(!vfsAsyncIsOpen(filename));
	ASSERT(!vfsAsyncIsOpen(filename + "-wal"));

	readCursors.resize(SERVER_KNOBS->SQLITE_READER_THREADS); //< number of read threads

	sqlite3_soft_heap_limit64(SERVER_KNOBS->SOFT_HEAP_LIMIT); // SOMEDAY: Is this a performance issue?  Should we drop
	                                                          // the cache sizes for individual threads?
	TaskPriority taskId = g_network->getCurrentTask();
	g_network->setCurrentTask(TaskPriority::DiskWrite);
	// Note: the below is actually a coroutine and not a thread.
	writeThread->addThread(new Writer(this,
	                                  type == KeyValueStoreType::SSD_BTREE_V2,
	                                  checkChecksums,
	                                  checkIntegrity,
	                                  writesComplete,
	                                  springCleaningStats,
	                                  diskBytesUsed,
	                                  freeListPages,
	                                  id,
	                                  &readCursors),
	                       "fdb-sqlite-wr");
	g_network->setCurrentTask(taskId);
	auto p = new Writer::InitAction();
	auto f = p->result.getFuture();
	writeThread->post(p);
	starting = startReadThreadsWhen(this, f, logID);
	cleaning = cleanPeriodically(this);
	logging = logPeriodically(this);
}
KeyValueStoreSQLite::~KeyValueStoreSQLite() {
	// printf("dbf=%lld bytes, wal=%lld bytes\n", getFileSize((filename+".fdb").c_str()),
	// getFileSize((filename+".fdb-wal").c_str()));
}

StorageBytes KeyValueStoreSQLite::getStorageBytes() const {
	int64_t free;
	int64_t total;

	g_network->getDiskBytes(parentDirectory(filename), free, total);

	return StorageBytes(free, total, diskBytesUsed, free + _PAGE_SIZE * freeListPages);
}

void KeyValueStoreSQLite::startReadThreads() {
	int nReadThreads = readCursors.size();
	TaskPriority taskId = g_network->getCurrentTask();
	g_network->setCurrentTask(TaskPriority::DiskRead);
	for (int i = 0; i < nReadThreads; i++) {
		std::string threadName = format("fdb-sqlite-r-%d", i);
		if (threadName.size() > 15) {
			threadName = "fdb-sqlite-r";
		}
		//  Note: the below is actually a coroutine and not a thread.
		readThreads->addThread(
		    new Reader(filename, type == KeyValueStoreType::SSD_BTREE_V2, readsComplete, logID, &readCursors[i]),
		    threadName.c_str());
	}
	g_network->setCurrentTask(taskId);
}

void KeyValueStoreSQLite::set(KeyValueRef keyValue, const Arena* arena) {
	++writesRequested;
	writeThread->post(new Writer::SetAction(keyValue));
}
void KeyValueStoreSQLite::clear(KeyRangeRef range, const Arena* arena) {
	++writesRequested;
	writeThread->post(new Writer::ClearAction(range));
}
Future<Void> KeyValueStoreSQLite::commit(bool sequential) {
	++writesRequested;
	auto p = new Writer::CommitAction;
	auto f = p->result.getFuture();
	writeThread->post(p);
	return f;
}
Future<Optional<Value>> KeyValueStoreSQLite::readValue(KeyRef key, IKeyValueStore::ReadType, Optional<UID> debugID) {
	++readsRequested;
	auto p = new Reader::ReadValueAction(key, debugID);
	auto f = p->result.getFuture();
	readThreads->post(p);
	return f;
}
Future<Optional<Value>> KeyValueStoreSQLite::readValuePrefix(KeyRef key,
                                                             int maxLength,
                                                             IKeyValueStore::ReadType,
                                                             Optional<UID> debugID) {
	++readsRequested;
	auto p = new Reader::ReadValuePrefixAction(key, maxLength, debugID);
	auto f = p->result.getFuture();
	readThreads->post(p);
	return f;
}
Future<RangeResult> KeyValueStoreSQLite::readRange(KeyRangeRef keys,
                                                   int rowLimit,
                                                   int byteLimit,
                                                   IKeyValueStore::ReadType) {
	++readsRequested;
	auto p = new Reader::ReadRangeAction(keys, rowLimit, byteLimit);
	auto f = p->result.getFuture();
	readThreads->post(p);
	return f;
}
Future<KeyValueStoreSQLite::SpringCleaningWorkPerformed> KeyValueStoreSQLite::doClean() {
	++writesRequested;
	auto p = new Writer::SpringCleaningAction;
	auto f = p->result.getFuture();
	writeThread->post(p);
	return f;
}

void createTemplateDatabase() {
	ASSERT(!vfs_registered);
	SQLiteDB db1("template.fdb", false, false);
	SQLiteDB db2("template.sqlite", true, true);
	db1.createFromScratch();
	db2.createFromScratch();
}

void GenerateIOLogChecksumFile(std::string filename) {
	if (!fileExists(filename)) {
		throw file_not_found();
	}

	FILE* f = fopen(filename.c_str(), "r");
	FILE* fout = fopen((filename + ".checksums").c_str(), "w");
	uint8_t buf[4096];
	unsigned int c = 0;
	while (fread(buf, 1, 4096, f) > 0)
		fprintf(fout, "%u %u\n", c++, hashlittle(buf, 4096, 0xab12fd93));
	fclose(f);
	fclose(fout);
}

// If integrity is true, a full btree integrity check is done.
// If integrity is false, only a scan of all pages to validate their checksums is done.
ACTOR Future<Void> KVFileCheck(std::string filename, bool integrity) {
	if (!fileExists(filename))
		throw file_not_found();

	StringRef kvFile(filename);
	KeyValueStoreType type = KeyValueStoreType::END;
	if (kvFile.endsWith(LiteralStringRef(".fdb")))
		type = KeyValueStoreType::SSD_BTREE_V1;
	else if (kvFile.endsWith(LiteralStringRef(".sqlite")))
		type = KeyValueStoreType::SSD_BTREE_V2;
	ASSERT(type != KeyValueStoreType::END);

	state IKeyValueStore* store = keyValueStoreSQLite(filename, UID(0, 0), type, !integrity, integrity);
	ASSERT(store != nullptr);

	// Wait for integry check to finish
	wait(success(store->readValue(StringRef())));

	if (store->getError().isError())
		wait(store->getError());
	Future<Void> c = store->onClosed();
	store->close();
	wait(c);

	return Void();
}

ACTOR Future<Void> KVFileDump(std::string filename) {
	if (!fileExists(filename))
		throw file_not_found();

	StringRef kvFile(filename);
	KeyValueStoreType type = KeyValueStoreType::END;
	if (kvFile.endsWith(".fdb"_sr))
		type = KeyValueStoreType::SSD_BTREE_V1;
	else if (kvFile.endsWith(".sqlite"_sr))
		type = KeyValueStoreType::SSD_BTREE_V2;
	ASSERT(type != KeyValueStoreType::END);

	state IKeyValueStore* store = keyValueStoreSQLite(filename, UID(0, 0), type);
	ASSERT(store != nullptr);

	// dump
	state int64_t count = 0;
	state Key k;
	state Key endk = allKeys.end;
	state bool debug = false;

	const char* startKey = getenv("FDB_DUMP_STARTKEY");
	const char* endKey = getenv("FDB_DUMP_ENDKEY");
	const char* debugS = getenv("FDB_DUMP_DEBUG");
	if (startKey != NULL)
		k = StringRef(unprintable(std::string(startKey)));
	if (endKey != NULL)
		endk = StringRef(unprintable(std::string(endKey)));
	if (debugS != NULL)
		debug = true;

	fprintf(stderr,
	        "Dump start: %s, end: %s, debug: %s\n",
	        printable(k).c_str(),
	        printable(endk).c_str(),
	        debug ? "true" : "false");

	while (true) {
		RangeResult kv = wait(store->readRange(KeyRangeRef(k, endk), 1000));
		for (auto& one : kv) {
			int size = 0;
			const uint8_t* data = NULL;

			size = one.key.size();
			data = one.key.begin();
			fwrite(&size, sizeof(int), 1, stdout);
			fwrite(data, sizeof(uint8_t), size, stdout);

			size = one.value.size();
			data = one.value.begin();
			fwrite(&size, sizeof(int), 1, stdout);
			fwrite(data, sizeof(uint8_t), size, stdout);

			if (debug) {
				fprintf(stderr, "key: %s\n", printable(one.key).c_str());
				fprintf(stderr, "val: %s\n", printable(one.value).c_str());
			}
		}

		count += kv.size();
		if (kv.size() <= 0)
			break;
		k = keyAfter(kv[kv.size() - 1].key);
	}
	fflush(stdout);
	fmt::print(stderr, "Counted: {}\n", count);

	if (store->getError().isError())
		wait(store->getError());
	Future<Void> c = store->onClosed();
	store->close();
	wait(c);

	return Void();
}
