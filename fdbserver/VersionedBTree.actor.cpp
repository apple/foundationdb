/*
 * VersionedBTree.actor.cpp
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

#include "flow/flow.h"
#include "fdbserver/IVersionedStore.h"
#include "fdbserver/IPager.h"
#include "fdbclient/Tuple.h"
#include "flow/serialize.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"
#include "fdbserver/MemoryPager.h"
#include "fdbserver/IndirectShadowPager.h"
#include <map>
#include <vector>
#include "fdbclient/CommitTransaction.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/PrefixTree.h"
#include <string.h>
#include "flow/actorcompiler.h"

// Convenience method for converting a Standalone to a Ref while adding its arena to another arena.
template<typename T> inline const Standalone<T> & dependsOn(Arena &arena, const Standalone<T> &s) {
	arena.dependsOn(s.arena());
	return s;
}

struct BTreePage {
	enum EPageFlags { IS_LEAF = 1};

#pragma pack(push,4)
	uint8_t flags;
	uint16_t count;
	uint32_t kvBytes;
	uint8_t extensionPageCount;
	LogicalPageID extensionPages[0];
#pragma pack(pop)

	PrefixTree & tree() {
		return *(PrefixTree *)(extensionPages + extensionPageCount);
	}

	const PrefixTree & tree() const {
		return *(const PrefixTree *)(extensionPages + extensionPageCount);
	}

	static inline int GetHeaderSize(int extensionPages = 0) {
		return sizeof(BTreePage) + extensionPages + sizeof(LogicalPageID);
	}

	std::string toString(bool write, LogicalPageID id, Version ver, StringRef lowerBoundKey, StringRef upperBoundKey) const {
		std::string r;
		r += format("BTreePage op=%s id=%d ver=%lld ptr=%p flags=0x%X count=%d kvBytes=%d\nlowerBoundKey='%s'\nupperBoundKey='%s'",
					write ? "write" : "read", id, ver, this, (int)flags, (int)count, (int)kvBytes,
					lowerBoundKey.toHexString(20).c_str(), upperBoundKey.toHexString(20).c_str());
		try {
			if(count > 0) {
				PrefixTree::Cursor c = tree().getCursor(lowerBoundKey, upperBoundKey);
				c.moveFirst();
				ASSERT(c.valid());

				do {
					r += "\n  ";
					Tuple t;
					try {
						t = Tuple::unpack(c.getKeyRef());
						for(int i = 0; i < t.size(); ++i) {
							if(i != 0)
								r += ",";
							if(t.getType(i) == Tuple::ElementType::BYTES)
								r += format("'%s'", t.getString(i).printable().c_str());
							if(t.getType(i) == Tuple::ElementType::INT)
								r += format("%lld", t.getInt(i, true));
						}
					} catch(Error &e) {
					}
					r += format("['%s']", c.getKeyRef().toHexString(20).c_str());

					r += " -> ";
					if(flags & IS_LEAF)
						r += format("'%s'", c.getValueRef().toHexString(20).c_str());
					else
						r += format("Page id=%u", *(const uint32_t *)c.getValueRef().begin());

				} while(c.moveNext());
			}
		} catch(Error &e) {
			debug_printf("BTreePage::toString ERROR: %s\n", e.what());
			debug_printf("BTreePage::toString partial result: %s\n", r.c_str());
			throw;
		}

		return r;
	}
};

static void writeEmptyPage(Reference<IPage> page, uint8_t newFlags, int pageSize) {
	BTreePage *btpage = (BTreePage *)page->begin();
	btpage->flags = newFlags;
	btpage->kvBytes = 0;
	btpage->count = 0;
	btpage->extensionPageCount = 0;
	btpage->tree().build(nullptr, nullptr, StringRef(), StringRef());
}

struct BoundaryAndPage {
	Key lowerBound;
	// Only firstPage or multiPage will be in use at once
	Reference<IPage> firstPage;
	std::vector<Reference<IPage>> extPages;
};

// Returns a std::vector of pairs of lower boundary key indices within kvPairs and encoded pages.
template<typename Allocator>
static std::vector<BoundaryAndPage> buildPages(bool minimalBoundaries, StringRef lowerBound, StringRef upperBound,  std::vector<PrefixTree::EntryRef> entries, uint8_t newFlags, Allocator const &newBlockFn, int usableBlockSize) {
	// This is how much space for the binary tree exists in the page, after the header
	int pageSize = usableBlockSize - (BTreePage::GetHeaderSize() + PrefixTree::GetHeaderSize());

	// Each new block adds (usableBlockSize - sizeof(LogicalPageID)) more net usable space *for the binary tree* to pageSize.
	int netTreeBlockSize = usableBlockSize - sizeof(LogicalPageID);

	int blockCount = 1;
	std::vector<BoundaryAndPage> pages;

	// TODO:  Move all of this abstraction breaking stuff into PrefixTree in the form of a helper function or class.
	int kvBytes = 0;      // User key/value bytes in page
	int compressedBytes = 0;   // Conservative estimate of size of compressed page.  TODO: Make this exactly right if possible

	int start = 0;
	int i = 0;
	const int iEnd = entries.size();
	// Lower bound of the page being added to
	Key pageLowerBound = lowerBound;
	Key pageUpperBound;

	while(i <= iEnd) {
		bool end = i == iEnd;
		bool flush = end;

		// If not the end, add i to the page if necessary
		if(end) {
			pageUpperBound = upperBound;
		}
		else {
			// Common prefix with previous record
			const PrefixTree::EntryRef &entry = entries[i];
			int prefixLen = commonPrefixLength(entry.key, (i == start) ? pageLowerBound : entries[i - 1].key);
			int keySize = entry.key.size();
			int valueSize = entry.value.size();

			int spaceNeeded = valueSize + keySize - prefixLen + PrefixTree::Node::getMaxOverhead(i, entry.key.size(), entry.value.size());

			debug_printf("Trying to add record %d of %lu (i=%d) klen %d  vlen %d  prefixLen %d  spaceNeeded %d  usedSoFar %d/%d '%s'\n",
				i + 1, entries.size(), i, keySize, valueSize, prefixLen,
				spaceNeeded, compressedBytes, pageSize, entry.key.toHexString(15).c_str());

			int spaceAvailable = pageSize - compressedBytes;

			// Does it fit?
			bool fits = spaceAvailable >= spaceNeeded;

			// If it doesn't fit, either end the current page or increase the page size
			if(!fits) {
				// For leaf level where minimal boundaries are used require at least 1 entry, otherwise require 4 to enforce a minimum branching factor
				int minimumEntries = minimalBoundaries ? 1 : 4;
				int count = i - start;

				// If not enough entries or page less than half full, increase page size to make the entry fit
				if(count < minimumEntries || spaceAvailable > pageSize / 2) {
					// Figure out how many additional whole or partial blocks are needed
					int newBlocks = 1 + (spaceNeeded - spaceAvailable - 1) / netTreeBlockSize;
					int newPageSize = pageSize + (newBlocks * netTreeBlockSize);
					if(newPageSize <= PrefixTree::MaximumTreeSize()) {
						blockCount += newBlocks;
						pageSize = newPageSize;
						fits = true;
					}
				}
				if(!fits) {
					// Flush page
					if(minimalBoundaries) {
						// Note that prefixLen is guaranteed to be < entry.key.size() because entries are in increasing order and cannot repeat.
						int len = prefixLen + 1;
						if(entry.key[prefixLen] == 0)
							len = std::min(len + 1, entry.key.size());
						pageUpperBound = entry.key.substr(0, len);
					}
					else {
						pageUpperBound = entry.key;
					}
				}
			}

			// If the record fits then add it to the page set
			if(fits) {
				kvBytes += keySize + valueSize;
				compressedBytes += spaceNeeded;
				++i;
			}

			flush = !fits;
		}

		// If flush then write a page using records from start to i.  It's guaranteed that pageUpperBound has been set above.
		if(flush) {
			end = i == iEnd;  // i could have been moved above
			int count = i - start;
			debug_printf("Flushing page start=%d i=%d\nlower='%s'\nupper='%s'\n", start, i, pageLowerBound.toHexString(20).c_str(), pageUpperBound.toHexString(20).c_str());
			ASSERT(pageLowerBound <= pageUpperBound);
			for(int j = start; j < i; ++j) {
				debug_printf(" %d: %s -> %s\n", j, entries[j].key.toHexString(15).c_str(), entries[j].value.toHexString(15).c_str());
			}

			union {
				BTreePage *btPage;
				uint8_t *btPageMem;
			};

			if(blockCount == 1) {
				Reference<IPage> page = newBlockFn();
				btPageMem = page->mutate();
				pages.push_back({std::move(pageLowerBound), std::move(page)});
			}
			else {
				ASSERT(blockCount > 1);
				btPageMem = new uint8_t[usableBlockSize * blockCount];
#if VALGRIND
				// Prevent valgrind errors caused by writing random unneeded bytes to disk.
				memset(btPageMem, 0, usableBlockSize * blockCount);
#endif
			}

			btPage->flags = newFlags;
			btPage->kvBytes = kvBytes;
			btPage->count = i - start;
			btPage->extensionPageCount = blockCount - 1;

			int written = btPage->tree().build(&entries[start], &entries[i], pageLowerBound, pageUpperBound);
			if(written > pageSize) {
				fprintf(stderr, "ERROR:  Wrote %d bytes to %d byte page (%d blocks). recs %d  kvBytes %d  compressed %d\n", written, pageSize, blockCount, i - start, kvBytes, compressedBytes);
				ASSERT(false);
			}

			if(blockCount != 1) {
				Reference<IPage> page = newBlockFn();
				const uint8_t *rptr = btPageMem;
				memcpy(page->mutate(), rptr, usableBlockSize);
				rptr += usableBlockSize;
				
				std::vector<Reference<IPage>> extPages;
				for(int b = 1; b < blockCount; ++b) {
					Reference<IPage> extPage = newBlockFn();
					//debug_printf("block %d write offset %d\n", b, firstBlockSize + (b - 1) * usableBlockSize);
					memcpy(extPage->mutate(), rptr, usableBlockSize);
					rptr += usableBlockSize;
					extPages.push_back(std::move(extPage));
				}

				pages.push_back({std::move(pageLowerBound), std::move(page), std::move(extPages)});
				delete btPageMem;
			}

			if(end)
				break;
			start = i;
			kvBytes = 0;
			compressedBytes = 0;
			pageLowerBound = pageUpperBound;
		}
	}

	//debug_printf("buildPages: returning pages.size %lu, kvpairs %lu\n", pages.size(), kvPairs.size());
	return pages;
}

// Internal key/value records represent either a cleared key at a version or a shard of a value of a key at a version.
// When constructing and packing these it is assumed that the key and value memory is being held elsewhere.
struct KeyVersionValueRef {
	KeyVersionValueRef() : version(invalidVersion) {}
	// Cleared key at version
	KeyVersionValueRef(KeyRef key, Version ver, Optional<ValueRef> val = {}) 
		: key(key), version(ver), value(val), valueIndex(0)
	{
		if(value.present())
			valueTotalSize = value.get().size();
	}

	KeyVersionValueRef(Arena &a, const KeyVersionValueRef &toCopy) {
		key = KeyRef(a, toCopy.key);
		version = toCopy.version;
		if(toCopy.value.present()) {
			value = ValueRef(a, toCopy.value.get());
		}
		valueTotalSize = toCopy.valueTotalSize;
		valueIndex = toCopy.valueIndex;
	}

	static inline Key searchKey(StringRef key, Version ver) {
		Tuple t;
		t.append(key);
		t.append(ver);
		Standalone<VectorRef<uint8_t>> packed = t.getData();
		packed.append(packed.arena(), (const uint8_t *)"\xff", 1);
		return Key(KeyRef(packed.begin(), packed.size()), packed.arena());
	}

	KeyRef key;
	Version version;
	int64_t valueTotalSize;      // Total size of value, including all other KVV parts if multipart
	int64_t valueIndex;          // Index within reconstituted value of this part
	Optional<ValueRef> value;

	// Result undefined if value is not present
	bool isMultiPart() const { return value.get().size() != valueTotalSize; }
	bool valid() const { return version != invalidVersion; }

	// Generate a kv shard from a complete kv
	KeyVersionValueRef split(int start, int len) {
		ASSERT(value.present());
		KeyVersionValueRef r(key, version);
		r.value = value.get().substr(start, len);
		r.valueIndex = start;
		r.valueTotalSize = valueTotalSize;
		return r;
	}

	// Encode the record for writing to a btree page.
	// If copyValue is false, the value is not copied into the returned arena.
	//
	// Encoded forms:
	//  userKey, version                             - the value is present and complete (which includes an empty value)
	//  userKey, version, valueSize=0                - the key was deleted as of this version
	//  userKey, version, valueSize>=0, valuePart    - the value is present and spans multiple records
	inline PrefixTree::Entry pack(bool copyValue = true) const {
		Tuple t;
		t.append(key);
		t.append(version);

		if(!value.present()) {
			t.append(0);
		}
		else {
			if(isMultiPart()) {
				t.append(valueTotalSize);
				t.append(valueIndex);
			}
		}

		Key k = t.getDataAsStandalone();
		ValueRef v;
		if(value.present()) {
			v = copyValue ? StringRef(k.arena(), value.get()) : value.get();
		}

		return PrefixTree::Entry({k, v}, k.arena());
	}

	// Supports partial/incomplete encoded sequences.
	// Unpack an encoded key/value pair.
	// Both key and value will be in the returned arena unless copyValue is false in which case
	// the value will not be copied to the arena.
	static Standalone<KeyVersionValueRef> unpack(KeyValueRef kv, bool copyValue = true) {
		//debug_printf("Unpacking: '%s' -> '%s' \n", kv.key.toHexString(15).c_str(), kv.value.toHexString(15).c_str());
		Standalone<KeyVersionValueRef> result;
		if(kv.key.size() != 0) {
#if REDWOOD_DEBUG
			try { Tuple t = Tuple::unpack(kv.key); } catch(Error &e) { debug_printf("UNPACK FAIL %s %s\n", kv.key.toHexString(20).c_str(), platform::get_backtrace().c_str()); }
#endif
			Tuple k = Tuple::unpack(kv.key);
			int s = k.size();
			switch(s) {
				case 4:
					// Value shard
					result.valueTotalSize = k.getInt(2);
					result.valueIndex = k.getInt(3, true);
					result.value = kv.value;
					break;
				case 3:
					// Deleted or Complete value
					result.valueIndex = 0;
					result.valueTotalSize = k.getInt(2, true);
					// If not a clear, set the value, otherwise it remains non-present
					if(result.valueTotalSize != 0)
						result.value = kv.value;
					break;
				default:
					result.valueIndex = 0;
					result.valueTotalSize = kv.value.size();
					result.value = kv.value;
					break;
			};
			if(s > 0) {
				Key sk = k.getString(0);
				result.arena().dependsOn(sk.arena());
				result.key = sk;
				if(s > 1) {
					result.version = k.getInt(1, true);
				}
			}
		}
		if(copyValue && result.value.present()) {
			result.value = StringRef(result.arena(), result.value.get());
		}
		return result;
	}

	static Standalone<KeyVersionValueRef> unpack(KeyRef k) {
		return unpack(KeyValueRef(k, StringRef()));
	}

	std::string toString() const {
		std::string r;
		r += format("'%s' @%lld -> ", key.toHexString(15).c_str(), version);
		r += value.present() ? format("'%s' %d/%d", value.get().toHexString(15).c_str(), valueIndex, valueTotalSize).c_str() : "<cleared>";
		return r;
	}
};

typedef Standalone<KeyVersionValueRef> KeyVersionValue;

#define NOT_IMPLEMENTED { UNSTOPPABLE_ASSERT(false); }

class VersionedBTree : public IVersionedStore {
public:
	// The first possible internal record possible in the tree
	static KeyVersionValueRef beginKVV;
	// A record which is greater than the last possible record in the tree
	static KeyVersionValueRef endKVV;

	// The encoded key form of the above two things.
	static Key beginKey;
	static Key endKey;

	// All async opts on the btree are based on pager reads, writes, and commits, so
	// we can mostly forward these next few functions to the pager
	virtual Future<Void> getError() {
		return m_pager->getError();
	}

	virtual Future<Void> onClosed() {
		return m_pager->onClosed();
	}

	void close_impl(bool dispose) {
		IPager *pager = m_pager;
		delete this;
		if(dispose)
			pager->dispose();
		else
			pager->close();
	}

	virtual void dispose() {
		return close_impl(true);
	}

	virtual void close() {
		return close_impl(false);
	}

	virtual KeyValueStoreType getType() NOT_IMPLEMENTED
	virtual bool supportsMutation(int op) NOT_IMPLEMENTED
	virtual StorageBytes getStorageBytes() {
		return m_pager->getStorageBytes();
	}

	// Writes are provided in an ordered stream.
	// A write is considered part of (a change leading to) the version determined by the previous call to setWriteVersion()
	// A write shall not become durable until the following call to commit() begins, and shall be durable once the following call to commit() returns
	virtual void set(KeyValueRef keyValue) {
		SingleKeyMutationsByVersion &changes = insertMutationBoundary(keyValue.key)->second.startKeyMutations;

		// Add the set if the changes set is empty or the last entry isn't a set to exactly the same value
		if(changes.empty() || !changes.rbegin()->second.equalToSet(keyValue.value)) {
			changes[m_writeVersion] = SingleKeyMutation(keyValue.value);
		}
	}
	virtual void clear(KeyRangeRef range) {
		MutationBufferT::iterator iBegin = insertMutationBoundary(range.begin);
		MutationBufferT::iterator iEnd = insertMutationBoundary(range.end);

		// For each boundary in the cleared range
		while(iBegin != iEnd) {
			RangeMutation &range = iBegin->second;

			// Set the rangeClearedVersion if not set
			if(!range.rangeClearVersion.present())
				range.rangeClearVersion = m_writeVersion;

			// Add a clear to the startKeyMutations map if it's empty or the last item is not a clear
			if(range.startKeyMutations.empty() || !range.startKeyMutations.rbegin()->second.isClear())
				range.startKeyMutations[m_writeVersion] = SingleKeyMutation();

			++iBegin;
		}
	}

	virtual void mutate(int op, StringRef param1, StringRef param2) NOT_IMPLEMENTED

	// Versions [begin, end) no longer readable
	virtual void forgetVersions(Version begin, Version end) NOT_IMPLEMENTED

	virtual Future<Version> getLatestVersion() {
		if(m_writeVersion != invalidVersion)
			return m_writeVersion;
		return m_pager->getLatestVersion();
	}

	Version getWriteVersion() {
		return m_writeVersion;
	}

	Version getLastCommittedVersion() {
		return m_lastCommittedVersion;
	}

	VersionedBTree(IPager *pager, std::string name, int target_page_size = -1)
	  : m_pager(pager),
		m_writeVersion(invalidVersion),
		m_usablePageSizeOverride(pager->getUsablePageSize()),
		m_lastCommittedVersion(invalidVersion),
		m_pBuffer(nullptr),
		m_name(name)
	{
		if(target_page_size > 0 && target_page_size < m_usablePageSizeOverride)
			m_usablePageSizeOverride = target_page_size;
		m_init = init_impl(this);
		m_latestCommit = m_init;
	}

	ACTOR static Future<Void> init_impl(VersionedBTree *self) {
		self->m_root = 0;
		state Version latest = wait(self->m_pager->getLatestVersion());
		if(latest == 0) {
			++latest;
			Reference<IPage> page = self->m_pager->newPageBuffer();
			writeEmptyPage(page, BTreePage::IS_LEAF, self->m_usablePageSizeOverride);
			self->writePage(self->m_root, page, latest, StringRef(), StringRef());
			self->m_pager->setLatestVersion(latest);
			wait(self->m_pager->commit());
		}
		self->m_lastCommittedVersion = latest;
		return Void();
	}

	Future<Void> init() { return m_init; }

	virtual ~VersionedBTree() {
		// This probably shouldn't be called directly (meaning deleting an instance directly) but it should be safe,
		// it will cancel init and commit and leave the pager alive but with potentially an incomplete set of 
		// uncommitted writes so it should not be committed.
		m_init.cancel();
		m_latestCommit.cancel();
	}

	// readAtVersion() may only be called on a version which has previously been passed to setWriteVersion() and never previously passed
	//   to forgetVersion.  The returned results when violating this precondition are unspecified; the store is not required to be able to detect violations.
	// The returned read cursor provides a consistent snapshot of the versioned store, corresponding to all the writes done with write versions less
	//   than or equal to the given version.
	// If readAtVersion() is called on the *current* write version, the given read cursor MAY reflect subsequent writes at the same
	//   write version, OR it may represent a snapshot as of the call to readAtVersion().
	virtual Reference<IStoreCursor> readAtVersion(Version v) {
		// TODO: Use the buffer to return uncommitted data
		// For now, only committed versions can be read.
		ASSERT(v <= m_lastCommittedVersion);
		return Reference<IStoreCursor>(new Cursor(v, m_pager, m_root, m_usablePageSizeOverride));
	}

	// Must be nondecreasing
	virtual void setWriteVersion(Version v) {
		ASSERT(v > m_lastCommittedVersion);
		// If there was no current mutation buffer, create one in the buffer map and update m_pBuffer
		if(m_pBuffer == nullptr) {
			// When starting a new mutation buffer its start version must be greater than the last write version
			ASSERT(v > m_writeVersion);
			m_pBuffer = &m_mutationBuffers[v];
			// Create range representing the entire keyspace.  This reduces edge cases to applying mutations
			// because now all existing keys are within some range in the mutation map.
			(*m_pBuffer)[beginKVV.key];
			(*m_pBuffer)[endKVV.key];
		}
		else {
			// It's OK to set the write version to the same version repeatedly so long as m_pBuffer is not null
			ASSERT(v >= m_writeVersion);
		}
		m_writeVersion = v;
	}

	virtual Future<Void> commit() {
		if(m_pBuffer == nullptr)
			return m_latestCommit;
		return commit_impl(this);
	}

private:
	void writePage(LogicalPageID id, Reference<IPage> page, Version ver, StringRef pageLowerBound, StringRef pageUpperBound) {
		debug_printf("writePage(): %s\n", ((const BTreePage *)page->begin())->toString(true, id, ver, pageLowerBound, pageUpperBound).c_str());
		m_pager->writePage(id, page, ver);
	}

	LogicalPageID m_root;

	typedef std::pair<Key, LogicalPageID> KeyPagePairT;
	typedef std::pair<Version, std::vector<KeyPagePairT>> VersionedKeyToPageSetT;
	typedef std::vector<VersionedKeyToPageSetT> VersionedChildrenT;

	// Represents a change to a single key - set, clear, or atomic op
	struct SingleKeyMutation {
		// Clear
		SingleKeyMutation() : op(MutationRef::ClearRange) {}
		// Set
		SingleKeyMutation(Value val) : op(MutationRef::SetValue), value(val) {}
		// Atomic Op
		SingleKeyMutation(MutationRef::Type op, Value val) : op(op), value(val) {}

		MutationRef::Type op;
		Value value;

		inline bool isClear() const { return op == MutationRef::ClearRange; }
		inline bool isSet() const { return op == MutationRef::SetValue; }
		inline bool isAtomicOp() const { return !isSet() && !isClear(); }

		inline bool equalToSet(ValueRef val) { return isSet() && value == val; }

		// The returned packed key will be added to arena, the value will just point to the SingleKeyMutation's memory
		inline KeyVersionValueRef toKVV(KeyRef userKey, Version version) const {
			// No point in serializing an atomic op, it needs to be coalesced to a real value.
			ASSERT(!isAtomicOp());

			if(isClear())
				return KeyVersionValueRef(userKey, version);

			return KeyVersionValueRef(userKey, version, value);
		}

		std::string toString() const {
			return format("op=%d val='%s'", op, printable(value).c_str());
		}
	};

	// Represents mutations on a single key and a possible clear to a range that begins
	// immediately after that key
	typedef std::map<Version, SingleKeyMutation> SingleKeyMutationsByVersion;
	struct RangeMutation {
		// Mutations for exactly the start key
		SingleKeyMutationsByVersion startKeyMutations;
		// A clear range version, if cleared, for the range starting immediately AFTER the start key
		Optional<Version> rangeClearVersion;

		// Returns true if this RangeMutation doesn't actually mutate anything
		bool noChanges() const {
			return !rangeClearVersion.present() && startKeyMutations.empty();
		}

		std::string toString() const {
			std::string result;
			result.append("rangeClearVersion: ");
			if(rangeClearVersion.present())
				result.append(format("%lld", rangeClearVersion.get()));
			else
				result.append("<not present>");
			result.append("  startKeyMutations: ");
			for(SingleKeyMutationsByVersion::value_type const &m : startKeyMutations)
				result.append(format("[%lld => %s] ", m.first, m.second.toString().c_str()));
			return result;
		}
	};

	typedef std::map<Key, RangeMutation> MutationBufferT;

	/* Mutation Buffer Overview
	 *
	 * MutationBuffer maps the start of a range to a RangeMutation.  The end of the range is
	 * the next range start in the map.
	 *
	 * - The buffer starts out with keys '' and endKVV.key already populated.
	 *
	 * - When a new key is inserted into the buffer map, it is by definition
	 *   splitting an existing range so it should take on the rangeClearVersion of
	 *   the immediately preceding key which is the start of that range
	 *
	 * - Keys are inserted into the buffer map for every individual operation (set/clear/atomic)
	 *   key and for both the start and end of a range clear.
	 *
	 * - To apply a single clear, add it to the individual ops only if the last entry is not also a clear.
	 *
	 * - To apply a range clear, after inserting the new range boundaries do the following to the start
	 *   boundary and all successive boundaries < end
	 *      - set the range clear version if not already set
	 *      - add a clear to the startKeyMutations if the final entry is not a clear.
	 *
	 * - Note that there are actually TWO valid ways to represent
	 *       set c = val1 at version 1
	 *       clear c\x00 to z at version 2
	 *   with this model.  Either
	 *      c =     { rangeClearVersion = 2, startKeyMutations = { 1 => val1 }
	 *      z =     { rangeClearVersion = <not present>, startKeyMutations = {}
	 *   OR
	 *      c =     { rangeClearVersion = <not present>, startKeyMutations = { 1 => val1 }
	 *      c\x00 = { rangeClearVersion = 2, startKeyMutations = { 2 => <not present> }
	 *      z =     { rangeClearVersion = <not present>, startKeyMutations = {}
	 *
	 *   This is because the rangeClearVersion applies to a range begining with the first
	 *   key AFTER the start key, so that the logic for reading the start key is more simple
	 *   as it only involves consulting startKeyMutations.  When adding a clear range, the
	 *   boundary key insert/split described above is valid, and is what is currently done,
	 *   but it would also be valid to see if the last key before startKey is equal to
	 *   keyBefore(startKey), and if so that mutation buffer boundary key can be used instead
	 *   without adding an additional key to the buffer.
	*/

	IPager *m_pager;
	MutationBufferT *m_pBuffer;
	std::map<Version, MutationBufferT> m_mutationBuffers;

	Version m_writeVersion;
	Version m_lastCommittedVersion;
	Future<Void> m_latestCommit;
	int m_usablePageSizeOverride;
	Future<Void> m_init;
	std::string m_name;

	void printMutationBuffer(MutationBufferT::const_iterator begin, MutationBufferT::const_iterator end) const {
#if REDWOOD_DEBUG
		debug_printf("-------------------------------------\n");
		debug_printf("BUFFER\n");
		while(begin != end) {
			debug_printf("'%s':  %s\n", printable(begin->first).c_str(), begin->second.toString().c_str());
			++begin;
		}
		debug_printf("-------------------------------------\n");
#endif
	}

	void printMutationBuffer(MutationBufferT *buf) const {
		return printMutationBuffer(buf->begin(), buf->end());
	}

	// Find or create a mutation buffer boundary for bound and return an iterator to it
	MutationBufferT::iterator insertMutationBoundary(Key boundary) {
		ASSERT(m_pBuffer != nullptr);

		// Find the first split point in buffer that is >= key
		MutationBufferT::iterator ib = m_pBuffer->lower_bound(boundary);

		// Since the initial state of the mutation buffer contains the range '' through
		// the maximum possible key, our search had to have found something.
		ASSERT(ib != m_pBuffer->end());

		// If we found the boundary we are looking for, return its iterator
		if(ib->first == boundary)
			return ib;

		// ib is our insert hint.  Insert the new boundary and set ib to its entry
		ib = m_pBuffer->insert(ib, {boundary, RangeMutation()});

		// ib is certainly > begin() because it is guaranteed that the empty string
		// boundary exists and the only way to have found that is to look explicitly
		// for it in which case we would have returned above.
		MutationBufferT::iterator iPrevious = ib;
		--iPrevious;
		if(iPrevious->second.rangeClearVersion.present()) {
			ib->second.rangeClearVersion = iPrevious->second.rangeClearVersion;
			ib->second.startKeyMutations[iPrevious->second.rangeClearVersion.get()] = SingleKeyMutation();
		}

		return ib;
	}

	void buildNewRoot(Version version, std::vector<BoundaryAndPage> &pages, std::vector<LogicalPageID> &logicalPageIDs, const BTreePage *pPage) {
		//debug_printf("buildNewRoot start %lu\n", pages.size());
		// While there are multiple child pages for this version we must write new tree levels.
		while(pages.size() > 1) {
			std::vector<PrefixTree::EntryRef> childEntries;
			for(int i=0; i<pages.size(); i++)
				childEntries.emplace_back(pages[i].lowerBound, StringRef((unsigned char *)&logicalPageIDs[i], sizeof(uint32_t)));

			int oldPages = pages.size();
			pages = buildPages(false, beginKey, endKey, childEntries, 0, [=](){ return m_pager->newPageBuffer(); }, m_usablePageSizeOverride);

			debug_printf("Writing a new root level at version %lld with %lu children across %lu pages\n", version, childEntries.size(), pages.size());

			logicalPageIDs = writePages(pages, version, m_root, pPage, endKey, nullptr);
		}
	}

	std::vector<LogicalPageID> writePages(std::vector<BoundaryAndPage> pages, Version version, LogicalPageID originalID, const BTreePage *originalPage, StringRef upperBound, void *actor_debug) {
		debug_printf("%p: writePages(): %u @%lld -> %lu replacement pages\n", actor_debug, originalID, version, pages.size());

		ASSERT(version != 0 || pages.size() == 1);

		std::vector<LogicalPageID> primaryLogicalPageIDs;

		// Reuse original primary page ID if it's not the root or if only one page is being written.
		if(originalID != m_root || pages.size() == 1)
			primaryLogicalPageIDs.push_back(originalID);

		// Allocate a primary page ID for each page to be written
		while(primaryLogicalPageIDs.size() < pages.size()) {
			primaryLogicalPageIDs.push_back(m_pager->allocateLogicalPage());
		}

		debug_printf("%p: writePages(): Writing %lu replacement pages for %d at version %lld\n", actor_debug, pages.size(), originalID, version);
		for(int i=0; i<pages.size(); i++) {
			// Allocate page number for main page first
			LogicalPageID id = primaryLogicalPageIDs[i];

			// Check for extension pages, if they exist assign IDs for them and write them at version
			auto const &extPages = pages[i].extPages;
			// If there are extension pages, write all pages using pager directly because this->writePage() is for whole primary pages
			if(extPages.size() != 0) {
				BTreePage *newPage = (BTreePage *)pages[i].firstPage->mutate();
				ASSERT(newPage->extensionPageCount == extPages.size());

				for(int e = 0, eEnd = extPages.size(); e < eEnd; ++e) {
					LogicalPageID eid = m_pager->allocateLogicalPage();
					debug_printf("%p: writePages(): Writing extension page op=write id=%u @%lld (%d of %lu) referencePage=%u\n", actor_debug, eid, version, e + 1, extPages.size(), id);
					newPage->extensionPages[e] = eid;
					// If replacing the primary page below (version == 0) then pass the primary page's ID as the reference page ID
					m_pager->writePage(eid, extPages[e], version, (version == 0) ? id : invalidLogicalPageID);
				}

				debug_printf("%p: writePages(): Writing primary page op=write id=%u @%lld (+%lu extension pages)\n", actor_debug, id, version, extPages.size());
				m_pager->writePage(id, pages[i].firstPage, version);
			}
			else {
				debug_printf("%p: writePages(): Writing normal page op=write id=%u @%lld\n", actor_debug, id, version);
				writePage(id, pages[i].firstPage, version, pages[i].lowerBound, (i == pages.size() - 1) ? upperBound : pages[i + 1].lowerBound);
			}
		}

		// Free the old extension pages now that all replacement pages have been written
		for(int i = 0; i < originalPage->extensionPageCount; ++i) {
			//debug_printf("%p: writePages(): Freeing old extension op=del id=%u @latest\n", actor_debug, originalPage->extensionPages[i]);
			//m_pager->freeLogicalPage(originalPage->extensionPages[i], version);
		}

		return primaryLogicalPageIDs;
	}

	class SuperPage : public IPage, ReferenceCounted<SuperPage> {
	public:
		SuperPage(std::vector<Reference<const IPage>> pages, int usablePageSize) : m_size(0) {
			for(auto &p : pages) {
				m_size += usablePageSize;
			}
			m_data = new uint8_t[m_size];
			uint8_t *wptr = m_data;
			for(auto &p : pages) {
				memcpy(wptr, p->begin(), usablePageSize);
				wptr += usablePageSize;
			}
		}

		virtual ~SuperPage() {
			delete m_data;
		}

		virtual void addref() const {
			ReferenceCounted<SuperPage>::addref();
		}

		virtual void delref() const {
			ReferenceCounted<SuperPage>::delref();
		}

		virtual int size() const {
			return m_size;
		}

		virtual uint8_t const* begin() const {
			return m_data;
		}

		virtual uint8_t* mutate() {
			return m_data;
		}

	private:
		uint8_t *m_data;
		int m_size;
	};

	ACTOR static Future<Reference<const IPage>> readPage(Reference<IPagerSnapshot> snapshot, LogicalPageID id, int usablePageSize) {
		debug_printf("readPage() op=read id=%u @%lld\n", id, snapshot->getVersion());
		Reference<const IPage> raw = wait(snapshot->getPhysicalPage(id));

		const BTreePage *pTreePage = (const BTreePage *)raw->begin();
		if(pTreePage->extensionPageCount == 0) {
			debug_printf("readPage() Found normal page for op=read id=%u @%lld\n", id, snapshot->getVersion());
			return raw;
		}

		std::vector<Future<Reference<const IPage>>> pageGets;
		pageGets.push_back(std::move(raw));

		for(int i = 0; i < pTreePage->extensionPageCount; ++i) {
			debug_printf("readPage() Reading extension page op=read id=%u @%lld ext=%d/%d\n", pTreePage->extensionPages[i], snapshot->getVersion(), i + 1, (int)pTreePage->extensionPageCount);
			pageGets.push_back(snapshot->getPhysicalPage(pTreePage->extensionPages[i]));
		}

		std::vector<Reference<const IPage>> pages = wait(getAll(pageGets));

		return Reference<const IPage>(new SuperPage(pages, usablePageSize));
	}

	// Returns list of (version, list of (lower_bound, list of children) )
	ACTOR static Future<VersionedChildrenT> commitSubtree(VersionedBTree *self, MutationBufferT *mutationBuffer, Reference<IPagerSnapshot> snapshot, LogicalPageID root, Key lowerBoundKey, Key upperBoundKey) {
		debug_printf("%p commitSubtree: root=%d lower='%s' upper='%s'\n", this, root, lowerBoundKey.toHexString(20).c_str(), upperBoundKey.toHexString(20).c_str());

		// Decode the (likely truncate) upper and lower bound keys for this subtree.
		state KeyVersionValue lowerBoundKVV = KeyVersionValue::unpack(lowerBoundKey);
		state KeyVersionValue upperBoundKVV = KeyVersionValue::unpack(upperBoundKey);

		// Find the slice of the mutation buffer that is relevant to this subtree
		// TODO:  Rather than two lower_bound searches, perhaps just compare each mutation to the upperBound key
		state MutationBufferT::const_iterator iMutationBoundary = mutationBuffer->lower_bound(lowerBoundKVV.key);
		state MutationBufferT::const_iterator iMutationBoundaryEnd = mutationBuffer->lower_bound(upperBoundKVV.key);

		// If the lower bound key and the upper bound key are the same then there can't be any changes to
		// this subtree since changes would happen after the upper bound key as the mutated versions would
		// necessarily be higher.
		if(lowerBoundKVV.key == upperBoundKVV.key) {
			debug_printf("%p no changes, lower and upper bound keys are the same.\n", this);
			return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
		}

		// If the mutation buffer key found is greater than the lower bound key then go to the previous mutation
		// buffer key because it may cover deletion of some keys at the start of this subtree.
		if(iMutationBoundary != mutationBuffer->begin() && iMutationBoundary->first > lowerBoundKVV.key) {
			--iMutationBoundary;
		}
		else {
			// If the there are no mutations, we're done
			if(iMutationBoundary == iMutationBoundaryEnd) {
				debug_printf("%p no changes, mutation buffer start/end are the same\n", this);
				return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
			}
		}

		// TODO:  Check if entire subtree is erased and return no pages, also have the previous pages deleted as of
		// the cleared version.

		// Another way to have no mutations is to have a single mutation range cover this
		// subtree but have no changes in it
		MutationBufferT::const_iterator iMutationBoundaryNext = iMutationBoundary;
		++iMutationBoundaryNext;
		if(iMutationBoundaryNext == iMutationBoundaryEnd && iMutationBoundary->second.noChanges()) {
			debug_printf("%p no changes because sole mutation range was empty\n", this);
			return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
		}

		state Reference<const IPage> rawPage = wait(readPage(snapshot, root, self->m_usablePageSizeOverride));
		state BTreePage *page = (BTreePage *) rawPage->begin();
		debug_printf("%p commitSubtree(): %s\n", this, page->toString(false, root, snapshot->getVersion(), lowerBoundKey, upperBoundKey).c_str());

		PrefixTree::Cursor existingCursor = page->tree().getCursor(lowerBoundKey, upperBoundKey);
		bool existingCursorValid = existingCursor.moveFirst();

		// Leaf Page
		if(page->flags & BTreePage::IS_LEAF) {
			VersionedChildrenT results;
			std::vector<PrefixTree::EntryRef> merged;
			Arena mergedArena;

			debug_printf("%p MERGING EXISTING DATA WITH MUTATIONS:\n", this);
			self->printMutationBuffer(iMutationBoundary, iMutationBoundaryEnd);

			// It's a given that the mutation map is not empty so it's safe to do this
			Key mutationRangeStart = iMutationBoundary->first;

			// There will be multiple loops advancing existing cursor, existing KVV will track its current value
			KeyVersionValue existing;

			if(existingCursorValid) {
				existing = KeyVersionValue::unpack(existingCursor.getKVRef());
			}
			// If replacement pages are written they will be at the minimum version seen in the mutations for this leaf
			Version minVersion = invalidVersion;

			// Now, process each mutation range and merge changes with existing data.
			while(iMutationBoundary != iMutationBoundaryEnd) {
				debug_printf("%p New mutation boundary: '%s': %s\n", this, printable(iMutationBoundary->first).c_str(), iMutationBoundary->second.toString().c_str());

				SingleKeyMutationsByVersion::const_iterator iMutations;

				// If the mutation boundary key is less than the lower bound key then skip startKeyMutations for
				// this bounary, we're only processing this mutation range here to apply any clears to existing data.
				if(iMutationBoundary->first < lowerBoundKVV.key)
					iMutations = iMutationBoundary->second.startKeyMutations.end();
				// If the mutation boundary key is the same as the page lowerBound key then start reading single
				// key mutations at the first version greater than the lowerBoundKey version.
				else if(iMutationBoundary->first == lowerBoundKVV.key)
					iMutations = iMutationBoundary->second.startKeyMutations.upper_bound(lowerBoundKVV.version);
				else
					iMutations = iMutationBoundary->second.startKeyMutations.begin();

				SingleKeyMutationsByVersion::const_iterator iMutationsEnd = iMutationBoundary->second.startKeyMutations.end();

				// Output old versions of the mutation boundary key
				while(existingCursorValid && existing.key == iMutationBoundary->first) {
					// Don't copy the value because this page will stay in memory until after we've built new version(s) of it
					merged.push_back(dependsOn(mergedArena, existingCursor.getKV(false)));
					debug_printf("%p: Added %s [existing, boundary start]\n", this, KeyVersionValue::unpack(merged.back()).toString().c_str());

					existingCursorValid = existingCursor.moveNext();
					if(existingCursorValid)
						existing = KeyVersionValue::unpack(existingCursor.getKVRef());
				}

				// TODO:  If a mutation set is equal to the previous existing value of the key, maybe don't write it.
				// Output mutations for the mutation boundary start key
				while(iMutations != iMutationsEnd) {
					const SingleKeyMutation &m = iMutations->second;
					int maxPartSize = std::min(255, self->m_usablePageSizeOverride / 5);
					if(m.isClear() || m.value.size() <= maxPartSize) {
						if(iMutations->first < minVersion || minVersion == invalidVersion)
							minVersion = iMutations->first;
						// Don't copy the value because this page will stay in memory until after we've built new version(s) of it
						merged.push_back(dependsOn(mergedArena, iMutations->second.toKVV(iMutationBoundary->first, iMutations->first).pack(false)));
						debug_printf("%p: Added %s [mutation, boundary start]\n", this, KeyVersionValue::unpack(merged.back()).toString().c_str());
					}
					else {
						if(iMutations->first < minVersion || minVersion == invalidVersion)
							minVersion = iMutations->first;
						int bytesLeft = m.value.size();
						int start = 0;
						KeyVersionValueRef whole(iMutationBoundary->first, iMutations->first, m.value);
						while(bytesLeft > 0) {
							int partSize = std::min(bytesLeft, maxPartSize);
							// Don't copy the value chunk because this page will stay in memory until after we've built new version(s) of it
							merged.push_back(dependsOn(mergedArena, whole.split(start, partSize).pack(false)));
							bytesLeft -= partSize;
							start += partSize;
							debug_printf("%p: Added %s [mutation, boundary start]\n", this, KeyVersionValue::unpack(merged.back()).toString().c_str());
						}
					}
					++iMutations;
				}

				// Get the clear version for this range, which is the last thing that we need from it,
				Optional<Version> clearRangeVersion = iMutationBoundary->second.rangeClearVersion;
				// Advance to the next boundary because we need to know the end key for the current range.
				++iMutationBoundary;

				debug_printf("%p Mutation range end: '%s'\n", this, printable(iMutationBoundary->first).c_str());

				// Write existing keys which are less than the next mutation boundary key, clearing if needed.
				while(existingCursorValid && existing.key < iMutationBoundary->first) {
					merged.push_back(dependsOn(mergedArena, existingCursor.getKV(false)));
					debug_printf("%p: Added %s [existing, middle]\n", this, KeyVersionValue::unpack(merged.back()).toString().c_str());

					// Write a clear of this key if needed.  A clear is required if clearRangeVersion is set and the next key is different
					// than this one.  Note that the next key might be the in our right sibling, we can use the page upperBound to get that.
					existingCursorValid = existingCursor.moveNext();
					KeyVersionValue nextEntry;
					if(existingCursorValid)
						nextEntry = KeyVersionValue::unpack(existingCursor.getKVRef());
					else
						nextEntry = upperBoundKVV;

					if(clearRangeVersion.present() && existing.key != nextEntry.key) {
						Version clearVersion = clearRangeVersion.get();
						if(clearVersion < minVersion || minVersion == invalidVersion)
							minVersion = clearVersion;
						merged.push_back(dependsOn(mergedArena, KeyVersionValueRef(existing.key, clearVersion).pack(false)));
						debug_printf("%p: Added %s [existing, middle clear]\n", this, KeyVersionValue::unpack(merged.back()).toString().c_str());
					}

					if(existingCursorValid)
						existing = nextEntry;
				}
			}

			// Write any remaining existing keys, which are not subject to clears as they are beyond the cleared range.
			while(existingCursorValid) {
				merged.push_back(dependsOn(mergedArena, existingCursor.getKV(false)));
				debug_printf("%p: Added %s [existing, tail]\n", this, KeyVersionValue::unpack(merged.back()).toString().c_str());

				existingCursorValid = existingCursor.moveNext();
				if(existingCursorValid)
					existing = KeyVersionValue::unpack(existingCursor.getKVRef());
			}

			debug_printf("%p Done merging mutations into existing leaf contents\n", this);

			// No changes were actually made.  This could happen if there is a clear which does not cover an entire leaf but also does
			// not which turns out to not match any existing data in the leaf.
			if(minVersion == invalidVersion) {
				debug_printf("%p No changes were made during mutation merge\n", this);
				return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
			}

			// TODO: Make version and key splits based on contents of merged list, if keeping history

			IPager *pager = self->m_pager;
			std::vector<BoundaryAndPage> pages = buildPages(true, lowerBoundKey, upperBoundKey, merged, BTreePage::IS_LEAF, [pager](){ return pager->newPageBuffer(); }, self->m_usablePageSizeOverride);

			// If there isn't still just a single page of data then this page became too large and was split.
			// The new split pages will be valid as of minVersion, but the old page remains valid at the old version
			// (TODO: unless history isn't being kept at all)
			if(pages.size() != 1) {
				results.push_back( {0, {{lowerBoundKey, root}}} );
			}

			if(pages.size() == 1)
				minVersion = 0;

			// Write page(s), get new page IDs
			std::vector<LogicalPageID> newPageIDs = self->writePages(pages, minVersion, root, page, upperBoundKey, this);

			// If this commitSubtree() is operating on the root, write new levels if needed until until we're returning a single page
			if(root == self->m_root && pages.size() > 1) {
				debug_printf("%p Building new root\n", this);
				self->buildNewRoot(minVersion, pages, newPageIDs, page);
			}

			results.push_back({minVersion, {}});

			// TODO: Can this be moved into writePages?
			// TODO: This can probably be skipped for root
			for(int i=0; i<pages.size(); i++) {
				// The lower bound of the first page is the lower bound of the subtree, not the first entry in the page
				Key lowerBound = (i == 0) ? lowerBoundKey : pages[i].lowerBound;
				debug_printf("%p Adding page to results: %s => %d\n", this, lowerBound.toHexString(20).c_str(), newPageIDs[i]);
				results.back().second.push_back( {lowerBound, newPageIDs[i]} );
			}

			debug_printf("%p DONE.\n", this);
			return results;
		}
		else {
			// Internal Page

			state std::vector<Future<VersionedChildrenT>> futureChildren;
			state std::vector<LogicalPageID> childPageIDs;

			// TODO:  Make this much more efficient with a skip-merge through the two sorted sets (mutations, existing cursor)
			bool first = true;
			while(existingCursorValid) {
				// The lower bound for the first child is lowerBoundKey
				Key childLowerBound = first ? lowerBoundKey : existingCursor.getKey();
				if(first)
					first = false;

				uint32_t pageID = *(uint32_t*)existingCursor.getValueRef().begin();
				ASSERT(pageID != 0);

				existingCursorValid = existingCursor.moveNext();
				Key childUpperBound = existingCursorValid ? existingCursor.getKey() : upperBoundKey;

				debug_printf("lower          '%s'\n", childLowerBound.toHexString(20).c_str());
				debug_printf("upper          '%s'\n", childUpperBound.toHexString(20).c_str());
				ASSERT(childLowerBound <= childUpperBound);

				futureChildren.push_back(self->commitSubtree(self, mutationBuffer, snapshot, pageID, childLowerBound, childUpperBound));
				childPageIDs.push_back(pageID);
			}

			wait(waitForAll(futureChildren));

			bool modified = false;
			for(int i = 0; i < futureChildren.size(); ++i) {
				const VersionedChildrenT &children = futureChildren[i].get();
				// Handle multipages
				if(children.size() != 1 || children[0].second.size() != 1) {
					modified = true;
					break;
				}
			}

			if(!modified) {
				debug_printf("%p not modified.\n", this);
				return VersionedChildrenT({{0, {{lowerBoundKey, root}}}});
			}

			Version version = 0;
			VersionedChildrenT result;

			loop { // over version splits of this page
				Version nextVersion = std::numeric_limits<Version>::max();

				std::vector<PrefixTree::EntryRef> childEntries;  // Logically std::vector<std::pair<std::string, LogicalPageID>> childEntries;

				// For each Future<VersionedChildrenT>
				debug_printf("%p creating replacement pages for id=%d at Version %lld\n", this, root, version);

				// If we're writing version 0, there is a chance that we don't have to write ourselves, if there are no changes
				bool modified = version != 0;

				for(int i = 0; i < futureChildren.size(); ++i) {
					LogicalPageID pageID = childPageIDs[i];
					const VersionedChildrenT &children = futureChildren[i].get();

					debug_printf("%p  Versioned page set that replaced Page id=%d: %lu versions\n", this, pageID, children.size());
					for(auto &versionedPageSet : children) {
						debug_printf("%p    version: Page id=%lld\n", this, versionedPageSet.first);
						for(auto &boundaryPage : versionedPageSet.second) {
							debug_printf("%p      '%s' -> Page id=%u\n", this, printable(boundaryPage.first).c_str(), boundaryPage.second);
						}
					}

					// Find the first version greater than the current version we are writing
					auto cv = std::upper_bound( children.begin(), children.end(), version, [](Version a, VersionedChildrenT::value_type const &b) { return a < b.first; } );

					// If there are no versions before the one we found, just update nextVersion and continue.
					if(cv == children.begin()) {
						debug_printf("%p First version (%lld) in set is greater than current, setting nextVersion and continuing\n", this, cv->first);
						nextVersion = std::min(nextVersion, cv->first);
						debug_printf("%p   curr %lld next %lld\n", this, version, nextVersion);
						continue;
					}

					// If a version greater than the current version being written was found, update nextVersion
					if(cv != children.end()) {
						nextVersion = std::min(nextVersion, cv->first);
						debug_printf("%p   curr %lld next %lld\n", this, version, nextVersion);
					}

					// Go back one to the last version that was valid prior to or at the current version we are writing
					--cv;

					debug_printf("%p   Using children for version %lld from this set, building version %lld\n", this, cv->first, version);

					// If page count isn't 1 then the root is definitely modified
					modified = modified || cv->second.size() != 1;

					// Add the children at this version to the child entries list for the current version being built.
					for (auto &childPage : cv->second) {
						debug_printf("%p  Adding child page '%s'\n", this, printable(childPage.first).c_str());
						childEntries.emplace_back(childPage.first, StringRef((unsigned char *)&childPage.second, sizeof(uint32_t)));
					}
				}

				debug_printf("%p Finished pass through futurechildren.  childEntries=%lu  version=%lld  nextVersion=%lld\n", this, childEntries.size(), version, nextVersion);

				if(modified) {
					// TODO: Track split points across iterations of this loop, so that they don't shift unnecessarily and
					// cause unnecessary path copying

					IPager *pager = self->m_pager;
					std::vector<BoundaryAndPage> pages = buildPages(false, lowerBoundKey, upperBoundKey, childEntries, 0, [pager](){ return pager->newPageBuffer(); }, self->m_usablePageSizeOverride);

					// Write page(s), use version 0 to replace latest version if only writing one page
					std::vector<LogicalPageID> newPageIDs = self->writePages(pages, version, root, page, upperBoundKey, this);

					// If this commitSubtree() is operating on the root, write new levels if needed until until we're returning a single page
					if(root == self->m_root)
						self->buildNewRoot(version, pages, newPageIDs, page);

					result.resize(result.size()+1);
					result.back().first = version;

					for(int i=0; i<pages.size(); i++)
						result.back().second.push_back( {pages[i].lowerBound, newPageIDs[i]} );

					// TODO: figure this out earlier instead of writing replacement page more than once
					if (result.size() > 1 && result.back().second == result.end()[-2].second) {
						debug_printf("%p Output same as last version, popping it.\n", this);
						result.pop_back();
					}
				}
				else {
					debug_printf("%p Version 0 has no changes\n", this);
					result.push_back({0, {{lowerBoundKey, root}}});
				}

				if (nextVersion == std::numeric_limits<Version>::max())
					break;
				version = nextVersion;
			}

			debug_printf("%p DONE.\n", this);
			return result;
		}
	}

	ACTOR static Future<Void> commit_impl(VersionedBTree *self) {
		state MutationBufferT *mutations = self->m_pBuffer;

		// No more mutations are allowed to be written to this mutation buffer we will commit
		// at m_writeVersion, which we must save locally because it could change during commit.
		self->m_pBuffer = nullptr;
		state Version writeVersion = self->m_writeVersion;

		// The latest mutation buffer start version is the one we will now (or eventually) commit.
		state Version mutationBufferStartVersion = self->m_mutationBuffers.rbegin()->first;

		// Replace the lastCommit future with a new one and then wait on the old one
		state Promise<Void> committed;
		Future<Void> previousCommit = self->m_latestCommit;
		self->m_latestCommit = committed.getFuture();

		// Wait for the latest commit that started to be finished.
		wait(previousCommit);
		debug_printf("%s: Beginning commit of version %lld\n", self->m_name.c_str(), writeVersion);

		// Get the latest version from the pager, which is what we will read at
		Version latestVersion = wait(self->m_pager->getLatestVersion());
		debug_printf("%s: pager latestVersion %lld\n", self->m_name.c_str(), latestVersion);

		self->printMutationBuffer(mutations);

		VersionedChildrenT _ = wait(commitSubtree(self, mutations, self->m_pager->getReadSnapshot(latestVersion), self->m_root, beginKey, endKey));

		self->m_pager->setLatestVersion(writeVersion);
		debug_printf("%s: Committing pager %lld\n", self->m_name.c_str(), writeVersion);
		wait(self->m_pager->commit());
		debug_printf("%s: Committed version %lld\n", self->m_name.c_str(), writeVersion);

		// Now that everything is committed we must delete the mutation buffer.
		// Our buffer's start version should be the oldest mutation buffer version in the map.
		ASSERT(mutationBufferStartVersion == self->m_mutationBuffers.begin()->first);
		self->m_mutationBuffers.erase(self->m_mutationBuffers.begin());

		self->m_lastCommittedVersion = writeVersion;
		committed.send(Void());

		return Void();
	}

	// InternalCursor is for seeking to and iterating over the internal / low level records in the Btree.
	// This records are versioned and they can represent deletions or partial values so they must be
	// post processed to obtain keys returnable to the user.
	class InternalCursor {
	public:
		InternalCursor() {}
		InternalCursor(Reference<IPagerSnapshot> pages, LogicalPageID root, int usablePageSizeOverride) : m_pages(pages), m_root(root), outOfBound(0), m_usablePageSizeOverride(usablePageSizeOverride) {
			m_path.reserve(6);
		}

		bool valid() const {
			return (outOfBound == 0) && kvv.valid();
		}

		Future<Void> seekLessThanOrEqual(KeyRef key) {
			return seekLessThanOrEqual_impl(this, key);
		}

		Future<Void> move(bool fwd) {
			return move_impl(this, fwd);
		}

		Standalone<KeyVersionValueRef> kvv;  // The decoded current internal record in the tree

		std::string toString(const char *wrapPrefix = "") const {
			std::string r;
			r += format("InternalCursor(%p) ver=%lld oob=%d valid=%d", this, m_pages->getVersion(), outOfBound, valid());
			r += format("\n%s  KVV: %s", wrapPrefix, kvv.toString().c_str());
			for(const PageEntryLocation &p : m_path) {
				std::string cur = p.cursor.valid() ? format("'%s' -> '%s'", p.cursor.getKey().toHexString(20).c_str(), p.cursor.getValueRef().toHexString(20).c_str()) : "invalid";
				r += format("\n%s  Page id=%d (%d records, %d bytes)  Cursor %s", wrapPrefix, p.pageNumber, p.btPage->count, p.btPage->kvBytes, cur.c_str());
			}
			return r;
		}

	private:
		Reference<IPagerSnapshot> m_pages;
		LogicalPageID m_root;
		int m_usablePageSizeOverride;

		struct PageEntryLocation {
			PageEntryLocation() {}
			PageEntryLocation(Key lowerBound, Key upperBound, Reference<const IPage> page, LogicalPageID id)
				: pageLowerBound(lowerBound), pageUpperBound(upperBound), page(page), pageNumber(id), btPage((BTreePage *)page->begin()), cursor(btPage->tree().getCursor(pageLowerBound, pageUpperBound))
			{
			}

			Key getNextOrUpperBound() {
				if(cursor.moveNext()) {
					Key r = cursor.getKey();
					cursor.movePrev();
					return r;
				}
				return pageUpperBound;
			}

			Key pageLowerBound;
			Key pageUpperBound;
			Reference<const IPage> page;
			BTreePage *btPage;
			PrefixTree::Cursor cursor;
			// For easier debugging
			LogicalPageID pageNumber;
		};

		typedef std::vector<PageEntryLocation> TraversalPathT;
		TraversalPathT m_path;
		int outOfBound;

		ACTOR static Future<Void> pushPage(InternalCursor *self, Key lowerBound, Key upperBound, LogicalPageID id) {
			Reference<const IPage> rawPage = wait(readPage(self->m_pages, id, self->m_usablePageSizeOverride));
			debug_printf("InternalCursor::pushPage() %s\n", ((const BTreePage *)rawPage->begin())->toString(false, id, self->m_pages->getVersion(), lowerBound, upperBound).c_str());
			self->m_path.emplace_back(lowerBound, upperBound, rawPage, id);
			return Void();
		}

		ACTOR static Future<Void> reset(InternalCursor *self) {
			if(self->m_path.empty()) {
				wait(pushPage(self, beginKey, endKey, self->m_root));
			}
			else {
				self->m_path.resize(1);
			}
			self->outOfBound = 0;
			return Void();
		}

		ACTOR static Future<Void> seekLessThanOrEqual_impl(InternalCursor *self, KeyRef key) {
			state TraversalPathT &path = self->m_path;
			wait(reset(self));

			debug_printf("InternalCursor::seekLTE(%s): start  %s\n", key.toHexString(20).c_str(), self->toString("  ").c_str());

			loop {
				state PageEntryLocation *p = &path.back();

				if(p->btPage->count == 0) {
					ASSERT(path.size() == 1);  // This must be the root page.
					self->outOfBound = -1;
					self->kvv.version = invalidVersion;
					debug_printf("InternalCursor::seekLTE(%s): Exit, root page empty.  %s\n", key.toHexString(20).c_str(), self->toString("  ").c_str());
					return Void();
				}

				state bool foundLTE = p->cursor.seekLessThanOrEqual(key);
				debug_printf("InternalCursor::seekLTE(%s): Seek on path tail, result %d.  %s\n", key.toHexString(20).c_str(), foundLTE, self->toString("  ").c_str());
				
				if(p->btPage->flags & BTreePage::IS_LEAF) {
					// It is possible for the current leaf key to be between the page's lower bound (in the parent page) and the 
					// first record in the leaf page, which means we must move backwards 1 step in the database to find the
					// record < key, if such a record exists.
					if(!foundLTE) {
						wait(self->move(false));
					}
					else {
						// Found the target record
						self->kvv = KeyVersionValue::unpack(p->cursor.getKVRef());
					}
					debug_printf("InternalCursor::seekLTE(%s): Exit, Found leaf page. %s\n", key.toHexString(20).c_str(), self->toString("  ").c_str());
					return Void();
				}
				else {
					// We don't have to check foundLTE here because if it's false then cursor will be at the first record in the page.
					// TODO:  It would, however, be more efficient to check foundLTE and if false move to the previous sibling page.
					// But the page should NOT be empty so let's assert that the cursor is valid.
					ASSERT(p->cursor.valid());

					state LogicalPageID newPage = (LogicalPageID)*(uint32_t *)p->cursor.getValueRef().begin();
					debug_printf("InternalCursor::seekLTE(%s): Found internal page, going to Page id=%d.  %s\n", 
						key.toHexString(20).c_str(), newPage, self->toString("  ").c_str());
					wait(pushPage(self, p->cursor.getKey(), p->getNextOrUpperBound(), newPage));
				}
			}
		}

		// Move one 'internal' key/value/version/valueindex/value record.
		// Iterating with this function will "see" all parts of all values and clears at all versions (that is, within the cursor's version of btree pages)
		ACTOR static Future<Void> move_impl(InternalCursor *self, bool fwd) {
			state TraversalPathT &path = self->m_path;
			state const char *dir = fwd ? "forward" : "backward";

			debug_printf("InternalCursor::move(%s) start  %s\n", dir, self->toString("  ").c_str());

			// If cursor was out of bound, adjust out of boundness by 1 in the correct direction
			if(self->outOfBound != 0) {
				self->outOfBound += fwd ? 1 : -1;
				// If we appear to be inbounds, see if we're off the other end of the db or if the page cursor is valid.
				if(self->outOfBound == 0) {
					if(!path.empty() && path.back().cursor.valid()) {
						self->kvv = KeyVersionValue::unpack(path.back().cursor.getKVRef());
					}
					else {
						self->outOfBound = fwd ? 1 : -1;
					}
				}
				debug_printf("InternalCursor::move(%s) was out of bound, exiting  %s\n", dir, self->toString("  ").c_str());
				return Void();
			}

			int i = path.size();
			// Find the closest path part to the end where the index can be moved in the correct direction.
			while(--i >= 0) {
				PrefixTree::Cursor &c = path[i].cursor;
				bool success = fwd ? c.moveNext() : c.movePrev();
				if(success) {
					debug_printf("InternalCursor::move(%s) Move successful on path index %d\n", dir, i);
					path.resize(i + 1);
					break;
				} else {
					debug_printf("InternalCursor::move(%s) Move failed on path index %d\n", dir, i);
				}
			}

			// If no path part could be moved without going out of range then the
			// new cursor position is either before the first record or after the last.
			// Leave the path steps in place and set outOfBound to 1 or -1 based on fwd.
			// This makes the cursor not valid() but a move in the opposite direction
			// will make it valid again, pointing to the previous target record.
			if(i < 0) {
				self->outOfBound = fwd ? 1 : -1;
				debug_printf("InternalCursor::move(%s) Passed an end of the database  %s\n", dir, self->toString("  ").c_str());
				return Void();
			}

			// We were able to advance the cursor on one of the pages in the page traversal path, so now traverse down to leaf level
			state PageEntryLocation *p = &(path.back());

			debug_printf("InternalCursor::move(%s): Descending if needed to find a leaf\n", dir);

			// Now we must traverse downward if needed until we are at a leaf level.
			// Each movement down will start on the far left or far right depending on fwd
			while(!(p->btPage->flags & BTreePage::IS_LEAF)) {
				// Get the page that the path's last entry points to
				LogicalPageID childPageID = (LogicalPageID)*(uint32_t *)p->cursor.getValueRef().begin();

				wait(pushPage(self, p->cursor.getKey(), p->getNextOrUpperBound(), childPageID));
				p = &(path.back());
				// No page traversed to in this manner should be empty.
				ASSERT(p->btPage->count != 0);
				// Go to the first or last entry in the page depending on traversal direction
				if(fwd)
					p->cursor.moveFirst();
				else
					p->cursor.moveLast();

				debug_printf("InternalCursor::move(%s) Descended one level  %s\n", dir, self->toString("  ").c_str());
			}

			// Found the target record, unpack it
			ASSERT(p->cursor.valid());
			self->kvv = KeyVersionValue::unpack(p->cursor.getKVRef());

			debug_printf("InternalCursor::move(%s) Exiting  %s\n", dir, self->toString("  ").c_str());
			return Void();
		}
	};

	// Cursor is for reading and interating over user visible KV pairs at a specific version
	// Keys and values returned are only valid until one of the move methods is called (find*, next, prev)
	// TODO: Make an option to copy all returned strings into an arena?
	class Cursor : public IStoreCursor, public ReferenceCounted<Cursor>, public NonCopyable {
	public:
		Cursor(Version version, IPager *pager, LogicalPageID root, int usablePageSizeOverride)
		  : m_version(version), m_pagerSnapshot(pager->getReadSnapshot(version)), m_icursor(m_pagerSnapshot, root, usablePageSizeOverride) {
		}
		virtual ~Cursor() {}

		virtual Future<Void> findEqual(KeyRef key) { return find_impl(Reference<Cursor>::addRef(this), key, true, 0); }
		virtual Future<Void> findFirstEqualOrGreater(KeyRef key, bool needValue, int prefetchNextBytes) { return find_impl(Reference<Cursor>::addRef(this), key, needValue, 1); }
		virtual Future<Void> findLastLessOrEqual(KeyRef key, bool needValue, int prefetchPriorBytes) { return find_impl(Reference<Cursor>::addRef(this), key, needValue, -1); }

		virtual Future<Void> next(bool needValue) { return next_impl(Reference<Cursor>::addRef(this), needValue); }
		virtual Future<Void> prev(bool needValue) { return prev_impl(Reference<Cursor>::addRef(this), needValue); }

		virtual bool isValid() {
			return m_kv.present();
		}

		virtual KeyRef getKey() {
			return m_kv.get().key;
		}
		//virtual StringRef getCompressedKey() = 0;
		virtual ValueRef getValue() {
			return m_kv.get().value;
		}

		// TODO: Either remove this method or change the contract so that key and value strings returned are still valid after the cursor is
		// moved and allocate them in some arena that this method resets.
		virtual void invalidateReturnedStrings() {
		}

		void addref() { ReferenceCounted<Cursor>::addref(); }
		void delref() { ReferenceCounted<Cursor>::delref(); }

		std::string toString(const char *wrapPrefix = "") const {
			std::string r;
			r += format("Cursor(%p) ver: %lld key: %s value: %s", this, m_version,
				(m_kv.present() ? m_kv.get().key.printable().c_str() : "<np>"),
				(m_kv.present() ? m_kv.get().value.printable().c_str() : ""));
			r += format("\n%s  InternalCursor: %s", wrapPrefix, m_icursor.toString(format("%s    ", wrapPrefix).c_str()).c_str());
			return r;
		}

	private:
		Version m_version;
		Reference<IPagerSnapshot> m_pagerSnapshot;
		InternalCursor m_icursor;
		Optional<KeyValueRef> m_kv; // The current user-level key/value in the tree
		Arena m_arena;

		// find key in tree closest to or equal to key (at this cursor's version)
		// for less than or equal use cmp < 0
		// for greater than or equal use cmp > 0
		// for equal use cmp == 0
		ACTOR static Future<Void> find_impl(Reference<Cursor> self, KeyRef key, bool needValue, int cmp) {
			state InternalCursor &icur = self->m_icursor;

			// Search for the last key at or before (key, version, \xff)
			state Key target = KeyVersionValueRef::searchKey(key, self->m_version);
			self->m_kv = Optional<KeyValueRef>();

			wait(icur.seekLessThanOrEqual(target));
			debug_printf("find%sE('%s'): %s\n", cmp > 0 ? "GT" : (cmp == 0 ? "" : "LT"), target.toHexString(15).c_str(), icur.toString().c_str());

			// If we found the target key, return it as it is valid for any cmp option
			if(icur.valid() && icur.kvv.value.present() && icur.kvv.key == key) {
				debug_printf("Reading full kv pair starting from: %s\n", icur.kvv.toString().c_str());
				wait(self->readFullKVPair(self));
				return Void();
			}

			// FindEqual, so if we're still here we didn't find it.
			if(cmp == 0) {
				return Void();
			}

			// FindEqualOrGreaterThan, so if we're here we have to go to the next present record at the target version.
			if(cmp > 0) {
				// icur is at a record < key, possibly before the start of the tree so move forward at least once.
				loop {
					wait(icur.move(true));
					if(!icur.valid() || icur.kvv.key > key)
						break;
				}
				// Get the next present key at the target version.  Handles invalid cursor too.
				wait(self->next(needValue));
			}
			else if(cmp < 0) {
				// Move to previous present kv pair at the target version
				wait(self->prev(needValue));
			}

			return Void();
		}

		ACTOR static Future<Void> next_impl(Reference<Cursor> self, bool needValue) {
			// TODO: use needValue
			state InternalCursor &i = self->m_icursor;

			debug_printf("Cursor::next(): cursor %s\n", i.toString().c_str());

			// Make sure we are one record past the last user key
			if(self->m_kv.present()) {
				while(i.valid() && i.kvv.key <= self->m_kv.get().key) {
					debug_printf("Cursor::next(): Advancing internal cursor to get passed previous returned user key.  cursor %s\n", i.toString().c_str());
					wait(i.move(true));
				}
			}

			state Version v = self->m_pagerSnapshot->getVersion();
			state InternalCursor iLast;
			while(1) {
				iLast = i;
				if(!i.valid())
					break;
				wait(i.move(true));
				// If the previous cursor position was a set at a version at or before v and the new cursor position
				// is not valid or a newer version of the same key or a different key, then get the full record
				// for the previous cursor position
				if(iLast.kvv.version <= v
					&& iLast.kvv.value.present()
					&& (
						!i.valid()
						|| i.kvv.key != iLast.kvv.key
						|| i.kvv.version > v
					)
				) {
					// Assume that next is the most likely next move, so save the one-too-far cursor position.
					std::swap(i, iLast);
					// readFullKVPair will have to go backwards to read the value
					wait(readFullKVPair(self));
					std::swap(i, iLast);
					return Void();
				}
			}

			self->m_kv = Optional<KeyValueRef>();
			return Void();
		}

		ACTOR static Future<Void> prev_impl(Reference<Cursor> self, bool needValue) {
			// TODO:  use needValue
			state InternalCursor &i = self->m_icursor;

			debug_printf("Cursor::prev(): cursor %s\n", i.toString().c_str());

			// Make sure we are one record before the last user key
			if(self->m_kv.present()) {
				while(i.valid() && i.kvv.key >= self->m_kv.get().key) {
					wait(i.move(false));
				}
			}

			state Version v = self->m_pagerSnapshot->getVersion();
			while(i.valid()) {
				// Once we reach a present value at or before v, return or skip it.
				if(i.kvv.version <= v) {
					// If it's present, return it
					if(i.kvv.value.present()) {
						wait(readFullKVPair(self));
						return Void();
					}
					// Value wasn't present as of the latest version <= v, so move backward to a new key
					state Key clearedKey = i.kvv.key;
					while(1) {
						wait(i.move(false));
						if(!i.valid() || i.kvv.key != clearedKey)
							break;
					}
				}
				else {
					wait(i.move(false));
				}
			}

			self->m_kv = Optional<KeyValueRef>();
			return Void();
		}

		// Read all of the current value, if it is split across multiple kv pairs, and set m_kv.
		// m_current must be at either the first or the last value part.
		ACTOR static Future<Void> readFullKVPair(Reference<Cursor> self) {
			state KeyVersionValue &kvv = self->m_icursor.kvv;
			state KeyValueRef &kv = (self->m_kv = KeyValueRef()).get();

			ASSERT(kvv.value.present());
			// Set the key and cursor arena to the arena containing that key
			self->m_arena = kvv.arena();
			kv.key = kvv.key;

			// Unsplit value
			if(!kvv.isMultiPart()) {
				kv.value = kvv.value.get();
				debug_printf("readFullKVPair:  Unsplit, exit.  %s\n", self->toString("  ").c_str());
			}
			else {
				// Figure out if we should go forward or backward to find all the parts
				state bool fwd = kvv.valueIndex == 0;
				ASSERT(fwd || kvv.valueIndex + kvv.value.get().size() == kvv.valueTotalSize);
				debug_printf("readFullKVPair:  Split, fwd %d totalsize %lld  %s\n", fwd, kvv.valueTotalSize, self->toString("  ").c_str());
				// Allocate space for the entire value in the same arena as the key
				state int bytesLeft = kvv.valueTotalSize;
				kv.value = makeString(bytesLeft, self->m_arena);
				while(1) {
					debug_printf("readFullKVPair:  Adding chunk start %lld len %d total %lld dir %d\n", kvv.valueIndex, kvv.value.get().size(), kvv.valueTotalSize, fwd);
					int partSize = kvv.value.get().size();
					memcpy(mutateString(kv.value) + kvv.valueIndex, kvv.value.get().begin(), partSize);
					bytesLeft -= partSize;
					if(bytesLeft == 0)
						break;
					ASSERT(bytesLeft > 0);
					wait(self->m_icursor.move(fwd));
					ASSERT(self->m_icursor.valid());
				}
			}

			return Void();
		}
	};
};

KeyVersionValueRef VersionedBTree::beginKVV(StringRef(), 0, StringRef());
KeyVersionValueRef VersionedBTree::endKVV(LiteralStringRef("\xff\xff\xff\xff"), std::numeric_limits<int>::max(), StringRef());
Key VersionedBTree::beginKey(beginKVV.pack().key);
Key VersionedBTree::endKey(endKVV.pack().key);

ACTOR template<class T>
Future<T> catchError(Promise<Void> error, Future<T> f) {
	try {
		T result = wait(f);
		return result;
	} catch(Error &e) {
		if(e.code() != error_code_actor_cancelled && error.canBeSet())
			error.sendError(e);
		throw;
	}
}

class KeyValueStoreRedwoodUnversioned : public IKeyValueStore {
public:
	KeyValueStoreRedwoodUnversioned(std::string filePrefix, UID logID) : m_filePrefix(filePrefix) {
		// TODO: This constructor should really just take an IVersionedStore
		IPager *pager = new IndirectShadowPager(filePrefix);
		m_tree = new VersionedBTree(pager, filePrefix, pager->getUsablePageSize());
		m_init = catchError(init_impl(this));
	}

	virtual Future<Void> init() {
		return m_init;
	}

	ACTOR Future<Void> init_impl(KeyValueStoreRedwoodUnversioned *self) {
		TraceEvent(SevInfo, "RedwoodInit").detail("FilePrefix", self->m_filePrefix);
		wait(self->m_tree->init());
		Version v = wait(self->m_tree->getLatestVersion());
		self->m_tree->setWriteVersion(v + 1);
		TraceEvent(SevInfo, "RedwoodInitComplete").detail("FilePrefix", self->m_filePrefix);
		return Void();
	}

	ACTOR void shutdown(KeyValueStoreRedwoodUnversioned *self, bool dispose) {
		TraceEvent(SevInfo, "RedwoodShutdown").detail("FilePrefix", self->m_filePrefix).detail("Dispose", dispose);
		if(self->m_error.canBeSet()) {
			self->m_error.sendError(actor_cancelled());  // Ideally this should be shutdown_in_progress
		}
		self->m_init.cancel();
		Future<Void> closedFuture = self->m_tree->onClosed();
		if(dispose)
			self->m_tree->dispose();
		else
			self->m_tree->close();
		wait(closedFuture);
		self->m_closed.send(Void());
		TraceEvent(SevInfo, "RedwoodShutdownComplete").detail("FilePrefix", self->m_filePrefix).detail("Dispose", dispose);
		delete self;
	}

	virtual void close() {
		shutdown(this, false);
	}

	virtual void dispose() {
		shutdown(this, true);
	}

	virtual Future< Void > onClosed() {
		return m_closed.getFuture();
	}

	Future<Void> commit(bool sequential = false) {
		Future<Void> c = m_tree->commit();
		m_tree->setWriteVersion(m_tree->getWriteVersion() + 1);
		return catchError(c);
	}

	virtual KeyValueStoreType getType() {
		return KeyValueStoreType::SSD_REDWOOD_V1;
	}

	virtual StorageBytes getStorageBytes() {
		return m_tree->getStorageBytes();
	}

	virtual Future< Void > getError() {
		return delayed(m_error.getFuture());
	};

	void clear(KeyRangeRef range, const Arena* arena = 0) {
		m_tree->clear(range);
	}

    virtual void set( KeyValueRef keyValue, const Arena* arena = NULL ) {
		//printf("SET write version %lld %s\n", m_tree->getWriteVersion(), printable(keyValue).c_str());
		m_tree->set(keyValue);
	}

	ACTOR static Future< Standalone< VectorRef< KeyValueRef > > > readRange_impl(KeyValueStoreRedwoodUnversioned *self, KeyRange keys, int rowLimit, int byteLimit) {
		state Standalone<VectorRef<KeyValueRef>> result;
		state int accumulatedBytes = 0;
		ASSERT( byteLimit > 0 );

		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());

		state Version readVersion = self->m_tree->getLastCommittedVersion();
		if(rowLimit >= 0) {
			wait(cur->findFirstEqualOrGreater(keys.begin, true, 0));
			while(cur->isValid() && cur->getKey() < keys.end) {
				KeyValueRef kv(KeyRef(result.arena(), cur->getKey()), ValueRef(result.arena(), cur->getValue()));
				accumulatedBytes += kv.expectedSize();
				result.push_back(result.arena(), kv);
				if(--rowLimit == 0 || accumulatedBytes >= byteLimit) {
					break;
				}
				wait(cur->next(true));
			}
		} else {
			wait(cur->findLastLessOrEqual(keys.end, true, 0));
			if(cur->isValid() && cur->getKey() == keys.end)
				wait(cur->prev(true));

			while(cur->isValid() && cur->getKey() >= keys.begin) {
				KeyValueRef kv(KeyRef(result.arena(), cur->getKey()), ValueRef(result.arena(), cur->getValue()));
				accumulatedBytes += kv.expectedSize();
				result.push_back(result.arena(), kv);
				if(--rowLimit == 0 || accumulatedBytes >= byteLimit) {
					break;
				}
				wait(cur->prev(true));
			}
		}
		return result;
	}

	virtual Future< Standalone< VectorRef< KeyValueRef > > > readRange(KeyRangeRef keys, int rowLimit = 1<<30, int byteLimit = 1<<30) {
		return catchError(readRange_impl(this, keys, rowLimit, byteLimit));
	}

	ACTOR static Future< Optional<Value> > readValue_impl(KeyValueStoreRedwoodUnversioned *self, Key key, Optional< UID > debugID) {
		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());
		state Version readVersion = self->m_tree->getLastCommittedVersion();

		wait(cur->findEqual(key));
		if(cur->isValid()) {
			return cur->getValue();
		}
		return Optional<Value>();
	}

	virtual Future< Optional< Value > > readValue(KeyRef key, Optional< UID > debugID = Optional<UID>()) {
		return catchError(readValue_impl(this, key, debugID));
	}

	ACTOR static Future< Optional<Value> > readValuePrefix_impl(KeyValueStoreRedwoodUnversioned *self, Key key, int maxLength, Optional< UID > debugID) {
		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());

		wait(cur->findEqual(key));
		if(cur->isValid()) {
			Value v = cur->getValue();
			int len = std::min(v.size(), maxLength);
			return Value(cur->getValue().substr(0, len));
		}
		return Optional<Value>();
	}

	virtual Future< Optional< Value > > readValuePrefix(KeyRef key, int maxLength, Optional< UID > debugID = Optional<UID>()) {
		return catchError(readValuePrefix_impl(this, key, maxLength, debugID));
	}

	virtual ~KeyValueStoreRedwoodUnversioned() {
	};

private:
	std::string m_filePrefix;
	VersionedBTree *m_tree;
	Future<Void> m_init;
	Promise<Void> m_closed;
	Promise<Void> m_error;

	template <typename T> inline Future<T> catchError(Future<T> f) {
		return ::catchError(m_error, f);
	}
};

IKeyValueStore* keyValueStoreRedwoodV1( std::string const& filename, UID logID) {
	return new KeyValueStoreRedwoodUnversioned(filename, logID);
}

int randomSize(int max) {
	int exp = g_random->randomInt(0, 6);
	int limit = (pow(10.0, exp) / 1e5 * max) + 1;
	int n = g_random->randomInt(0, max);
	return n;
}

KeyValue randomKV(int keySize = 10, int valueSize = 5) {
	int kLen = randomSize(1 + keySize);
	int vLen = valueSize > 0 ? randomSize(valueSize) : 0;
	KeyValue kv;
	kv.key = makeString(kLen, kv.arena());
	kv.value = makeString(vLen, kv.arena());
	for(int i = 0; i < kLen; ++i)
		mutateString(kv.key)[i] = (uint8_t)g_random->randomInt('a', 'm');
	for(int i = 0; i < vLen; ++i)
		mutateString(kv.value)[i] = (uint8_t)g_random->randomInt('n', 'z');
	return kv;
}

ACTOR Future<int> verifyRandomRange(VersionedBTree *btree, Version v, std::map<std::pair<std::string, Version>, Optional<std::string>> *written) {
	state int errors = 0;
	state Key start = randomKV().key;
	state Key end = randomKV().key;
	if(end <= start)
		end = keyAfter(start);
	debug_printf("VerifyRange '%s' to '%s' @%lld\n", printable(start).c_str(), printable(end).c_str(), v);

	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i =    written->lower_bound(std::make_pair(start.toString(), 0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd = written->upper_bound(std::make_pair(end.toString(),   0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iLast;

	state Reference<IStoreCursor> cur = btree->readAtVersion(v);

	// Randomly use the cursor for something else first.
	if(g_random->coinflip()) {
		debug_printf("VerifyRange: Dummy seek\n");
		state Key randomKey = randomKV().key;
		wait(g_random->coinflip() ? cur->findFirstEqualOrGreater(randomKey, true, 0) : cur->findLastLessOrEqual(randomKey, true, 0));
	}

	debug_printf("VerifyRange: Actual seek\n");
	wait(cur->findFirstEqualOrGreater(start, true, 0));

	state std::vector<KeyValue> results;

	while(cur->isValid() && cur->getKey() < end) {
		// Find the next written kv pair that would be present at this version
		while(1) {
			iLast = i;
			if(i == iEnd)
				break;
			++i;
			if(iLast->first.second <= v
				&& iLast->second.present()
				&& (
					i == iEnd
					|| i->first.first != iLast->first.first
					|| i->first.second > v
				)
			)
				break;
		}

		if(iLast == iEnd) {
			errors += 1;
			printf("VerifyRange(@%lld, %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str());
			break;
		}

		if(cur->getKey() != iLast->first.first) {
			errors += 1;
			printf("VerifyRange(@%lld, %s, %s) ERROR: Tree key '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), iLast->first.first.c_str());
			break;
		}
		if(cur->getValue() != iLast->second.get()) {
			errors += 1;
			printf("VerifyRange(@%lld, %s, %s) ERROR: Tree key '%s' has tree value '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), iLast->second.get().c_str());
			break;
		}

		results.push_back(KeyValue(KeyValueRef(cur->getKey(), cur->getValue())));
		wait(cur->next(true));
	}

	// Make sure there are no further written kv pairs that would be present at this version.
	while(1) {
		iLast = i;
		if(i == iEnd)
			break;
		++i;
		if(iLast->first.second <= v
			&& iLast->second.present()
			&& (
				i == iEnd
				|| i->first.first != iLast->first.first
				|| i->first.second > v
			)
		)
			break;
	}

	if(iLast != iEnd) {
		errors += 1;
		printf("VerifyRange(@%lld, %s, %s) ERROR: Tree range ended but written has @%lld '%s'\n", v, start.toString().c_str(), end.toString().c_str(), iLast->first.second, iLast->first.first.c_str());
	}

	debug_printf("VerifyRangeReverse '%s' to '%s' @%lld\n", printable(start).c_str(), printable(end).c_str(), v);
	// Randomly use a new cursor for the revere range read
	if(g_random->coinflip()) {
		cur = btree->readAtVersion(v);
	}

	// Now read the range from the tree in reverse order and compare to the saved results
	wait(cur->findLastLessOrEqual(end, true, 0));
	if(cur->isValid() && cur->getKey() == end)
		wait(cur->prev(true));

	state std::vector<KeyValue>::const_reverse_iterator r = results.rbegin();

	while(cur->isValid() && cur->getKey() >= start) {
		if(r == results.rend()) {
			errors += 1;
			printf("VerifyRangeReverse(@%lld, %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str());
			break;
		}

		if(cur->getKey() != r->key) {
			errors += 1;
			printf("VerifyRangeReverse(@%lld, %s, %s) ERROR: Tree key '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), r->key.toString().c_str());
			break;
		}
		if(cur->getValue() != r->value) {
			errors += 1;
			printf("VerifyRangeReverse(@%lld, %s, %s) ERROR: Tree key '%s' has tree value '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), r->value.toString().c_str());
			break;
		}

		++r;
		wait(cur->prev(true));
	}

	if(r != results.rend()) {
		errors += 1;
		printf("VerifyRangeReverse(@%lld, %s, %s) ERROR: Tree range ended but written has '%s'\n", v, start.toString().c_str(), end.toString().c_str(), r->key.toString().c_str());
	}

	return errors;
}

ACTOR Future<int> verifyAll(VersionedBTree *btree, Version maxCommittedVersion, std::map<std::pair<std::string, Version>, Optional<std::string>> *written) {
	// Read back every key at every version set or cleared and verify the result.
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i = written->cbegin();
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd = written->cend();
	state int errors = 0;

	while(i != iEnd) {
		state std::string key = i->first.first;
		state Version ver = i->first.second;
		if(ver <= maxCommittedVersion) {
			state Optional<std::string> val = i->second;

			state Reference<IStoreCursor> cur = btree->readAtVersion(ver);

			debug_printf("Verifying @%lld '%s'\n", ver, key.c_str());
			wait(cur->findEqual(key));

			if(val.present()) {
				if(!(cur->isValid() && cur->getKey() == key && cur->getValue() == val.get())) {
					++errors;
					if(!cur->isValid())
						printf("Verify ERROR: key_not_found: '%s' -> '%s' @%lld\n", key.c_str(), val.get().c_str(), ver);
					else if(cur->getKey() != key)
						printf("Verify ERROR: key_incorrect: found '%s' expected '%s' @%lld\n", cur->getKey().toString().c_str(), key.c_str(), ver);
					else if(cur->getValue() != val.get())
						printf("Verify ERROR: value_incorrect: for '%s' found '%s' expected '%s' @%lld\n", cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), val.get().c_str(), ver);
				}
			} else {
				if(cur->isValid() && cur->getKey() == key) {
					++errors;
					printf("Verify ERROR: cleared_key_found: '%s' -> '%s' @%lld\n", key.c_str(), cur->getValue().toString().c_str(), ver);
				}
			}
		}
		++i;
	}
	return errors;
}

ACTOR Future<Void> verify(VersionedBTree *btree, FutureStream<Version> vStream, std::map<std::pair<std::string, Version>, Optional<std::string>> *written, int *pErrorCount) {
	try {
		loop {
			state Version v = waitNext(vStream);

			debug_printf("Verifying through version %lld\n", v);
			state Future<int> vall = verifyAll(btree, v, written);
			state Future<int> vrange = verifyRandomRange(btree, g_random->randomInt(1, v + 1), written);
			wait(success(vall) && success(vrange));

			int errors = vall.get() + vrange.get();
			*pErrorCount += errors;

			debug_printf("Verified through version %lld, %d errors\n", v, errors);

			if(*pErrorCount != 0)
				break;
		}
	} catch(Error &e) {
		if(e.code() != error_code_end_of_stream) {
			throw;
		}
	}
	return Void();
}

// Does a random range read, doesn't trap/report errors
ACTOR Future<Void> randomReader(VersionedBTree *btree) {
	state Reference<IStoreCursor> cur;
	loop {
		wait(yield());
		if(!cur || g_random->random01() > .1) {
			Version v = g_random->randomInt(1, btree->getLastCommittedVersion() + 1);
			cur = btree->readAtVersion(v);
		}

		wait(cur->findFirstEqualOrGreater(randomKV(10, 0).key, true, 0));
		state int c = g_random->randomInt(0, 100);
		while(cur->isValid() && c-- > 0) {
			wait(success(cur->next(true)));
			wait(yield());
		}
	}
}

TEST_CASE("!/redwood/correctness") {
	state bool useDisk = true;  // MemoryPager is not being maintained currently.

	state std::string pagerFile = "unittest_pageFile";
	IPager *pager;

	if(useDisk) {
		deleteFile(pagerFile);
		deleteFile(pagerFile + "0.pagerlog");
		deleteFile(pagerFile + "1.pagerlog");
		pager = new IndirectShadowPager(pagerFile);
	}
	else
		pager = createMemoryPager();

	state int pageSize = g_random->coinflip() ? pager->getUsablePageSize() : g_random->randomInt(200, 400);
	state VersionedBTree *btree = new VersionedBTree(pager, pagerFile, pageSize);
	wait(btree->init());

	state int mutationBytesTarget = g_random->randomInt(100, 20e6);

	// We must be able to fit at least two any two keys plus overhead in a page to prevent
	// a situation where the tree cannot be grown upward with decreasing level size.
	// TODO:  Handle arbitrarily large keys
	state int maxKeySize = std::min(pageSize * 8, 30000);
	ASSERT(maxKeySize > 0);
	state int maxValueSize = std::min(pageSize * 25, 100000);

	printf("Using page size %d, max key size %d, max value size %d, total mutation byte target %d\n", pageSize, maxKeySize, maxValueSize, mutationBytesTarget);

	state std::map<std::pair<std::string, Version>, Optional<std::string>> written;
	state std::set<Key> keys;

	state Version lastVer = wait(btree->getLatestVersion());
	printf("Starting from version: %lld\n", lastVer);

	state Version version = lastVer + 1;
	state int mutationBytes = 0;

	btree->setWriteVersion(version);

	state int64_t keyBytesInserted = 0;
	state int64_t ValueBytesInserted = 0;
	state int errorCount;

	state PromiseStream<Version> committedVersions;
	state Future<Void> verifyTask = verify(btree, committedVersions.getFuture(), &written, &errorCount);
	state Future<Void> randomTask = randomReader(btree) || btree->getError();

	state Future<Void> commit = Void();

	while(mutationBytes < mutationBytesTarget) {
		// Sometimes advance the version
		if(g_random->random01() < 0.10) {
			++version;
			btree->setWriteVersion(version);
		}

		// Sometimes do a clear range
		if(g_random->random01() < .10) {
			Key start = randomKV(maxKeySize, 1).key;
			Key end = (g_random->random01() < .01) ? keyAfter(start) : randomKV(maxKeySize, 1).key;

			// Sometimes replace start and/or end with a close actual (previously used) value
			if(g_random->random01() < .10) {
				auto i = keys.upper_bound(start);
				if(i != keys.end())
					start = *i;
			}
			if(g_random->random01() < .10) {
				auto i = keys.upper_bound(end);
				if(i != keys.end())
					end = *i;
			}

			if(end == start)
				end = keyAfter(start);
			else if(end < start) {
				std::swap(end, start);
			}

			KeyRangeRef range(start, end);
			debug_printf("      Clear '%s' to '%s' @%lld\n", start.toString().c_str(), end.toString().c_str(), version);
			auto e = written.lower_bound(std::make_pair(start.toString(), 0));
			if(e != written.end()) {
				auto last = e;
				auto eEnd = written.lower_bound(std::make_pair(end.toString(), 0));
				while(e != eEnd) {
					auto w = *e;
					++e;
					// If e key is different from last and last was present then insert clear for last's key at version
					if(last != eEnd && ((e == eEnd || e->first.first != last->first.first) && last->second.present())) {
						debug_printf("         Clearing key '%s' @%lld\n", last->first.first.c_str(), version);
						mutationBytes += (last->first.first.size() + last->second.get().size());
						// If the last set was at version then just make it not present
						if(last->first.second == version) {
							last->second = Optional<std::string>();
						}
						else {
							written[std::make_pair(last->first.first, version)] = Optional<std::string>();
						}
					}
					last = e;
				}
			}

			btree->clear(range);
		}
		else {
			// Set a key
			KeyValue kv = randomKV(maxKeySize, maxValueSize);
			// Sometimes change key to a close previously used key
			if(g_random->random01() < .01) {
				auto i = keys.upper_bound(kv.key);
				if(i != keys.end())
					kv.key = StringRef(kv.arena(), *i);
			}
			keyBytesInserted += kv.key.size();
			ValueBytesInserted += kv.value.size();
			mutationBytes += (kv.key.size() + kv.value.size());
			debug_printf("      Set '%s' -> '%s' @%lld\n", kv.key.toString().c_str(), kv.value.toString().c_str(), version);
			btree->set(kv);
			written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			keys.insert(kv.key);
		}

		// Sometimes (and at end) commit then check all results
		if(mutationBytes >= std::min(mutationBytesTarget, (int)20e6) || g_random->random01() < .002) {
			// Wait for btree commit and send the new version to committedVersions.
			// Avoid capture of version as a member of *this
			Version v = version;
			commit = map(commit && btree->commit(), [=](Void) {
				// Notify the background verifier that version is committed and therefore readable
				committedVersions.send(v);
				return Void();
			});

			printf("Cumulative: %d total mutation bytes, %lu key changes, %lld key bytes, %lld value bytes\n", mutationBytes, written.size(), keyBytesInserted, ValueBytesInserted);

			// Recover from disk at random
			if(useDisk && g_random->random01() < .1) {
				printf("Recovering from disk.\n");

				// Wait for outstanding commit
				debug_printf("Waiting for outstanding commit\n");
				wait(commit);

				// Stop and wait for the verifier task
				committedVersions.sendError(end_of_stream());
				debug_printf("Waiting for verification to complete.\n");
				wait(verifyTask);

				Future<Void> closedFuture = btree->onClosed();
				btree->close();
				wait(closedFuture);

				debug_printf("Reopening btree\n");
				IPager *pager = new IndirectShadowPager(pagerFile);
				btree = new VersionedBTree(pager, pagerFile, pageSize);
				wait(btree->init());

				Version v = wait(btree->getLatestVersion());
				ASSERT(v == version);
				printf("Recovered from disk.  Latest version %lld\n", v);

				// Create new promise stream and start the verifier again
				committedVersions = PromiseStream<Version>();
				verifyTask = verify(btree, committedVersions.getFuture(), &written, &errorCount);
				randomTask = randomReader(btree) || btree->getError();
			}

			// Check for errors
			if(errorCount != 0)
				throw internal_error();

			++version;
			btree->setWriteVersion(version);
		}

	}

	debug_printf("Waiting for outstanding commit\n");
	wait(commit);
	committedVersions.sendError(end_of_stream());
	debug_printf("Waiting for verification to complete.\n");
	wait(verifyTask);

	Future<Void> closedFuture = btree->onClosed();
	btree->close();
	wait(closedFuture);

	return Void();
}

TEST_CASE("!/redwood/performance/set") {
	state std::string pagerFile = "unittest_pageFile";
	deleteFile(pagerFile);
	deleteFile(pagerFile + "0.pagerlog");
	deleteFile(pagerFile + "1.pagerlog");
	IPager *pager = new IndirectShadowPager(pagerFile);
	state VersionedBTree *btree = new VersionedBTree(pager, "unittest_pageFile");
	wait(btree->init());

	state int nodeCount = 10000000;
	state int maxChangesPerVersion = 1000;
	state int versions = 5000;
	int maxKeySize = 50;
	int maxValueSize = 100;

	state std::string key(maxKeySize, 'k');
	state std::string value(maxKeySize, 'v');
	state int64_t kvBytes = 0;
	state int records = 0;
	state Future<Void> commit = Void();

	state double startTime = now();
	while(--versions) {
		Version lastVer = wait(btree->getLatestVersion());
		state Version version = lastVer + 1;
		btree->setWriteVersion(version);
		int changes = g_random->randomInt(0, maxChangesPerVersion);
		while(changes--) {
			KeyValue kv;
			// Change first 4 bytes of key to an int
			*(uint32_t *)key.data() = g_random->randomInt(0, nodeCount);
			kv.key = StringRef((uint8_t *)key.data(), g_random->randomInt(10, key.size()));
			kv.value = StringRef((uint8_t *)value.data(), g_random->randomInt(0, value.size()));
			btree->set(kv);
			kvBytes += kv.key.size() + kv.value.size();
			++records;
		}

		if(g_random->random01() < (1.0 / 300)) {
			wait(commit);
			commit = btree->commit();
			double elapsed = now() - startTime;
			printf("Committed (cumulative) %lld bytes in %d records in %f seconds, %.2f MB/s\n", kvBytes, records, elapsed, kvBytes / elapsed / 1e6);
		}
	}

	wait(btree->commit());

	Future<Void> closedFuture = btree->onClosed();
	btree->close();
	wait(closedFuture);

	double elapsed = now() - startTime;
	printf("Wrote (final) %lld bytes in %d records in %f seconds, %.2f MB/s\n", kvBytes, records, elapsed, kvBytes / elapsed / 1e6);

	return Void();
}
