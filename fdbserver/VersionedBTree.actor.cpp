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
#include "fdbserver/DeltaTree.h"
#include <string.h>
#include "flow/actorcompiler.h"

#define STR(x) LiteralStringRef(x)
struct RedwoodRecordRef {

#pragma pack(push,1)
	struct ValuePart {
		int32_t total;
		int32_t start;
	};
#pragma pack(pop)

	RedwoodRecordRef(KeyRef key = KeyRef(), Version ver = std::numeric_limits<Version>::max(), Optional<ValueRef> value = {}, Optional<ValuePart> part = {})
		: key(key), version(ver), value(value), valuePart(part)
	{
		ASSERT(!part.present() || value.present());
	}

	RedwoodRecordRef(Arena &arena, const RedwoodRecordRef &toCopy) {
		*this = toCopy;
		key = KeyRef(arena, key);
		if(value.present()) {
			value = ValueRef(arena, toCopy.value.get());
		}
	}

	KeyRef key;
	Version version;
	Optional<ValueRef> value;
	Optional<ValuePart> valuePart;

	int expectedSize() const {
		return key.expectedSize() + value.expectedSize() + sizeof(version) + sizeof(valuePart);
	}

	bool isMultiPart() const {
		return valuePart.present();
	}

	// Generate a kv shard from a complete kv
	RedwoodRecordRef split(int start, int len) {
		ASSERT(!isMultiPart() && value.present());
		return RedwoodRecordRef(key, version, value.get().substr(start, len), ValuePart({value.get().size(), start}));
	}

#pragma pack(push,1)
	struct Delta {
		// TODO:  Make this actually a delta
		enum EFlags {HAS_VALUE = 1, HAS_VALUE_PART = 4};

		uint8_t flags;
		uint16_t keySize;
		Version version;
		uint8_t bytes[];

		RedwoodRecordRef apply(const RedwoodRecordRef &prev, const RedwoodRecordRef &next, Arena arena) {
			RedwoodRecordRef r;
			const uint8_t *rptr = bytes;
			r.key = StringRef(rptr, keySize);
			rptr += keySize;
			r.version = version;
			if(flags & HAS_VALUE) {
				uint16_t valueSize = *(uint16_t *)rptr;
				rptr += 2;
				r.value = StringRef(rptr, valueSize);
				rptr += valueSize;
				if(flags & HAS_VALUE_PART) {
					r.valuePart = *(ValuePart *)rptr;
				}
			}
			return r;
		}

		int size() const {
			int s = sizeof(Delta) + keySize;
			if(flags & HAS_VALUE) {
				s += 2;
				s += *(uint16_t *)(bytes + keySize);
				if(flags & HAS_VALUE_PART) {
					s += sizeof(ValuePart);
				}
			}
			return s;
		}

		std::string toString() const {
			return format("DELTA{ %s | %s }",
				StringRef((const uint8_t *)this, sizeof(Delta)).toHexString().c_str(),
				StringRef(bytes, size() - sizeof(Delta)).toHexString().c_str()
			);
		}
	};
#pragma pack(pop)

	int compare(const RedwoodRecordRef &rhs) const {
		//printf("compare %s to %s\n", toString().c_str(), rhs.toString().c_str());
		int cmp = key.compare(rhs.key);
		if(cmp == 0) {
			cmp = version - rhs.version;
			if(cmp == 0) {
				// Absent value is greater than present (for reasons)
				cmp = (value.present() ? 0 : 1) - (rhs.value.present() ? 0 : 1);
				if(cmp == 0) {
					// Chunked is greater than whole
					cmp = (valuePart.present() ? 1 : 0) - (rhs.valuePart.present() ? 1 : 0);
					if(cmp == 0 && valuePart.present()) {
						// Larger total size is greater
						cmp = valuePart.get().total - rhs.valuePart.get().total;
						if(cmp == 0) {
							// Order by start
							cmp = valuePart.get().start - rhs.valuePart.get().start;
						}
					}
				}
			}
		}
		return cmp;
	}

	bool operator==(const RedwoodRecordRef &rhs) const {
		return compare(rhs) == 0;
	}

	bool operator<(const RedwoodRecordRef &rhs) const {
		return compare(rhs) < 0;
	}

	bool operator>(const RedwoodRecordRef &rhs) const {
		return compare(rhs) > 0;
	}

	bool operator<=(const RedwoodRecordRef &rhs) const {
		return compare(rhs) <= 0;
	}

	bool operator>=(const RedwoodRecordRef &rhs) const {
		return compare(rhs) >= 0;
	}

	int deltaSize(const RedwoodRecordRef &base) const {
		int s = sizeof(Delta) + key.size();
		if(value.present()) {
			s += 2;
			s += value.get().size();
			if(valuePart.present()) {
				s += sizeof(ValuePart);
			}
		}
		return s;
	}

	void writeDelta(Delta &d, const RedwoodRecordRef &prev, const RedwoodRecordRef &next) const {
		d.flags = value.present() ? Delta::EFlags::HAS_VALUE : 0;
		if(valuePart.present())
			d.flags |= Delta::EFlags::HAS_VALUE_PART;
		d.keySize = key.size();
		d.version = version;
		uint8_t *wptr = d.bytes;
		memcpy(wptr, key.begin(), key.size());
		wptr += key.size();
		if(value.present()) {
			*(uint16_t *)wptr = value.get().size();
			wptr += 2;
			memcpy(wptr, value.get().begin(), value.get().size());
			wptr += value.get().size();
			if(valuePart.present()) {
				*(ValuePart *)wptr = valuePart.get();
			}
		}
	}

	static std::string kvformat(StringRef s, int hexLimit = -1) {
		bool hex = false;

		for(auto c : s) {
			if(!isprint(c)) {
				hex = true;
				break;
			}
		}

		return hex ? s.toHexString(hexLimit) : s.toString();
	}

	std::string toString(int hexLimit = 15) const {
		std::string r;
		r += format("'%s' @%lld ", kvformat(key, hexLimit).c_str(), version);
		if(valuePart.present()) {
			r += format("[%d/%d] ", valuePart.get().start, valuePart.get().total);
		}
		if(value.present()) {
			r += format("-> '%s'", kvformat(value.get(), hexLimit).c_str());
		}
		else {
			r += "-> <cleared>";
		}
		return r;
	}
};

struct BTreePage {

	enum EPageFlags { IS_LEAF = 1};

	typedef DeltaTree<RedwoodRecordRef> BinaryTree;

#pragma pack(push,1)
	struct {
		uint8_t flags;
		uint16_t count;
		uint32_t kvBytes;
		uint8_t extensionPageCount;
		LogicalPageID extensionPages[0];
	};
#pragma pack(pop)

	int size() const {
		const BinaryTree *t = &tree();
		return (uint8_t *)t - (uint8_t *)this + t->size();
	}

	bool isLeaf() const {
		return flags & IS_LEAF;
	}

	BinaryTree & tree() {
		return *(BinaryTree *)(extensionPages + extensionPageCount);
	}

	const BinaryTree & tree() const {
		return *(const BinaryTree *)(extensionPages + extensionPageCount);
	}

	static inline int GetHeaderSize(int extensionPages = 0) {
		return sizeof(BTreePage) + extensionPages + sizeof(LogicalPageID);
	}

	std::string toString(bool write, LogicalPageID id, Version ver, const RedwoodRecordRef *lowerBound, const RedwoodRecordRef *upperBound) const {
		std::string r;
		r += format("BTreePage op=%s id=%d ver=%lld ptr=%p flags=0x%X count=%d kvBytes=%d extPages=%d\n  lowerBound: %s\n  upperBound: %s\n",
					write ? "write" : "read", id, ver, this, (int)flags, (int)count, (int)kvBytes, (int)extensionPageCount,
					lowerBound->toString().c_str(), upperBound->toString().c_str());
		try {
			if(count > 0) {
				// This doesn't use the cached reader for the page but it is only for debugging purposes
				BinaryTree::Reader reader(&tree(), lowerBound, upperBound);
				BinaryTree::Cursor c = reader.getCursor();

				c.moveFirst();
				ASSERT(c.valid());

				do {
					r += "  ";
					if(!(flags & IS_LEAF)) {
						RedwoodRecordRef rec = c.get();
						ASSERT(rec.value.present() && rec.value.get().size() == sizeof(uint32_t));
						uint32_t id = *(const uint32_t *)rec.value.get().begin();
						std::string val = format("[Page id=%u]", id);
						rec.value = val;
						r += rec.toString();
					}
					else {
						r += c.get().toString();
					}

					r += "\n";

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
	VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());
	BTreePage *btpage = (BTreePage *)page->begin();
	btpage->flags = newFlags;
	btpage->kvBytes = 0;
	btpage->count = 0;
	btpage->extensionPageCount = 0;
	btpage->tree().build(nullptr, nullptr, nullptr, nullptr);
}

BTreePage::BinaryTree::Reader * getReader(Reference<const IPage> page) {
	return (BTreePage::BinaryTree::Reader *)page->userData;
}

struct BoundaryAndPage {
	Standalone<RedwoodRecordRef> lowerBound;
	// Only firstPage or multiPage will be in use at once
	Reference<IPage> firstPage;
	std::vector<Reference<IPage>> extPages;
};

// Returns a std::vector of pairs of lower boundary key indices within kvPairs and encoded pages.
// TODO:  Refactor this as an accumulator you add sorted keys to which makes pages.
template<typename Allocator>
static std::vector<BoundaryAndPage> buildPages(bool minimalBoundaries, const RedwoodRecordRef &lowerBound, const RedwoodRecordRef &upperBound,  std::vector<RedwoodRecordRef> entries, uint8_t newFlags, Allocator const &newBlockFn, int usableBlockSize) {
	// TODO:  Figure out how to do minimal boundaries with RedwoodRecordRef
	minimalBoundaries = false;

	// This is how much space for the binary tree exists in the page, after the header
	int pageSize = usableBlockSize - BTreePage::GetHeaderSize();

	// Each new block adds (usableBlockSize - sizeof(LogicalPageID)) more net usable space *for the binary tree* to pageSize.
	int netTreeBlockSize = usableBlockSize - sizeof(LogicalPageID);

	int blockCount = 1;
	std::vector<BoundaryAndPage> pages;

	int kvBytes = 0;
	int compressedBytes = BTreePage::BinaryTree::GetTreeOverhead();

	int start = 0;
	int i = 0;
	const int iEnd = entries.size();
	// Lower bound of the page being added to
	RedwoodRecordRef pageLowerBound = lowerBound;
	RedwoodRecordRef pageUpperBound;

	while(i <= iEnd) {
		bool end = i == iEnd;
		bool flush = end;

		// If not the end, add i to the page if necessary
		if(end) {
			pageUpperBound = upperBound;
		}
		else {
			// Get delta from previous record
			const RedwoodRecordRef &entry = entries[i];
			int deltaSize = entry.deltaSize((i == start) ? pageLowerBound : entries[i - 1]);
			int keySize = entry.key.size();
			int valueSize = entry.value.present() ? entry.value.get().size() : 0;

			int spaceNeeded = sizeof(BTreePage::BinaryTree::Node) + deltaSize;

			debug_printf("Trying to add record %3d of %3lu (i=%3d) klen %4d  vlen %3d  deltaSize %4d  spaceNeeded %4d  compressed %4d / page %4d bytes  %s\n",
				i + 1, entries.size(), i, keySize, valueSize, deltaSize,
				spaceNeeded, compressedBytes, pageSize, entry.toString().c_str());

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
					if(newPageSize <= BTreePage::BinaryTree::MaximumTreeSize()) {
						blockCount += newBlocks;
						pageSize = newPageSize;
						fits = true;
					}
				}
				if(!fits) {
					// Flush page
					if(minimalBoundaries) {
						// Note that prefixLen is guaranteed to be < entry.key.size() because entries are in increasing order and cannot repeat.
// 						int len = prefixLen + 1;
// 						if(entry.key[prefixLen] == 0)
// 							len = std::min(len + 1, entry.key.size());
// 						pageUpperBound = entry.key.substr(0, len);
					}
					else {
						pageUpperBound = entry;
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
			debug_printf("Flushing page start=%d i=%d\nlower: %s\nupper: %s\n", start, i, pageLowerBound.toString().c_str(), pageUpperBound.toString().c_str());
#if REDWOOD_DEBUG
			for(int j = start; j < i; ++j) {
				debug_printf(" %3d: %s\n", j, entries[j].toString().c_str());
				if(j > start) {
					ASSERT(entries[j] > entries[j - 1]);
				}
			}
#endif
			ASSERT(pageLowerBound <= pageUpperBound);

			union {
				BTreePage *btPage;
				uint8_t *btPageMem;
			};

			int allocatedSize;
			if(blockCount == 1) {
				Reference<IPage> page = newBlockFn();
				VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());
				btPageMem = page->mutate();
				allocatedSize = page->size();
				pages.push_back({pageLowerBound, page});
			}
			else {
				ASSERT(blockCount > 1);
				allocatedSize = usableBlockSize * blockCount;
				btPageMem = new uint8_t[allocatedSize];
				VALGRIND_MAKE_MEM_DEFINED(btPageMem, allocatedSize);
			}

			btPage->flags = newFlags;
			btPage->kvBytes = kvBytes;
			btPage->count = i - start;
			btPage->extensionPageCount = blockCount - 1;

			int written = btPage->tree().build(&entries[start], &entries[i], &pageLowerBound, &pageUpperBound);
			if(written > pageSize) {
				fprintf(stderr, "ERROR:  Wrote %d bytes to %d byte page (%d blocks). recs %d  kvBytes %d  compressed %d\n", written, pageSize, blockCount, i - start, kvBytes, compressedBytes);
				ASSERT(false);
			}

			if(blockCount != 1) {
				Reference<IPage> page = newBlockFn();
				VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());

				const uint8_t *rptr = btPageMem;
				memcpy(page->mutate(), rptr, usableBlockSize);
				rptr += usableBlockSize;
				
				std::vector<Reference<IPage>> extPages;
				for(int b = 1; b < blockCount; ++b) {
					Reference<IPage> extPage = newBlockFn();
					VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());

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
			compressedBytes = BTreePage::BinaryTree::GetTreeOverhead();
			pageLowerBound = pageUpperBound;
		}
	}

	//debug_printf("buildPages: returning pages.size %lu, kvpairs %lu\n", pages.size(), kvPairs.size());
	return pages;
}

#define NOT_IMPLEMENTED { UNSTOPPABLE_ASSERT(false); }

class VersionedBTree : public IVersionedStore {
public:
	// The first possible internal record possible in the tree
	static RedwoodRecordRef dbBegin;
	// A record which is greater than the last possible record in the tree
	static RedwoodRecordRef dbEnd;

	struct Counts {
		Counts() {
			memset(this, 0, sizeof(Counts));
		}

		void clear() {
			*this = Counts();
		}

		int64_t pageWrites;
		int64_t blockWrites;
		int64_t sets;
		int64_t clears;
		int64_t commits;

		std::string toString() const {
			std::string s = format("sets=%lld clears=%lld commits=%lld pages=%lld blocks=%lld\n", sets, clears, commits, pageWrites, blockWrites);
			return s;
		}
	};

	Counts counts;

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
		++counts.sets;
		SingleKeyMutationsByVersion &changes = insertMutationBoundary(keyValue.key)->second.startKeyMutations;

		// Add the set if the changes set is empty or the last entry isn't a set to exactly the same value
		if(changes.empty() || !changes.rbegin()->second.equalToSet(keyValue.value)) {
			changes[m_writeVersion] = SingleKeyMutation(keyValue.value);
		}
	}
	virtual void clear(KeyRangeRef range) {
		++counts.clears;
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
			self->writePage(self->m_root, page, latest, &dbBegin, &dbEnd);
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
		return Reference<IStoreCursor>(new Cursor(m_pager->getReadSnapshot(v), m_root, m_usablePageSizeOverride));
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
			(*m_pBuffer)[dbBegin.key];
			(*m_pBuffer)[dbEnd.key];
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
	void writePage(LogicalPageID id, Reference<IPage> page, Version ver, const RedwoodRecordRef *pageLowerBound, const RedwoodRecordRef *pageUpperBound) {
		debug_printf("writePage(): %s\n", ((const BTreePage *)page->begin())->toString(true, id, ver, pageLowerBound, pageUpperBound).c_str());
		m_pager->writePage(id, page, ver);
	}

	LogicalPageID m_root;

	typedef std::pair<Standalone<RedwoodRecordRef>, LogicalPageID> KeyPagePairT;
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

		inline RedwoodRecordRef toRecord(KeyRef userKey, Version version) const {
			// No point in serializing an atomic op, it needs to be coalesced to a real value.
			ASSERT(!isAtomicOp());

			if(isClear())
				return RedwoodRecordRef(userKey, version);

			return RedwoodRecordRef(userKey, version, value);
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
			std::vector<RedwoodRecordRef> childEntries;
			for(int i=0; i<pages.size(); i++) {
				RedwoodRecordRef entry = pages[i].lowerBound;
				entry.value = StringRef((unsigned char *)&logicalPageIDs[i], sizeof(uint32_t));
				childEntries.push_back(entry);
			}

			int oldPages = pages.size();
			pages = buildPages(false, dbBegin, dbEnd, childEntries, 0, [=](){ return m_pager->newPageBuffer(); }, m_usablePageSizeOverride);

			debug_printf("Writing a new root level at version %lld with %lu children across %lu pages\n", version, childEntries.size(), pages.size());

			logicalPageIDs = writePages(pages, version, m_root, pPage, &dbEnd, nullptr);
		}
	}

	std::vector<LogicalPageID> writePages(std::vector<BoundaryAndPage> pages, Version version, LogicalPageID originalID, const BTreePage *originalPage, const RedwoodRecordRef *upperBound, void *actor_debug) {
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
			++counts.pageWrites;

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
					++counts.blockWrites;
				}

				debug_printf("%p: writePages(): Writing primary page op=write id=%u @%lld (+%lu extension pages)\n", actor_debug, id, version, extPages.size());
				m_pager->writePage(id, pages[i].firstPage, version);
			}
			else {
				debug_printf("%p: writePages(): Writing normal page op=write id=%u @%lld\n", actor_debug, id, version);
				writePage(id, pages[i].firstPage, version, &pages[i].lowerBound, (i == pages.size() - 1) ? upperBound : &pages[i + 1].lowerBound);
				++counts.blockWrites;
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

	ACTOR static Future<Reference<const IPage>> readPage(Reference<IPagerSnapshot> snapshot, LogicalPageID id, int usablePageSize, const RedwoodRecordRef *lowerBound, const RedwoodRecordRef *upperBound) {
		debug_printf("readPage() op=read id=%u @%lld\n", id, snapshot->getVersion());

		state Reference<const IPage> result = wait(snapshot->getPhysicalPage(id));
		state const BTreePage *pTreePage = (const BTreePage *)result->begin();

		if(pTreePage->extensionPageCount == 0) {
			debug_printf("readPage() Found normal page for op=read id=%u @%lld\n", id, snapshot->getVersion());
		}
		else {
			std::vector<Future<Reference<const IPage>>> pageGets;
			pageGets.push_back(std::move(result));

			for(int i = 0; i < pTreePage->extensionPageCount; ++i) {
				debug_printf("readPage() Reading extension page op=read id=%u @%lld ext=%d/%d\n", pTreePage->extensionPages[i], snapshot->getVersion(), i + 1, (int)pTreePage->extensionPageCount);
				pageGets.push_back(snapshot->getPhysicalPage(pTreePage->extensionPages[i]));
			}

			std::vector<Reference<const IPage>> pages = wait(getAll(pageGets));
			result = Reference<const IPage>(new SuperPage(pages, usablePageSize));
			pTreePage = (const BTreePage *)result->begin();
		}

		if(result->userData == nullptr) {
			result->userData = new BTreePage::BinaryTree::Reader(&pTreePage->tree(), lowerBound, upperBound);
			result->userDataDestructor = [](void *ptr) { delete (BTreePage::BinaryTree::Reader *)ptr; };
		}

		debug_printf("readPage() %s\n", pTreePage->toString(false, id, snapshot->getVersion(), lowerBound, upperBound).c_str());

		// Nothing should attempt to read bytes in the page outside the BTreePage structure
		VALGRIND_MAKE_MEM_UNDEFINED(result->begin() + pTreePage->size(), result->size() - pTreePage->size());

		return result;
	}

	// Returns list of (version, list of (lower_bound, list of children) )
	// TODO:  Probably should pass prev/next records by pointer in many places
	ACTOR static Future<VersionedChildrenT> commitSubtree(VersionedBTree *self, MutationBufferT *mutationBuffer, Reference<IPagerSnapshot> snapshot, LogicalPageID root, const RedwoodRecordRef *lowerBound, const RedwoodRecordRef *upperBound) {
		debug_printf("%p commitSubtree: root=%d lower='%s' upper='%s'\n", this, root, lowerBound->toString().c_str(), upperBound->toString().c_str());

		// Find the slice of the mutation buffer that is relevant to this subtree
		// TODO:  Rather than two lower_bound searches, perhaps just compare each mutation to the upperBound key while iterating
		state MutationBufferT::const_iterator iMutationBoundary = mutationBuffer->lower_bound(lowerBound->key);
		state MutationBufferT::const_iterator iMutationBoundaryEnd = mutationBuffer->lower_bound(upperBound->key);

		// If the lower bound key and the upper bound key are the same then there can't be any changes to
		// this subtree since changes would happen after the upper bound key as the mutated versions would
		// necessarily be higher.
		if(lowerBound->key == upperBound->key) {
			debug_printf("%p no changes, lower and upper bound keys are the same.\n", this);
			return VersionedChildrenT({ {0,{{*lowerBound,root}}} });
		}

		// If the mutation buffer key found is greater than the lower bound key then go to the previous mutation
		// buffer key because it may cover deletion of some keys at the start of this subtree.
		if(iMutationBoundary != mutationBuffer->begin() && iMutationBoundary->first > lowerBound->key) {
			--iMutationBoundary;
		}
		else {
			// If the there are no mutations, we're done
			if(iMutationBoundary == iMutationBoundaryEnd) {
				debug_printf("%p no changes, mutation buffer start/end are the same\n", this);
				return VersionedChildrenT({ {0,{{*lowerBound,root}}} });
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
			return VersionedChildrenT({ {0,{{*lowerBound,root}}} });
		}

		state Reference<const IPage> rawPage = wait(readPage(snapshot, root, self->m_usablePageSizeOverride, lowerBound, upperBound));
		state BTreePage *page = (BTreePage *) rawPage->begin();
		debug_printf("%p commitSubtree(): %s\n", this, page->toString(false, root, snapshot->getVersion(), lowerBound, upperBound).c_str());

		BTreePage::BinaryTree::Cursor cursor = getReader(rawPage)->getCursor();
		cursor.moveFirst();

		// Leaf Page
		if(page->flags & BTreePage::IS_LEAF) {
			VersionedChildrenT results;
			std::vector<RedwoodRecordRef> merged;
			Arena mergedArena;

			debug_printf("%p MERGING EXISTING DATA WITH MUTATIONS:\n", this);
			self->printMutationBuffer(iMutationBoundary, iMutationBoundaryEnd);

			// It's a given that the mutation map is not empty so it's safe to do this
			Key mutationRangeStart = iMutationBoundary->first;

			// If replacement pages are written they will be at the minimum version seen in the mutations for this leaf
			Version minVersion = invalidVersion;

			// Now, process each mutation range and merge changes with existing data.
			while(iMutationBoundary != iMutationBoundaryEnd) {
				debug_printf("%p New mutation boundary: '%s': %s\n", this, printable(iMutationBoundary->first).c_str(), iMutationBoundary->second.toString().c_str());

				SingleKeyMutationsByVersion::const_iterator iMutations;

				// If the mutation boundary key is less than the lower bound key then skip startKeyMutations for
				// this bounary, we're only processing this mutation range here to apply any clears to existing data.
				if(iMutationBoundary->first < lowerBound->key)
					iMutations = iMutationBoundary->second.startKeyMutations.end();
				// If the mutation boundary key is the same as the page lowerBound key then start reading single
				// key mutations at the first version greater than the lowerBound key's version.
				else if(iMutationBoundary->first == lowerBound->key)
					iMutations = iMutationBoundary->second.startKeyMutations.upper_bound(lowerBound->version);
				else
					iMutations = iMutationBoundary->second.startKeyMutations.begin();

				SingleKeyMutationsByVersion::const_iterator iMutationsEnd = iMutationBoundary->second.startKeyMutations.end();

				// Output old versions of the mutation boundary key
				while(cursor.valid() && cursor.get().key == iMutationBoundary->first) {
					merged.push_back(cursor.get());
					debug_printf("%p: Added %s [existing, boundary start]\n", this, merged.back().toString().c_str());
					cursor.moveNext();
				}

				// TODO:  If a mutation set is equal to the previous existing value of the key, maybe don't write it.
				// Output mutations for the mutation boundary start key
				while(iMutations != iMutationsEnd) {
					const SingleKeyMutation &m = iMutations->second;
					int maxPartSize = std::min(255, self->m_usablePageSizeOverride / 5);
					if(m.isClear() || m.value.size() <= maxPartSize) {
						if(iMutations->first < minVersion || minVersion == invalidVersion)
							minVersion = iMutations->first;
						merged.push_back(iMutations->second.toRecord(iMutationBoundary->first, iMutations->first));
						debug_printf("%p: Added non-split %s [mutation, boundary start]\n", this, merged.back().toString().c_str());
					}
					else {
						if(iMutations->first < minVersion || minVersion == invalidVersion)
							minVersion = iMutations->first;
						int bytesLeft = m.value.size();
						int start = 0;
						RedwoodRecordRef whole(iMutationBoundary->first, iMutations->first, m.value);
						while(bytesLeft > 0) {
							int partSize = std::min(bytesLeft, maxPartSize);
							// Don't copy the value chunk because this page will stay in memory until after we've built new version(s) of it
							merged.push_back(whole.split(start, partSize));
							bytesLeft -= partSize;
							start += partSize;
							debug_printf("%p: Added split %s [mutation, boundary start]\n", this, merged.back().toString().c_str());
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
				while(cursor.valid() && cursor.get().key < iMutationBoundary->first) {
					merged.push_back(cursor.get());
					debug_printf("%p: Added %s [existing, middle]\n", this, merged.back().toString().c_str());

					// Write a clear of this key if needed.  A clear is required if clearRangeVersion is set and the next cursor
					// key is different than the current one.  If the last cursor key in the page is different from the
					// first key in the right sibling page then the page's upper bound will reflect that.
					auto nextCursor = cursor;
					nextCursor.moveNext();

					if(clearRangeVersion.present() && cursor.get().key != nextCursor.getOrUpperBound().key) {
						Version clearVersion = clearRangeVersion.get();
						if(clearVersion < minVersion || minVersion == invalidVersion)
							minVersion = clearVersion;
						merged.push_back(RedwoodRecordRef(cursor.get().key, clearVersion));
						debug_printf("%p: Added %s [existing, middle clear]\n", this, merged.back().toString().c_str());
					}
					cursor = nextCursor;
				}
			}

			// Write any remaining existing keys, which are not subject to clears as they are beyond the cleared range.
			while(cursor.valid()) {
				merged.push_back(cursor.get());
				debug_printf("%p: Added %s [existing, tail]\n", this, merged.back().toString().c_str());
				cursor.moveNext();
			}

			debug_printf("%p Done merging mutations into existing leaf contents\n", this);

			// No changes were actually made.  This could happen if there is a clear which does not cover an entire leaf but also does
			// not which turns out to not match any existing data in the leaf.
			if(minVersion == invalidVersion) {
				debug_printf("%p No changes were made during mutation merge\n", this);
				return VersionedChildrenT({ {0,{{*lowerBound,root}}} });
			}

			// TODO: Make version and key splits based on contents of merged list, if keeping history

			IPager *pager = self->m_pager;
			std::vector<BoundaryAndPage> pages = buildPages(true, *lowerBound, *upperBound, merged, BTreePage::IS_LEAF, [pager](){ return pager->newPageBuffer(); }, self->m_usablePageSizeOverride);

			// If there isn't still just a single page of data then this page became too large and was split.
			// The new split pages will be valid as of minVersion, but the old page remains valid at the old version
			// (TODO: unless history isn't being kept at all)
			if(pages.size() != 1) {
				results.push_back( {0, {{*lowerBound, root}}} );
			}

			if(pages.size() == 1)
				minVersion = 0;

			// Write page(s), get new page IDs
			std::vector<LogicalPageID> newPageIDs = self->writePages(pages, minVersion, root, page, upperBound, this);

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
				const RedwoodRecordRef &lower = (i == 0) ? *lowerBound : pages[i].lowerBound;
				debug_printf("%p Adding page to results: %s => %d\n", this, lower.toString().c_str(), newPageIDs[i]);
				results.back().second.push_back( {lower, newPageIDs[i]} );
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
			while(cursor.valid()) {
				// The lower bound for the first child is the lowerBound arg
				const RedwoodRecordRef &childLowerBound = first ? *lowerBound : cursor.get();
				if(first)
					first = false;

				uint32_t pageID = *(uint32_t*)cursor.get().value.get().begin();
				ASSERT(pageID != 0);

				const RedwoodRecordRef &childUpperBound = cursor.moveNext() ? cursor.get() : *upperBound;

				debug_printf("lower          '%s'\n", childLowerBound.toString().c_str());
				debug_printf("upper          '%s'\n", childUpperBound.toString().c_str());
				ASSERT(childLowerBound <= childUpperBound);

				futureChildren.push_back(self->commitSubtree(self, mutationBuffer, snapshot, pageID, &childLowerBound, &childUpperBound));
				childPageIDs.push_back(pageID);
			}

			//wait(waitForAll(futureChildren));
			state int k;
			for(k = 0; k < futureChildren.size(); ++k) {
				wait(success(futureChildren[k]));
			}

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
				return VersionedChildrenT({{0, {{*lowerBound, root}}}});
			}

			Version version = 0;
			VersionedChildrenT result;

			loop { // over version splits of this page
				Version nextVersion = std::numeric_limits<Version>::max();

				std::vector<RedwoodRecordRef> childEntries;

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
							debug_printf("%p      '%s' -> Page id=%u\n", this, boundaryPage.first.toString().c_str(), boundaryPage.second);
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
						debug_printf("%p  Adding child page %s\n", this, childPage.first.toString().c_str());
						RedwoodRecordRef entry = childPage.first;
						entry.value = StringRef((unsigned char *)&childPage.second, sizeof(uint32_t));
						childEntries.push_back(entry);
					}
				}

				debug_printf("%p Finished pass through futurechildren.  childEntries=%lu  version=%lld  nextVersion=%lld\n", this, childEntries.size(), version, nextVersion);

				if(modified) {
					// TODO: Track split points across iterations of this loop, so that they don't shift unnecessarily and
					// cause unnecessary path copying

					IPager *pager = self->m_pager;
					std::vector<BoundaryAndPage> pages = buildPages(false, *lowerBound, *upperBound, childEntries, 0, [pager](){ return pager->newPageBuffer(); }, self->m_usablePageSizeOverride);

					// Write page(s), use version 0 to replace latest version if only writing one page
					std::vector<LogicalPageID> newPageIDs = self->writePages(pages, version, root, page, upperBound, this);

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
					result.push_back({0, {{*lowerBound, root}}});
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

		VersionedChildrenT _ = wait(commitSubtree(self, mutations, self->m_pager->getReadSnapshot(latestVersion), self->m_root, &dbBegin, &dbEnd));

		self->m_pager->setLatestVersion(writeVersion);
		debug_printf("%s: Committing pager %lld\n", self->m_name.c_str(), writeVersion);
		wait(self->m_pager->commit());
		debug_printf("%s: Committed version %lld\n", self->m_name.c_str(), writeVersion);

		// Now that everything is committed we must delete the mutation buffer.
		// Our buffer's start version should be the oldest mutation buffer version in the map.
		ASSERT(mutationBufferStartVersion == self->m_mutationBuffers.begin()->first);
		self->m_mutationBuffers.erase(self->m_mutationBuffers.begin());

		self->m_lastCommittedVersion = writeVersion;
		++self->counts.commits;
		committed.send(Void());

		return Void();
	}

	// InternalCursor is for seeking to and iterating over the 'internal' records (not user-visible) in the Btree.
	// These records are versioned and they can represent deletedness or partial values.
	struct InternalCursor {
	private:
		// Each InternalCursor's position is represented by a reference counted PageCursor, which links
		// to its parent PageCursor, up to a PageCursor representing a cursor on the root page.
		// PageCursors can be shared by many InternalCursors, making InternalCursor copying low overhead
		struct PageCursor : ReferenceCounted<PageCursor>, FastAllocated<PageCursor> {
			Reference<PageCursor> parent;
			LogicalPageID pageID;       // Only needed for debugging purposes
			Reference<const IPage> page;
			BTreePage::BinaryTree::Cursor cursor;

			PageCursor(LogicalPageID id, Reference<const IPage> page, Reference<PageCursor> parent = {})
				: pageID(id), page(page), parent(parent), cursor(getReader().getCursor())
			{
			}

			PageCursor(const PageCursor &toCopy) : parent(toCopy.parent), pageID(toCopy.pageID), page(toCopy.page), cursor(toCopy.cursor) {
			}

			// Convenience method for copying a PageCursor
			Reference<PageCursor> copy() const {
				return Reference<PageCursor>(new PageCursor(*this));
			}

			// Multiple InternalCursors can share a Page 
			BTreePage::BinaryTree::Reader & getReader() const {
				return *(BTreePage::BinaryTree::Reader *)page->userData;
			}

			bool isLeaf() const {
				const BTreePage *p = ((const BTreePage *)page->begin());
				return p->isLeaf();
			}

			Future<Reference<PageCursor>> getChild(Reference<IPagerSnapshot> pager, int usablePageSizeOverride) {
				ASSERT(!isLeaf());
				BTreePage::BinaryTree::Cursor next = cursor;
				next.moveNext();
				const RedwoodRecordRef &rec = cursor.get();
				LogicalPageID id = *(LogicalPageID *)rec.value.get().begin();
				Future<Reference<const IPage>> child = readPage(pager, id, usablePageSizeOverride, &rec, &next.getOrUpperBound());
				return map(child, [=](Reference<const IPage> page) {
					return Reference<PageCursor>(new PageCursor(id, page, Reference<PageCursor>::addRef(this)));
				});
			}

			std::string toString() const {
				return format("Page %lu, %s", pageID, cursor.valid() ? cursor.get().toString().c_str() : "<invalid>");
			}
		};

		LogicalPageID rootPageID;
		int usablePageSizeOverride;
		Reference<IPagerSnapshot> pager;
		Reference<PageCursor> pageCursor;

	public:
		InternalCursor() {
		}

		InternalCursor(Reference<IPagerSnapshot> pager, LogicalPageID root, int usablePageSizeOverride)
			: pager(pager), rootPageID(root), usablePageSizeOverride(usablePageSizeOverride) {
		}

		std::string toString() const {
			std::string r;
			Reference<PageCursor> c = pageCursor;
			while(c) {
				r = format("[%s] ", c->toString().c_str()) + r;
				c = c->parent;
			}
			return r;
		}

		// Returns true if cursor position is a valid leaf page record
		bool valid() const {
			return pageCursor && pageCursor->isLeaf() && pageCursor->cursor.valid();
		}

		// Returns true if cursor position is valid() and has a present record value
		bool present() {
			return valid() && pageCursor->cursor.get().value.present();
		}

		// Returns true if cursor position is present() and has an effective version <= v
		bool presentAtVersion(Version v) {
			return present() && pageCursor->cursor.get().version <= v;
		}

		// Returns true if cursor position is present() and has an effective version <= v
		bool validAtVersion(Version v) {
			return valid() && pageCursor->cursor.get().version <= v;
		}

		const RedwoodRecordRef & get() const {
			return pageCursor->cursor.get();
		}

		// Ensure that pageCursor is not shared with other cursors so we can modify it
		void ensureUnshared() {
			if(!pageCursor->isSoleOwner()) {
				pageCursor = pageCursor->copy();
			}
		}

		Future<Void> moveToRoot() {
			// If pageCursor exists follow parent links to the root
			if(pageCursor) {
				while(pageCursor->parent) {
					pageCursor = pageCursor->parent;
				}
				return Void();
			}

			// Otherwise read the root page
			Future<Reference<const IPage>> root = readPage(pager, rootPageID, usablePageSizeOverride, &dbBegin, &dbEnd);
			return map(root, [=](Reference<const IPage> p) {
				pageCursor = Reference<PageCursor>(new PageCursor(rootPageID, p));
				return Void();
			});
		}

		ACTOR Future<bool> seekLessThanOrEqual_impl(InternalCursor *self, RedwoodRecordRef query) {
			Future<Void> f = self->moveToRoot();

			// f will almost always be ready
			if(!f.isReady()) {
				wait(f);
			}

			self->ensureUnshared();

			loop {
				if(self->pageCursor->cursor.seekLessThanOrEqual(query)) {
					// If we found a record <= query at a leaf page then return success
					if(self->pageCursor->isLeaf()) {
						return true;
					}

					// Otherwise move to next child page
					Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager, self->usablePageSizeOverride));
					self->pageCursor = child;
				}
				else {
					// No records <= query on this page, so move to immediate previous record at leaf level
					bool success = wait(self->move(false));
					return success;
				}
			}
		}

		Future<bool> seekLTE(RedwoodRecordRef query) {
			return seekLessThanOrEqual_impl(this, query);
		}

		ACTOR Future<bool> move_impl(InternalCursor *self, bool forward) {
			// Try to move pageCursor, if it fails to go parent, repeat until it works or root cursor can't be moved
			while(1) {
				self->ensureUnshared();
				bool success = self->pageCursor->cursor.valid() && (forward ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev());

				// Stop if successful or there's no parent to move to
				if(success || !self->pageCursor->parent) {
					break;
				}

				// Move to parent
				self->pageCursor = self->pageCursor->parent;
			}

			// If pageCursor not valid we've reached an end of the tree
			if(!self->pageCursor->cursor.valid()) {
				return false;
			}

			// While not on a leaf page, move down to get to one.
			while(!self->pageCursor->isLeaf()) {
				Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager, self->usablePageSizeOverride));
				bool success = forward ? child->cursor.moveFirst() : child->cursor.moveLast();
				self->pageCursor = child;
			}

			return true;
		}

		Future<bool> move(bool forward) {
			return move_impl(this, forward);
		}

		Future<bool> moveNext() {
			return move_impl(this, true);
		}
		Future<bool> movePrev() {
			return move_impl(this, false);
		}

		// Move to the first or last record of the database.
		ACTOR Future<bool> move_end(InternalCursor *self, bool begin) {
			Future<Void> f = self->moveToRoot();

			// f will almost always be ready
			if(!f.isReady()) {
				wait(f);
			}

			self->ensureUnshared();

			loop {
				// Move to first or last record in the page
				bool success = begin ? self->pageCursor->cursor.moveFirst() : self->pageCursor->cursor.moveLast();

				// If it worked, return true if we've reached a leaf page otherwise go to the next child
				if(success) {
					if(self->pageCursor->isLeaf()) {
						return true;
					}
					Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager, self->usablePageSizeOverride));
					self->pageCursor = child;
				}
				else {
					return false;
				}
			}
		}

		Future<bool> moveFirst() {
			return move_end(this, true);
		}
		Future<bool> moveLast() {
			return move_end(this, false);
		}

	};

	// Cursor is for reading and interating over user visible KV pairs at a specific version
	// KeyValueRefs returned become invalid once the cursor is moved
	class Cursor : public IStoreCursor, public ReferenceCounted<Cursor>, public FastAllocated<Cursor>, NonCopyable  {
	public:
		Cursor(Reference<IPagerSnapshot> pageSource, LogicalPageID root, int usablePageSizeOverride)
			: m_version(pageSource->getVersion()),
			m_cur1(pageSource, root, usablePageSizeOverride),
			m_cur2(m_cur1)
		{
		}

		void addref() { ReferenceCounted<Cursor>::addref(); }
		void delref() { ReferenceCounted<Cursor>::delref(); }

	private:
		Version m_version;
		// If kv is valid
		//   - kv.key references memory held by cur1
		//   - If cur1 points to a non split KV pair
		//       - kv.value references memory held by cur1
		//       - cur2 points to the next internal record after cur1
		//     Else
		//       - kv.value references memory in arena
		//       - cur2 points to the first internal record of the split KV pair
		InternalCursor m_cur1;
		InternalCursor m_cur2;
		Arena m_arena;
		Optional<KeyValueRef> m_kv;

	public:
		virtual Future<Void> findEqual(KeyRef key) { return find_impl(this, key, true, 0); }
		virtual Future<Void> findFirstEqualOrGreater(KeyRef key, bool needValue, int prefetchNextBytes) { return find_impl(this, key, needValue, 1); }
		virtual Future<Void> findLastLessOrEqual(KeyRef key, bool needValue, int prefetchPriorBytes) { return find_impl(this, key, needValue, -1); }

		virtual Future<Void> next(bool needValue) { return move(this, true, needValue); }
		virtual Future<Void> prev(bool needValue) { return move(this, false, needValue); }

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

		std::string toString() const {
			std::string r;
			r += format("Cursor(%p) ver: %lld ", this, m_version);
			r += format("  KV: '%s' -> '%s'\n",
				m_kv.present() ? m_kv.get().key.printable().c_str() : "<np>",
				m_kv.present() ? m_kv.get().value.printable().c_str() : "");
			r += format("  Cur1: %s\n", m_cur1.toString().c_str());
			r += format("  Cur2: %s\n", m_cur2.toString().c_str());
			return r;
		}

	private:
		// find key in tree closest to or equal to key (at this cursor's version)
		// for less than or equal use cmp < 0
		// for greater than or equal use cmp > 0
		// for equal use cmp == 0
		ACTOR static Future<Void> find_impl(Cursor *self, KeyRef key, bool needValue, int cmp) {
			// Search for the last key at or before (key, version, \xff)
			state RedwoodRecordRef query(key, self->m_version);
			self->m_kv.reset();

			wait(success(self->m_cur1.seekLTE(query)));
			debug_printf("find%sE(%s): %s\n", cmp > 0 ? "GT" : (cmp == 0 ? "" : "LT"), query.toString().c_str(), self->toString().c_str());

			// If we found the target key with a present value then return it as it is valid for any cmp type
			if(self->m_cur1.present() && self->m_cur1.get().key == key) {
				debug_printf("Target key found, reading full KV pair.  Cursor: %s\n", self->toString().c_str());
				wait(self->readFullKVPair(self));
				return Void();
			}

			// Mode is ==, so if we're still here we didn't find it.
			if(cmp == 0) {
				return Void();
			}

			// Mode is >=, so if we're here we have to go to the next present record at the target version
			// because the seek done above was <= query
			if(cmp > 0) {
				// icur is at a record < query or invalid.

				// If cursor is invalid, try to go to start of tree
				if(!self->m_cur1.valid()) {
					bool valid = wait(self->m_cur1.moveFirst());
					if(!valid) {
						self->m_kv.reset();
						return Void();
					}
				}
				else {
					loop {
						bool valid = wait(self->m_cur1.move(true));
						if(!valid) {
							self->m_kv.reset();
							return Void();
						}

						if(self->m_cur1.get().key > key) {
							break;
						}
					}
				}

				// Get the next present key at the target version.  Handles invalid cursor too.
				wait(self->next(needValue));
			}
			else if(cmp < 0) {
				// Mode is <=, which is the same as the seekLTE(query)
				if(!self->m_cur1.valid()) {
					self->m_kv.reset();
					return Void();
				}

				// Move to previous present kv pair at the target version
				wait(self->prev(needValue));
			}

			return Void();
		}

		// TODO: use needValue
		ACTOR static Future<Void> move(Cursor *self, bool fwd, bool needValue) {
			debug_printf("Cursor::move(%d): Cursor = %s\n", fwd, self->toString().c_str());
			ASSERT(self->m_cur1.valid());

			// If kv is present then the key/version at cur1 was already returned so move to a new key
			// Move cur1 until failure or a new key is found, keeping prior record visited in cur2
			if(self->m_kv.present()) {
				ASSERT(self->m_cur1.valid());
				loop {
					self->m_cur2 = self->m_cur1;
					bool valid = wait(self->m_cur1.move(fwd));
					if(!valid || self->m_cur1.get().key != self->m_cur2.get().key) {
						break;
					}
				}
			}

			// Given two consecutive cursors c1 and c2, c1 represents a returnable record if
			//    c1.presentAtVersion(v) || (!c2.validAtVersion() || c2.get().key != c1.get().key())
			// Note the distinction between 'present' and 'valid'.  Present means the value for the key
			// exists at the version (but could be the empty string) while valid just means the internal
			// record is in effect at that version but it could indicate that the key was cleared and
			// no longer exists from the user's perspective at that version
			//
			// If moving forward, cur2 must be the record after cur1 so we can determine if
			// cur1 is to be returned below.
			// If moving backward, cur2 is already the record after cur1
			if(fwd && self->m_cur1.valid()) {
				self->m_cur2 = self->m_cur1;
				wait(success(self->m_cur2.move(true)));
			}

			while(self->m_cur1.valid()) {

				if(self->m_cur1.presentAtVersion(self->m_version) &&
					(!self->m_cur2.validAtVersion(self->m_version) ||
					self->m_cur2.get().key != self->m_cur1.get().key)
				) {
					wait(readFullKVPair(self));
					return Void();
				}

				if(fwd) {
					// Moving forward, move cur2 forward and keep cur1 pointing to the prior (predecessor) record
					self->m_cur1 = self->m_cur2;
					wait(success(self->m_cur2.move(true)));
				}
				else {
					// Moving backward, move cur1 backward and keep cur2 pointing to the prior (successor) record
					self->m_cur2 = self->m_cur1;
					wait(success(self->m_cur1.move(false)));
				}

			}

			self->m_kv.reset();
			return Void();
		}

		// Read all of the current key-value record starting at cur1 into kv
		ACTOR static Future<Void> readFullKVPair(Cursor *self) {
			self->m_arena = Arena();
			const RedwoodRecordRef &rec = self->m_cur1.get();
	
			debug_printf("readFullKVPair:  Starting at %s\n", self->toString().c_str());

			// Unsplit value, cur1 will hold the key and value memory
			if(!rec.isMultiPart()) {
				debug_printf("readFullKVPair:  Unsplit, exit.  %s\n", self->toString().c_str());

				self->m_kv = KeyValueRef(rec.key, rec.value.get());
				return Void();
			}

			// Split value, need to coalesce split value parts into a buffer in arena,
			// after which cur1 will point to the first part and kv.key will reference its key
			const RedwoodRecordRef::ValuePart &part = rec.valuePart.get();
			ASSERT(part.start + rec.value.get().size() == part.total);

			debug_printf("readFullKVPair:  Split, totalsize %d  %s\n", part.total, self->toString().c_str());

			// Allocate space for the entire value in the same arena as the key
			state int bytesLeft = part.total;
			state StringRef dst = makeString(bytesLeft, self->m_arena);

			loop {
				const RedwoodRecordRef &rec = self->m_cur1.get();
				const RedwoodRecordRef::ValuePart &part = rec.valuePart.get();

				debug_printf("readFullKVPair:  Adding chunk %s\n", rec.toString().c_str());

				int partSize = rec.value.get().size();
				memcpy(mutateString(dst) + part.start, rec.value.get().begin(), partSize);
				bytesLeft -= partSize;
				if(bytesLeft == 0) {
					self->m_kv = KeyValueRef(rec.key, dst);
					return Void();
				}
				ASSERT(bytesLeft > 0);
				// Move backward
				bool success = wait(self->m_cur1.move(false));
				ASSERT(success);
			}
		}
	};

};

RedwoodRecordRef VersionedBTree::dbBegin(StringRef(), 0);
RedwoodRecordRef VersionedBTree::dbEnd(LiteralStringRef("\xff\xff\xff\xff"), std::numeric_limits<Version>::max());

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
	int n = pow(g_random->random01(), 3) * max;
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
			state Arena arena;
			wait(cur->findEqual(KeyRef(arena, key)));

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

		state KeyValue kv = randomKV(10, 0);
		wait(cur->findFirstEqualOrGreater(kv.key, true, 0));
		state int c = g_random->randomInt(0, 100);
		while(cur->isValid() && c-- > 0) {
			wait(success(cur->next(true)));
			wait(yield());
		}
	}
}

struct SplitStringRef {
	StringRef a;
	StringRef b;

	SplitStringRef(StringRef a = StringRef(), StringRef b = StringRef()) : a(a), b(b) {
	}

	SplitStringRef getSplitPrefix(int len) const {
		if(len <= a.size()) {
			return SplitStringRef(a.substr(0, len));
		}
		len -= a.size();
		ASSERT(b.size() <= len);
		return SplitStringRef(a, b.substr(0, len));
	}

	StringRef getContiguousPrefix(int len, Arena &arena) const {
		if(len <= a.size()) {
			return a.substr(0, len);
		}
		StringRef c = makeString(len, arena);
		memcpy(mutateString(c), a.begin(), a.size());
		len -= a.size();
		memcpy(mutateString(c) + a.size(), b.begin(), len);
		return c;
	}

	int compare(const SplitStringRef &rhs) const {
		// TODO: Rewrite this..
		Arena a;
		StringRef self = getContiguousPrefix(size(), a);
		StringRef other = rhs.getContiguousPrefix(rhs.size(), a);
		return self.compare(other);
	}

	int compare(const StringRef &rhs) const {
		// TODO: Rewrite this..
		Arena a;
		StringRef self = getContiguousPrefix(size(), a);
		return self.compare(rhs);
	}

	int size() const {
		return a.size() + b.size();
	}

	std::string toString() const {
		return format("%s%s", a.toString().c_str(), b.toString().c_str());
	}

	std::string toHexString() const {
		return format("%s%s", a.toHexString().c_str(), b.toHexString().c_str());
	}
};

struct IntIntPair {
	IntIntPair() {}
	IntIntPair(int k, int v) : k(k), v(v) {}

	IntIntPair(Arena &arena, const IntIntPair &toCopy) {
		*this = toCopy;
	}

	struct Delta {
		int dk;
		int dv;

		IntIntPair apply(const IntIntPair &prev, const IntIntPair &next, Arena arena) {
			return {prev.k + dk, prev.v + dv};
		}

		int size() const {
			return sizeof(Delta);
		}

		std::string toString() const {
			return format("DELTA{dk=%d(0x%x) dv=%d(0x%x)}", dk, dk, dv, dv);
		}
	};

	int compare(const IntIntPair &rhs) const {
		//printf("compare %s to %s\n", toString().c_str(), rhs.toString().c_str());
		return k - rhs.k;
	}

	bool operator==(const IntIntPair &rhs) const {
		return k == rhs.k;
	}

	int deltaSize(const IntIntPair &base) const {
		return sizeof(Delta);
	}

	void writeDelta(Delta &d, const IntIntPair &prev, const IntIntPair &next) const {
		// Always borrow from prev
		d.dk = k - prev.k;
		d.dv = v - prev.v;
	}

	int k;
	int v;

	std::string toString() const {
		return format("{k=%d(0x%x) v=%d(0x%x)}", k, k, v, v);
	}
};

TEST_CASE("!/redwood/mutableDeltaTreeCorrectnessRedwoodRecord") {
	const int N = 200;

	RedwoodRecordRef prev;
	RedwoodRecordRef next(LiteralStringRef("\xff\xff\xff\xff"));

	Arena arena;
	std::vector<RedwoodRecordRef> items;
	for(int i = 0; i < N; ++i) {
		std::string k = g_random->randomAlphaNumeric(30);
		std::string v = g_random->randomAlphaNumeric(30);
		RedwoodRecordRef rec;
		rec.key = StringRef(arena, k);
		rec.version = g_random->coinflip() ? g_random->randomInt64(0, std::numeric_limits<Version>::max()) : invalidVersion;
		if(g_random->coinflip()) {
			rec.value = StringRef(arena, v);
			if(g_random->coinflip()) {
				RedwoodRecordRef::ValuePart part;
				part.start = g_random->randomInt(0, 5000);
				part.total = part.start + v.size() + g_random->randomInt(0, 5000);
				rec.valuePart = part;
			}
		}
		items.push_back(rec);
		//printf("i=%d %s\n", i, items.back().toString().c_str());
	}
	std::sort(items.begin(), items.end());

	DeltaTree<RedwoodRecordRef> *tree = (DeltaTree<RedwoodRecordRef> *) new uint8_t[N * 100];

	tree->build(&items[0], &items[items.size()], &prev, &next);

	printf("Count=%d  Size=%d  InitialDepth=%d\n", (int)items.size(), (int)tree->size(), (int)tree->initialDepth);
	debug_printf("Data(%p): %s\n", tree, StringRef((uint8_t *)tree, tree->size()).toHexString().c_str());

	DeltaTree<RedwoodRecordRef>::Reader r(tree, &prev, &next);
	DeltaTree<RedwoodRecordRef>::Cursor fwd = r.getCursor();
	DeltaTree<RedwoodRecordRef>::Cursor rev = r.getCursor();

	ASSERT(fwd.moveFirst());
	ASSERT(rev.moveLast());
	int i = 0;
	while(1) {
		if(fwd.get() != items[i]) {
			printf("forward iterator i=%d\n  %s found\n  %s expected\n", i, fwd.get().toString().c_str(), items[i].toString().c_str());
			printf("Delta: %s\n", fwd.node->raw->delta->toString().c_str());
			ASSERT(false);
		}
		if(rev.get() != items[items.size() - 1 - i]) {
			printf("reverse iterator i=%d\n  %s found\n  %s expected\n", i, rev.get().toString().c_str(), items[items.size() - 1 - i].toString().c_str());
			printf("Delta: %s\n", rev.node->raw->delta->toString().c_str());
			ASSERT(false);
		}
		++i;
		ASSERT(fwd.moveNext() == rev.movePrev());
		ASSERT(fwd.valid() == rev.valid());
		if(!fwd.valid()) {
			break;
		}
	}
	ASSERT(i == items.size());

	double start = timer();
	DeltaTree<RedwoodRecordRef>::Cursor c = r.getCursor();

	for(int i = 0; i < 20000000; ++i) {
		const RedwoodRecordRef &query = items[g_random->randomInt(0, items.size())];
		if(!c.seekLessThanOrEqual(query)) {
			printf("Not found!  query=%s\n", query.toString().c_str());
			ASSERT(false);
		}
		if(c.get() != query) {
			printf("Found incorrect node!  query=%s  found=%s\n", query.toString().c_str(), c.get().toString().c_str());
			ASSERT(false);
		}
	}
	double elapsed = timer() - start;
	printf("Elapsed %f\n", elapsed);

	return Void();
}

TEST_CASE("!/redwood/mutableDeltaTreeCorrectnessIntInt") {
	const int N = 200;
	IntIntPair prev = {0, 0};
	IntIntPair next = {1000, 0};

	std::vector<IntIntPair> items;
	for(int i = 0; i < N; ++i) {
		items.push_back({i*10, i*1000});
		//printf("i=%d %s\n", i, items.back().toString().c_str());
	}

	DeltaTree<IntIntPair> *tree = (DeltaTree<IntIntPair> *) new uint8_t[10000];

	tree->build(&items[0], &items[items.size()], &prev, &next);

	printf("Count=%d  Size=%d  InitialDepth=%d\n", (int)items.size(), (int)tree->size(), (int)tree->initialDepth);
	debug_printf("Data(%p): %s\n", tree, StringRef((uint8_t *)tree, tree->size()).toHexString().c_str());

	DeltaTree<IntIntPair>::Reader r(tree, &prev, &next);
	DeltaTree<IntIntPair>::Cursor fwd = r.getCursor();
	DeltaTree<IntIntPair>::Cursor rev = r.getCursor();

	ASSERT(fwd.moveFirst());
	ASSERT(rev.moveLast());
	int i = 0;
	while(1) {
		if(fwd.get() != items[i]) {
			printf("forward iterator i=%d\n  %s found\n  %s expected\n", i, fwd.get().toString().c_str(), items[i].toString().c_str());
			ASSERT(false);
		}
		if(rev.get() != items[items.size() - 1 - i]) {
			printf("reverse iterator i=%d\n  %s found\n  %s expected\n", i, rev.get().toString().c_str(), items[items.size() - 1 - i].toString().c_str());
			ASSERT(false);
		}
		++i;
		ASSERT(fwd.moveNext() == rev.movePrev());
		ASSERT(fwd.valid() == rev.valid());
		if(!fwd.valid()) {
			break;
		}
	}
	ASSERT(i == items.size());

	DeltaTree<IntIntPair>::Cursor c = r.getCursor();

	double start = timer();
	for(int i = 0; i < 20000000; ++i) {
		IntIntPair p({g_random->randomInt(0, items.size() * 10), 0});
		if(!c.seekLessThanOrEqual(p)) {
			printf("Not found!  query=%s\n", p.toString().c_str());
			ASSERT(false);
		}
		if(c.get().k != (p.k - (p.k % 10))) {
			printf("Found incorrect node!  query=%s  found=%s\n", p.toString().c_str(), c.get().toString().c_str());
			ASSERT(false);
		}
	}
	double elapsed = timer() - start;
	printf("Elapsed %f\n", elapsed);

	return Void();
}

struct SimpleCounter {
	SimpleCounter() : x(0), xt(0), t(timer()), start(t) {}
	void operator+=(int n) { x += n; }
	void operator++() { x++; }
	int64_t get() { return x; }
	double rate() {
		double t2 = timer();
		int r = (x - xt) / (t2 - t);
		xt = x;
		t = t2;
		return r;
	}
	double avgRate() { return x / (timer() - start); }
	int64_t x;
	double t;
	double start;
	int64_t xt;
	std::string toString() { return format("%lld/%.2f/%.2f", x, rate() / 1e6, avgRate() / 1e6); }
};

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

	// We must be able to fit at least two any two keys plus overhead in a page to prevent
	// a situation where the tree cannot be grown upward with decreasing level size.
	// TODO:  Handle arbitrarily large keys
	state int maxKeySize = g_random->randomInt(4, pageSize * 2);
	state int maxValueSize = g_random->randomInt(0, pageSize * 2);

	state int mutationBytesTarget = g_random->randomInt(100, (maxKeySize + maxValueSize) * 2000);

	printf("Using page size %d, max key size %d, max value size %d, total mutation byte target %d\n", pageSize, maxKeySize, maxValueSize, mutationBytesTarget);

	state std::map<std::pair<std::string, Version>, Optional<std::string>> written;
	state std::set<Key> keys;

	state Version lastVer = wait(btree->getLatestVersion());
	printf("Starting from version: %lld\n", lastVer);

	state Version version = lastVer + 1;
	btree->setWriteVersion(version);

	state SimpleCounter mutationBytes;
	state SimpleCounter keyBytesInserted;
	state SimpleCounter valueBytesInserted;
	state SimpleCounter sets;
	state SimpleCounter rangeClears;
	state SimpleCounter keyBytesCleared;
	state SimpleCounter valueBytesCleared;
	state int errorCount;

	state PromiseStream<Version> committedVersions;
	state Future<Void> verifyTask = verify(btree, committedVersions.getFuture(), &written, &errorCount);
	state Future<Void> randomTask = randomReader(btree) || btree->getError();

	state Future<Void> commit = Void();

	while(mutationBytes.get() < mutationBytesTarget) {
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

			++rangeClears;
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

						keyBytesCleared += last->first.first.size();
						valueBytesCleared += last->second.get().size();
						mutationBytes += (last->first.first.size() + last->second.get().size());

						// If the last set was at version then just make it not present
						if(last->first.second == version) {
							last->second.reset();
						}
						else {
							written[std::make_pair(last->first.first, version)].reset();
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

			debug_printf("      Set '%s' -> '%s' @%lld\n", kv.key.toString().c_str(), kv.value.toString().c_str(), version);

			++sets;
			keyBytesInserted += kv.key.size();
			valueBytesInserted += kv.value.size();
			mutationBytes += (kv.key.size() + kv.value.size());

			btree->set(kv);
			written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			keys.insert(kv.key);
		}

		// Sometimes (and at end) commit
		if(mutationBytes.get() >= mutationBytesTarget || g_random->random01() < .002) {
			// Wait for btree commit and send the new version to committedVersions.
			// Avoid capture of version as a member of *this
			Version v = version;
			commit = map(commit && btree->commit(), [=](Void) {
				// Notify the background verifier that version is committed and therefore readable
				committedVersions.send(v);
				return Void();
			});

			printf("Committing.  Stats: sets %s  clears %s  setKey %s  setVal %s  clearKey %s  clearVal %s  mutations %s\n",
				   sets.toString().c_str(),
				   rangeClears.toString().c_str(),
				   keyBytesInserted.toString().c_str(),
				   valueBytesInserted.toString().c_str(),
				   keyBytesCleared.toString().c_str(),
				   valueBytesCleared.toString().c_str(),
				   mutationBytes.toString().c_str()
			);

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
	state int versions = 15000;
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

		if(g_random->random01() < (1.0 / 1000)) {
			wait(commit);
			commit = btree->commit();
			wait(commit);
			double elapsed = now() - startTime;
			printf("Committed (cumulative) %lld bytes in %d records in %f seconds, %.2f MB/s %s\n", kvBytes, records, elapsed, kvBytes / elapsed / 1e6, btree->counts.toString().c_str());
		}
	}

	wait(btree->commit());
	double elapsed = now() - startTime;
	printf("Committed (cumulative) %lld bytes in %d records in %f seconds, %.2f MB/s %s\n", kvBytes, records, elapsed, kvBytes / elapsed / 1e6, btree->counts.toString().c_str());

	Future<Void> closedFuture = btree->onClosed();
	btree->close();
	wait(closedFuture);

	double elapsed = now() - startTime;
	printf("Wrote (final) %lld bytes in %d records in %f seconds, %.2f MB/s\n", kvBytes, records, elapsed, kvBytes / elapsed / 1e6);

	return Void();
}
