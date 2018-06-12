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
#include "IVersionedStore.h"
#include "IPager.h"
#include "fdbclient/Tuple.h"
#include "flow/serialize.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"
#include "MemoryPager.h"
#include "IndirectShadowPager.h"
#include <map>
#include <vector>
#include "fdbclient/CommitTransaction.h"
#include "IKeyValueStore.h"
#include "PrefixTree.h"

struct BTreePage {
	enum EPageFlags { IS_LEAF = 1};

	uint8_t flags;
	uint16_t count;
	uint32_t kvBytes;
	PrefixTree tree;

	std::string toString() const {
		std::string r;
		r += format("BTreePage %p tree %p flags 0x%X count %d kvBytes %d", this, &tree, (int)flags, (int)count, (int)kvBytes);
		if(count > 0) {
			PrefixTree::Cursor c = tree.getCursor();
			c.moveFirst();

			do {
				r += "\n  ";
				Tuple t = Tuple::unpack(c.getKey());
				for(int i = 0; i < t.size(); ++i) {
					if(i != 0)
						r += ",";
					if(t.getType(i) == Tuple::ElementType::BYTES)
						r += format("'%s'", t.getString(i).printable().c_str());
					if(t.getType(i) == Tuple::ElementType::INT)
						r += format("%lld", t.getInt(i));
				}
				r += " -> ";
				if(flags && IS_LEAF)
					r += format("'%s'", c.getValue().printable().c_str());
				else
					r += format("Page %u", *(const uint32_t *)c.getValue().begin());

			} while(c.moveNext());
		}

		return r;
	}
} __attribute__((packed, aligned(1)));

static void writeEmptyPage(Reference<IPage> page, uint8_t newFlags, int pageSize) {
	BTreePage *btpage = (BTreePage *)page->begin();
	btpage->flags = newFlags;
	btpage->kvBytes = 0;
	btpage->count = 0;
	btpage->tree.build(nullptr, nullptr, StringRef());
}

// Returns a vector of pairs of lower boundary key indices within kvPairs and encoded pages.
template<typename Allocator>
static vector<std::pair<int, Reference<IPage>>> buildPages(const PrefixTree::EntriesT &entries, uint8_t newFlags, Allocator const &newPageFn, int pageSize) {
	vector<std::pair<int, Reference<IPage>>> pages;

	int start = 0;
	// User key/value bytes in page
	int kvBytes = entries[start].key.size() + entries[start].value.size();
	// Estimate of per-node overhead
	const int avgOverheadPerNode = 6;
	int overheadEstimate = avgOverheadPerNode;
	int uniqueBytes = kvBytes;

	int i = start + 1;
	const int iEnd = entries.size();

	// Subtrace space used for page and prefix tree headers
	pageSize -= sizeof(BTreePage) + sizeof(PrefixTree);

	// Go all the way to the first out of bound index so the same page building block can catch the last incomplete page
	while(i <= iEnd) {
		bool end = i == iEnd;
		int common = end ? 0 : commonPrefixLength(entries[i].key, entries[i - 1].key);

		int valueSize = entries[i].value.size();
		int kvAdd = entries[i].key.size() + valueSize;
		int uniqueAdd = kvAdd - common;
		int overheadAdd = avgOverheadPerNode + (valueSize ? 1 : 0);

		// If the item is unlikely to fit within the page or the end is reached, write page for the interval [start, i)
		if((uniqueBytes + overheadEstimate + kvAdd + uniqueAdd + overheadAdd) > pageSize || end) {
			Reference<IPage> page = newPageFn();
			BTreePage *btpage = (BTreePage *)page->begin();
			btpage->flags = newFlags;
			btpage->kvBytes = kvBytes;
			btpage->count = i - start;
			// TODO:  Key boundary for tree level compression
			int written = btpage->tree.build(&entries[start], &entries[i], StringRef());
			if(written > pageSize) {
				printf("ERROR:  Wrote %d bytes to %d byte page. recs %d  uniqueBytes %d  overheadEstimate %d\n", written, pageSize, i - start, uniqueBytes, overheadEstimate);
				ASSERT(false);
			}
			pages.push_back({start, page});
			start = i;
			kvBytes = 0;
			overheadEstimate = 0;
			uniqueBytes = common;
			common = 0;
			if(end)
				break;
		}

		kvBytes += kvAdd;
		uniqueBytes += uniqueAdd;
		overheadEstimate += overheadAdd;
		++i;
	}

	//debug_printf("buildPages: returning pages.size %lu, kvpairs %lu\n", pages.size(), kvPairs.size());
	return pages;
}

// Represents a key, version, and value whole/part or lack thereof.
// This structure exists to make working with low level key/value pairs in the tree convenient.
// Values that are written whole have a value index of -1, which is not *actually* written
// to the tree but will be seen in this structure when unpacked.
// Split values are written such that the value index indicates the first byte offset in the
// value that the value part represents.
struct KeyVersionValue {
	KeyVersionValue() : version(invalidVersion), valueIndex(-1) {}
	KeyVersionValue(Key k, Version ver, int valIndex = -1, Optional<Value> val = Optional<Value>()) : key(k), version(ver), valueIndex(valIndex), value(val) {}
	/*
	bool operator< (KeyVersionValue const &rhs) const {
		int64_t cmp = key.compare(rhs.key);
		if(cmp == 0) {
			cmp = version - rhs.version;
			if(cmp == 0)
				return false;
		}
		return cmp < 0;
	} */
	Key key;
	Version version;
	int64_t valueIndex;
	Optional<Value> value;

	bool valid() const { return version != invalidVersion; }

	inline Standalone<KeyValueRef> pack() const {
		Tuple k;
		k.append(key);
		k.append(version);
		if(valueIndex >= 0)
			k.append(valueIndex);
		Tuple v;
		if(value.present())
			v.append(value.get());
		return KeyValueRef(k.pack(), v.pack());
	}

	// Supports partial/incomplete encoded sequences.
	static inline KeyVersionValue unpack(KeyValueRef kv) {
		//debug_printf("Unpacking: '%s' -> '%s' \n", kv.key.toHexString().c_str(), kv.value.toHexString().c_str());
		KeyVersionValue result;
		if(kv.key.size() != 0) {
			Tuple k = Tuple::unpack(kv.key, true);
			if(k.size() >= 1) {
				result.key = k.getString(0);
				if(k.size() >= 2) {
					result.version = k.getInt(1);
					if(k.size() >= 3) {
						result.valueIndex = k.getInt(2);
					}
				}
			}
		}

		if(kv.value.size() != 0) {
			Tuple v = Tuple::unpack(kv.value);
			if(v.getType(0) == Tuple::BYTES)
				result.value = v.getString(0);
		}

		return result;
	}

	// Convenience function for unpacking keys only
	static inline KeyVersionValue unpack(KeyRef key) {
		return unpack({key, StringRef()});
	}

	std::string toString() const {
		//return format("'%s' -> '%s' @%lld [%lld]", key.toString().c_str(), value.present() ? value.get().toString().c_str() : "<cleared>", version, valueIndex);
		return format("'%s' -> %d bytes @%lld [%lld]", key.printable().c_str(), value.present() ? (int)value.get().size() : -1, version, valueIndex);
	}
};

#define NOT_IMPLEMENTED { UNSTOPPABLE_ASSERT(false); }

class VersionedBTree : public IVersionedStore {
public:
	// The first possible KV pair that could exist in the tree
	static KeyVersionValue beginKey;
	// A KV pair which is beyond the last possible KV pair that could exist in the tree
	static KeyVersionValue endKey;

	virtual Future<Void> getError() NOT_IMPLEMENTED
	virtual Future<Void> onClosed() NOT_IMPLEMENTED
	virtual void dispose() NOT_IMPLEMENTED
	virtual void close() NOT_IMPLEMENTED

	virtual KeyValueStoreType getType() NOT_IMPLEMENTED
	virtual bool supportsMutation(int op) NOT_IMPLEMENTED
	virtual StorageBytes getStorageBytes() NOT_IMPLEMENTED

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
		m_pageSize(pager->getUsablePageSize()),
		m_lastCommittedVersion(invalidVersion),
		m_pBuffer(nullptr),
		m_name(name)
	{
		if(target_page_size > 0 && target_page_size < m_pageSize)
			m_pageSize = target_page_size;
		m_init = init_impl(this);
		m_latestCommit = m_init;
	}

	ACTOR static Future<Void> init_impl(VersionedBTree *self) {
		self->m_root = 0;
		state Version latest = wait(self->m_pager->getLatestVersion());
		if(latest == 0) {
			++latest;
			Reference<IPage> page = self->m_pager->newPageBuffer();
			writeEmptyPage(page, BTreePage::IS_LEAF, self->m_pageSize);
			self->writePage(self->m_root, page, latest);
			self->m_pager->setLatestVersion(latest);
			Void _ = wait(self->m_pager->commit());
		}
		self->m_lastCommittedVersion = latest;
		return Void();
	}

	Future<Void> init() { return m_init; }

	virtual ~VersionedBTree() {
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
		return Reference<IStoreCursor>(new Cursor(v, m_pager, m_root));
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
			(*m_pBuffer)[beginKey.key];
			(*m_pBuffer)[endKey.key];
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
	void writePage(LogicalPageID id, Reference<IPage> page, Version ver) {
		debug_printf("\nWriting page: id=%d ver=%lld\n%s\n", id, ver, ((const BTreePage *)page->begin())->toString().c_str());
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

		KeyVersionValue toKVV(Key key, Version version) const {
			// No point in serializing an atomic op, it needs to be coalesced to a real value.
			ASSERT(!isAtomicOp());

			if(isClear())
				return KeyVersionValue(key, version);

			return KeyVersionValue(key, version, -1, value);
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
	 * - The buffer starts out with keys '' and endKey.key already populated.
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

	void buildNewRoot(Version version, vector<std::pair<int, Reference<IPage>>> &pages, std::vector<LogicalPageID> &logicalPageIDs, PrefixTree::EntriesT &childEntries) {
		// While there are multiple child pages for this version we must write new tree levels.
		while(pages.size() > 1) {
			PrefixTree::EntriesT newChildEntries;
			for(int i=0; i<pages.size(); i++)
				newChildEntries.push_back(KeyValueRef(childEntries[pages[i].first].key, StringRef((unsigned char *)&logicalPageIDs[i], sizeof(uint32_t))));
			childEntries = std::move(newChildEntries);

			int oldPages = pages.size();
			pages = buildPages(childEntries, 0, [=](){ return m_pager->newPageBuffer(); }, m_pageSize);
			// If there isn't a reduction in page count then we'll build new root levels forever.
			ASSERT(pages.size() < oldPages);

			debug_printf("Writing a new root level at version %lld with %lu children across %lu pages\n", version, childEntries.size(), pages.size());

			// Allocate logical page ids for the new level
			logicalPageIDs.clear();

			// Only reuse root if there's one replacement page being written or if the subtree root is not the tree root
			if(pages.size() == 1)
				logicalPageIDs.push_back(m_root);

			// Allocate enough pageIDs for all of the pages
			for(int i=logicalPageIDs.size(); i<pages.size(); i++)
				logicalPageIDs.push_back( m_pager->allocateLogicalPage() );

			for(int i=0; i<pages.size(); i++)
				writePage( logicalPageIDs[i], pages[i].second, version );
		}
	}

	// Returns list of (version, list of (lower_bound, list of children) )
	ACTOR static Future<VersionedChildrenT> commitSubtree(VersionedBTree *self, MutationBufferT *mutationBuffer, Reference<IPagerSnapshot> snapshot, LogicalPageID root, Key lowerBoundKey, Key upperBoundKey) {
		state std::string printPrefix = format("commit subtree root=%lld lower='%s' upper='%s'", root, printable(lowerBoundKey).c_str(), printable(upperBoundKey).c_str());
		debug_printf("%s\n", printPrefix.c_str());

		// Decode the upper and lower bound keys for this subtree
		// Note that these could be truncated to be the shortest usable boundaries between two pages but that
		// still works for what we're using them for here, so instead of using the KVV unpacker we'll have to
		// something else here that supports incomplete keys.
		state KeyVersionValue lowerBoundKVV = KeyVersionValue::unpack(lowerBoundKey);
		state KeyVersionValue upperBoundKVV = KeyVersionValue::unpack(upperBoundKey);

		// Find the slice of the mutation buffer that is relevant to this subtree
		state MutationBufferT::const_iterator iMutationBoundary = mutationBuffer->lower_bound(lowerBoundKVV.key);
		state MutationBufferT::const_iterator iMutationBoundaryEnd = mutationBuffer->lower_bound(upperBoundKVV.key);

		// If the lower bound key and the upper bound key are the same then there can't be any changes to
		// this subtree since changes would happen after the upper bound key as the mutated versions would
		// necessarily be higher.
		if(lowerBoundKVV.key == upperBoundKVV.key) {
			debug_printf("%s no changes, lower and upper bound keys are the same.\n", printPrefix.c_str());
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
				debug_printf("%s no changes, mutation buffer start/end are the same\n", printPrefix.c_str());
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
			debug_printf("%s no changes because sole mutation range was empty\n", printPrefix.c_str());
			return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
		}

		Reference<const IPage> rawPage = wait(snapshot->getPhysicalPage(root));
		state BTreePage *page = (BTreePage *) rawPage->begin();
		debug_printf("Read page: id=%d ver=%lld\n%s\n", root, snapshot->getVersion(), page->toString().c_str());

		PrefixTree::Cursor existingCursor = page->tree.getCursor();  // TODO: pass key boundary leading to this page
		bool existingCursorValid = existingCursor.moveFirst();

		if(page->flags & BTreePage::IS_LEAF) {
			VersionedChildrenT results;
			PrefixTree::EntriesT merged;

			debug_printf("MERGING EXISTING DATA WITH MUTATIONS:\n");
			self->printMutationBuffer(iMutationBoundary, iMutationBoundaryEnd);

			// It's a given that the mutation map is not empty so it's safe to do this
			Key mutationRangeStart = iMutationBoundary->first;

			// There will be multiple loops advancing existing cursor, existing KVV will track its current value
			KeyVersionValue existing;
			KeyVersionValue lastExisting;
			if(existingCursorValid)
				existing = KeyVersionValue::unpack(existingCursor.getKV());
			// If replacement pages are written they will be at the minimum version seen in the mutations for this leaf
			Version minVersion = invalidVersion;

			// Now, process each mutation range and merge changes with existing data.
			while(iMutationBoundary != iMutationBoundaryEnd) {
				debug_printf("%s New mutation boundary: '%s': %s\n", printPrefix.c_str(), printable(iMutationBoundary->first).c_str(), iMutationBoundary->second.toString().c_str());

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
					merged.push_back(existing.pack());
					debug_printf("%s: Added %s [existing, boundary start]\n", printPrefix.c_str(), KeyVersionValue::unpack(merged.back()).toString().c_str());

					existingCursorValid = existingCursor.moveNext();
					if(existingCursorValid)
						existing = KeyVersionValue::unpack(existingCursor.getKV());
				}

				// TODO:  If a mutation set is equal to the previous existing value of the key, don't write it.
				// Output mutations for the mutation boundary start key
				while(iMutations != iMutationsEnd) {
					const SingleKeyMutation &m = iMutations->second;
					//                ( (page_size - map_overhead) / min_kvpairs_per_leaf ) - kvpair_overhead_est - keybytes
					int maxPartSize = ((self->m_pageSize - 1 - 4) / 3) - 21 - iMutationBoundary->first.size();
					ASSERT(maxPartSize > 0);
					if(m.isClear() || m.value.size() <= maxPartSize) {
						if(iMutations->first < minVersion || minVersion == invalidVersion)
							minVersion = iMutations->first;
						merged.push_back(iMutations->second.toKVV(iMutationBoundary->first, iMutations->first).pack());
						debug_printf("%s: Added %s [mutation, boundary start]\n", printPrefix.c_str(), KeyVersionValue::unpack(merged.back()).toString().c_str());
					}
					else {
						int bytesLeft = m.value.size();
						KeyVersionValue kvv(iMutationBoundary->first, iMutations->first);
						kvv.valueIndex = 0;
						while(bytesLeft > 0) {
							int partSize = std::min(bytesLeft, maxPartSize);
							kvv.value = Key(m.value.substr(kvv.valueIndex, partSize), m.value.arena());
							if(iMutations->first < minVersion || minVersion == invalidVersion)
								minVersion = iMutations->first;
							merged.push_back(kvv.pack());
							debug_printf("%s: Added %s [mutation, boundary start]\n", printPrefix.c_str(), KeyVersionValue::unpack(merged.back()).toString().c_str());
							kvv.valueIndex += partSize;
							bytesLeft -= partSize;
						}
					}
					++iMutations;
				}

				// Get the clear version for this range, which is the last thing that we need from it,
				Optional<Version> clearRangeVersion = iMutationBoundary->second.rangeClearVersion;
				// Advance to the next boundary because we need to know the end key for the current range.
				++iMutationBoundary;

				debug_printf("%s Mutation range end: '%s'\n", printPrefix.c_str(), printable(iMutationBoundary->first).c_str());

				// Write existing keys which are less than the next mutation boundary key, clearing if needed.
				while(existingCursorValid && existing.key < iMutationBoundary->first) {
					merged.push_back(existing.pack());
					debug_printf("%s: Added %s [existing, middle]\n", printPrefix.c_str(), KeyVersionValue::unpack(merged.back()).toString().c_str());

					// Write a clear of this key if needed.  A clear is required if clearRangeVersion is set and the next key is different
					// than this one.  Note that the next key might be the in our right sibling, we can use the page upperBound to get that.
					existingCursorValid = existingCursor.moveNext();
					KeyVersionValue nextEntry;
					if(existingCursorValid)
						nextEntry = KeyVersionValue::unpack(existingCursor.getKV());
					else
						nextEntry = upperBoundKVV;

					if(clearRangeVersion.present() && existing.key != nextEntry.key) {
						Version clearVersion = clearRangeVersion.get();
						if(clearVersion < minVersion || minVersion == invalidVersion)
							minVersion = clearVersion;
						merged.push_back(KeyVersionValue(existing.key, clearVersion).pack());
						debug_printf("%s: Added %s [existing, middle clear]\n", printPrefix.c_str(), KeyVersionValue::unpack(merged.back()).toString().c_str());
					}

					if(existingCursorValid)
						existing = nextEntry;
				}
			}

			// Write any remaining existing keys, which are not subject to clears as they are beyond the cleared range.
			while(existingCursorValid) {
				merged.push_back(existing.pack());
				debug_printf("%s: Added %s [existing, tail]\n", printPrefix.c_str(), KeyVersionValue::unpack(merged.back()).toString().c_str());

				existingCursorValid = existingCursor.moveNext();
				if(existingCursorValid)
					existing = KeyVersionValue::unpack(existingCursor.getKV());
			}

			debug_printf("%s Done merging mutations into existing leaf contents\n", printPrefix.c_str());

			// No changes were actually made.  This could happen if there is a clear which does not cover an entire leaf but also does
			// not which turns out to not match any existing data in the leaf.
			if(minVersion == invalidVersion) {
				debug_printf("%s No changes were made during mutation merge\n", printPrefix.c_str());
				return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
			}

			// TODO: Make version and key splits based on contents of merged list

			IPager *pager = self->m_pager;
			vector< std::pair<int, Reference<IPage>> > pages = buildPages( merged, BTreePage::IS_LEAF, [pager](){ return pager->newPageBuffer(); }, self->m_pageSize);

			// If there isn't still just a single page of data then return the previous lower bound and page ID that lead to this page to be used for version 0
			if(pages.size() != 1) {
				results.push_back( {0, {{lowerBoundKey, root}}} );
			}

			// For each IPage of data, assign a logical pageID.
			std::vector<LogicalPageID> logicalPages;

			// Only reuse first page if only one page is being returned or if root is not the btree root.
			if(pages.size() == 1 || root != self->m_root)
				logicalPages.push_back(root);

			// Allocate enough pageIDs for all of the pages
			for(int i=logicalPages.size(); i<pages.size(); i++)
				logicalPages.push_back(self->m_pager->allocateLogicalPage() );

			if(pages.size() == 1)
				minVersion = 0;
			// Write each page using its assigned page ID
			debug_printf("%s Writing %lu replacement pages for %d at version %lld\n", printPrefix.c_str(), pages.size(), root, minVersion);
			for(int i=0; i<pages.size(); i++)
				self->writePage(logicalPages[i], pages[i].second, minVersion);

			// If this commitSubtree() is operating on the root, write new levels if needed until until we're returning a single page
			if(root == self->m_root) {
				debug_printf("%s Building new root\n", printPrefix.c_str());
				self->buildNewRoot(minVersion, pages, logicalPages, merged);
			}

			results.push_back({minVersion, {}});

			for(int i=0; i<pages.size(); i++) {
				// The lower bound of the first page is the lower bound of the subtree, not the first entry in the page
				Key lowerBound = (i == 0) ? lowerBoundKey : merged[pages[i].first].key;
				debug_printf("%s Adding page to results: index=%d lowerBound=%s\n", printPrefix.c_str(), pages[i].first, printable(lowerBound).c_str());
				results.back().second.push_back( {lowerBound, logicalPages[i]} );
			}

			debug_printf("%s DONE.\n", printPrefix.c_str());
			return results;
		}
		else {
			state std::vector<Future<VersionedChildrenT>> futureChildren;
			state std::vector<LogicalPageID> childPageIDs;

			bool first = false;
			while(existingCursorValid) {
				// The lower bound for the first child is lowerBoundKey
				Key childLowerBound = first ? lowerBoundKey : existingCursor.getKey();
				if(first)
					first = false;

				uint32_t pageID = *(uint32_t*)existingCursor.getValue().begin();

				existingCursorValid = existingCursor.moveNext();

				// The upper bound for the last child is upperBoundKey, and entries[i+1] for the others.
				Key childUpperBound = existingCursorValid ? existingCursor.getKey() : upperBoundKey;

				ASSERT(childLowerBound <= childUpperBound);

				futureChildren.push_back(self->commitSubtree(self, mutationBuffer, snapshot, pageID, childLowerBound, childUpperBound));
				childPageIDs.push_back(pageID);
			}

			Void _ = wait(waitForAll(futureChildren));

			bool modified = false;
			for(int i = 0; i < futureChildren.size(); ++i) {
				const VersionedChildrenT &children = futureChildren[i].get();
				if(children.size() != 1 || children[0].second.size() != 1) {
					modified = true;
					break;
				}
			}

			if(!modified) {
				debug_printf("%s not modified.\n", printPrefix.c_str());
				return VersionedChildrenT({{0, {{lowerBoundKey, root}}}});
			}

			Version version = 0;
			VersionedChildrenT result;

			loop { // over version splits of this page
				Version nextVersion = std::numeric_limits<Version>::max();

				PrefixTree::EntriesT childEntries;  // Logically std::vector<std::pair<std::string, LogicalPageID>> childEntries;

				// For each Future<VersionedChildrenT>
				debug_printf("%s creating replacement pages for id=%d at Version %lld\n", printPrefix.c_str(), root, version);

				// If we're writing version 0, there is a chance that we don't have to write ourselves, if there are no changes
				bool modified = version != 0;

				for(int i = 0; i < futureChildren.size(); ++i) {
					LogicalPageID pageID = childPageIDs[i];
					const VersionedChildrenT &children = futureChildren[i].get();

					debug_printf("%s  Versioned page set that replaced page %d: %lu versions\n", printPrefix.c_str(), pageID, children.size());
					for(auto &versionedPageSet : children) {
						debug_printf("%s    version: %lld\n", printPrefix.c_str(), versionedPageSet.first);
						for(auto &boundaryPage : versionedPageSet.second) {
							debug_printf("%s      '%s' -> %u\n", printPrefix.c_str(), printable(boundaryPage.first).c_str(), boundaryPage.second);
						}
					}

					// Find the first version greater than the current version we are writing
					auto cv = std::upper_bound( children.begin(), children.end(), version, [](Version a, VersionedChildrenT::value_type const &b) { return a < b.first; } );

					// If there are no versions before the one we found, just update nextVersion and continue.
					if(cv == children.begin()) {
						debug_printf("%s First version (%lld) in set is greater than current, setting nextVersion and continuing\n", printPrefix.c_str(), cv->first);
						nextVersion = std::min(nextVersion, cv->first);
						debug_printf("%s   curr %lld next %lld\n", printPrefix.c_str(), version, nextVersion);
						continue;
					}

					// If a version greater than the current version being written was found, update nextVersion
					if(cv != children.end()) {
						nextVersion = std::min(nextVersion, cv->first);
						debug_printf("%s   curr %lld next %lld\n", printPrefix.c_str(), version, nextVersion);
					}

					// Go back one to the last version that was valid prior to or at the current version we are writing
					--cv;

					debug_printf("%s   Using children for version %lld from this set, building version %lld\n", printPrefix.c_str(), cv->first, version);

					// If page count isn't 1 then the root is definitely modified
					modified = modified || cv->second.size() != 1;

					// Add the children at this version to the child entries list for the current version being built.
					for (auto &childPage : cv->second) {
						debug_printf("%s  Adding child page '%s'\n", printPrefix.c_str(), printable(childPage.first).c_str());
						childEntries.push_back( KeyValueRef(childPage.first, StringRef((unsigned char *)&childPage.second, sizeof(uint32_t))));
					}
				}

				debug_printf("%s Finished pass through futurechildren.  childEntries=%lu  version=%lld  nextVersion=%lld\n", printPrefix.c_str(), childEntries.size(), version, nextVersion);

				if(modified) {
					// TODO: Track split points across iterations of this loop, so that they don't shift unnecessarily and
					// cause unnecessary path copying

					IPager *pager = self->m_pager;
					vector< std::pair<int, Reference<IPage>> > pages = buildPages( childEntries, 0, [pager](){ return pager->newPageBuffer(); }, self->m_pageSize);

					// For each IPage of data, assign a logical pageID.
					std::vector<LogicalPageID> logicalPages;

					// Only reuse first page if only one page is being returned or if root is not the btree root.
					if(pages.size() == 1 || root != self->m_root)
						logicalPages.push_back(root);

					// Allocate enough pageIDs for all of the pages
					for(int i=logicalPages.size(); i<pages.size(); i++)
						logicalPages.push_back( self->m_pager->allocateLogicalPage() );

					// Write each page using its assigned page ID
					debug_printf("%s Writing %lu internal pages\n", printPrefix.c_str(), pages.size());
					for(int i=0; i<pages.size(); i++)
						self->writePage( logicalPages[i], pages[i].second, version );

					// If this commitSubtree() is operating on the root, write new levels if needed until until we're returning a single page
					if(root == self->m_root)
						self->buildNewRoot(version, pages, logicalPages, childEntries);

					result.resize(result.size()+1);
					result.back().first = version;

					for(int i=0; i<pages.size(); i++)
						result.back().second.push_back( {childEntries[pages[i].first].key, logicalPages[i]} );

					if (result.size() > 1 && result.back().second == result.end()[-2].second) {
						debug_printf("%s Output same as last version, popping it.\n", printPrefix.c_str());
						result.pop_back();
					}
				}
				else {
					debug_printf("%s Version 0 has no changes\n", printPrefix.c_str());
					result.push_back({0, {{lowerBoundKey, root}}});
				}

				if (nextVersion == std::numeric_limits<Version>::max())
					break;
				version = nextVersion;
			}

			debug_printf("%s DONE.\n", printPrefix.c_str());
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
		Void _ = wait(previousCommit);
		debug_printf("%s: Beginning commit of version %lld\n", self->m_name.c_str(), writeVersion);

		// Get the latest version from the pager, which is what we will read at
		Version latestVersion = wait(self->m_pager->getLatestVersion());
		debug_printf("%s: pager latestVersion %lld\n", self->m_name.c_str(), latestVersion);

		self->printMutationBuffer(mutations);

		VersionedChildrenT _ = wait(commitSubtree(self, mutations, self->m_pager->getReadSnapshot(latestVersion), self->m_root, beginKey.pack().key, endKey.pack().key));

		self->m_pager->setLatestVersion(writeVersion);
		debug_printf("%s: Committing pager %lld\n", self->m_name.c_str(), writeVersion);
		Void _ = wait(self->m_pager->commit());
		debug_printf("%s: Committed version %lld\n", self->m_name.c_str(), writeVersion);

		// Now that everything is committed we must delete the mutation buffer.
		// Our buffer's start version should be the oldest mutation buffer version in the map.
		ASSERT(mutationBufferStartVersion == self->m_mutationBuffers.begin()->first);
		self->m_mutationBuffers.erase(self->m_mutationBuffers.begin());

		self->m_lastCommittedVersion = writeVersion;
		committed.send(Void());

		return Void();
	}

	IPager *m_pager;
	MutationBufferT *m_pBuffer;
	std::map<Version, MutationBufferT> m_mutationBuffers;

	Version m_writeVersion;
	Version m_lastCommittedVersion;
	Future<Void> m_latestCommit;
	int m_pageSize;
	Future<Void> m_init;
	std::string m_name;

	// InternalCursor is for seeking to and iterating over the internal / low level records in the Btree.
	// This records are versioned and they can represent deletions or partial values so they must be
	// post processed to obtain keys returnable to the user.
	class InternalCursor {
	public:
		InternalCursor() {}
		InternalCursor(Reference<IPagerSnapshot> pages, LogicalPageID root) : m_pages(pages), m_root(root), outOfBound(0) {
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

		KeyVersionValue kvv;  // The decoded current internal record in the tree

		std::string toString(const char *wrapPrefix = "") const {
			std::string r;
			r += format("InternalCursor(%p) ver=%lld oob=%d valid=%d", this, m_pages->getVersion(), outOfBound, valid());
			r += format("\n%s  KVV: %s", wrapPrefix, kvv.toString().c_str());
			for(const PageEntryLocation &p : m_path) {
				std::string cur = p.cursor.valid() ? format("'%s' -> '%s'", p.cursor.getKey().toHexString().c_str(), p.cursor.getValue().toHexString().c_str()) : "invalid";
				r += format("\n%s  Page %d (%d records, %d bytes)  Cursor %s", wrapPrefix, p.pageNumber, p.btPage->count, p.btPage->kvBytes, cur.c_str());
			}
			return r;
		}

	private:
		Reference<IPagerSnapshot> m_pages;
		LogicalPageID m_root;

		struct PageEntryLocation {
			PageEntryLocation() {}
			PageEntryLocation(Reference<const IPage> page, LogicalPageID id, int index = -1)
				: page(page), pageNumber(id), btPage((BTreePage *)page->begin()), cursor(btPage->tree.getCursor()) {}

			Reference<const IPage> page;
			BTreePage *btPage;
			LogicalPageID pageNumber;    // This is really just for debugging
			PrefixTree::Cursor cursor;
		};

		typedef std::vector<PageEntryLocation> TraversalPathT;
		TraversalPathT m_path;
		int outOfBound;

		ACTOR static Future<Void> pushPage(InternalCursor *self, LogicalPageID id) {
			Reference<const IPage> rawPage = wait(self->m_pages->getPhysicalPage(id));
			debug_printf("Read page: id=%d ver=%lld\n%s\n", id, self->m_pages->getVersion(), ((const BTreePage *)rawPage->begin())->toString().c_str());
			self->m_path.emplace_back(rawPage, id);
			return Void();
		}

		ACTOR static Future<Void> reset(InternalCursor *self) {
			if(self->m_path.empty()) {
				Void _ = wait(pushPage(self, self->m_root));
			}
			else {
				self->m_path.resize(1);
			}
			self->outOfBound = 0;
			return Void();
		}

		ACTOR static Future<Void> seekLessThanOrEqual_impl(InternalCursor *self, KeyRef key) {
			state TraversalPathT &path = self->m_path;
			Void _ = wait(reset(self));

			debug_printf("InternalCursor::seekLTE(%s): start  %s\n", KeyVersionValue::unpack(key).toString().c_str(), self->toString("  ").c_str());

			loop {
				state PageEntryLocation *p = &path.back();

				if(p->btPage->count == 0) {
					ASSERT(path.size() == 1);  // This must be the root page.
					self->outOfBound = -1;
					self->kvv.version = invalidVersion;
					debug_printf("InternalCursor::seekLTE(%s): Exit, root page empty.  %s\n", KeyVersionValue::unpack(key).toString().c_str(), self->toString("  ").c_str());
					return Void();
				}

				state bool foundLTE = p->cursor.seekLessThanOrEqual(key);
				debug_printf("InternalCursor::seekLTE(%s): Seek on path tail, result %d.  %s\n", KeyVersionValue::unpack(key).toString().c_str(), foundLTE, self->toString("  ").c_str());
				
				if(p->btPage->flags & BTreePage::IS_LEAF) {
					// It is possible for the current leaf key to be between the page's lower bound (in the parent page) and the 
					// first record in the leaf page, which means we must move backwards 1 step in the database to find the
					// record < key, if such a record exists.
					if(!foundLTE) {
						Void _ = wait(self->move(false));
					}
					else {
						// Found the target record
						self->kvv = KeyVersionValue::unpack(path.back().cursor.getKV());
					}
					debug_printf("InternalCursor::seekLTE(%s): Exit, Found leaf page. %s\n", KeyVersionValue::unpack(key).toString().c_str(), self->toString("  ").c_str());
					return Void();
				}
				else {
					// We don't have to check foundLTE here because if it's false then cursor will be at the first record in the page.
					// TODO:  It would, however, be more efficient to check foundLTE and if false move to the previous sibling page.
					// But the page should NOT be empty so let's assert that the cursor is valid.
					ASSERT(p->cursor.valid());
					LogicalPageID newPage = (LogicalPageID)*(uint32_t *)path.back().cursor.getValue().begin();
					debug_printf("InternalCursor::seekLTE(%s): Found internal page, going to page %d.  %s\n", 
						KeyVersionValue::unpack(key).toString().c_str(), newPage, self->toString("  ").c_str());
					Void _ = wait(pushPage(self, newPage));
				}
			}
		}

		// Move one 'internal' key/value/version/valueindex/value record.
		// Iterating with this function will "see" all parts of all values and clears at all versions.
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
						self->kvv = KeyVersionValue::unpack(path.back().cursor.getKV());
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
					break;
				} else {
					debug_printf("InternalCursor::move(%s) Move failed on path index %d\n", dir, i);
				}
			}
			debug_printf("InternalCursor::move(%s) Finished ascending  %s\n", dir, self->toString("  ").c_str());

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

			// We were able to advance the cursor on one of the pages in the page traversal path, so 
			// resize the path to stop at that page.
			path.resize(i + 1);
			state PageEntryLocation *p = &(path.back());

			debug_printf("InternalCursor::move(%s): Descending if needed to find a leaf\n", dir);

			// Now we must traverse downward if needed until we are at a leaf level.
			// Each movement down will start on the far left or far right depending on fwd
			while(!(p->btPage->flags & BTreePage::IS_LEAF)) {
				// Get the page that the path's last entry points to
				Void _ = wait(pushPage(self, (LogicalPageID)*(uint32_t *)p->cursor.getValue().begin()));
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
			self->kvv = KeyVersionValue::unpack(p->cursor.getKV());

			debug_printf("InternalCursor::move(%s) Exiting  %s\n", dir, self->toString("  ").c_str());
			return Void();
		}
	};

	// Cursor is for reading and interating over user visible KV pairs at a specific version
	class Cursor : public IStoreCursor, public ReferenceCounted<Cursor> {
	public:
		Cursor(Version version, IPager *pager, LogicalPageID root)
		  : m_version(version), m_pagerSnapshot(pager->getReadSnapshot(version)), m_icursor(m_pagerSnapshot, root) {
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

		virtual void invalidateReturnedStrings() {
			m_pagerSnapshot->invalidateReturnedPages();
		}

		void addref() { ReferenceCounted<Cursor>::addref(); }
		void delref() { ReferenceCounted<Cursor>::delref(); }

		std::string toString(const char *wrapPrefix = "") const {
			std::string r;
			r += format("Cursor(%p) ver: %lld key: %s value: %s\n", this, m_version,
				(m_kv.present() ? m_kv.get().key.printable().c_str() : "<np>"),
				(m_kv.present() ? m_kv.get().value.printable().c_str() : ""));
			r += format("%s  InternalCursor: %s\n", wrapPrefix, m_icursor.toString(format("%s    ", wrapPrefix).c_str()).c_str());
			return r;
		}

	private:
		Version m_version;
		Reference<IPagerSnapshot> m_pagerSnapshot;
		InternalCursor m_icursor;
		Optional<KeyValueRef> m_kv; // The current user-level key/value in the tree
		Arena m_arena;

		// find key in tree closest to or equal to key.
		// for less than or equal use cmp < 0
		// for greater than or equal use cmp > 0
		// for equal use cmp == 0
		ACTOR static Future<Void> find_impl(Reference<Cursor> self, KeyRef key, bool needValue, int cmp) {
			state InternalCursor &icur = self->m_icursor;

			// Search for the last key at or before (key, version, max_value_index)
			state KeyVersionValue target(key, self->m_version, std::numeric_limits<int>::max());
			state Key record = target.pack().key;
			self->m_kv = Optional<KeyValueRef>();

			Void _ = wait(icur.seekLessThanOrEqual(record));
			debug_printf("find%sE('%s'): %s\n", cmp > 0 ? "GT" : (cmp == 0 ? "" : "LT"), target.toString().c_str(), icur.toString().c_str());

			// If we found the target key, return it as it is valid for any cmp option
			if(icur.valid() && icur.kvv.value.present() && icur.kvv.key == key) {
				debug_printf("Reading full kv pair starting from: %s\n", icur.kvv.toString().c_str());
				Void _ = wait(self->readFullKVPair(self));
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
					Void _ = wait(icur.move(true));
					if(!icur.valid() || icur.kvv.key > key)
						break;
				}
				// Get the next present key at the target version.  Handles invalid cursor too.
				Void _ = wait(self->next(needValue));
			}
			else if(cmp < 0) {
				// Move to previous present kv pair at the target version
				Void _ = wait(self->prev(needValue));
			}

			return Void();
		}

		ACTOR static Future<Void> next_impl(Reference<Cursor> self, bool needValue) {
			// TODO: use needValue
			state InternalCursor &i = self->m_icursor;

			debug_printf("next(): cursor %s\n", i.toString().c_str());

			// Make sure we are one record past the last user key
			if(self->m_kv.present()) {
				while(i.valid() && i.kvv.key <= self->m_kv.get().key) {
					Void _ = wait(i.move(true));
				}
			}

			state Version v = self->m_pagerSnapshot->getVersion();
			state InternalCursor iLast;
			while(1) {
				iLast = i;
				if(!i.valid())
					break;
				Void _ = wait(i.move(true));
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
					Void _ = wait(readFullKVPair(self));
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

			debug_printf("prev(): cursor %s\n", i.toString().c_str());

			// Make sure we are one record before the last user key
			if(self->m_kv.present()) {
				while(i.valid() && i.kvv.key >= self->m_kv.get().key) {
					Void _ = wait(i.move(false));
				}
			}

			state Version v = self->m_pagerSnapshot->getVersion();
			while(i.valid()) {
				// Once we reach a present value at or before v, return or skip it.
				if(i.kvv.version <= v) {
					// If it's present, return it
					if(i.kvv.value.present()) {
						Void _ = wait(readFullKVPair(self));
						return Void();
					}
					// Value wasn't present as of the latest version <= v, so move backward to a new key
					state Key clearedKey = i.kvv.key;
					while(1) {
						Void _ = wait(i.move(false));
						if(!i.valid() || i.kvv.key != clearedKey)
							break;
					}
				}
				else {
					Void _ = wait(i.move(false));
				}
			}

			self->m_kv = Optional<KeyValueRef>();
			return Void();
		}

		// Read all of the current value, if it is split across multiple kv pairs, and set m_kv.
		// m_current must be at either the first or the last value part.
		ACTOR static Future<Void> readFullKVPair(Reference<Cursor> self) {
			state KeyVersionValue &kvv = self->m_icursor.kvv;
			// Initialize the optional kv to a KeyValueRef and get a reference to it.
			state KeyValueRef &kv = (self->m_kv = KeyValueRef()).get();
			kv.key = KeyRef(self->m_arena, kvv.key);
			debug_printf("readFullKVPair:  starting from %s\n", kvv.toString().c_str());
			state Version valueVersion = kvv.version;

			// Unsplit value
			if(kvv.valueIndex == -1) {
				ASSERT(kvv.value.present());
				kv.value = ValueRef(self->m_arena, kvv.value.get());
				return Void();
			}

			// If we find a 0 valueindex then we're at the first segment and must iterate forward
			if(kvv.valueIndex == 0) {
				// TODO:  Performance, lots of extra copying is happening.
				state std::vector<Value> parts;
				state int size = 0;
				while(1) {
					ASSERT(kvv.value.present());
					parts.push_back(kvv.value.get());
					size += kvv.value.get().size();
					Void _ = wait(self->m_icursor.move(true));
					if(!kvv.valid() || kvv.version != valueVersion || kvv.key != kv.key)
						break;
				}
				kv.value = makeString(size, self->m_arena);
				uint8_t *wptr = mutateString(kv.value);
				for(const Value &part : parts) {
					memcpy(wptr, part.begin(), part.size());
					wptr += part.size();
				}
				return Void();
			}

			// If we find any other valueIndex we must assume we're at the last segment and must iterate backward.
			ASSERT(kvv.value.present());
			kv.value = makeString(kvv.valueIndex + kvv.value.get().size(), self->m_arena);

			while(1) {
				ASSERT(kvv.value.present());
				ASSERT(kvv.valueIndex >= 0);
				ASSERT(kvv.valueIndex + kvv.value.get().size() <= kv.value.size());
				memcpy(mutateString(kv.value) + kvv.valueIndex, kvv.value.get().begin(), kvv.value.get().size());
				if(kvv.valueIndex == 0)
					break;
				Void _ = wait(self->m_icursor.move(false));
			}

			return Void();
		}
	};
};

KeyVersionValue VersionedBTree::beginKey(StringRef(), 0);
KeyVersionValue VersionedBTree::endKey(LiteralStringRef("\xff\xff\xff\xff"), 0);

ACTOR template<class T>
Future<T> catchError(Promise<Void> error, Future<T> f) {
	try {
		T result = wait(f);
		return result;
	} catch(Error &e) {
		if(error.canBeSet())
			error.sendError(e);
		throw;
	}
}

class KeyValueStoreRedwoodUnversioned : public IKeyValueStore {
public:
	KeyValueStoreRedwoodUnversioned(std::string filePrefix, UID logID) : m_filePrefix(filePrefix) {
		m_pager = new IndirectShadowPager(filePrefix);
		m_tree = new VersionedBTree(m_pager, filePrefix, m_pager->getUsablePageSize());
		m_init = catchError(m_error, init_impl(this));
	}

	virtual Future<Void> init() {
		return m_init;
	}

	ACTOR Future<Void> init_impl(KeyValueStoreRedwoodUnversioned *self) {
		Void _ = wait(self->m_tree->init());
		Version v = wait(self->m_tree->getLatestVersion());
		self->m_tree->setWriteVersion(v + 1);
		return Void();
	}

	ACTOR void shutdown(KeyValueStoreRedwoodUnversioned *self, bool dispose) {
		self->m_init.cancel();
		delete self->m_tree;
		Future<Void> closedFuture = self->m_pager->onClosed();
		if(dispose)
			self->m_pager->dispose();
		else
			self->m_pager->close();
		Void _ = wait(closedFuture);
		self->m_closed.send(Void());
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
		return catchError(m_error, c);
	}

	virtual KeyValueStoreType getType() {
		return KeyValueStoreType::SSD_REDWOOD_V1;
	}

	virtual StorageBytes getStorageBytes() {
		return m_pager->getStorageBytes();
	}

	virtual Future< Void > getError() { return m_error.getFuture(); };

	void clear(KeyRangeRef range, const Arena* arena = 0) {
		m_tree->clear(range);
	}

    virtual void set( KeyValueRef keyValue, const Arena* arena = NULL ) {
		//printf("SET write version %lld %s\n", m_tree->getWriteVersion(), printable(keyValue).c_str());
		m_tree->set(keyValue);
	}

	ACTOR static Future< Standalone< VectorRef< KeyValueRef > > > readRange_impl(KeyValueStoreRedwoodUnversioned *self, KeyRangeRef keys, int rowLimit, int byteLimit) {
		Void _ = wait(self->m_init);
		state Standalone<VectorRef<KeyValueRef>> result;
		state int accumulatedBytes = 0;
		ASSERT( byteLimit > 0 );

		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());

		state Version readVersion = self->m_tree->getLastCommittedVersion();
		if(rowLimit >= 0) {
			Void _ = wait(cur->findFirstEqualOrGreater(keys.begin, true, 0));
			while(cur->isValid() && cur->getKey() < keys.end) {
				KeyValueRef kv(KeyRef(result.arena(), cur->getKey()), ValueRef(result.arena(), cur->getValue()));
				accumulatedBytes += kv.expectedSize();
				result.push_back(result.arena(), kv);
				if(--rowLimit == 0 || accumulatedBytes >= byteLimit)
					break;
				Void _ = wait(cur->next(true));
			}
		} else {
			Void _ = wait(cur->findLastLessOrEqual(keys.end, true, 0));
			if(cur->isValid() && cur->getKey() == keys.end)
				Void _ = wait(cur->prev(true));

			while(cur->isValid() && cur->getKey() >= keys.begin) {
				KeyValueRef kv(KeyRef(result.arena(), cur->getKey()), ValueRef(result.arena(), cur->getValue()));
				accumulatedBytes += kv.expectedSize();
				result.push_back(result.arena(), kv);
				if(--rowLimit == 0 || accumulatedBytes >= byteLimit)
					break;
				Void _ = wait(cur->prev(true));
			}
		}
		return result;
	}

	virtual Future< Standalone< VectorRef< KeyValueRef > > > readRange(KeyRangeRef keys, int rowLimit = 1<<30, int byteLimit = 1<<30) {
		return catchError(m_error, readRange_impl(this, keys, rowLimit, byteLimit));
	}

	ACTOR static Future< Optional<Value> > readValue_impl(KeyValueStoreRedwoodUnversioned *self, KeyRef key, Optional< UID > debugID) {
		Void _ = wait(self->m_init);
		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());
		state Version readVersion = self->m_tree->getLastCommittedVersion();

		Void _ = wait(cur->findEqual(key));
		if(cur->isValid()) {
			return cur->getValue();
		}
		return Optional<Value>();
	}

	virtual Future< Optional< Value > > readValue(KeyRef key, Optional< UID > debugID = Optional<UID>()) {
		return catchError(m_error, readValue_impl(this, key, debugID));
	}

	ACTOR static Future< Optional<Value> > readValuePrefix_impl(KeyValueStoreRedwoodUnversioned *self, KeyRef key, int maxLength, Optional< UID > debugID) {
		Void _ = wait(self->m_init);
		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());

		Void _ = wait(cur->findEqual(key));
		if(cur->isValid()) {
			Value v = cur->getValue();
			int len = std::min(v.size(), maxLength);
			return Value(cur->getValue().substr(0, len));
		}
		return Optional<Value>();
	}

	virtual Future< Optional< Value > > readValuePrefix(KeyRef key, int maxLength, Optional< UID > debugID = Optional<UID>()) {
		return catchError(m_error, readValuePrefix_impl(this, key, maxLength, debugID));
	}

	virtual ~KeyValueStoreRedwoodUnversioned() {
	};

private:
	std::string m_filePrefix;
	IPager *m_pager;
	VersionedBTree *m_tree;
	Future<Void> m_init;
	Promise<Void> m_closed;
	Promise<Void> m_error;
};

IKeyValueStore* keyValueStoreRedwoodV1( std::string const& filename, UID logID) {
	return new KeyValueStoreRedwoodUnversioned(filename, logID);
}


KeyValue randomKV(int keySize = 10, int valueSize = 5) {
	int kLen = g_random->randomInt(1, keySize);
	int vLen = g_random->randomInt(0, valueSize);
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
		state Key randomKey = randomKV().key;
		Void _ = wait(g_random->coinflip() ? cur->findFirstEqualOrGreater(randomKey, true, 0) : cur->findLastLessOrEqual(randomKey, true, 0));
	}
	Void _ = wait(cur->findFirstEqualOrGreater(start, true, 0));

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
		Void _ = wait(cur->next(true));
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
	Void _ = wait(cur->findLastLessOrEqual(end, true, 0));
	if(cur->isValid() && cur->getKey() == end)
		Void _ = wait(cur->prev(true));

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
		Void _ = wait(cur->prev(true));
	}

	if(r != results.rend()) {
		errors += 1;
		printf("VerifyRangeReverse(@%lld, %s, %s) ERROR: Tree range ended but written has '%s'\n", v, start.toString().c_str(), end.toString().c_str(), r->key.toString().c_str());
	}

	if(errors > 0)
		throw internal_error();

	return errors;
}

TEST_CASE("/redwood/correctness") {
	state bool useDisk = true;

	state std::string pagerFile = "testPager";
	state IPager *pager;
	if(useDisk)
		pager = new IndirectShadowPager(pagerFile);
	else
		pager = createMemoryPager();

	state int pageSize = g_random->coinflip() ? pager->getUsablePageSize() : g_random->randomInt(200, 400);
	state VersionedBTree *btree = new VersionedBTree(pager, pagerFile, pageSize);
	Void _ = wait(btree->init());

	state int maxCommits = 10;
	state int maxVersionsPerCommit = 4;
	state int maxChangesPerVersion = 5;

	// We must be able to fit at least two any two keys plus overhead in a page to prevent
	// a situation where the tree cannot be grown upward with decreasing level size.
	// TODO:  Handle arbitrarily large keys
	state int maxKeySize = 5; //(pageSize / 4) - 40;
	ASSERT(maxKeySize > 0);
	state int maxValueSize = 5; //pageSize * 3;

	printf("Using page size %d, max key size %d, max value size %d\n", pageSize, maxKeySize, maxValueSize);

	state std::map<std::pair<std::string, Version>, Optional<std::string>> written;
	state std::set<Key> keys;

	state Version lastVer = wait(btree->getLatestVersion());
	printf("Starting from version: %lld\n", lastVer);

	state Version version = lastVer + 1;
	state int commits = g_random->randomInt(1, maxCommits);
	//printf("Will do %d commits\n", commits);
	state double insertTime = 0;
	state int64_t keyBytesInserted = 0;
	state int64_t ValueBytesInserted = 0;

	while(commits--) {
		state double startTime = now();
		int versions = g_random->randomInt(1, maxVersionsPerCommit);
		debug_printf("  Commit will have %d versions\n", versions);
		while(versions--) {
			++version;
			btree->setWriteVersion(version);
			int changes = g_random->randomInt(0, maxChangesPerVersion);
			debug_printf("    Version %lld will have %d changes\n", version, changes);
			while(changes--) {
				if(g_random->random01() < .10) {
					// Delete a random range
					Key start = randomKV().key;
					Key end = randomKV().key;
					if(end <= start)
						end = keyAfter(start);
					KeyRangeRef range(start, end);
					debug_printf("      Clear '%s' to '%s' @%lld\n", start.toString().c_str(), end.toString().c_str(), version);
					auto w = keys.lower_bound(start);
					auto wEnd = keys.lower_bound(end);
					while(w != wEnd) {
						debug_printf("   Clearing key '%s' @%lld\n", w->toString().c_str(), version);
						written[std::make_pair(w->toString(), version)] = Optional<std::string>();
						++w;
					}
					btree->clear(range);
				}
				else {
					KeyValue kv = randomKV(maxKeySize, maxValueSize);
					keyBytesInserted += kv.key.size();
					ValueBytesInserted += kv.value.size();
					debug_printf("      Set '%s' -> '%s' @%lld\n", kv.key.toString().c_str(), kv.value.toString().c_str(), version);
					btree->set(kv);
					written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
					keys.insert(kv.key);
				}
			}
		}
		Void _ = wait(btree->commit());

		// Check that all writes can be read at their written versions
		state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i = written.cbegin();
		state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd = written.cend();
		state int errors = 0;

		insertTime += now() - startTime;
		printf("Checking changes committed thus far.\n");
		if(useDisk && g_random->random01() < .1) {
			printf("Reopening disk btree\n");
			delete btree;
			Future<Void> closedFuture = pager->onClosed();
			pager->close();
			Void _ = wait(closedFuture);

			pager = new IndirectShadowPager(pagerFile);

			btree = new VersionedBTree(pager, pagerFile, pageSize);
			Void _ = wait(btree->init());

			Version v = wait(btree->getLatestVersion());
			ASSERT(v == version);
		}

		// Read back every key at every version set or cleared and verify the result.
		while(i != iEnd) {
			state std::string key = i->first.first;
			state Version ver = i->first.second;
			state Optional<std::string> val = i->second;

			state Reference<IStoreCursor> cur = btree->readAtVersion(ver);

			//debug_printf("Verifying @%lld '%s'\n", ver, key.c_str());
			Void _ = wait(cur->findEqual(key));

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
			++i;
		}

		// For every version written thus far, range read a random range and verify the results.
		state Version iVersion = lastVer;
		while(iVersion < version) {
			int e = wait(verifyRandomRange(btree, iVersion, &written));
			errors += e;
			++iVersion;
		}

		printf("%d sets, %d errors\n", (int)written.size(), errors);

		if(errors != 0)
			throw internal_error();
		printf("Inserted %lld bytes (%lld key, %lld value) in %f seconds.\n", keyBytesInserted + ValueBytesInserted, keyBytesInserted, ValueBytesInserted, insertTime);

	}
	printf("Inserted %lld bytes (%lld key, %lld value) in %f seconds.\n", keyBytesInserted + ValueBytesInserted, keyBytesInserted, ValueBytesInserted, insertTime);

	Future<Void> closedFuture = pager->onClosed();
	pager->close();
	Void _ = wait(closedFuture);

	return Void();
}

TEST_CASE("/redwood/performance/set") {
	state IPager *pager = new IndirectShadowPager("pagerfile");
	state VersionedBTree *btree = new VersionedBTree(pager, "pagerfile");
	Void _ = wait(btree->init());

	state int nodeCount = 100000;
	state int maxChangesPerVersion = 5;
	state int versions = 1000;
	int maxKeySize = 100;
	int maxValueSize = 500;

	state std::string key(maxKeySize, 'k');
	state std::string value(maxKeySize, 'v');

	state double startTime = now();
	while(--versions) {
		Version lastVer = wait(btree->getLatestVersion());
		state Version version = lastVer + 1;
		printf("Writing version %lld\n", version);
		btree->setWriteVersion(version);
		int changes = g_random->randomInt(0, maxChangesPerVersion);
		while(changes--) {
			KeyValue kv;
			// Change first 4 bytes of key to an int
			*(uint32_t *)key.data() = g_random->randomInt(0, nodeCount);
			kv.key = StringRef((uint8_t *)key.data(), g_random->randomInt(10, key.size()));
			kv.value = StringRef((uint8_t *)value.data(), g_random->randomInt(0, value.size()));
			btree->set(kv);
		}

		if(g_random->random01() < .01) {
			printf("Committing %lld\n", version);
			Void _ = wait(btree->commit());
		}
	}

	Void _ = wait(btree->commit());

	Future<Void> closedFuture = pager->onClosed();
	pager->close();
	Void _ = wait(closedFuture);

	return Void();
}
