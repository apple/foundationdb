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

#ifdef REDWOOD_DEBUG
  #define debug_printf(args...) fprintf(stdout, args)
#else
  #define debug_printf(...)
#endif

struct SimpleFixedSizeMapRef {
	typedef std::vector<KeyValue> KVPairsT;

	SimpleFixedSizeMapRef() : flags(0) {}

	static SimpleFixedSizeMapRef decode(StringRef buf) {
		SimpleFixedSizeMapRef result;
		BinaryReader r(buf, AssumeVersion(currentProtocolVersion));
		r >> result.flags;
		r >> result.entries;
		return result;
	};

	// Returns -1 if key is less than first key, otherwise index into entries
	int findLastLessOrEqual(StringRef key) {
		return std::upper_bound(entries.begin(), entries.end(),
								key,
								[](StringRef const& a, KVPairsT::value_type const& b) { return a < b.key; })
					- entries.begin() - 1;
	}

	template<typename Allocator>
	static Reference<IPage> emptyPage(uint8_t newFlags, Allocator const &newPageFn) {
		Reference<IPage> page = newPageFn();
		BinaryWriter bw(AssumeVersion(currentProtocolVersion));
		bw << newFlags;
		bw << KVPairsT();
		memcpy(page->mutate(), bw.getData(), bw.getLength());
		return page;
	}

	template<typename Allocator>
	static vector<std::pair<int, Reference<IPage>>> buildMany(const KVPairsT &kvPairs, uint8_t newFlags, Allocator const &newPageFn, int page_size_override = -1) {
		vector<std::pair<int, Reference<IPage>>> pages;

		Reference<IPage> page = newPageFn();
		int pageSize = page->size();
		if(page_size_override > 0 && page_size_override < pageSize)
			pageSize = page_size_override;
		BinaryWriter bw(AssumeVersion(currentProtocolVersion));
		bw << newFlags;
		uint32_t i = 0;
		uint32_t start = i;
		int mapSizeOffset = bw.getLength();
		bw << start;  // placeholder for map size

		for(auto const &kv : kvPairs) {
			// If page would overflow, output it and start new one
			if(bw.getLength() + 8 + kv.key.size() + kv.value.size() > pageSize) {
				// Page so far can't be empty, this means a single kv pair is too big for a page.
				ASSERT(bw.getLength() != sizeof(newFlags));
				memcpy(page->mutate(), bw.getData(), bw.getLength());
				*(uint32_t *)(page->mutate() + mapSizeOffset) = i - start;
				//debug_printf("buildmany: writing page start=%d %s\n", start, kvPairs[start].first.c_str());
				pages.push_back({start, page});
				bw = BinaryWriter(AssumeVersion(currentProtocolVersion));
				bw << newFlags;
				page = newPageFn();
				start = i;
				int mapSizeOffset = bw.getLength();
				bw << start; // placeholder for map size;
			}

			bw << kv;
			++i;
		}

		if(bw.getLength() != sizeof(newFlags)) {
			//debug_printf("buildmany: adding last page start=%d %s\n", start, kvPairs[start].first.c_str());
			memcpy(page->mutate(), bw.getData(), bw.getLength());
			*(uint32_t *)(page->mutate() + mapSizeOffset) = i - start;
			pages.push_back({start, page});
		}

		//debug_printf("buildmany: returning pages.size %lu, kvpairs %lu\n", pages.size(), kvPairs.size());
		return pages;
	}

	std::string toString() const;

	KVPairsT entries;
	uint8_t flags;
};

struct KeyVersionValue {
	KeyVersionValue() : version(invalidVersion) {}
	KeyVersionValue(Key k, Version ver, Optional<Value> val = Optional<Value>()) : key(k), version(ver), value(val) {}
	bool operator< (KeyVersionValue const &rhs) const {
		int64_t cmp = key.compare(rhs.key);
		if(cmp == 0) {
			cmp = version - rhs.version;
			if(cmp == 0)
				return false;
		}
		return cmp < 0;
	}
	Key key;
	Version version;
	Optional<Value> value;

	bool valid() const { return version != invalidVersion; }

	inline KeyValue pack() const {
		Tuple k;
		k.append(key);
		k.append(version);
		Tuple v;
		if(value.present())
			v.append(value.get());
		else
			v.appendNull();
		return KeyValueRef(k.pack(), v.pack());
	}
	static inline KeyVersionValue unpack(KeyValueRef kv) {
		Tuple k = Tuple::unpack(kv.key);
		if(kv.value.size() == 0)
			return KeyVersionValue(k.getString(0), k.getInt(1));
		Tuple v = Tuple::unpack(kv.value);
		return KeyVersionValue(k.getString(0), k.getInt(1), v.getType(0) == Tuple::NULL_TYPE ? Optional<Value>() : v.getString(0));
	}

	static inline KeyVersionValue unpack(KeyRef key) {
		if(key.size() == 0)
			return KeyVersionValue(KeyRef(), 0);
		return unpack(KeyValueRef(key, ValueRef()));
	}

	std::string toString() const {
		return format("'%s' -> '%s' @%lld", key.toString().c_str(), value.present() ? value.get().toString().c_str() : "<cleared>", version);
	}
};

#define NOT_IMPLEMENTED { UNSTOPPABLE_ASSERT(false); }

class VersionedBTree : public IVersionedStore {
public:
	enum EPageFlags { IS_LEAF = 1};

	// The end key for the entire tree
	static KeyVersionValue endKey;

	typedef SimpleFixedSizeMapRef FixedSizeMap;

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
		ASSERT(m_writeVersion != invalidVersion);

		SingleKeyMutationsByVersion &changes = insertMutationBoundary(keyValue.key)->second.startKeyMutations;

		// Add the set if the changes set is empty or the last entry isn't a set to exactly the same value
		if(changes.empty() || !changes.rbegin()->second.equalToSet(keyValue.value)) {
			changes[m_writeVersion] = SingleKeyMutation(keyValue.value);
		}
	}
	virtual void clear(KeyRangeRef range) {
		ASSERT(m_writeVersion != invalidVersion);

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

	VersionedBTree(IPager *pager, int page_size_override = -1)
	  : m_pager(pager),
		m_writeVersion(invalidVersion),
		m_page_size_override(page_size_override),
		m_lastCommittedVersion(invalidVersion) {
	}

	ACTOR static Future<Void> init(VersionedBTree *self) {
		self->m_root = 0;
		state Version latest = wait(self->m_pager->getLatestVersion());
		if(latest == 0) {
			IPager *pager = self->m_pager;
			++latest;
			self->writePage(self->m_root, FixedSizeMap::emptyPage(EPageFlags::IS_LEAF, [pager](){ return pager->newPageBuffer(); }), latest);
			self->m_pager->setLatestVersion(latest);
			Void _ = wait(self->m_pager->commit());
		}
		self->m_lastCommittedVersion = latest;
		self->initMutationBuffer();
		return Void();
	}

	Future<Void> init() { return init(this); }

	virtual ~VersionedBTree() {
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
		ASSERT(v >= m_writeVersion);
		m_writeVersion = v;
		//m_pager->setLatestVersion(v);
	}

	virtual Future<Void> commit() {
		return commit_impl(this);
	}

private:
	void writePage(LogicalPageID id, Reference<IPage> page, Version ver) {
		FixedSizeMap map = FixedSizeMap::decode(StringRef(page->begin(), page->size()));
		debug_printf("Writing page: id=%d ver=%lld %s\n", id, ver, map.toString().c_str());
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
			ASSERT(!isAtomicOp());

			if(isClear())
				return KeyVersionValue(key, version);

			return KeyVersionValue(key, version, value);
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

	void printMutationBuffer() const {
		return printMutationBuffer(m_buffer.begin(), m_buffer.end());
	}

	void initMutationBuffer() {
		// Create range representing the entire keyspace.  This reduces edge cases to applying mutations
		// because now all existing keys are within some range in the mutation map.
		m_buffer.clear();
		m_buffer[StringRef()];
		m_buffer[endKey.key];
	}

		// Find or create a mutation buffer boundary for bound and return an iterator to it
	MutationBufferT::iterator insertMutationBoundary(Key boundary) {
		// Find the first split point in buffer that is >= key
		MutationBufferT::iterator ib = m_buffer.lower_bound(boundary);

		// Since the initial state of the mutation buffer contains the range '' through
		// the maximum possible key, our search had to have found something.
		ASSERT(ib != m_buffer.end());

		// If we found the boundary we are looking for, return its iterator
		if(ib->first == boundary)
			return ib;

		// ib is our insert hint.  Insert the new boundary and set ib to its entry
		ib = m_buffer.insert(ib, {boundary, RangeMutation()});

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

	void buildNewRoot(Version version, vector<std::pair<int, Reference<IPage>>> &pages, std::vector<LogicalPageID> &logicalPageIDs, FixedSizeMap::KVPairsT &childEntries) {
		// While there are multiple child pages for this version we must write new tree levels.
		while(pages.size() > 1) {
			FixedSizeMap::KVPairsT newChildEntries;
			for(int i=0; i<pages.size(); i++)
				newChildEntries.push_back(KeyValueRef(childEntries[pages[i].first].key, StringRef((unsigned char *)&logicalPageIDs[i], sizeof(uint32_t))));
			childEntries = std::move(newChildEntries);

			int oldPages = pages.size();
			pages = FixedSizeMap::buildMany( childEntries, 0, [=](){ return m_pager->newPageBuffer(); }, m_page_size_override);
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
	ACTOR static Future<VersionedChildrenT> commitSubtree(VersionedBTree *self, Reference<IPagerSnapshot> snapshot, LogicalPageID root, Key lowerBoundKey, Key upperBoundKey) {
		state std::string printPrefix = format("commit subtree root=%lld lower='%s' upper='%s'", root, printable(lowerBoundKey).c_str(), printable(upperBoundKey).c_str());
		debug_printf("%s\n", printPrefix.c_str());

		// Decode the upper and lower bound keys for this subtree
		// Note that these could be truncated to be the shortest usable boundaries between two pages but that
		// still works for what we're using them for here, so instead of using the KVV unpacker we'll have to
		// something else here that supports incomplete keys.
		state KeyVersionValue lowerBoundKVV = KeyVersionValue::unpack(lowerBoundKey);
		state KeyVersionValue upperBoundKVV = KeyVersionValue::unpack(upperBoundKey);

		// Find the slice of the mutation buffer that is relevant to this subtree
		state MutationBufferT::const_iterator iMutationBoundary = self->m_buffer.lower_bound(lowerBoundKVV.key);
		state MutationBufferT::const_iterator iMutationBoundaryEnd = self->m_buffer.lower_bound(upperBoundKVV.key);

		// If the lower bound key and the upper bound key are the same then there can't be any changes to
		// this subtree since changes would happen after the upper bound key as the mutated versions would
		// necessarily be higher.
		if(lowerBoundKVV.key == upperBoundKVV.key) {
			debug_printf("%s no changes, lower and upper bound keys are the same.\n", printPrefix.c_str());
			return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
		}

		// If the mutation buffer key found is greater than the lower bound key then go to the previous mutation
		// buffer key because it may cover deletion of some keys at the start of this subtree.
		if(iMutationBoundary != self->m_buffer.begin() && iMutationBoundary->first > lowerBoundKVV.key)
			--iMutationBoundary;
		else {
			// If the there are no mutations, we're done
			if(iMutationBoundary == iMutationBoundaryEnd) {
				debug_printf("%s no changes, mutation buffer start/end are the same\n", printPrefix.c_str());
				return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
			}
		}

		// Another way to have no mutations is to have a single mutation range cover this
		// subtree but have no changes in it
		MutationBufferT::const_iterator iMutationBoundaryNext = iMutationBoundary;
		++iMutationBoundaryNext;
		if(iMutationBoundaryNext == iMutationBoundaryEnd && iMutationBoundaryNext->second.noChanges()) {
			debug_printf("%s no changes because sole mutation range was empty\n", printPrefix.c_str());
			return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
		}

		state FixedSizeMap map;
		Reference<const IPage> rawPage = wait(snapshot->getPhysicalPage(root));
		map = FixedSizeMap::decode(StringRef(rawPage->begin(), rawPage->size()));
		debug_printf("%s Read page %d: %s\n", printPrefix.c_str(), root, map.toString().c_str());

		if(map.flags & EPageFlags::IS_LEAF) {
			VersionedChildrenT results;
			FixedSizeMap::KVPairsT merged;

			debug_printf("MERGING EXISTING DATA WITH MUTATIONS:\n");
			self->printMutationBuffer(iMutationBoundary, iMutationBoundaryEnd);

			SimpleFixedSizeMapRef::KVPairsT::const_iterator iExisting = map.entries.begin();
			SimpleFixedSizeMapRef::KVPairsT::const_iterator iExistingEnd = map.entries.end();

			// It's a given that the mutation map is not empty so it's safe to do this
			Key mutationRangeStart = iMutationBoundary->first;

			// There will be multiple loops advancing iExisting, existing will track its current value
			KeyVersionValue existing;
			KeyVersionValue lastExisting;
			if(iExisting != iExistingEnd)
				existing = KeyVersionValue::unpack(*iExisting);
			// If replacement pages are written they will be at the minimum version seen in the mutations for this leaf
			Version minVersion = invalidVersion;

			// Now, process each mutation range and merge changes with existing data.
			while(iMutationBoundary != iMutationBoundaryEnd) {
				debug_printf("%s Processing Mutation Range: '%s' to '%s'\n", printPrefix.c_str(), printable(iMutationBoundary->first).c_str(), printable(iMutationBoundaryEnd->first).c_str());
				debug_printf("%s Mutations: %s\n", printPrefix.c_str(), iMutationBoundary->second.toString().c_str());

				SingleKeyMutationsByVersion::const_iterator iMutations;

				// If the mutation boundary key is the same as the page lowerBound key then start reading single
				// key mutations at the first version greater than the lowerBoundKey version.
				if(iMutationBoundary->first == lowerBoundKVV.key)
					iMutations = iMutationBoundary->second.startKeyMutations.upper_bound(lowerBoundKVV.version);
				else
					iMutations = iMutationBoundary->second.startKeyMutations.begin();

				SingleKeyMutationsByVersion::const_iterator iMutationsEnd = iMutationBoundary->second.startKeyMutations.end();

				// Output old versions of the mutation boundary key
				while(iExisting != iExistingEnd && existing.key == iMutationBoundary->first) {
					merged.push_back(existing.pack());
					debug_printf("Added existing version of mutation boundary start key: %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());

					++iExisting;
					if(iExisting != iExistingEnd)
						existing = KeyVersionValue::unpack(*iExisting);
				}

				// TODO:  If a mutation set is equal to the previous existing value of the key, don't write it.
				// Output mutations for the mutation boundary start key
				while(iMutations != iMutationsEnd) {
					if(iMutations->first < minVersion || minVersion == invalidVersion)
						minVersion = iMutations->first;
					merged.push_back(iMutations->second.toKVV(iMutationBoundary->first, iMutations->first).pack());
					debug_printf("Added mutation of boundary start key: %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());
					++iMutations;
				}

				// Get the clear version for this range, which is the last thing that we need from it,
				Optional<Version> clearRangeVersion = iMutationBoundary->second.rangeClearVersion;
				// Advance to the next boundary because we need to know the end key for the current range.
				++iMutationBoundary;

				// Write existing keys which are less than the next mutation boundary key, clearing if needed.
				while(iExisting != iExistingEnd && existing.key < iMutationBoundary->first) {
					merged.push_back(existing.pack());
					debug_printf("Added existing key in mutation range: %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());

					// Write a clear of this key if needed.  A clear is required if clearRangeVersion is set and the next key is different
					// than this one.  Note that the next key might be the in our right sibling, we can use the page upperBound to get that.
					++iExisting;
					KeyVersionValue nextEntry;
					if(iExisting != iExistingEnd)
						nextEntry = KeyVersionValue::unpack(*iExisting);
					else
						nextEntry = upperBoundKVV;

					if(clearRangeVersion.present() && existing.key != nextEntry.key) {
						Version clearVersion = clearRangeVersion.get();
						if(clearVersion < minVersion || minVersion == invalidVersion)
							minVersion = clearVersion;
						merged.push_back(KeyVersionValue(existing.key, clearVersion).pack());
						debug_printf("Added clear of existing key in mutation range: %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());
					}

					if(iExisting != iExistingEnd)
						existing = nextEntry;
				}
			}

			// Write any remaining existing keys, which are not subject to clears as they are beyond the cleared range.
			while(iExisting != iExistingEnd) {
				merged.push_back(existing.pack());
				debug_printf("Added existing tail key: %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());

				++iExisting;
				if(iExisting != iExistingEnd)
					existing = KeyVersionValue::unpack(*iExisting);
			}

			// No changes were actually made.  This could happen if there is a single-key clear which turns out
			// to not actually exist in the leaf page.
			if(minVersion == invalidVersion)
				return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });

			debug_printf("%s DONE MERGING MUTATIONS WITH EXISTING LEAF CONTENTS\n", printPrefix.c_str());

			// TODO: Make version and key splits based on contents of merged list

			IPager *pager = self->m_pager;
			vector< std::pair<int, Reference<IPage>> > pages = FixedSizeMap::buildMany( merged, EPageFlags::IS_LEAF, [pager](){ return pager->newPageBuffer(); }, self->m_page_size_override);

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

			// Write each page using its assigned page ID
			debug_printf("%s Writing %lu replacement pages for %d at version %lld\n", printPrefix.c_str(), pages.size(), root, minVersion);
			for(int i=0; i<pages.size(); i++)
				self->writePage(logicalPages[i], pages[i].second, minVersion);

			// If this commitSubtree() is operating on the root, write new levels if needed until until we're returning a single page
			if(root == self->m_root)
				self->buildNewRoot(minVersion, pages, logicalPages, merged);

			results.push_back({minVersion, {}});

			for(int i=0; i<pages.size(); i++) {
				// The lower bound of the first page is the lower bound of the subtree, not the first entry in the page
				Key lowerBound = (i == 0) ? lowerBoundKey : merged[pages[i].first].key;
				results.back().second.push_back( {lowerBound, logicalPages[i]} );
			}

			debug_printf("%s DONE.\n", printPrefix.c_str());
			return results;
		}
		else {
			state std::vector<Future<VersionedChildrenT>> m_futureChildren;

			for(int i=0; i<map.entries.size(); i++) {
				// The lower bound for the first child is lowerBoundKey, and entries[i] for the rest.
				Key childLowerBound = (i == 0) ? lowerBoundKey : map.entries[i].key;
				// The upper bound for the last child is upperBoundKey, and entries[i+1] for the others.
				Key childUpperBound = ( (i + 1) == map.entries.size()) ? upperBoundKey : map.entries[i + 1].key;

				m_futureChildren.push_back(self->commitSubtree(self, snapshot, *(uint32_t*)map.entries[i].value.begin(), childLowerBound, childUpperBound));
			}

			Void _ = wait(waitForAll(m_futureChildren));

			bool modified = false;
			for( auto &c : m_futureChildren) {
				if(c.get().size() != 1 || c.get()[0].second.size() != 1) {
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

				FixedSizeMap::KVPairsT childEntries;  // Logically std::vector<std::pair<std::string, LogicalPageID>> childEntries;

				// For each Future<VersionedChildrenT>
				debug_printf("%s creating replacement pages for id=%d at Version %lld\n", printPrefix.c_str(), root, version);

				// If we're writing version 0, there is a chance that we don't have to write ourselves, if there are no changes
				bool modified = version != 0;

				for(int i = 0; i < m_futureChildren.size(); ++i) {
					const VersionedChildrenT &children = m_futureChildren[i].get();
					LogicalPageID pageID = *(uint32_t*)map.entries[i].value.begin();

					debug_printf("  Versioned page set that replaced page %d: %lu versions\n", pageID, children.size());
					for(auto &versionedPageSet : children) {
						debug_printf("    version: %lld\n", versionedPageSet.first);
						for(auto &boundaryPage : versionedPageSet.second) {
							debug_printf("      %s -> %u\n", boundaryPage.first.toString().c_str(), boundaryPage.second);
						}
					}

					// Find the first version greater than the current version we are writing
					auto cv = std::upper_bound( children.begin(), children.end(), version, [](Version a, VersionedChildrenT::value_type const &b) { return a < b.first; } );

					// If there are no versions before the one we found, just update nextVersion and continue.
					if(cv == children.begin()) {
						debug_printf("  First version (%lld) in set is greater than current, setting nextVersion and continuing\n", cv->first);
						nextVersion = std::min(nextVersion, cv->first);
						debug_printf("  curr %lld next %lld\n", version, nextVersion);
						continue;
					}

					// If a version greater than the current version being written was found, update nextVersion
					if(cv != children.end()) {
						nextVersion = std::min(nextVersion, cv->first);
						debug_printf("  curr %lld next %lld\n", version, nextVersion);
					}

					// Go back one to the last version that was valid prior to or at the current version we are writing
					--cv;

					debug_printf("  Using children for version %lld from this set, building version %lld\n", cv->first, version);

					// If page count isn't 1 then the root is definitely modified
					modified = modified || cv->second.size() != 1;

					// Add the children at this version to the child entries list for the current version being built.
					for (auto &childPage : cv->second) {
						debug_printf("  Adding child page '%s'\n", childPage.first.toString().c_str());
						childEntries.push_back( KeyValueRef(childPage.first, StringRef((unsigned char *)&childPage.second, sizeof(uint32_t))));
					}
				}

				debug_printf("Finished pass through futurechildren.  childEntries=%lu  version=%lld  nextVersion=%lld\n", childEntries.size(), version, nextVersion);

				if(modified) {
					// TODO: Track split points across iterations of this loop, so that they don't shift unnecessarily and
					// cause unnecessary path copying

					IPager *pager = self->m_pager;
					vector< std::pair<int, Reference<IPage>> > pages = FixedSizeMap::buildMany( childEntries, 0, [pager](){ return pager->newPageBuffer(); }, self->m_page_size_override);

					// For each IPage of data, assign a logical pageID.
					std::vector<LogicalPageID> logicalPages;

					// Only reuse first page if only one page is being returned or if root is not the btree root.
					if(pages.size() == 1 || root != self->m_root)
						logicalPages.push_back(root);

					// Allocate enough pageIDs for all of the pages
					for(int i=logicalPages.size(); i<pages.size(); i++)
						logicalPages.push_back( self->m_pager->allocateLogicalPage() );

					// Write each page using its assigned page ID
					debug_printf("Writing internal pages, subtreeRoot=%u\n", root);
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
						debug_printf("Output same as last version, popping it.\n");
						result.pop_back();
					}
				}
				else {
					debug_printf("Version 0 has no changes\n");
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
		Version latestVersion = wait(self->m_pager->getLatestVersion());

		debug_printf("BEGINNING COMMIT.\n");
		self->printMutationBuffer();

		VersionedChildrenT _ = wait(commitSubtree(self, self->m_pager->getReadSnapshot(latestVersion), self->m_root, StringRef(), endKey.pack().key));

		self->m_pager->setLatestVersion(self->m_writeVersion);
		Void _ = wait(self->m_pager->commit());
		self->m_lastCommittedVersion = self->m_writeVersion;

		self->initMutationBuffer();

		return Void();
	}

	IPager *m_pager;
	MutationBufferT m_buffer;

	Version m_writeVersion;
	Version m_lastCommittedVersion;
	int m_page_size_override;

	class Cursor : public IStoreCursor, public ReferenceCounted<Cursor> {
	public:
		Cursor(Version version, IPager *pager, LogicalPageID root)
		  : m_version(version), m_pager(pager->getReadSnapshot(version)), m_root(root) {
		}
		virtual ~Cursor() {}

		virtual Future<Void> findFirstGreaterOrEqual(KeyRef key, int prefetchNextBytes) NOT_IMPLEMENTED
		virtual Future<Void> findLastLessOrEqual(KeyRef key, int prefetchPriorBytes) NOT_IMPLEMENTED
		virtual Future<Void> next(bool needValue) NOT_IMPLEMENTED
		virtual Future<Void> prev(bool needValue) NOT_IMPLEMENTED

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
			m_pager->invalidateReturnedPages();
		}

		Version m_version;
		Reference<IPagerSnapshot> m_pager;
		Optional<KeyValueRef> m_kv;
		Arena m_arena;
		LogicalPageID m_root;

		void addref() { ReferenceCounted<Cursor>::addref(); }
		void delref() { ReferenceCounted<Cursor>::delref(); }

		ACTOR static Future<Void> findEqual_impl(Reference<Cursor> self, KeyRef key) {
			state LogicalPageID pageNumber = self->m_root;

			state Tuple t;
			t.append(key);
			t.append(self->m_version);
			state KeyRef tupleKey = t.pack();

			loop {
				debug_printf("findEqual: Reading page %d\n", pageNumber);
				Reference<const IPage> rawPage = wait(self->m_pager->getPhysicalPage(pageNumber));
				FixedSizeMap map = FixedSizeMap::decode(StringRef(rawPage->begin(), rawPage->size()));
				//debug_printf("Read page %d @%lld: %s\n", pageNumber, self->m_version, map.toString().c_str());

				// Special case of empty page (which should only happen for root)
				if(map.entries.empty()) {
					ASSERT(pageNumber == self->m_root);
					self->m_kv = Optional<KeyValueRef>();
					return Void();
				}

				if(map.flags && EPageFlags::IS_LEAF) {
					int i = map.findLastLessOrEqual(tupleKey);
					if(i >= 0) {
						KeyVersionValue kvv = KeyVersionValue::unpack(map.entries[i]);
						if(key == kvv.key && kvv.value.present()) {
							self->m_kv = KeyValueRef(StringRef(self->m_arena, key), StringRef(self->m_arena, kvv.value.get()));
							return Void();
						}
					}

					self->m_kv = Optional<KeyValueRef>();
					return Void();
				}
				else {
					int i = map.findLastLessOrEqual(tupleKey);
					i = std::max(i, 0);
					pageNumber = *(uint32_t *)map.entries[i].value.begin();
				}
			}
		}

		virtual Future<Void> findEqual(KeyRef key) {
			return findEqual_impl(Reference<Cursor>::addRef(this), key);
		}
	};
};

KeyVersionValue VersionedBTree::endKey(LiteralStringRef("\xff\xff\xff\xff\xff\xff\xff\xff"), std::numeric_limits<Version>::max());

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

TEST_CASE("/redwood/correctness/memory/set") {
	state bool useDisk = true;

	state IPager *pager;
	if(useDisk)
		pager = new IndirectShadowPager("pagerfile");
	else
		pager = createMemoryPager();

	state int pageSize = g_random->coinflip() ? -1 : g_random->randomInt(50, 200);

	state VersionedBTree *btree = new VersionedBTree(pager, pageSize);

	Void _ = wait(btree->init());

	state std::map<std::pair<std::string, Version>, Optional<std::string>> written;
	state std::set<Key> keys;

	Version lastVer = wait(btree->getLatestVersion());
	printf("Starting from version: %lld\n", lastVer);

	state Version version = lastVer + 1;
	state int commits = g_random->randomInt(1, 20);
	//printf("Will do %d commits\n", commits);
	state double insertTime = 0;
	state int64_t keyBytesInserted = 0;
	state int64_t ValueBytesInserted = 0;

	while(commits--) {
		state double startTime = now();
		int versions = g_random->randomInt(1, 20);
		debug_printf("  Commit will have %d versions\n", versions);
		while(versions--) {
			++version;
			btree->setWriteVersion(version);
			int changes = g_random->randomInt(0, 20);
			debug_printf("    Version %lld will have %d changes\n", version, changes);
			while(changes--) {
				if(g_random->random01() < .10) {
					// Delete a random key
					Key start = randomKV().key;
					Key end = randomKV().key;
					if(end <= start)
						end = keyAfter(start);
					KeyRangeRef range(start, end);
					debug_printf("      Clear '%s' to '%s' @%lld\n", start.toString().c_str(), end.toString().c_str(), version);
					auto w = keys.lower_bound(start);
					auto wEnd = keys.lower_bound(end);
					while(w != wEnd) {
						written[std::make_pair(w->toString(), version)] = Optional<std::string>();
						++w;
					}
					btree->clear(range);
				}
				else {
					KeyValue kv = randomKV();
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

			Future<Void> closedFuture = pager->onClosed();
			pager->close();
			Void _ = wait(closedFuture);

			pager = new IndirectShadowPager("pagerfile");

			btree = new VersionedBTree(pager, pageSize);
			Void _ = wait(btree->init());

			Version lastVer = wait(btree->getLatestVersion());
			ASSERT(lastVer == version);
		}

		while(i != iEnd) {
			state std::string key = i->first.first;
			state Version ver = i->first.second;
			state Optional<std::string> val = i->second;

			state Reference<IStoreCursor> cur = btree->readAtVersion(ver);

			Void _ = wait(cur->findEqual(i->first.first));

			if(val.present()) {
				if(!(cur->isValid() && cur->getKey() == key && cur->getValue() == val.get())) {
					++errors;
					if(!cur->isValid())
						printf("Verify failed: key_not_found: '%s' -> '%s' @%lld\n", key.c_str(), val.get().c_str(), ver);
					else if(cur->getKey() != key)
						printf("Verify failed: key_incorrect: found '%s' expected '%s' @%lld\n", cur->getKey().toString().c_str(), key.c_str(), ver);
					else if(cur->getValue() != val.get())
						printf("Verify failed: value_incorrect: for '%s' found '%s' expected '%s' @%lld\n", cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), val.get().c_str(), ver);
				}
			} else {
				if(cur->isValid() && cur->getKey() == key) {
					++errors;
					printf("Verify failed: cleared_key_found: '%s' -> '%s' @%lld\n", key.c_str(), cur->getValue().toString().c_str(), ver);
				}
			}
			++i;
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
	state VersionedBTree *btree = new VersionedBTree(pager);
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

std::string SimpleFixedSizeMapRef::toString() const {
	std::string result;
	result.append(format("flags=0x%x data: ", flags));
	for(auto const &kv : entries) {
		result.append(" ");
		Tuple t = Tuple::unpack(kv.key);
		result.append("[");
		for(int i = 0; i < t.size(); ++i) {
			if(i != 0)
				result.append(",");
			if(t.getType(i) == Tuple::ElementType::BYTES)
				result.append(format("'%s'", t.getString(i).toString().c_str()));
			if(t.getType(i) == Tuple::ElementType::INT)
				result.append(format("%lld", t.getInt(i)));
		}
		result.append("->");
		if(flags && VersionedBTree::IS_LEAF)
			result.append(format("'%s'", printable(StringRef(kv.value)).c_str()));
		else
			result.append(format("%u", *(const uint32_t *)kv.value.begin()));
		result.append("]");
	}

	return result;
}
