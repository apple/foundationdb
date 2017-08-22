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

	std::string toString();

	KVPairsT entries;
	uint8_t flags;
};

#define NOT_IMPLEMENTED { UNSTOPPABLE_ASSERT(false); }

class VersionedBTree : public IVersionedStore {
public:
	enum EPageFlags { IS_LEAF = 1};

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

		applyMutation(new Mutation(m_writeVersion, keyValue.key, keyValue.value, MutationRef::SetValue));
	}
	virtual void clear(KeyRangeRef range) {
		ASSERT(m_writeVersion != invalidVersion);

		applyMutation(new Mutation(m_writeVersion, range.begin, range.end, MutationRef::ClearRange));
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
		return Void();
	}

	Future<Void> init() { return init(this); }

	virtual ~VersionedBTree() {
		for(Mutation *m : m_mutations)
			delete m;
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

	struct Mutation {
		Mutation(Version ver, Key key, Value valueOrEnd, MutationRef::Type op) : version(ver), key(key), valueOrEnd(valueOrEnd), op(op) {}
		Version version;
		Key key;
		Value valueOrEnd;
		MutationRef::Type op;

		inline bool isClear() const { return op == MutationRef::ClearRange; }
		inline bool isSet() const { return op == MutationRef::SetValue; }
		inline bool isAtomicOp() const { return !isClear() && !isSet(); }

		struct byVersion { bool operator() (const Mutation *a, const Mutation *b) { return a->version < b->version; } };
		struct byKey { bool operator() (const Mutation *a, const Mutation *b) { return a->key < b->key; } };

		std::string toString() {
			return format("@%lld op=%d key=%s value_or_end=%s", version, op, printable(key).c_str(), printable(valueOrEnd).c_str());
		}
	};

	typedef std::set<Mutation *, Mutation::byVersion> MutationsByVersionT;
	typedef std::set<Mutation *, Mutation::byKey> MutationsByKeyT;

	typedef std::map<Key, MutationsByVersionT> MutationBufferT;
	typedef std::map<Version, MutationsByKeyT> MutationBufferPerVersionT;

	/* MutationBufferT should be simplified to the following.  It is more efficient to mutate,
	 * has a smaller memory footprint, and may even be easier to use during commit.  It does
	 * not require sets to be modeled as a mutation of a range from key to keyAfter(key).  It
	 * loses the ability to model "range sets" of unspecified existing keys, but that doesn't
	 * seem very useful anyway.  This model also no longer uses the Mutation structure,
	 * which could possibly be removed entirely unless there is some benefit to having it.
	 *
	 * typedef std::map<Key, std::pair<Optional<Version>, std::map<Version, Optional<Value>>>> MutationBufferT;
	 *
	 * The pair stored for a key in the buffer map represents
	 *   rangeClearVersion: the version at which a range starting with this key was cleared
	 *   individualOps: the individual ops (sets/clear/atomic) done on this key
	 *
	 * - Keys are inserted into the buffer map for every individual operation (set/clear/atomic)
	 *   key and for both the start and end of a range clear.
	 * - When a new key is inserted in the buffer map it should take on the immediately previous
	 *   entry's range clear version.
	 * - To apply a single clear, add it to the individual ops only if the last entry is not also a clear.
	 * - To apply a range clear, set any unset range clear values >= start and < end.
	 *
	 * Example:
	 *
	 * Version Op
	 * 1 set b
	 * 2 set b
	 * 3 clear bb c
	 * 3 clear b
	 * 4 clear a d
	 * 4 set cc
	 * 5 clear cc g
	 *
	 * Buffer state after these operations:
	 * Key  RangeClearVersion   IndividualOps
	 * a    4
	 * b    4                   1,set  2,set  3,clear
	 * bb   3
	 * c    4
	 * cc   4                   4,set  5,clear
	 * d    5
	 * gg
	*/

	// Find or create a mutation buffer boundary for bound and return an iterator to it
	MutationBufferT::iterator insertMutationBoundary(Key bound) {
		// Find the first split point in buffer that is >= key
		MutationBufferT::iterator start = m_buffer.lower_bound(bound);

		// If an exact match was found then we're done
		if(start != m_buffer.end() && start->first == bound)
			return start;

		// If begin was found then bound will be the first lexically ordered boundary so we can just insert it.
		if(start == m_buffer.begin())
			return m_buffer.insert(start, {bound, {}});

		// At this point, we know that
		//  - buffer is not empty
		//  - bound does not exist in buffer
		//  - start points to the first thing passed bound (which could even be end())
		// Previous will refer to the start of the range we are splitting
		MutationBufferT::iterator previous = start;
		--previous;

		// Insert the new boundary
		start = m_buffer.insert(start, {bound, {}});

		// Copy any range mutations from previous boundary to the new one
		for(Mutation *m : previous->second)
			if(m->isClear())
				start->second.insert(m);

		return start;
	}

	void applyMutation(Mutation *m) {
		// Mutation pointers can exist in many places in m_buffer so rather than reference count them
		// since they will all be deleted together we'll just stash them here.
		m_mutations.push_back(m);

		// TODO:  Update mapForVersion if we're going to use it in commitSubtree, or remove it
		//auto &mapForVersion = m_buffer_byVersion[m_writeVersion];

		// TODO:  Combine both cases of these if's as they are now nearly identical except for mutation repeat check and boundary selection
		if(m->isClear()) {
			auto start = insertMutationBoundary(m->key);
			auto end = insertMutationBoundary(m->valueOrEnd);
			while(start != end) {
				auto &mutationSet = start->second;
				auto im = mutationSet.end();
				bool skip = false;
				// If mutationSet has stuff in it, see if the new mutation replaces or
				// combines with the last entry in mutationSet.
				if(im != mutationSet.begin()) {
					--im;
					// If the previous last mutation is a clear, then the new clear does nothing so we can skip it.
					if((*im)->isClear()) {
						skip = true;
					}
					else {
						// If the version is the same, erase it so we can replace it with the new mutation
						if((*im)->version == m->version)
							mutationSet.erase(im);
					}
				}
				if(!skip)
					mutationSet.insert(mutationSet.end(), m);
				++start;
			}
		}
		else {
			insertMutationBoundary(keyAfter(m->key));
			auto &mutationSet = insertMutationBoundary(m->key)->second;
			auto im = mutationSet.end();
			bool skip = false;
			if(im != mutationSet.begin()) {
				--im;
				// If the previous last mutation is a set to the same value as the new one then we can skip the new mutation as it does nothing
				if((*im)->isSet() && m->isSet() && (*im)->valueOrEnd == m->valueOrEnd) {
					skip = true;
				}
				else {
					// If the version is the same, erase it so we can replace it with the new mutation
					if((*im)->version == m->version)
						mutationSet.erase(im);
				}
			}
			if(!skip)
				mutationSet.insert(mutationSet.end(), m);
		}

		/*
		debug_printf("-------------------------------------\n");
		debug_printf("BUFFER\n");
		for(auto &i : m_buffer) {
			debug_printf("'%s'\n", printable(i.first).c_str());
			for(auto j : i.second) {
				debug_printf("\t%s\n", j->toString().c_str());
			}
		}
		debug_printf("-------------------------------------\n");
		*/

	}

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

		std::string toString() const {
			return format("'%s' -> '%s' @%lld", key.toString().c_str(), value.present() ? value.get().toString().c_str() : "<cleared>", version);
		}
	};

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
	ACTOR static Future<VersionedChildrenT> commitSubtree(VersionedBTree *self, Reference<IPagerSnapshot> snapshot, LogicalPageID root, Key lowerBoundKey, MutationBufferT::const_iterator iMutationMap, MutationBufferT::const_iterator iMutationMapEnd) {
		state std::string printPrefix = format("commit subtree(lowerboundkey %s, page %u) ", lowerBoundKey.toString().c_str(), root);
		debug_printf("%s\n", printPrefix.c_str());

		if(iMutationMap == iMutationMapEnd) {
			debug_printf("%s no changes\n", printPrefix.c_str());
			return VersionedChildrenT({ {0,{{lowerBoundKey,root}}} });
		}

		state FixedSizeMap map;
		Reference<const IPage> rawPage = wait(snapshot->getPhysicalPage(root));
		map = FixedSizeMap::decode(StringRef(rawPage->begin(), rawPage->size()));
		debug_printf("%s Read page %d: %s\n", printPrefix.c_str(), root, map.toString().c_str());

		if(map.flags & EPageFlags::IS_LEAF) {
			VersionedChildrenT results;
			FixedSizeMap::KVPairsT merged;

			SimpleFixedSizeMapRef::KVPairsT::const_iterator iExisting = map.entries.begin();
			SimpleFixedSizeMapRef::KVPairsT::const_iterator iExistingEnd = map.entries.end();

			// It's a given that the mutation map is not empty so it's safe to do this
			Key mutationRangeStart = iMutationMap->first;

			// There will be multiple loops advancing iExisting, existing will track its current value
			KeyVersionValue existing;
			if(iExisting != iExistingEnd)
				existing = KeyVersionValue::unpack(*iExisting);
			// If replacement pages are written they will be at the minimum version seen in the mutations for this leaf
			Version minVersion = std::numeric_limits<Version>::max();

			while(iMutationMap != iMutationMapEnd) {
				printf("mutationRangeStart %s\n", printable(mutationRangeStart).c_str());
				MutationsByVersionT::const_iterator iMutations = iMutationMap->second.begin();
				MutationsByVersionT::const_iterator iMutationsEnd = iMutationMap->second.end();

				// The end of the mutation range is the next key in the mutation map
				++iMutationMap;
				Key mutationRangeEnd = (iMutationMap == iMutationMapEnd) ? Key(LiteralStringRef("\xff\xff\xff\xff")) : iMutationMap->first;
				printf("mutationRangeEnd %s\n", printable(mutationRangeEnd).c_str());

				// Tracks whether there was a clearRange covering mutationRange to mutationEnd (or beyond) in the mutation set
				Version rangeClearedVersion = invalidVersion;
				KeyVersionValue lastExisting;

				// First read and output any existing values that are less than or equal to mutationRangeStart
				while(iExisting != iExistingEnd && existing.key <= mutationRangeStart) {
					merged.push_back(existing.pack());
					debug_printf("Added existing pre range %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());
					++iExisting;
					if(iExisting != iExistingEnd) {
						lastExisting = existing;
						existing = KeyVersionValue::unpack(*iExisting);
					}
				}

				// Now insert the mutations which affect the individual key of mutationRangeStart
				bool firstMutation = true;
				while(iMutations != iMutationsEnd) {
					Mutation *m = *iMutations;
					debug_printf("mutation: %s first: %d\n", m->toString().c_str(), firstMutation);
					// Potentially update earliest version of mutations being applied.
					if(m->version < minVersion)
						minVersion = m->version;

					if(m->isClear()) {
						// Any clear range mutation applies at least from mutationRangeStart to mutationRangeEnd, by definition,
						// because clears cause insertions of split points into the mutation buffer
						if(rangeClearedVersion == invalidVersion)
							rangeClearedVersion = m->version;

						// Here we're only handling clears for the key mutationRangeStart, if the rest of the range
						// is cleared it will be handled below
						// Only write a clear if this is either not the first mutation OR
						// lastExisting was a clear for the same key
						if(!firstMutation || (lastExisting.value.present() && lastExisting.key == mutationRangeStart)) {
							merged.push_back(KeyVersionValue(mutationRangeStart, m->version).pack());
							debug_printf("Added clear of existing or from mutation buffer %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());
						}
					}
					else if(m->isSet()) {
						// Write the new value if this is not the first mutation or they is different from
						// lastExisting or the lastExisting value was either empty or not the same as m.
						if(    !firstMutation
							|| lastExisting.key != m->key
							|| !lastExisting.value.present()
							|| lastExisting.value.get() != m->valueOrEnd
						)
							merged.push_back(KeyVersionValue(m->key, m->version, m->valueOrEnd).pack());
							debug_printf("Added set mutation %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());
					}
					else { // isAtomicOp
						ASSERT(m->isAtomicOp());
						// TODO: apply atomic op to last existing, update lastExisting
						ASSERT(false);
					}
					++iMutations;
					if(firstMutation)
						firstMutation = false;
				}

				// Next, while the existing key is in this mutation range output it and also emit
				// a clear at each key if required.
				while(iExisting != iExistingEnd && existing.key < mutationRangeEnd) {
					merged.push_back(existing.pack());
					debug_printf("Added existing in range %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());
					++iExisting;
					if(iExisting != iExistingEnd) {
						lastExisting = existing;
						existing = KeyVersionValue::unpack(*iExisting);
						// If the next key is different than the last then add a clear of the last key
						// at the range clear version if the range was cleared
						if(existing.key != lastExisting.key && rangeClearedVersion != invalidVersion) {
							merged.push_back(KeyVersionValue(lastExisting.key, rangeClearedVersion).pack());
							debug_printf("Added clear of existing, next key different %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());
						}
					}
					else if(rangeClearedVersion != invalidVersion) {
						// If there are no more existing keys but the range was cleared then add a clear of the last key
						merged.push_back(KeyVersionValue(lastExisting.key, rangeClearedVersion).pack());
						debug_printf("Added clear of last existing %s\n", KeyVersionValue::unpack(merged.back()).toString().c_str());
					}
				}

				mutationRangeStart = mutationRangeEnd;
			}
			debug_printf("DONE MERGING MUTATIONS WITH EXISTING LEAF CONTENTS\n");

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
				results.back().second.push_back( {merged[pages[i].first].key, logicalPages[i]} );
			}

			debug_printf("%s DONE.\n", printPrefix.c_str());
			return results;
		}
		else {
			state std::vector<Future<VersionedChildrenT>> m_futureChildren;

			auto childMutBegin = iMutationMap;

			for(int i=0; i<map.entries.size(); i++) {
				auto childMutEnd = iMutationMapEnd;
				if (i+1 != map.entries.size()) {
					childMutEnd = self->m_buffer.lower_bound( KeyVersionValue::unpack(KeyValueRef(map.entries[i+1].key, ValueRef())).key );
				}

				m_futureChildren.push_back(self->commitSubtree(self, snapshot, *(uint32_t*)map.entries[i].value.begin(), map.entries[i].key, childMutBegin, childMutEnd));
				childMutBegin = childMutEnd;
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

		debug_printf("-------------------------------------\n");
		debug_printf("BEGIN COMMIT.  MUTATION BUFFER:\n");
		for(auto &i : self->m_buffer) {
			debug_printf("'%s'\n", printable(i.first).c_str());
			for(auto j : i.second) {
				debug_printf("\t%s\n", j->toString().c_str());
			}
		}
		debug_printf("-------------------------------------\n");

		VersionedChildrenT _ = wait(commitSubtree(self, self->m_pager->getReadSnapshot(latestVersion), self->m_root, StringRef(), self->m_buffer.begin(), self->m_buffer.end()));

		self->m_pager->setLatestVersion(self->m_writeVersion);
		Void _ = wait(self->m_pager->commit());
		self->m_lastCommittedVersion = self->m_writeVersion;

		self->m_buffer.clear();
		for(Mutation *m : self->m_mutations)
			delete m;
		self->m_mutations.clear();

		return Void();
	}

	IPager *m_pager;
	MutationBufferT m_buffer;

	// TODO:  Use or lose this
	MutationBufferPerVersionT m_buffer_byVersion;

	std::vector<Mutation *> m_mutations;
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
				debug_printf("Read page %d @%lld: %s\n", pageNumber, self->m_version, map.toString().c_str());

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

std::string SimpleFixedSizeMapRef::toString() {
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
				result.append(format("%s", t.getString(i).toString().c_str()));
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
